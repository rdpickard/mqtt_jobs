import uuid
import threading
import time
import json
import logging
import re
import traceback
import base64
import datetime
import codecs
import pickle
import os
import secrets

import sqlalchemy
import sqlalchemy.orm
from sqlalchemy.ext.declarative import declarative_base

import paho.mqtt.client as mqtt

# QUESTION Should these messages have version-ing?
# QUESTION Should there be a generic message envelope to identify the payload type?
# QUESTION Should the envelope include a response topic?
# QUESTION Should the message envelope support cryptographic signing of the payload?

jobbermessage_worker_heartbeat_message = {"client_id": None, "sent_timestamp": None}

jobber_topic_offers_path = "mqtt_jobber/offers/{offer_id}.json"
jobber_topic_workers_path = "mqtt_jobber/job/{offer_id}/workers"
jobber_topic_dispatcher_path = "mqtt_jobber/job/{consignment_id}/dispatcher.json"
jobber_thing_client_message = "mqtt_jobber/thing/{thing_id}/{client_id}/incoming"

result_pattern_each = "AFTER_EACH_CALLBACK"
result_pattern_total = "AFTER_{total}_CALLBACK"

registered_jobs = {}

Base = declarative_base()


def random_hex_string(prefix: str = "", length: int = 22):
    return "{prefix}{rando}".format(prefix=prefix, rando=secrets.token_hex(length))


class Consignment(Base):
    __tablename__ = "consignment"

    id = sqlalchemy.Column(sqlalchemy.String, primary_key=True, default=random_hex_string("c", 22))
    created_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP, default=datetime.datetime.utcnow)
    finished_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP)
    last_updated_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP)

    offers = sqlalchemy.orm.relationship("ConsignmentOffer")
    work_parameters = sqlalchemy.Column(sqlalchemy.PickleType)
    worker_requirements = sqlalchemy.Column(sqlalchemy.PickleType)
    worker_pattern = sqlalchemy.Column(sqlalchemy.String)
    results = sqlalchemy.orm.relationship("ConsignmentResult")

    job_pattern = sqlalchemy.Column(sqlalchemy.String)
    results_pattern = sqlalchemy.Column(sqlalchemy.String)

    job_name = sqlalchemy.Column(sqlalchemy.String)
    description = sqlalchemy.Column(sqlalchemy.String)

    def dump_json_dispatcher_message(self):
        json_dict = {
            "id": self.id,
            "work_parameters": self.work_parameters,
            "job_pattern": self.job_pattern,
            "job_name": self.job_name,
        }
        return json_dict


class ConsignmentOffer(Base):
    __tablename__ = "consignment_offer"

    id = sqlalchemy.Column(sqlalchemy.String, primary_key=True, default=random_hex_string("co", 22))

    created_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP, default=datetime.datetime.utcnow)
    closed_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP)
    ttl_in_seconds = sqlalchemy.Column(sqlalchemy.Integer)
    consignment_id = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey('consignment.id'))
    consignment = sqlalchemy.orm.relationship("Consignment", back_populates="offers")

    def dispatcher_dump_json_message(self):
        json_dict = {
            "offer_id": self.id,
            "worker_criteria": self.consignment.worker_requirements,
            "job": self.consignment.job,
            "ttl": self.ttl_in_seconds
        }
        return json_dict

    def dispatcher_send_client_consignment_details(self, jobber_mqtt_client, client_id):
        msg = self.consignment.dump_json_dispatcher_message()
        jobber_mqtt_client.jobber_publish("mqtt_jobber/workers/{client_id}/contracts.json".format(client_id=client_id),
                                          json.dumps(msg))

    @staticmethod
    def client_send_accept_offer(consignment_offer_id, jobber_mqtt_client):

        msg = {
            "client_id": jobber_mqtt_client.client_id,
            "offer_id": consignment_offer_id
        }

        jobber_mqtt_client.jobber_publish(jobber_topic_offers_path.format(offer_id=consignment_offer_id),
                                          json.dumps(msg))


class ConsignmentResult(Base):
    __tablename__ = "consignment_result"

    id = sqlalchemy.Column(sqlalchemy.String, primary_key=True, default=lambda :random_hex_string("cr", 22))
    consignment_id = sqlalchemy.Column(sqlalchemy.String, sqlalchemy.ForeignKey('consignment.id'))
    timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP, default=datetime.datetime.utcnow)
    received_timestamp_utc = sqlalchemy.Column(sqlalchemy.TIMESTAMP)
    client_id = sqlalchemy.Column(sqlalchemy.String)
    result = sqlalchemy.Column(sqlalchemy.String)
    result_encoding = sqlalchemy.Column(sqlalchemy.String)
    work_state = sqlalchemy.Column(sqlalchemy.Integer)
    work_sequence_number = sqlalchemy.Column(sqlalchemy.Integer)

    WORK_STATE_ONGOING = 1
    WORK_STATE_FINISHED = 10

    consignment = sqlalchemy.orm.relationship("Consignment",
                                              back_populates="results")

    @staticmethod
    def load_from_message(mqtt_message_dict, mqtt_topic, logger):
        try:
            consignment_id = re.match("mqtt_jobber/job/([a-zA-Z0-9]*)/dispatcher.json", mqtt_topic).groups()[0]
        except IndexError:
            logger.error("Could not find expected consignment id in topic \"{topic}\"".format(topic=mqtt_topic))
            return

        result = ConsignmentResult()
        result.consignment_id = consignment_id
        result.received_timestamp_utc = datetime.datetime.utcnow()
        result.timestamp_utc = datetime.datetime.fromisoformat(mqtt_message_dict["sent_timestamp"])
        result.client_id = mqtt_message_dict["client_id"]
        result.result = mqtt_message_dict["results"]
        result.result_encoding = mqtt_message_dict["results_encoding"]
        result.work_state = mqtt_message_dict["work_state"]
        result.work_sequence_number = mqtt_message_dict["work_seq"]
        result.message = mqtt_message_dict["message"]

        return result

    @staticmethod
    def encode_dict_result(res):
        if type(res) is dict:
            return codecs.encode(pickle.dumps(res), "base64").decode(), "pickle_base64"

    @staticmethod
    def decode_result(result, result_encoding, logger=logging.getLogger("jobber")):
        if result_encoding == "pickle_base64":
            return pickle.loads(codecs.decode(result.encode(), 'base64'))
        else:
            logger.warning("Can decode results for ConsignmentResult result encoding \"{encoding}\" isn't implemented".format(encoding=result_encoding))
            return None

    @staticmethod
    def send_update(consignment_id, jobber_mqtt_client,
                    result, results_encoding,
                    message,
                    work_state, work_sequence,
                    logger):
        if result is not None and type(result) is not str:
            logger.error("Result value must be a string.")
            return

        msg = {
            "client_id": jobber_mqtt_client.client_id,
            "sent_timestamp": str(datetime.datetime.utcnow()),
            "work_state": work_state,
            "results": result,
            "results_encoding": results_encoding,
            "message": message,
            "work_seq": work_sequence
        }

        jobber_mqtt_client.jobber_publish(jobber_topic_dispatcher_path.format(consignment_id=consignment_id),
                                          json.dumps(msg))

    @staticmethod
    def send_result(consignment_id, jobber_mqtt_client,
                    result, results_encoding,
                    work_sequence_number, logger):
        ConsignmentResult.send_update(consignment_id, jobber_mqtt_client,
                                      result, results_encoding,
                                      None,
                                      ConsignmentResult.WORK_STATE_ONGOING, work_sequence_number,
                                      logger)

    @staticmethod
    def send_heartbeat(consignment_id, jobber_mqtt_client, work_sequence_number, logger):
        ConsignmentResult.send_update(consignment_id, jobber_mqtt_client,
                                      None, None,
                                      None,
                                      ConsignmentResult.WORK_STATE_ONGOING, work_sequence_number,
                                      logger)

    @staticmethod
    def send_finished(consignment_id, jobber_mqtt_client, logger):
        ConsignmentResult.send_update(consignment_id, jobber_mqtt_client,
                                      None, None,
                                      None,
                                      ConsignmentResult.WORK_STATE_FINISHED, 0,
                                      logger)


def register_job(cls):
    print(cls.name)
    registered_jobs[cls.name] = cls

    return cls


class Job:

    name = None

    @staticmethod
    def on_results_callback(result, db_session):
        pass

    @staticmethod
    def on_worker_finished_callback(all_results_for_worker):
        pass

    @staticmethod
    def do_task(worker, job_id, task_parameters):
        pass


def mqtt_threaded_client_exception_catcher(func):
    def wrapper(*args):
        try:
            return func(*args)
        except Exception as e:
            tb = traceback.format_exc()
            logger = args[0]._logger
            logger.error("{client_id} {function} \"{error}\" \"{tb}\"".format(client_id=args[0]._mqtt_client_my_id,
                                                                              function=str(func),
                                                                              error=str(e),
                                                                              tb=str(
                                                                                  base64.b64encode(tb.encode("utf-8")),
                                                                                  "utf-8")))

    return wrapper


class JobberMQTTThreadedClient(threading.Thread):
    _mqtt_client = None
    _mqtt_client_my_id = None
    _logger = None

    thing_id = None

    def __init__(self, thing_id,
                 mqtt_broker_host, mqtt_broker_port=1883, keep_alive=60,
                 client_id=None,
                 do_connect=True,
                 logger=logging.getLogger("mqtt_jobber.JobberWorker")):
        threading.Thread.__init__(self)
        self.excepthook = self.worker_threading_excepthook

        self.thing_id = thing_id
        # Create the client to the MQTT broker for the worker

        # AFAIK there isn't a way to get a the client_id from the mqtt Client object if it isn't specified in the
        # initialization of Client object and the class generates it's own id. So just set it to some value using
        # the same method the Client object implementation does. That way I know what the ID is. The ID is used later
        # for dispatching jobs on a per-client topic
        if client_id is None:
            client_id = "{}-{}".format(thing_id, mqtt.base62(uuid.uuid4().int))
        self._mqtt_client_my_id = client_id

        logger = logging.getLogger("mqtt_jobber.JobberWorker." + self._mqtt_client_my_id)

        self._logger = logger

        self._mqtt_client = mqtt.Client(client_id=self._mqtt_client_my_id)
        self._mqtt_client.on_connect = self.on_connect
        self._mqtt_client.on_message = self.on_message
        self._mqtt_client.connect(mqtt_broker_host, mqtt_broker_port, keep_alive)

    def on_connect(self, client, userdata, flags, rc):
        self._logger.info("\\CON\\ " + self._mqtt_client_my_id + "@* Connected with result code " + str(rc))

    def on_message(self, client, userdata, msg):
        # self._logger.info("\\RCV\\ "+self._mqtt_client_my_id+"@"+msg.topic+"\""+msg.payload+"\"")

        if msg.topic.endswith(".json"):
            payload = json.loads(msg.payload)
        else:
            payload = msg.payload

    def jobber_publish(self, topic, payload):
        self._logger.info("\\PUB\\ " + self._mqtt_client_my_id + "@" + topic + "\"" + payload + "\"")
        self._mqtt_client.publish(topic, payload)

    def jobber_subscribe(self, topic):
        self._logger.info("\\SUB\\ " + self._mqtt_client_my_id + "@" + topic)
        self._mqtt_client.subscribe(topic)

    def run(self):
        self._mqtt_client.loop_forever()

    def stop(self):
        self._mqtt_client.disconnect()

    def worker_threading_excepthook(self, exc_type, exc_value, exc_traceback, thread):
        logging.error("CRAP")
        self.stop()

    def __repr__(self):
        me = {"thing_id": self.thing_id, "client_id": self._mqtt_client_my_id}
        return json.dumps(me)


results_patterns = {}

class BehaviorPattern:
    behavior_pattern = None
    behavior_regex = None
    behavior = None
    description = None

    def __init__(self, **kwargs):
        self.behavior_regex = re.sub("{([a-zA-Z0-9]*)}", "(?P<\\1>.[a-zA-Z0-9]*)", self.behavior_pattern)
        self.behavior = self.behavior_pattern.format(**kwargs)

    def _eval(self, pattern):
        m = re.match(self.behavior_regex, self.behavior)
        if m is not None:
            return m.group
        else:
            return None

    def test(self, pattern, consignment, db_session):
        return False

    def __str__(self):
        return self.behavior


class BehaviorAfterNFinishedWorkers(BehaviorPattern):
    behavior_pattern = "-AFTER_{count}_FINISHED_WORKERS-"

    def test(self, pattern, consignment, db_session):
        group = self._eval(pattern)
        if group is None:
            return False
        finished_workers = db_session.query(ConsignmentResult).filter_by(consignment_id=consignment.id,
                                                                         work_state=ConsignmentResult.WORK_STATE_FINISHED).count()
        return finished_workers >= group('count')


class BehaviorAfterNResults(BehaviorPattern):
    behavior_pattern = "-AFTER_{count}_RESULTS-"

    def test(self, pattern, consignment, db_session):
        group = self._eval(pattern)
        if group is None:
            return False
        return db_session.query(ConsignmentResult).filter_by(consignment_id=consignment.id).count() >= group('count')


class BehaviorAfterNSecondsActivity(BehaviorPattern):
    behavior_pattern = "-AFTER_{seconds}_SECONDS_ACTIVITY-"

    def test(self, pattern, consignment, db_session):
        group = self._eval(pattern)
        if group is None:
            return False
        print(int(group('seconds')))
        return (datetime.datetime.utcnow() - consignment.created_timestamp_utc).total_seconds() > int(group('seconds'))


class PatternAfterNSecondsInactivity(BehaviorPattern):
    behavior = "-AFTER_{seconds}_SECONDS_INACTIVITY-"

    def test(self, pattern, consignment, db_session):
        group = self._eval(pattern)
        if group is None:
            return False
        return (consignment.last_updated_timestamp_utc - datetime.datetime.utcnow()).total_seconds() > int(group('seconds'))

