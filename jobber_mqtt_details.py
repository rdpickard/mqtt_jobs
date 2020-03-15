import uuid
import threading
import time
import json
import logging
import re
import traceback
import base64

import paho.mqtt.client as mqtt

# QUESTION Should these messages have version-ing?
# QUESTION Should there be a generic message envelope to identify the payload type?
# QUESTION Should the envelope include a response topic?
# QUESTION Should the message envelope support cryptographic signing of the payload?

jobbermessage_worker_heartbeat_message = {"client_id": None, "sent_timestamp": None}

jobber_topic_offers_path = "mqtt_jobber/offers/{offer_id}.json"
jobber_topic_workers_path = "mqtt_jobber/job/{job_number}/workers"
jobber_topic_dispatcher_path = "mqtt_jobber/job/{job_number}/dispatcher.json"
jobber_thing_client_message = "mqtt_jobber/thing/{thing_id}/{client_id}/incoming"


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


class JobberJob:

    def __init__(self):
        pass

    def on_results_callback(self, result):
        pass

    def on_worker_finished_callback(self, result):
        pass

    def task(self, task_parameters):
        pass


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
        print("HERE")
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
