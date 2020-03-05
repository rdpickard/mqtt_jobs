# I'm not sure what I'll end up calling this
# To start the dispatcher and the worker code is going to be in the same file
# It'll probably make sense to break at least the job scheduling logic out at some point soon

# This is going to assume the version of MQTT is lower than 5 with no native support for response topics in messages

import uuid
import threading
import time
import json
import logging

import paho.mqtt.client as mqtt

# QUESTION Should these messages have version-ing?
# QUESTION Should there be a generic message envelope to identify the payload type?
# QUESTION Should the envelope include a response topic?
# QUESTION Should the message envelope support cryptographic signing of the payload?
jobbermessage_helloworld_schema = {
  "$id": "https://example.com/person.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "helloworld jobber message",
  "type": "object",
  "properties": {
    "thing_id": {
      "type": "string",
      "description": "The id of the sender. The id persists between thing reboots and joining of broker"
    },
    "message": {
      "type": "string",
      "description": "Generic message sent"
    },
  }
}

jobbermessage_joboffer_schema = {
  "$id": "https://example.com/person.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "job offer jobber message",
  "type": "object",
  "properties": {
    "offer_id": {
      "type": "string",
      "description": "The offer identifier. Not the same as the job. The offer is for recruiting workers, who are then"
                     "directed to the right topic for the job"
    },
    "message": {
      "type": "string",
      "description": "Generic message sent"
    },
    "worker_thing_criteria": {
        "type": "array",
        "description": "list of criteria a worker must meet to accept a job"
    }
  }
}

jobber_topic_offers_path = "mqtt_jobber/offers/{offer_id}"
jobber_topic_workers_path = "mqtt_jobber/job/{job_number}/workers"
jobber_thing_client_message = "mqtt_jobber/thing/{thing_id}/{client_id}/incoming"


class JobberDispatcher(threading.Thread):
    _mqtt_client = None
    _mqtt_client_my_id = None
    _logger = None

    # TODO Array of job dictionaries needs to be replaced by something like a DB for persistence
    jobs = []

    def __init__(self, mqtt_broker_host, mqtt_broker_port=1883, keep_alive=60,
                 client_id=mqtt.base62(uuid.uuid4().int, padding=22),
                 logger=logging.getLogger("mqtt_jobber.JobberDispatcher")):
        threading.Thread.__init__(self)
        self._logger = logger

        # AFAIK there isn't a way to get a the client_id from the mqtt Client object if it isn't specified in the
        # initialization of Client object and the class generates it's own id. So just set it to some value using
        # the same method the Client object implementation does. That way I know what the ID is. The ID is used later
        # for dispatching jobs on a per-client topic
        self._mqtt_client_my_id = client_id

        # Create the client to the MQTT broker for the dispatcher
        self._mqtt_client = mqtt.Client(client_id=self._mqtt_client_my_id)
        self._mqtt_client.on_connect = self.on_connect
        self._mqtt_client.on_message = self.on_message
        self._mqtt_client.connect(mqtt_broker_host, mqtt_broker_port, keep_alive)

    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, client, userdata, flags, rc):
        self._logger.info("Connected with result code " + str(rc))

        # Join the jobber dispatcher topic
        # QUESTION is there a reason for Dispatcher to get messages on the dispatch topic?
        #self._mqtt_client.subscribe("mqtt_jobber/dispatch.json")

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        self._logger.info(msg.topic + " " + str(msg.payload))

    def run(self):
        self._mqtt_client.loop_forever()

    def stop(self):
        self._mqtt_client.disconnect()

    def dispatch_job_offer(self, job):

        # Create / subscribe to topic for workers to offer services for new job
        # QUESTION is it better to subscribe to a wildcard topic
        self._mqtt_client.subscribe(jobber_topic_offers_path.format(offer_id=job["offer_id"]))

        self._mqtt_client.publish("mqtt_jobber/dispatch.json", json.dumps(job))

    def new_job(self, description="", max_workers=-1, min_workers=-1, pattern=None):
        # TODO Job needs to be persisted to some kind of DB
        # TODO Look at SymPy for implementing worker criteria as modal logic expression
        #  https://docs.sympy.org/latest/index.html
        job = {
            "description": description,
            "offer_id": "o"+mqtt.base62(uuid.uuid4().int, padding=22),
            "job_id": "j"+mqtt.base62(uuid.uuid4().int, padding=22),
            "max_workers": max_workers,
            "min_workers": min_workers,
            "worker_criteria": None,
            "pattern": pattern,
        }
        return job


class JobberWorker(threading.Thread):

    _mqtt_client = None
    _mqtt_client_my_id = None
    _logger = None

    thing_id = None

    def __init__(self, thing_id,
                 mqtt_broker_host, mqtt_broker_port=1883, keep_alive=60,
                 client_id=None,
                 logger=logging.getLogger("mqtt_jobber.JobberWorker")):
        threading.Thread.__init__(self)

        # Create the client to the MQTT broker for the worker

        # AFAIK there isn't a way to get a the client_id from the mqtt Client object if it isn't specified in the
        # initialization of Client object and the class generates it's own id. So just set it to some value using
        # the same method the Client object implementation does. That way I know what the ID is. The ID is used later
        # for dispatching jobs on a per-client topic
        if client_id is None:
            client_id = "{}-{}".format(thing_id, mqtt.base62(uuid.uuid4().int))
        self._mqtt_client_my_id = client_id

        logger = logging.getLogger("mqtt_jobber.JobberWorker."+self._mqtt_client_my_id)

        self._logger = logger

        self._mqtt_client = mqtt.Client(client_id=self._mqtt_client_my_id)
        self._mqtt_client.on_connect = self.on_connect
        self._mqtt_client.on_message = self.on_message
        self._mqtt_client.connect(mqtt_broker_host, mqtt_broker_port, keep_alive)

    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, client, userdata, flags, rc):
        self._logger.info("Connected with result code " + str(rc))

        # Join the jobber dispatcher topic
        self._mqtt_client.subscribe("mqtt_jobber/dispatch.json")

        # Join topic for this client
        # QUESTION Is there a way to kick others off a topic?
        self._mqtt_client.subscribe("mqtt_jobber/workers/"+self._mqtt_client_my_id)

        # Join the topic for this client
        self._mqtt_client.subscribe(jobber_thing_client_message.format(thing_id=self.thing_id,
                                                                       client_id=self._mqtt_client_my_id))

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        if msg.topic.endswith(".json"):
            payload = json.loads(msg.payload)
        else:
            payload = msg.payload

        self._logger.info(msg.topic + " " + str(payload))

    def run(self):
        self._mqtt_client.loop_forever()

    def stop(self):
        self._mqtt_client.disconnect()

    def bleep(self):
        self._mqtt_client.publish("mqtt_jobber/dispatch", "Bleep!")

    def worker_threading_excepthook(self, exc_type, exc_value, exc_traceback, thread):
        self.stop()


logging.basicConfig(level=logging.DEBUG)
dispatcher = JobberDispatcher("localhost")
dispatcher.start()

worker = JobberWorker("thing 1", "localhost")
worker.start()
time.sleep(1)
#worker.bleep()
dispatcher.dispatch_job_offer(dispatcher.new_job("news jobs"))
time.sleep(2)

worker.stop()
dispatcher.stop()
