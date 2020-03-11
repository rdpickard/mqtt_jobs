import uuid
import threading
import time
import json
import logging
import re

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

# TODO need to specify timestamp format and tz
jobbermessage_worker_heartbeat_message_schema = {
    "$id": "https://example.com/person.schema.json",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "job offer jobber message",
    "type": "object",
    "properties": {
        "client_id": {
            "type": "string",
            "description": "ID of who is sending the heartbeat"
        },
        "sent_timestamp": {
            "type": "string",
            "description": "The time stamp of when the heartbeat was sent from the client"
        },
        "work_seq": {
            "type": "integer",
            "description": "How much work has been done since last heart beat"
        }
    }
}
jobbermessage_worker_heartbeat_message = {"client_id": None, "sent_timestamp": None}

jobber_topic_offers_path = "mqtt_jobber/offers/{offer_id}.json"
jobber_topic_workers_path = "mqtt_jobber/job/{job_number}/workers"
jobber_topic_dispatcher_path = "mqtt_jobber/job/{job_number}/dispatcher"
jobber_thing_client_message = "mqtt_jobber/thing/{thing_id}/{client_id}/incoming"

mqtt_topics_and_messages = {
    "job_offers": {
        "topic_path": "mqtt_jobber/offers/{offer_id}.json",
        "match_reject": 

    },
    "workers_topic": {

    },
    "job_topic": {

    }
}


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

        logger = logging.getLogger("mqtt_jobber.JobberWorker."+self._mqtt_client_my_id)

        self._logger = logger

        self._mqtt_client = mqtt.Client(client_id=self._mqtt_client_my_id)
        self._mqtt_client.on_connect = self.on_connect
        self._mqtt_client.on_message = self.on_message
        self._mqtt_client.connect(mqtt_broker_host, mqtt_broker_port, keep_alive)

    def on_connect(self, client, userdata, flags, rc):
        self._logger.info(self._mqtt_client_my_id+"@* Connected with result code " + str(rc))


    def on_message(self, client, userdata, msg):
        #self._logger.warning("Using base class JobberMQTTThreadedClient on_message, need to over load in implementing" +
        #                    " subclass")

        if msg.topic.endswith(".json"):
            payload = json.loads(msg.payload)
        else:
            payload = msg.payload


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

