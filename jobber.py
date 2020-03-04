# I'm not sure what I'll end up calling this
# To start the dispatcher and the worker code is going to be in the same file
# It'll probably make sense to break at least the job scheduling logic out at some point soon

# This is going to assume the version of MQTT is lower than 5 with no native support for response topics in messages

import uuid
import threading
import time

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
    "job_id": {
      "type": "string",
      "description": "The id of the sender. The id persists between thing reboots and joining of broker"
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


class JobberDispatcher(threading.Thread):
    _mqtt_client = None
    _mqtt_client_my_id = None

    def __init__(self, mqtt_broker_host, mqtt_broker_port=1883, keep_alive=60,
                 client_id=mqtt.base62(uuid.uuid4().int, padding=22)):
        threading.Thread.__init__(self)
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
        print("Connected with result code " + str(rc))

        # Join the jobber dispatcher topic
        self._mqtt_client.subscribe("mqtt_jobber/dispatch")

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        print(msg.topic + " " + str(msg.payload))

    def run(self):
        self._mqtt_client.loop_forever()

    def stop(self):
        self._mqtt_client.disconnect()

    def dispatch_job_offer(self):
        self._mqtt_client.publish("mqtt_jobber/dispatch", "Bleep!")


class JobberWorker(threading.Thread):

    _mqtt_client = None
    _mqtt_client_my_id = None

    def __init__(self, mqtt_broker_host, mqtt_broker_port=1883, keep_alive=60,
                 client_id=mqtt.base62(uuid.uuid4().int, padding=22)):
        threading.Thread.__init__(self)
        # Create the client to the MQTT broker for the worker

        # AFAIK there isn't a way to get a the client_id from the mqtt Client object if it isn't specified in the
        # initialization of Client object and the class generates it's own id. So just set it to some value using
        # the same method the Client object implementation does. That way I know what the ID is. The ID is used later
        # for dispatching jobs on a per-client topic
        self._mqtt_client_my_id = client_id

        self._mqtt_client = mqtt.Client(client_id=self._mqtt_client_my_id)
        self._mqtt_client.on_connect = self.on_connect
        self._mqtt_client.on_message = self.on_message
        self._mqtt_client.connect(mqtt_broker_host, mqtt_broker_port, keep_alive)

    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code " + str(rc))

        # Join the jobber dispatcher topic
        self._mqtt_client.subscribe("mqtt_jobber/dispatch")

        # Join topic for this client
        # QUESTION Is there a way to kick others off a topic?
        self._mqtt_client.subscribe("mqtt_jobber/workers/"+self._mqtt_client_my_id)

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        print(msg.topic + " " + str(msg.payload))

    def run(self):
        self._mqtt_client.loop_forever()

    def stop(self):
        self._mqtt_client.disconnect()

    def bleep(self):
        self._mqtt_client.publish("mqtt_jobber/dispatch", "Bleep!")


dispatcher = JobberDispatcher("localhost")
dispatcher.start()

worker = JobberWorker("localhost")
worker.start()
worker.bleep()
time.sleep(2)

worker.stop()
dispatcher.stop()
