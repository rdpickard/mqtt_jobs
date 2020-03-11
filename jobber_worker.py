from jobber_mqtt_details import *


class JobberWorker(JobberMQTTThreadedClient):
    _mqtt_client = None
    _mqtt_client_my_id = None
    _logger = None

    thing_id = None

    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, client, userdata, flags, rc):
        JobberMQTTThreadedClient.on_connect(self, client, userdata, flags, rc)

        # Join the jobber dispatcher topic
        self.jobber_subscribe("mqtt_jobber/dispatch.json")

        # Join topic for this client
        # QUESTION Is there a way to kick others off a topic?
        self.jobber_subscribe("mqtt_jobber/workers/" + self._mqtt_client_my_id + "/contracts.json")
        # Join the topic for this client
        self.jobber_subscribe(jobber_thing_client_message.format(thing_id=self.thing_id,
                                                                 client_id=self._mqtt_client_my_id))

    def on_message(self, client, userdata, msg):
        try:
            self._logger.info("\\RCV\\ {client_id}@{topic} \"{payload}\"".format(client_id=self._mqtt_client_my_id,
                                                                                 topic=msg.topic,
                                                                                 payload=msg.payload))


            if msg.topic.endswith(".json"):
                payload = json.loads(msg.payload)
            else:
                payload = msg.payload

            if msg.topic == "mqtt_jobber/dispatch.json":
                # New job offer
                if self._do_i_meet_job_criteria(payload["worker_criteria"]):
                    # send a message to the job offer topic that this worker is available
                    self.jobber_publish(jobber_topic_offers_path.format(offer_id=payload['offer_id']), repr(self))
            elif msg.topic == "mqtt_jobber/workers/" + self._mqtt_client_my_id + "/contracts.json":
                # New job contract, join the job topic and send a hello
                self.jobber_subscribe(jobber_topic_workers_path.format(job_number=payload["job_number"]))
                self.send_heartbeat_for_job(payload["job_number"], 0)

        except Exception as e:
            print(e)

    def _do_i_meet_job_criteria(self, criteria):
        return True

    def send_heartbeat_for_job(self, job_number, work_sequence):
        self.jobber_publish(jobber_topic_dispatcher_path.format(job_number=job_number),
                            json.dumps({"client_id": self._mqtt_client_my_id,
                                        "sent_timestamp": None,
                                        "work_seq": work_sequence}))

    def __repr__(self):
        me = {"thing_id": self.thing_id, "client_id": self._mqtt_client_my_id}
        return json.dumps(me)
