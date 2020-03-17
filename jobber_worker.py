from jobber_mqtt_details import *


class JobberWorker(JobberMQTTThreadedClient):
    _mqtt_client = None
    _mqtt_client_my_id = None
    _logger = None

    thing_id = None

    _registered_work_types = dict()

    @property
    def client_id(self):
        return self._mqtt_client_my_id

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
                if payload["job_name"] not in self._registered_work_types.keys():
                    self._logger.info("Don't know how to do work of type task \"{job_name}\"".format(task_name=payload["job_name"]))
                    return
                elif not self._do_i_meet_job_criteria(payload["worker_criteria"]):
                    return

                # send a message to the job offer topic that this worker is available
                ConsignmentOffer.client_send_accept_offer(payload['offer_id'], self)

            elif msg.topic == "mqtt_jobber/workers/" + self._mqtt_client_my_id + "/contracts.json":
                # New job contract, join the job topic and send a hello
                # QUESTION is this correct? Is there any reason for the worker to get messages here
                self.jobber_subscribe(jobber_topic_workers_path.format(job_number=payload["job_number"]))
                self.send_heartbeat_for_job(payload["job_number"], 0)

                jobb = self._registered_work_types[payload["task_name"]]["task"]
                thread = threading.Thread(target = jobb.do_task, args = (self,
                                                                      payload["job_number"],
                                                                      payload["task_parameters"]))
                thread.start()

        except Exception as e:
            # TODO remove the following print with a correct log message
            self._logger.error(e)

    def _do_i_meet_job_criteria(self, criteria):
        return True

    def send_heartbeat_for_job(self, job_number, work_sequence):
        self.jobber_publish(jobber_topic_dispatcher_path.format(job_number=job_number),
                            json.dumps({"client_id": self._mqtt_client_my_id,
                                        "sent_timestamp": None,
                                        "job_state": 1,
                                        "results": None,
                                        "message": None,
                                        "work_seq": work_sequence}))

    def work_finished_for_job(self, job_number, finished_state, results=None, message=None):
        # TODO unsubscribe from the job topic
        # TODO Make sure the task thread is cleaned up ok
        self.jobber_publish(jobber_topic_dispatcher_path.format(job_number=job_number),
                            json.dumps({"client_id": self._mqtt_client_my_id,
                                        "sent_timestamp": None,
                                        "job_state": finished_state,
                                        "results": results,
                                        "message": message,
                                        "work_seq": -1}))

    def register_work_type(self, job_name, task):
        self._registered_work_types[job_name] = {
            "task": task
        }

    def __repr__(self):
        me = {"thing_id": self.thing_id, "client_id": self._mqtt_client_my_id}
        return json.dumps(me)

