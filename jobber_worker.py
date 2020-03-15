from jobber_mqtt_details import *


class JobberWorker(JobberMQTTThreadedClient):
    _mqtt_client = None
    _mqtt_client_my_id = None
    _logger = None

    thing_id = None

    class JobberWorkerTaskRunner(threading.Thread):

        _worker = None
        _task = None
        _task_parameters = None
        _job_number = None

        def __init__(self, worker, job_number, task, task_parameters):

            threading.Thread.__init__(self)
            self._worker = worker
            self._task = task
            self._task_parameters = task_parameters
            self._job_number = job_number

        def run(self):
            if self._task == "count":
                i = 0
                while i < self._task_parameters["limit"]:
                    for k in range(10):
                        i += 1
                    self._worker.send_heartbeat_for_job(self._job_number, i)

                self._worker.work_finished_for_job(self._job_number, 0,
                                                   results=base64.b64encode(str(i).encode()), message="DONES")
            else:
                self._worker.work_finished_for_job(self._job_number, -1, results=None,
                                                   message="Don't know how to do task \"{task}\"".format(task=self._task))

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
                # QUESTION is this correct? Is there any reason for the worker to get messages here
                self.jobber_subscribe(jobber_topic_workers_path.format(job_number=payload["job_number"]))
                self.send_heartbeat_for_job(payload["job_number"], 0)
                workertask = self.JobberWorkerTaskRunner(worker=self, job_number=payload["job_number"],
                                                         task="count", task_parameters={"limit": 100})
                workertask.start()

        except Exception as e:
            # TODO remove the following print with a correct log message
            print(e)

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

    def work_finished_for_job(self, job_number, finished_state, results= None, message=None):
        # TODO unsubscribe from the job topic
        # TODO Make sure the task thread is cleaned up ok
        self.jobber_publish(jobber_topic_dispatcher_path.format(job_number=job_number),
                            json.dumps({"client_id": self._mqtt_client_my_id,
                                        "sent_timestamp": None,
                                        "job_state": finished_state,
                                        "results": results,
                                        "message": message,
                                        "work_seq": -1}))

    def __repr__(self):
        me = {"thing_id": self.thing_id, "client_id": self._mqtt_client_my_id}
        return json.dumps(me)

