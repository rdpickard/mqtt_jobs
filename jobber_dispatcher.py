# I'm not sure what I'll end up calling this
# To start the dispatcher and the worker code is going to be in the same file
# It'll probably make sense to break at least the job scheduling logic out at some point soon

# This is going to assume the version of MQTT is lower than 5 with no native support for response topics in messages

from jobber_mqtt_details import *


class JobberDispatcher(JobberMQTTThreadedClient):
    # TODO Array of job dictionaries needs to be replaced by something like a DB for persistence
    jobs = {}

    def on_connect(self, client, userdata, flags, rc):
        JobberMQTTThreadedClient.on_connect(self, client, userdata, flags, rc)

        # Join the jobber dispatcher topic
        # QUESTION is there a reason for Dispatcher to get messages on the dispatch topic?
        #self._mqtt_client.subscribe("mqtt_jobber/dispatch.json")

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        if msg.topic.endswith(".json"):
            payload = json.loads(msg.payload)
        else:
            payload = msg.payload

        if re.match("mqtt_jobber/offers/o([a-zA-Z0-9]*)\.json", msg.topic):
            # response to an offer from a client
            # TODO more efficient way to do these regexes without doing each match twice
            offer = "o"+re.match("mqtt_jobber/offers/o([a-zA-Z0-9]*)\.json", msg.topic).groups()[0]

            # TODO fix this search for job by offer id, it's inefficient and dumb
            job = None
            for job in self.jobs.values():
                if offer == job["offer_id"]:
                    break
            if job is None or job["offer_id"] != offer:
                self._logger.error("Could not find job with offer_is {}"+offer)
                return

            self._logger.info("Worker {} wants to join job {} from offer {}".format(payload["client_id"], job["job_number"], offer))
            # TODO Check to see if there are more workers needed
            self._mqtt_client.publish("mqtt_jobber/workers/"+payload["client_id"]+"/contracts.json",
                                      json.dumps({"job_number": job["job_number"]}))

        else:
            self._logger.info(self._logger.info(self._mqtt_client_my_id+"@"+msg.topic+" unprocessed "+msg)
)

    def dispatch_job_offer(self, job):

        # Create / subscribe to topic for workers to offer services for new job
        # QUESTION is it better to subscribe to a wildcard topic
        self._mqtt_client.subscribe(jobber_topic_dispatcher_path.format(job_number=job["job_number"]))
        self._mqtt_client.subscribe(jobber_topic_offers_path.format(offer_id=job["offer_id"]))
        self.jobs[job['job_number']] = job

        job_offer = {
            "description": job["description"],
            "offer_id": job["offer_id"],
            "worker_criteria": job["worker_criteria"]
        }

        self._mqtt_client.publish("mqtt_jobber/dispatch.json", json.dumps(job_offer))

    def new_job(self, description="", max_workers=-1, min_workers=-1, pattern=None):
        # TODO Job needs to be persisted to some kind of DB
        # TODO Look at SymPy for implementing worker criteria as modal logic expression
        #  https://docs.sympy.org/latest/index.html
        job = {
            "description": description,
            "offer_id": "o"+mqtt.base62(uuid.uuid4().int, padding=22),
            "job_number": "j"+mqtt.base62(uuid.uuid4().int, padding=22),
            "max_workers": max_workers,
            "min_workers": min_workers,
            "worker_criteria": None,
            "pattern": pattern,
        }
        return job

