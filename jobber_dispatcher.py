# I'm not sure what I'll end up calling this
# To start the dispatcher and the worker code is going to be in the same file
# It'll probably make sense to break at least the job scheduling logic out at some point soon

# This is going to assume the version of MQTT is lower than 5 with no native support for response topics in messages
import datetime
import binascii

from jobber_mqtt_details import *


class JobberDispatcher(JobberMQTTThreadedClient):

    _db_engine = None
    _db_session_maker = None
    _registered_job_types = dict()

    def __init__(self,
                 db_connection_uri,
                 thing_id,
                 mqtt_broker_host, mqtt_broker_port=1883, keep_alive=60,
                 client_id=None,
                 do_connect=True,
                 logger=logging.getLogger("mqtt_jobber.JobberWorker")):
        super().__init__(thing_id, mqtt_broker_host, mqtt_broker_port, keep_alive, client_id, do_connect, logger)
        self._db_engine = sqlalchemy.create_engine(db_connection_uri, echo=False)
        Base.metadata.create_all(self._db_engine)
        self._db_session_maker = sqlalchemy.orm.sessionmaker(bind=self._db_engine)

    @mqtt_threaded_client_exception_catcher
    def on_connect(self, client, userdata, flags, rc):
        JobberMQTTThreadedClient.on_connect(self, client, userdata, flags, rc)

    @mqtt_threaded_client_exception_catcher
    def on_message(self, client, userdata, msg):
        self._logger.info(
            "\\RCV\\ {client_id}@{topic} [msg id={msg_id}]\"{payload}\"".format(client_id=self._mqtt_client_my_id,
                                                                                msg_id=msg.mid,
                                                                                topic=msg.topic,
                                                                                payload=msg.payload))

        if msg.topic.endswith(".json"):
            payload = json.loads(msg.payload)
        else:
            payload = msg.payload

        db_session = self._db_session_maker()

        if re.match("mqtt_jobber/offers/([a-zA-Z0-9]*)\.json", msg.topic):
            # response to an offer from a client

            offer_id = re.match("mqtt_jobber/offers/([a-zA-Z0-9]*)\.json", msg.topic).groups()[0]
            offer = db_session.query(JobOffer).filter_by(id=offer_id).one_or_none()
            if offer is None:
                self._logger.error("Could not find offer with id \"{offer_id} in DB\"".format(offer_id=offer_id))
                return
            job = db_session.query(Job).filter_by(id=offer.job).one_or_none()
            if job is None:
                self._logger.error("Could not find job with id \"{job_id} in DB\"".format(job_id=offer.job))
                return

            self._logger.info("\\   \\ Worker {} wants to join job {} from offer {}".format(
                payload["client_id"], offer.job, offer.id))

            # TODO Check to see if there are more workers needed
            self.jobber_publish("mqtt_jobber/workers/" + payload["client_id"] + "/contracts.json",
                                json.dumps({"job_number": offer.job,
                                            "task_name": job.task_name,
                                            "task_parameters": job.task_parameters}))

        elif re.match("mqtt_jobber/job/([a-zA-Z0-9]*)/dispatcher.json", msg.topic):
            # Information from a worker about a job
            job_id = re.match("mqtt_jobber/job/([a-zA-Z0-9]*)/dispatcher.json", msg.topic).groups()[0]
            job = db_session.query(Job).filter_by(id=job_id).one_or_none()
            if job is None:
                self._logger.warn("Can not create offer for job with id \"{id}\", no such id in DB".format(id=job_id))
                return

            if payload["results"] is not None:
                result = JobResult(id="r" + mqtt.base62(uuid.uuid4().int, padding=22),
                                   job=job.id,
                                   worker=payload["client_id"],
                                   result=payload["results"])
                db_session.add(result)
                db_session.commit()

                if payload["work_seq"] == -1:
                    self._registered_job_types[job.job_type_name]["task"].on_worker_finished_callback(result, self._db_session_maker)

            if payload["job_state"] == 1:
                self._logger.info("\\   \\ {client_id}@{topic} [msg id={msg_id}] 💖 {worker_id}".format(
                    client_id=self._mqtt_client_my_id,
                    msg_id=msg.mid,
                    topic=msg.topic,
                    worker_id=payload["client_id"]))

            # TODO update the job data
            # TODO refresh the worker's status in my job record to indicate that it's sent proof of life
        else:
            self._logger.info("\\   \\ {client_id}@{topic} [msg id={msg_id}] Unprocessed".format(
                client_id=self._mqtt_client_my_id,
                msg_id=msg.mid,
                topic=msg.topic))

    @mqtt_threaded_client_exception_catcher
    def new_consignment(self, job_name, description, job_parameters, worker_requirements,
                        results_pattern=None, worker_pattern=None, job_pattern=None):

        db_session = self._db_session_maker()

        consignment = Consignment(
            id="c" + mqtt.base62(uuid.uuid4().int, padding=22),
            description=description,
            job=job_name,
            results_pattern=results_pattern,
            worker_pattern=worker_pattern,
            job_pattern=job_pattern,
            work_parameters=job_parameters,
            worker_requirements=worker_requirements
        )
        db_session.add(consignment)
        db_session.commit()

        return consignment.id

    @mqtt_threaded_client_exception_catcher
    def dispatch_consignment_offer(self, consignment_id, offer_description):

        db_session = self._db_session_maker()

        consignment = db_session.query(Consignment).filter_by(id=consignment_id).one_or_none()
        if consignment is None:
            self._logger.error("Could not find consignment with id \"{id} in DB\"".format(id=consignment_id))
            return None
        elif consignment.finished_timestamp_utc is not None:
            self._logger.error("Tried to dispatch offer for consignment \"{id} which is already finished\"".format(id=consignment_id))
            return None

        offer = ConsignmentOffer(id="o" + mqtt.base62(uuid.uuid4().int, padding=22), consignment=consignment_id)
        db_session.add(offer)
        db_session.commit()

        # Create / subscribe to topic for workers to offer services for new job
        # QUESTION is it better to subscribe to a wildcard topic
        self.jobber_subscribe(jobber_topic_dispatcher_path.format(offer_id=offer.id))
        self.jobber_subscribe(jobber_topic_offers_path.format(offer_id=offer.id))

        job_offer = {
            "description": offer_description,
            "offer_id": offer.id,
            "job_name": consignment.job,
            "worker_criteria": consignment.worker_requirements
        }

        self.jobber_publish("mqtt_jobber/dispatch.json", json.dumps(job_offer))

        return offer.id
