from jobber_mqtt_details import *
import jobber_worker

import jobber_dispatcher
import time


class CountJob(Consignment):

    @staticmethod
    def do_task(worker, job_id, task_parameters):
        i = 0
        while i < task_parameters["limit"]:
            for k in range(10):
                i += 1
            ConsignmentResult.send_heartbeat(job_id, worker, 0, worker.logger)

        ConsignmentResult.send_result(job_id, worker, *ConsignmentResult.encode_dict_result({"total": i}), 0, worker.logger)
        ConsignmentResult.send_finished(job_id, worker, worker.logger)

    def process_results(self):
        totals = 0
        for result in self.results:
            res_dict = ConsignmentResult.decode_result(result.result, result.result_encoding)
            totals += res_dict["total"]
        print("totals at {}".format(totals))


logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')

if os.path.exists("/tmp/db.sqlite"):
    os.remove("/tmp/db.sqlite")

db_connection_uri = 'sqlite:////tmp/db.sqlite?check_same_thread=False'
db_engine = sqlalchemy.create_engine(db_connection_uri, echo=False)
db_session_maker = sqlalchemy.orm.sessionmaker(bind=db_engine)
db_session = db_session_maker()

dispatcher = jobber_dispatcher.JobberDispatcher(db_connection_uri,
                                                "dispatcher-thing", "localhost")
dispatcher.start()

workers = []
for i in range(3):
    workers.append(jobber_worker.JobberWorker("thing {}".format(i), "localhost"))
    workers[-1].register_work_type("count", CountJob)
    workers[-1].start()

ten_workers = BehaviorAfterNFinishedWorkers(count=10)
five_seconds = BehaviorAfterNSecondsActivity(seconds=10)

count_to_100 = CountJob()
count_to_100.name = "count"
db_session.add(count_to_100)
db_session.commit()
count_to_100.mqtt_client = dispatcher
count_to_100.begin()

time.sleep(2)

#consignment_id = dispatcher.new_consignment("count", "testing behavior pattern", {}, {}, None, None, str(five_seconds))
#consignment = db_session.query(Consignment).filter_by(id=consignment_id).one_or_none()

#time.sleep(5)

#print(five_seconds.test(consignment.job_pattern, consignment, db_session))
