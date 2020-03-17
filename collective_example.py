import os
import pickle
import codecs

from jobber_mqtt_details import *

import jobber_worker
import jobber_dispatcher


class CountJobber(Job):

    name = "count"
    results_pattern = result_pattern_each

    @staticmethod
    def do_task(worker, job_id, task_parameters):
        i = 0
        while i < task_parameters["limit"]:
            for k in range(10):
                i += 1
            worker.send_heartbeat_for_job(job_id, i)

        results = codecs.encode(pickle.dumps({"total": i}), "base64")
        worker.work_finished_for_job(job_id, 0, results=results.decode(), message="DONE")

    @staticmethod
    def on_results_callback(result, db_session_maker):
        print(result)

    @staticmethod
    def on_worker_finished_callback(result, db_session_maker):
        totals = 0

        session = db_session_maker()
        for r in session.query(ConsignmentResult).filter(ConsignmentResult.consignment == result.consignment).all():
            res_dict = ConsignmentResult.decode_result(r.result, r.result_ecoding)
            totals += res_dict["total"]
        print("totals at {}".format(totals))


logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')

if os.path.exists("/tmp/db.sqlite"):
    os.remove("/tmp/db.sqlite")

dispatcher = jobber_dispatcher.JobberDispatcher('sqlite:////tmp/db.sqlite?check_same_thread=False',
                                                "dispatcher-thing", "localhost")
dispatcher.start()

workers = []
for i in range(3):
    workers.append(jobber_worker.JobberWorker("thing {}".format(i), "localhost"))
    workers[-1].register_work_type("count", CountJobber)
    workers[-1].start()

time.sleep(2)


count_job_id = dispatcher.new_consignment("count", "count to 100", {"limit": 100}, {})
dispatcher.dispatch_consignment_offer(count_job_id, "first offer")

# Loop doing nothing in the main thread. Will break with a TERM signal from a ^C or equivalent
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass

for cworker in workers:
    cworker.stop()

dispatcher.stop()
