import os
import pickle
import codecs

from jobber_mqtt_details import *

import jobber_worker
import jobber_dispatcher


@register_job
class CountJobber(Job):

    name = "count"
    results_pattern = result_pattern_each

    @staticmethod
    def do_task(worker, job_id, task_parameters):
        i = 0
        while i < task_parameters["limit"]:
            for k in range(10):
                i += 1
            ConsignmentResult.send_heartbeat(job_id, worker, 0, worker.logger)

        ConsignmentResult.send_result(job_id, worker, *ConsignmentResult.encode_dict_result({"total": i}), 0, worker.logger)
        ConsignmentResult.send_finished(job_id, worker, worker.logger)

    @staticmethod
    def on_results_callback(result, db_session_maker):
        pass

    @staticmethod
    def on_worker_finished_callback(all_results_for_worker):
        pass

    @staticmethod
    def on_consignment_closed_callback(consignment):
        totals = 0
        for result in consignment.results:
            res_dict = ConsignmentResult.decode_result(result.result, result.result_encoding)
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
