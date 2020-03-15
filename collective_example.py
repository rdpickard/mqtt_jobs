from jobber_mqtt_details import *

import jobber_worker
import jobber_dispatcher

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')

dispatcher = jobber_dispatcher.JobberDispatcher("dispatcher-thing", "localhost")
dispatcher.start()

workers = []
for i in range(100):
    workers.append(jobber_worker.JobberWorker("thing {}".format(i), "localhost"))
    workers[-1].start()

time.sleep(2)

dispatcher.dispatch_job_offer(dispatcher.new_job("news jobs"))

# Loop doing nothing in the main thread. Will break with a TERM signal from a ^C or equivalent
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass

for worker in workers:
    worker.stop()

dispatcher.stop()
