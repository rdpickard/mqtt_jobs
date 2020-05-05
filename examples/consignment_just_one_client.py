import mqtt_jobs.consignmentshop as consignmentshop
import signal
import os
import time
import logging
import logging.handlers

mqtt_broker_host = os.environ.get('mqtt_broker_host',"localhost")
mqtt_broker_port = os.environ.get('mqtt_broker_port', 1883)

shoppe = consignmentshop.ConsignmentShop("localhost", do_debug=True)

class CountTask(consignmentshop.ConsignmentTask):

    @staticmethod
    def on_consignment_open(shop_client, consignment):
        shop_client.logger.info("NEW CONSIGNMENT! {id}".format(id=consignment.id))

    @staticmethod
    def task(shop_client, task_parameters):
        shop_client.logger.info("Got task!")
        for i in range(50):
            yield i, task_parameters['limit']


class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.kill_now = True


if __name__ == '__main__':
    killer = GracefulKiller()

    remote_syslog_handler = logging.handlers.SysLogHandler(address=('127.0.0.1', 514))

    client = shoppe.consignment_worker_factory(consignmentshop.gen_hex_id("worker-", 22),
                                               {"count": CountTask})

    client.add_remote_syslog_handler("127.0.0.1")

    while not killer.kill_now:
        time.sleep(1)

    client.stop()
