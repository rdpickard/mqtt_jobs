import consignmentshop
import time
import os
import traceback
import signal

if os.path.exists("/tmp/db.sqlite"):
    os.remove("/tmp/db.sqlite")

mqtt_broker_host = os.environ.get('mqtt_broker_host', "localhost")
mqtt_broker_port = os.environ.get('mqtt_broker_port', 1883)

shoppe = consignmentshop.ConsignmentShop(mqtt_broker_host, mqtt_broker_port, do_debug=True)


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

    keeper = shoppe.consignment_keeper_factory("keeper", {"count": CountTask},
                                               'sqlite:////tmp/db.sqlite?check_same_thread=False',
                                               db_echo=False)
    keeper.add_remote_syslog_handler("127.0.0.1")

    count_to_100_consignment = consignmentshop.Consignment.new_consignment(keeper,
                                                                           "count to 100",
                                                                           "count", {"limit": 100}, {})
    consignmentshop.Consignment.is_healthy_when(count_to_100_consignment.id,
                                                keeper,
                                                "@ACTIVE_WORKERS(10) >= 2 or @RECENTLY_OPENED_CONTRACTS >= 2")

    offer = count_to_100_consignment.make_new_offer(keeper)

    while not killer.kill_now:
        time.sleep(1)

    keeper.stop()
