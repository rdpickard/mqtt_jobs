import mqtt_jobs.consignmentshop as consignmentshop
import time
import os
import traceback


if os.path.exists("/tmp/db.sqlite"):
    os.remove("/tmp/db.sqlite")

shoppe = consignmentshop.ConsignmentShop("localhost", do_debug=False)


class PingTask(consignmentshop.ConsignmentTask):
    @staticmethod
    def task(shop_client, task_parameters):
        shop_client.logger.info("Got task!")
        for i in range(50):
            yield i, task_parameters['limit']


class CountTask(consignmentshop.ConsignmentTask):

    @staticmethod
    def on_consignment_open(shop_client, consignment):
        shop_client.logger.info("NEW CONSIGNMENT! {id}".format(id=consignment.id))

    @staticmethod
    def task(shop_client, task_parameters):
        shop_client.logger.info("Got task!")
        for i in range(50):
            yield i, task_parameters['limit']


clients = []
keeper = None

try:
    keeper = shoppe.consignment_keeper_factory("keeper", {"count": CountTask},
                                               'sqlite:////tmp/db.sqlite?check_same_thread=False',
                                               db_echo=False)

    for i in range(4):
        clients.append(shoppe.consignment_worker_factory(consignmentshop.gen_hex_id("worker-", 22),
                       {"count": CountTask}))

    time.sleep(2)

    count_to_100_consignment = consignmentshop.Consignment.new_consignment(keeper,
                                                                           "count to 100",
                                                                           "count", {"limit": 100}, {})
    consignmentshop.Consignment.is_healthy_when(count_to_100_consignment.id,
                                                keeper,
                                                "@ACTIVE_WORKERS(10) >= 2 or @RECENTLY_OPENED_CONTRACTS >= 2")

    offer = count_to_100_consignment.make_new_offer(keeper)

    time.sleep(10)

    consignmentshop.eval_consignment_characteristic("@ACTIVE_WORKERS(10) >= 2", count_to_100_consignment.id, keeper.db_session_maker, keeper.logger)


except Exception as e:
    track = traceback.format_exc()
    print(track)

for client in clients:
    client.stop()


if keeper is not None:
    keeper.stop()
