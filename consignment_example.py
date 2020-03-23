import consignmentshop
import time
import os

import sqlalchemy
import sqlalchemy.orm


if os.path.exists("/tmp/db.sqlite"):
    os.remove("/tmp/db.sqlite")

shoppe = consignmentshop.ConsignmentShop("localhost", do_debug=True)


class CountTask(consignmentshop.ConsignmentTask):

    @staticmethod
    def on_consignment_open(shop_client, consignment):
        shop_client.logger.info("NEW CONSIGNMENT! {id}".format(id=consignment.id))

    @staticmethod
    def task(shop_client, task_parameters):
        shop_client.logger.info("Got task!")
        yield 100, task_parameters['limit']


clients = []
keeper = None

try:
    keeper = shoppe.consignment_keeper_factory("keeper", {"count": CountTask},
                                               'sqlite:////tmp/db.sqlite?check_same_thread=False'
                                               )

    for i in range(1):
        clients.append(shoppe.consignment_worker_factory(consignmentshop.gen_hex_id("worker-", 22),
                       {"count": CountTask}))

    time.sleep(2)

    #count_to_100_consignment = consignmentshop.Consignment.new_consignment(keeper, "count to 100", "count", {"limit": 100}, {})
    #offer = count_to_100_consignment.make_new_offer(keeper)

    # TODO Need to test sending messages to defunct or closed consignments

    time.sleep(3)
except Exception as e:
    print(e)

for client in clients:
    client.stop()

if keeper is not None:
    keeper.stop()
