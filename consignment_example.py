import consignmentshop
import time
import os

import sqlalchemy
import sqlalchemy.orm


if os.path.exists("/tmp/db.sqlite"):
    os.remove("/tmp/db.sqlite")

shoppe = consignmentshop.ConsignmentShop()


class CountTask(consignmentshop.ConsignmentTask):

    @staticmethod
    def do_task(shop_client, task_parameters):
        shop_client.logger("Got task!")


shoppe.task("count", CountTask)


keeper = shoppe.consignment_keeper_factory([], 'sqlite:////tmp/db.sqlite?check_same_thread=False', "keeper", "localhost")

clients = []
for i in range(1):
    clients.append(shoppe.consignment_worker_factory([], consignmentshop.gen_hex_id("worker-", 22), "localhost"))

time.sleep(2)

count_to_100_consignment = consignmentshop.Consignment.new_consignment(keeper, "count to 100", "count", {"limit": 100}, {})
offer = count_to_100_consignment.make_new_offer(keeper)

# TODO Need to create a task class for passing back control to when behaviors are triggered
# TODO Need to test sending messages to defunct or closed consignments

time.sleep(3)

for client in clients:
    client.stop()

keeper.stop()
