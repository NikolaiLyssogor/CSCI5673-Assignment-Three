from types import SimpleNamespace

import os
import yaml
import json
import threading

import buyer_server
import seller_server
import customer_db_group
import product_db
import transactions_db


def getConfig():
    fp = os.getcwd()+"/config.yml"
    with open(fp, "r") as file:
        config = json.dumps(yaml.safe_load(file))
    file.close()
    return json.loads(config, object_hook=lambda d: SimpleNamespace(**d))


def startBuyerServer(config):
    buyer_server.serve(config)


def startSellerServer(config):
    seller_server.serve(config)


def startCustomerDB(config):
    customer_db_group.startServers(config.customerDB)


def startProductDB(config):
    product_db.serve(config.productDB)


def startTransactionsDB(config):
    transactions_db.serve(config.transactionsDB)


def main():
    threads = []
    try:
        config = getConfig()
        t1 = threading.Thread(target=startTransactionsDB, name="TransactionsDBServicer", args=[config])
        t1.start()
        threads.append(t1)

        t2 = threading.Thread(target=startCustomerDB, name="CustomerDBServicer", args=[config])
        t2.start()
        threads.append(t2)

        t3 = threading.Thread(target=startProductDB, name="ProductDBServicer", args=[config])
        t3.start()
        threads.append(t3)

        t4 = threading.Thread(target=startBuyerServer, name="BuyerServer", args=[config])
        t4.start()
        threads.append(t4)

        t5 = threading.Thread(target=startSellerServer, name="SellerServer", args=[config])
        t5.start()
        threads.append(t5)
    except:
        for thread in threads:
            thread.join()


if __name__ == '__main__':
    main()
