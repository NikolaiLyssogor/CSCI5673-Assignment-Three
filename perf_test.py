import concurrent.futures
import start_servers as startup
from seller_client import SellerClient
from buyer_client import BuyerClient

n_client_pairs = 100
time_between_requests = 0.2

config = startup.getConfig()
sellerAddrs =[ip + ':' + str(port) for ip, port in zip(config.sellerServer.hosts, config.sellerServer.ports)]
buyerAddrs =[ip + ':' + str(port) for ip, port in zip(config.buyerServer.hosts, config.buyerServer.ports)]
sellerClient = SellerClient(sellerAddrs, debug=False)
buyerClient = BuyerClient(buyerAddrs, debug=False)
clients = {
    sellerClient: ["create_account", "login", "get_seller_rating", "sell_item", "list_items", "remove_item", "change_item_price"],
    buyerClient: ["create_account", "login", "search", "add_items_to_cart", "make_purchase", "get_seller_rating_by_id", "get_purchase_history"]
}

data = []

with concurrent.futures.ThreadPoolExecutor() as executor:

    results = [executor.submit(client.testEach, client.__getattribute__(method)) for client, methods in clients.items() for method in methods]

    for f in concurrent.futures.as_completed(results):
        data.append(f.result())

# 'data' is tuple of (average_response_time, experiment_time, n_operations)
response_times = [t[0] for t in data]
experiment_time = max(data, key=lambda x: x[1])[1]
n_operations = max(data, key=lambda x: x[2])[2]

print("============================")
print(f"Average response time: {round(sum(response_times)/len(response_times), 4)} seconds")
print(f"Throughput: {round(n_operations/experiment_time, 4)} operations/seconds")
print("============================")