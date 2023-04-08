import concurrent.futures
from seller_client import SellerClient
from buyer_client import BuyerClient

n_client_pairs = 100
time_between_requests = 0.2

clients = []
for _ in range(n_client_pairs):
    clients += [SellerClient(debug=False), BuyerClient(debug=False)]
    # clients += [SellerClient(debug=False)]

data = []

with concurrent.futures.ThreadPoolExecutor() as executor:
    results = [executor.submit(client.test, time_between_requests) for client in clients]

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