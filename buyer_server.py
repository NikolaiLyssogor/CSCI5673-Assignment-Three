import start_servers as startup

import grpc
import database_pb2
import database_pb2_grpc

import pickle
import json
import time
import threading
from flask import Flask, request, Response
from zeep import Client

import random


# Define Flask service
app = Flask(__name__)
# Used for tracking throughput
n_ops = 0

customer_stubs = []
product_stubs = []
soap_client: Client


def setConfig(config):
    # Stub for communicating with customer database
    global customer_stubs
    # Stub for communicating with product database
    global product_stub
    # Stub for communicating with the SOAP transactions database
    global soap_client

    for server_idx in range(len(config.customerDB.ports)):
        # Add customer db stubs
        host = config.customerDB.hosts[server_idx]
        port = config.customerDB.ports[server_idx]
        customer_channel = grpc.insecure_channel("{}:{}".format(host, port))
        customer_stub = database_pb2_grpc.databaseStub(customer_channel)
        customer_stubs.append(customer_stub)

        # Add product db stubs
        host = config.productDB.hosts[server_idx]
        port = config.productDB.ports[server_idx]
        product_channel = grpc.insecure_channel("{}:{}".format(host, port))
        product_stub = database_pb2_grpc.databaseStub(product_channel)
        product_stubs.append(product_stub)

    # product_channel = grpc.insecure_channel("{}:{}".format(config.productDB.host, config.productDB.port))
    # product_stub = database_pb2_grpc.databaseStub(product_channel)
    soap_client = Client('http://{}:{}/?wsdl'.format(config.transactionsDB.addr, config.transactionsDB.port))


@app.route('/createAccount', methods=['POST'])
def createAccount():
    global n_ops
    n_ops += 1

    data = json.loads(request.data)
    unm, pwd = data['username'], data['password']

    # Check if username is already taken
    try:
        sql = f"SELECT * FROM buyers WHERE username = '{unm}'"
        db_response = query_database(sql, 'customer')
    except:
        response = json.dumps({'status': 'Error: Failed to connect to database'})
        return Response(response=response, status=500)
    else:
        if isinstance(db_response, dict):
            return Response(response=db_response, status=500)
        if db_response:
            # Username taken
            response = json.dumps({'status': 'Error: Username already taken'})
            return Response(response=response, status=400)

    # Create new account
    try:
        sql = f"""
            INSERT INTO buyers ('username', 'password') VALUES
            ('{unm}', '{pwd}')
        """
        db_response = query_database(sql, 'customer')
    except:
        response = json.dumps({'status': 'Error: Failed to connect to database'})
        return Response(response=response, status=500)
    else:
        # Check for database error
        if 'Error' in db_response['status']:
            response = json.dumps(db_response)
        else:
            response = json.dumps({'status': 'Success: Account created successfully'})

        return Response(response=response, status=200)
    

@app.route('/login', methods=['POST'])
def login():
    global n_ops
    n_ops += 1
    data = json.loads(request.data)
    unm, pwd = data['username'], data['password']

    try:
        sql = f"SELECT * FROM buyers WHERE username = '{unm}'"
        db_response = query_database(sql, 'customer')
    except:
        response = json.dumps({'status': 'Error: Failed to connect to database'})
        return Response(response=response, status=500)
    else:
        # Check for database error
        if isinstance(db_response, dict):
            response = json.dumps(db_response)
        elif not db_response:
            # Username not found
            response = json.dumps({'status': 'Error: Username not found'})
        else:
            # Check if username is in database
            response = json.dumps({'status': 'Error: Incorrect password'})
            for user in db_response:
                if user[1] == pwd:
                    # Found user, need to update DB that they are logged in
                    response = json.dumps({'status': 'Success: Login successful'})
                    sql = f"""
                        UPDATE buyers SET is_logged_in = 'true'
                        WHERE username = '{unm}' AND password = '{pwd}'
                    """
                    try:
                        db_response2 = query_database(sql, 'customer')
                    except:
                        response = json.dumps({'status': 'Error: Failed to connect to database'})
                        return Response(response=response, status=500)
                    else:
                        if 'Error' in db_response2['status']:
                            return Response(response=db_response2, status=500)
                        else:
                            break

        return Response(response=response, status=200)

@app.route('/logout', methods=['POST'])
def logout():
    global n_ops
    n_ops += 1
    data = json.loads(request.data)
    unm = data['username']
    sql = f"""
        UPDATE buyers SET is_logged_in = 'false'
        WHERE username = '{unm}'
    """

    try:
        db_response = query_database(sql, 'customer')
    except:
        response = json.dumps({'status': 'Error: Failed to connect to database'})
        return Response(response=response, status=500)
    else:
        if 'Error' in db_response['status']:
            return Response(response=db_response, status=500)
        else:
            response = json.dumps({'status': 'Success: You have logged out'})
            return Response(response=response, status=200)

# def check_if_logged_in():
#     data = json.loads(request.data)
#     unm = data['username']

#     try:
#         # Check the DB if the user is logged in
#         sql = f"SELECT is_logged_in FROM buyers WHERE username = '{unm}'"
#         db_response = query_database(sql, 'customer')
#     except:
#         # Return database connection error
#         response = json.dumps({'status': 'Error: Failed to connect to database'})
#         return Response(response=response, status=500)
#     else:
#         # DB server couldn't connect to DB
#         if isinstance(db_response, dict):
#             return Response(response=db_response, status=500)
        
#         if not db_response:
#             # No such account so not logged in
#             response = json.dumps({'is_logged_in': False})
#         else:
#             # Account exists: Check if logged in or not
#             is_logged_in = True if db_response[0][0] == 'true' else False
#             response = json.dumps({'is_logged_in': is_logged_in})

#         return Response(response=response, status=200)

def check_if_logged_in(username: str) -> dict:
    try:
        # Check the DB if the user is logged in
        sql = f"SELECT is_logged_in FROM buyers WHERE username = '{username}'"
        db_response = query_database(sql, 'customer')
    except:
        # Return database connection error
        return {'status': 'Error: Failed to connect to database'}
    else:
        # DB server couldn't connect to DB
        if isinstance(db_response, dict):
            return db_response
            
        if not db_response:
            # No such account so not logged in
            return {'is_logged_in': False}
        else:
            # Account exists: Check if logged in or not
            is_logged_in = True if db_response[0][0] == 'true' else False
            return {'is_logged_in': is_logged_in}

@app.route('/search', methods=['POST'])
def search():
    global n_ops
    n_ops += 1
    data = json.loads(request.data)
    sql = f"SELECT rowid, * FROM products WHERE category = {data['category']}"
    for keyword in data['keywords']:
        sql += f" OR keywords LIKE '%{keyword}%'"

    try:
        db_response = query_database(sql, 'product')
    except:
        response = json.dumps({'status': 'Error: Failed to connect to database'})
        return Response(response=response, status=500)
    else:
        if isinstance(db_response, dict):
            return Response(response=db_response, status=500)

    # Return result as json
    items = products_query_to_json(db_response)
    response = json.dumps({
        'status': 'Success: Items queried successfully',
        'items': items
    })
    return Response(response=response, status=200)

@app.route('/addItemsToCart', methods=['POST'])
def add_items_to_cart():
    global n_ops
    n_ops += 1
    data = json.loads(request.data)

    # Check if logged in before proceeding
    login_status = check_if_logged_in(data['username'])
    if 'status' in login_status.keys():
        return Response(response=login_status, status=500)
    elif not login_status['is_logged_in']:
        response = json.dumps({'status': 'Error: You must be logged in to add items to your cart.'})
        return Response(response=response, status=401)

    sql = f"SELECT rowid, * FROM products WHERE rowid = {data['item_id']}"

    try:
        db_response = query_database(sql, 'product')
    except:
        response = json.dumps({'status': 'Error: Failed to connect to database'})
        return Response(response=response, status=500)
    else:
        if isinstance(db_response, dict):
            return Response(response=db_response, status=500)

    # Check if that item isn't in the DB
    if not db_response:
        response = json.dumps({'status': 'Error: This item is not in inventory'})
        return Response(response=response, status=400)
    
    items = products_query_to_json(db_response)[0]

    # Check if the user is requesting more than is available
    if (data['quantity']) > items['quantity']:
        response = json.dumps({'status': 'Error: You requested more items than are in our inventory'})
        return Response(response=response, status=400)
    else:
        items['quantity'] = data['quantity']

    # Good request, return items
    response = json.dumps({
        'status': 'Success: Items queried successfully',
        'items': items
    })
    return Response(response=response, status=200)

@app.route('/getSellerRatingByID/<string:seller_id>', methods=['GET'])
def get_seller_rating_by_id(seller_id):
    global n_ops
    n_ops += 1
    sql = f"SELECT rowid, * from sellers WHERE rowid = {seller_id}"
    try:
        db_response = query_database(sql, 'customer')
    except:
        response = json.dumps({'status': 'Error: Failed to connect to database'})
        return Response(response=response, status=500)
    else:
        if isinstance(db_response, dict):
            return Response(response=db_response, status=500)

    if not db_response:
        response = json.dumps({'status': 'Error: The user you are trying to find does not exist'})
        return Response(response=response, status=400)
    
    thumbs_up, thumbs_down = db_response[0][3], db_response[0][4]
    response = json.dumps({
        'status': 'Success: Seller queried without error',
        'thumbs_up': thumbs_up,
        'thumbs_down': thumbs_down
    })
    return Response(response=response, status=200)

@app.route('/makePurchase', methods=['POST'])
def make_purchase():
    global n_ops
    n_ops += 1
    # Return "payment error" with 0.1 probability
    if random.random() < 0.1:
        response = json.dumps({'status': 'Error: payment failed. Try again.'})
        return Response(response=response, status=500)

    # Check that the items exist/have enough in stock
    data = json.loads(request.data)

    # Check if logged in before proceeding
    login_status = check_if_logged_in(data['username'])
    if 'status' in login_status.keys():
        return Response(response=login_status, status=500)
    elif not login_status['is_logged_in']:
        response = json.dumps({'status': 'Error: You must be logged in to add items to your cart.'})
        return Response(response=response, status=401)
    
    items_info = []
    for item_id, req_qty in data['items']:
        sql = f"SELECT seller, quantity FROM products WHERE rowid = {item_id}"
        db_response = query_database(sql, 'product')

        if not db_response:
            response = json.dumps({'status': 'Error: One of your items does not exist or is no longer available'})
            return Response(response=response, status=400)

        seller, act_qty = db_response[0][0], db_response[0][1]
        if act_qty < req_qty:
            response = json.dumps({'status': 'Error: One of your items is no longer available in the quantity you requested'})
            return Response(response=response, status=400)

        items_info.append((item_id, act_qty-req_qty, seller))

    for item_id, new_qty, seller in items_info:
        # Make the transaction
        sql = f"""
        INSERT INTO transactions ('cc_name', 'cc_number', 'cc_exp', 'item_id', 'quantity', 'buyer_name', 'seller_name') VALUES
        ('{data['cc_name']}', '{data['cc_number']}', '{data['cc_exp']}', {item_id}, {new_qty}, '{data['username']}', '{seller}')
        """
        db_response = query_database(sql, 'transaction') # Check 'Error' or 'Status' if needed

        # Lower the quantity available/delete the item
        if new_qty == 0:
            sql = f"DELETE FROM products WHERE rowid = {item_id}"
        else:
            sql = f"UPDATE products SET quantity = {new_qty} WHERE seller = '{seller}'"
        db_response = query_database(sql, 'product')

        # Increment the seller's items sold
        sql = f"UPDATE sellers SET items_sold = items_sold + 1 WHERE rowid = {item_id}"
        db_response = query_database(sql, 'customer')

    # Increment the buyer's items bought
    sql = f"""
    UPDATE buyers
    SET items_purchased = items_purchased + {len(items_info)}
    WHERE username = '{data['username']}'
    """
    db_response = query_database(sql, 'customer')

    response = json.dumps({'status': 'Success: Transaction completed without errors'})
    return Response(response=response, status=200)

@app.route('/getPurchaseHistory/<string:unm>', methods=['GET'])
def get_purchase_history(unm):
    global n_ops
    n_ops += 1

    # Check if logged in before proceeding
    login_status = check_if_logged_in(unm)
    if 'status' in login_status.keys():
        return Response(response=login_status, status=500)
    elif not login_status['is_logged_in']:
        response = json.dumps({'status': 'Error: You must be logged in to view your purchase history.'})
        return Response(response=response, status=401)

    sql = f"SELECT items_purchased FROM buyers WHERE username = '{unm}'"
    try:
        db_response = query_database(sql, 'customer')
    except:
        response = json.dumps({'status': 'Error: Failed to connect to database'})
        return Response(response=response, status=500)
    else:
        if isinstance(db_response, dict):
            return Response(response=db_response, status=500)

        items_purchased = db_response[0][0]
        response = json.dumps({
            'status': 'Success: Operation executed without errors',
            'items_purchased': items_purchased
        })
        return Response(response=response, status=200)

def products_query_to_json(db_response):
    items = []
    for db_item in db_response:
        item = {
            'id': db_item[0],
            'name': db_item[1],
            'category': db_item[2],
            'keywords': db_item[3].split(','),
            'condition': db_item[4],
            'price': db_item[5],
            'quantity': db_item[6],
            'seller': db_item[7],
            'status': db_item[8],
        }
        items.append(item)

    return items

def query_database(sql: str, db: str):
    """
    Sends a query over gRPC to the database and returns 
    the object that the database returns.
    """
    if db == 'transaction':
        db_response = soap_client.service.query_database(sql)
        return pickle.loads(db_response)
    elif db == 'product':
        query = database_pb2.databaseRequest(query=sql)
        stub = random.choice(product_stubs)
        db_response = stub.queryDatabase(request=query)
        return pickle.loads(db_response.db_response)
    elif db == 'customer':
        query = database_pb2.databaseRequest(query=sql)
        # TODO handle failure
        stub = random.choice(customer_stubs)
        db_response = stub.executeClientRequest(request=query)
        return pickle.loads(db_response.db_response)

    # TODO error
    print("ERROR")
    return None

@app.route('/getServerInfo', methods=['GET'])
def get_server_info():
    global n_ops
    n_ops += 1
    response = json.dumps({
        'time': time.time(),
        'n_ops': n_ops
    })
    return Response(response=response, status=200)

def serve(config=None):
    setConfig(config)

    threads = []
    try:
        for server_idx in range(len(config.buyerServer.ports)):
            port = config.buyerServer.ports[server_idx]
            thread = threading.Thread(target=app.run, name=f"buyer server {server_idx}", args=['0.0.0.0', port])
            thread.start()
            threads.append(thread)
    except Exception as e:
        print(e)
        for thread in threads:
            thread.join()

    # app.run(host=config.sellerServer.host, port=config.sellerServer.port, debug=True, use_reloader=False)


if __name__ == "__main__":
    serve(startup.getConfig())
