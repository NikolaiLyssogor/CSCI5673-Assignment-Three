import grpc
import database_pb2
import database_pb2_grpc

import pickle
import json
import time
from flask import Flask, request, Response

# Define Flask service
app = Flask(__name__)
# Stub for communicating with customer database
customer_channel = grpc.insecure_channel('localhost:50051')
customer_stub = database_pb2_grpc.databaseStub(customer_channel)
# Stub for communicating with product database
product_channel = grpc.insecure_channel('localhost:50052')
product_stub = database_pb2_grpc.databaseStub(product_channel)
# Used for tracking throughput
n_ops = 0

@app.route('/createAccount', methods=['POST'])
def createAccount():
    global n_ops
    n_ops += 1
    data = json.loads(request.data)
    unm, pwd = data['username'], data['password']

    # Check if username is already taken
    try:
        sql = f"SELECT * FROM sellers WHERE username = '{unm}'"
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
            INSERT INTO sellers ('username', 'password') VALUES
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
        sql = f"SELECT * FROM sellers WHERE username = '{unm}'"
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
                        UPDATE sellers SET is_logged_in = 'true'
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
        UPDATE sellers SET is_logged_in = 'false'
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


# @app.route('/checkIfLoggedIn', methods=['POST'])
# def check_if_logged_in():
#     global n_ops
#     n_ops += 1
#     data = json.loads(request.data)
#     unm = data['username']

#     try:
#         # Check the DB if the user is logged in
#         sql = f"SELECT is_logged_in FROM sellers WHERE username = '{unm}'"
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
        sql = f"SELECT is_logged_in FROM sellers WHERE username = '{username}'"
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

@app.route('/getSellerRating/<string:unm>', methods=['GET'])
def get_seller_rating(unm):
    global n_ops
    n_ops += 1

    # Check if logged in before proceeding
    login_status = check_if_logged_in(unm)
    if 'status' in login_status.keys():
        return Response(response=login_status, status=500)
    elif not login_status['is_logged_in']:
        response = json.dumps({'status': 'Error: You must be logged in to check your rating.'})
        return Response(response=response, status=401)

    sql = f"""
        SELECT thumbs_up, thumbs_down FROM sellers
        WHERE username = '{unm}'
    """
    try:
        db_response = query_database(sql, 'customer')
    except:
        response = json.dumps({'status': 'Error: Failed to connect to database'})
        return Response(response=response, status=500)
    else:
        if isinstance(db_response, dict):
            return Response(response=db_response, status=500)
        
        thumbs_up, thumbs_down = db_response[0]
        response = json.dumps({
            'status': 'Success: Operation completed',
            'thumbs_up': thumbs_up,
            'thumbs_down': thumbs_down
        })
        return Response(response=response, status=200)

@app.route('/sellItem', methods=['POST'])
def sell_item():
    global n_ops
    n_ops += 1
    data = json.loads(request.data)

    # Check if logged in before proceeding
    login_status = check_if_logged_in(data['seller'])
    if 'status' in login_status.keys():
        return Response(response=login_status, status=500)
    elif not login_status['is_logged_in']:
        response = json.dumps({'status': 'Error: You must be logged in to sell an item.'})
        return Response(response=response, status=401)

    sql = f"""
        INSERT INTO products
        ('name', 'category', 'keywords', 'condition', 'price', 'quantity', 'seller') VALUES
        ('{data['name']}', '{data['category']}', '{data['keywords']}', '{data['condition']}',
        '{data['price']}', '{data['quantity']}', '{data['seller']}')
    """
    try:
        db_response = query_database(sql, 'product')
    except:
        response = json.dumps({'status': 'Error: Failed to connect to database'})
        return Response(response=response, status=500)
    else:
        # Check for database error
        if 'Error' in db_response['status']:
            response = json.dumps(db_response)
        else:
            response = json.dumps({'status': 'Success: Your item(s) have been listed'})

        return Response(response=response, status=200)

@app.route('/listItems/<string:unm>', methods=['GET'])
def list_items(unm):
    global n_ops
    n_ops += 1

    # Check if logged in before proceeding
    login_status = check_if_logged_in(unm)
    if 'status' in login_status.keys():
        return Response(response=login_status, status=500)
    elif not login_status['is_logged_in']:
        response = json.dumps({'status': 'Error: You must be logged in to view your listed items.'})
        return Response(response=response, status=401)

    sql = f"SELECT ROWID, * FROM products WHERE seller = '{unm}'"

    try:
        db_response = query_database(sql, 'product')
    except:
        response = json.dumps({'status': 'Error: Failed to connect to database'})
        return Response(response=response, status=500)
    else:
        if isinstance(db_response, dict):
            response = json.dumps(db_response)
            return Response(response=response, status=500)
        else:
            # Response successful, make it json
            items = []
            print("")
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
            
            response = json.dumps({
                'status': 'Success: Items queried successfully',
                'items': items
            })
            return Response(response=response, status=200)

@app.route('/deleteItem', methods=['PUT'])
def delete_item():
    global n_ops
    n_ops += 1
    data = json.loads(request.data)

    # Check if logged in before proceeding
    login_status = check_if_logged_in(data['username'])
    if 'status' in login_status.keys():
        return Response(response=login_status, status=500)
    elif not login_status['is_logged_in']:
        response = json.dumps({'status': 'Error: You must be logged in to remove an item.'})
        return Response(response=response, status=401)

    # Do some checks before deleting
    sql = f"""
        SELECT quantity FROM products
        WHERE seller = '{data['username']}'
          AND ROWID = '{data['item_id']}'
    """
    try:
        db_response = query_database(sql, 'product')
    except:
        response = json.dumps({'status': 'Error: Failed to connect to database'})
        return Response(response=response, status=500)
    else:
        if isinstance(db_response, dict):
            response = json.dumps(db_response)
            return Response(response=response, status=500)
        else:
            if not db_response:
                response = json.dumps({'status': 'Error: You are not the one selling this item or it does not exist'})
                return Response(response=response, status=400)
            elif db_response[0][0] < data['quantity']:
                response = json.dumps({'status': 'Error: You have requested to delete more items than are available'})
                return Response(response=response, status=400)     

    # Now actually delete the items
    actual_quantity = db_response[0][0]
    if actual_quantity == data['quantity']:
        sql = f"DELETE FROM products WHERE rowid = {data['item_id']}"
    else:
        new_qty = actual_quantity = data['quantity']
        sql = f"UPDATE products SET quantity = {new_qty} WHERE rowid = {data['item_id']}"

    try:
        db_response = query_database(sql, 'product')
    except:
        response = json.dumps({'status': 'Error: Failed to connect to database'})
        return Response(response=response, status=500)
    else:
        if 'Error' in db_response['status']:
            response = json.dumps(db_response)
            return Response(response=response, status=500)
        else:
            response = json.dumps({'status': 'Item(s) deleted successfully'})
            return Response(response=response, status=200)

@app.route('/changeItemPrice', methods=['PUT'])
def change_item_price():
    global n_ops
    n_ops += 1
    data = json.loads(request.data)

    # Check if logged in before proceeding
    login_status = check_if_logged_in(data['username'])
    if 'status' in login_status.keys():
        return Response(response=login_status, status=500)
    elif not login_status['is_logged_in']:
        response = json.dumps({'status': 'Error: You must be logged in to change the price of an item.'})
        return Response(response=response, status=401)

    # Check if this is the user's item before changing the price
    sql = f"""
        SELECT rowid FROM products
        WHERE seller = '{data['username']}'
          AND ROWID = {data['item_id']}
    """
    try:
        db_response = query_database(sql, 'product')
    except:
        response = json.dumps({'status': 'Error: Failed to connect to database'})
        return Response(response=response, status=500)
    else:
        if isinstance(db_response, dict):
            response = json.dumps(db_response)
            return Response(response=response, status=500)
        elif not db_response:
                response = json.dumps({'status': 'Error: You are not the one selling this item'})
                return Response(response=response, status=400)    

    # Now actually change the price of the item
    sql = f"""
        UPDATE products SET price = {data['new_price']}
        WHERE rowid = {data['item_id']}
    """
    try:
        db_response = query_database(sql, 'product')
    except:
        response = json.dumps({'status': 'Error: Failed to connect to database'})
        return Response(response=response, status=500)
    else:
        if 'Error' in db_response['status']:
            response = json.dumps(db_response)
            return Response(response=response, status=500)
        else:
            response = json.dumps({'status': 'Items price updated successfully'})
            return Response(response=response, status=200)
        
def query_database(sql: str, db: str):
    """
    Sends a query over gRPC to the database and returns 
    the object that the database returns.
    """
    if db == 'product':
        stub = product_stub
    elif db == 'customer':
        stub = customer_stub

    query = database_pb2.databaseRequest(query=sql)
    db_response = stub.queryDatabase(request=query)
    return pickle.loads(db_response.db_response)

@app.route('/getServerInfo', methods=['GET'])
def get_server_info():
    global n_ops
    n_ops += 1
    response = json.dumps({
        'time': time.time(),
        'n_ops': n_ops
    })
    return Response(response=response, status=200)

    

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
