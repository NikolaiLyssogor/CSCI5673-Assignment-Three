# gRPC-related dependencies
import grpc
import database_pb2_grpc
import database_pb2

# Other dependencies
import sqlite3
from concurrent.futures import ThreadPoolExecutor
import pickle

class productDBServicer(database_pb2_grpc.databaseServicer):

    def __init__(self):
        """
        Reinitializes the tables, creates the cursor
        and connection objects.
        """
        with sqlite3.connect('products.db') as con:
            cur = con.cursor()

            # Create products table
            cur.execute("DROP TABLE IF EXISTS products")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS products (
                name TEXT NOT NULL,
                category INTEGER NOT NULL,
                keywords TEXT NOT NULL,
                condition TEXT NOT NULL,
                price REAL NOT NULL,
                quantity INTEGER NOT NULL,
                seller TEXT NOT NULL,
                status TEXT DEFAULT 'For Sale'
                )
            """)
            con.commit()

    def queryDatabase(self, request, context):
        """
        Implements the gRPC route for querying the database. All
        other operations go through updateDatabase.
        """
        print(request.query)
        with sqlite3.connect('products.db') as con:
            try:
                cur = con.cursor()
                db_resp = cur.execute(request.query)
                if 'SELECT' in request.query:
                    # R in CRUD
                    serv_resp = pickle.dumps(db_resp.fetchall())
                else:
                    # CUD in CRUD
                    con.commit()
                    serv_resp = pickle.dumps({'status': 'Success: Operation completed'})
            except:
                serv_resp = pickle.dumps({'status': 'Error: Bad query or unable to connect'})
            finally:
                return database_pb2.databaseResponse(db_response=serv_resp)


if __name__ == "__main__":
    # Start the server
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    database_pb2_grpc.add_databaseServicer_to_server(productDBServicer(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    server.wait_for_termination()