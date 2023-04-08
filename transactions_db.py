# Spyne-related dependencies (SOAP)
# from spyne.application import Application
# # from spyne.decorator import srpc
# from spyne.service import ServiceBase
# from spyne.model.primitive import String, Integer
# from spyne.model.binary import ByteArray
# from spyne.server.wsgi import WsgiApplication
# from spyne.protocol.soap.soap11 import Soap11


from spyne import Application, rpc, ServiceBase, String, ByteArray, Integer

from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication

from wsgiref.simple_server import make_server

import sqlite3
import pickle
import logging


class TransactionsDBService(ServiceBase):

    # @rpc(String, String, String, Integer, Integer, Integer, _returns=Integer)
    # def write_transaction(ctx, cc_name, cc_number, cc_exp, item_id, quantity, buyer_id):
    #     """
    #     For writing a new transaction.
    #     """
    #     sql = f"""
    #         INSERT INTO transactions ('cc_name', 'cc_number', 'cc_exp', 'item_id', 'quantity', 'buyer_id') VALUES
    #         ('{cc_name}', '{cc_number}', '{cc_exp}', {item_id}, {quantity}, {buyer_id})
    #     """
    #     print(sql)

    #     with sqlite3.connect('transactions.db') as con:
    #         try:
    #             cur = con.cursor()
    #             cur.execute(sql)
    #             con.commit()
    #             return 1
    #         except:
    #             return 0

    @rpc(String, _returns=ByteArray)
    def query_database(ctx, query):
        """
        API for general queries. Simply executes the sql specified.

        @param query: The string SQL query.
        @return The pickled database response or error message.
        """
        print(query)
        with sqlite3.connect('transactions.db') as con:
            try:
                cur = con.cursor()
                db_resp = cur.execute(query)
                if 'SELECT' in query:
                    # R in CRUD
                    serv_resp = db_resp.fetchall()
                else:
                    # CUD in CRUD
                    con.commit()
                    serv_resp = {'status': 'Success: Operation completed'}
            except:
                serv_resp = {'status': 'Error: Bad query or unable to connect'}
            finally:
                print("\n", serv_resp,  '\n')
                return pickle.dumps(serv_resp)

def init_database():
    with sqlite3.connect('transactions.db') as con:
        cur = con.cursor()

        # Create products table
        cur.execute("DROP TABLE IF EXISTS transactions")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
            cc_name TEXT NOT NULL,
            cc_number TEXT NOT NULL,
            cc_exp TEXT NOT NULL,
            item_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            buyer_name TEXT NOT NULL,
            seller_name TEXT NOT NULL
            )
        """)
        con.commit()

if __name__ == "__main__":
    init_database()

    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('spyne.protocol.xml').setLevel(logging.DEBUG)

    app = Application([TransactionsDBService], 'spyne.csci.transactions.http',
        in_protocol=Soap11(validator='lxml'),
        out_protocol=Soap11(),
    )

    wsgi_app = WsgiApplication(app)
    server = make_server('127.0.0.1', 8000, wsgi_app)

    print("listening to http://127.0.0.1:8000") 
    print("wsdl is at: http://localhost:8000/?wsdl")

    server.serve_forever()
