import start_servers as startup
import grpc
import database_pb2_grpc
import database_pb2
import threading

import sqlite3
from concurrent import futures
import pickle

import socket


class customerDBServicerGroup:
    def __init__(self):
        pass


class customerDBServicerGroupMember(database_pb2_grpc.databaseServicer):
    def __init__(self, sID, addr, port):
        self.senderID = sID
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((addr, port))
        self.db = customerDB()

    def receiveClientRequest(self):
        pass

    def buildRequestMessage(self):
        pass

    def sendRequestMessage(self):
        pass

    def sendSequenceMessage(self):
        pass

    def buildSequenceMessage(self):
        pass

    def receiveGroupMessages(self):
        pass


class rotatingSequencerRequestMessage:
    def __init__(self, sID, localSeq):
        self.requestID = (sID, localSeq)
        self.globalSeq = None
        self.receivedCount = 0
        pass


class rotatingSequencerSequenceMessage:
    def __init__(self):
        pass


class customerDB:
    def __init__(self):
        """
        Reinitializes the tables, creates the cursor
        and connection objects.
        """
        with sqlite3.connect('customers.db') as con:
            cur = con.cursor()

            # Create sellers table
            cur.execute("DROP TABLE IF EXISTS sellers")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sellers (
                username TEXT NOT NULL, 
                password TEXT NOT NULL, 
                thumbs_up INTEGER DEFAULT 0, 
                thumbs_down INTEGER DEFAULT 0, 
                items_sold INTEGER DEFAULT 0,
                is_logged_in TEXT DEFAULT 'false'
                )
            """)

            # Create buyers table
            cur.execute("DROP TABLE IF EXISTS buyers")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS buyers (
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                items_purchased INTEGER DEFAULT 0,
                is_logged_in TEXT DEFAULT 'false'
                )
            """)
            con.commit()

    def queryDatabase(self, request, context):
        """
        Implements the gRPC route for querying the database. All
        other operations go through updateDatabase.
        """
        print(request.query)
        with sqlite3.connect('customers.db') as con:
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


def serve(config=None, local=False, groupMember=0):
    servers = []
    try:
        port = config.ports[groupMember]
        host = config.localHost if local else config.hosts[groupMember]

        # initialize customer DB server
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=config.maxThreads))
        database_pb2_grpc.add_databaseServicer_to_server(customerDBServicerGroupMember(groupMember, host, port), server)
        server.add_insecure_port('{}:{}'.format(host, port))
        server.start()
        servers.append(server)
        print("Starting customer DB server on port {}".format(port))
        server.wait_for_termination()
    except:
        for server in servers:
            server.stop(None).wait()
            print("\nStopping customer DB server")


def startServers(config=None, local=False):
    threads = []
    try:
        for groupMember in range(0, len(config.ports)):
            thread = threading.Thread(target=serve, name="CustomerDB-{}".format(groupMember), args=[config, local, groupMember])
            thread.start()
            threads.append(thread)
    except:
        for thread in threads:
            thread.join()


if __name__ == "__main__":
    startServers(startup.getConfig().customerDB, local=True)
