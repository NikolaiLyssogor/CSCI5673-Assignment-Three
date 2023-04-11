import time

import start_servers as startup
import grpc
import database_pb2_grpc
import database_pb2
import threading

import sqlite3
from concurrent import futures
import pickle

import socket
import select


class customerDBServicerGroupMember(database_pb2_grpc.databaseServicer):
    def __init__(self, sID, ports, hosts):
        self.senderID = sID
        self.localSeq = 0
        self.globalSeq = 0

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((hosts[sID], ports[sID]))
        self.addrs = list(zip(hosts[:], ports[:]))
        self.addrs.remove((hosts[sID], ports[sID]))

        self.db = customerDB()
        self.completedMessages = {}
        self.pendingMessages = {}
        self.pendingMessagesLock = threading.Lock()

        thread = threading.Thread(target=self.receiveGroupMessages, name="groupMessages", args=[])
        thread.start()

    def executeClientRequest(self, request, context):
        reqMessage = self.buildRequestMessage(request)
        # store pending request
        self.pendingMessagesLock.acquire()
        self.pendingMessages[reqMessage.requestID] = reqMessage
        self.pendingMessagesLock.release()
        # update localSequence
        self.localSeq += 1
        self.sendReqMessage(reqMessage)

        # all requests w/ req.globalSeq < self.globalReq are completed
        # the majority of group members (3/5) have received all request AND sequence messages where req.globalSeq < self.globalSeq
        pendingReqs = False
        while pendingReqs is False:
            pendingReqs = self.validatePendingRequests(reqMessage.requestID)
            time.sleep(0.5)

        res = self.db.queryDatabase(request)
        return res

    def validatePendingRequests(self, currReqID):
        # check each request has global seq #
        allReqsCompleted = len(self.completedMessages) == self.globalSeq
        for reqID, req in self.completedMessages.items():
            req.globalSequence < self.globalSeq

        # if found missing message retransmit message to sender
        if allReqsCompleted is False:
            reqMessage = self.findMissingMessage()
            self.retransmitMessage(reqMessage)

        # move current request to completed
        # update global sequence
        return True

    def buildRequestMessage(self, request):
        reqMessage = rotatingSequencerRequestMessage(self.senderID, self.localSeq, request)
        self.localSeq += 1
        return reqMessage

    def sendReqMessage(self, reqMessage):
        for addr in self.addrs:
            self.socket.sendto(reqMessage, addr)

    def sendSeqMessage(self, seqMessage):
        # has received all seq messages w/ global seq # < self.globalSeq
        # all req messages RECEIVED have a globalSeq assigned
        # all req messages SENT where member's id = req.sid && req.localSeq < self.localSeq have a globalSeq assigned
        for addr in self.addrs:
            self.socket.sendto(seqMessage, addr)

    def buildSequenceMessage(self, reqID, globalSeq):
        # check if member should send
        groupMember = self.globalSeq % len(self.po)
        # create seq message
        # send seq message
        # process client request
        return rotatingSequencerSequenceMessage(reqID, globalSeq)

    def receiveGroupMessages(self):
        # receive group Message
        while True:
            rlist, _, _ = select.select([self.socket], [], [], 1)
            if len(rlist) > 0:
                print("received group message")
                for tempSocket in rlist:
                    try:
                        data, addr = tempSocket.recvfrom(1024)
                        print("data = ", data)
                        if data.type == "request":
                            self.pendingMessagesLock.acquire()
                            self.pendingMessages[data.requestID] = data
                            self.pendingMessagesLock.release()
                        else:

                            self.completeMessage(data)
                    except socket.error as msg:
                        print(msg)

    def completeMessage(self, seqMessage):
        # get request message
        self.pendingMessagesLock.acquire()
        reqMessage = self.pendingMessages.pop(seqMessage.requestID)
        self.pendingMessagesLock.release()

        # process request
        self.db.queryDatabase(reqMessage.clientRequest)

        # update globalSeq
        if self.globalSeq < seqMessage.globalSequence:
            print("updating global sequence from {} to {}".format(self.globalSeq, seqMessage.globalSequence))
            self.globalSeq = seqMessage.globalSequence

        # move message from pending to completed
        reqMessage.globalSequence = seqMessage.globalSequence
        self.completedMessages[reqMessage.requestID] = reqMessage

    def findMissingMessage(self):
        reqMessage = None
        foundMessages = [self.completedMessages.keys()]
        self.pendingMessagesLock.acquire()
        # missing a sequence message
        for reqID, req in self.pendingMessages.items():
            if req.globalSequence < self.globalSeq:
                reqMessage = req
            foundMessages.append(reqID)
        self.pendingMessagesLock.release()

        # TODO missing a request message
        if reqMessage is None:
            print("Missing a message!")
        return reqMessage

    def retransmitMessage(self, reqMessage):
        # retransmit message to message sender
        self.socket.sendto(reqMessage, self.addrs[reqMessage.requestID[0]])


class rotatingSequencerRequestMessage:
    def __init__(self, sID, localSeq, request):
        self.requestID = (sID, localSeq)
        self.globalSeq = None
        self.clientReq = request
        self.type = "request"
        self.receivedCount = 0


class rotatingSequencerSequenceMessage:
    def __init__(self, reqID, globalSeq):
        self.requestID = reqID
        self.globalSeq = globalSeq
        self.type = "sequence"


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

    def queryDatabase(self, request):
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
    server = None
    try:
        port = config.ports[groupMember]
        host = config.localHost if local else config.hosts[groupMember]

        # initialize customer DB server
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=config.maxThreads))
        database_pb2_grpc.add_databaseServicer_to_server(customerDBServicerGroupMember(groupMember, config.groupPorts, config.groupHosts), server)
        server.add_insecure_port('{}:{}'.format(host, port))
        server.start()
        print("Starting customer DB server on port {}".format(port))
        server.wait_for_termination()
    except Exception as err:
        print("err = ", err)
    except:
        server.stop(None).wait()
        print("\nStopping customer DB server on port {}".format(config.ports[groupMember]))


def startServers(config=None, local=False):
    threads = []
    try:
        for groupMember in range(len(config.ports)):
            thread = threading.Thread(target=serve, name="CustomerDB-{}".format(groupMember), args=[config, local, groupMember])
            thread.start()
            threads.append(thread)
    except:
        for thread in threads:
            thread.join()


if __name__ == "__main__":
    startServers(startup.getConfig().customerDB, local=True)
