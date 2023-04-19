import queue
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

        self.db = customerDB("customers_{}.db".format(ports[sID]))
        self.completedMessages = {}
        self.seqMessages = {}
        self.seqMessageQueue = queue.Queue()
        self.pendingMessages = {}
        self.pendingMessagesLock = threading.Lock()

        t1 = threading.Thread(target=self.receiveGroupMessages, name="recvGroupMessages", args=[])
        t2 = threading.Thread(target=self.sendSeqMessage, name="sendSeqMessages", args=[])
        t1.start()
        t2.start()

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
        # TODO the majority of group members (3/5) have received all request AND sequence messages where req.globalSeq < self.globalSeq
        pendingReqs = self.validatePendingRequests(reqMessage.requestID)
        while pendingReqs is False:
            pendingReqs = self.validatePendingRequests(reqMessage.requestID)
            time.sleep(0.5)

        res = self.db.queryDatabase(request)
        return res

    def validatePendingRequests(self, currReqID):
        # check each completed request has global seq #
        completedReqs = len(self.completedMessages)
        if completedReqs > 0:
            allReqsCompleted = completedReqs == self.globalSeq + 1
            for reqID, req in self.completedMessages.items():
                prevReqHasPrevSeq = req.globalSequence
                if prevReqHasPrevSeq is None:
                    return False

            # if found missing req message retransmit message to sender
            if allReqsCompleted is False:
                reqMessageIDs = self.findMissingReqMessage()
                self.retransmitMessage(reqMessageIDs)
                return False
        return True

    def buildRequestMessage(self, request):
        reqMessage = rotatingSequencerRequestMessage(self.senderID, self.localSeq, request)
        self.localSeq += 1
        return reqMessage

    def sendReqMessage(self, reqMessage):
        for addr in self.addrs:
            print("groupMember {} sent request message".format(self.senderID))
            self.socket.sendto(reqMessage.toByteArray(), addr)

    def shouldSendSeqMessage(self):
        # check if member should send seq message
        # update globalSequence once seqMessage is successfully sent
        assignedGlobalSeq = self.globalSeq if self.globalSeq == 0 else self.globalSeq + 1
        return self.senderID == assignedGlobalSeq % (len(self.addrs) + 1)

    def buildSequenceMessage(self, reqID):
        assignedGlobalSeq = self.globalSeq + 1
        # create seq message
        return rotatingSequencerSequenceMessage(reqID, assignedGlobalSeq)

    def readyToSendSeqMessage(self, seqMessage):
        # has received all sequence messages w/ gSeq < k where k = seqMessage.globalSeq
        # has received all req messages w/ gSeq < k
        # for all req messages sent by member where self.senderID = seqMessage.requestID[0]
        # reqMessage.requestID[1] < seqMessage.requestID[1]
        return self.validatePendingRequests(seqMessage.requestID)

    def sendSeqMessage(self):
        # has received all seq messages w/ global seq # < self.globalSeq
        # all req messages RECEIVED have a globalSeq assigned
        # all req messages SENT where member's id = req.sid && req.localSeq < self.localSeq have a globalSeq assigned
        while True:
            try:
                seqMessage = self.seqMessageQueue.get(timeout=1)
                readyToSend = False
                while readyToSend is False:
                    readyToSend = self.readyToSendSeqMessage(seqMessage)

                # send sequence message
                for addr in self.addrs:
                    print("groupMember {} sent sequence message".format(self.senderID))
                    self.socket.sendto(seqMessage.toByteArray(), addr)

                # add sequence message
                self.seqMessages[seqMessage.requestID] = seqMessage
            except queue.Empty:
                continue

    def receiveGroupMessages(self):
        # receive group Message
        while True:
            rlist, _, _ = select.select([self.socket], [], [], 1)
            if len(rlist) > 0:
                for tempSocket in rlist:
                    try:
                        byteData, addr = tempSocket.recvfrom(1024)
                        groupMessage = pickle.loads(byteData)
                        if groupMessage.type == "request":
                            print("groupMember {} received request message".format(self.senderID))
                            # store pending request in shared resource
                            self.pendingMessagesLock.acquire()
                            self.pendingMessages[groupMessage.requestID] = groupMessage
                            self.pendingMessagesLock.release()

                            # determine if sequence message should be sent
                            if self.shouldSendSeqMessage():
                                self.seqMessageQueue.put(self.buildSequenceMessage(groupMessage.requestID))
                        elif groupMessage.type == "sequence":
                            print("group member {} received sequence message".format(self.senderID))
                            self.completeMessage(groupMessage)
                        else:
                            # TODO improve error
                            raise Exception("BAD GROUP MESSAGE TYPE")
                    except socket.error as msg:
                        print(msg)

    def completeMessage(self, seqMessage):
        # get request message
        reqMessage = None
        self.pendingMessagesLock.acquire()
        hasMessage = self.pendingMessages.keys().__contains__(seqMessage.requestID)
        if hasMessage:
            reqMessage = self.pendingMessages.pop(seqMessage.requestID)
        else:
            self.retransmitMessage([seqMessage.requestID])
            print("ERROR message not found!")
        self.pendingMessagesLock.release()

        # process request
        if reqMessage:
            print("groupMember {} processing client request".format(self.senderID))
            self.db.queryDatabase(reqMessage.clientReq)

        # update globalSeq
        #  consider using completed request count as globalSequence value
        if self.globalSeq < seqMessage.globalSequence:
            print("updating global sequence from {} to {}".format(self.globalSeq, seqMessage.globalSequence))
            self.globalSeq = seqMessage.globalSequence

        # move message from pending to completed
        reqMessage.globalSequence = seqMessage.globalSequence
        self.completedMessages[reqMessage.requestID] = reqMessage

    def findMissingReqMessage(self):
        missingMessageIDs = []
        globalSeqs = range(0, self.globalSeq)
        messageGlobalSeqs = [v.globalSequence for v in self.completedMessages.values()]
        missingSeqs = [x for x in globalSeqs if x not in messageGlobalSeqs]
        if len(missingSeqs) > 0:
            print("Missing a message!")
            missingMessageIDs = [x.requestID for x in self.seqMessages if x.globalSequence not in missingSeqs]
        return missingMessageIDs

    def retransmitMessage(self, groupMessages):
        # retransmit message to message sender
        for msg in groupMessages:
            print("groupMember {} retransmitting groupMessage: {}".format(self.senderID, msg.requestID))
            self.socket.sendto(msg, self.addrs[msg.requestID[0]])


class rotatingSequencerRequestMessage:
    def __init__(self, sID, localSeq, request):
        self.requestID = (sID, localSeq)
        self.globalSeq = None
        self.clientReq = request
        self.type = "request"
        self.receivedCount = 0

    def toByteArray(self):
        return pickle.dumps(self)

class rotatingSequencerSequenceMessage:
    def __init__(self, reqID, globalSeq):
        self.requestID = reqID
        self.globalSeq = globalSeq
        self.type = "sequence"

    def toByteArray(self):
        return pickle.dumps(self)


class customerDB:
    def __init__(self, db="customers.db"):
        """
        Reinitializes the tables, creates the cursor
        and connection objects.
        """
        self.db_name = db
        with sqlite3.connect(self.db_name) as con:
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
