# gRPC-related dependencies
import grpc
import database_pb2_grpc
import database_pb2

# Other dependencies
import sqlite3
from concurrent.futures import ThreadPoolExecutor
import pickle
import json
import time
from pathlib import Path
import random
import threading
import re
import queue
import socket
import traceback
import raft_utils

class productDBServicer(database_pb2_grpc.databaseServicer):

    def __init__(self, ip: str, port: int, replicas: list[str]):
        self.ip = ip
        self.port = port
        self.commit_log = raft_utils.CommitLog(f'commit-log-{self.ip}-{self.port}')
        self.replicas = replicas
        self.conns = [None]*len(self.replicas)
        self.replica_index = -1
        self.db_name = f'products_{self.port}.db'

        # Create commit log file
        commit_logfile = Path(self.commit_log.file)
        commit_logfile.touch(exist_ok=True)

        for i, replica in enumerate(replicas):
            ip, port = replica.split(':')
            port = int(port)
            if (ip, port) == (self.ip, self.port):
                self.replica_index = i
            else:
                self.conns[i] = (ip, port)

        self.current_term = 1
        self.voted_for = -1
        self.votes = set()

        self.state = 'LEADER' if self.replica_index == 0 else 'FOLLOWER'
        self.leader_id = -1
        self.commit_index = 0
        self.next_indices = [0]*len(self.replicas)
        self.matchindices = [-1]*len(self.replicas)
        self.election_period_ms = random.randint(1000, 5000)
        self.rpc_period_ms = 3000
        self.election_timeout = -1

        # Create the database file
        with sqlite3.connect(self.db_name) as con:
            cur = con.cursor()
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

            print("Ready.....")       

    def init(self):
        self.set_election_timeout()

        # Check for election timeout in background
        raft_utils.run_thread(fn=self.on_election_timeout, args=())

        # Sync logs or send heartbeats in the background
        raft_utils.run_thread(fn=self.leader_send_append_entries, args=()) 

    def set_election_timeout(self, timeout=None):
        """
        Reset the timeout after election. Chosen randomly so that all the replicas don't
        start at the same time leading to a split vote.
        """
        if timeout:
            self.election_timeout = timeout
        else:
            self.election_timeout = time.time() + random.randint(self.election_period_ms, 2*self.election_period_ms)/1000.0

    def on_election_timeout(self):
        """
        Check if the replica has timed out. Reset timeout time and start election
        if it has.
        """
        while True:
            if (time.time() > self.election_timeout
            and (self.state == 'FOLLOWER' or self.state == 'CANDIDATE')):
                print("Timeout.....")
                self.set_election_timeout()
                self.start_election()

    def start_election(self):
        """
        Increment the term, vote for self, and send request vote
        to all other replicas in "parallel".
        """
        print("Starting election.....")

        # Increment term and vote for self
        self.state = 'CANDIDATE'
        self.voted_for = self.replica_index
        self.current_term += 1
        self. votes.add(self.replica_index)

        # Send vote requests in parallel
        thread = []
        for i in range(len(self.replicas)):
            if i != self.replica_index:
                t = raft_utils.run_thread(fn=self.request_vote, args=(i, ))
                threads += [t]

        # Wait for requests to be sent
        for t in threads:
            t.join()

        return True
    
    def request_vote(self, server):
        """
        Request a vote from a replica. Retry if a response is not received. 
        """
        last_index, last_term = self.commit_log.get_last_index_term()

        while True:
            # Retry on timeout
            print(f"Requesting vote from {server}")

            # Check if state is still candidate
            if self.state == ' CANDIDATE' and time.time() < self.election_timeout:
                ip, port = self.conns[server]

                msg = f"VOTE-REQ {self.server_index} {self.current_term} \
                        {last_term} {last_index}"
                
                resp = raft_utils.send_and_recv_no_retry(msg, ip, port, timeout=self.rpc_period_ms/1000.0)

                # Retry if request vote times out
                if resp:
                    vote_rep = re.match('^VOTE-REP ([0-9]+) ([0-9\-]+) ([0-9\-]+)$', resp)

                    if vote_rep:
                        server, curr_term, voted_for = vote_rep.groups()
                        self.process_vote_reply(int(server), int(curr_term), int(voted_for))
                        break
            else:
                break
    
    def process_vote_request(self, server, term, last_term, last_index):
        """
        Self will vote for a requestor only if `term` is equal to the self's
        current term, self has not voted or has voted for `server` already, and
        replica's `term` is greater than self's term or is equal but replica's
        `last_index` is greater than or equal to self's.

        This ensures that self only votes for replicas whose logs contain at least
        as much information as self's. Self becomes follower if it votes for someone.
        """
        print(f"Processing vote request from {server} {term}")
        
        # Revert to follower if new term has already started
        if term > self.current_term:
            self.step_down(term)

        # Get last index and term from log
        self_last_index, self_last_term = self.commit_log.get_last_index_term()

        # Vote only under certain conditions
        if (term == self.current_term
        and (self.voted_for == server or self.voted_for == -1)
        and (last_term > self_last_term or (last_term == self_last_term
                                        and last_index >= self_last_index))):
            self.voted_for = server
            self.state = 'FOLLOWER'
            self.set_election_timeout()

        return f"VOTE-REP {self.replica_index} {self.current_term} {self.voted_for}"
    
    def step_down(self, term):
        """
        Step down to FOLLOWER state in `term`. Specify that self hasn't
        voted this term and reset the election timeout.
        """
        print("Stepping down.....")
        self.current_term = term
        self.state = 'FOLLOWER'
        self.voted_for = -1
        self.set_election_timeout()

    def process_vote_reply(self, server, term, voted_for):
        """
        Add vote if vote term is equal to self's current term and self
        is a candidate. Become leader if self received a majority of votes.
        """
        print("Processing vote reply from {server} {term}.....")

        if term > self.current_term:
            self.step_down

        if term == self.current_term and self.state == 'CANDIDATE':
            if voted_for == self.server_index:
                self.votes.add(server)
            
            # Become leader if received majority of votes
            if len(self.votes) > len(self.replicas)/2.0:
                self.state = 'LEADER'
                self.leader_id = self.replica_index
                print(f"{self.replica_index} became leader")
                print(f"{self.votes}-{self.current_term}")

    def leader_send_append_entries(self):
        print(f"Sending append entries from leader")

        while True:
            if self.state == 'LEADER':
                self.append_entries()

                # Commit entry after it has been replicated
                last_index, _ = self.commit_log.get_last_index_term()
                self.commit_index = last_index

    def append_entries(self):
        """
        Send append entries in parallel to other replicas. Process the responses
        using a blocking queue and wait until a majority of replicas recieved the
        entry.
        """
        res = queue.Queue()

        for i in range(len(self.replicas)):
            if i != self.replica_index:
                # Send append entries in parallel
                raft_utils.run_thread(fn=self.send_append_entries_request, args=(i, res, ))

        # Wait for servers to respond
        if len(self.replicas) > 1:
            counts = 0

            while True:
                res.get(block=True)
                counts += 1
                if counts > len(self.replicas/2.0) - 1:
                    return
        else:
            return
        
    def send_append_entries_request(self, server, res=None):
        print(f"Sending append entries to {server}.....")

        # Get logs that server needs to replicate
        prev_idx = self.next_indices[server] - 1
        log_slice = self.commit_log.read_logs_start_end(prev_idx)

        if prev_idx == -1:
            prev_term = 0
        else:
            if len(log_slice) > 0:
                prev_term = log_slice[0][0]
                log_slice = log_slice[1:] if len(log_slice) > 1 else []

        msg = f"APPEND-REQ {self.replica_index} {self.current_term} {prev_idx} {prev_term} \
                {str(log_slice)} {self.commit_index}"   

        while True:
            if self.state == 'LEADER':
                ip, port = self.conns[server]

                resp = raft_utils.send_and_recv_no_retry(msg, ip, port,
                                                         timeout=self.rpc_period_ms/1000.0)

                if resp:
                    append_rep =  re.match('^APPEND-REP ([0-9]+) ([0-9\-]+) ([0-9]+) ([0-9\-]+)$', resp)

                    if append_rep:
                        server, curr_term, flag, index = append_rep.groups()
                        server = int(server)
                        curr_term = int(curr_term)
                        flag = int(flag)
                        success = True if flag == 1 else False
                        index = int(index)

                        self.process_append_reply(server, curr_term, success, index)
                        break
            else:
                break
        if res:
            res.put('ok')

    def process_append_requests(self,
                               server,
                               term,
                               prev_idx,
                               prev_term,
                               logs,
                               commit_index):
        
        # Reset election timeout since heartbeat received
        self.set_election_timeout()

        flag, index = 0, 0

        # Satisfied if server was down during last election
        if term > self.current_term:
            self.step_down(term)

        # Request came from current leader
        if term == self.current_term:
            self.leader_id = server

            # Check if term corresponding to prev_idx matches with that of leader
            self_logs = self.commit_log.read_logs_start_end(prev_idx, prev_idx) \
                            if prev_idx != -1 else []
            
            success = prev_idx == -1 or (len(self_logs) > 0 and self_logs[0][0] == prev_term)

            if success:
                last_index, last_term = self.commit_log.get_last_index_term()

                if len(logs) > 0 and last_term == logs[-1][0] and last_index == self.commit_index:
                    index = self.commit_index
                else:
                    index = self.store_entries(prev_idx, logs)

            flag = 1 if success else 0

        return f"APPEND-REP {self.server_index} {self.current_term} {flag} {index}"
            
    def store_entries(self, prev_idx, leader_logs):
        """
        Syncs a replica's log with the leader's, starting from `prev_idx + 1` and 
        overwriting the next entries with `leader_logs`.
        """
        commands = [f"{leader_logs[i][1]}" for i in range(len(leader_logs))]
        last_index, _ = self.commit_log.log_replace(self.current_term, commands, prev_idx+1)
        self.commit_index = last_index

        # TODO: Execute SQL queries here
        for command in commands:
            self.update_state_machine(command)
        
        return last_index
    
    def update_state_machine(self, command):
        """
        Executes CUD queries once their corresponding log has been replicated.
        """
        match = re.match(r'^QUERY\s+(.+)$', command, re.IGNORECASE)

        if match:
            query = match.group(1)
            with sqlite3.connect(self.db_name) as con:
                cur = con.cursor()
                cur.execute(query)
                con.commit()
    
    def process_append_reply(self, server, term, success, index):
        print(f"Processing append reply from {server} {term}.....")

        if term > self.current_term:
            self.step_down(term)

        if self.state == 'LEADER' and term == self.current_term:
            if success:
                # Logs replicated successfully, increment index for that replica
                self.next_indices[server] = index + 1
            else:
                # Replication failed: Retry processing with one index less
                self.next_indices[server] = max(0, self.next_indices[server]-1)
                self.send_append_entries_request(server)

    def queryDatabase(self, request, context):
        """
        gRPC route that handles connections from the customer server or product server.

        If the database server is the leader: Commit to log, wait for replication, then
        execute query and return to user. 

        If the database server is not the leader: Send the query to the leader. Leader
        commits to log, waits for replication, then executes the query. If query is CUD,
        leader returns response/failure. If query is read, leader returns json response
        from SQLite
        """
        if self.state == 'LEADER':

            # Request is read, don't need to replicate
            if 'SELECT' in request.query:
                try:
                    with sqlite3.connect(self.db_name) as con:
                        cur = con.cursor()
                        db_resp = cur.execute(request.query)
                        serv_resp = pickle.dumps(db_resp.fetchall())
                except:
                    serv_resp = pickle.dumps({'status': 'Error: Bad query or unable to connect'})
                finally:
                    return database_pb2.databaseResponse(db_response=serv_resp) 
            
            # Request is CUD: Log, replicate, execute
            else:
                # Log the query
                query = request.query.replace('\n', ' ')
                last_index, _ = self.commit_log.log(
                    self.current_term, query
                )

                # Wait for the log to be replicated
                while True:
                    if last_index == self.commit_index:
                        break

                # Execute the query
                try:
                    with sqlite3.connect(self.db_name) as con:
                        cur = con.cursor()
                        db_resp = cur.execute(request.query)
                        con.commit()
                        serv_resp = pickle.dumps({'status': 'Success: Operation completed'})
                except:
                    serv_resp = pickle.dumps({'status': 'Error: Bad query or unable to connect'})
                finally:
                    return database_pb2.databaseResponse(db_response=serv_resp) 
        
        else:
            while True:
                if 'SELECT' in request.query:
                    query = request.query.replace('\n', ' ')
                    msg = f"QUERY {query}"
                    resp = raft_utils.send_and_recv_no_retry(
                        msg,
                        self.conns[self.leader_id][0],
                        self.conns[self.leader_id][1]
                    )

                    if 'error' in resp:
                        serv_resp = pickle.dumps({'status': 'Error: Bad query or unable to connect'})
                    else:
                        serv_resp = pickle.dumps(json.loads(resp))

                    return database_pb2.databaseResponse(db_response=serv_resp)
                
                # Send query message to the leader
                else:
                    query = request.query.replace('\n', ' ')
                    msg = f"QUERY {query}"
                    resp = raft_utils.send_and_recv_no_retry(
                        msg,
                        self.conns[self.leader_id][0],
                        self.conns[self.leader_id][1],
                        timeout = self.rpc_period_ms
                    )

                    if 'success' in resp:
                        serv_resp = pickle.dumps({'status': 'Success: Operation completed'})
                    else:
                       serv_resp = pickle.dumps({'status': 'Error: Bad query or unable to connect'})

                    return database_pb2.databaseResponse(db_response=serv_resp)


    def handle_commands(self, msg, conn):
        """
        Handles all commands passed from one replica to another.
        """
        query_req = re.match(r'^QUERY\s+(.+)$', msg, re.IGNORECASE)
        vote_req = re.match(
            '^VOTE-REQ ([0-9]+) ([0-9\-]+) ([0-9\-]+) ([0-9\-]+)$', msg)
        append_req = re.match(
            '^APPEND-REQ ([0-9]+) ([0-9\-]+) ([0-9\-]+) ([0-9\-]+) (\[.*?\]) ([0-9\-]+)$', msg)

        if query_req:
            # NOTE: Don't need to check for leader because this only gets called
            # when a follower passes the query to the leader
            try:
                query = query_req.group(1)

                while True:

                    # Don't need to replicate read logs
                    if 'SELECT' in query:
                        try:
                            with sqlite3.connect(self.db_name) as con:
                                cur = con.cursor()
                                db_resp = cur.execute(query)
                                output = json.dumps(db_resp.fetchall())
                        except:
                            output = 'error'

                    else:
                        last_index, _ = self.commit_log.log(
                            self.current_term, msg
                        )

                        # Wait for log replication (happens in other thread)
                        while True:
                            if last_index == self.commit_index:
                                break

                        # Update state machine
                        try:
                            with sqlite3.connect(self.db_name) as con:
                                cur = con.cursor()
                                cur.execute(query)
                                con.commit()
                                output = 'success'
                        except:
                            output = 'error'
            except Exception as e:
                traceback.print_exc(limit=1000)

        elif vote_req:
            try:
                server, curr_term, last_term, last_idx = vote_req.groups()
                server, curr_term = int(server), int(curr_term)
                last_term, last_idx = int(last_term), int(last_idx)

                output = self.process_vote_request(
                    server, curr_term, last_term, last_idx
                )
            except Exception as e:
                traceback.print_exc(limit=1000)

        elif append_req:
            try:
                server, curr_term, prev_idx, prev_term, logs, commit_idx = append_req.groups()
                server, curr_term = int(server), int(curr_term)
                last_term, last_idx = int(prev_idx), int(prev_term)
                logs, commit_idx = eval(logs), int(commit_idx)

                output = self.process_append_requests(
                    server, curr_term, prev_idx, prev_term, logs, commit_idx
                )
            except Exception as e:
                traceback.print_exc(limit=1000)

        else:
            print("Hello1 - " + msg + " - Hello2")
            output = "Error: Invalid command"
        
        return output

    def process_request(self, conn):
        while True:
            try:
                msg = conn.recv(2048)
                if msg:
                    msg = msg.decode()
                    print(f"{msg} received")
                    output = self.handle_commands(msg, conn)
                    conn.sendall(output.encode())
            except Exception as e:
                traceback.print_exc(limit=1000)
                print("Error processing message from client")
                conn.close()
                break

    def listen_to_replicas(self):
        """
        Handles communication between database replicas. Clients talk
        to the servers via `queryDatabase`
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockpot(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('0.0.0.0', self.port))
        sock.listen(50)

        while True:
            try:
                client_socket, client_address = sock.accept()

                print(f"Connected to new client at address {client_address}")
                my_thread = threading.Thread(
                    target = self.process_request,
                    args = (client_socket, )
                )
                my_thread.daemon = True
                my_thread.start()

            except:
                print("Error accepting connection.....")


if __name__ == "__main__":
    # Start the server
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    database_pb2_grpc.add_databaseServicer_to_server(productDBServicer(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    server.wait_for_termination()