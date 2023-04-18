import threading
import socket
import select
import traceback
import datetime

def run_thread(fn, args):
    my_thread = threading.Thread(target=fn, args=args)
    my_thread.daemon = True
    my_thread.start()
    return my_thread

def wait_for_server_startup(ip, port):
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.connect((str(ip), int(port)))
            return sock
                
        except Exception as e:
            traceback.print_exc(limit=1000)

def send_and_recv_no_retry(msg, ip, port, timeout=-1):
    # Could not connect possible reasons:
    # 1. Server is not ready
    # 2. Server is busy and not responding
    # 3. Server crashed and not responding
    
    conn = wait_for_server_startup(ip, port)
    resp = None
    
    try:
        conn.sendall(msg.encode())
        
        if timeout > 0:
            ready = select.select([conn], [], [], timeout)
            if ready[0]:
                resp = conn.recv(2048).decode()
        else:
            resp = conn.recv(2048).decode()
                
    except Exception as e:
        traceback.print_exc(limit=1000)
        # The server crashed but it is still not marked in current node
    
    conn.close()
    return resp
            
def send_and_recv(msg, ip, port, res=None, timeout=-1):
    resp = None
    # Could not connect possible reasons:
    # 1. Server is not ready
    # 2. Server is busy and not responding
    # 3. Server crashed and not responding
    
    while True:
        resp = send_and_recv_no_retry(msg, ip, port, timeout)
        
        if resp:
            break
            
    if res is not None:
        res.put(resp)
        
    return resp


class CommitLog:
    def __init__(self, file):
        self.file = file
        self.lock = threading.Lock()
        self.last_term = 0
        self.last_index = -1
        
    def get_last_index_term(self):
        with self.lock:
            return self.last_index, self.last_term
        
    def log(self, term, command):
        # Append the term and command to file along with timestamp
        with self.lock:
            with open(self.file, 'a') as f:
                now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                f.write(f"{now},{term},{command}\n")
                self.last_term = term
                self.last_index += 1

            return self.last_index, self.last_term
                
    def log_replace(self, term, commands, start):
        # Replace or Append multiple commands starting at 'start' index line number in file
        index = 0
        i = 0
        with self.lock:
            with open(self.file, 'r+') as f:
                if len(commands) > 0:
                    while i < len(commands):
                        if index >= start:
                            command = commands[i]
                            i += 1
                            now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                            f.write(f"{now},{term},{command}\n")
                            
                            if index > self.last_index:
                                self.last_term = term
                                self.last_index = index
                        
                        index += 1
                    
                    # Truncate all lines coming after last command.
                    f.truncate()
        
            return self.last_index, self.last_term
    
    def read_log(self):
        # Return in memory array of term and command
        with self.lock:
            output = []
            with open(self.file, 'r') as f:
                for line in f:
                    _, term, command = line.strip().split(",")
                    output += [(term, command)]
            
            return output
        
    def read_logs_start_end(self, start, end=None):
        # Return in memory array of term and command between start and end indices
        with self.lock:
            output = []
            index = 0
            with open(self.file, 'r') as f:
                for line in f:
                    if index >= start:
                        _, term, command = line.strip().split(",")
                        output += [(term, command)]
                    
                    index += 1
                    if end and index > end:
                        break
            
            return output