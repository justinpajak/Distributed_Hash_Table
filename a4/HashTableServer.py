#!/usr/bin/env python3

# Justin Pajak
# CSE 40771 - Distributed Systems
# Professor Thain
# HashTableServer.py

import sys, socket, json, re, os, time, select
from HashTable import HashTable

# Display usage
def usage(status):
    print("./HashTableServer.py PROJECT")
    sys.exit(status)

class HashTableServer:

    def __init__(self, host):
        self.host = host
        self.table = HashTable()
        self.txn_size = 0
        self.socks = {}
    
    def listen(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.txn = open("table.txn", "a")
        self.s.bind((self.host, 0))
        self.port = self.s.getsockname()[1]
        self.s.listen()
        print(f"Listening on port {self.port}")

    def contact_ns(self):
        message = {
            "type": "hashtable",
            "port": self.port,
            "owner": "jpajak",
            "project": self.project
        }
        self.ns_sock.sendto(json.dumps(message).encode("ascii"), ("catalog.cse.nd.edu", 9097))

    def send_failure_or_invalid(self, result, sock):
        response = json.dumps({"result": result})
        response = len(response).to_bytes(8, "little") + response.encode("ascii")
        sock.sendall(response)

    def accept_clients(self):
        # Start timer for name server messaging
        self.start_time = time.time()

        # Establish dictionary of sockets for select
        self.socks["rlist"] = [self.s]
        self.socks["wlist"] = []
        self.socks["xlist"] = []

        try:
            while True:
                # Use select to make server event-driven
                rlist, wlist, xlist = select.select(self.socks["rlist"], self.socks["wlist"], self.socks["xlist"])
                for sock in rlist:
                    if sock == self.s:
                        conn, addr = sock.accept()
                        self.socks["rlist"].append(conn)
                    else:
                        try:
                            # Read length of message from client
                            message_len = sock.recv(8)
                            if not message_len:
                                self.socks["rlist"].remove(sock)
                                sock.close()
                                continue
                            message_len = int.from_bytes(message_len, "little")

                            # Read message from client
                            message = ""
                            total_read = 0
                            while True:
                                data = sock.recv(1024).decode("ascii")
                                total_read += len(data)
                                message += data
                                if total_read == message_len:
                                    break
            
                            # Determine if request is valid and parse request
                            message_json = {}
                            method = ""
                            try:
                                message_json = json.loads(message)
                                method = message_json["method"]
                            except:
                                # Client did not send JSON data or...
                                # Client sent JSON w/o "method" entry
                                # Disconnect and wait for new connections
                                self.send_failure_or_invalid("invalid", sock)
                                self.socks["rlist"].remove(sock)
                                sock.close()
                                continue

                            # Invoke INSERT
                            #----------------------------------------------------------#
                            if method == "insert":
                                key = ""
                                value = ""
                                try:
                                    key = message_json["key"]
                                    value = message_json["value"]
                                except:
                                    # Client sent JSON w/o "key" or "value" entry
                                    self.send_failure_or_invalid("invalid", sock)
                                    self.socks["rlist"].remove(sock)
                                    sock.close()
                                    continue
                                if not isinstance(message_json["key"], str):
                                    # "key" entry is not a string
                                    self.send_failure_or_invalid("invalid", sock)
                                    self.socks["rlist"].remove(sock)
                                    sock.close()
                                    continue
                                
                                # First add entry to transaction log before inserting into table
                                self.txn.write(message)
                                self.txn.write("\n")
                                self.txn.flush()
                                os.fsync(self.txn.fileno())
                                self.txn_size += 1

                                # Complete INSERT operation
                                self.table.insert(key, value)
                                
                                # Send response back
                                response = json.dumps({"result": "success", "method": "Insert"})
                                response = len(response).to_bytes(8, "little") + response.encode("ascii")
                                sock.sendall(response)

                            # Invoke LOOKUP
                            #----------------------------------------------------------#
                            elif method == "lookup":
                                key = ""
                                try:
                                    key = message_json["key"]
                                except:
                                    # Client sent JSON w/o "key" entry
                                    self.send_failure_or_invalid("invalid", sock)
                                    self.socks["rlist"].remove(sock)
                                    sock.close()
                                    continue
                                if not isinstance(message_json["key"], str):
                                    # "key" entry is not a string
                                    self.send_failure_or_invalid("invalid", sock)
                                    self.socks["rlist"].remove(sock)
                                    sock.close()
                                    continue

                                # Complete LOOKUP operation
                                value = {}
                                try:
                                    value = self.table.lookup(key)
                                except:
                                    # Operation failed during lookup
                                    self.send_failure_or_invalid("failure", sock)
                                    continue
                                
                                # Send response back
                                response = json.dumps({"result": "success", "method": "Lookup", "value": value})
                                response = len(response).to_bytes(8, "little") + response.encode("ascii")
                                sock.sendall(response)

                            # Invoke REMOVE
                            #----------------------------------------------------------#
                            elif method == "remove":
                                key = ""
                                try:
                                    key = message_json["key"]
                                except:
                                    # Client sent JSON w/o "key" entry
                                    self.send_failure_or_invalid("invalid", sock)
                                    self.socks["rlist"].remove(sock)
                                    sock.close()
                                    continue
                                if not isinstance(message_json["key"], str):
                                    # "key" entry is not a string
                                    self.send_failure_or_invalid("invalid", sock)
                                    self.socks["rlist"].remove(sock)
                                    sock.close()
                                    continue

                                # First add entry to transaction log before inserting into table
                                if key in self.table.table:
                                    self.txn.write(message)
                                    self.txn.write("\n")
                                    self.txn.flush()
                                    os.fsync(self.txn.fileno())
                                    self.txn_size += 1
                                
                                    # Complete REMOVE operation
                                    value = {}
                                    value = self.table.remove(key)
                                else:
                                    # Key not in table - KEYERROR
                                    self.send_failure_or_invalid("failure", sock)
                                    continue
                                
                                # Send response back
                                response = json.dumps({"result": "success", "method": "Remove", "value": value})
                                response = len(response).to_bytes(8, "little") + response.encode("ascii")
                                sock.sendall(response)
            
                            # Invoke SCAN
                            #----------------------------------------------------------#
                            elif method == "scan":
                                regex = ""
                                try:
                                    regex = message_json["regex"]
                                    temp = re.compile(regex)
                                except:
                                    # Client sent JSON w/o "regex" entry or...
                                    # client sent an invalid regular expression
                                    self.send_failure_or_invalid("invalid", sock)
                                    self.socks["rlist"].remove(sock)
                                    sock.close()
                                    continue
                                # Complete SCAN operation
                                results = self.table.scan(regex)

                                # Send response back
                                response = json.dumps({"result": "success", "method": "Scan", "pairs": results})
                                response = len(response).to_bytes(8, "little") + response.encode("ascii")
                                sock.sendall(response)
                            
                            # Invalid method entered
                            else:
                                self.send_failure_or_invalid("invalid", sock)
                                self.socks["rlist"].remove(sock)
                                sock.close()

                            # Check if transaction log has more than 100 entries
                            # if yes, compact.
                            if self.txn_size > 100:
                                with open("temp.ckpt", "w") as temp:
                                    for key in self.table.table:
                                        op = {"key": key, "value": self.table.table[key]}
                                        temp.write(json.dumps(op))
                                        temp.write("\n")
                                    temp.flush()
                                    os.fsync(temp.fileno())
                                # Atomically update the checkpoint file
                                os.rename("temp.ckpt", "table.ckpt")
                                self.txn.close()
                                os.remove("table.txn")
                                self.txn = open("table.txn", "a")
                                self.txn_size = 0
                        except:
                            pass
        except:
            pass


    def restore(self):
        # Read checkpoint file into memory in hash table
        if os.path.exists("table.ckpt"):
            with open("table.ckpt") as cp:
                state = cp.readlines()
                for op in state:
                    op = json.loads(op.strip())
                    self.table.insert(op["key"], op["value"])
        else:
            open("table.ckpt", "w")
            
        # Play back all entries in transaction log
        if os.path.exists("table.txn"):
            with open("table.txn") as txn:
                ops = txn.readlines()
                self.txn_size = len(ops)
                for op in ops:
                    op = json.loads(op.strip())
                    if op["method"] == "insert":
                        self.table.insert(op["key"], op["value"])
                    elif op["method"] == "remove":
                        self.table.remove(op["key"])
        else:
            open("table.txn", "w")
            

def run_server():

    # Get port number from command line
    args = sys.argv[1:]
    if len(args) != 1:
        usage(1)
    if args[0] == '-h':
        usage(0)

    # Create HashTableServer Object
    host = socket.gethostbyname(socket.gethostname())
    server = HashTableServer(host)
    server.project = args[0]

    # Restore state of hash table from table.ckpt and table.txn
    server.restore()

    # Listen on port
    server.listen()

    # Send UDP packet to catalog.cse.nd.edu:9097 now and every 1 minute after
    server.ns_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.contact_ns()
    pid = os.fork()
    if pid == 0:
        start_time = time.time()
        while True:
            if time.time() - start_time >= 60:
                start_time = time.time()
                server.contact_ns()
    elif pid > 0:
        # Accept client connections
        server.accept_clients()
        server.s.close()
    
run_server()