#!/usr/bin/env python3

# Justin Pajak
# CSE 40771 - Distributed Systems
# Professor Thain
# HashTableServer.py

import sys, socket, json, re, os
from HashTable import HashTable

# Display usage
def usage(status):
    print("./HashTableServer.py PORT")
    sys.exit(status)

class HashTableServer:

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.table = HashTable()
        self.txn_size = 0
    
    def listen(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.txn = open("table.txn", "a")
        self.s.bind((self.host, self.port))
        if self.port == 0:
            self.port = self.s.getsockname()[1]
        self.s.listen()
        print(f"Listening on port {self.port}")

    def send_failure_or_invalid(self, result):
        response = json.dumps({"result": result})
        self.conn.sendall(len(response).to_bytes(8, "little"))
        self.conn.sendall(response.encode("ascii"))

    def accept_client(self):
        self.conn, addr = self.s.accept()
        with self.conn:
            # Keep accepting requests from connected client
            try:
                while True:

                    # Read length of message from client
                    message_len = self.conn.recv(8)
                    if not message_len:
                        break
                    message_len = int.from_bytes(message_len, "little")

                    # Read message from client
                    message = ""
                    total_read = 0
                    while True:
                        data = self.conn.recv(1024).decode("ascii")
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
                        self.send_failure_or_invalid("invalid")
                        break

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
                            self.send_failure_or_invalid("invalid")
                            break
                        if not isinstance(message_json["key"], str):
                            # "key" entry is not a string
                            self.send_failure_or_invalid("invalid")
                            break
                        
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
                        self.conn.sendall(response)

                    # Invoke LOOKUP
                    #----------------------------------------------------------#
                    elif method == "lookup":
                        key = ""
                        try:
                            key = message_json["key"]
                        except:
                            # Client sent JSON w/o "key" entry
                            self.send_failure_or_invalid("invalid")
                            break
                        if not isinstance(message_json["key"], str):
                            # "key" entry is not a string
                            self.send_failure_or_invalid("invalid")
                            break

                        # Complete LOOKUP operation
                        value = {}
                        try:
                            value = self.table.lookup(key)
                        except:
                            # Operation failed during lookup
                            self.send_failure_or_invalid("failure")
                            break
                        
                        # Send response back
                        response = json.dumps({"result": "success", "method": "Lookup", "value": value})
                        response = len(response).to_bytes(8, "little") + response.encode("ascii")
                        self.conn.sendall(response)

                    # Invoke REMOVE
                    #----------------------------------------------------------#
                    elif method == "remove":
                        key = ""
                        try:
                            key = message_json["key"]
                        except:
                            # Client sent JSON w/o "key" entry
                            self.send_failure_or_invalid("invalid")
                            break
                        if not isinstance(message_json["key"], str):
                            # "key" entry is not a string
                            self.send_failure_or_invalid("invalid")
                            break

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
                            self.send_failure_or_invalid("failure")
                            break
                        
                        # Send response back
                        response = json.dumps({"result": "success", "method": "Remove", "value": value})
                        response = len(response).to_bytes(8, "little") + response.encode("ascii")
                        self.conn.sendall(response)
    
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
                            self.send_failure_or_invalid("invalid")
                            break
                        # Complete SCAN operation
                        results = self.table.scan(regex)

                        # Send response back
                        response = json.dumps({"result": "success", "method": "Scan", "pairs": results})
                        response = len(response).to_bytes(8, "little") + response.encode("ascii")
                        self.conn.sendall(response)
                    
                    # Invalid method entered
                    else:
                        self.send_failure_or_invalid("invalid")

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
                passx


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
    port = int(args[0])

    # Create HashTableServer Object
    host = socket.gethostbyname(socket.gethostname())
    server = HashTableServer(host, port)

    # Restore state of hash table from table.ckpt and table.txn
    server.restore()

    # Listen on port
    server.listen()

    # Accept client connections
    while True:
        server.accept_client()
        
    server.s.close()
    
run_server()