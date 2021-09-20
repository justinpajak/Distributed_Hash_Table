#!/usr/bin/env python3

# Justin Pajak
# CSE 40771 - Distributed Systems
# Professor Thain
# HashTableServer.py

import sys, socket, json, re
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
    
    def listen(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.bind((self.host, self.port))
        if self.port == 0:
            self.port = self.s.getsockname()[1]
        self.s.listen()
        print(f"Listening on port {self.port}")

    def send_failure_or_invalid(self, result):
        response = json.dumps({"result": result})
        self.conn.sendall(len(response).to_bytes(8, "little"))
        self.conn.sendall(response.encode("ascii"))

    def accept(self):
        self.conn, addr = self.s.accept()
        with self.conn:
            # Keep accepting requests from connected client
            try:
                while True:

                        # Read length of message from client
                        try:
                            message_len = self.conn.recv(8)
                        except:
                            continue
                        message_len = int.from_bytes(message_len, "little")

                        # Read message from client
                        message = ""
                        total_read = 0
                        while True:
                            data = self.conn.recv(message_len).decode("ascii")
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
                            
                            # Complete INSERT operation
                            self.table.insert(key, value)
                            
                            # Send response back
                            response = json.dumps({"result": "success", "method": "Insert"})
                            self.conn.sendall(len(response).to_bytes(8, "little"))
                            self.conn.sendall(response.encode("ascii"))


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
                            self.conn.sendall(len(response).to_bytes(8, "little"))
                            self.conn.sendall(response.encode("ascii"))

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
                            
                            # Complete REMOVE operation
                            value = {}
                            try:
                                value = self.table.remove(key)
                            except:
                                # Operation failed during remove
                                self.send_failure_or_invalid("failure")
                                break
                            
                            # Send response back
                            response = json.dumps({"result": "success", "method": "Remove", "value": value})
                            self.conn.sendall(len(response).to_bytes(8, "little"))
                            self.conn.sendall(response.encode("ascii"))

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
                            self.conn.sendall(len(response).to_bytes(8, "little"))
                            self.conn.sendall(response.encode("ascii"))
                        
                        # Invalid method entered
                        else:
                            self.send_failure_or_invalid("invalid")
            except:
                pass
                            

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
    server.listen()
    while True:
        server.accept()
    
run_server()