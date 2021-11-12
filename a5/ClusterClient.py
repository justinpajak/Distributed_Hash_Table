#!/usr/bin/env python3

# Justin Pajak
# CSE 40771 - Distributed Systems
# Professor Thain
# ClusterClient.py

import hashlib, time

from HashTableClient import HashTableClient

class ClusterClient:
    
    def __init__(self, name, n, k):
        self.name = name
        self.n = n
        self.k = k
        self.client_socks = {}
    
    def hash_function(self, key):
        hash = int(hashlib.md5(key.encode("ascii")).hexdigest(), 16)
        return hash % self.n

    def add_server(self, server):
        if server not in self.client_socks:
            client = HashTableClient(server)
            client.query_ns()
            self.client_socks[server] = client
    
    def insert(self, key, value):

        # Compute hash function to get main server
        main_server = self.hash_function(key)

        # Connect to the k different servers
        for i in range(self.k):
            server = self.name + "-" + str((main_server + i) % self.n)

            # If not connected to this server before, add to client_socks
            # client_socks (key: server name, value: HashTableClient object)
            self.add_server(server)

            # Attempt to write each replica in turn
            client_obj = self.client_socks[server]
            while True:
                try:
                    client_obj.connect()
                except:
                    time.sleep(5)
                    continue
                client_obj.insert(key, value)
                break

    def lookup(self, key):
        
        # Compute hash function to get main server
        main_server = self.hash_function(key)

        # Attempt to access resource at each replica until success
        while True:
            for i in range(self.k):
                server = self.name + "-" + str((main_server + i) % self.n)

                self.add_server(server)
                
                # Attempt to read from each replica
                client_obj = self.client_socks[server]
                try:
                    client_obj.connect()
                except:
                    pass
                return client_obj.lookup(key)
            
            # None of the replicas are available
            # Client waits for 5 secs and then starts over
            time.sleep(5)

    def remove(self, key):
        
        # Compute hash function to get main server
        main_server = self.hash_function(key)

        # Connect to the k different servers
        for i in range(self.k):
            server = self.name + "-" + str((main_server + i) % self.n)

            self.add_server(server)
            
            # Attempt to write each replica in turn
            client_obj = self.client_socks[server]
            while True:
                try:
                    client_obj.connect()
                except:
                    time.sleep(5)
                    continue
                client_obj.remove(key)
                break

    def scan(self, regex):

        # Gain resources from each replica
        pairs = []
        for i in range(self.n):
            server = self.name + "-" + str(i)

            self.add_server(server)

            client_obj = self.client_socks[server]
            while True:
                try:
                    client_obj.connect()
                except:
                    time.sleep(5)
                    continue
                results = client_obj.scan(regex)
                for r in results:
                    if r not in pairs:
                        pairs.extend([r])
                break
        
        return pairs