#!/usr/bin/env python3

# Justin Pajak
# CSE 40771 - Distributed Systems
# Professor Thain
# HashTableClient.py

import socket, sys, json, time

class InvalidRequest(Exception):
	pass

class HashTableClient:
	
	def __init__(self, host, port):
		self.host = host
		self.port = port
	
	def connect(self):
		self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.s.connect((self.host, self.port))
	
	def cleanup(self):
		self.s.close()

	def send_message(self, message):
		message = json.dumps(message)
		message = len(message).to_bytes(8, "little") + message.encode("ascii")

		# Send message to server with length in front 8 bytes	
		self.s.sendall(message)
	
	def receive_response(self):
		
		response_len = self.s.recv(8)
		response_len = int.from_bytes(response_len, "little")
		response = ""
		total_read = 0
		
		while True:
			data = self.s.recv(1024).decode("ascii")
			total_read += len(data)
			response += data
			if total_read == response_len:
				break
		response_json = json.loads(response)
		
		if response_json["result"] == "invalid":
			raise InvalidRequest
		elif response_json["result"] == "failure":
			raise KeyError
		elif response_json["result"] == "success":
			method = response_json["method"]

		if method == "Lookup":
			return response_json["value"]
		elif method == "Remove":
			return response_json["value"]
		elif method == "Scan":
			return response_json["pairs"]

	def insert(self, key, value):
		# Marshall parameters into a message
		message = {"method": "insert",
				   "key": key,
			       "value": value}
	
		self.send_message(message)
		self.receive_response()		
	
	def lookup(self, key):
		# Marshall parameters into a message
		message = {"method": "lookup",
				   "key": key}
		
		self.send_message(message)
		value = self.receive_response()
		return value
	
	def remove(self, key):
		# Marshall parameters into a message
		message = {"method": "remove",
				   "key": key}

		self.send_message(message)
		value = self.receive_response()
		return value

	def scan(self, regex):
		# Marshall parameters into a message
		message = {"method": "scan",
				   "regex": regex}

		self.send_message(message)
		pairs = self.receive_response()
		return pairs
