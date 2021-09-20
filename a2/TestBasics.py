#!/usr/bin/env python3

# Justin Pajak
# CSE 40771 - Distributed Systems
# Professor Thain
# TestBasics.py

from HashTableClient import HashTableClient
from HashTableClient import InvalidRequest
import socket, sys

def run_tests():

	# Parse command line arguments
	args = sys.argv[1:]
	if len(args) < 2:
		sys.exit(1)
	host = args[0]
	port = int(args[1])
	
	# Create HashTableClient object
	client = HashTableClient(host, port)
	
	# Connect to HashTableServer
	client.connect()

	#--------------------------------------#
	# Tests 
	#--------------------------------------#

	# Test #1: Insert value and then lookup
	print("\nTest #1: Insert value and then lookup")
	print("---------------------------------------")
	client.insert("golfer", {"name": "justin", "attributes": {"weight": 145}})
	print(client.lookup("golfer"))
	

	# Test #2: Remove value and then lookup
	print("\n\nTest #2: Remove value and then lookup")
	print("---------------------------------------")
	print(client.remove("golfer"))
	try:
		client.lookup("golfer")
	except KeyError:
		print("Caught KeyError")
	client.connect()


	# Test #3: Remove value that doesn't exist
	print("\n\nTest #3: Remove value that doesn't exist")
	print("---------------------------------------")
	try:
		client.remove("fake-key")
	except KeyError:
		print("Caught KeyError")
	client.connect()


	# Test #4: Insert values and run scan - matches
	print("\n\nTest #4: Insert values and then scan - matches")
	print("---------------------------------------")
	client.insert("ab", "val0")
	client.insert("zzzz", "val2")
	client.insert("abcccc", "val3")
	client.insert("helloabccthere", "val4")
	print(client.scan("abc*"))


	# Test #5: Insert values and run scan - no matches
	print("\n\nTest #5: Insert values and then scan - no matches")
	print("---------------------------------------")
	print(client.scan("pppppppp"))


	# Test #6: Send invalid regex to scan
	print("\n\nTest #6: Send invalid regex to scan")
	print("---------------------------------------")
	try:
		client.scan("a[")
	except InvalidRequest:
		print("Caught Invalid Request - invalid regex")
	client.connect()


	# Test #7: Send non-string key in request
	print("\n\nTest #7: Send non-string key in request")
	print("---------------------------------------")
	try:
		client.insert({}, "val5")
	except InvalidRequest:
		print("Caught Invalid Request - invalid key")
	client.connect()

	client.cleanup()

run_tests()
