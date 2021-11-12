#!/usr/bin/env python3

# Justin Pajak
# CSE 40771 - Distributed Systems
# Professor Thain
# TestBasics.py

from HashTableClient import InvalidRequest
from ClusterClient import ClusterClient

import sys

def run_tests():

	# Parse command line arguments
	args = sys.argv[1:]
	if len(args) < 3:
		sys.exit(1)
	name = args[0]
	n = int(args[1])
	k = int(args[2])
	if k < 1 or k > n:
		print("k must be >= 1 and <= n")
		sys.exit(1)

	# Create ClusterClient object
	client = ClusterClient(name, n, k)
	
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

	# Test #3: Remove value that doesn't exist
	print("\n\nTest #3: Remove value that doesn't exist")
	print("---------------------------------------")
	try:
		client.remove("fake-key")
	except KeyError:
		print("Caught KeyError")

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

run_tests()