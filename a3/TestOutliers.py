#!/usr/bin/env python3

# Justin Pajak
# CSE 40771 - Distributed Systems
# Professor Thain
# TestOutliers.py

from HashTableClient import HashTableClient
import sys, time

def run_tests():

	# Parse command line arguments
	args = sys.argv[1:]
	if len(args) < 2:
		sys.exit(1)
	host = args[0]
	port = int(args[1])

	# Create HashTableClient object
	client = HashTableClient(host, port)

	# Connect to a HashTableServer
	client.connect()
	
	# Find outliers
	fastest_insert = sys.maxsize
	slowest_insert = -sys.maxsize
	fastest_remove = sys.maxsize
	slowest_remove = -sys.maxsize
	for i in range(1, 550):
		key = "key" + str(i)
		val = "val" + str(i)

		t0 = time.time_ns()
		client.insert(key, val)
		t1 = time.time_ns()
		diff = t1 - t0
		if diff < fastest_insert:
			fastest_insert = diff
		if diff > slowest_insert:
			slowest_insert = diff

		t0 = time.time_ns()
		client.remove(key)
		t1 = time.time_ns()
		diff = t1 - t0
		if diff < fastest_remove:
			fastest_remove = diff
		if diff > slowest_remove:
			slowest_remove = diff

	print(f"\nFastest Insert: {fastest_insert} ns")
	print(f"Slowest Insert: {slowest_insert} ns\n")
	print(f"Fastest Remove: {fastest_remove} ns")
	print(f"Slowest Remove: {slowest_remove} ns\n")

run_tests()
