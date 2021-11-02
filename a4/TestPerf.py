#!/usr/bin/env python3

# Justin Pajak
# CSE 40771 - Distributed Systems
# Professor Thain
# TestPerf.py

from HashTableClient import HashTableClient
import sys, time

def run_tests():
	
	# Parse command line arguments
	args = sys.argv[1:]
	if len(args) != 1:
		sys.exit(1)
	project = args[0]

	# Create HashTableClient object
	client = HashTableClient(project)

	# Query ND Name Server
	client.query_ns()

	# Connect to HashTableServer
	client.connect()
	
	# Test INSERT
	ops = 3000
	total_time = 0
	for i in range(ops):
		key = "key" + str(i)
		val = "value" + str(i)
		t0 = time.time()
		client.insert(key, val)
		t1 = time.time()
		diff = t1 - t0
		total_time += diff
	throughput = float("{:.5f}".format(ops / total_time))
	latency = float("{:.5f}".format(1 / throughput))
	print("\nINSERT")
	print("-----------")
	print(f"Throughput: {throughput} ops / sec.")
	print(f"Avg. Latency: {latency} sec / op.\n\n")

	# Test LOOKUP
	total_time = 0
	for i in range(ops):
		key = "key" + str(i)
		t0 = time.time()
		try:
			client.lookup(key)
		except KeyError:
			pass
		t1 = time.time()
		diff = t1 - t0
		total_time += diff
	throughput = float("{:.5f}".format(ops / total_time))
	latency = float("{:.5f}".format(1 / throughput))
	print("LOOKUP")
	print("-----------")
	print(f"Throughput: {throughput} ops / sec.")
	print(f"Avg. Latency: {latency} sec / op.\n\n")

	# Test SCAN
	total_time = 0
	for i in range(ops):
		t0 = time.time()
		client.scan(f"{i}*")
		t1 = time.time()
		diff = t1 - t0
		total_time += diff
	throughput = float("{:.5f}".format(ops / total_time))
	latency = float("{:.5f}".format(1 / throughput))
	print("SCAN")
	print("-----------")
	print(f"Throughput: {throughput} ops / sec.")
	print(f"Avg. Latency: {latency} sec / op.\n\n")

	# Test REMOVE
	total_time = 0
	for i in range(ops):
		key = "key" + str(i)
		t0 = time.time()
		try:
			client.remove(key)
		except KeyError:
			pass
		t1 = time.time()
		diff = t1 - t0
		total_time += diff
	throughput = float("{:.5f}".format(ops / total_time))
	latency = float("{:.5f}".format(1 / throughput))
	print("REMOVE")
	print("-----------")
	print(f"Throughput: {throughput} ops / sec.")
	print(f"Avg. Latency: {latency} sec / op.\n\n")

run_tests()
