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
	if len(args) < 2:
		sys.exit(1)
	host = args[0]
	port = int(args[1])

	# Create HashTableClient object
	client = HashTableClient(host, port)

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
		client.lookup(key)
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
		client.remove(key)
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
