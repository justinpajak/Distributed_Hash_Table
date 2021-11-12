#!/usr/bin/env python3

# Justin Pajak
# CSE 40771 - Distributed Systems
# Professor Thain
# TestPerf.py

from HashTableClient import InvalidRequest
from ClusterClient import ClusterClient

import sys, time, random

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

	# Create HashTableClient object
	client = ClusterClient(name, n, k)

	# Generate data
	ops = 3000
	data = []
	for i in range(ops):
		data.append(random.randint(0, 10000))

	# Test INSERT
	total_time = 0
	for i in data:
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
	for i in data:
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
	for i in data:
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
	for i in data:
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
