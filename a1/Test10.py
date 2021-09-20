#!/usr/bin/env python3

# Test 10

# Justin Pajak
# Distributed Systems
# Run "ls -l" as a subprocess to accomplish the same thing as test 9

import time, sys, os, subprocess

total_time = 0
operations = 0
fastest = sys.maxsize
slowest = -sys.maxsize
while total_time < 2000000000:
	t0 = time.time_ns()

	# OPERATION HERE
	result = subprocess.run(["ls", "-l", "/escnfs/home/jpajak"], stdout=subprocess.PIPE)

	t1 = time.time_ns()
	diff = t1 - t0

	total_time += diff
	operations += 1
	if diff < fastest:
		fastest = diff
	if diff > slowest:
		slowest = diff	

average = float("{:.3f}".format(total_time / operations))
total_time_sec = float("{:.3f}".format(total_time / 1000000000))

print(f"Total elapsed time: {total_time_sec} s")
print(f"Number of ops: {operations}")
print(f"Average time per op: {average} ns")
print(f"Fastest single op: {fastest} ns")
print(f"Slowest single op: {slowest} ns")
