#!/usr/bin/env python3

# Test 7

# Justin Pajak
# Distributed Systems
# Make an HTTP connection to a well-knwon website, read back the HTML,
# and then close it

import time, sys, os, socket

total_time = 0
operations = 0
fastest = sys.maxsize
slowest = -sys.maxsize
HOST = "www.google.com"
PORT = 80
while total_time < 2000000000:
	t0 = time.time_ns()

	# OPERATION HERE
	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
		s.connect((HOST, PORT))
		s.sendall(b'GET / HTTP/1.0\r\nHost: www.google.com\r\n\r\n')
		result = s.recv(10000)
		while (len(result) > 0):
			result = s.recv(10000)

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
