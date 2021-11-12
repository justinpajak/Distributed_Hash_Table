#!/usr/bin/env python3

# Justin Pajak
# CSE 40771 - Distributed Systems
# Professor Thain
# HashTable.py

import re

class HashTable:

	def __init__(self):
		self.table = {}

	def insert(self, key, value):
		self.table[key] = value

	def lookup(self, key):
		return self.table[key]
	
	def remove(self, key):
		value = self.table[key]
		del self.table[key]
		return value
	
	def scan(self, regex):
		results = []
		for key in self.table:
			match = re.findall(rf"{regex}", key)
			if len(match):
				results.append([key, self.table[key]])
		return results