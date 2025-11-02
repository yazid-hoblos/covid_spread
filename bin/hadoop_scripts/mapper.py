#!/usr/bin/env python3
import sys, io, csv

sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

reader = csv.reader(sys.stdin)
next(reader)  # skip header

for row in reader:
    country, cases = row[6], int(row[4])
    print("{}\t{}".format(country, cases))

