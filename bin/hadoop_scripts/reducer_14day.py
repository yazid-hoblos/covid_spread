#!/usr/bin/env python3
import sys, io

sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

current_country = None
values = []

for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 2:
        continue
    country, val = parts
    val = float(val)

    if current_country == country:
        values.append(val)
    else:
        if current_country:
            avg = sum(values) / len(values)
            mx = max(values)
            mn = min(values)
            print("{}\t{:.2f}\t{:.2f}\t{:.2f}".format(current_country, avg, mx, mn))
        current_country = country
        values = [val]

# Output last country
if current_country and values:
    avg = sum(values) / len(values)
    mx = max(values)
    mn = min(values)
    print("{}\t{:.2f}\t{:.2f}\t{:.2f}".format(current_country, avg, mx, mn))

