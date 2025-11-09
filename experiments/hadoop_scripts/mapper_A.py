#!/usr/bin/env python3
import sys, io, csv

sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

reader = csv.reader(sys.stdin)
next(reader)  # skip header

for row in reader:
    # original pipeline expected country at column 6 and cases at column 4
    try:
        country = row[6]
        cases = int(row[4])
    except Exception:
        continue
    print("{}\t{}".format(country, cases))
