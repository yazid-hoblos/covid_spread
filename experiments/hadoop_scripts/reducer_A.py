#!/usr/bin/env python3

import sys
import io

sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

current = None
total = 0

for line in sys.stdin:
    country, cases = line.strip().split("\t")
    try:
        cases = int(cases)
    except Exception:
        continue
    if current == country:
        total += cases
    else:
        if current:
            print("{}\t{}".format(current, total))
        current, total = country, cases

if current:
    print("{}\t{}".format(current, total))
