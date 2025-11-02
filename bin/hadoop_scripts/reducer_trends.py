#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import io

# UTF-8 input/output
sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

current_key = None
total_cases = 0
total_deaths = 0

for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 3:
        continue
    key, cases, deaths = parts
    try:
        cases = int(cases)
        deaths = int(deaths)
    except ValueError:
        continue

    if key == current_key:
        total_cases += cases
        total_deaths += deaths
    else:
        if current_key:
            print("{}\t{}\t{}".format(current_key, total_cases, total_deaths))
        current_key = key
        total_cases = cases
        total_deaths = deaths

# Output last key
if current_key:
    print("{}\t{}\t{}".format(current_key, total_cases, total_deaths))

