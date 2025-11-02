#!/usr/bin/env python3
import sys
import io
import csv

# Ensure UTF-8 encoding
sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

reader = csv.reader(sys.stdin)
next(reader)  # skip header

for row in reader:
    country = row[6]
    cases = row[4]
    deaths = row[5]
    population = row[9]

    if cases == '' or deaths == '' or population == '':
        continue

    print("{}\t{}\t{}\t{}".format(country, cases, deaths, population))

