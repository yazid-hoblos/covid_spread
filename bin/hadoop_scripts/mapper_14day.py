#!/usr/bin/env python3
import sys
import io
import csv

sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

reader = csv.reader(sys.stdin)
next(reader)  # skip header

for row in reader:
    country = row[6]
    incidence_str = row[11]  # Cumulative_number_for_14_days_of_COVID-19_cases_per_100000

    if not incidence_str:
        continue

    try:
        incidence = float(incidence_str)
        print("{}\t{}".format(country, incidence))
    except ValueError:
        continue

