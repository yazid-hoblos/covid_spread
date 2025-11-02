#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import sys, io

# UTF-8 for Python 2/3
if hasattr(sys.stdin, 'buffer'):
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

current_country = None
total_cases = 0
total_deaths = 0

for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 3:
        continue
    country, cases, deaths = parts
    try:
        cases = int(cases)
        deaths = int(deaths)
    except ValueError:
        continue

    if current_country == country:
        total_cases += cases
        total_deaths += deaths
    else:
        if current_country:
            ratio = float(total_deaths) / total_cases if total_cases > 0 else 0
            print("%s\t%d\t%d\t%.6f" % (current_country, total_cases, total_deaths, ratio))
        current_country = country
        total_cases = cases
        total_deaths = deaths

# last country
if current_country:
    ratio = float(total_deaths) / total_cases if total_cases > 0 else 0
    print("%s\t%d\t%d\t%.6f" % (current_country, total_cases, total_deaths, ratio))

