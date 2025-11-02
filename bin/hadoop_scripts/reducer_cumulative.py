#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
from datetime import datetime

current_country = None
cumulative_cases = 0
cumulative_deaths = 0

for line in sys.stdin:
    try:
        line = line.strip()
        if not line:
            continue

        country, date, cases, deaths = line.split("\t")
        date = datetime.strptime(date, "%d/%m/%Y")  # match csv format
        cases = int(cases)
        deaths = int(deaths)

        if current_country != country:
            if current_country:
                print("%s\t%s\t%d\t%d" % (current_country, last_date.strftime("%Y-%m-%d"), cumulative_cases, cumulative_deaths))
            current_country = country
            cumulative_cases = 0
            cumulative_deaths = 0

        cumulative_cases += cases
        cumulative_deaths += deaths
        last_date = date

    except Exception:
        continue

if current_country:
    print("%s\t%s\t%d\t%d" % (current_country, last_date.strftime("%Y-%m-%d"), cumulative_cases, cumulative_deaths))

