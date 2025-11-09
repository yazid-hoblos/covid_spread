#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import csv
from datetime import datetime
import io

sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

reader = csv.reader(sys.stdin)
header = next(reader, None)

for row in reader:
    try:
        date_str = row[0]
        country = row[6]
        cases = int(row[4])
        deaths = int(row[5])
        date_obj = datetime.strptime(date_str, "%d/%m/%Y")
    except (ValueError, IndexError):
        continue

    # Daily
    key_daily = "{}|{}".format(country, date_obj.strftime("%Y-%m-%d"))
    print("{}\t{}\t{}".format(key_daily, cases, deaths))

    # Weekly
    iso_week = date_obj.isocalendar()[1]
    key_weekly = "{}|{}-W{:02d}".format(country, date_obj.year, iso_week)
    print("{}\t{}\t{}".format(key_weekly, cases, deaths))

    # Monthly
    key_monthly = "{}|{}-{:02d}".format(country, date_obj.year, date_obj.month)
    print("{}\t{}\t{}".format(key_monthly, cases, deaths))
