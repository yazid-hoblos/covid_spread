#!/usr/bin/env python
from __future__ import print_function
import sys, io, csv

sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8') if hasattr(sys.stdin, 'buffer') else sys.stdin
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8') if hasattr(sys.stdout, 'buffer') else sys.stdout

reader = csv.reader(sys.stdin)
next(reader, None)
for row in reader:
    date = row[0]
    country = row[6]
    daily_cases = row[4]
    daily_deaths = row[5]
    print("%s\t%s\t%s\t%s" % (country, date, daily_cases, daily_deaths))

