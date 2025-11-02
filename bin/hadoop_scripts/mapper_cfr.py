#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import sys, io, csv

# UTF-8 for Python 2/3 compatibility
if hasattr(sys.stdin, 'buffer'):
    sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

reader = csv.reader(sys.stdin)
header = next(reader, None)

for row in reader:
    try:
        country = row[6]
        cases = int(row[4])
        deaths = int(row[5])
        print("%s\t%d\t%d" % (country, cases, deaths))
    except (IndexError, ValueError):
        continue

