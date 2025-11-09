#!/usr/bin/env python3
"""
Simple analyzer to parse `/usr/bin/time -v` logs and build a results CSV.

It looks for files in `experiments/logs/*.time.txt` and extracts elapsed time and max RSS.
"""
import re
from pathlib import Path
import csv

LOGDIR = Path('experiments/logs')
OUT = Path('experiments/results/results_raw2.csv')
OUT.parent.mkdir(parents=True, exist_ok=True)

# Match lines like:
# "Elapsed (wall clock) time (h:mm:ss or m:ss): 0:00.73"
# or "Elapsed (wall clock) time: 0:00:01"
pattern_elapsed = re.compile(r"Elapsed.*?:\s*([0-9]+(?::[0-9]+){0,2}(?:\.[0-9]+)?)")
pattern_rss = re.compile(r"Maximum resident set size \(kbytes\)\s*:\s*(\d+)")

def parse_time_file(path):
    text = path.read_text(errors='ignore')
    elapsed = None
    rss = None
    m = pattern_elapsed.search(text)
    if m:
        elapsed = m.group(1)
        # convert H:MM:SS.sss or M:SS.sss to seconds
        parts = elapsed.split(':')
        parts = [float(p) for p in parts]
        if len(parts) == 3:
            secs = parts[0]*3600 + parts[1]*60 + parts[2]
        elif len(parts) == 2:
            secs = parts[0]*60 + parts[1]
        else:
            secs = parts[0]
    else:
        secs = None
    m2 = pattern_rss.search(text)
    if m2:
        rss = int(m2.group(1))
    return secs, rss

files = sorted(LOGDIR.glob('*.time.txt'))
rows = []
for f in files:
    # filename format: experiments/logs/<framework>_<workload>_run<id>.time.txt
    name = f.name
    parts = name.split('.')
    base = parts[0]
    toks = base.split('_')
    if len(toks) < 3:
        continue
    framework = toks[0]
    workload = toks[1]
    runid = toks[2].lstrip('run')
    secs, rss = parse_time_file(f)
    rows.append({'framework': framework, 'workload': workload, 'runid': runid, 'wall_time_sec': secs, 'max_rss_kb': rss, 'logfile': str(f)})

with OUT.open('w', newline='') as fh:
    writer = csv.DictWriter(fh, fieldnames=['framework','workload','runid','wall_time_sec','max_rss_kb','logfile'])
    writer.writeheader()
    for r in rows:
        writer.writerow(r)

print('Wrote', OUT)
