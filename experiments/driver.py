#!/usr/bin/env python3
"""
Driver to run experiments across frameworks/workloads/sizes.
It calls experiments/run_experiment.sh for combinations and collects logs.

Usage: python3 experiments/driver.py
"""
import subprocess
import itertools
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RUN_SCRIPT = ROOT / 'run_experiment.sh'

# Config
frameworks = ['mongo', 'hadoop', 'spark']
workloads = ['A', 'B', 'C']
datasets = {
    'small': 'data/data_small.csv',
    'medium': 'data/data_medium.csv',
    'x5': 'data/data_x5.csv'
}
repeats = 3  # change to 5 for full runs

def run_one(framework, workload, dataset_path, runid):
    cmd = [str(RUN_SCRIPT), framework, workload, dataset_path, str(runid)]
    print('Running:', ' '.join(cmd))
    subprocess.run(cmd, check=True)

def main():
    for size, ds in datasets.items():
        if not Path(ds).exists():
            print(f"WARNING: dataset {ds} not found. Skipping size {size}.")
            continue
        for framework in frameworks:
            for workload in workloads:
                for runid in range(1, repeats+1):
                    run_one(framework, workload, ds, runid)

if __name__ == '__main__':
    main()
