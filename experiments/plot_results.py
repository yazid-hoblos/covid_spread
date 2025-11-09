#!/usr/bin/env python3
"""
Alternative results plotting script.

This file intentionally does not modify any existing analyzer scripts.

Usage:
  python experiments/plot_results.py

Outputs:
 - experiments/results/summary.csv
 - plots/box_walltime_w<workload>_v<variant>.png
 - plots/time_vs_memory_scatter.png

Requires: pandas, matplotlib
"""
from pathlib import Path
import os
import sys

try:
    import pandas as pd
    import matplotlib.pyplot as plt
except Exception:
    print("Missing dependencies: pip install pandas matplotlib")
    raise


ROOT = Path(__file__).resolve().parents[1]
RESULTS_RAW = ROOT / 'experiments' / 'results' / 'results_raw.csv'
SUMMARY_CSV = ROOT / 'experiments' / 'results' / 'summary.csv'
PLOTS_DIR = ROOT / 'experiments' / 'comparison'


def load_results(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"No results file at: {path}")
        sys.exit(1)
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    if 'wall_time_sec' in df.columns:
        df['wall_time_sec'] = pd.to_numeric(df['wall_time_sec'], errors='coerce')
    if 'max_rss_kb' in df.columns:
        df['max_rss_kb'] = pd.to_numeric(df['max_rss_kb'], errors='coerce')

    def detect_variant(row):
        rid = row.get('runid', '')
        if isinstance(rid, str) and '_' in rid:
            return rid.split('_', 1)[1]
        lf = row.get('logfile', '')
        if isinstance(lf, str) and lf:
            name = os.path.basename(lf)
            for suf in ('.time.txt', '.out.txt', '.err.txt'):
                if name.endswith(suf):
                    name = name[:-len(suf)]
            parts = name.split('_')
            if parts:
                return parts[-1]
        return 'unknown'

    df['variant'] = df.apply(detect_variant, axis=1)
    return df


def write_summary(df: pd.DataFrame):
    group = ['framework', 'workload', 'variant']
    agg = df.groupby(group).agg(
        n=('wall_time_sec', 'count'),
        median_time=('wall_time_sec', 'median'),
        mean_time=('wall_time_sec', 'mean'),
        std_time=('wall_time_sec', 'std'),
        q1_time=('wall_time_sec', lambda x: x.quantile(0.25)),
        q3_time=('wall_time_sec', lambda x: x.quantile(0.75)),
        median_rss_kb=('max_rss_kb', 'median'),
        mean_rss_kb=('max_rss_kb', 'mean')
    ).reset_index()
    agg['iqr_time'] = agg['q3_time'] - agg['q1_time']
    SUMMARY_CSV.parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(SUMMARY_CSV, index=False)
    print(f"Wrote summary: {SUMMARY_CSV}")
    return agg


def plot_boxplots(df: pd.DataFrame, outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)
    workloads = sorted(df['workload'].unique())
    variants = sorted(df['variant'].unique())
    for w in workloads:
        for v in variants:
            sel = df[(df['workload'] == w) & (df['variant'] == v)]
            if sel.empty:
                continue
            frameworks = sorted(sel['framework'].unique())
            data = [sel[sel['framework'] == f]['wall_time_sec'].dropna() for f in frameworks]
            plt.figure(figsize=(7, 4.5))
            plt.boxplot(data, labels=frameworks, showfliers=False)
            plt.ylabel('Wall time (s)')
            plt.title(f'Workload {w} — variant {v}')
            plt.grid(axis='y', alpha=0.25)
            out = outdir / f'box_walltime_w{w}_v{v}.png'
            plt.tight_layout()
            plt.savefig(out, dpi=180)
            plt.close()
            print(f"Saved {out}")


def plot_time_memory(df: pd.DataFrame, outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(9, 6))
    markers = {'hadoop': 'o', 'mongo': 's', 'spark': '^'}
    for fw in sorted(df['framework'].unique()):
        sel = df[df['framework'] == fw]
        if sel.empty:
            continue
        plt.scatter(sel['wall_time_sec'], sel['max_rss_kb'] / 1024.0, label=fw, alpha=0.85, marker=markers.get(fw, 'o'))
    plt.xlabel('Wall time (s)')
    plt.ylabel('Max RSS (MB)')
    plt.title('Time vs Memory')
    plt.legend()
    plt.grid(alpha=0.25)
    out = outdir / 'time_vs_memory_scatter.png'
    plt.tight_layout()
    plt.savefig(out, dpi=180)
    plt.close()
    print(f"Saved {out}")


def main():
    df = load_results(RESULTS_RAW)
    write_summary(df)
    plot_boxplots(df, PLOTS_DIR)
    plot_time_memory(df, PLOTS_DIR)


if __name__ == '__main__':
    main()
