#!/usr/bin/env python3
"""
Generate two highlighted scatter plots:
 - median wall time per (framework, workload) for small/normal/big variants
 - median memory (max_rss_kb -> MB) per (framework, workload) for small/normal/big variants

This script expects `experiments/results/results_raw.csv` to contain rows with a logfile or runid
that indicates the variant (data, data_small, data_large). It will normalize those into labels
small / normal / big and plot median values for each framework×workload, using distinct colors
for the three variants.

Usage: python experiments/plot_highlight.py
Requires: pandas, matplotlib
"""
from pathlib import Path
import sys
import os
import pandas as pd
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / 'experiments' / 'results' / 'results_raw.csv'
OUT = ROOT / 'experiments' / 'comparison'
OUT.mkdir(exist_ok=True)


def detect_variant_from_row(row):
    # try runid first
    rid = row.get('runid', '')
    if isinstance(rid, str) and '_' in rid:
        v = rid.split('_', 1)[1]
        return v
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


def normalize_variant(v: str) -> str:
    if not isinstance(v, str):
        return 'unknown'
    v = v.lower()
    if 'small' in v:
        return 'small'
    if 'large' in v or 'big' in v:
        return 'big'
    if v in ('data', 'normal', 'baseline'):
        return 'normal'
    return v


def load_and_prep(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"Results file not found: {path}")
        sys.exit(1)
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    if 'wall_time_sec' in df.columns:
        df['wall_time_sec'] = pd.to_numeric(df['wall_time_sec'], errors='coerce')
    if 'max_rss_kb' in df.columns:
        df['max_rss_kb'] = pd.to_numeric(df['max_rss_kb'], errors='coerce')
    df['raw_variant'] = df.apply(detect_variant_from_row, axis=1)
    df['variant'] = df['raw_variant'].apply(normalize_variant)
    return df


def compute_medians(df: pd.DataFrame) -> pd.DataFrame:
    # kept for compatibility but not used for plotting medians anymore
    grp = df.groupby(['framework', 'workload', 'variant']).agg(
        median_time=('wall_time_sec', 'median'),
        median_rss_kb=('max_rss_kb', 'median')
    ).reset_index()
    return grp


def plot_highlight_time(df: pd.DataFrame, out_dir: Path):
    # For each workload, create grouped boxplots: one group per framework, three boxes per group (small/normal/big)
    variant_order = ['small', 'normal', 'big']
    colors = {'small': '#1f77b4', 'normal': '#2ca02c', 'big': '#d62728'}

    workloads = sorted(df['workload'].dropna().unique())
    for w in workloads:
        sub_w = df[df['workload'] == w]
        frameworks = sorted(sub_w['framework'].dropna().unique())
        if not frameworks:
            continue

        # prepare data in order: for each framework, three variant lists
        data = []
        for fw in frameworks:
            for v in variant_order:
                vals = sub_w[(sub_w['framework'] == fw) & (sub_w['variant'] == v)]['wall_time_sec'].dropna().tolist()
                data.append(vals if len(vals) > 0 else [])

        # positions: spacing groups by 4 units, place 3 boxes per group
        n = len(frameworks)
        group_width = 4
        positions = []
        for i in range(n):
            base = i * group_width
            positions.extend([base + 0.8, base + 1.8, base + 2.8])

        plt.figure(figsize=(max(8, n * 1.5), 5))
        box = plt.boxplot(data, positions=positions, widths=0.7, patch_artist=True, showfliers=False)

        # color boxes by variant
        for i, patch in enumerate(box['boxes']):
            v = variant_order[i % 3]
            patch.set_facecolor(colors[v])

        # x ticks centered per group
        xticks = [i * group_width + 1.8 for i in range(n)]
        plt.xticks(xticks, frameworks, rotation=45)
        plt.ylabel('Wall time (s)')
        plt.title(f'Workload {w}: wall time (small / normal / big)')

        # legend
        for v in variant_order:
            plt.plot([], c=colors[v], label=v)
        plt.legend(title='variant')

        plt.grid(axis='y', alpha=0.3)
        out = out_dir / f'box_time_workload_{w}.png'
        plt.tight_layout()
        plt.savefig(out, dpi=200)
        plt.close()
        print(f"Saved {out}")


def plot_highlight_memory(df: pd.DataFrame, out_dir: Path):
    # For each workload, create grouped boxplots for memory (MB)
    variant_order = ['small', 'normal', 'big']
    colors = {'small': '#1f77b4', 'normal': '#2ca02c', 'big': '#d62728'}

    workloads = sorted(df['workload'].dropna().unique())
    for w in workloads:
        sub_w = df[df['workload'] == w]
        frameworks = sorted(sub_w['framework'].dropna().unique())
        if not frameworks:
            continue

        data = []
        for fw in frameworks:
            for v in variant_order:
                vals = (sub_w[(sub_w['framework'] == fw) & (sub_w['variant'] == v)]['max_rss_kb'] / 1024.0).dropna().tolist()
                data.append(vals if len(vals) > 0 else [])

        n = len(frameworks)
        group_width = 4
        positions = []
        for i in range(n):
            base = i * group_width
            positions.extend([base + 0.8, base + 1.8, base + 2.8])

        plt.figure(figsize=(max(8, n * 1.5), 5))
        box = plt.boxplot(data, positions=positions, widths=0.7, patch_artist=True, showfliers=False)
        for i, patch in enumerate(box['boxes']):
            v = variant_order[i % 3]
            patch.set_facecolor(colors[v])

        xticks = [i * group_width + 1.8 for i in range(n)]
        plt.xticks(xticks, frameworks, rotation=45)
        plt.ylabel('Max RSS (MB)')
        plt.title(f'Workload {w}: memory (small / normal / big)')
        for v in variant_order:
            plt.plot([], c=colors[v], label=v)
        plt.legend(title='variant')
        plt.grid(axis='y', alpha=0.3)
        out = out_dir / f'box_memory_workload_{w}.png'
        plt.tight_layout()
        plt.savefig(out, dpi=200)
        plt.close()
        print(f"Saved {out}")


def main():
    df = load_and_prep(RAW)
    # plot raw runs (no median aggregation) — highlight the three variants
    plot_highlight_time(df, OUT)
    plot_highlight_memory(df, OUT)


if __name__ == '__main__':
    main()
