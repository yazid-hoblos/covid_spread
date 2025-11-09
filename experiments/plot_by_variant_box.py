#!/usr/bin/env python3
"""
Boxplot-by-variant plots (similar style to plot_highlight):
- plots/time_by_variant_box.png: Wall time distributions grouped by dataset variant (large, data, small);
  three boxes per variant (spark, hadoop, mongo).
- plots/memory_by_variant_box.png: Max RSS (MB) distributions grouped by dataset variant; same grouping.

The script reads experiments/results/results_raw2.csv if present, else falls back to results_raw.csv.
It infers the dataset variant from the logfile suffix: _data.time.txt, _data_small.time.txt, _data_large.time.txt.
"""
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path('.').resolve()
RESULTS_CSV = ROOT / 'experiments' / 'results' / 'results_raw2.csv'
PLOTS_DIR = ROOT / 'plots'
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

variant_order = ['large', 'data', 'small']  # requested order
framework_order = ['spark', 'hadoop', 'mongo']
framework_colors = {'spark': '#1f77b4', 'hadoop': '#ff7f0e', 'mongo': '#2ca02c'}


def infer_variant_from_logfile(logfile: str) -> str:
    name = os.path.basename(str(logfile))
    if name.endswith('_data_small.time.txt'):
        return 'small'
    if name.endswith('_data_large.time.txt'):
        return 'large'
    if name.endswith('_data.time.txt'):
        return 'data'
    # Fallback: try last token before extension
    stem = name.split('.time.txt')[0]
    tail = stem.split('_')[-1]
    if tail in {'small', 'large', 'data'}:
        return tail
    return 'data'


def load_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Results file not found: {path}")
    df = pd.read_csv(path)
    # Coerce expected columns
    for c in ['wall_time_sec', 'max_rss_kb']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    # Framework lowercase
    if 'framework' in df.columns:
        df['framework'] = df['framework'].astype(str).str.lower()
    # Infer variant
    df['variant'] = df['logfile'].apply(infer_variant_from_logfile)
    # Memory MB for convenience
    df['max_rss_mb'] = df['max_rss_kb'] / 1024.0
    return df


def plot_box_by_variant(df: pd.DataFrame, value_col: str, ylabel: str, title: str, outfile: Path):
    # Build data lists in variant-major order, with three boxes per variant (spark, hadoop, mongo)
    data = []
    for var in variant_order:
        sub_v = df[df['variant'] == var]
        for fw in framework_order:
            vals = sub_v[sub_v['framework'] == fw][value_col].dropna().tolist()
            data.append(vals if len(vals) > 0 else [])

    # Positions: group per variant, three boxes per group
    n_var = len(variant_order)
    group_width = 4
    positions = []
    for i in range(n_var):
        base = i * group_width
        positions.extend([base + 0.8, base + 1.8, base + 2.8])

    plt.figure(figsize=(9, 4.5))
    box = plt.boxplot(data, positions=positions, widths=0.7, patch_artist=True, showfliers=False)

    # Color boxes by framework in repeated order
    for i, patch in enumerate(box['boxes']):
        fw = framework_order[i % len(framework_order)]
        patch.set_facecolor(framework_colors.get(fw, '#999999'))

    # X ticks centered at each variant group
    xticks = [i * group_width + 1.8 for i in range(n_var)]
    plt.xticks(xticks, variant_order)
    plt.ylabel(ylabel)
    plt.title(title)

    # Legend (dummy handles)
    for fw in framework_order:
        plt.plot([], c=framework_colors.get(fw, '#999999'), label=fw)
    plt.legend(title='framework')

    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(outfile, dpi=200)
    plt.close()
    print(f"Saved {outfile}")


def main():
    df = load_data(RESULTS_CSV)

    # TIME
    plot_box_by_variant(
        df,
        value_col='wall_time_sec',
        ylabel='Wall time (s)',
        title='Wall time by dataset variant (boxplots)',
        outfile=PLOTS_DIR / 'time_by_variant_box.png',
    )

    # MEMORY
    plot_box_by_variant(
        df,
        value_col='max_rss_mb',
        ylabel='Max RSS (MB)',
        title='Memory by dataset variant (boxplots)',
        outfile=PLOTS_DIR / 'memory_by_variant_box.png',
    )


if __name__ == '__main__':
    main()
