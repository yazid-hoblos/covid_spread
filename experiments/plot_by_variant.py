#!/usr/bin/env python3
"""
Generate two summary plots by dataset variant (small / data / large):
- time_by_variant.png: median wall time (seconds) by framework and variant
- memory_by_variant.png: median max RSS (MB) by framework and variant

Reads experiments/results/results_raw.csv and infers the dataset variant from the logfile name suffix.
"""
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

RESULTS_CSV = Path('experiments/results/results_raw2.csv')
PLOTS_DIR = Path('plots')
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# Consistent variant ordering and display labels
variant_order = ["large", "data", "small"]  # as requested: large, data, small
variant_label = {"large": "large", "data": "data", "small": "small"}

# Framework display order/colors (optional)
framework_order = ["spark", "hadoop", "mongo"]
framework_colors = {"spark": "#1f77b4", "hadoop": "#ff7f0e", "mongo": "#2ca02c"}


def infer_variant(logfile: str) -> str:
    name = os.path.basename(logfile)
    # Examples: ..._data.time.txt, ..._data_small.time.txt, ..._data_large.time.txt
    if name.endswith('_data_small.time.txt'):
        return 'small'
    if name.endswith('_data_large.time.txt'):
        return 'large'
    if name.endswith('_data.time.txt'):
        return 'data'
    # Fallback: try to parse last token before extension
    stem = name.split('.time.txt')[0]
    tail = stem.split('_')[-1]
    if tail in {'small','large','data'}:
        return tail
    return 'data'


def load_and_prepare(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Basic validation
    required = {"framework", "wall_time_sec", "max_rss_kb", "logfile"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in {path}: {missing}")

    # Infer variant from logfile
    df["variant"] = df["logfile"].apply(infer_variant)

    # Normalize framework names (lowercase)
    df["framework"] = df["framework"].str.lower()

    # Compute MB for memory for readability
    df["max_rss_mb"] = df["max_rss_kb"] / 1024.0

    return df


def aggregate_by_variant(df: pd.DataFrame) -> pd.DataFrame:
    # We aggregate across workloads by taking median per (framework, variant)
    agg = (
        df.groupby(["framework", "variant"], as_index=False)
          .agg(median_time=("wall_time_sec", "median"),
               median_rss_mb=("max_rss_mb", "median"),
               n=("wall_time_sec", "size"))
    )
    return agg


def ensure_category_order(agg: pd.DataFrame) -> pd.DataFrame:
    agg = agg.copy()
    agg["variant"] = pd.Categorical(agg["variant"], categories=variant_order, ordered=True)
    agg["framework"] = pd.Categorical(agg["framework"], categories=framework_order, ordered=True)
    return agg.sort_values(["variant", "framework"]).reset_index(drop=True)


def plot_grouped_bars(agg: pd.DataFrame, value_col: str, ylabel: str, title: str, outfile: Path):
    # Prepare bar positions
    variants = variant_order
    n_var = len(variants)
    n_fw = len(framework_order)
    width = 0.22
    x = range(n_var)

    plt.figure(figsize=(9, 4.5))

    for i, fw in enumerate(framework_order):
        vals = []
        for var in variants:
            row = agg[(agg["framework"] == fw) & (agg["variant"] == var)]
            if not row.empty:
                vals.append(float(row.iloc[0][value_col]))
            else:
                vals.append(float('nan'))
        # Shift bars for each framework
        offsets = [xi + (i - (n_fw-1)/2)*width for xi in x]
        plt.bar(offsets, vals, width=width, label=fw, color=framework_colors.get(fw))

    plt.xticks(list(x), [variant_label[v] for v in variants])
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend(title="framework", ncol=n_fw, frameon=False)
    plt.grid(axis='y', linestyle='--', alpha=0.3)
    plt.tight_layout()
    plt.savefig(outfile, dpi=200)
    print(f"Saved {outfile}")


def main():
    df = load_and_prepare(RESULTS_CSV)
    agg = aggregate_by_variant(df)
    agg = ensure_category_order(agg)

    # Time plot (seconds)
    plot_grouped_bars(
        agg,
        value_col="median_time",
        ylabel="Median wall time (s)",
        title="Wall time by dataset variant",
        outfile=PLOTS_DIR / "time_by_variant.png",
    )

    # Memory plot (MB)
    plot_grouped_bars(
        agg,
        value_col="median_rss_mb",
        ylabel="Median max RSS (MB)",
        title="Memory by dataset variant",
        outfile=PLOTS_DIR / "memory_by_variant.png",
    )


if __name__ == "__main__":
    main()
