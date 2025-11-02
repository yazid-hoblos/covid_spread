# spark_per100k.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import pandas as pd
import numpy as np
import os

spark = SparkSession.builder.appName("Per100k").getOrCreate()
df = spark.read.option("header", True).csv("data/data.csv")
df = df.withColumn("cases", col("cases").cast("int")).withColumn("deaths", col("deaths").cast("int")) \
          .withColumn("pop", col("popData2019").cast("int"))

# Aggregate totals per country and carry population value
totals = df.groupBy("countriesAndTerritories", "pop").sum("cases", "deaths")

pdf = totals.toPandas()
if pdf.empty:
       print("No data available to plot cases per 100k.")
else:
       # Friendly column names
       pdf = pdf.rename(columns={"sum(cases)": "total_cases", "sum(deaths)": "total_deaths"})

       # Ensure numeric types and avoid division by zero
       pdf['pop'] = pd.to_numeric(pdf['pop'], errors='coerce').fillna(0).astype(int)
       pdf['total_cases'] = pd.to_numeric(pdf['total_cases'], errors='coerce').fillna(0).astype(int)
       pdf['cases_per_100k'] = np.where(pdf['pop'] > 0, pdf['total_cases'] / pdf['pop'] * 100000, 0.0)

       # Filter out tiny populations to avoid noisy rates (adjust threshold as needed)
       MIN_POP = 1000
       filtered = pdf[pdf['pop'] >= MIN_POP]
       if filtered.empty:
              filtered = pdf.copy()

       # Select top N countries by cases per 100k
       TOP_N = 15
       top = filtered.sort_values(by='cases_per_100k', ascending=False).head(TOP_N)
       top_sorted = top.sort_values(by='cases_per_100k', ascending=True)

       plt.style.use('seaborn-whitegrid')
       fig, ax = plt.subplots(figsize=(10, max(6, 0.35 * len(top_sorted))))
       ax.barh(top_sorted['countriesAndTerritories'], top_sorted['cases_per_100k'], color='C4')

       # Format x-axis with one decimal place
       ax.xaxis.set_major_formatter(mtick.StrMethodFormatter('{x:,.1f}'))
       ax.set_xlabel('Cases per 100k')
       ax.set_ylabel('Country')
       ax.set_title(f'Top {min(TOP_N, len(top_sorted))} countries by cases per 100k')

       # Annotate bars
       xmax = top_sorted['cases_per_100k'].max() if not top_sorted['cases_per_100k'].empty else 0
       for i, (idx, row) in enumerate(top_sorted.iterrows()):
              value = row['cases_per_100k']
              ax.text(value + xmax * 0.01, i, f"{value:,.1f}", va='center', fontsize=8)

       plt.tight_layout()
       out_dir = 'plots'
       os.makedirs(out_dir, exist_ok=True)
       out_path = os.path.join(out_dir, 'per100k_top_countries.png')
       plt.savefig(out_path, bbox_inches='tight', dpi=300)
       print(f"Saved per-100k plot to {out_path}")

# Stop Spark session
spark.stop()
