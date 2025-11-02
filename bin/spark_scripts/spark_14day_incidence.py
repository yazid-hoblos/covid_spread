# spark_14day_incidence.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as _max, min as _min
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import pandas as pd
import numpy as np
import os

spark = SparkSession.builder.appName("Incidence14Day").getOrCreate()
df = spark.read.option("header", True).csv("data/data.csv")

# Parse the 14-day incidence column (column name may contain dashes)
col_name = "Cumulative_number_for_14_days_of_COVID-19_cases_per_100000"
df = df.withColumn("incidence14", col(col_name).cast("double"))

# Filter nulls and compute per-country statistics
df_filtered = df.filter(col("incidence14").isNotNull())
stats = df_filtered.groupBy("countriesAndTerritories") \
                       .agg(avg("incidence14").alias("avg14"),
                              _max("incidence14").alias("max14"),
                              _min("incidence14").alias("min14")) \
                       .orderBy(col("avg14").desc())

# Show top rows for quick inspection
stats.show(20)

# Convert to pandas for plotting
pdf = stats.toPandas()
if pdf.empty:
     print("No 14-day incidence data available to plot.")
else:
     # Ensure numeric and handle NaNs
     pdf['avg14'] = pd.to_numeric(pdf['avg14'], errors='coerce').fillna(0.0)
     pdf['max14'] = pd.to_numeric(pdf['max14'], errors='coerce').fillna(0.0)
     pdf['min14'] = pd.to_numeric(pdf['min14'], errors='coerce').fillna(0.0)

     # Select top N countries by average 14-day incidence
     TOP_N = 20
     top = pdf.head(TOP_N)
     top_sorted = top.sort_values(by='avg14', ascending=True)

     plt.style.use('seaborn-whitegrid')
     fig, ax = plt.subplots(figsize=(10, max(6, 0.35 * len(top_sorted))))
     ax.barh(top_sorted['countriesAndTerritories'], top_sorted['avg14'], color='C5')

     # Format x-axis with one decimal place
     ax.xaxis.set_major_formatter(mtick.StrMethodFormatter('{x:,.1f}'))
     ax.set_xlabel('Average 14-day incidence (per 100k)')
     ax.set_ylabel('Country')
     ax.set_title(f'Top {min(TOP_N, len(top_sorted))} countries by average 14-day incidence')

     # Annotate bars
     xmax = top_sorted['avg14'].max() if not top_sorted['avg14'].empty else 0
     for i, (idx, row) in enumerate(top_sorted.iterrows()):
          value = row['avg14']
          ax.text(value + xmax * 0.01, i, f"{value:,.1f}", va='center', fontsize=8)

     plt.tight_layout()
     out_dir = 'plots'
     os.makedirs(out_dir, exist_ok=True)
     out_path = os.path.join(out_dir, '14day_incidence_top.png')
     plt.savefig(out_path, bbox_inches='tight', dpi=300)
     print(f"Saved 14-day incidence plot to {out_path}")

# Stop Spark session
spark.stop()