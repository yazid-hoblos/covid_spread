# spark_case_fatality.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd
import os

# Start Spark
spark = SparkSession.builder.appName("CaseFatality").getOrCreate()

# Read CSV and cast numeric columns
df = spark.read.option("header", True).csv("data/data.csv")
df = df.withColumn("cases", col("cases").cast("int")).withColumn("deaths", col("deaths").cast("int"))

# Aggregate totals per country
totals = df.groupBy("countriesAndTerritories").sum("cases", "deaths")

# Convert to pandas for plotting and further small-sample manipulations
pdf = totals.toPandas()

if pdf.empty:
	print("No data available to plot CFR.")
else:
	# Rename columns to friendly names
	pdf = pdf.rename(columns={"sum(cases)": "total_cases", "sum(deaths)": "total_deaths"})

	# Compute CFR safely in pandas (avoid division by zero)
	pdf['total_cases'] = pd.to_numeric(pdf['total_cases'], errors='coerce').fillna(0).astype(int)
	pdf['total_deaths'] = pd.to_numeric(pdf['total_deaths'], errors='coerce').fillna(0).astype(int)
	pdf['CFR'] = np.where(pdf['total_cases'] > 0, pdf['total_deaths'] / pdf['total_cases'], 0.0)

	# To avoid misleading very high CFRs on tiny counts, require a min total cases threshold
	MIN_CASES = 100
	TOP_N = 20

	filtered = pdf[pdf['total_cases'] >= MIN_CASES]
	if filtered.empty:
		# If too strict, relax the threshold to include more countries
		filtered = pdf.copy()

	# Select top N countries by CFR for the visualization
	top = filtered.sort_values(by='CFR', ascending=False).head(TOP_N)

	# Prepare for horizontal bar chart
	top_sorted = top.sort_values(by='CFR', ascending=True)  # ascending for barh so largest is on top
	plt.style.use('seaborn-whitegrid')
	fig, ax = plt.subplots(figsize=(10, max(6, 0.35 * len(top_sorted))))

	ax.barh(top_sorted['countriesAndTerritories'], top_sorted['CFR'], color='C3')

	# Format x-axis as percentage
	ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=1))
	ax.set_xlabel('Case Fatality Ratio (CFR)')
	ax.set_ylabel('Country')
	ax.set_title(f'Top {min(TOP_N, len(top_sorted))} countries by CFR (min {MIN_CASES} cases)')

	# Annotate bars with percentage values
	xmax = top_sorted['CFR'].max() if not top_sorted['CFR'].empty else 0
	for i, (idx, row) in enumerate(top_sorted.iterrows()):
		value = row['CFR']
		ax.text(value + xmax * 0.01, i, f"{value:.1%}", va='center', fontsize=8)

	plt.tight_layout()
	out_dir = "plots"
	os.makedirs(out_dir, exist_ok=True)
	out_path = os.path.join(out_dir, "cfr_top_countries.png")
	plt.savefig(out_path, bbox_inches='tight', dpi=300)
	print(f"Saved CFR plot to {out_path}")

# Stop Spark session
spark.stop()