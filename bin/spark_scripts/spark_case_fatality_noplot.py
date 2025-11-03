#!/usr/bin/env python3
"""
Non-plotting variant of spark_case_fatality.py for fair performance comparison.
Computes case-fatality ratio (CFR) per country and writes top N rows to CSV.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import numpy as np
import pandas as pd
import os


def main():
    spark = SparkSession.builder.appName("CaseFatalityNoPlot").getOrCreate()

    # Read CSV and cast numeric columns
    df = spark.read.option("header", True).csv("data/data.csv")
    df = df.withColumn("cases", col("cases").cast("int")).withColumn("deaths", col("deaths").cast("int"))

    # Aggregate totals per country
    totals = df.groupBy("countriesAndTerritories").sum("cases", "deaths")

    # Convert to pandas for small result writing
    pdf = totals.toPandas()
    if pdf.empty:
        print("No data available to compute CFR.")
    else:
        pdf = pdf.rename(columns={"sum(cases)": "total_cases", "sum(deaths)": "total_deaths"})
        pdf['total_cases'] = pd.to_numeric(pdf['total_cases'], errors='coerce').fillna(0).astype(int)
        pdf['total_deaths'] = pd.to_numeric(pdf['total_deaths'], errors='coerce').fillna(0).astype(int)
        pdf['CFR'] = np.where(pdf['total_cases'] > 0, pdf['total_deaths'] / pdf['total_cases'], 0.0)

        # Avoid tiny-sample noise by applying a minimum-case filter, but keep results if empty
        MIN_CASES = 100
        filtered = pdf[pdf['total_cases'] >= MIN_CASES]
        if filtered.empty:
            filtered = pdf.copy()

        TOP_N = 20
        top = filtered.sort_values(by='CFR', ascending=False).head(TOP_N)

        out_dir = "results"
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "cfr_top_countries.csv")
        top.to_csv(out_path, index=False)
        print(f"Wrote CFR data to {out_path}")

    spark.stop()


if __name__ == '__main__':
    main()
