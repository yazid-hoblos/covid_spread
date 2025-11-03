#!/usr/bin/env python3
"""
Non-plotting variant of spark_top_countries.py for fair performance comparison.
Produces a CSV with the top countries by total cases.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os


def main():
    spark = SparkSession.builder.appName("TopCountriesNoPlot").getOrCreate()

    # Read CSV and cast numeric columns
    df = spark.read.option("header", True).csv("data/data.csv")
    df = df.withColumn("cases", col("cases").cast("int")).withColumn("deaths", col("deaths").cast("int"))

    # Aggregate totals per country and keep sorted order
    totals = (df.groupBy("countriesAndTerritories").sum("cases", "deaths").orderBy(col("sum(cases)").desc()))

    TOP_N = 15
    pdf = totals.limit(TOP_N).toPandas()

    # Rename columns to friendly names and write results
    if not pdf.empty:
        pdf = pdf.rename(columns={"sum(cases)": "total_cases", "sum(deaths)": "total_deaths"})

    out_dir = "results"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "top_countries_cases.csv")
    pdf.to_csv(out_path, index=False)
    print(f"Wrote top countries data to {out_path}")

    spark.stop()


if __name__ == '__main__':
    main()
