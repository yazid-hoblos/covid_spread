#!/usr/bin/env python3
"""
Non-plotting variant of spark_trends.py for fair performance comparison.
Aggregates monthly totals and writes them to CSV (no plotting).
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, month, year
import os


def main():
    spark = SparkSession.builder.appName("TrendsNoPlot").getOrCreate()

    df = spark.read.option("header", True).csv("data/data.csv")
    df = df.withColumn("cases", col("cases").cast("int")).withColumn("deaths", col("deaths").cast("int")) \
              .withColumn("date", to_date(col("dateRep"), "dd/MM/yyyy"))

    monthly = df.groupBy(year("date").alias("year"), month("date").alias("month")).sum("cases", "deaths").orderBy("year", "month")

    pdf = monthly.toPandas()
    if pdf.empty:
        print("No monthly data available.")
    else:
        pdf = pdf.rename(columns={"sum(cases)": "total_cases", "sum(deaths)": "total_deaths"})
        out_dir = "results"
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "monthly_trends.csv")
        pdf.to_csv(out_path, index=False)
        print(f"Wrote monthly trends to {out_path}")

    spark.stop()


if __name__ == '__main__':
    main()
