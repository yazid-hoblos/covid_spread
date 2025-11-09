from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count
import pandas as pd
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DailyRecordsTrend") \
    .getOrCreate()

# Load CSV with Spark
df = spark.read.csv("data/data.csv", header=True, inferSchema=True)

# Convert dateRep to proper date format and count records per date
df_with_date = df.withColumn("date_parsed", to_date(col("dateRep"), "dd/MM/yyyy"))

# Count records per date
daily_counts_spark = df_with_date.groupBy("date_parsed") \
    .agg(count("*").alias("record_count")) \
    .orderBy("date_parsed")

# Convert to Pandas for plotting
daily_counts_pd = daily_counts_spark.toPandas()
daily_counts_pd.set_index("date_parsed", inplace=True)

# Plot
plt.figure(figsize=(12,5))
daily_counts_pd['record_count'].plot(kind='line', marker='o')
plt.xlabel("Date")
plt.ylabel("Number of records")
plt.title("Number of Records per Day in the COVID-19 Dataset")
plt.grid(True)
plt.tight_layout()
# plt.savefig("figures/daily_records_trend.png", dpi=300, bbox_inches='tight')
# print("Plot saved to figures/daily_records_trend.png")
plt.show()

# Stop Spark session
spark.stop()

