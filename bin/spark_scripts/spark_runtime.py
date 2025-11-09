import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col

spark = SparkSession.builder.appName("CovidAnalysis").getOrCreate()
df = spark.read.csv("data/data.csv", header=True, inferSchema=True)

start = time.time()
df.groupBy("countriesAndTerritories").agg(spark_sum("cases").alias("total_cases")).collect()
end = time.time()

print("Execution time (seconds):", end - start)