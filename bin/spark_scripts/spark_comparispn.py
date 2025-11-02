# spark_comparison.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("Comparison").getOrCreate()
df = spark.read.option("header",True).csv("data/data.csv")
df = df.withColumn("cases", col("cases").cast("int")).withColumn("deaths", col("deaths").cast("int"))

continent_totals = df.groupBy("continentExp").sum("cases","deaths").orderBy("continentExp")
continent_totals.show()
continent_totals.toPandas().plot(kind='bar', x='continentExp', y='sum(cases)', title="Cases by Continent")
plt.savefig("plots/continent_comparison.png", bbox_inches='tight')
