# spark_daily_cumulative.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, to_date
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("DailyCumulative").getOrCreate()

df = spark.read.option("header",True).csv("data/data.csv")
df = df.withColumn("cases", col("cases").cast("int")) \
       .withColumn("deaths", col("deaths").cast("int")) \
       .withColumn("pop", col("popData2019").cast("int")) \
       .withColumn("date", to_date(col("dateRep"), "dd/MM/yyyy"))

# Daily totals per country
daily_country = df.groupBy("countriesAndTerritories","date") \
                  .sum("cases","deaths") \
                  .orderBy("countriesAndTerritories","date")

# Cumulative totals
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as _sum

window_country = Window.partitionBy("countriesAndTerritories").orderBy("date").rowsBetween(Window.unboundedPreceding,0)
daily_country = daily_country.withColumn("cumulativeCases", _sum("sum(cases)").over(window_country)) \
                             .withColumn("cumulativeDeaths", _sum("sum(deaths)").over(window_country))

# Show
daily_country.show(10)

# Save results
daily_country.toPandas().to_csv("spark_daily_cumulative.csv", index=False)

# Plot example for one country
df_plot = daily_country.filter(col("countriesAndTerritories")=="Lebanon").toPandas()
plt.plot(df_plot['date'], df_plot['cumulativeCases'], label="Cumulative Cases")
plt.plot(df_plot['date'], df_plot['cumulativeDeaths'], label="Cumulative Deaths")
plt.xticks(rotation=45)
plt.legend()
plt.title("Lebanon COVID-19 cumulative")
plt.savefig("plots/spark_daily_cumulative.png", bbox_inches='tight')
