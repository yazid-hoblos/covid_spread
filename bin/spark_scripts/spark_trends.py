# spark_trends.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, month, year, weekofyear, sum as _sum
import matplotlib.pyplot as plt
import pandas as pd
import os
import matplotlib.ticker as mtick
import matplotlib.dates as mdates

spark = SparkSession.builder.appName("Trends").getOrCreate()
df = spark.read.option("header", True).csv("data/data.csv")
df = df.withColumn("cases", col("cases").cast("int")).withColumn("deaths", col("deaths").cast("int")) \
          .withColumn("date", to_date(col("dateRep"), "dd/MM/yyyy"))

# Monthly trends: group and order by year/month
monthly = df.groupBy(year("date").alias("year"), month("date").alias("month")) \
                     .sum("cases", "deaths").orderBy("year", "month")

# show more rows for quick inspection (increase from 10 to 50)
monthly.show(50)

# Convert to pandas and prepare a proper datetime x-axis
pdf = monthly.toPandas()
if pdf.empty:
       print("No monthly data available to plot.")
else:
       # Rename aggregated columns to friendly names
       pdf = pdf.rename(columns={"sum(cases)": "total_cases", "sum(deaths)": "total_deaths"})

       # Build a datetime from year/month (use first day of month)
       pdf['date'] = pd.to_datetime(pdf['year'].astype(str) + '-' + pdf['month'].astype(str) + '-01')

       # Sort by date to ensure the timeseries is ordered
       pdf = pdf.sort_values('date')

       # Set date as index for plotting and plot full series (no multi-column x argument)
       pdf.set_index('date', inplace=True)

       fig, ax = plt.subplots(figsize=(12, 6))
       ax.plot(pdf.index, pdf['total_cases'], marker='o', linestyle='-')
       ax.set_title('Monthly Cases')
       ax.set_ylabel('Total cases')
       ax.set_xlabel('Date')
       ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
       fig.autofmt_xdate(rotation=45)

       # Ensure output directory exists and save
       out_dir = 'plots'
       os.makedirs(out_dir, exist_ok=True)
       out_path = os.path.join(out_dir, 'monthly_trends.png')
       plt.tight_layout()
       plt.savefig(out_path, bbox_inches='tight', dpi=300)
       print(f"Saved monthly trends plot to {out_path}")

# Stop Spark session
spark.stop()
