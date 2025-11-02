# spark_top_countries.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import os

spark = SparkSession.builder.appName("TopCountries").getOrCreate()

# Read CSV and cast numeric columns
df = spark.read.option("header", True).csv("data/data.csv")
df = df.withColumn("cases", col("cases").cast("int")).withColumn("deaths", col("deaths").cast("int"))

# Aggregate totals per country and keep sorted order
totals = (df.groupBy("countriesAndTerritories")
            .sum("cases", "deaths")
            .orderBy(col("sum(cases)").desc()))

# Select top N countries for visualization to avoid overcrowding
TOP_N = 15
pdf = totals.limit(TOP_N).toPandas()

# Rename columns to friendly names
pdf = pdf.rename(columns={"sum(cases)": "total_cases", "sum(deaths)": "total_deaths"})

if pdf.empty:
    print("No data available to plot.")
else:
    # Use horizontal bar chart for better readability of country names
    plt.style.use('seaborn-whitegrid')
    fig, ax = plt.subplots(figsize=(10, 8))

    # Sort ascending so the largest bar is on top in horizontal bar chart
    pdf_sorted = pdf.sort_values(by='total_cases')
    ax.barh(pdf_sorted['countriesAndTerritories'], pdf_sorted['total_cases'], color='C2')

    # Labels, title and grid
    ax.set_xlabel('Total cases')
    ax.set_ylabel('Country')
    ax.set_title(f'Top {min(TOP_N, len(pdf))} countries by total cases')
    ax.xaxis.set_major_formatter(mtick.StrMethodFormatter('{x:,.0f}'))

    # Annotate bars with values
    # for i, (value) in enumerate(pdf_sorted['total_cases']):
    #     ax.text(value + max(pdf_sorted['total_cases']) * 0.01, pdf_sorted.index[i], f'{int(value):,}', va='center', fontsize=8)

    plt.tight_layout()
    out_dir = "plots"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "top_countries_cases.png")
    plt.savefig(out_path, bbox_inches='tight', dpi=300)
    print(f"Saved top countries plot to plots/top_countries_cases.png")

# Stop Spark session
spark.stop()