"""Generate an animated choropleth of global COVID-19 daily cases.

This script reads the CSV dataset using Spark, aggregates daily cases by
country, converts the small result to a Pandas DataFrame and builds an
interactive Plotly choropleth animation. The animation is saved as an
HTML file in the `plots/` directory so it can be opened locally in a browser.
"""

from pyspark.sql import SparkSession
import plotly.express as px
import pandas as pd
import os

# Start Spark
spark = SparkSession.builder.appName("CovidEvolution").getOrCreate()

# Load data
df = spark.read.csv("data/data.csv", header=True, inferSchema=True)

# Ensure correct column names (adapt to your dataset)
# Common columns: dateRep, countriesAndTerritories, cases
df = df.withColumnRenamed("countriesAndTerritories", "country")

# Aggregate cases per day per country
daily_cases = (
    df.groupBy("country", "dateRep")
      .sum("cases")
      .withColumnRenamed("sum(cases)", "cases")
      .orderBy("dateRep")
)

# Convert to Pandas for visualization
pdf = daily_cases.toPandas()

# Optional: format date
pdf["dateRep"] = pd.to_datetime(pdf["dateRep"], errors="coerce")

# Sort by date
pdf = pdf.sort_values("dateRep")

# Plot dynamic world map (Plotly animation)
fig = px.choropleth(
    pdf,
    locations="country",
    locationmode="country names",
    color="cases",
    hover_name="country",
    animation_frame=pdf["dateRep"].dt.strftime("%Y-%m-%d"),
    color_continuous_scale="Reds",
    range_color=[0, pdf["cases"].max()],
    title="COVID-19 Case Evolution Over Time"
)

# Ensure output directory exists and save results
os.makedirs("plots", exist_ok=True)

# Save interactive HTML (recommended for animations)
out_html = os.path.join("plots", "dynamic_map.html")
fig.write_html(out_html)
print(f"Saved interactive animation to {out_html}")

# Attempt to save a static PNG thumbnail (requires the `kaleido` package)
out_png = os.path.join("plots", "dynamic_map.png")
try:
    fig.write_image(out_png, scale=2)
    print(f"Saved static PNG to {out_png}")
except Exception:
    print("Static PNG not saved (kaleido may be missing). HTML file is available.")

# Optionally display in interactive session
fig.show()
