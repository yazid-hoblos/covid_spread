from pyspark.sql import SparkSession
import plotly.express as px
import geopandas as gpd
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("CovidAnalysis").getOrCreate()
df = spark.read.csv("data/data.csv", header=True, inferSchema=True)

cases_by_country = df.groupBy("countriesAndTerritories").sum("cases")
deaths_by_continent = df.groupBy("continentExp").sum("deaths")

# After Spark aggregation:
pdf = cases_by_country.toPandas()
pdf.columns = ["country", "cases"]

# Plot choropleth map
fig = px.choropleth(
    pdf,
    locations="country",
    locationmode="country names",
    color="cases",
    color_continuous_scale="Reds",
    title="COVID-19 Cases by Country"
)
fig.show()

# -------- Alternative: Plot using GeoPandas --------

# Load world geometry.
# GeoPandas 1.0 removed the packaged example datasets. Try to use the original
# helper if present; otherwise fall back to a hosted GeoJSON of country boundaries.
try:
    # older GeoPandas (<1.0)
    world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
except Exception:
    # Fallback: load a GeoJSON of countries from a maintained dataset
    # Source: https://github.com/datasets/geo-countries
    geojson_url = "https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson"
    world = gpd.read_file(geojson_url)

# Convert Spark output to Pandas
pdf = cases_by_country.toPandas()
pdf.columns = ["country", "cases"]

# Merge with geographic data
merged = world.merge(pdf, how="left", left_on="name", right_on="country")

# Plot
merged.plot(column="cases", cmap="Reds", legend=True, figsize=(12,6))
plt.title("COVID-19 Cases by Country")
plt.axis("off")
plt.show()

