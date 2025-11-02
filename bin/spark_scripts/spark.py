from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import warnings

warnings.filterwarnings("ignore")

spark = SparkSession.builder.appName("CovidAnalysis").getOrCreate()
df = spark.read.csv("data/data.csv", header=True, inferSchema=True)

cases_by_country = df.groupBy("countriesAndTerritories").sum("cases")
deaths_by_continent = df.groupBy("continentExp").sum("deaths")

cases_by_country.show()
deaths_by_continent.show()

pdf = cases_by_country.toPandas()

# United_States_of_America ->  USA and United_Kingdom -> UK for better visualization
pdf["countriesAndTerritories"] = pdf["countriesAndTerritories"].replace({
    "United_States_of_America": "USA",
    "United_Kingdom": "UK"
})

# Plot top 10 countries and rotate x-axis labels slightly for readability
ax = pdf.sort_values("sum(cases)", ascending=False).head(10).plot(
    x="countriesAndTerritories",
    y="sum(cases)",
    kind="bar",
    rot=35
)
ax.set_xlabel("")
plt.tight_layout()
plt.savefig("figures/top_10_countries_cases.png", dpi=500)

