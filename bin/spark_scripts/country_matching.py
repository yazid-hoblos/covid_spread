# -*- coding: utf-8 -*-
import pandas as pd
from pyspark.sql import SparkSession
from fuzzywuzzy import process
import pycountry
import plotly.express as px

# -----------------------------------------
# 1. Spark session and data loading
# -----------------------------------------
spark = SparkSession.builder.appName("CovidAnalysis").getOrCreate()

# Adjust path if needed (can be HDFS or local)
df = spark.read.csv("data/data.csv", header=True, inferSchema=True)

# -----------------------------------------
# 2. Select and clean relevant columns
# -----------------------------------------
cols = ["dateRep", "cases", "countriesAndTerritories", "continentExp"]
df = df.select(*cols)

# Convert to Pandas for processing and visualization
pdf = df.toPandas()
pdf["dateRep"] = pd.to_datetime(pdf["dateRep"], format="%d/%m/%Y", errors="coerce")

# -----------------------------------------
# 3. Manual corrections for edge cases
# -----------------------------------------
manual_fixes = {
    "Cases_on_an_international_conveyance_Japan": "Diamond Princess",
    "Bonaire_Saint_Eustatius_and_Saba": "Bonaire, Sint Eustatius and Saba",
    "Congo": "Republic of the Congo",
    "Democratic_Republic_of_the_Congo": "Democratic Republic of the Congo",
    "Cote_dIvoire": "Ivory Coast",
    "Kosovo": "Republic of Kosovo",
    "Saint_Kitts_and_Nevis": "Saint Kitts and Nevis",
    "Saint_Lucia": "Saint Lucia",
    "Saint_Vincent_and_the_Grenadines": "Saint Vincent and the Grenadines",
    "Timor_Leste": "Timor-Leste",
    "United_States_of_America": "United States",
    "United_Republic_of_Tanzania": "Tanzania",
    "Viet_Nam": "Vietnam",
}

pdf["countriesAndTerritories"] = pdf["countriesAndTerritories"].replace(manual_fixes)

# -----------------------------------------
# 4. Build ISO country reference set
# -----------------------------------------
valid_countries = {c.name for c in pycountry.countries}
valid_countries.update({
    'Kosovo', 'Taiwan', 'Vatican City', 'Caribbean Netherlands', 'Diamond Princess'
})

# -----------------------------------------
# 5. Fuzzy match function
# -----------------------------------------
def normalize_country(name):
    if not isinstance(name, str):
        return None
    name_clean = name.replace("_", " ").strip()
    match, score = process.extractOne(name_clean, valid_countries)
    
    # print(f"Original: {name_clean} → Match: {match} (Score: {score})")
    
    if score < 95:
        print(f"[WARN] Unmatched: {name_clean} → Closest: {match} ({score})")
        return None
    return match

pdf["country_clean"] = pdf["countriesAndTerritories"].apply(normalize_country)

# -----------------------------------------
# 6. Aggregate data per day and country
# -----------------------------------------
agg = (
    pdf.groupby(["dateRep", "country_clean"], as_index=False)["cases"]
    .sum()
    .sort_values("dateRep")
)

# -----------------------------------------
# 7. Coverage check
# -----------------------------------------
coverage = agg["country_clean"].notnull().mean() * 100
print(f"[INFO] Country coverage after normalization: {coverage:.2f}%")

# -----------------------------------------
# 8. Dynamic geographic visualization
# -----------------------------------------
fig = px.choropleth(
    agg,
    locations="country_clean",
    locationmode="country names",
    color="cases",
    hover_name="country_clean",
    animation_frame=agg["dateRep"].dt.strftime("%Y-%m-%d"),
    color_continuous_scale="Reds",
    title="COVID-19 Daily Cases Evolution by Country",
)
fig.update_layout(
    geo=dict(showframe=False, showcoastlines=False),
    coloraxis_colorbar=dict(title="Daily Cases")
)
fig.show()
