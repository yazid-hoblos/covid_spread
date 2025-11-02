"""Build an animated choropleth showing daily COVID-19 cases across Europe.

This script:
 - loads the dataset with Spark and selects only the relevant columns,
 - converts to Pandas and filters to records whose `continentExp` is
     "Europe" for efficiency,
 - normalizes country names (fuzzy matching + manual fixes) so they match
     ISO country names used by Plotly,
 - aggregates daily cases per country and builds a Plotly animated map
     limited to the European scope.
"""

import io
import pandas as pd
import os
from pyspark.sql import SparkSession
from fuzzywuzzy import process
import pycountry
import plotly.express as px

# -----------------------------------------
# 1. Spark session and data loading
# -----------------------------------------
spark = SparkSession.builder.appName("CovidAnalysisEurope").getOrCreate()

df = spark.read.csv("data/data.csv", header=True, inferSchema=True)

# -----------------------------------------
# 2. Select and clean relevant columns
# -----------------------------------------
cols = ["dateRep", "cases", "countriesAndTerritories", "continentExp"]
df = df.select(*cols)
pdf = df.toPandas()
pdf["dateRep"] = pd.to_datetime(pdf["dateRep"], format="%d/%m/%Y", errors="coerce")

# -----------------------------------------
# 3. Filter Europe first for efficiency
# -----------------------------------------
european_continents = {"Europe"}
pdf_europe = pdf[pdf["continentExp"].isin(european_continents)]

# -----------------------------------------
# 4. Build ISO country reference set
# -----------------------------------------
valid_countries = {c.name for c in pycountry.countries}
valid_countries.update({
    'Kosovo', 'Congo', 'Democratic Republic of the Congo',
    'Ivory Coast', 'Palestine', 'Taiwan', 'Vatican City',
    'Bonaire, Saint Eustatius and Saba', 'Diamond Princess'
})

# -----------------------------------------
# 5. Fuzzy match function with optional print
# -----------------------------------------

warn=[]
def normalize_country(name):
    if not isinstance(name, str):
        return None
    name_clean = name.replace("_", " ").strip()
    match, score = process.extractOne(name_clean, valid_countries)
    
    # Optional: show all matches for debugging
    # print(f"Original: {name_clean} → Match: {match} (Score: {score})")
    
    if score < 95 and name_clean not in warn:
        print(f"[WARN] Low-confidence match: {name_clean} → {match} ({score})")
        warn.append(name_clean)
        return None
    return match

pdf_europe["country_clean"] = pdf_europe["countriesAndTerritories"].apply(normalize_country)

print(f"[INFO] Total low-confidence matches: {len(warn)}")
print(f"Low-confidence countries: {warn}")

# -----------------------------------------
# 6. Manual corrections for edge cases
# -----------------------------------------
manual_fixes = {
    "Cote_dIvoire": "Ivory Coast",
    "Bonaire, Saint Eustatius and Saba": "Caribbean Netherlands",
    "Cases_on_an_international_conveyance_Japan": "Diamond Princess",
}
pdf_europe["country_clean"] = pdf_europe["country_clean"].fillna(
    pdf_europe["countriesAndTerritories"].replace(manual_fixes)
)

# -----------------------------------------
# 7. Aggregate data per day and country (Europe)
# -----------------------------------------
agg_europe = (
    pdf_europe.groupby(["dateRep", "country_clean"], as_index=False)["cases"]
    .sum()
    .sort_values("dateRep")
)

# -----------------------------------------
# 8. Coverage check
# -----------------------------------------
coverage = agg_europe["country_clean"].notnull().mean() * 100
print(f"[INFO] Country coverage in Europe after normalization: {coverage:.2f}%")

# -----------------------------------------
# 9. Dynamic geographic visualization (Europe)
# -----------------------------------------
fig = px.choropleth(
    agg_europe,
    locations="country_clean",
    locationmode="country names",
    color="cases",
    hover_name="country_clean",
    animation_frame=agg_europe["dateRep"].dt.strftime("%Y-%m-%d"),
    color_continuous_scale="Reds",
    title="COVID-19 Daily Cases Evolution in Europe",
    scope="europe"
)
fig.update_layout(
    geo=dict(showframe=False, showcoastlines=True),
    coloraxis_colorbar=dict(title="Daily Cases")
)

# Ensure output directory exists
os.makedirs("plots", exist_ok=True)

# Save interactive HTML for the Europe animation
out_html = os.path.join("plots", "europe_dynamic_map.html")
fig.write_html(out_html)
print(f"Saved interactive animation to {out_html}")

# Try to save a static PNG (requires `kaleido`)
out_png = os.path.join("plots", "europe_dynamic_map.png")
try:
    fig.write_image(out_png, scale=2)
    print(f"Saved static PNG to {out_png}")
except Exception:
    print("Static PNG not saved (kaleido may be missing). HTML file is available.")

# Display interactively when possible
fig.show()
