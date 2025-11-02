import streamlit as st
import pandas as pd
import plotly.express as px
import pycountry
from datetime import datetime
import os


@st.cache_data
def load_data(path="data.csv"):
    df = pd.read_csv(path, parse_dates=["dateRep"] , dayfirst=True)
    # normalize column names if needed
    df.columns = [c.strip() for c in df.columns]
    # ensure a month column for filtering
    if "dateRep" in df.columns:
        df["month"] = df["dateRep"].dt.to_period("M").astype(str)
    return df


def main():
    st.set_page_config(layout="wide", page_title="COVID Analysis")
    st.title("Interactive COVID Analysis")

    df = load_data("data.csv")

    # Sidebar filters
    st.sidebar.header("Filters")
    continents = [None] + sorted(df["continentExp"].dropna().unique().tolist())
    continent = st.sidebar.selectbox("Continent", continents, index=0)

    # Filter by continent if selected
    df_f = df.copy()
    if continent:
        df_f = df_f[df_f["continentExp"] == continent]

    # Helper: determine appropriate map view (scope / center / zoom) for a selected continent
    def _view_for_continent(cont):
        # Returns a dict with keys: scope (for px.choropleth) and center/zoom for mapbox
        if not cont:
            return {"scope": None, "center": {"lat": 10, "lon": 0}, "zoom": 0.6}
        lookup = {
            "Europe": {"scope": "europe", "center": {"lat": 54.0, "lon": 15.0}, "zoom": 3.0},
            "Asia": {"scope": "asia", "center": {"lat": 34.0, "lon": 100.0}, "zoom": 2.0},
            "Africa": {"scope": "africa", "center": {"lat": 2.0, "lon": 20.0}, "zoom": 1.5},
            "North America": {"scope": "north america", "center": {"lat": 45.0, "lon": -100.0}, "zoom": 2.0},
            "South America": {"scope": "south america", "center": {"lat": -15.0, "lon": -60.0}, "zoom": 2.0},
            "Oceania": {"scope": "oceania", "center": {"lat": -25.0, "lon": 140.0}, "zoom": 2.8},
            "Australia": {"scope": "oceania", "center": {"lat": -25.0, "lon": 134.0}, "zoom": 3.0},
        }
        return lookup.get(cont, {"scope": None, "center": {"lat": 10, "lon": 0}, "zoom": 0.6})


    months = sorted(df_f["month"].dropna().unique().tolist())
    months = ["All"] + months
    month = st.sidebar.selectbox("Month (YYYY-MM)", months, index=0)
    if month != "All":
        df_f = df_f[df_f["month"] == month]

    countries = sorted(df_f["countriesAndTerritories"].dropna().unique().tolist())
    country = st.sidebar.selectbox("Country (optional)", ["All"] + countries, index=0)
    if country != "All":
        df_f = df_f[df_f["countriesAndTerritories"] == country]

    st.sidebar.markdown("---")
    st.sidebar.write(f"Rows: {len(df_f)}")

    # Aggregations
    cases_by_country = df_f.groupby("countriesAndTerritories")["cases"].sum().reset_index()
    deaths_by_country = df_f.groupby("countriesAndTerritories")["deaths"].sum().reset_index()

    # Normalize a human-friendly country name for plotting (replace underscores)
    cases_by_country["country"] = (
        cases_by_country["countriesAndTerritories"].astype(str).str.replace("_", " ")
    )

    # Top 10 bar chart
    top10 = cases_by_country.sort_values("cases", ascending=False).head(10)
    fig_bar = px.bar(top10, x="country", y="cases", title="Top 10 Countries by Cases")
    fig_bar.update_layout(xaxis_tickangle=35)

    # Time series (if not filtering to single month)
    fig_ts = None
    if "dateRep" in df.columns and (month == "All"):
        ts = df_f.groupby("dateRep")["cases"].sum().reset_index().sort_values("dateRep")
        fig_ts = px.line(ts, x="dateRep", y="cases", title="Daily Cases")

    # Visualization selector
    viz = st.sidebar.selectbox("Visualization", ["Bar chart", "Time series", "Choropleth map"]) 

    # Layout
    col1, col2 = st.columns([2, 1])
    with col1:
        if viz == "Bar chart":
            st.plotly_chart(fig_bar, use_container_width=True)
            if fig_ts:
                st.plotly_chart(fig_ts, use_container_width=True)

        elif viz == "Time series":
            if fig_ts:
                st.plotly_chart(fig_ts, use_container_width=True)
            else:
                st.info("Time series is available only when Month = All and dateRep column exists.")

        elif viz == "Choropleth map":
            # Prepare map data using country names and map to ISO-3 codes for robustness
            map_df = cases_by_country[["country", "cases"]].copy()
            map_df = map_df.rename(columns={"country": "country_name", "cases": "cases"})

            @st.cache_data
            def name_to_iso3(name: str):
                if not isinstance(name, str) or name.strip() == "":
                    return None
                # common cleanup
                nm = name.replace("_", " ").strip()
                # small alias map for known differences
                aliases = {
                    "Russia": "Russian Federation",
                    "Czechia": "Czech Republic",
                    "South Korea": "Korea, Republic of",
                    "North Korea": "Korea, Democratic People's Republic of",
                    "Ivory Coast": "Côte d'Ivoire",
                    "Syria": "Syrian Arab Republic",
                    "Laos": "Lao People's Democratic Republic",
                    "Bolivia": "Bolivia (Plurinational State of)",
                    "Venezuela": "Venezuela (Bolivarian Republic of)",
                    "Moldova": "Moldova, Republic of",
                    "Tanzania": "Tanzania, United Republic of",
                    "United States": "United States of America",
                    "UK": "United Kingdom",
                }
                if nm in aliases:
                    nm_try = aliases[nm]
                else:
                    nm_try = nm
                try:
                    c = pycountry.countries.lookup(nm_try)
                    return c.alpha_3
                except Exception:
                    # try fuzzy alternatives: remove content in parentheses
                    import re

                    nm2 = re.sub(r"\s*\(.*\)", "", nm_try).strip()
                    try:
                        c = pycountry.countries.lookup(nm2)
                        return c.alpha_3
                    except Exception:
                        return None

            # build iso mapping column
            map_df["iso_a3"] = map_df["country_name"].apply(name_to_iso3)
            missing = map_df[map_df["iso_a3"].isna()]
            if len(missing) > 0:
                st.sidebar.warning(f"{len(missing)} countries could not be mapped to ISO3; they will be omitted from the map.")

            map_df = map_df.dropna(subset=["iso_a3"])  # Plotly needs ISO-3 for reliable plotting

            # Map mode: static for current filter, animated across months, or slider
            map_mode = st.sidebar.selectbox("Map mode", ["Static (current filter)", "Animated by month", "Month slider"])
            # Map style: offer several nicer basemaps; prefer Mapbox styles when token is present
            has_mapbox = bool(os.getenv("MAPBOX_TOKEN"))
            # Provide a palette of nicer styles (Plotly default + OSM + Carto + Stamen)
            map_style_options = ["Plotly (default)", "open-street-map", "carto-positron", "carto-darkmatter", "stamen-terrain"]
            if has_mapbox:
                # allow Mapbox proprietary styles as well
                map_style_options += ["streets", "satellite-streets"]
            map_style = st.sidebar.selectbox("Map style", map_style_options, index=0)

            if map_mode == "Animated by month":
                # need monthly breakdown across full df_f
                monthly = (
                    df_f.assign(country=df_f["countriesAndTerritories"].astype(str).str.replace("_", " "))
                    .groupby(["month", "country"])["cases"]
                    .sum()
                    .reset_index()
                )
                monthly["iso_a3"] = monthly["country"].apply(name_to_iso3)
                monthly = monthly.dropna(subset=["iso_a3"])
                try:
                    # choose a Mapbox style param; allow mapbox:// style names for some choices
                    mapbox_style_param = map_style
                    if map_style in ("streets", "satellite-streets"):
                        mapbox_style_param = f"mapbox://styles/mapbox/{map_style}-v11"

                    # determine view (scope/center/zoom) for the selected continent
                    view = _view_for_continent(continent)
                    if has_mapbox:
                        px.set_mapbox_token(os.getenv("MAPBOX_TOKEN"))
                        fig_map = px.choropleth_mapbox(
                            monthly,
                            locations="iso_a3",
                            color="cases",
                            hover_name="country",
                            animation_frame="month",
                            color_continuous_scale="OrRd",
                            title="COVID-19 Cases by Country (animated by month)",
                            labels={"cases": "cases"},
                            mapbox_style=mapbox_style_param,
                            center=view.get("center", {"lat": 10, "lon": 0}),
                            zoom=view.get("zoom", 0.6),
                        )
                        fig_map.update_traces(marker_line_width=0.5, marker_line_color="white")
                    else:
                        scope = view.get("scope") if continent else None
                        if scope:
                            fig_map = px.choropleth(
                                monthly,
                                locations="iso_a3",
                                color="cases",
                                hover_name="country",
                                animation_frame="month",
                                color_continuous_scale="OrRd",
                                title="COVID-19 Cases by Country (animated by month)",
                                labels={"cases": "cases"},
                                scope=scope,
                            )
                        else:
                            fig_map = px.choropleth(
                                monthly,
                                locations="iso_a3",
                                color="cases",
                                hover_name="country",
                                animation_frame="month",
                                color_continuous_scale="OrRd",
                                title="COVID-19 Cases by Country (animated by month)",
                                labels={"cases": "cases"},
                            )
                        fig_map.update_geos(showcountries=True)

                    fig_map.update_layout(margin={"r":0,"t":30,"l":0,"b":0}, coloraxis_colorbar=dict(title="Cases"))
                    try:
                        fig_map.update_traces(hovertemplate="%{hovertext}<br>Cases: %{z:,}")
                    except Exception:
                        pass
                    st.plotly_chart(fig_map, use_container_width=True)
                except Exception as e:
                    st.error(f"Could not render animated map: {e}")

            elif map_mode == "Month slider":
                months_all = sorted(df_f["month"].dropna().unique().tolist())
                if not months_all:
                    st.info("No monthly data available for slider mode.")
                else:
                    month_choice = st.sidebar.select_slider("Select month", options=months_all, value=months_all[-1])
                    month_df = (
                        df_f[df_f["month"] == month_choice]
                        .groupby(df_f["countriesAndTerritories"].astype(str).str.replace("_", " "))["cases"]
                        .sum()
                        .reset_index()
                    )
                    month_df.columns = ["country_name", "cases"]
                    month_df["iso_a3"] = month_df["country_name"].apply(name_to_iso3)
                    month_df = month_df.dropna(subset=["iso_a3"])
                    try:
                        mapbox_style_param = map_style
                        if map_style in ("streets", "satellite-streets"):
                            mapbox_style_param = f"mapbox://styles/mapbox/{map_style}-v11"
                        view = _view_for_continent(continent)
                        if has_mapbox:
                            px.set_mapbox_token(os.getenv("MAPBOX_TOKEN"))
                            fig_map = px.choropleth_mapbox(
                                month_df,
                                locations="iso_a3",
                                color="cases",
                                hover_name="country_name",
                                color_continuous_scale="OrRd",
                                title=f"COVID-19 Cases by Country — {month_choice}",
                                mapbox_style=mapbox_style_param,
                                center=view.get("center", {"lat": 10, "lon": 0}),
                                zoom=view.get("zoom", 0.6),
                            )
                            fig_map.update_traces(marker_line_width=0.5, marker_line_color="white")
                        else:
                            scope = view.get("scope") if continent else None
                            if scope:
                                fig_map = px.choropleth(
                                    month_df,
                                    locations="iso_a3",
                                    color="cases",
                                    hover_name="country_name",
                                    color_continuous_scale="OrRd",
                                    title=f"COVID-19 Cases by Country — {month_choice}",
                                    scope=scope,
                                )
                            else:
                                fig_map = px.choropleth(
                                    month_df,
                                    locations="iso_a3",
                                    color="cases",
                                    hover_name="country_name",
                                    color_continuous_scale="OrRd",
                                    title=f"COVID-19 Cases by Country — {month_choice}",
                                )
                            fig_map.update_geos(showcountries=True)
                        fig_map.update_layout(margin={"r":0,"t":30,"l":0,"b":0}, coloraxis_colorbar=dict(title="Cases"))
                        try:
                            fig_map.update_traces(hovertemplate="%{hovertext}<br>Cases: %{z:,}")
                        except Exception:
                            pass
                        st.plotly_chart(fig_map, use_container_width=True)
                    except Exception as e:
                        st.error(f"Could not render month map: {e}")

            else:
                # Static map for current filter
                try:
                    mapbox_style_param = map_style
                    if map_style in ("streets", "satellite-streets"):
                        mapbox_style_param = f"mapbox://styles/mapbox/{map_style}-v11"
                    view = _view_for_continent(continent)
                    if has_mapbox:
                        px.set_mapbox_token(os.getenv("MAPBOX_TOKEN"))
                        fig_map = px.choropleth_mapbox(
                            map_df,
                            locations="iso_a3",
                            color="cases",
                            hover_name="country_name",
                            color_continuous_scale="OrRd",
                            title="COVID-19 Cases by Country",
                            mapbox_style=mapbox_style_param,
                            center=view.get("center", {"lat": 10, "lon": 0}),
                            zoom=view.get("zoom", 0.6),
                        )
                        fig_map.update_traces(marker_line_width=0.5, marker_line_color="white")
                    else:
                        scope = view.get("scope") if continent else None
                        if scope:
                            fig_map = px.choropleth(
                                map_df,
                                locations="iso_a3",
                                color="cases",
                                hover_name="country_name",
                                color_continuous_scale="OrRd",
                                title="COVID-19 Cases by Country",
                                scope=scope,
                            )
                        else:
                            fig_map = px.choropleth(
                                map_df,
                                locations="iso_a3",
                                color="cases",
                                hover_name="country_name",
                                color_continuous_scale="OrRd",
                                title="COVID-19 Cases by Country",
                            )
                        fig_map.update_geos(showcountries=True)
                    fig_map.update_layout(margin={"r":0,"t":30,"l":0,"b":0}, coloraxis_colorbar=dict(title="Cases"))
                    try:
                        fig_map.update_traces(hovertemplate="%{hovertext}<br>Cases: %{z:,}")
                    except Exception:
                        pass
                    st.plotly_chart(fig_map, use_container_width=True)
                except Exception as e:
                    st.error(f"Could not render choropleth map: {e}")

    with col2:
        # Show a compact table with only country and cases (no redundant columns)
        display_df = (
            cases_by_country[["country", "cases"]]
            .sort_values("cases", ascending=False)
            .head(50)
            .reset_index(drop=True)
        )
        display_df.columns = ["country", "cases"]
        st.dataframe(display_df)


if __name__ == "__main__":
    main()
