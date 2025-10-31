Deploying this project to Render (interactive Streamlit app)

Overview
- This repository includes `app.py`, a Streamlit app that reads `data.csv` and provides interactive filters for continent, country, and month. It uses Plotly for plotting.

Why Streamlit
- Streamlit is simple to deploy and provides an interactive UI quickly. It's suitable for small to medium datasets. For large Spark jobs, consider pre-processing data and storing CSV/parquet for the web app to use.

Render setup
1. Create a new Web Service on Render.
2. Connect your GitHub repo (or push this code to a repo).
3. Set the build command (Render will run pip install automatically if `requirements.txt` is present):
   - Build Command: (leave blank or) pip install -r requirements.txt
4. Set the Start Command exactly as:
   - streamlit run app.py --server.port $PORT --server.address 0.0.0.0
5. Environment:
   - No special env vars required by default. If using large data, set `STREAMLIT_SERVER_HEADLESS=true` in Render environment settings.

Notes and assumptions
- This app reads `data.csv` directly. Running Spark on Render is not included because Render's web services are not designed for long-running Spark clusters. If you need Spark for heavy processing, pre-compute summaries and ship the results into a lightweight web app.
- If your `data.csv` uses a different date column name than `dateRep`, update `app.py` accordingly.

Local testing
1. Create a virtualenv and install deps:
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
2. Run locally:
   streamlit run app.py
