# Oregon Gardening Dashboard

A live weather analytics pipeline that helps Oregon gardeners know when to plant and when to irrigate.

## What it does
- Pulls daily weather data for 6 Oregon cities from the Open-Meteo API
- Models planting windows based on temperature thresholds by crop
- Tracks weekly rainfall vs irrigation needs
- Displays insights through an interactive dashboard

## Tech Stack
- **Python** — data ingestion and pipeline orchestration
- **DuckDB** — local analytical database
- **SQL** — data modeling and transformation
- **Plotly Dash** — interactive dashboard

## Pipeline Architecture
```
Open-Meteo API → ingest.py → weather_raw.csv → model.py → weather.db → app.py → Dashboard
```

## How to run
1. Install dependencies: `pip install -r requirements.txt`
2. Run ingestion: `python scripts/ingest.py`
3. Build models: `python scripts/model.py`
4. Launch dashboard: `python dashboard/app.py`