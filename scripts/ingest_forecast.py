# ingest_forecast.py
# Pulls 30 days historical + 7 day forecast weather data
# for 6 Oregon cities from the Open-Meteo API
# saves to data/weather_raw.csv and rebuilds affected DB tables

import requests
import pandas as pd
import duckdb
import logging
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

## Config
DB_PATH = "data/weather.db"
CSV_PATH = "data/weather_raw.csv"
TIMEOUT = 15

# Cities
cities = {
    "Portland": {"latitude": 45.5051, "longitude": -122.6750},
    "Eugene": {"latitude": 44.0521, "longitude": -123.0868},
    "Medford": {"latitude": 42.3265, "longitude": -122.8756},
    "Bend": {"latitude": 44.0582, "longitude": -121.3153},
    "Astoria": {"latitude": 46.1879, "longitude": -123.8313},
    "Hood River": {"latitude": 45.7054, "longitude": -121.5217},
}

## Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

## Session retry
def get_session():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    return session

def run_forecast_ingest():

    logging.info("Starting forecast ingest")

    session = get_session()
    all_cities = []

    for city, coords in cities.items():
        lat = coords["latitude"]
        lon = coords["longitude"]

        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
            f"&temperature_unit=fahrenheit"
            f"&precipitation_unit=inch"
            f"&timezone=America/Los_Angeles"
            f"&past_days=30"
            f"&forecast_days=7"
        )

        try:
            response = session.get(url, timeout=TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"{city} request failed: {e}")
            continue

        try:
            data = response.json()
            daily = data["daily"]
        except Exception as e:
            logging.error(f"{city} malformed response: {e}")
            continue

        try:
            df = pd.DataFrame({
                "date": daily["time"],
                "temp_max": daily["temperature_2m_max"],
                "temp_min": daily["temperature_2m_min"],
                "precipitation": daily["precipitation_sum"]
            })
            df["city"] = city
            all_cities.append(df)
            logging.info(f"{city} success")
        except Exception as e:
            logging.error(f"{city} dataframe build failed: {e}")
            continue

    if not all_cities:
        logging.error("All city fetches failed — aborting ingest safely")
        return

    final_df = pd.concat(all_cities, ignore_index=True)

    # Write CSV atomically
    temp_csv = CSV_PATH + ".tmp"
    final_df.to_csv(temp_csv, index=False)
    import os
    os.replace(temp_csv, CSV_PATH)
    logging.info("weather_raw.csv updated")

## Rebuild DB tables
    try:
        con = duckdb.connect(DB_PATH)

        con.execute("""
            CREATE OR REPLACE TABLE raw_weather AS
            SELECT * FROM read_csv_auto(?)
        """, [CSV_PATH])

        con.execute("""
            CREATE OR REPLACE TABLE six_weeks_weather AS
            WITH daily_avg AS (
                SELECT
                    city,
                    date,
                    temp_max,
                    temp_min,
                    ROUND((temp_max + temp_min) / 2, 1) AS temp_avg,
                    precipitation
                FROM raw_weather
            )
            SELECT
                city,
                date,
                temp_avg,
                temp_max,
                temp_min,
                precipitation
            FROM daily_avg
        """)

        con.execute("""
            CREATE OR REPLACE TABLE irrigation_tracker AS
            WITH weekly_rain AS (
                SELECT
                    city,
                    DATE_TRUNC('week', date::DATE) AS week_start,
                    ROUND(SUM(precipitation), 3) AS total_rainfall,
                    1.0 AS rainfall_needed
                FROM six_weeks_weather
                GROUP BY city, DATE_TRUNC('week', date::DATE)
            )
            SELECT
                city,
                week_start,
                total_rainfall,
                rainfall_needed,
                ROUND(total_rainfall - rainfall_needed, 3) AS surplus_deficit,
                CASE
                    WHEN total_rainfall >= rainfall_needed THEN 'No irrigation needed'
                    WHEN total_rainfall >= 0.5 THEN 'Light irrigation needed'
                    ELSE 'Irrigation needed'
                END AS irrigation_status
            FROM weekly_rain
            ORDER BY city, week_start
        """)

        con.close()
        logging.info("DuckDB tables rebuilt successfully")

    except Exception as e:
        logging.error(f"DuckDB update failed: {e}")
        return

    logging.info("Forecast ingest complete")