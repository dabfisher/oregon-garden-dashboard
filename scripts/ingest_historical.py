# ingest_historical.py
# Pulls 85 year historical air and soil temp
# for 6 Oregon cities from the Open-Meteo API
# and saves to data/temp_soil_historical.csv

import os
import requests
import pandas as pd
import logging
from datetime import date
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Paths (always relative to this file) ─────────────────────────────────────
_HERE    = os.path.dirname(os.path.abspath(__file__))
_ROOT    = os.path.dirname(_HERE)
CSV_PATH = os.path.join(_ROOT, "data", "temp_soil_historical.csv")
TIMEOUT  = 30  # longer timeout — large historical payload

# ── Cities ────────────────────────────────────────────────────────────────────
cities = {
    "Portland":   {"latitude": 45.5051, "longitude": -122.6750},
    "Eugene":     {"latitude": 44.0521, "longitude": -123.0868},
    "Medford":    {"latitude": 42.3265, "longitude": -122.8756},
    "Bend":       {"latitude": 44.0582, "longitude": -121.3153},
    "Astoria":    {"latitude": 46.1879, "longitude": -123.8313},
    "Hood River": {"latitude": 45.7054, "longitude": -121.5217},
}

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ── Session with retry ────────────────────────────────────────────────────────
def get_session():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def run_historical_ingest():
    logging.info("Starting historical ingest")

    session = get_session()
    all_cities = []

    for city, coords in cities.items():
        lat = coords["latitude"]
        lon = coords["longitude"]

        url = (
            f"https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={lat}&longitude={lon}"
            f"&start_date=1940-01-01&end_date={date.today()}"
            f"&daily=temperature_2m_min,temperature_2m_max"
            f",soil_temperature_0_to_7cm_mean,soil_temperature_7_to_28cm_mean"
            f"&timezone=America%2FLos_Angeles"
            f"&temperature_unit=fahrenheit"
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
                "date":                data["daily"]["time"],
                "temp_min":            data["daily"]["temperature_2m_min"],
                "temp_max":            data["daily"]["temperature_2m_max"],
                "soil_temp_0_7cm":     data["daily"]["soil_temperature_0_to_7cm_mean"],
                "soil_temp_7_to_28cm": data["daily"]["soil_temperature_7_to_28cm_mean"],
            })
            df["city"] = city
            all_cities.append(df)
            logging.info(f"{city} success — {len(df)} rows")
        except Exception as e:
            logging.error(f"{city} dataframe build failed: {e}")
            continue

        time.sleep(60)  # API rate limit

    if not all_cities:
        logging.error("All city fetches failed — aborting")
        return

    final_df = pd.concat(all_cities, ignore_index=True)

    temp_csv = CSV_PATH + ".tmp"
    final_df.to_csv(temp_csv, index=False)
    os.replace(temp_csv, CSV_PATH)
    logging.info(f"temp_soil_historical.csv saved → {CSV_PATH} ({len(final_df)} rows)")


if __name__ == "__main__":
    run_historical_ingest()
