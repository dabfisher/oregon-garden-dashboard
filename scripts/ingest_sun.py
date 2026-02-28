# ingest_sun.py
# Pulls daily sun data for the current year
# for 6 Oregon cities from the Open-Meteo API
# and saves to data/sun_times.csv

import os
import requests
import pandas as pd
from datetime import date
import time

# Cities
cities = {
    "Portland": {"latitude": 45.5051, "longitude": -122.6750},
    "Eugene": {"latitude": 44.0521, "longitude": -123.0868},
    "Medford": {"latitude": 42.3265, "longitude": -122.8756},
    "Bend": {"latitude": 44.0582, "longitude": -121.3153},
    "Astoria": {"latitude": 46.1879, "longitude": -123.8313},
    "Hood River": {"latitude": 45.7054, "longitude": -121.5217},
}

all_cities = []

for city, coords in cities.items():
    lat = coords["latitude"]
    lon = coords["longitude"]
    start = date(date.today().year, 1, 1).isoformat()
    end = date(date.today().year, 12, 31).isoformat()

    url = (
        f"https://api.sunrisesunset.io/json"
        f"?lat={lat}&lng={lon}"
        f"&timezone=America/Los_Angeles"
        f"&date_start={start}&date_end={end}"
    )

    for attempts in range(5):
        response = requests.get(url)
        if response.status_code == 200:
            break
        print(f"{city}: failed with {response.status_code}, retrying")
        time.sleep(10)
        continue
    else:
        print(f"{city} failed all request attempts")
        continue
    
    data = response.json()

    df = pd.DataFrame(data["results"])
    df = df[["date", "nautical_twilight_begin", "sunrise", "solar_noon", "sunset", "nautical_twilight_end", "day_length"]]
    df = df.rename(columns={
        "nautical_twilight_begin": "morning_twilight",
        "nautical_twilight_end": "evening_twilight"
        })

    # Convert 12h AM/PM times to 24h HH:MM:SS so DuckDB can cast directly to TIME
    time_cols = ['morning_twilight', 'sunrise', 'solar_noon', 'sunset', 'evening_twilight']
    for col in time_cols:
        df[col] = pd.to_datetime(df[col], format='%I:%M:%S %p').dt.strftime('%H:%M:%S')

    df["city"] = city
    all_cities.append(df)

    time.sleep(60) # Rate limit
    print(f"{city} is done")

final_df = pd.concat(all_cities, ignore_index=True)

# ── Paths (always relative to this file) ─────────────────────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
final_df.to_csv(os.path.join(_ROOT, "data", "sun_times.csv"), index=False)
print("sun_times.csv saved")

