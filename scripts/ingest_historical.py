# ingest_historical.py
# Pulls 85 year historical air and soil temp
# for 6 Oregon cities from the Open-Meteo API
# and saves to data/temp_soil_historical.csv

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

    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date=1940-01-01&end_date={date.today()}"
        f"&daily=temperature_2m_min,temperature_2m_max,soil_temperature_0_to_7cm_mean,soil_temperature_7_to_28cm_mean"
        f"&timezone=America%2FLos_Angeles"
        f"&temperature_unit=fahrenheit"
    )

    response = requests.get(url)
    if response.status_code != 200:
        print(f"{city}: failed with {response.status_code}, skipping")
        continue
    print(response.text[:500])
    data = response.json()

    df = pd.DataFrame({
        "date": data["daily"]["time"],
        "temp_min": data["daily"]["temperature_2m_min"],
        "temp_max": data["daily"]["temperature_2m_max"],
        "soil_temp_0_7cm": data["daily"]["soil_temperature_0_to_7cm_mean"],
        "soil_temp_7_to_28cm": data["daily"]["soil_temperature_7_to_28cm_mean"]
    })

    df["city"] = city
    all_cities.append(df)

    time.sleep(60) # Call exceeding

final_df = pd.concat(all_cities, ignore_index=True)

final_df.to_csv("data/temp_soil_historical.csv", index=False)