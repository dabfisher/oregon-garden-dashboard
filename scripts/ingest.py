# ingest.py
# Pulls 30 days historical + 7 day forecast weather data
# for 6 Oregon cities from the Open-Meteo API
# and saves to data/weather_raw.csv

import requests
import json
import pandas as pd

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
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
        f"&temperature_unit=fahrenheit"
        f"&precipitation_unit=inch"
        f"&timezone=America/Los_Angeles"
        f"&past_days=30"
    )

    response = requests.get(url)
    data = response.json()

    df = pd.DataFrame({
        "date": data["daily"]["time"],
        "temp_max": data["daily"]["temperature_2m_max"],
        "temp_min": data["daily"]["temperature_2m_min"],
        "precipitation": data["daily"]["precipitation_sum"]
    })

    df["city"] = city
    all_cities.append(df)

final_df = pd.concat(all_cities, ignore_index=True)

final_df.to_csv("data/weather_raw.csv", index=False)