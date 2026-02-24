# ingest_forecast.py
# Pulls 30 days historical + 7 day forecast weather data
# for 6 Oregon cities from the Open-Meteo API
# saves to data/weather_raw.csv and rebuilds affected DB tables

import requests
import pandas as pd
import duckdb

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
        f"&forecast_days=7"
    )

    response = requests.get(url)
    print(f"{city}: {response.status_code}")

    if response.status_code != 200:
        print(f"{city}: failed, skipping")
        continue

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
print("weather_raw.csv saved")

# Rebuild affected tables in weather.db
con = duckdb.connect("data/weather.db")

con.execute("""
    CREATE OR REPLACE TABLE raw_weather AS
    SELECT * FROM read_csv_auto('data/weather_raw.csv')
""")
print("raw_weather rebuilt")

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
print("six_weeks_weather rebuilt")

con.execute("""
    CREATE OR REPLACE TABLE irrigation_tracker AS
    WITH weekly_rain AS (
        SELECT
            city,
            DATE_TRUNC('week', date::DATE) AS week_start,
            ROUND(SUM(precipitation), 3) AS total_rainfall,
            1.0 AS rainfall_needed
        FROM raw_weather
        GROUP BY
            city,
            DATE_TRUNC('week', date::DATE),
            1.0
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
print("irrigation_tracker rebuilt")

con.close()
print("Done — all tables updated")
