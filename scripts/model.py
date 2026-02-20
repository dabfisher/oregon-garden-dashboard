# Loads weather data into DuckDB and builds analytical models

import duckdb
import pandas as pd

# Connect to a local database file
con = duckdb.connect("data/weather.db")

# Load the raw csv into a table 
con.execute("""
    CREATE OR REPLACE TABLE raw_weather AS
    SELECT * FROM read_csv_auto('data/weather_raw.csv')
""")

# Determine when temperature dips into plants that can be planted
con.execute("""
    CREATE OR REPLACE TABLE planting_window AS
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
        precipitation,
        CASE
            WHEN temp_avg >= 65 THEN 'Safe for peppers'
            WHEN temp_avg >= 60 THEN 'Safe for tomatoes'
            WHEN temp_avg >= 50 THEN 'Safe for beans/zucchini'
            WHEN temp_avg >= 40 THEN 'Safe for kale/carrots'
            ELSE 'Too cold to plant'
        END AS planting_status
    FROM daily_avg
""")

# Establish when irrigation will be needed or natural precipitation is enough
con.execute("""
    CREATE OR REPLACE TABLE irrigation_tracker AS
    WITH weekly_rain as (
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

result = con.execute("""
    SELECT * FROM irrigation_tracker
    WHERE city = 'Portland'
""").df()
print(result)