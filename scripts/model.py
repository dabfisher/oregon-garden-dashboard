# Loads weather data into DuckDB and builds analytical models

import duckdb
import pandas as pd

# Connect to a local database file
con = duckdb.connect("data/weather.db")

# Load the raw forecast csv into a table 
con.execute("""
    CREATE OR REPLACE TABLE raw_weather AS
    SELECT * FROM read_csv_auto('data/weather_raw.csv')
""")

# Load the raw historical csv into a table 
con.execute("""
    CREATE OR REPLACE TABLE temp_soil_historical AS
    SELECT * FROM read_csv_auto('data/temp_soil_historical.csv')
""")

# Load the raw historical csv into a table 
con.execute("""
    CREATE OR REPLACE TABLE sun_times AS
    SELECT * FROM read_csv_auto('data/sun_times.csv')
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

# Establish historical metrics needed for last frost
con.execute("""
    CREATE OR REPLACE TABLE avg_last_freeze_date AS
    WITH max_freeze_date AS (
        SELECT 
            city,
            YEAR(date) AS year,
            YEAR(current_date) AS current_year,
            MAX(date) AS last_freeze
        FROM temp_soil_historical
        WHERE
            MONTH(date) <= 6
            AND temp_min <= 32
        GROUP BY 
            city,
            YEAR(date),
            YEAR(current_date)
        )
        SELECT
            city,
            MAKE_DATE(ANY_VALUE(current_year), 1, 1) + CAST(AVG(DAYOFYEAR(last_freeze)) AS INTEGER) AS avg_last_freeze_all_time,
            MAKE_DATE(ANY_VALUE(current_year), 1, 1) + CAST(AVG(CASE WHEN year >= current_year - 10 THEN DAYOFYEAR(last_freeze) END) AS INTEGER) AS avg_last_freeze_ten_years,
            MAKE_DATE(ANY_VALUE(current_year), 1, 1) + CAST(AVG(CASE WHEN year >= current_year - 5 THEN DAYOFYEAR(last_freeze) END) AS INTEGER) AS avg_last_freeze_five_years
        FROM max_freeze_date
        GROUP BY city
        ORDER BY city
""")

# Establish historical metrics for daily average soil temps
con.execute("""
    CREATE OR REPLACE TABLE avg_soil_temp_daily AS
        SELECT
            city,
            MAKE_DATE(YEAR(current_date), 1, 1) + CAST(DAYOFYEAR(date) AS INTEGER) AS date,
            AVG(soil_temp_0_7cm) AS avg_shallow_soil_temp,
            AVG(soil_temp_7_to_28cm) AS avg_deep_soil_temp
        FROM temp_soil_historical
        GROUP BY city,MAKE_DATE(YEAR(current_date), 1, 1) + CAST(DAYOFYEAR(date) AS INTEGER) 
        ORDER BY city,date
""")

# Load the raw historical csv into a table 
con.execute("""
    CREATE OR REPLACE TABLE daily_data AS
    SELECT 
        tsh.date,
        tsh.city,
        sun.morning_twilight,
        sun.sunrise,
        sun.solar_noon,
        sun.sunset,
        sun.evening_twilight,
        sun.day_length,
        tsh.temp_min,
        tsh.soil_temp_0_7cm,
        tsh.soil_temp_7_to_28cm
    FROM sun_times AS sun
    JOIN temp_soil_historical AS tsh
        ON tsh.date = sun.date AND tsh.city = sun.city
""")

result = con.execute("""
    SELECT * FROM avg_last_freeze_date
""").df()
print(result)

result = con.execute("""
    SELECT * FROM avg_soil_temp_daily
""").df()
print(result)



