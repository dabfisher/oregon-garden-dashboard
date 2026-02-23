# model.py
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

# Load the raw sun_times csv into a table 
con.execute("""
    CREATE OR REPLACE TABLE sun_times AS
    SELECT * FROM read_csv_auto('data/sun_times.csv')
""")

# Load the raw plant csv into a table 
con.execute("""
    CREATE OR REPLACE TABLE plants AS
    SELECT * FROM read_csv_auto('data/plants.csv')
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
        precipitation
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

con.execute("""
    CREATE OR REPLACE TABLE avg_freeze_dates AS
    WITH max_freeze_date AS (
        SELECT 
            city,
            YEAR(date) AS year,
            MAX(date) AS last_freeze
        FROM temp_soil_historical
        WHERE MONTH(date) <= 6
          AND temp_min <= 32
        GROUP BY city, YEAR(date)
    ),
    min_freeze_date AS (
        SELECT 
            city,
            YEAR(date) AS year,
            MIN(date) AS first_freeze
        FROM temp_soil_historical
        WHERE MONTH(date) > 6
          AND temp_min <= 32
        GROUP BY city, YEAR(date)
    ),
    freeze_by_year AS (
        SELECT
            max.city,
            max.year,
            max.last_freeze,
            min.first_freeze
        FROM max_freeze_date max
        JOIN min_freeze_date min
          ON max.city = min.city
         AND max.year = min.year
    )
    SELECT
        city,
        MAKE_DATE(YEAR(current_date),1,1)
            + CAST(ROUND(AVG(DAYOFYEAR(last_freeze))) AS INTEGER)
            AS avg_last_freeze_all_time,
        MAKE_DATE(YEAR(current_date),1,1)
            + CAST(ROUND(AVG(CASE WHEN year >= YEAR(current_date)-10 
                                  THEN DAYOFYEAR(last_freeze) END)) AS INTEGER)
            AS avg_last_freeze_ten_years,
        MAKE_DATE(YEAR(current_date),1,1)
            + CAST(ROUND(AVG(CASE WHEN year >= YEAR(current_date)-5 
                                  THEN DAYOFYEAR(last_freeze) END)) AS INTEGER)
            AS avg_last_freeze_five_years,
        MAKE_DATE(YEAR(current_date),1,1)
            + CAST(ROUND(AVG(DAYOFYEAR(first_freeze))) AS INTEGER)
            AS avg_first_freeze_all_time,
        MAKE_DATE(YEAR(current_date),1,1)
            + CAST(ROUND(AVG(CASE WHEN year >= YEAR(current_date)-10 
                                  THEN DAYOFYEAR(first_freeze) END)) AS INTEGER)
            AS avg_first_freeze_ten_years,
        MAKE_DATE(YEAR(current_date),1,1)
            + CAST(ROUND(AVG(CASE WHEN year >= YEAR(current_date)-5 
                                  THEN DAYOFYEAR(first_freeze) END)) AS INTEGER)
            AS avg_first_freeze_five_years
    FROM freeze_by_year
    GROUP BY city
    ORDER BY city
""")

# Establish historical metrics for daily average air and soil temps
con.execute("""
    CREATE OR REPLACE TABLE avg_temp_daily AS
    SELECT
        city,
            MAKE_DATE(YEAR(current_date), 1, 1) + CAST(DAYOFYEAR(date) AS INTEGER) AS date,
            AVG(temp_min) AS avg_min_temp,
            AVG(temp_max) AS avg_max_temp,
            AVG(soil_temp_0_7cm) AS avg_shallow_soil_temp,
            AVG(soil_temp_7_to_28cm) AS avg_deep_soil_temp
        FROM temp_soil_historical
        GROUP BY city,MAKE_DATE(YEAR(current_date), 1, 1) + CAST(DAYOFYEAR(date) AS INTEGER) 
        ORDER BY city,date
""")

# Load the raw daily_data csv into a table 
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
        tsh.avg_min_temp,
        tsh.avg_max_temp,
        tsh.avg_shallow_soil_temp,
        tsh.avg_deep_soil_temp
    FROM sun_times AS sun
    JOIN avg_temp_daily AS tsh
        ON tsh.date = sun.date AND tsh.city = sun.city
""")

# Viable plant time period
con.execute("""
    CREATE OR REPLACE TABLE planting_gantt AS
    WITH group_params AS (
        SELECT
            p.growing_season,
            p.harvest_type,
            AVG(p.weeks_indoor_before_transplant) AS avg_weeks_indoor,
            AVG(p.days_to_maturity) AS avg_days_to_maturity,
            AVG(p.max_viable_temp_f) AS avg_max_viable_temp,
            AVG(p.min_viable_temp_f) AS avg_min_viable_temp
        FROM plants p
        GROUP BY p.growing_season, p.harvest_type
    ),
    freeze_dates AS (
        SELECT city, avg_last_freeze_all_time
        FROM avg_freeze_dates
    ),
    soil_start AS (
        SELECT
            d.city,
            g.growing_season,
            g.harvest_type,
            g.avg_weeks_indoor,
            g.avg_days_to_maturity,
            g.avg_max_viable_temp,
            MIN(d.date) AS outdoor_start
        FROM group_params g
        CROSS JOIN freeze_dates f
        JOIN daily_data d ON d.city = f.city
        WHERE d.date > f.avg_last_freeze_all_time
          AND d.avg_shallow_soil_temp > g.avg_min_viable_temp
        GROUP BY
            d.city, g.growing_season, g.harvest_type,
            g.avg_weeks_indoor, g.avg_days_to_maturity, g.avg_max_viable_temp
    ),
    temp_bounds AS (
        SELECT
            s.city,
            s.growing_season,
            s.harvest_type,
            s.outdoor_start,
            s.avg_days_to_maturity,
            s.avg_weeks_indoor,
            (s.outdoor_start - CAST(ROUND(s.avg_weeks_indoor * 7) AS INTEGER)) AS planting_start,
            MIN(CASE
                WHEN d.date >= s.outdoor_start
                AND d.avg_max_temp > s.avg_max_viable_temp
                THEN d.date
            END) AS temp_limit_date
        FROM soil_start s
        JOIN daily_data d ON d.city = s.city
        GROUP BY
            s.city, s.growing_season, s.harvest_type,
            s.outdoor_start, s.avg_days_to_maturity, s.avg_weeks_indoor,
            s.avg_max_viable_temp
    )
    SELECT
        city,
        growing_season,
        harvest_type,
        planting_start,
        outdoor_start,
        LEAST(
            outdoor_start + CAST(ROUND(avg_days_to_maturity) AS INTEGER),
            COALESCE(temp_limit_date, outdoor_start + CAST(ROUND(avg_days_to_maturity) AS INTEGER))
        ) AS planting_end,
        LEAST(
            outdoor_start + CAST(ROUND(avg_days_to_maturity) AS INTEGER),
            COALESCE(temp_limit_date, outdoor_start + CAST(ROUND(avg_days_to_maturity) AS INTEGER))
        ) - planting_start AS planting_range
    FROM temp_bounds
    ORDER BY city, growing_season, harvest_type
""")

result = con.execute("""
    SELECT * FROM planting_gantt
""").df()
print(result)