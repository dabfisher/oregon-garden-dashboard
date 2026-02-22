import duckdb
import pandas as pd

con = duckdb.connect('data/weather.db')

result = con.execute("""
    SELECT * FROM avg_soil_temp_daily
""").df()

print(result)