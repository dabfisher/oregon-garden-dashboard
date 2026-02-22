import duckdb
import pandas as pd

con = duckdb.connect('data/weather.db')

result = con.execute("""
    SELECT * FROM plants
""").df()

print(result)