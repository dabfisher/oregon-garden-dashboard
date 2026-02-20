# Oregon Garden Intelligence Dashboard

import dash
from dash import dcc, html, Input, Output
import duckdb
import plotly.express as px

# Connect to database
con = duckdb.connect("data/weather.db", read_only=True)
df = con.execute("""
        SELECT *
        FROM irrigation_tracker
        ORDER BY city,week_start
    """).df()

print(df)