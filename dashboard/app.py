# Oregon Garden Intelligence Dashboard

import dash
from dash import dcc, html, Input, Output
import duckdb
import plotly.express as px

# Initialize the app
app = dash.Dash(__name__)

# Layout
app.layout = html.Div([
    html.H1("Oregon Gardening Dashboard"),
    html.P("Live Insights for Garden Management"),
    dcc.Dropdown(
        id="city-dropdown",
        options=[
            {"label": "Portland", "value": "Portland"},
            {"label": "Eugene", "value": "Eugene"},
            {"label": "Medford", "value": "Medford"},
            {"label": "Bend", "value": "Bend"},
            {"label": "Astoria", "value": "Astoria"},
            {"label": "Hood River", "value": "Hood River"},
        ],
        value="Portland",
        clearable=False
    ),
    dcc.Graph(id="temperature-chart"),
    dcc.Graph(id="irrigation-chart")
])

@app.callback(
    Output("temperature-chart", "figure"),
    Input("city-dropdown", "value")
)
def update_temperature_chart(selected_city):
    # Connect to database
    con = duckdb.connect("data/weather.db", read_only=True)
    # Run query
    df = con.execute("""
        SELECT date, temp_max, temp_min, planting_status
        FROM planting_window
        WHERE city = ?
        ORDER BY date
    """,[selected_city]).df()
    con.close() # Close connection
    # Build Line Chart
    fig = px.line(
        df,
        x="date",
        y=["temp_max","temp_min"],
        title=f"Temperature - {selected_city}",
        labels={"value": "Temperature (°F)", "date": "Date"}
    )

    return fig

@app.callback(
    Output("irrigation-chart", "figure"),
    Input("city-dropdown", "value")
)
def update_irrigation_chart(selected_city):
    # Connect to database
    con = duckdb.connect("data/weather.db", read_only=True)
    # Run query
    df = con.execute("""
        SELECT week_start, total_rainfall, rainfall_needed, surplus_deficit, irrigation_status
        FROM irrigation_tracker
        WHERE city = ?
        ORDER BY week_start
    """,[selected_city]).df()
    con.close() # Close connection
    # Build Bar Chart
    fig = px.bar(
        df,
        x="week_start",
        y="total_rainfall",
        color="irrigation_status",
        color_discrete_map={
            "Irrigation needed": "red",
            "Light irrigation needed": "orange",
            "No irrigation needed": "green"
        },
        title=f"Weekly Rainfall vs Need - {selected_city}",
        labels={
            "total_rainfall": "Rainfall (inches)",
            "week_start": "Week"
        }
    )
    # Add surplus line to bar chart
    fig.add_hline(
        y=1.0,
        line_dash="dash",
        line_color="red",
        annotation_text="1 inch needed"
    )

    return fig

if __name__ == "__main__":
    app.run(debug=True)