# app.py
# Oregon Garden Intelligence Dashboard
import dash
from dash import dcc, html, Input, Output, State, dash_table
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os
import pytz
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from scripts.ingest_forecast import run_forecast_ingest

# Initialize the app
app = dash.Dash(__name__)

# Expose Flask server for Render
server = app.server

# Layout
app.layout = html.Div([
    html.H1("Oregon Gardening Dashboard"),
    html.P("Live Insights for Garden Management"),

    dcc.Dropdown(
        id="city-dropdown",
        options=[],
        value="Portland",
        clearable=False
    ),

    # Store for selected plant names
    dcc.Store(id="selected-plants-store", data=[]),

    dcc.Graph(id="temperature-chart"),

    html.H3("Irrigation Tracker — Last 4 Weeks"),
    dash_table.DataTable(id="irrigation-table"),

    dcc.Graph(id="seasonal-chart"),

    # Planting Gantt + Plant Selector Section
    html.H3(id="freeze-date-display"),
    html.Div([

        # Left: Gantt chart
        html.Div([
            dcc.Graph(id="gantt-chart")
        ], style={"width": "60%", "display": "inline-block", "verticalAlign": "top"}),

        # Right: Export + Filters + Plant Table
        html.Div([
            html.Button(
                "Export Selected Plants",
                id="export-button",
                style={"marginBottom": "10px", "padding": "6px 12px"}
            ),
            dcc.Download(id="export-download"),
            dcc.Dropdown(
                id="growing-season-filter",
                placeholder="Filter by Growing Season",
                clearable=True
            ),
            dcc.Dropdown(
                id="harvest-type-filter",
                placeholder="Filter by Harvest Type",
                clearable=True,
                style={"marginTop": "10px"}
            ),
            dash_table.DataTable(
                id="plant-table",
                row_selectable="multi",
                selected_rows=[],
                style_table={
                    "marginTop": "10px",
                    "overflowX": "auto",
                    "overflowY": "auto",
                    "maxHeight": "400px"
                },
                style_cell={
                    "minWidth": "60px",
                    "maxWidth": "140px",
                    "whiteSpace": "normal",
                    "textAlign": "left",
                    "padding": "6px",
                    "fontSize": "12px"
                },
                style_header={
                    "fontWeight": "bold",
                    "whiteSpace": "normal",
                    "textAlign": "left"
                },
                style_data_conditional=[{
                    "if": {"state": "selected"},
                    "backgroundColor": "#d4edda",
                    "border": "1px solid #28a745"
                }],
                fixed_rows={"headers": True},
                page_size=500
            )
        ], style={"width": "38%", "display": "inline-block", "verticalAlign": "top", "paddingLeft": "2%"})
    ]),

    # What to Plant This Week Section
    html.H3("What to Plant This Week"),
    html.Div(id="plant-cards")
])


# --- Populate city dropdown ---
@app.callback(
    Output("city-dropdown", "options"),
    Output("city-dropdown", "value"),
    Input("city-dropdown", "id")
)
def populate_city_dropdown(_):
    con = duckdb.connect("data/weather.db", read_only=True)
    cities = con.execute("""
        SELECT DISTINCT city FROM planting_gantt ORDER BY city
    """).fetchall()
    con.close()
    options = [{"label": r[0], "value": r[0]} for r in cities]
    return options, "Portland"


# --- Temperature chart ---
@app.callback(
    Output("temperature-chart", "figure"),
    Input("city-dropdown", "value")
)
def update_temperature_chart(selected_city):
    if not selected_city:
        return {}
    con = duckdb.connect("data/weather.db", read_only=True)
    df = con.execute("""
        SELECT date, temp_max, temp_min
        FROM six_weeks_weather
        WHERE city = ?
        ORDER BY date
    """, [selected_city]).df()
    con.close()
    fig = px.line(
        df,
        x="date",
        y=["temp_max", "temp_min"],
        title=f"Temperature - {selected_city}",
        labels={"value": "Temperature (°F)", "date": "Date"}
    )
    return fig


# --- Irrigation table ---
@app.callback(
    Output("irrigation-table", "data"),
    Output("irrigation-table", "columns"),
    Input("city-dropdown", "value")
)
def update_irrigation_table(selected_city):
    if not selected_city:
        return [], []
    con = duckdb.connect("data/weather.db", read_only=True)
    df = con.execute("""
        SELECT
            CAST(week_start AS VARCHAR) AS "Week Start",
            total_rainfall AS "Total Rainfall (in)",
            irrigation_status AS "Irrigation Status"
        FROM irrigation_tracker
        WHERE city = ?
        ORDER BY week_start DESC
        LIMIT 4
    """, [selected_city]).df()
    con.close()
    columns = [{"name": col, "id": col} for col in df.columns]
    data = df.to_dict("records")
    return data, columns


# --- Seasonal chart ---
@app.callback(
    Output("seasonal-chart", "figure"),
    Input("city-dropdown", "value")
)
def update_seasonal_chart(selected_city):
    if not selected_city:
        return {}
    con = duckdb.connect("data/weather.db", read_only=True)
    df = con.execute("""
        SELECT
            date,
            avg_shallow_soil_temp,
            avg_deep_soil_temp,
            HOUR(day_length) +
            MINUTE(day_length) / 60.0 +
            SECOND(day_length) / 3600.0 AS day_length_hours
        FROM daily_data
        WHERE city = ?
        ORDER BY date
    """, [selected_city]).df()
    con.close()
    fig = px.line(
        df,
        x="date",
        y=["avg_shallow_soil_temp", "avg_deep_soil_temp"],
        title=f"2026 Seasonal Conditions - {selected_city}",
        labels={"value": "Temperature (°F)", "date": "Date"}
    )
    fig.add_scatter(
        x=df["date"],
        y=df["day_length_hours"],
        name="Day Length (Hours)",
        yaxis="y2",
        line={"dash": "dot", "color": "gold"}
    )
    fig.update_layout(
        yaxis2={
            "title": "Day Length (Hours)",
            "overlaying": "y",
            "side": "right"
        }
    )
    return fig


# --- Freeze date display ---
@app.callback(
    Output("freeze-date-display", "children"),
    Input("city-dropdown", "value")
)
def update_freeze_date(selected_city):
    if not selected_city:
        return ""
    con = duckdb.connect("data/weather.db", read_only=True)
    df = con.execute("""
        SELECT avg_last_freeze_all_time
        FROM avg_freeze_dates
        WHERE city = ?
    """, [selected_city]).df()
    con.close()
    if df.empty:
        return ""
    date = df["avg_last_freeze_all_time"].iloc[0].strftime("%B %d")
    return f"Average Last Freeze ({selected_city}): {date} — based on all-time historical data (conservative estimate)"


# --- Plant table + filter options ---
@app.callback(
    Output("plant-table", "data"),
    Output("plant-table", "columns"),
    Output("growing-season-filter", "options"),
    Output("harvest-type-filter", "options"),
    Input("city-dropdown", "value"),
    Input("growing-season-filter", "value"),
    Input("harvest-type-filter", "value")
)
def update_plant_table(selected_city, growing_season, harvest_type):
    if not selected_city:
        return [], [], [], []
    con = duckdb.connect("data/weather.db", read_only=True)

    season_options = [
        {"label": r[0], "value": r[0]}
        for r in con.execute("SELECT DISTINCT growing_season FROM plants ORDER BY growing_season").fetchall()
    ]
    type_options = [
        {"label": r[0], "value": r[0]}
        for r in con.execute("SELECT DISTINCT harvest_type FROM plants ORDER BY harvest_type").fetchall()
    ]

    query = """
        SELECT
            common_name AS "Plant",
            plant_family AS "Family",
            growing_season AS "Season",
            harvest_type AS "Type",
            ideal_temp_min_f AS "Ideal Min",
            ideal_temp_max_f AS "Ideal Max",
            min_viable_temp_f AS "Min Viable",
            CASE WHEN direct_sow THEN 'Direct Sow'
                 ELSE CAST(weeks_indoor_before_transplant AS VARCHAR) || ' weeks indoor'
            END AS "Sow Method",
            square_feet_needed AS "Sq Ft"
        FROM plants
        WHERE 1=1
    """
    params = []
    if growing_season:
        query += " AND growing_season = ?"
        params.append(growing_season)
    if harvest_type:
        query += " AND harvest_type = ?"
        params.append(harvest_type)
    query += " ORDER BY common_name"

    df = con.execute(query, params).df()
    con.close()

    columns = [{"name": col, "id": col} for col in df.columns]
    data = df.to_dict("records")
    return data, columns, season_options, type_options


# --- Store selected plant names from row selection ---
@app.callback(
    Output("selected-plants-store", "data"),
    Input("plant-table", "selected_rows"),
    State("plant-table", "data")
)
def store_selected_plants(selected_rows, table_data):
    if not selected_rows or not table_data:
        return []
    return [table_data[i]["Plant"] for i in selected_rows]


# --- Gantt chart — shows selected plants or prompt ---
@app.callback(
    Output("gantt-chart", "figure"),
    Input("city-dropdown", "value"),
    Input("selected-plants-store", "data")
)
def update_gantt(selected_city, selected_plants):
    if not selected_city:
        return {}

    if not selected_plants:
        fig = go.Figure()
        fig.update_layout(
            title="Select plants to see their planting windows",
            height=400,
            margin=dict(l=20, r=20, t=40, b=20)
        )
        return fig

    con = duckdb.connect("data/weather.db", read_only=True)
    placeholders = ",".join(["?" for _ in selected_plants])
    df = con.execute(f"""
        SELECT
            p.common_name,
            p.growing_season,
            pg.planting_start,
            pg.outdoor_start,
            pg.planting_end
        FROM plants p
        JOIN planting_gantt pg
            ON p.growing_season = pg.growing_season
            AND p.harvest_type = pg.harvest_type
        WHERE pg.city = ?
          AND p.common_name IN ({placeholders})
        ORDER BY p.growing_season, p.common_name
    """, [selected_city] + selected_plants).df()
    con.close()

    if df.empty:
        return {}

    df["planting_start"] = pd.to_datetime(df["planting_start"])
    df["outdoor_start"] = pd.to_datetime(df["outdoor_start"])
    df["planting_end"] = pd.to_datetime(df["planting_end"])

    colors = {
        "Cool Season": "#4a90d9",
        "Warm Season": "#e07b39",
        "Perennial": "#6abf69"
    }

    rows = []
    for _, row in df.iterrows():
        if row["planting_start"] < row["outdoor_start"]:
            rows.append({
                "Task": row["common_name"],
                "Start": row["planting_start"],
                "Finish": row["outdoor_start"],
                "Season": row["growing_season"],
                "Segment": "Indoor"
            })
        rows.append({
            "Task": row["common_name"],
            "Start": row["outdoor_start"],
            "Finish": row["planting_end"],
            "Season": row["growing_season"],
            "Segment": "Outdoor"
        })

    timeline_df = pd.DataFrame(rows)

    fig = px.timeline(
        timeline_df,
        x_start="Start",
        x_end="Finish",
        y="Task",
        color="Season",
        color_discrete_map=colors,
        pattern_shape="Segment",
        pattern_shape_map={"Indoor": "/", "Outdoor": ""},
        title=f"Planting Windows — {selected_city}",
        labels={"Task": "", "Season": "Season"}
    )

    fig.update_yaxes(autorange="reversed")
    fig.update_layout(
        height=max(400, len(selected_plants) * 40),
        margin=dict(l=20, r=20, t=40, b=20)
    )

    return fig


# --- Plant cards — filtered to selected plants ---
@app.callback(
    Output("plant-cards", "children"),
    Input("city-dropdown", "value"),
    Input("selected-plants-store", "data")
)
def update_plant_cards(selected_city, selected_plants):
    if not selected_city:
        return []

    if not selected_plants:
        return [html.P("Select plants from the table to see this week's viability.")]

    con = duckdb.connect("data/weather.db", read_only=True)

    forecast = con.execute("""
        SELECT date, temp_max, temp_min
        FROM six_weeks_weather
        WHERE city = ?
        AND date >= CURRENT_DATE
        AND date < CURRENT_DATE + 7
        ORDER BY date
    """, [selected_city]).df()

    placeholders = ",".join(["?" for _ in selected_plants])
    plants = con.execute(f"""
        SELECT
            common_name,
            plant_family,
            min_viable_temp_f,
            max_viable_temp_f,
            CASE WHEN direct_sow THEN 'Direct Sow'
                 ELSE CAST(weeks_indoor_before_transplant AS VARCHAR) || ' weeks indoor'
            END AS sow_method
        FROM plants
        WHERE common_name IN ({placeholders})
    """, selected_plants).df()
    con.close()

    if forecast.empty:
        return [html.P("No forecast data available.")]

    viable_plants = []
    for _, plant in plants.iterrows():
        viable_days = forecast[
            (forecast["temp_max"] >= plant["min_viable_temp_f"]) &
            (forecast["temp_max"] <= plant["max_viable_temp_f"])
        ].shape[0]

        viable_plants.append({
            "name": plant["common_name"],
            "family": plant["plant_family"],
            "sow_method": plant["sow_method"],
            "viable_days": viable_days
        })

    viable_plants.sort(key=lambda x: x["viable_days"], reverse=True)

    cards = []
    for plant in viable_plants:
        color = "#4a9d4a" if plant["viable_days"] >= 4 else "#cc4444"
        cards.append(
            html.Div([
                html.Strong(plant["name"]),
                html.Br(),
                html.Span(plant["family"],
                    style={"fontSize": "11px", "color": "#666"}),
                html.Br(),
                html.Span(plant["sow_method"],
                    style={"fontSize": "11px"}),
                html.Br(),
                html.Span(f"{plant['viable_days']}/7 days viable",
                    style={"fontSize": "11px", "color": color, "fontWeight": "bold"})
            ], style={
                "display": "inline-block",
                "border": "1px solid #ddd",
                "borderRadius": "8px",
                "padding": "10px",
                "margin": "5px",
                "width": "140px",
                "verticalAlign": "top",
                "backgroundColor": "#f9f9f9"
            })
        )

    return cards


# --- Export selected plants to CSV ---
@app.callback(
    Output("export-download", "data"),
    Input("export-button", "n_clicks"),
    State("selected-plants-store", "data"),
    State("city-dropdown", "value"),
    prevent_initial_call=True
)
def export_selected_plants(n_clicks, selected_plants, selected_city):
    if not selected_plants:
        return None

    con = duckdb.connect("data/weather.db", read_only=True)
    placeholders = ",".join(["?" for _ in selected_plants])
    df = con.execute(f"""
        SELECT
            p.common_name AS "Plant",
            p.plant_family AS "Family",
            p.growing_season AS "Season",
            p.harvest_type AS "Type",
            p.ideal_temp_min_f AS "Ideal Temp Min",
            p.ideal_temp_max_f AS "Ideal Temp Max",
            p.min_viable_temp_f AS "Min Viable Temp",
            p.max_viable_temp_f AS "Max Viable Temp",
            p.days_to_maturity AS "Days to Maturity",
            CASE WHEN p.direct_sow THEN 'Direct Sow'
                 ELSE CAST(p.weeks_indoor_before_transplant AS VARCHAR) || ' weeks indoor'
            END AS "Sow Method",
            p.square_feet_needed AS "Sq Ft",
            pg.planting_start AS "Planting Start",
            pg.outdoor_start AS "Outdoor Start",
            pg.planting_end AS "Planting End"
        FROM plants p
        JOIN planting_gantt pg
            ON p.growing_season = pg.growing_season
            AND p.harvest_type = pg.harvest_type
        WHERE pg.city = ?
          AND p.common_name IN ({placeholders})
        ORDER BY p.common_name
    """, [selected_city] + selected_plants).df()
    con.close()

    return dcc.send_data_frame(df.to_csv, "selected_plants.csv", index=False)


# --- Scheduler ---
def refresh_forecast():
    try:
        print("Starting scheduled forecast ingest...")
        run_forecast_ingest()
        print("Scheduled ingest complete")
    except Exception as e:
        print(f"Ingest failed safely: {e}")


if os.environ.get("WEB_CONCURRENCY", "1") == "1":
    try:
        scheduler.add_job(refresh_forecast, 'cron', hour=12, minute=0)
        scheduler.start()
        print(f"Scheduler started — test fire at {fire_time}")
    except Exception as e:
        print(f"Scheduler failed to start: {e}")


if __name__ == "__main__":
    app.run(debug=True)