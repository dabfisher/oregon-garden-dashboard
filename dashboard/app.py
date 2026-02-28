# app.py
# Oregon Garden Intelligence Dashboard
import sys
import os
# Add project root to path so 'scripts' is importable when running from any subdirectory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dash
from dash import dcc, html, Input, Output, State, dash_table
import duckdb
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import pytz
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
##from scripts.ingest_forecast import run_forecast_ingest

# Initialize the app
app = dash.Dash(__name__)
server = app.server

# ── Earthy PNW palette ───────────────────────────────────────────────────────
COLORS = {
    "bg":           "#f5f0e8",
    "panel":        "#ede8dc",
    "border":       "#c8b89a",
    "forest":       "#3a5c3a",
    "moss":         "#6b8f5e",
    "bark":         "#7a5c3a",
    "terracotta":   "#c4622d",
    "slate":        "#4a7a9b",
    "sage":         "#8aab7a",
    "gold":         "#c9a84c",
    "text":         "#2c2416",
    "muted":        "#7a6a54",
}

GANTT_COLORS = {
    "Cool Season": COLORS["slate"],
    "Warm Season": COLORS["terracotta"],
    "Perennial":   COLORS["sage"],
}

CHART_LAYOUT = dict(
    paper_bgcolor="white",
    plot_bgcolor="#faf8f4",
    font=dict(family="Lato, sans-serif", color=COLORS["text"], size=11),
    title_font=dict(family="Playfair Display, serif", size=15, color=COLORS["text"]),
    margin=dict(l=20, r=20, t=40, b=20),
    legend=dict(
        bgcolor="rgba(245,240,232,0.8)",
        bordercolor=COLORS["border"],
        borderwidth=1,
        font=dict(size=11),
    )
)

# ── CSS ──────────────────────────────────────────────────────────────────────
app.index_string = f"""
<!DOCTYPE html>
<html>
<head>
    {{%metas%}}
    <title>Oregon Garden Dashboard</title>
    {{%favicon%}}
    {{%css%}}
    <link href="https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,600;1,400&family=DM+Mono:wght@400;500&family=Lato:wght@300;400;700&display=swap" rel="stylesheet">
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}

        body {{
            background: {COLORS["bg"]};
            font-family: 'Lato', sans-serif;
            color: {COLORS["text"]};
        }}

        /* ── Magazine header ── */
        .mag-header {{
            display: flex;
            align-items: stretch;
            background: {COLORS["forest"]};
        }}
        .mag-flag {{
            background: {COLORS["terracotta"]};
            padding: 18px 28px;
            flex-shrink: 0;
        }}
        .mag-flag-title {{
            font-family: 'Playfair Display', serif;
            font-size: 1.5rem;
            color: {COLORS["bg"]};
            line-height: 1.1;
        }}
        .mag-flag-sub {{
            font-size: 0.6rem;
            letter-spacing: 0.16em;
            text-transform: uppercase;
            color: rgba(245,240,232,0.55);
            margin-top: 4px;
        }}
        .mag-city-block {{
            display: flex;
            align-items: center;
            padding: 0 28px;
            border-left: 1px solid rgba(255,255,255,0.1);
            gap: 14px;
            margin-left: auto;
        }}
        .mag-city-label {{
            font-size: 0.62rem;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: rgba(245,240,232,0.5);
        }}
        .mag-date-block {{
            display: flex;
            flex-direction: column;
            justify-content: center;
            padding: 0 28px;
            border-left: 1px solid rgba(255,255,255,0.1);
        }}
        .mag-date-dow {{
            font-size: 0.6rem;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            color: rgba(245,240,232,0.5);
        }}
        .mag-date-full {{
            font-family: 'Playfair Display', serif;
            font-size: 1.05rem;
            color: rgba(245,240,232,0.9);
            line-height: 1.2;
        }}

        /* ── Body layout ── */
        .mag-body {{
            display: flex;
            min-height: 100vh;
            max-width: 1400px;
            margin: 0 auto;
        }}
        .mag-main {{
            flex: 1;
            padding: 24px 24px 24px 28px;
            border-right: 1px solid {COLORS["border"]};
            min-width: 0;
        }}
        .mag-sidebar {{
            width: 290px;
            flex-shrink: 0;
            padding: 24px 20px;
            background: {COLORS["panel"]};
        }}

        /* ── Section headers ── */
        .sec-hed {{
            font-family: 'Playfair Display', serif;
            font-size: 0.95rem;
            color: {COLORS["text"]};
            border-bottom: 2px solid {COLORS["text"]};
            padding-bottom: 6px;
            margin-bottom: 14px;
            display: flex;
            justify-content: space-between;
            align-items: baseline;
        }}
        .sec-hed span {{
            font-family: 'Lato', sans-serif;
            font-size: 0.62rem;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: {COLORS["muted"]};
            font-style: normal;
        }}
        .sec-divider {{
            border: none;
            border-top: 1px solid {COLORS["border"]};
            margin: 20px 0;
        }}

        /* ── Today stats bar ── */
        .today-bar {{
            display: flex;
            border: 1px solid {COLORS["border"]};
            border-radius: 3px;
            overflow: hidden;
            background: white;
            margin-bottom: 20px;
        }}
        .today-stat {{
            flex: 1;
            padding: 12px 16px;
            border-right: 1px solid {COLORS["border"]};
            text-align: center;
        }}
        .today-stat:last-child {{ border-right: none; }}
        .today-stat-label {{
            font-size: 0.58rem;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            color: {COLORS["muted"]};
            margin-bottom: 4px;
        }}
        .today-stat-value {{
            font-family: 'DM Mono', monospace;
            font-size: 1.6rem;
            line-height: 1;
            font-weight: 500;
        }}

        /* ── Chart panels ── */
        .chart-panel {{
            background: white;
            border: 1px solid {COLORS["border"]};
            margin-bottom: 20px;
            border-radius: 2px;
            padding: 16px 16px 8px;
        }}

        /* ── Sidebar blocks ── */
        .sb-freeze {{
            background: white;
            border: 1px solid {COLORS["border"]};
            border-left: 3px solid {COLORS["terracotta"]};
            padding: 14px;
            margin-bottom: 18px;
            border-radius: 0 2px 2px 0;
        }}
        .sb-freeze-label {{
            font-size: 0.62rem;
            letter-spacing: 0.14em;
            text-transform: uppercase;
            color: {COLORS["terracotta"]};
            font-weight: 700;
            margin-bottom: 6px;
        }}
        .sb-freeze-date {{
            font-family: 'Playfair Display', serif;
            font-size: 1.45rem;
            color: {COLORS["text"]};
        }}
        .sb-freeze-sub {{
            font-size: 0.63rem;
            color: {COLORS["muted"]};
            margin-top: 2px;
        }}
        .sb-sec-hed {{
            font-family: 'Playfair Display', serif;
            font-size: 0.88rem;
            color: {COLORS["text"]};
            border-bottom: 1px solid {COLORS["border"]};
            padding-bottom: 6px;
            margin-bottom: 12px;
        }}

        /* ── Plant cards (sidebar) ── */
        .plant-card {{
            background: white;
            border: 1px solid {COLORS["border"]};
            border-radius: 2px;
            padding: 9px 10px;
        }}
        .plant-card-name {{
            font-family: 'Playfair Display', serif;
            font-size: 0.78rem;
            color: {COLORS["text"]};
            display: block;
        }}
        .plant-card-family {{
            font-size: 0.6rem;
            color: {COLORS["muted"]};
            font-style: italic;
            display: block;
            margin-top: 1px;
        }}
        .plant-card-sow {{
            font-size: 0.65rem;
            color: {COLORS["bark"]};
            display: block;
            margin-top: 3px;
        }}
        .plant-card-viable-good {{
            font-size: 0.65rem;
            font-weight: 700;
            color: {COLORS["forest"]};
            display: block;
            margin-top: 4px;
        }}
        .plant-card-viable-bad {{
            font-size: 0.65rem;
            font-weight: 700;
            color: {COLORS["terracotta"]};
            display: block;
            margin-top: 4px;
        }}

        /* ── Buttons ── */
        .btn-export {{
            background: {COLORS["forest"]};
            color: {COLORS["bg"]};
            border: none;
            padding: 8px 14px;
            font-family: 'Lato', sans-serif;
            font-size: 0.68rem;
            font-weight: 700;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            border-radius: 2px;
            cursor: pointer;
        }}
        .btn-export:hover {{ background: {COLORS["moss"]}; }}
        .btn-clear {{
            background: transparent;
            color: {COLORS["muted"]};
            border: 1px solid {COLORS["border"]};
            padding: 8px 12px;
            font-family: 'Lato', sans-serif;
            font-size: 0.68rem;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            border-radius: 2px;
            cursor: pointer;
            margin-left: 6px;
        }}
        .btn-clear:hover {{ border-color: {COLORS["bark"]}; color: {COLORS["bark"]}; }}

        /* ── Dropdown overrides ── */
        .Select-control {{
            border-color: {COLORS["border"]} !important;
            background-color: white !important;
            font-size: 0.8rem !important;
        }}
    </style>
</head>
<body>
    {{%app_entry%}}
    <footer>
        {{%config%}}
        {{%scripts%}}
        {{%renderer%}}
    </footer>
</body>
</html>
"""

# ── Layout ───────────────────────────────────────────────────────────────────
app.layout = html.Div([

    # ── Magazine header ──
    html.Div([
        html.Div([
            html.Div("Oregon", className="mag-flag-title"),
            html.Div("Garden", className="mag-flag-title"),
            html.Div("Intelligence · 2026", className="mag-flag-sub"),
        ], className="mag-flag"),

        html.Div(id="header-date", className="mag-date-block"),

        html.Div([
            html.Div("Viewing", className="mag-city-label"),
            dcc.Dropdown(
                id="city-dropdown",
                options=[],
                value="Portland",
                clearable=False,
                style={
                    "width": "200px",
                    "fontSize": "0.9rem",
                    "fontFamily": "Playfair Display, serif",
                }
            ),
        ], className="mag-city-block"),
    ], className="mag-header"),

    # ── Hidden stores ──
    dcc.Store(id="selected-plants-store", data=[]),

    # ── Body ──
    html.Div([

        # ── Main column ──
        html.Div([

            # Today's stats bar
            html.Div(id="today-stats-bar", className="today-bar"),

            # Temp + Precip + Irrigation
            html.Div([
                html.Div([
                    html.Span("Temperature & Precipitation"),
                    html.Span("Last 30 Days · Weekly Averages"),
                ], className="sec-hed"),
                dcc.Graph(id="temp-precip-chart", config={"displayModeBar": False}),
            ], className="chart-panel"),

            # 10-day forecast
            html.Div([
                html.Div([
                    html.Span("10-Day Forecast"),
                    html.Span("High · Low · Precipitation"),
                ], className="sec-hed"),
                dcc.Graph(id="forecast-chart", config={"displayModeBar": False}),
            ], className="chart-panel"),

            html.Hr(className="sec-divider"),

            # Planting Gantt
            html.Div([
                html.Div([
                    html.Span("Planting Windows"),
                    html.Span("Historical norm · Full year"),
                ], className="sec-hed"),
                dcc.Graph(id="gantt-chart", config={"displayModeBar": False}),
            ], className="chart-panel"),

            # Seasonal conditions
            html.Div([
                html.Div([
                    html.Span("Seasonal Conditions"),
                    html.Span("Sunrise · Sunset · Twilight · Shallow Soil Temp"),
                ], className="sec-hed"),
                dcc.Graph(id="seasonal-chart", config={"displayModeBar": False}),
            ], className="chart-panel"),

        ], className="mag-main"),

        # ── Sidebar ──
        html.Div([

            # Freeze date callout
            html.Div([
                html.Div("❄ Average Last Freeze", className="sb-freeze-label"),
                html.Div(id="freeze-date-display", className="sb-freeze-date"),
                html.Div(id="freeze-date-sub", className="sb-freeze-sub"),
            ], className="sb-freeze"),

            # What to plant this week
            html.Div("What to Plant This Week", className="sb-sec-hed"),
            html.Div(id="plant-cards", style={
                "display": "grid",
                "gridTemplateColumns": "1fr 1fr",
                "gap": "7px",
                "marginBottom": "16px",
            }),

            html.Hr(style={
                "border": "none",
                "borderTop": f"1px solid {COLORS['border']}",
                "margin": "16px 0",
            }),

            # Plant selection
            html.Div("Plant Selection", className="sb-sec-hed"),
            dcc.Dropdown(
                id="growing-season-filter",
                placeholder="Filter by Season",
                clearable=True,
                style={"marginBottom": "7px", "fontSize": "0.78rem"},
            ),
            dcc.Dropdown(
                id="harvest-type-filter",
                placeholder="Filter by Type",
                clearable=True,
                style={"marginBottom": "7px", "fontSize": "0.78rem"},
            ),
            dcc.Dropdown(
                id="pollinator-filter",
                placeholder="Filter by Pollinator",
                options=[
                    {"label": "🐝 Attracts Bees",         "value": "bees"},
                    {"label": "🦋 Attracts Butterflies",   "value": "butterflies"},
                    {"label": "🌺 Attracts Hummingbirds",  "value": "hummingbirds"},
                ],
                clearable=True,
                style={"marginBottom": "10px", "fontSize": "0.78rem"},
            ),
            dash_table.DataTable(
                id="plant-table",
                row_selectable="multi",
                selected_rows=[],
                style_table={
                    "overflowX": "auto",
                    "overflowY": "auto",
                    "maxHeight": "320px",
                    "border": f"1px solid {COLORS['border']}",
                    "borderRadius": "2px",
                    "marginBottom": "12px",
                },
                style_cell={
                    "fontFamily": "Lato, sans-serif",
                    "fontSize": "11px",
                    "padding": "6px 8px",
                    "color": COLORS["text"],
                    "backgroundColor": "white",
                    "border": f"1px solid {COLORS['border']}",
                    "maxWidth": "110px",
                    "whiteSpace": "normal",
                    "textAlign": "left",
                },
                style_header={
                    "fontFamily": "Lato, sans-serif",
                    "fontWeight": "700",
                    "backgroundColor": COLORS["panel"],
                    "color": COLORS["muted"],
                    "fontSize": "9px",
                    "letterSpacing": "0.08em",
                    "textTransform": "uppercase",
                    "border": f"1px solid {COLORS['border']}",
                    "textAlign": "left",
                },
                style_data_conditional=[{
                    "if": {"state": "selected"},
                    "backgroundColor": "#e8f4e8",
                    "border": f"1px solid {COLORS['moss']}",
                }],
                fixed_rows={"headers": True},
                page_size=500,
            ),

            html.Div([
                html.Button("Export CSV", id="export-button", className="btn-export"),
                html.Button("Clear", id="clear-button", className="btn-clear"),
                dcc.Download(id="export-download"),
            ]),

        ], className="mag-sidebar"),

    ], className="mag-body"),
])


# ── Helpers ──────────────────────────────────────────────────────────────────

def get_con():
    return duckdb.connect("data/weather.db", read_only=True)


def time_to_decimal(t):
    """Convert a time/timedelta to decimal hours (e.g. 06:30 → 6.5)."""
    if t is None:
        return None
    if isinstance(t, timedelta):
        return t.total_seconds() / 3600.0
    if hasattr(t, "hour"):
        return t.hour + t.minute / 60.0 + t.second / 3600.0
    return None


def fmt_time(t):
    """Format a time/timedelta as '6:58 AM'."""
    if t is None:
        return "—"
    if isinstance(t, timedelta):
        total = int(t.total_seconds())
        h, remainder = divmod(total, 3600)
        m = remainder // 60
    elif hasattr(t, "hour"):
        h, m = t.hour, t.minute
    else:
        return "—"
    suffix = "AM" if h < 12 else "PM"
    h12 = h % 12 or 12
    return f"{h12}:{m:02d} {suffix}"


# ── Callbacks ─────────────────────────────────────────────────────────────────

@app.callback(
    Output("city-dropdown", "options"),
    Output("city-dropdown", "value"),
    Input("city-dropdown", "id")
)
def populate_city_dropdown(_):
    con = get_con()
    cities = con.execute(
        "SELECT DISTINCT city FROM planting_gantt ORDER BY city"
    ).fetchall()
    con.close()
    return [{"label": r[0], "value": r[0]} for r in cities], "Portland"


# ── Header date ──────────────────────────────────────────────────────────────
@app.callback(
    Output("header-date", "children"),
    Input("city-dropdown", "id"),
)
def update_header_date(_):
    now = datetime.now()
    return [
        html.Div(now.strftime("%A"), className="mag-date-dow"),
        html.Div(now.strftime("%B %d, %Y"), className="mag-date-full"),
    ]


# ── Today's stats bar ─────────────────────────────────────────────────────────
@app.callback(
    Output("today-stats-bar", "children"),
    Input("city-dropdown", "value")
)
def update_today_bar(selected_city):
    if not selected_city:
        return []

    con = get_con()
    today_row = con.execute("""
        SELECT temp_max, temp_min FROM six_weeks_weather
        WHERE city = ? AND date = CAST(CURRENT_DATE AS DATE)
        LIMIT 1
    """, [selected_city]).fetchone()

    sun_row = con.execute("""
        SELECT sunrise, sunset FROM sun_times
        WHERE city = ? AND date = CAST(CURRENT_DATE AS DATE)
        LIMIT 1
    """, [selected_city]).fetchone()
    con.close()

    high_str = f"{int(today_row[0])}°F" if today_row else "—"
    low_str  = f"{int(today_row[1])}°F" if today_row else "—"
    rise_str = fmt_time(sun_row[0]) if sun_row else "—"
    set_str  = fmt_time(sun_row[1]) if sun_row else "—"

    def stat(label, value, color):
        return html.Div([
            html.Div(label, className="today-stat-label"),
            html.Div(value, className="today-stat-value", style={"color": color}),
        ], className="today-stat")

    return [
        stat("Today's High", high_str, COLORS["terracotta"]),
        stat("Today's Low",  low_str,  COLORS["slate"]),
        stat("Sunrise",      rise_str, "#e07b39"),
        stat("Sunset",       set_str,  "#e07b39"),
    ]


# ── Temp + Precip + Irrigation ────────────────────────────────────────────────
@app.callback(
    Output("temp-precip-chart", "figure"),
    Input("city-dropdown", "value")
)
def update_temp_precip(selected_city):
    if not selected_city:
        return {}

    con = get_con()
    # Try irrigation_tracker first for clean weekly data
    irr = con.execute("""
        SELECT week_start, total_rainfall, irrigation_status
        FROM irrigation_tracker
        WHERE city = ?
        ORDER BY week_start DESC LIMIT 5
    """, [selected_city]).df()

    # Weekly avg temps from six_weeks_weather (last 30 days)
    temps = con.execute("""
        SELECT
            DATE_TRUNC('week', date) AS week_start,
            AVG(temp_max) AS avg_high,
            AVG(temp_min) AS avg_low,
            SUM(precipitation) AS total_precip
        FROM six_weeks_weather
        WHERE city = ? AND date >= CURRENT_DATE - 30 AND date < CURRENT_DATE
        GROUP BY week_start
        ORDER BY week_start
    """, [selected_city]).df()
    con.close()

    if temps.empty:
        return {}

    temps["week_start"] = pd.to_datetime(temps["week_start"])
    temps["avg_high"]   = temps["avg_high"].round(1)
    temps["avg_low"]    = temps["avg_low"].round(1)
    temps["total_precip"] = temps["total_precip"].round(2)

    # Merge irrigation status
    if not irr.empty:
        irr["week_start"] = pd.to_datetime(irr["week_start"])
        temps = temps.merge(irr[["week_start", "total_rainfall", "irrigation_status"]],
                            on="week_start", how="left")
        temps["total_precip"] = temps["total_rainfall"].combine_first(temps["total_precip"])
        temps["irrigation_status"] = temps["irrigation_status"].fillna("Unknown")
    else:
        temps["irrigation_status"] = temps["total_precip"].apply(
            lambda x: "No irrigation needed" if x >= 0.5 else "Irrigation needed"
        )

    week_labels = temps["week_start"].dt.strftime("%b %d")

    # Subplots: temp on top (row 1), precip on bottom (row 2)
    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.60, 0.40],
        shared_xaxes=True,
        vertical_spacing=0.03,
        subplot_titles=("", ""),
    )

    # Avg high
    fig.add_trace(go.Scatter(
        x=week_labels, y=temps["avg_high"],
        mode="lines+markers+text",
        name="Avg High",
        line=dict(color=COLORS["terracotta"], width=2),
        marker=dict(color=COLORS["terracotta"], size=9),
        text=[f"{v:.0f}°" for v in temps["avg_high"]],
        textposition="top center",
        textfont=dict(color=COLORS["terracotta"], size=11, family="DM Mono"),
    ), row=1, col=1)

    # Avg low
    fig.add_trace(go.Scatter(
        x=week_labels, y=temps["avg_low"],
        mode="lines+markers+text",
        name="Avg Low",
        line=dict(color=COLORS["slate"], width=2),
        marker=dict(color=COLORS["slate"], size=9),
        text=[f"{v:.0f}°" for v in temps["avg_low"]],
        textposition="bottom center",
        textfont=dict(color=COLORS["slate"], size=11, family="DM Mono"),
    ), row=1, col=1)

    # Precip bars — color by irrigation status
    bar_colors = []
    for _, row in temps.iterrows():
        s = str(row["irrigation_status"]).lower()
        is_needed = "needed" in s and "no" not in s
        bar_colors.append(COLORS["terracotta"] if is_needed else COLORS["moss"])

    fig.add_trace(go.Bar(
        x=week_labels,
        y=temps["total_precip"],
        name="Weekly Precip (in)",
        marker_color=COLORS["slate"],
        marker_opacity=0.8,
        # Label inside bar if tall enough, handled via texttemplate
        texttemplate="%{y:.1f}\"",
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(size=10, family="DM Mono", color="white"),
        constraintext="none",
    ), row=2, col=1)

    # Irrigation status as colored annotations under each precip bar
    for i, (_, row) in enumerate(temps.iterrows()):
        s = str(row["irrigation_status"]).lower()
        is_needed = "needed" in s and "no" not in s
        fig.add_annotation(
            x=week_labels.iloc[i],
            y=-0.25,
            yref="y2",
            text="Needed" if is_needed else "OK ✓",
            showarrow=False,
            font=dict(
                size=9, family="Lato",
                color=COLORS["terracotta"] if is_needed else COLORS["forest"],
            ),
            bgcolor="#fff3ee" if is_needed else "#eef6ee",
            borderpad=2,
        )

    fig.update_layout(**CHART_LAYOUT)
    fig.update_layout(
        height=300,
        showlegend=True,
        legend=dict(orientation="h", y=1.05, x=0, font=dict(size=10)),
        margin=dict(l=10, r=10, t=20, b=50),
        bargap=0.3,
    )
    fig.update_yaxes(title_text="°F", row=1, col=1, gridcolor="#f0e8d8", zeroline=False)
    fig.update_yaxes(
        title_text="in", row=2, col=1,
        gridcolor="#f0e8d8", zeroline=False, rangemode="tozero",
        range=[0, temps["total_precip"].max() * 1.5] if temps["total_precip"].max() > 0 else [0, 1],
    )
    fig.update_xaxes(showgrid=False)

    return fig


# ── 10-day forecast ───────────────────────────────────────────────────────────
@app.callback(
    Output("forecast-chart", "figure"),
    Input("city-dropdown", "value")
)
def update_forecast_chart(selected_city):
    if not selected_city:
        return {}

    con = get_con()
    df = con.execute("""
        SELECT date, temp_max, temp_min, precipitation
        FROM six_weeks_weather
        WHERE city = ? AND date >= CURRENT_DATE
        ORDER BY date
        LIMIT 10
    """, [selected_city]).df()
    con.close()

    if df.empty:
        return {}

    df["date_str"]     = pd.to_datetime(df["date"]).dt.strftime("%b %d")
    df["temp_max"]     = df["temp_max"].round(0)
    df["temp_min"]     = df["temp_min"].round(0)
    df["precipitation"]= df["precipitation"].fillna(0).round(2)

    y_min = df["temp_min"].min()
    y_max = df["temp_max"].max()

    fig = go.Figure()

    # Alternating column background shading
    for i in range(len(df)):
        if i % 2 == 0:
            fig.add_vrect(
                x0=i - 0.5, x1=i + 0.5,
                fillcolor="#f8f4ec", opacity=0.6, line_width=0,
                layer="below",
            )

    # Dashed connecting lines (behind text)
    fig.add_trace(go.Scatter(
        x=df["date_str"], y=df["temp_max"],
        mode="lines", showlegend=False,
        line=dict(color=COLORS["terracotta"], width=1, dash="dot"),
        opacity=0.35,
    ))
    fig.add_trace(go.Scatter(
        x=df["date_str"], y=df["temp_min"],
        mode="lines", showlegend=False,
        line=dict(color=COLORS["slate"], width=1, dash="dot"),
        opacity=0.35,
    ))

    # High temps
    fig.add_trace(go.Scatter(
        x=df["date_str"], y=df["temp_max"],
        mode="text", name="High",
        text=[f"{int(v)}°" for v in df["temp_max"]],
        textposition="top center",
        textfont=dict(color=COLORS["terracotta"], size=14, family="DM Mono"),
    ))

    # Low temps
    fig.add_trace(go.Scatter(
        x=df["date_str"], y=df["temp_min"],
        mode="text", name="Low",
        text=[f"{int(v)}°" for v in df["temp_min"]],
        textposition="bottom center",
        textfont=dict(color=COLORS["slate"], size=14, family="DM Mono"),
    ))

    # Precip annotations below x-axis
    for _, row in df.iterrows():
        p = row["precipitation"]
        label = f"🌧 {p:.2f}\"" if p > 0.01 else "☁ —"
        color = COLORS["slate"] if p > 0.01 else COLORS["muted"]
        fig.add_annotation(
            x=row["date_str"],
            y=y_min - 5,
            text=label, showarrow=False,
            font=dict(size=9, color=color, family="Lato"),
        )

    fig.update_layout(**CHART_LAYOUT)
    fig.update_layout(
        height=190,
        showlegend=True,
        legend=dict(orientation="h", y=1.1, x=0, font=dict(size=10)),
        margin=dict(l=10, r=10, t=20, b=40),
        xaxis=dict(showgrid=False, zeroline=False),
        yaxis=dict(
            showgrid=True, gridcolor="#f0e8d8", zeroline=False,
            range=[y_min - 10, y_max + 10],
            tickfont=dict(size=9),
        ),
    )

    return fig


# ── Seasonal conditions ───────────────────────────────────────────────────────
@app.callback(
    Output("seasonal-chart", "figure"),
    Input("city-dropdown", "value")
)
def update_seasonal_chart(selected_city):
    if not selected_city:
        return {}

    con = get_con()

    sun = con.execute("""
        SELECT date, sunrise, sunset, morning_twilight, evening_twilight
        FROM sun_times
        WHERE city = ?
        ORDER BY date
    """, [selected_city]).df()

    soil = con.execute("""
        SELECT date, avg_shallow_soil_temp, avg_min_temp, avg_max_temp
        FROM daily_data
        WHERE city = ?
        ORDER BY date
    """, [selected_city]).df()

    freeze = con.execute("""
        SELECT avg_last_freeze_all_time FROM avg_freeze_dates WHERE city = ?
    """, [selected_city]).fetchone()

    con.close()

    if sun.empty:
        return {}

    sun["date"] = pd.to_datetime(sun["date"])
    for col in ["sunrise", "sunset", "morning_twilight", "evening_twilight"]:
        if col in sun.columns:
            sun[col] = sun[col].apply(time_to_decimal)

    fig = go.Figure()

    # Shaded daylight band between morning and evening twilight
    if "morning_twilight" in sun.columns and "evening_twilight" in sun.columns:
        mt = sun["morning_twilight"].dropna()
        et = sun["evening_twilight"].dropna()
        if not mt.empty and not et.empty:
            fig.add_trace(go.Scatter(
                x=pd.concat([sun["date"], sun["date"][::-1]]),
                y=pd.concat([sun["morning_twilight"], sun["evening_twilight"][::-1]]),
                fill="toself",
                fillcolor="rgba(201,168,76,0.08)",
                line=dict(width=0),
                showlegend=False,
                hoverinfo="skip",
            ))

    # Morning twilight
    if "morning_twilight" in sun.columns:
        fig.add_trace(go.Scatter(
            x=sun["date"], y=sun["morning_twilight"],
            mode="lines", name="Morning Twilight",
            line=dict(color=COLORS["gold"], width=1.2, dash="dash"),
            opacity=0.85,
        ))

    # Sunrise
    if "sunrise" in sun.columns:
        fig.add_trace(go.Scatter(
            x=sun["date"], y=sun["sunrise"],
            mode="lines", name="Sunrise",
            line=dict(color="#e07b39", width=2),
            opacity=0.9,
        ))

    # Sunset
    if "sunset" in sun.columns:
        fig.add_trace(go.Scatter(
            x=sun["date"], y=sun["sunset"],
            mode="lines", name="Sunset",
            line=dict(color="#e07b39", width=2),
            opacity=0.9,
        ))

    # Evening twilight
    if "evening_twilight" in sun.columns:
        fig.add_trace(go.Scatter(
            x=sun["date"], y=sun["evening_twilight"],
            mode="lines", name="Evening Twilight",
            line=dict(color=COLORS["gold"], width=1.2, dash="dash"),
            opacity=0.85,
        ))

    # Avg temp band + soil temp — secondary Y axis (0–100°F)
    if not soil.empty:
        soil["date"] = pd.to_datetime(soil["date"])
        # Shaded avg temp band (high/low)
        fig.add_trace(go.Scatter(
            x=pd.concat([soil["date"], soil["date"][::-1]]),
            y=pd.concat([soil["avg_max_temp"], soil["avg_min_temp"][::-1]]),
            fill="toself",
            fillcolor="rgba(196,98,45,0.08)",
            line=dict(width=0),
            name="Avg Temp Range",
            showlegend=True,
            hoverinfo="skip",
            yaxis="y2",
        ))
        # Avg high
        fig.add_trace(go.Scatter(
            x=soil["date"], y=soil["avg_max_temp"],
            mode="lines", name="Avg High °F",
            line=dict(color=COLORS["terracotta"], width=1.5, dash="dot"),
            opacity=0.7,
            yaxis="y2",
        ))
        # Avg low
        fig.add_trace(go.Scatter(
            x=soil["date"], y=soil["avg_min_temp"],
            mode="lines", name="Avg Low °F",
            line=dict(color=COLORS["slate"], width=1.5, dash="dot"),
            opacity=0.7,
            yaxis="y2",
        ))
        # Shallow soil temp
        fig.add_trace(go.Scatter(
            x=soil["date"], y=soil["avg_shallow_soil_temp"],
            mode="lines", name="Shallow Soil °F",
            line=dict(color=COLORS["moss"], width=2),
            opacity=0.9,
            yaxis="y2",
        ))

    # Today line — use add_shape to avoid plotly annotation mean bug
    today_str = datetime.now().strftime("%Y-%m-%d")
    fig.add_shape(
        type="line", xref="x", yref="paper",
        x0=today_str, x1=today_str, y0=0, y1=1,
        line=dict(color=COLORS["forest"], width=1.5, dash="dot"),
        opacity=0.7,
    )
    fig.add_annotation(
        x=today_str, xref="x", yref="paper", y=1.02,
        text="Today", showarrow=False,
        font=dict(size=9, color=COLORS["forest"]),
        xanchor="left",
    )

    # Last freeze — quieter, muted
    if freeze:
        freeze_date = pd.Timestamp(freeze[0])
        fig.add_shape(
            type="rect", xref="x", yref="paper",
            x0=(freeze_date - timedelta(days=5)).strftime("%Y-%m-%d"),
            x1=(freeze_date + timedelta(days=5)).strftime("%Y-%m-%d"),
            y0=0, y1=1,
            fillcolor=COLORS["muted"], opacity=0.06,
            line_width=0,
        )
        fig.add_shape(
            type="line", xref="x", yref="paper",
            x0=freeze_date.strftime("%Y-%m-%d"),
            x1=freeze_date.strftime("%Y-%m-%d"),
            y0=0, y1=1,
            line=dict(color=COLORS["muted"], width=1, dash="dot"),
            opacity=0.5,
        )
        fig.add_annotation(
            x=freeze_date.strftime("%Y-%m-%d"), xref="x", yref="paper", y=1.02,
            text=f"Avg Last Freeze · {freeze_date.strftime('%b %d')}",
            showarrow=False,
            font=dict(size=9, color=COLORS["muted"]),
            xanchor="right",
        )

    # Left Y axis: time of day (0–24h), midnight at top
    tick_vals = [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24]
    tick_text = ["12am", "2am", "4am", "6am", "8am", "10am",
                 "12pm", "2pm", "4pm", "6pm", "8pm", "10pm", "12am"]

    fig.update_layout(**CHART_LAYOUT)
    fig.update_layout(
        height=420,
        margin=dict(l=10, r=60, t=20, b=20),
        xaxis=dict(showgrid=False, zeroline=False),
        yaxis=dict(
            tickvals=tick_vals,
            ticktext=tick_text,
            gridcolor="#f0e8d8",
            zeroline=False,
            title="Time of Day",
            range=[24, 0],
        ),
        yaxis2=dict(
            title="°F",
            overlaying="y",
            side="right",
            showgrid=False,
            range=[0, 100],
            tickvals=[0, 20, 40, 60, 80, 100],
            color=COLORS["muted"],
        ),
        legend=dict(orientation="h", y=1.08, x=0, font=dict(size=10)),
    )

    return fig


# ── Freeze date sidebar ───────────────────────────────────────────────────────
@app.callback(
    Output("freeze-date-display", "children"),
    Output("freeze-date-sub", "children"),
    Input("city-dropdown", "value")
)
def update_freeze_date(selected_city):
    if not selected_city:
        return "—", ""
    con = get_con()
    row = con.execute("""
        SELECT avg_last_freeze_all_time FROM avg_freeze_dates WHERE city = ?
    """, [selected_city]).fetchone()
    con.close()
    if not row:
        return "—", ""
    return pd.Timestamp(row[0]).strftime("%B %d"), f"{selected_city} · All-time historical avg"


# ── Plant table ───────────────────────────────────────────────────────────────
@app.callback(
    Output("plant-table", "data"),
    Output("plant-table", "columns"),
    Output("growing-season-filter", "options"),
    Output("harvest-type-filter", "options"),
    Input("city-dropdown", "value"),
    Input("growing-season-filter", "value"),
    Input("harvest-type-filter", "value"),
    Input("pollinator-filter", "value"),
)
def update_plant_table(selected_city, growing_season, harvest_type, pollinator):
    if not selected_city:
        return [], [], [], []
    con = get_con()
    season_opts = [{"label": r[0], "value": r[0]} for r in
        con.execute("SELECT DISTINCT growing_season FROM plants ORDER BY growing_season").fetchall()]
    type_opts = [{"label": r[0], "value": r[0]} for r in
        con.execute("SELECT DISTINCT harvest_type FROM plants ORDER BY harvest_type").fetchall()]
    query = """
        SELECT
            common_name AS "Plant",
            plant_family AS "Family",
            growing_season AS "Season",
            harvest_type AS "Type",
            CASE WHEN direct_sow THEN 'Direct'
                 ELSE CAST(weeks_indoor_before_transplant AS VARCHAR) || 'wk'
            END AS "Sow",
            CASE WHEN attracts_bees        THEN '🐝' ELSE '' END ||
            CASE WHEN attracts_butterflies THEN '🦋' ELSE '' END ||
            CASE WHEN attracts_hummingbirds THEN '🌺' ELSE '' END AS "Pollinators"
        FROM plants WHERE 1=1
    """
    params = []
    if growing_season:
        query += " AND growing_season = ?"; params.append(growing_season)
    if harvest_type:
        query += " AND harvest_type = ?"; params.append(harvest_type)
    if pollinator == "bees":
        query += " AND attracts_bees = true"
    elif pollinator == "butterflies":
        query += " AND attracts_butterflies = true"
    elif pollinator == "hummingbirds":
        query += " AND attracts_hummingbirds = true"
    query += " ORDER BY common_name"
    df = con.execute(query, params).df()
    con.close()
    return df.to_dict("records"), [{"name": c, "id": c} for c in df.columns], season_opts, type_opts


# ── Store selected plants ─────────────────────────────────────────────────────
@app.callback(
    Output("selected-plants-store", "data"),
    Input("plant-table", "selected_rows"),
    State("plant-table", "data"),
)
def store_selected_plants(selected_rows, table_data):
    if not selected_rows or not table_data:
        return []
    return [table_data[i]["Plant"] for i in selected_rows]


# ── Clear selection ───────────────────────────────────────────────────────────
@app.callback(
    Output("plant-table", "selected_rows"),
    Input("clear-button", "n_clicks"),
    prevent_initial_call=True,
)
def clear_selection(_):
    return []


# ── Plant cards ───────────────────────────────────────────────────────────────
@app.callback(
    Output("plant-cards", "children"),
    Input("city-dropdown", "value"),
    Input("selected-plants-store", "data"),
)
def update_plant_cards(selected_city, selected_plants):
    if not selected_city:
        return []
    if not selected_plants:
        return [html.P(
            "Select plants below to check this week's viability.",
            style={
                "color": COLORS["muted"], "fontStyle": "italic",
                "fontSize": "0.72rem", "gridColumn": "1/-1",
            },
        )]

    con = get_con()
    forecast = con.execute("""
        SELECT date, temp_max, temp_min FROM six_weeks_weather
        WHERE city = ? AND date >= CURRENT_DATE AND date < CURRENT_DATE + 7
        ORDER BY date
    """, [selected_city]).df()
    ph = ",".join(["?"] * len(selected_plants))
    plants = con.execute(f"""
        SELECT common_name, plant_family, min_viable_temp_f, max_viable_temp_f,
               attracts_bees, attracts_butterflies, attracts_hummingbirds,
               CASE WHEN direct_sow THEN 'Direct Sow'
                    ELSE CAST(weeks_indoor_before_transplant AS VARCHAR) || ' wks indoor'
               END AS sow_method
        FROM plants WHERE common_name IN ({ph})
    """, selected_plants).df()
    con.close()

    if forecast.empty:
        return [html.P("No forecast data.", style={"color": COLORS["muted"]})]

    cards = []
    for _, plant in plants.sort_values("common_name").iterrows():
        viable_days = forecast[
            (forecast["temp_max"] >= plant["min_viable_temp_f"]) &
            (forecast["temp_max"] <= plant["max_viable_temp_f"])
        ].shape[0]
        good = viable_days >= 4
        cards.append(html.Div([
            html.Span(plant["common_name"], className="plant-card-name"),
            html.Span(plant["plant_family"], className="plant-card-family"),
            html.Span(plant["sow_method"], className="plant-card-sow"),
            html.Span(
                f"{viable_days}/7 ✓" if good else f"{viable_days}/7 ✗",
                className="plant-card-viable-good" if good else "plant-card-viable-bad",
            ),
            html.Span(
                " ".join(filter(None, [
                    "🐝" if plant.get("attracts_bees") else "",
                    "🦋" if plant.get("attracts_butterflies") else "",
                    "🌺" if plant.get("attracts_hummingbirds") else "",
                ])),
                style={"fontSize": "0.7rem", "display": "block", "marginTop": "2px"},
            ),
        ], className="plant-card"))

    return cards


# ── Gantt ─────────────────────────────────────────────────────────────────────
@app.callback(
    Output("gantt-chart", "figure"),
    Input("city-dropdown", "value"),
    Input("selected-plants-store", "data"),
)
def update_gantt(selected_city, selected_plants):
    if not selected_city:
        return {}
    if not selected_plants:
        year = datetime.now().year
        today_str = datetime.now().strftime("%Y-%m-%d")
        # Dummy invisible scatter to force datetime x-axis type
        fig = go.Figure(go.Scatter(
            x=[f"{year}-01-01", f"{year}-12-31"],
            y=[0, 0],
            mode="markers",
            marker=dict(opacity=0),
            showlegend=False,
            hoverinfo="skip",
        ))
        fig.update_layout(**CHART_LAYOUT)
        fig.update_layout(
            height=120,
            xaxis=dict(
                range=[f"{year}-01-01", f"{year}-12-31"],
                tickformat="%b",
                dtick="M1",
                ticklabelmode="period",
                showgrid=True,
                gridcolor="#f0e8d8",
                zeroline=False,
            ),
            yaxis=dict(visible=False, range=[-1, 1]),
            annotations=[dict(
                text="Select plants from the sidebar to see planting windows",
                xref="paper", yref="paper", x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=12, color=COLORS["muted"], family="Lato"),
            )],
        )
        fig.add_shape(
            type="line", xref="x", yref="paper",
            x0=today_str, x1=today_str, y0=0, y1=1,
            line=dict(color=COLORS["terracotta"], width=1.5, dash="dash"),
            opacity=0.8,
        )
        fig.add_annotation(
            x=today_str, xref="x", yref="paper", y=1.02,
            text="Today", showarrow=False,
            font=dict(size=9, color=COLORS["terracotta"]),
            xanchor="center",
        )
        return fig

    con = get_con()
    ph = ",".join(["?"] * len(selected_plants))
    df = con.execute(f"""
        SELECT p.common_name, p.growing_season,
               pg.planting_start, pg.outdoor_start, pg.planting_end,
               p.attracts_bees, p.attracts_butterflies, p.attracts_hummingbirds
        FROM plants p
        JOIN planting_gantt pg
          ON p.growing_season = pg.growing_season
         AND p.harvest_type   = pg.harvest_type
        WHERE pg.city = ? AND p.common_name IN ({ph})
        ORDER BY p.growing_season, p.common_name
    """, [selected_city] + selected_plants).df()
    con.close()

    if df.empty:
        return {}

    for col in ["planting_start", "outdoor_start", "planting_end"]:
        df[col] = pd.to_datetime(df[col])

    rows = []
    pollinator_labels = {}  # common_name -> emoji string
    for _, row in df.iterrows():
        if row["planting_start"] < row["outdoor_start"]:
            rows.append({
                "Task": row["common_name"], "Season": row["growing_season"],
                "Start": row["planting_start"], "Finish": row["outdoor_start"],
                "Segment": "Indoor start",
            })
        rows.append({
            "Task": row["common_name"], "Season": row["growing_season"],
            "Start": row["outdoor_start"], "Finish": row["planting_end"],
            "Segment": "Outdoor",
        })
        icons = "".join(filter(None, [
            "🐝" if row.get("attracts_bees") else "",
            "🦋" if row.get("attracts_butterflies") else "",
            "🌺" if row.get("attracts_hummingbirds") else "",
        ]))
        if icons:
            pollinator_labels[row["common_name"]] = {"icons": icons}

    tdf = pd.DataFrame(rows)
    fig = px.timeline(
        tdf, x_start="Start", x_end="Finish", y="Task",
        color="Season", color_discrete_map=GANTT_COLORS,
        pattern_shape="Segment",
        pattern_shape_map={"Indoor start": "/", "Outdoor": ""},
        labels={"Task": ""},
    )
    fig.update_yaxes(autorange="reversed")

    # Static full-year x-axis — always Jan 1 to Dec 31 of current year
    year = datetime.now().year
    fig.update_xaxes(
        range=[f"{year}-01-01", f"{year}-12-31"],
        tickformat="%b",
        dtick="M1",
        ticklabelmode="period",
        showgrid=True,
        gridcolor="#f0e8d8",
        zeroline=False,
    )

    # Pollinator icons — fixed just right of y-axis labels, one per plant row
    for plant_name, info in pollinator_labels.items():
        fig.add_annotation(
            x=0, xref="paper",
            y=plant_name, yref="y",
            text=info["icons"],
            showarrow=False,
            font=dict(size=11),
            xanchor="right",
            yanchor="middle",
            xshift=-90,
        )

    # Today marker
    today_str = datetime.now().strftime("%Y-%m-%d")
    fig.add_shape(
        type="line", xref="x", yref="paper",
        x0=today_str, x1=today_str, y0=0, y1=1,
        line=dict(color=COLORS["terracotta"], width=1.5, dash="dash"),
        opacity=0.8,
    )
    fig.add_annotation(
        x=today_str, xref="x", yref="paper", y=1.02,
        text="Today", showarrow=False,
        font=dict(size=9, color=COLORS["terracotta"]),
        xanchor="center",
    )

    fig.update_layout(**CHART_LAYOUT)
    fig.update_layout(
        height=max(160, len(selected_plants) * 46),
        margin=dict(l=120, r=10, t=30, b=20),
    )
    return fig


# ── Export CSV ────────────────────────────────────────────────────────────────
@app.callback(
    Output("export-download", "data"),
    Input("export-button", "n_clicks"),
    State("selected-plants-store", "data"),
    State("city-dropdown", "value"),
    prevent_initial_call=True,
)
def export_selected_plants(_, selected_plants, selected_city):
    if not selected_plants:
        return None
    con = get_con()
    ph = ",".join(["?"] * len(selected_plants))
    df = con.execute(f"""
        SELECT p.common_name AS "Plant", p.plant_family AS "Family",
               p.growing_season AS "Season", p.harvest_type AS "Type",
               p.ideal_temp_min_f AS "Ideal Min", p.ideal_temp_max_f AS "Ideal Max",
               p.min_viable_temp_f AS "Min Viable", p.max_viable_temp_f AS "Max Viable",
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
         AND p.harvest_type   = pg.harvest_type
        WHERE pg.city = ? AND p.common_name IN ({ph})
        ORDER BY p.common_name
    """, [selected_city] + selected_plants).df()
    con.close()
    return dcc.send_data_frame(df.to_csv, "selected_plants.csv", index=False)


# ── Scheduler ─────────────────────────────────────────────────────────────────
def refresh_forecast():
    try:
        print("Starting scheduled forecast ingest...")
        run_forecast_ingest()
        print("Scheduled ingest complete")
    except Exception as e:
        print(f"Ingest failed safely: {e}")


if os.environ.get("WEB_CONCURRENCY", "1") == "1":
    try:
        scheduler = BackgroundScheduler(
            daemon=True,
            timezone=pytz.timezone("America/Los_Angeles"),
        )
        scheduler.add_job(refresh_forecast, "cron", hour=6, minute=0)
        scheduler.start()
        print("Scheduler started — daily at 6am Pacific")
    except Exception as e:
        print(f"Scheduler failed to start: {e}")


if __name__ == "__main__":
    app.run(debug=True)
