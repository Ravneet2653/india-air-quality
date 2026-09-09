"""
Serving layer for the India Air Quality pipeline.

Reads the SQLite warehouse produced by ingest.py and presents trends,
diurnal patterns, city comparisons and anomaly detection.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

DB_PATH = Path(__file__).parent / "data" / "air_quality.db"

BAND_COLOURS = {
    "Good": "#2e9e5b",
    "Satisfactory": "#8bc34a",
    "Moderate": "#f2c200",
    "Poor": "#f28c28",
    "Very Poor": "#e34a33",
    "Severe": "#8b0000",
}

st.set_page_config(
    page_title="India Air Quality Pipeline",
    page_icon="🌫️",
    layout="wide",
)


# --------------------------------------------------------------------------
# Data access
# --------------------------------------------------------------------------

@st.cache_data(ttl=900)
def load_readings() -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query("SELECT * FROM readings", conn)
    if df.empty:
        return df
    df["observed_at"] = pd.to_datetime(df["observed_at"])
    df["is_spike"] = df["is_spike"].astype(bool)
    return df


@st.cache_data(ttl=900)
def load_runs() -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(
            "SELECT * FROM ingest_runs ORDER BY run_id DESC LIMIT 10", conn
        )


data = load_readings()

st.title("🌫️ India Air Quality — Data Pipeline")
st.caption(
    "Hourly ingestion from the Open-Meteo Air Quality API → transformation and "
    "CPCB AQI derivation → SQLite warehouse → this dashboard. "
    "Scheduled via GitHub Actions."
)

if data.empty:
    st.warning(
        "No data yet. Run `python ingest.py --backfill` locally and commit "
        "`data/air_quality.db`, or wait for the first scheduled run."
    )
    st.stop()


# --------------------------------------------------------------------------
# Sidebar filters
# --------------------------------------------------------------------------

with st.sidebar:
    st.header("Filters")

    cities = sorted(data["city"].unique())
    default_cities = [c for c in ("Delhi", "Patiala", "Bengaluru") if c in cities]
    chosen = st.multiselect("Cities", cities, default=default_cities or cities[:3])

    min_date = data["observed_at"].min().date()
    max_date = data["observed_at"].max().date()
    date_range = st.date_input(
        "Date range",
        value=(max(min_date, max_date - pd.Timedelta(days=14)), max_date),
        min_value=min_date,
        max_value=max_date,
    )

    pollutant = st.selectbox(
        "Pollutant",
        options=["pm2_5", "pm10", "no2", "o3", "so2", "co"],
        format_func=lambda c: {
            "pm2_5": "PM2.5", "pm10": "PM10", "no2": "NO₂",
            "o3": "O₃", "so2": "SO₂", "co": "CO",
        }[c],
    )

    st.divider()
    st.caption(f"Warehouse: **{len(data):,}** readings")
    st.caption(f"Latest: {data['observed_at'].max():%d %b %Y, %H:%M} IST")

if not chosen:
    st.info("Select at least one city.")
    st.stop()

start, end = (date_range if isinstance(date_range, tuple) and len(date_range) == 2
              else (min_date, max_date))

view = data[
    data["city"].isin(chosen)
    & (data["observed_at"].dt.date >= start)
    & (data["observed_at"].dt.date <= end)
].copy()

if view.empty:
    st.info("No readings in that window.")
    st.stop()


# --------------------------------------------------------------------------
# Headline metrics — latest reading per city
# --------------------------------------------------------------------------

latest = view.sort_values("observed_at").groupby("city").tail(1)

st.subheader("Latest readings")
columns = st.columns(len(latest))
for col, (_, row) in zip(columns, latest.iterrows()):
    day_ago = view[
        (view["city"] == row["city"])
        & (view["observed_at"] <= row["observed_at"] - pd.Timedelta(hours=24))
    ]
    delta = None
    if not day_ago.empty:
        previous = day_ago.iloc[-1]["aqi"]
        if pd.notna(previous) and pd.notna(row["aqi"]):
            delta = f"{row['aqi'] - previous:+.0f} vs 24h ago"

    col.metric(
        label=f"{row['city']} — {row['aqi_band']}",
        value=f"AQI {row['aqi']:.0f}" if pd.notna(row["aqi"]) else "—",
        delta=delta,
        delta_color="inverse",  # rising AQI is bad
    )

st.divider()


# --------------------------------------------------------------------------
# Trend
# --------------------------------------------------------------------------

label = {"pm2_5": "PM2.5", "pm10": "PM10", "no2": "NO₂",
         "o3": "O₃", "so2": "SO₂", "co": "CO"}[pollutant]

st.subheader(f"{label} over time")

trend = (
    alt.Chart(view)
    .mark_line(strokeWidth=1.6, opacity=0.85)
    .encode(
        x=alt.X("observed_at:T", title=None),
        y=alt.Y(f"{pollutant}:Q", title=f"{label} (µg/m³)",
                scale=alt.Scale(zero=True, nice=True)),
        color=alt.Color("city:N", title="City"),
        tooltip=[
            alt.Tooltip("city:N", title="City"),
            alt.Tooltip("observed_at:T", title="Time", format="%d %b %H:%M"),
            alt.Tooltip(f"{pollutant}:Q", title=label, format=".1f"),
            alt.Tooltip("aqi:Q", title="AQI", format=".0f"),
        ],
    )
    .properties(height=320)
    .interactive()
)
st.altair_chart(trend, width="stretch")


# --------------------------------------------------------------------------
# Diurnal pattern + band distribution
# --------------------------------------------------------------------------

left, right = st.columns(2)

with left:
    st.subheader("Average by hour of day")
    diurnal = view.groupby(["city", "hour"], as_index=False)[pollutant].mean()
    st.altair_chart(
        alt.Chart(diurnal)
        .mark_line(point=True, strokeWidth=2)
        .encode(
            x=alt.X("hour:Q", title="Hour (IST)",
                    scale=alt.Scale(domain=[0, 23]),
                    axis=alt.Axis(values=list(range(0, 24, 3)))),
            y=alt.Y(f"{pollutant}:Q", title=f"Mean {label}"),
            color=alt.Color("city:N", title="City"),
            tooltip=["city:N", "hour:Q", alt.Tooltip(f"{pollutant}:Q", format=".1f")],
        )
        .properties(height=300),
        width="stretch",
    )
    st.caption(
        "Concentrations peak overnight and fall through the middle of the day: "
        "the atmospheric mixing layer collapses after sunset, trapping "
        "pollutants near the surface, then daytime convection disperses them."
    )

with right:
    st.subheader("Share of hours in each AQI band")
    share = (
        view.groupby(["city", "aqi_band"], as_index=False)
        .size()
        .rename(columns={"size": "hours"})
    )
    order = list(BAND_COLOURS.keys())
    st.altair_chart(
        alt.Chart(share)
        .mark_bar()
        .encode(
            x=alt.X("hours:Q", stack="normalize", title="Share of hours",
                    axis=alt.Axis(format="%")),
            y=alt.Y("city:N", title=None),
            color=alt.Color(
                "aqi_band:N", title="CPCB band",
                scale=alt.Scale(domain=order, range=[BAND_COLOURS[b] for b in order]),
                sort=order,
            ),
            tooltip=["city:N", "aqi_band:N", "hours:Q"],
        )
        .properties(height=300),
        width="stretch",
    )
    st.caption("Bands follow the CPCB National Air Quality Index scale.")


# --------------------------------------------------------------------------
# Anomalies
# --------------------------------------------------------------------------

st.divider()
st.subheader("Detected pollution spikes")
st.caption(
    "A reading is flagged when PM2.5 both exceeds its own 24-hour rolling mean "
    "by more than 2.5 standard deviations and sits above 30 µg/m³. Each city is "
    "judged against its own baseline, but the absolute floor stops a clean city "
    "reporting harmless fluctuations as pollution events."
)

spikes = view[view["is_spike"]].sort_values("observed_at", ascending=False)
if spikes.empty:
    st.success("No spikes in the selected window.")
else:
    st.dataframe(
        spikes[["city", "observed_at", "pm2_5", "pm2_5_roll_24h", "pm2_5_z",
                "aqi", "aqi_band"]]
        .head(50)
        .rename(columns={
            "city": "City", "observed_at": "Observed (IST)", "pm2_5": "PM2.5",
            "pm2_5_roll_24h": "24h baseline", "pm2_5_z": "Z-score",
            "aqi": "AQI", "aqi_band": "Band",
        })
        .style.format({
            "PM2.5": "{:.1f}", "24h baseline": "{:.1f}",
            "Z-score": "{:.2f}", "AQI": "{:.0f}",
        }),
        width="stretch",
        hide_index=True,
    )


# --------------------------------------------------------------------------
# Pipeline health
# --------------------------------------------------------------------------

with st.expander("Pipeline health — recent ingestion runs"):
    runs = load_runs()
    if runs.empty:
        st.write("No runs recorded yet.")
    else:
        st.dataframe(
            runs.rename(columns={
                "run_id": "Run", "started_at": "Started", "finished_at": "Finished",
                "mode": "Mode", "cities": "Cities", "rows_seen": "Rows seen",
                "rows_written": "Rows written", "status": "Status",
            }),
            width="stretch",
            hide_index=True,
        )
    st.caption(
        "Every run is logged, so a silent failure is visible here rather than "
        "showing up as a quietly stale dashboard."
    )

st.divider()
st.caption(
    f"Built by Ravneet Kaur · data from Open-Meteo · "
    f"dashboard rendered {datetime.now():%d %b %Y %H:%M}"
)
