"""
Ingestion layer for the India Air Quality pipeline.

Fetches hourly air-quality readings from the Open-Meteo Air Quality API
(free, no API key), normalises them into a tidy schema, and upserts them
into a local SQLite warehouse.

Run modes
---------
  python ingest.py              # incremental: last 2 days, hourly cron
  python ingest.py --backfill   # historical: last 60 days, run once at setup

The upsert is idempotent: re-running over a window that is already stored
updates those rows instead of duplicating them, so a retried or overlapping
cron run can never corrupt the series.
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

DB_PATH = Path(__file__).parent / "data" / "air_quality.db"
API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

# Pollutants requested from the API. Keys are the API's field names; the
# values are the column names used in the warehouse.
POLLUTANTS = {
    "pm2_5": "pm2_5",
    "pm10": "pm10",
    "nitrogen_dioxide": "no2",
    "ozone": "o3",
    "sulphur_dioxide": "so2",
    "carbon_monoxide": "co",
}

CITIES = [
    {"city": "Delhi",     "state": "Delhi",           "lat": 28.6139, "lon": 77.2090},
    {"city": "Mumbai",    "state": "Maharashtra",     "lat": 19.0760, "lon": 72.8777},
    {"city": "Bengaluru", "state": "Karnataka",       "lat": 12.9716, "lon": 77.5946},
    {"city": "Kolkata",   "state": "West Bengal",     "lat": 22.5726, "lon": 88.3639},
    {"city": "Chennai",   "state": "Tamil Nadu",      "lat": 13.0827, "lon": 80.2707},
    {"city": "Hyderabad", "state": "Telangana",       "lat": 17.3850, "lon": 78.4867},
    {"city": "Patiala",   "state": "Punjab",          "lat": 30.3398, "lon": 76.3869},
    {"city": "Lucknow",   "state": "Uttar Pradesh",   "lat": 26.8467, "lon": 80.9462},
]

# CPCB (Indian National AQI) breakpoints for PM2.5, in ug/m3.
# (concentration_low, concentration_high, aqi_low, aqi_high)
PM25_BREAKPOINTS = [
    (0.0,   30.0,    0,  50),
    (30.0,  60.0,   51, 100),
    (60.0,  90.0,  101, 200),
    (90.0, 120.0,  201, 300),
    (120.0, 250.0, 301, 400),
    (250.0, 1000.0, 401, 500),
]

AQI_BANDS = [
    (0,   50,  "Good"),
    (51,  100, "Satisfactory"),
    (101, 200, "Moderate"),
    (201, 300, "Poor"),
    (301, 400, "Very Poor"),
    (401, 500, "Severe"),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ingest")


# --------------------------------------------------------------------------
# Extract
# --------------------------------------------------------------------------

def fetch_city(city: dict, past_days: int, retries: int = 3) -> pd.DataFrame:
    """Fetch hourly readings for one city. Returns a tidy long-ish frame."""
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "hourly": ",".join(POLLUTANTS.keys()),
        "past_days": past_days,
        "forecast_days": 1,
        "timezone": "Asia/Kolkata",
    }

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(API_URL, params=params, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            break
        except (requests.RequestException, ValueError) as exc:
            if attempt == retries:
                log.error("%s: giving up after %d attempts (%s)", city["city"], retries, exc)
                return pd.DataFrame()
            wait = 2 ** attempt
            log.warning("%s: attempt %d failed (%s); retrying in %ds",
                        city["city"], attempt, exc, wait)
            time.sleep(wait)

    hourly = payload.get("hourly")
    if not hourly or "time" not in hourly:
        log.error("%s: response had no hourly block", city["city"])
        return pd.DataFrame()

    frame = pd.DataFrame({"observed_at": hourly["time"]})
    for api_name, column in POLLUTANTS.items():
        frame[column] = hourly.get(api_name)

    frame["city"] = city["city"]
    frame["state"] = city["state"]
    frame["latitude"] = city["lat"]
    frame["longitude"] = city["lon"]

    log.info("%-10s fetched %4d rows", city["city"], len(frame))
    return frame


# --------------------------------------------------------------------------
# Transform
# --------------------------------------------------------------------------

def pm25_to_aqi(value: float) -> float | None:
    """Convert a PM2.5 concentration to an Indian CPCB AQI sub-index."""
    if value is None or pd.isna(value) or value < 0:
        return None
    for c_low, c_high, a_low, a_high in PM25_BREAKPOINTS:
        if c_low <= value <= c_high:
            # Linear interpolation within the band.
            return round(
                (a_high - a_low) / (c_high - c_low) * (value - c_low) + a_low
            )
    return 500.0  # above the top breakpoint


def aqi_band(aqi: float) -> str | None:
    if aqi is None or pd.isna(aqi):
        return None
    for low, high, label in AQI_BANDS:
        if low <= aqi <= high:
            return label
    return "Severe"


def transform(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean, derive AQI, flag anomalies, and drop unusable rows."""
    if raw.empty:
        return raw

    df = raw.copy()
    df["observed_at"] = pd.to_datetime(df["observed_at"], errors="coerce")
    df = df.dropna(subset=["observed_at"])

    # Negative concentrations are sensor errors, not real readings.
    for column in POLLUTANTS.values():
        df[column] = pd.to_numeric(df[column], errors="coerce")
        df.loc[df[column] < 0, column] = pd.NA

    # A row with no PM2.5 tells us nothing useful; drop it.
    df = df.dropna(subset=["pm2_5"])
    if df.empty:
        return df

    df["aqi"] = df["pm2_5"].apply(pm25_to_aqi)
    df["aqi_band"] = df["aqi"].apply(aqi_band)

    # Calendar features, used by the dashboard for diurnal patterns.
    df["date"] = df["observed_at"].dt.date.astype(str)
    df["hour"] = df["observed_at"].dt.hour
    df["day_of_week"] = df["observed_at"].dt.day_name()

    # Per-city rolling baseline and a simple z-score anomaly flag.
    df = df.sort_values(["city", "observed_at"])
    grouped = df.groupby("city")["pm2_5"]
    df["pm2_5_roll_24h"] = grouped.transform(
        lambda s: s.rolling(24, min_periods=6).mean()
    )
    rolling_std = grouped.transform(lambda s: s.rolling(24, min_periods=6).std())
    df["pm2_5_z"] = (df["pm2_5"] - df["pm2_5_roll_24h"]) / rolling_std

    # A spike must be both a sharp rise against the city's own baseline AND a
    # concentration that actually matters. Without the absolute floor, a clean
    # city produces "spikes" at 12 ug/m3 that are statistically real but
    # meaningless as air quality events.
    SPIKE_Z = 2.5
    SPIKE_FLOOR = 30.0  # CPCB Good/Satisfactory boundary for PM2.5
    df["is_spike"] = (
        (df["pm2_5_z"] > SPIKE_Z) & (df["pm2_5"] >= SPIKE_FLOOR)
    ).fillna(False)

    df["ingested_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    # Natural key for the warehouse.
    df["reading_id"] = df["city"] + "|" + df["observed_at"].dt.strftime("%Y-%m-%dT%H:%M")

    return df


# --------------------------------------------------------------------------
# Load
# --------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    reading_id      TEXT PRIMARY KEY,
    city            TEXT NOT NULL,
    state           TEXT,
    latitude        REAL,
    longitude       REAL,
    observed_at     TEXT NOT NULL,
    date            TEXT,
    hour            INTEGER,
    day_of_week     TEXT,
    pm2_5           REAL,
    pm10            REAL,
    no2             REAL,
    o3              REAL,
    so2             REAL,
    co              REAL,
    aqi             REAL,
    aqi_band        TEXT,
    pm2_5_roll_24h  REAL,
    pm2_5_z         REAL,
    is_spike        INTEGER,
    ingested_at     TEXT
);
CREATE INDEX IF NOT EXISTS idx_city_time ON readings (city, observed_at);
CREATE INDEX IF NOT EXISTS idx_observed  ON readings (observed_at);

CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at   TEXT,
    finished_at  TEXT,
    mode         TEXT,
    cities       INTEGER,
    rows_seen    INTEGER,
    rows_written INTEGER,
    status       TEXT
);
"""

COLUMNS = [
    "reading_id", "city", "state", "latitude", "longitude", "observed_at",
    "date", "hour", "day_of_week", "pm2_5", "pm10", "no2", "o3", "so2", "co",
    "aqi", "aqi_band", "pm2_5_roll_24h", "pm2_5_z", "is_spike", "ingested_at",
]


def connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    return conn


def upsert(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """Insert new readings, update existing ones. Idempotent by reading_id."""
    if df.empty:
        return 0

    frame = df.reindex(columns=COLUMNS).copy()
    frame["is_spike"] = frame["is_spike"].astype("boolean").fillna(False).astype(int)
    frame["observed_at"] = pd.to_datetime(frame["observed_at"]).dt.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    frame = frame.astype(object).where(pd.notna(frame), None)

    placeholders = ", ".join("?" for _ in COLUMNS)
    updates = ", ".join(f"{c}=excluded.{c}" for c in COLUMNS if c != "reading_id")
    sql = (
        f"INSERT INTO readings ({', '.join(COLUMNS)}) VALUES ({placeholders}) "
        f"ON CONFLICT(reading_id) DO UPDATE SET {updates}"
    )

    conn.executemany(sql, frame.itertuples(index=False, name=None))
    conn.commit()
    return len(frame)


def log_run(conn, started, mode, cities, seen, written, status) -> None:
    conn.execute(
        "INSERT INTO ingest_runs "
        "(started_at, finished_at, mode, cities, rows_seen, rows_written, status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            started,
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
            mode, cities, seen, written, status,
        ),
    )
    conn.commit()


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def run(past_days: int, mode: str) -> int:
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    conn = connect()

    frames, failures = [], 0
    for city in CITIES:
        frame = fetch_city(city, past_days=past_days)
        if frame.empty:
            failures += 1
        else:
            frames.append(frame)
        time.sleep(0.5)  # be polite to a free API

    if not frames:
        log.error("no city returned data; aborting without writing")
        log_run(conn, started, mode, 0, 0, 0, "failed")
        return 1

    raw = pd.concat(frames, ignore_index=True)
    clean = transform(raw)
    written = upsert(conn, clean)

    status = "ok" if failures == 0 else f"partial ({failures} city failures)"
    log_run(conn, started, mode, len(frames), len(raw), written, status)

    total = conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
    log.info("wrote %d rows (%s); warehouse now holds %d readings", written, status, total)
    conn.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="India air quality ingestion")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="fetch the last 60 days instead of the last 2",
    )
    args = parser.parse_args()

    past_days = 60 if args.backfill else 2
    mode = "backfill" if args.backfill else "incremental"
    log.info("starting %s ingestion (past_days=%d)", mode, past_days)
    return run(past_days=past_days, mode=mode)


if __name__ == "__main__":
    sys.exit(main())
