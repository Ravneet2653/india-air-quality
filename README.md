# India Air Quality — Data Pipeline

An end-to-end batch data pipeline that ingests hourly air-quality readings for
eight Indian cities, derives the Indian CPCB National Air Quality Index,
detects pollution spikes, and serves the result through a live dashboard.

**Live dashboard:** _add your Streamlit URL here after deploying_

---

## Architecture

```
   Open-Meteo Air Quality API          (source: free, keyless, hourly)
              │
              ▼
   ┌──────────────────────┐
   │  ingest.py           │  EXTRACT   8 cities, retry with backoff
   │                      │  TRANSFORM clean → CPCB AQI → rolling
   │                      │            baseline → spike flags
   │                      │  LOAD      idempotent UPSERT
   └──────────┬───────────┘
              │
              ▼
   ┌──────────────────────┐
   │ data/air_quality.db  │  SQLite warehouse
   │  • readings          │  one row per city per hour
   │  • ingest_runs       │  run log for observability
   └──────────┬───────────┘
              │
              ▼
   ┌──────────────────────┐
   │  app.py (Streamlit)  │  SERVE  trends, diurnal profile,
   └──────────────────────┘         band distribution, anomalies

   Orchestration: GitHub Actions cron, hourly
```

## Design decisions

**Idempotent loads.** Each reading gets a natural key of `city|timestamp`, and
the load is an `INSERT ... ON CONFLICT DO UPDATE`. Every run fetches a 2-day
window that deliberately overlaps what is already stored, so a missed or
retried run backfills itself rather than leaving a hole, and re-running the
same window never duplicates rows.

**Late-arriving corrections.** The API revises recent values as better sensor
data arrives. Because the load updates on conflict rather than skipping,
those corrections propagate instead of being silently ignored.

**Anomalies are relative, not absolute.** A spike is flagged when PM2.5 exceeds
its own city's 24-hour rolling mean by more than 2.5 standard deviations. A
fixed national threshold would flag Delhi almost permanently and Bengaluru
almost never, which tells you about the city rather than about an event.

**Run logging.** Every execution writes to `ingest_runs` with row counts and a
status. A pipeline that fails silently looks identical to one with no new
data, and the run log distinguishes them.

**Concurrency guard.** The workflow uses a concurrency group so two runs can
never write the database at once, and rebases before pushing.

## Schema

`readings` — one row per city per hour

| Column | Type | Notes |
|---|---|---|
| `reading_id` | TEXT | Primary key, `city\|timestamp` |
| `city`, `state` | TEXT | Location |
| `latitude`, `longitude` | REAL | Coordinates |
| `observed_at` | TEXT | ISO timestamp, IST |
| `date`, `hour`, `day_of_week` | — | Calendar features |
| `pm2_5`, `pm10`, `no2`, `o3`, `so2`, `co` | REAL | Concentrations, µg/m³ |
| `aqi` | REAL | CPCB sub-index from PM2.5 |
| `aqi_band` | TEXT | Good … Severe |
| `pm2_5_roll_24h`, `pm2_5_z` | REAL | Rolling baseline, z-score |
| `is_spike` | INTEGER | Anomaly flag |
| `ingested_at` | TEXT | Load timestamp |

`ingest_runs` — one row per pipeline execution, for observability.

## Running locally

```bash
pip install -r requirements.txt

python ingest.py --backfill    # ~60 days of history, run once
streamlit run app.py
```

Incremental runs (`python ingest.py`, no flag) fetch the last 2 days and are
what the hourly schedule executes.

## Deployment

Ingestion runs on GitHub Actions (`.github/workflows/ingest.yml`) hourly and
commits the updated database back to the repository. The dashboard is hosted
on Streamlit Community Cloud, pointed at `app.py` on the default branch.

No API keys or secrets are required.

## Possible extensions

- Swap SQLite for Postgres once the database outgrows comfortable git storage
- Add a forecasting model for next-24-hour AQI
- Data quality tests as a pipeline stage that fails the run on schema drift
- Alerting when a city crosses into the Severe band

---

Built by **Ravneet Kaur** · data from [Open-Meteo](https://open-meteo.com/)
