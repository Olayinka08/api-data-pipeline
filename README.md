# API Data Ingestion Pipeline

**Portfolio Project 3 · Olayinka Bello · Data / Analytics Engineering**

A Python ETL pipeline that ingests from three public REST APIs into a Bronze/Silver/Gold medallion architecture, runs automated data quality checks, and publishes a live GitHub Pages dashboard.

---

## Live Dashboard

**https://olayinka08.github.io/api-data-pipeline/**

---

## Architecture

```
Open-Meteo (weather) ──┐
REST Countries        ──┼──► Bronze (raw JSON) ──► Silver (flat, typed) ──► Gold (aggregated) ──► Dashboard
ExchangeRate-API (FX) ──┘
```

Three sources, zero API keys. The pipeline runs anywhere with `python pipeline.py`.

| Layer | What it contains |
|---|---|
| **Bronze** | Raw API responses, timestamped, never modified |
| **Silver** | Flattened, typed fields — one row per entity |
| **Gold** | Aggregated summaries consumed by the dashboard |

---

## Sources

| API | Data | Endpoint |
|---|---|---|
| [Open-Meteo](https://open-meteo.com/) | Current weather for 5 UK cities | `api.open-meteo.com/v1/forecast` |
| [REST Countries](https://restcountries.com/) | Population, area, density for 10 countries | `restcountries.com/v3.1/alpha` |
| [ExchangeRate-API](https://open.er-api.com/) | GBP exchange rates (10 pairs) | `open.er-api.com/v6/latest/GBP` |

---

## Quick start

```bash
git clone https://github.com/Olayinka08/api-data-pipeline.git
cd api-data-pipeline
pip install -r requirements.txt

# Run the full pipeline (fetches live data)
python pipeline.py

# Run a single source
python pipeline.py --source weather
python pipeline.py --source countries
python pipeline.py --source fx

# Validate config without fetching
python pipeline.py --dry-run

# Seed static demo data (for the dashboard when live APIs are unavailable)
python seed_demo_data.py
```

---

## Tests

```bash
pytest tests/ -v
```

19 unit tests covering:
- Fetch helper: success, retry on transient failure, exhausted retries
- Bronze write/read helpers
- Weather, Countries, and FX transforms (Bronze to Silver to Gold)
- Quality checker: all checks pass on valid data, report written to Gold, failures detected

Tests use monkeypatching throughout; no network calls are made.

---

## Project structure

```
api-data-pipeline/
├── pipeline.py             Main pipeline (ingest, transform, quality, manifest)
├── seed_demo_data.py       Static seed data for the dashboard
├── requirements.txt
├── index.html              GitHub Pages dashboard
├── pipeline_manifest.json  Dashboard data layer (auto-generated)
├── bronze/                 Raw API responses (timestamped JSON)
├── silver/                 Flattened Silver files
│   ├── weather_current.json
│   ├── countries.json
│   └── fx_rates.json
├── gold/                   Aggregated Gold summaries
│   ├── weather_summary.json
│   ├── countries_summary.json
│   ├── fx_summary.json
│   └── quality_report.json
├── logs/                   Per-run pipeline logs
├── tests/
│   └── test_pipeline.py    19 unit tests
└── .github/
    └── workflows/
        └── ci.yml          GitHub Actions: pytest on push
```

---

## Design decisions

**Retry with exponential back-off.** Each `fetch()` call retries up to 3 times with 2ⁿ-second delays, logging every attempt. Transient network errors never silently drop data.

**Quality checks before Gold.** 13 assertions run against Silver data on every full pipeline run: row counts, null checks, plausible value ranges, and presence of key entities. Results are written to `gold/quality_report.json` and surfaced on the dashboard.

**Manifest-driven dashboard.** `pipeline_manifest.json` is the single entry point for `index.html`. The dashboard reads it first, then fetches individual Gold JSON files with relative `fetch()` calls, so the HTML never needs to change when the pipeline adds new sources.

**Seed data for static hosting.** GitHub Pages cannot run Python. `seed_demo_data.py` writes realistic static Gold files so the dashboard always has something to show. Run `python pipeline.py` locally to overwrite with live API data.

---

## CI

GitHub Actions runs `pytest tests/ -v` on every push to `main`. Badge:

![CI](https://github.com/Olayinka08/api-data-pipeline/actions/workflows/ci.yml/badge.svg)

---

*Olayinka Bello · [linkedin.com/in/olayinka-bello-mba-04960b12a](https://linkedin.com/in/olayinka-bello-mba-04960b12a)*
