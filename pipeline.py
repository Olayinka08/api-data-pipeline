"""
API Data Ingestion Pipeline
============================
Multi-source ingestion from 3 public APIs into a Bronze/Silver/Gold
medallion architecture. Runs locally with no API keys required.

Sources:
  - Open-Meteo  (weather forecasts for 5 UK cities)
  - REST Countries (country reference data)
  - ExchangeRate-API open endpoint (GBP FX rates)

Usage:
  python pipeline.py            # full run
  python pipeline.py --source weather
  python pipeline.py --source countries
  python pipeline.py --source fx
  python pipeline.py --dry-run  # validate without writing
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).parent
BRONZE = ROOT / "bronze"
SILVER = ROOT / "silver"
GOLD = ROOT / "gold"
LOGS = ROOT / "logs"

for d in (BRONZE, SILVER, GOLD, LOGS):
    d.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log_file = LOGS / f"pipeline_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch(url: str, params: dict | None = None, retries: int = 3) -> dict | list:
    """GET with retries and basic error handling."""
    for attempt in range(1, retries + 1):
        try:
            log.info("GET %s (attempt %d/%d)", url, attempt, retries)
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as exc:
            log.warning("Request failed: %s", exc)
            if attempt < retries:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"All {retries} attempts failed for {url}")


def write_bronze(source: str, data: Any) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = BRONZE / f"{source}_{ts}.json"
    path.write_text(json.dumps({"_ingested_at": now_iso(), "data": data}, indent=2))
    log.info("Bronze written: %s (%d bytes)", path.name, path.stat().st_size)
    return path


def latest_bronze(source: str) -> Path | None:
    files = sorted(BRONZE.glob(f"{source}_*.json"), reverse=True)
    return files[0] if files else None


# ---------------------------------------------------------------------------
# Source 1: Open-Meteo — hourly weather for 5 UK cities
# ---------------------------------------------------------------------------

CITIES = {
    "london":     (51.5074, -0.1278),
    "manchester": (53.4808, -2.2426),
    "birmingham": (52.4862, -1.8904),
    "edinburgh":  (55.9533, -3.1883),
    "bristol":    (51.4545, -2.5879),
}

WEATHER_URL = "https://api.open-meteo.com/v1/forecast"


def ingest_weather() -> Path:
    log.info("=== INGEST: weather ===")
    results = {}
    for city, (lat, lon) in CITIES.items():
        raw = fetch(
            WEATHER_URL,
            params={
                "latitude": lat,
                "longitude": lon,
                "current_weather": True,
                "hourly": "temperature_2m,precipitation,windspeed_10m,relative_humidity_2m",
                "forecast_days": 3,
                "timezone": "Europe/London",
            },
        )
        results[city] = raw
        log.info("  %s: %.1f°C", city, raw["current_weather"]["temperature"])
    return write_bronze("weather", results)


def transform_weather():
    """Bronze → Silver: flatten current conditions per city."""
    log.info("=== TRANSFORM: weather ===")
    src = latest_bronze("weather")
    if not src:
        log.warning("No bronze weather file found — run ingest first")
        return
    raw = json.loads(src.read_text())
    cities_data = raw["data"]
    ingested_at = raw["_ingested_at"]

    rows = []
    for city, data in cities_data.items():
        cw = data["current_weather"]
        rows.append({
            "city": city,
            "temperature_c": cw["temperature"],
            "windspeed_kmh": cw["windspeed"],
            "weathercode": cw["weathercode"],
            "is_day": bool(cw["is_day"]),
            "ingested_at": ingested_at,
            "source_file": src.name,
        })

    silver_path = SILVER / "weather_current.json"
    silver_path.write_text(json.dumps(rows, indent=2))
    log.info("Silver written: %s (%d rows)", silver_path.name, len(rows))

    # Gold: summary stats
    temps = [r["temperature_c"] for r in rows]
    gold_data = {
        "generated_at": now_iso(),
        "city_count": len(rows),
        "avg_temp_c": round(sum(temps) / len(temps), 1),
        "max_temp_c": max(temps),
        "min_temp_c": min(temps),
        "hottest_city": max(rows, key=lambda r: r["temperature_c"])["city"],
        "coldest_city": min(rows, key=lambda r: r["temperature_c"])["city"],
        "cities": rows,
    }
    gold_path = GOLD / "weather_summary.json"
    gold_path.write_text(json.dumps(gold_data, indent=2))
    log.info("Gold written: %s", gold_path.name)
    return gold_data


# ---------------------------------------------------------------------------
# Source 2: REST Countries — reference data for selected countries
# ---------------------------------------------------------------------------

COUNTRIES_URL = "https://restcountries.com/v3.1/alpha"
COUNTRY_CODES = ["gb", "us", "de", "fr", "ng", "jp", "au", "ca", "sg", "za"]


def ingest_countries() -> Path:
    log.info("=== INGEST: countries ===")
    codes = ",".join(COUNTRY_CODES)
    raw = fetch(f"https://restcountries.com/v3.1/alpha?codes={codes}")
    return write_bronze("countries", raw)


def transform_countries():
    log.info("=== TRANSFORM: countries ===")
    src = latest_bronze("countries")
    if not src:
        log.warning("No bronze countries file found — run ingest first")
        return
    raw = json.loads(src.read_text())
    countries_data = raw["data"]
    ingested_at = raw["_ingested_at"]

    rows = []
    for c in countries_data:
        name = c.get("name", {}).get("common", "Unknown")
        capital = c.get("capital", ["Unknown"])[0] if c.get("capital") else "Unknown"
        region = c.get("region", "Unknown")
        sub = c.get("subregion", "Unknown")
        population = c.get("population", 0)
        area = c.get("area", 0)
        currencies = list(c.get("currencies", {}).keys())
        languages = list(c.get("languages", {}).values())
        cca2 = c.get("cca2", "")

        rows.append({
            "cca2": cca2,
            "name": name,
            "capital": capital,
            "region": region,
            "subregion": sub,
            "population": population,
            "area_km2": area,
            "pop_density": round(population / area, 2) if area else None,
            "currencies": currencies,
            "languages": languages,
            "ingested_at": ingested_at,
        })

    silver_path = SILVER / "countries.json"
    silver_path.write_text(json.dumps(rows, indent=2))
    log.info("Silver written: %s (%d rows)", silver_path.name, len(rows))

    total_pop = sum(r["population"] for r in rows)
    gold_data = {
        "generated_at": now_iso(),
        "country_count": len(rows),
        "total_population": total_pop,
        "largest_by_population": max(rows, key=lambda r: r["population"])["name"],
        "largest_by_area": max(rows, key=lambda r: r["area_km2"] or 0)["name"],
        "most_dense": max(rows, key=lambda r: r["pop_density"] or 0)["name"],
        "countries": rows,
    }
    gold_path = GOLD / "countries_summary.json"
    gold_path.write_text(json.dumps(gold_data, indent=2))
    log.info("Gold written: %s", gold_path.name)
    return gold_data


# ---------------------------------------------------------------------------
# Source 3: ExchangeRate-API — GBP exchange rates (open endpoint, no key)
# ---------------------------------------------------------------------------

FX_URL = "https://open.er-api.com/v6/latest/GBP"
TARGET_CURRENCIES = ["USD", "EUR", "JPY", "AUD", "CAD", "NGN", "ZAR", "SGD", "INR", "CHF"]


def ingest_fx() -> Path:
    log.info("=== INGEST: fx ===")
    raw = fetch(FX_URL)
    return write_bronze("fx", raw)


def transform_fx():
    log.info("=== TRANSFORM: fx ===")
    src = latest_bronze("fx")
    if not src:
        log.warning("No bronze fx file found — run ingest first")
        return
    raw = json.loads(src.read_text())
    fx_data = raw["data"]
    ingested_at = raw["_ingested_at"]

    all_rates = fx_data.get("rates", {})
    rows = [
        {
            "base": "GBP",
            "target": currency,
            "rate": all_rates.get(currency),
            "last_updated": fx_data.get("time_last_update_utc"),
            "ingested_at": ingested_at,
        }
        for currency in TARGET_CURRENCIES
        if currency in all_rates
    ]

    silver_path = SILVER / "fx_rates.json"
    silver_path.write_text(json.dumps(rows, indent=2))
    log.info("Silver written: %s (%d rates)", silver_path.name, len(rows))

    gold_data = {
        "generated_at": now_iso(),
        "base_currency": "GBP",
        "rate_count": len(rows),
        "last_updated": fx_data.get("time_last_update_utc"),
        "rates": {r["target"]: r["rate"] for r in rows},
    }
    gold_path = GOLD / "fx_summary.json"
    gold_path.write_text(json.dumps(gold_data, indent=2))
    log.info("Gold written: %s", gold_path.name)
    return gold_data


# ---------------------------------------------------------------------------
# Data quality checks
# ---------------------------------------------------------------------------

def run_quality_checks() -> dict:
    """Run basic quality assertions on Silver data and report results."""
    log.info("=== QUALITY CHECKS ===")
    results = {"passed": 0, "failed": 0, "checks": []}

    def check(name: str, condition: bool, detail: str = ""):
        status = "PASS" if condition else "FAIL"
        results["checks"].append({"name": name, "status": status, "detail": detail})
        results["passed" if condition else "failed"] += 1
        log.info("  [%s] %s%s", status, name, f" — {detail}" if detail else "")

    # Weather checks
    wp = SILVER / "weather_current.json"
    if wp.exists():
        rows = json.loads(wp.read_text())
        check("weather: row count == 5", len(rows) == 5, f"got {len(rows)}")
        check("weather: no null temperatures", all(r["temperature_c"] is not None for r in rows))
        check("weather: temps in plausible range (-20 to 45)",
              all(-20 <= r["temperature_c"] <= 45 for r in rows))
        expected = set(CITIES.keys())
        actual = {r["city"] for r in rows}
        check("weather: all 5 cities present", expected == actual,
              f"missing: {expected - actual}")
    else:
        check("weather: silver file exists", False, str(wp))

    # Countries checks
    cp = SILVER / "countries.json"
    if cp.exists():
        rows = json.loads(cp.read_text())
        check("countries: row count == 10", len(rows) == 10, f"got {len(rows)}")
        check("countries: no null names", all(r["name"] for r in rows))
        check("countries: no null populations", all(r["population"] is not None for r in rows))
        check("countries: positive populations", all(r["population"] > 0 for r in rows))
        check("countries: gb present", any(r["cca2"] == "GB" for r in rows))
    else:
        check("countries: silver file exists", False, str(cp))

    # FX checks
    fxp = SILVER / "fx_rates.json"
    if fxp.exists():
        rows = json.loads(fxp.read_text())
        check("fx: at least 8 rates returned", len(rows) >= 8, f"got {len(rows)}")
        check("fx: no null rates", all(r["rate"] is not None for r in rows))
        check("fx: positive rates", all(r["rate"] > 0 for r in rows))
        usd = next((r for r in rows if r["target"] == "USD"), None)
        check("fx: USD rate in plausible range (0.5 to 2.0)",
              usd is not None and 0.5 <= usd["rate"] <= 2.0,
              f"USD = {usd['rate'] if usd else 'missing'}")
    else:
        check("fx: silver file exists", False, str(fxp))

    total = results["passed"] + results["failed"]
    log.info("Quality summary: %d/%d checks passed", results["passed"], total)
    results["total"] = total
    results["pass_rate"] = round(results["passed"] / total * 100, 1) if total else 0

    # Write results to gold
    quality_path = GOLD / "quality_report.json"
    quality_path.write_text(json.dumps({
        "generated_at": now_iso(),
        **results,
    }, indent=2))
    return results


# ---------------------------------------------------------------------------
# Pipeline manifest (for dashboard)
# ---------------------------------------------------------------------------

def write_manifest(run_results: dict):
    """Write a pipeline run manifest consumed by the GitHub Pages dashboard."""
    manifest = {
        "last_run": now_iso(),
        "sources": {
            "weather": {
                "status": "ok" if (GOLD / "weather_summary.json").exists() else "missing",
                "cities": list(CITIES.keys()),
                "gold_file": "gold/weather_summary.json",
            },
            "countries": {
                "status": "ok" if (GOLD / "countries_summary.json").exists() else "missing",
                "country_count": len(COUNTRY_CODES),
                "gold_file": "gold/countries_summary.json",
            },
            "fx": {
                "status": "ok" if (GOLD / "fx_summary.json").exists() else "missing",
                "base_currency": "GBP",
                "gold_file": "gold/fx_summary.json",
            },
        },
        "quality": run_results.get("quality", {}),
        "bronze_files": [f.name for f in sorted(BRONZE.glob("*.json"))[-10:]],
    }
    manifest_path = ROOT / "pipeline_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    log.info("Manifest written: pipeline_manifest.json")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

SOURCES = {
    "weather":   (ingest_weather,   transform_weather),
    "countries": (ingest_countries, transform_countries),
    "fx":        (ingest_fx,        transform_fx),
}


def main():
    parser = argparse.ArgumentParser(description="API Data Ingestion Pipeline")
    parser.add_argument("--source", choices=list(SOURCES.keys()) + ["all"], default="all")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate config but do not fetch or write")
    args = parser.parse_args()

    if args.dry_run:
        log.info("DRY RUN — config validated. Sources: %s", ", ".join(SOURCES))
        return

    log.info("Pipeline starting — source=%s", args.source)
    run_results: dict = {}

    selected = list(SOURCES.keys()) if args.source == "all" else [args.source]

    for source in selected:
        ingest_fn, transform_fn = SOURCES[source]
        try:
            ingest_fn()
            gold = transform_fn()
            run_results[source] = {"status": "ok", "gold": gold}
        except Exception as exc:
            log.error("Source %s failed: %s", source, exc)
            run_results[source] = {"status": "error", "error": str(exc)}

    if args.source == "all":
        quality = run_quality_checks()
        run_results["quality"] = quality

    write_manifest(run_results)
    log.info("Pipeline complete. Log: %s", log_file)


if __name__ == "__main__":
    main()
