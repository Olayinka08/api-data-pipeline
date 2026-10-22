"""
Seed demo data for GitHub Pages dashboard.
Run this once to pre-populate bronze/silver/gold with realistic static data.
The pipeline.py script replaces this with live API data when run locally.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent
BRONZE = ROOT / "bronze"
SILVER = ROOT / "silver"
GOLD = ROOT / "gold"

for d in (BRONZE, SILVER, GOLD):
    d.mkdir(exist_ok=True)

NOW = datetime.now(timezone.utc).isoformat()

# ---------------------------------------------------------------------------
# Weather
# ---------------------------------------------------------------------------
weather_silver = [
    {"city": "london",     "temperature_c": 13.4, "windspeed_kmh": 22.1, "weathercode": 3,  "is_day": True,  "ingested_at": NOW, "source_file": "demo"},
    {"city": "manchester", "temperature_c": 11.8, "windspeed_kmh": 28.3, "weathercode": 61, "is_day": True,  "ingested_at": NOW, "source_file": "demo"},
    {"city": "birmingham", "temperature_c": 12.5, "windspeed_kmh": 18.7, "weathercode": 2,  "is_day": True,  "ingested_at": NOW, "source_file": "demo"},
    {"city": "edinburgh",  "temperature_c":  9.2, "windspeed_kmh": 35.6, "weathercode": 71, "is_day": True,  "ingested_at": NOW, "source_file": "demo"},
    {"city": "bristol",    "temperature_c": 14.1, "windspeed_kmh": 15.4, "weathercode": 1,  "is_day": True,  "ingested_at": NOW, "source_file": "demo"},
]

temps = [r["temperature_c"] for r in weather_silver]
weather_gold = {
    "generated_at": NOW,
    "city_count": 5,
    "avg_temp_c": round(sum(temps) / len(temps), 1),
    "max_temp_c": max(temps),
    "min_temp_c": min(temps),
    "hottest_city": max(weather_silver, key=lambda r: r["temperature_c"])["city"],
    "coldest_city": min(weather_silver, key=lambda r: r["temperature_c"])["city"],
    "cities": weather_silver,
}

(SILVER / "weather_current.json").write_text(json.dumps(weather_silver, indent=2))
(GOLD / "weather_summary.json").write_text(json.dumps(weather_gold, indent=2))
print("Weather data written")

# ---------------------------------------------------------------------------
# Countries
# ---------------------------------------------------------------------------
countries_silver = [
    {"cca2": "GB", "name": "United Kingdom",  "capital": "London",          "region": "Europe",   "subregion": "Northern Europe", "population": 67_215_293, "area_km2": 242_900, "pop_density": 276.7, "currencies": ["GBP"], "languages": ["English"],  "ingested_at": NOW},
    {"cca2": "US", "name": "United States",   "capital": "Washington D.C.", "region": "Americas", "subregion": "North America",   "population": 329_484_123, "area_km2": 9_372_610, "pop_density": 35.2, "currencies": ["USD"], "languages": ["English"],  "ingested_at": NOW},
    {"cca2": "DE", "name": "Germany",         "capital": "Berlin",          "region": "Europe",   "subregion": "Western Europe",  "population": 83_783_942, "area_km2": 357_114, "pop_density": 234.6, "currencies": ["EUR"], "languages": ["German"],   "ingested_at": NOW},
    {"cca2": "FR", "name": "France",          "capital": "Paris",           "region": "Europe",   "subregion": "Western Europe",  "population": 67_391_582, "area_km2": 551_695, "pop_density": 122.2, "currencies": ["EUR"], "languages": ["French"],   "ingested_at": NOW},
    {"cca2": "NG", "name": "Nigeria",         "capital": "Abuja",           "region": "Africa",   "subregion": "Western Africa",  "population": 206_139_589, "area_km2": 923_768, "pop_density": 223.2, "currencies": ["NGN"], "languages": ["English"],  "ingested_at": NOW},
    {"cca2": "JP", "name": "Japan",           "capital": "Tokyo",           "region": "Asia",     "subregion": "Eastern Asia",    "population": 125_836_021, "area_km2": 377_930, "pop_density": 332.9, "currencies": ["JPY"], "languages": ["Japanese"], "ingested_at": NOW},
    {"cca2": "AU", "name": "Australia",       "capital": "Canberra",        "region": "Oceania",  "subregion": "Australia and NZ","population": 25_499_884, "area_km2": 7_692_024, "pop_density": 3.3,  "currencies": ["AUD"], "languages": ["English"],  "ingested_at": NOW},
    {"cca2": "CA", "name": "Canada",          "capital": "Ottawa",          "region": "Americas", "subregion": "North America",   "population": 38_005_238, "area_km2": 9_984_670, "pop_density": 3.8,  "currencies": ["CAD"], "languages": ["English", "French"], "ingested_at": NOW},
    {"cca2": "SG", "name": "Singapore",       "capital": "Singapore",       "region": "Asia",     "subregion": "South-Eastern Asia","population": 5_850_342, "area_km2": 710,    "pop_density": 8240.0,"currencies": ["SGD"], "languages": ["English", "Malay", "Tamil", "Mandarin"], "ingested_at": NOW},
    {"cca2": "ZA", "name": "South Africa",    "capital": "Pretoria",        "region": "Africa",   "subregion": "Southern Africa", "population": 59_308_690, "area_km2": 1_219_090, "pop_density": 48.7, "currencies": ["ZAR"], "languages": ["Zulu", "Xhosa", "Afrikaans", "English"], "ingested_at": NOW},
]

countries_gold = {
    "generated_at": NOW,
    "country_count": 10,
    "total_population": sum(c["population"] for c in countries_silver),
    "largest_by_population": "United States",
    "largest_by_area": "Canada",
    "most_dense": "Singapore",
    "countries": countries_silver,
}

(SILVER / "countries.json").write_text(json.dumps(countries_silver, indent=2))
(GOLD / "countries_summary.json").write_text(json.dumps(countries_gold, indent=2))
print("Countries data written")

# ---------------------------------------------------------------------------
# FX
# ---------------------------------------------------------------------------
fx_rates = {"USD": 1.2748, "EUR": 1.1712, "JPY": 193.42, "AUD": 1.9613,
            "CAD": 1.7398, "NGN": 2052.18, "ZAR": 23.47, "SGD": 1.7141,
            "INR": 106.94, "CHF": 1.1289}

fx_silver = [
    {"base": "GBP", "target": t, "rate": r,
     "last_updated": "Thu, 22 Oct 2026 00:02:01 +0000",
     "ingested_at": NOW}
    for t, r in fx_rates.items()
]

fx_gold = {
    "generated_at": NOW,
    "base_currency": "GBP",
    "rate_count": len(fx_rates),
    "last_updated": "Thu, 22 Oct 2026 00:02:01 +0000",
    "rates": fx_rates,
}

(SILVER / "fx_rates.json").write_text(json.dumps(fx_silver, indent=2))
(GOLD / "fx_summary.json").write_text(json.dumps(fx_gold, indent=2))
print("FX data written")

# ---------------------------------------------------------------------------
# Quality report
# ---------------------------------------------------------------------------
quality_report = {
    "generated_at": NOW,
    "passed": 17,
    "failed": 0,
    "total": 17,
    "pass_rate": 100.0,
    "checks": [
        {"name": "weather: row count == 5",          "status": "PASS", "detail": ""},
        {"name": "weather: no null temperatures",    "status": "PASS", "detail": ""},
        {"name": "weather: temps in plausible range","status": "PASS", "detail": ""},
        {"name": "weather: all 5 cities present",   "status": "PASS", "detail": ""},
        {"name": "countries: row count == 10",       "status": "PASS", "detail": ""},
        {"name": "countries: no null names",         "status": "PASS", "detail": ""},
        {"name": "countries: no null populations",   "status": "PASS", "detail": ""},
        {"name": "countries: positive populations",  "status": "PASS", "detail": ""},
        {"name": "countries: gb present",            "status": "PASS", "detail": ""},
        {"name": "fx: at least 8 rates returned",   "status": "PASS", "detail": ""},
        {"name": "fx: no null rates",                "status": "PASS", "detail": ""},
        {"name": "fx: positive rates",               "status": "PASS", "detail": ""},
        {"name": "fx: USD rate in plausible range",  "status": "PASS", "detail": "USD = 1.2748"},
    ],
}

(GOLD / "quality_report.json").write_text(json.dumps(quality_report, indent=2))
print("Quality report written")

# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------
manifest = {
    "last_run": NOW,
    "sources": {
        "weather":   {"status": "ok", "cities": ["london","manchester","birmingham","edinburgh","bristol"], "gold_file": "gold/weather_summary.json"},
        "countries": {"status": "ok", "country_count": 10, "gold_file": "gold/countries_summary.json"},
        "fx":        {"status": "ok", "base_currency": "GBP", "gold_file": "gold/fx_summary.json"},
    },
    "quality": quality_report,
    "bronze_files": [],
}

(ROOT / "pipeline_manifest.json").write_text(json.dumps(manifest, indent=2))
print("Manifest written")
print("\nAll demo data seeded successfully.")
