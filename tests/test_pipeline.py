"""
Unit tests for the API data ingestion pipeline.
Run: pytest tests/ -v
"""

import json
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Make sure the project root is importable
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))


# ---------------------------------------------------------------------------
# Fixtures: mock API responses
# ---------------------------------------------------------------------------

MOCK_WEATHER = {
    "current_weather": {
        "temperature": 14.2,
        "windspeed": 18.5,
        "weathercode": 3,
        "is_day": 1,
    },
    "hourly": {
        "time": ["2024-01-01T00:00", "2024-01-01T01:00"],
        "temperature_2m": [13.0, 13.5],
        "precipitation": [0.0, 0.1],
        "windspeed_10m": [15.0, 16.0],
        "relative_humidity_2m": [72, 74],
    },
    "timezone": "Europe/London",
}

MOCK_COUNTRIES = [
    {
        "cca2": "GB",
        "name": {"common": "United Kingdom"},
        "capital": ["London"],
        "region": "Europe",
        "subregion": "Northern Europe",
        "population": 67215293,
        "area": 242900.0,
        "currencies": {"GBP": {"name": "British pound"}},
        "languages": {"eng": "English"},
    },
    {
        "cca2": "US",
        "name": {"common": "United States"},
        "capital": ["Washington, D.C."],
        "region": "Americas",
        "subregion": "North America",
        "population": 329484123,
        "area": 9372610.0,
        "currencies": {"USD": {"name": "United States dollar"}},
        "languages": {"eng": "English"},
    },
]

MOCK_FX = {
    "result": "success",
    "base_code": "GBP",
    "time_last_update_utc": "Thu, 01 Jan 2024 00:00:02 +0000",
    "rates": {
        "USD": 1.27,
        "EUR": 1.17,
        "JPY": 185.3,
        "AUD": 1.93,
        "CAD": 1.71,
        "NGN": 1050.0,
        "ZAR": 23.8,
        "SGD": 1.71,
        "INR": 105.2,
        "CHF": 1.13,
    },
}


# ---------------------------------------------------------------------------
# Helper: build a minimal in-memory file tree for tests
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_pipeline(tmp_path, monkeypatch):
    """Patch pipeline ROOT, BRONZE, SILVER, GOLD, LOGS to use tmp_path."""
    import pipeline as pl

    monkeypatch.setattr(pl, "ROOT", tmp_path)
    monkeypatch.setattr(pl, "BRONZE", tmp_path / "bronze")
    monkeypatch.setattr(pl, "SILVER", tmp_path / "silver")
    monkeypatch.setattr(pl, "GOLD", tmp_path / "gold")
    monkeypatch.setattr(pl, "LOGS", tmp_path / "logs")

    for d in (pl.BRONZE, pl.SILVER, pl.GOLD, pl.LOGS):
        d.mkdir(parents=True, exist_ok=True)

    return pl


# ---------------------------------------------------------------------------
# Tests: fetch helper
# ---------------------------------------------------------------------------

class TestFetch:
    def test_returns_json_on_success(self, tmp_pipeline):
        pl = tmp_pipeline
        mock_response = MagicMock()
        mock_response.json.return_value = {"key": "value"}
        mock_response.raise_for_status = MagicMock()

        with patch("pipeline.requests.get", return_value=mock_response):
            result = pl.fetch("https://example.com/api")

        assert result == {"key": "value"}

    def test_retries_on_failure(self, tmp_pipeline):
        pl = tmp_pipeline
        import requests as req_lib

        fail = MagicMock()
        fail.raise_for_status.side_effect = req_lib.RequestException("timeout")

        success = MagicMock()
        success.raise_for_status = MagicMock()
        success.json.return_value = {"ok": True}

        with patch("pipeline.requests.get", side_effect=[fail, success]):
            with patch("pipeline.time.sleep"):  # skip sleep in tests
                result = pl.fetch("https://example.com/api", retries=2)

        assert result == {"ok": True}

    def test_raises_after_all_retries_exhausted(self, tmp_pipeline):
        pl = tmp_pipeline
        import requests as req_lib

        fail = MagicMock()
        fail.raise_for_status.side_effect = req_lib.RequestException("timeout")

        with patch("pipeline.requests.get", return_value=fail):
            with patch("pipeline.time.sleep"):
                with pytest.raises(RuntimeError, match="All 2 attempts failed"):
                    pl.fetch("https://example.com/api", retries=2)


# ---------------------------------------------------------------------------
# Tests: Bronze write/read helpers
# ---------------------------------------------------------------------------

class TestBronze:
    def test_write_bronze_creates_file(self, tmp_pipeline):
        pl = tmp_pipeline
        path = pl.write_bronze("test_source", {"hello": "world"})
        assert path.exists()
        content = json.loads(path.read_text())
        assert content["data"] == {"hello": "world"}
        assert "_ingested_at" in content

    def test_latest_bronze_returns_most_recent(self, tmp_pipeline):
        pl = tmp_pipeline
        pl.write_bronze("src", {"run": 1})
        import time as t; t.sleep(0.01)
        path2 = pl.write_bronze("src", {"run": 2})
        result = pl.latest_bronze("src")
        assert result == path2

    def test_latest_bronze_returns_none_when_empty(self, tmp_pipeline):
        pl = tmp_pipeline
        assert pl.latest_bronze("nonexistent") is None


# ---------------------------------------------------------------------------
# Tests: Weather transform
# ---------------------------------------------------------------------------

class TestWeatherTransform:
    def _seed_bronze(self, pl):
        cities_data = {city: MOCK_WEATHER for city in pl.CITIES}
        pl.write_bronze("weather", cities_data)

    def test_produces_silver_with_5_rows(self, tmp_pipeline):
        pl = tmp_pipeline
        self._seed_bronze(pl)
        pl.transform_weather()
        silver = json.loads((pl.SILVER / "weather_current.json").read_text())
        assert len(silver) == 5

    def test_silver_rows_have_required_fields(self, tmp_pipeline):
        pl = tmp_pipeline
        self._seed_bronze(pl)
        pl.transform_weather()
        silver = json.loads((pl.SILVER / "weather_current.json").read_text())
        for row in silver:
            assert "city" in row
            assert "temperature_c" in row
            assert "windspeed_kmh" in row

    def test_produces_gold_summary(self, tmp_pipeline):
        pl = tmp_pipeline
        self._seed_bronze(pl)
        pl.transform_weather()
        gold = json.loads((pl.GOLD / "weather_summary.json").read_text())
        assert "avg_temp_c" in gold
        assert "hottest_city" in gold
        assert gold["city_count"] == 5

    def test_all_temps_equal_gives_same_hottest_coldest(self, tmp_pipeline):
        pl = tmp_pipeline
        self._seed_bronze(pl)
        result = pl.transform_weather()
        # All cities have 14.2°C from mock, so avg == 14.2
        assert result["avg_temp_c"] == 14.2


# ---------------------------------------------------------------------------
# Tests: Countries transform
# ---------------------------------------------------------------------------

class TestCountriesTransform:
    def _seed_bronze(self, pl):
        pl.write_bronze("countries", MOCK_COUNTRIES)

    def test_produces_silver_with_correct_count(self, tmp_pipeline):
        pl = tmp_pipeline
        self._seed_bronze(pl)
        pl.transform_countries()
        silver = json.loads((pl.SILVER / "countries.json").read_text())
        assert len(silver) == 2

    def test_pop_density_calculated(self, tmp_pipeline):
        pl = tmp_pipeline
        self._seed_bronze(pl)
        pl.transform_countries()
        silver = json.loads((pl.SILVER / "countries.json").read_text())
        gb = next(r for r in silver if r["cca2"] == "GB")
        assert gb["pop_density"] is not None
        assert gb["pop_density"] > 0

    def test_gold_identifies_largest_by_population(self, tmp_pipeline):
        pl = tmp_pipeline
        self._seed_bronze(pl)
        pl.transform_countries()
        gold = json.loads((pl.GOLD / "countries_summary.json").read_text())
        assert gold["largest_by_population"] == "United States"


# ---------------------------------------------------------------------------
# Tests: FX transform
# ---------------------------------------------------------------------------

class TestFXTransform:
    def _seed_bronze(self, pl):
        pl.write_bronze("fx", MOCK_FX)

    def test_produces_silver_rows(self, tmp_pipeline):
        pl = tmp_pipeline
        self._seed_bronze(pl)
        pl.transform_fx()
        silver = json.loads((pl.SILVER / "fx_rates.json").read_text())
        assert len(silver) == len(pl.TARGET_CURRENCIES)

    def test_all_rates_positive(self, tmp_pipeline):
        pl = tmp_pipeline
        self._seed_bronze(pl)
        pl.transform_fx()
        silver = json.loads((pl.SILVER / "fx_rates.json").read_text())
        assert all(r["rate"] > 0 for r in silver)

    def test_gold_contains_rates_dict(self, tmp_pipeline):
        pl = tmp_pipeline
        self._seed_bronze(pl)
        pl.transform_fx()
        gold = json.loads((pl.GOLD / "fx_summary.json").read_text())
        assert "rates" in gold
        assert gold["base_currency"] == "GBP"
        assert "USD" in gold["rates"]


# ---------------------------------------------------------------------------
# Tests: Quality checks
# ---------------------------------------------------------------------------

class TestQualityChecks:
    def _seed_all_silver(self, pl):
        """Write minimal valid silver files."""
        weather_rows = [
            {"city": city, "temperature_c": 14.0, "windspeed_kmh": 12.0,
             "weathercode": 1, "is_day": True, "ingested_at": "2024-01-01T00:00:00Z",
             "source_file": "x.json"}
            for city in pl.CITIES
        ]
        (pl.SILVER / "weather_current.json").write_text(json.dumps(weather_rows))

        # Seed one row per expected country code so count check passes
        country_stubs = [
            ("GB", "United Kingdom"), ("US", "United States"), ("DE", "Germany"),
            ("FR", "France"), ("NG", "Nigeria"), ("JP", "Japan"),
            ("AU", "Australia"), ("CA", "Canada"), ("SG", "Singapore"), ("ZA", "South Africa"),
        ]
        countries_rows = [
            {"cca2": cca2, "name": name, "capital": "Capital",
             "region": "Region", "subregion": "Subregion", "population": 10_000_000,
             "area_km2": 100_000, "pop_density": 100.0, "currencies": ["GBP"],
             "languages": ["English"], "ingested_at": "2024-01-01T00:00:00Z"}
            for cca2, name in country_stubs
        ]
        (pl.SILVER / "countries.json").write_text(json.dumps(countries_rows))

        fx_rows = [
            {"base": "GBP", "target": c, "rate": 1.27,
             "last_updated": "Thu, 01 Jan 2024 00:00:02 +0000",
             "ingested_at": "2024-01-01T00:00:00Z"}
            for c in pl.TARGET_CURRENCIES
        ]
        (pl.SILVER / "fx_rates.json").write_text(json.dumps(fx_rows))

    def test_all_checks_pass_on_valid_data(self, tmp_pipeline):
        pl = tmp_pipeline
        self._seed_all_silver(pl)
        result = pl.run_quality_checks()
        assert result["failed"] == 0

    def test_quality_report_written_to_gold(self, tmp_pipeline):
        pl = tmp_pipeline
        self._seed_all_silver(pl)
        pl.run_quality_checks()
        assert (pl.GOLD / "quality_report.json").exists()

    def test_missing_silver_fails_checks(self, tmp_pipeline):
        pl = tmp_pipeline
        # No silver files seeded
        result = pl.run_quality_checks()
        assert result["failed"] > 0
