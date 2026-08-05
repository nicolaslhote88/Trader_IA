from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from components import build_ag5_rows, build_ag6_rows, build_ag7_rows, build_ag8_rows
from macro_db import MacroDB


NOW = datetime(2026, 8, 5, 12, tzinfo=timezone.utc)


class FakeDB:
    def get_indicators(self, currency=None, indicator=None):
        rows = [
            {"as_of": "2026-07-01", "currency": "EUR", "indicator": "gdp_growth_qoq", "value": 1.2, "unit": "pct", "source": "FRED", "updated_at": "2026-08-01T00:00:00Z"},
            {"as_of": "2026-07-01", "currency": "EUR", "indicator": "gdp_momentum", "value": 0.1, "unit": "pct", "source": "FRED", "updated_at": "2026-08-01T00:00:00Z"},
            {"as_of": "2026-07-15", "currency": "EUR", "indicator": "cpi_yoy", "value": 2.1, "unit": "pct", "source": "FRED", "updated_at": "2026-08-01T00:00:00Z"},
            {"as_of": "2025-12-31", "currency": "EUR", "indicator": "current_account_pct_gdp", "value": 2.5, "unit": "pct_gdp", "source": "WORLD_BANK", "updated_at": "2026-08-01T00:00:00Z"},
            {"as_of": "2026-07-20", "currency": "EUR", "indicator": "unemployment_pct", "value": 6.2, "unit": "pct", "source": "FRED", "updated_at": "2026-08-01T00:00:00Z"},
            {"as_of": "2026-06-20", "currency": "EUR", "indicator": "unemployment_pct", "value": 6.4, "unit": "pct", "source": "FRED", "updated_at": "2026-07-01T00:00:00Z"},
            {"as_of": "2026-07-15", "currency": "USD", "indicator": "cpi_yoy", "value": 2.8, "unit": "pct", "source": "FRED", "updated_at": "2026-08-01T00:00:00Z"},
        ]
        if currency:
            rows = [row for row in rows if row["currency"] == currency]
        if indicator:
            rows = [row for row in rows if row["indicator"] == indicator]
        return rows

    def get_latest_policy_rates(self):
        return [
            {"as_of": "2026-07-20", "currency": "EUR", "rate_pct": 2.5, "source": "FRED", "updated_at": "2026-08-01T00:00:00Z"},
            {"as_of": "2026-07-20", "currency": "USD", "rate_pct": 3.5, "source": "FRED", "updated_at": "2026-08-01T00:00:00Z"},
        ]

    def get_neutral_rates(self):
        return {"EUR": {"rate_pct": 2.0, "uncertainty_pct": 1.0, "confidence": 0.35}, "USD": {"rate_pct": 2.5, "uncertainty_pct": 1.0, "confidence": 0.35}}

    def get_latest_cot(self):
        return [{"report_date": "2026-07-28", "currency": "EUR", "net_spec": 100, "net_z_score": 2.0, "source": "CFTC_COT", "confidence": "high", "updated_at": "2026-07-31T00:00:00Z"}]

    def get_latest_yield_curve(self):
        return [{"as_of": "2026-08-04", "currency": "EUR", "yield_2y_pct": 2.0, "yield_10y_pct": 2.8, "source": "FRED", "updated_at": "2026-08-05T00:00:00Z"}]

    def get_yield_curve_history(self, currency, limit=120):
        return self.get_latest_yield_curve() + [{"as_of": "2026-07-05", "currency": "EUR", "yield_2y_pct": 2.4, "yield_10y_pct": 2.7}]


def test_component_builders_expose_lineage_and_missingness():
    db = FakeDB()
    ag5 = build_ag5_rows(db, now=NOW)
    eur5 = next(row for row in ag5 if row["entity_id"] == "EUR")
    assert eur5["macro_score"] is not None
    assert eur5["lineage"]["metrics"]["current_account_pct_gdp"]["unit"] == "pct_gdp"

    spots = {"EUR": {"value": 1.15, "observation_time": "2026-08-05T11:00:00Z", "ingestion_time": "2026-08-05T11:01:00Z", "source": "YF", "status": "direct_observation", "confidence": 0.95}}
    ag6 = build_ag6_rows(db, spots, now=NOW)
    eur6 = next(row for row in ag6 if row["currency"] == "EUR")
    assert eur6["ppp_gap"] is None
    assert eur6["input_status"]["ppp"] == "unavailable"

    ag7 = build_ag7_rows(db, now=NOW)
    usd = next(row for row in ag7 if row["entity_id"] == "USD")
    assert usd["is_proxy"] is True
    assert usd["confidence"] <= 0.60

    ag8 = build_ag8_rows(db, now=NOW)
    assert ag8[0]["curve_regime"] == "bull_steepening"
    assert ag8[0]["liquidity_score"] is None
    assert "liquidity" in ag8[0]["missing_inputs"]


def test_duckdb_component_views_and_writes(tmp_path):
    db = MacroDB(str(tmp_path / "macro_data_test.duckdb"))
    fake = FakeDB()
    rows = build_ag5_rows(fake, now=NOW)
    db.upsert_ag5_macro(rows)
    spots = {"EUR": {"value": 1.15, "observation_time": "2026-08-05T11:00:00Z", "ingestion_time": "2026-08-05T11:01:00Z", "source": "YF", "status": "direct_observation", "confidence": 0.95}}
    ag6 = build_ag6_rows(fake, spots, now=NOW)
    ag7 = build_ag7_rows(fake, now=NOW)
    ag8 = build_ag8_rows(fake, now=NOW)
    db.upsert_ag6_fx_valuation(ag6)
    db.upsert_ag7_positioning(ag7)
    db.upsert_ag8_rates_liquidity(ag8)
    latest = db.get_latest_component("ag5")
    assert len(latest) == len(rows)
    assert {row["component_snapshot_id"] for row in latest} == {rows[0]["component_snapshot_id"]}
    assert len(db.get_latest_component("ag6")) == len(ag6)
    assert len(db.get_latest_component("ag7")) == len(ag7)
    assert len(db.get_latest_component("ag8")) == len(ag8)


def test_stale_component_rows_remain_explicit_instead_of_becoming_zero_signal():
    rows = build_ag5_rows(FakeDB(), now=datetime(2030, 1, 1, tzinfo=timezone.utc))
    assert rows
    assert all(row["coverage_ratio"] == 0 for row in rows)
    assert all(row["macro_score"] is None for row in rows)
    assert all(row["freshness_status"] in {"stale", "missing"} for row in rows)
    assert next(row for row in rows if row["entity_id"] == "EUR")["freshness_status"] == "stale"
