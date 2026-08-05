from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve()
SERVICE = HERE.parents[1]
REPO = HERE.parents[3]
MACRO = REPO / "services" / "macro-data-api"
sys.path.insert(0, str(SERVICE))
sys.path.insert(0, str(MACRO))

from db import GlobalContextDB
from macro_db import MacroDB
from synthesizer import advisory_pack_for_run, canonical_json, payload_hash, synthesize


NOW = datetime(2026, 8, 5, 12, tzinfo=timezone.utc)


def seed_macro(path: Path) -> None:
    db = MacroDB(str(path))
    db.upsert_ag5_macro([{
        "component_snapshot_id": "AG5_TEST", "component": "AG5_MACRO", "schema_version": "AG5_MACRO_V2",
        "method_version": "AG5_MACRO_V2", "entity_id": "EUR", "entity_type": "country_or_currency",
        "observation_time": "2026-07-01T00:00:00Z", "publication_time": None,
        "ingestion_time": "2026-08-05T10:00:00Z", "calculation_time": "2026-08-05T11:00:00Z",
        "macro_score": 0.2, "subscores": {"growth": 0.2}, "coverage_ratio": 0.5,
        "confidence": 0.4, "freshness_status": "aging", "missing_inputs": ["fiscal"],
        "stale_inputs": [], "proxy_inputs": [], "weights": {"growth": 1},
        "contributions": {"growth": 0.2}, "lineage": {"source": "fixture"}, "source": "TEST",
    }])


def test_degraded_snapshot_keeps_missing_components_explicit(tmp_path):
    macro_path = tmp_path / "macro_data_test.duckdb"
    seed_macro(macro_path)
    bundle = synthesize(str(macro_path), str(tmp_path / "missing-world.duckdb"), now=NOW)
    assert bundle["snapshot"]["status"] == "DEGRADED"
    assert bundle["snapshot"]["component_snapshot_ids"]["AG5"] == "AG5_TEST"
    assert bundle["snapshot"]["component_snapshot_ids"]["AG9"] is None
    assert "AG9_UNAVAILABLE" in bundle["snapshot"]["ag1_pack"]["source_warnings"]
    assert bundle["snapshot"]["ag1_pack"]["advisory_only"] is True
    assert len(canonical_json(bundle["snapshot"]["ag1_pack"])) <= 12000


def test_atomic_publication_and_canonical_views(tmp_path):
    macro_path = tmp_path / "macro_data_test.duckdb"
    seed_macro(macro_path)
    bundle = synthesize(str(macro_path), str(tmp_path / "missing-world.duckdb"), now=NOW)
    db = GlobalContextDB(str(tmp_path / "global.duckdb"))
    bundle["run_log"] = {"run_id": "RUN1", "started_at": NOW, "finished_at": NOW, "status": "DEGRADED", "components_available": 1, "components_missing": 4, "rows_written": 10}
    db.publish(bundle)
    assert db.latest()["snapshot_id"] == bundle["snapshot"]["snapshot_id"]
    assert db.latest_pack()["payload_hash"] == bundle["snapshot"]["ag1_pack"]["payload_hash"]
    with db.connect() as con:
        assert con.execute("SELECT COUNT(*) FROM main.v_component_health").fetchone()[0] == 5


def test_dormant_ag9_is_excluded_from_freshness_and_coverage(tmp_path, monkeypatch):
    macro_path = tmp_path / "macro_data_test.duckdb"
    seed_macro(macro_path)
    monkeypatch.setenv("GLOBAL_CONTEXT_ENABLED_COMPONENTS", "AG5")
    bundle = synthesize(str(macro_path), str(tmp_path / "missing-world.duckdb"), now=NOW)
    statuses = {row["component"]: row for row in bundle["component_status"]}
    pack = bundle["snapshot"]["ag1_pack"]
    assert statuses["AG9"]["status"] == "DISABLED"
    assert statuses["AG9"]["freshness_status"] == "disabled"
    assert "AG9_DORMANT" in pack["source_warnings"]
    assert "AG9_UNAVAILABLE" not in pack["source_warnings"]
    assert pack["freshness_status"] == "aging"
    assert pack["coverage_ratio"] == 0.5
    assert pack["confidence"] == 0.4


def test_run_specific_advisory_mapping_does_not_invent_exposure():
    base = {
        "schema_version": "AG1_GLOBAL_CONTEXT_PACK_V1", "method_version": "GLOBAL_CONTEXT_SYNTHESIS_V1",
        "snapshot_id": "GC1", "as_of": NOW.isoformat(), "freshness_status": "fresh", "coverage_ratio": 1,
        "confidence": 0.8, "status": "OK", "advisory_only": True,
        "macro_regime": {"status": "OK", "by_currency": {"EUR": {
            "macro_score": 0.25, "confidence": 0.8, "freshness_status": "fresh",
        }}},
        "rates_liquidity_regime": {}, "positioning_regime": {},
        "fx_relative_valuation": {}, "geopolitical_risk_regime": {},
        "portfolio_exposure_review": [], "opportunity_exposure_review": [],
        "sector_overlays": [], "country_overlays": [], "critical_events": [], "source_warnings": [],
    }
    context = {"sectors": [{"sector": "Energy", "risk_score": 0.7, "confidence": 0.8, "contributors_json": "[\"E1\"]"}], "countries": [], "assets": []}
    pack = advisory_pack_for_run(
        base,
        context,
        [{"symbol": "SHEL", "sector": "Energy"}, {"symbol": "UNKNOWN1"}],
        [{"symbol": "SAP.DE", "sector": "Technology"}],
        now=NOW,
    )
    assert pack["schema_version"] == "AG1_GLOBAL_CONTEXT_LLM_V2"
    assert pack["method_version"] == "GLOBAL_CONTEXT_LLM_COMPACTION_V2"
    assert pack["use_policy"] == "NORMAL"
    assert pack["relevant_currencies"] == ["EUR"]
    assert pack["currency_signals"]["EUR"]["macro"]["macro_score"] == 0.25
    assert pack["score_legend"]["positioning_score"].startswith("contrarian:")
    assert pack["exposure_summary"]["portfolio"] == {"total": 2, "known": 1, "unknown": 1}
    assert pack["exposure_summary"]["opportunities"] == {"total": 1, "known": 0, "unknown": 1}
    assert pack["exposure_summary"]["limitation"] == "PARTIAL_EXPOSURE_MAPPING"
    assert len(pack["known_asset_overlays"]) == 1
    assert "portfolio_exposure_review" not in pack
    recorded_hash = pack.pop("payload_hash")
    assert recorded_hash == payload_hash(pack)


def test_degraded_llm_pack_is_caveat_only_small_and_has_no_repeated_unknown_rows():
    currencies = ("AUD", "CAD", "CHF", "EUR", "GBP", "JPY", "KRW", "MXN", "NOK", "NZD", "SEK", "USD")
    low_quality = {
        currency: {
            "macro_score": 0.123456789,
            "confidence": 0.12,
            "freshness_status": "stale",
        }
        for currency in currencies
    }
    positioning = {
        currency: {
            "positioning_score": 0.8195,
            "crowded_direction": "short",
            "crowded_flag": True,
            "confidence": 0.9,
            "freshness_status": "fresh",
        }
        for currency in currencies
    }
    base = {
        "schema_version": "AG1_GLOBAL_CONTEXT_PACK_V1", "method_version": "GLOBAL_CONTEXT_SYNTHESIS_V1",
        "snapshot_id": "GC_DEGRADED", "as_of": NOW.isoformat(), "freshness_status": "missing",
        "coverage_ratio": 0.584444, "confidence": 0.400186, "status": "DEGRADED",
        "macro_regime": {"status": "DEGRADED", "by_currency": low_quality},
        "fx_relative_valuation": {"status": "DEGRADED", "by_currency": low_quality},
        "positioning_regime": {"status": "OK", "by_currency": positioning},
        "rates_liquidity_regime": {"status": "DEGRADED", "by_currency": low_quality},
        "geopolitical_risk_regime": {"status": "DISABLED", "global_risk_regime": "unknown"},
        "critical_events": [], "sector_overlays": [], "country_overlays": [],
        "source_warnings": ["AG9_DORMANT"],
    }
    portfolio = [{"symbol": f"P{i}"} for i in range(9)]
    opportunities = [{"symbol": f"O{i}.PA"} for i in range(12)]
    pack = advisory_pack_for_run(base, {"sectors": [], "countries": [], "assets": []}, portfolio, opportunities, now=NOW)
    text = canonical_json(pack)
    assert pack["use_policy"] == "CAVEAT_ONLY"
    assert "currency_signals" not in pack
    assert pack["exposure_summary"]["portfolio"] == {"total": 9, "known": 0, "unknown": 9}
    assert pack["exposure_summary"]["opportunities"] == {"total": 12, "known": 0, "unknown": 12}
    assert text.count("NO_RELIABLE_EXPOSURE_MAPPING") == 1
    assert len(text) < 4000
    assert "0.123456789" not in text


def test_old_snapshot_is_ignored_even_when_component_scores_are_high():
    base = {
        "snapshot_id": "GC_OLD", "as_of": "2026-08-04T00:00:00+00:00",
        "freshness_status": "fresh", "coverage_ratio": 1.0, "confidence": 1.0,
        "status": "OK", "source_warnings": [],
        "macro_regime": {"status": "OK", "by_currency": {"USD": {
            "macro_score": 0.9, "confidence": 0.9, "freshness_status": "fresh",
        }}},
    }
    pack = advisory_pack_for_run(base, {"sectors": [], "countries": [], "assets": []}, [{"symbol": "NVDA"}], [], now=NOW)
    assert pack["status"] == "GLOBAL_CONTEXT_STALE"
    assert pack["use_policy"] == "IGNORE"
    assert "currency_signals" not in pack
    assert "GLOBAL_CONTEXT_STALE" in pack["source_warnings"]
