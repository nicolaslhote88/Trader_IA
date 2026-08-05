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


def test_run_specific_advisory_mapping_does_not_invent_exposure():
    base = {
        "schema_version": "AG1_GLOBAL_CONTEXT_PACK_V1", "method_version": "GLOBAL_CONTEXT_SYNTHESIS_V1",
        "snapshot_id": "GC1", "as_of": NOW.isoformat(), "freshness_status": "fresh", "coverage_ratio": 1,
        "confidence": 0.8, "status": "OK", "advisory_only": True,
        "macro_regime": {}, "rates_liquidity_regime": {}, "positioning_regime": {},
        "fx_relative_valuation": {}, "geopolitical_risk_regime": {},
        "portfolio_exposure_review": [], "opportunity_exposure_review": [],
        "sector_overlays": [], "country_overlays": [], "critical_events": [], "source_warnings": [],
    }
    context = {"sectors": [{"sector": "Energy", "risk_score": 0.7, "confidence": 0.8, "contributors_json": "[\"E1\"]"}], "countries": [], "assets": []}
    pack = advisory_pack_for_run(base, context, [{"symbol": "SHEL", "sector": "Energy"}, {"symbol": "UNKNOWN1"}], [], now=NOW)
    assert pack["portfolio_exposure_review"][0]["exposure_known"] is True
    assert pack["portfolio_exposure_review"][1]["limitation"] == "NO_RELIABLE_EXPOSURE_MAPPING"
    recorded_hash = pack.pop("payload_hash")
    assert recorded_hash == payload_hash(pack)
