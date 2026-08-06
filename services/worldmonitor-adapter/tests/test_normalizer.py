from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from client import redact
from db import WorldMonitorDB
from normalizer import (
    bounded_aggregate,
    build_ag9_snapshot,
    deduplicate_events,
    event_fingerprint,
    extract_records,
    freshness_decay,
    normalize_record,
)


FIXTURES = Path(__file__).parent / "fixtures"
NOW = datetime(2026, 8, 5, 12, tzinfo=timezone.utc)


def fixture(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def normalize_fixture(name, domain="conflict"):
    return [normalize_record(row, domain=domain, tool_name="fixture_tool", request_id="REQ1", now=NOW) for row in extract_records(fixture(name))]


def test_complete_response_formula_and_mapping():
    event = normalize_fixture("complete_response.json", "chokepoints")[0]
    assert event["severity_normalized"] == 0.75
    assert event["source_diversity"] == 1.0
    assert event["effective_score"] is not None
    assert event["currencies"] == ["USD"]
    assert "Energy" in event["sectors"]


def test_schema_change_and_empty_are_not_successful_records():
    assert extract_records(fixture("schema_change.json")) == []
    assert extract_records(fixture("empty_response.json")) == []


def test_all_transport_and_capability_failure_fixtures_are_explicitly_empty():
    for name in ("tool_absent.json", "timeout.json", "quota.json"):
        payload = fixture(name)
        assert extract_records(payload) == []
        assert payload.get("error") or payload.get("expected_missing")


def test_event_decay_old_vs_long_lived_sanction():
    old_cyber = normalize_fixture("old_event.json", "cyber")[0]
    sanction = normalize_fixture("sanction_long_duration.json", "sanctions")[0]
    assert old_cyber["freshness_decay"] < sanction["freshness_decay"]
    assert sanction["half_life_hours"] == 1080


def test_bounded_aggregation_never_exceeds_one():
    assert 0 < bounded_aggregate([0.8, 0.8, 0.8]) < 1
    assert bounded_aggregate([]) is None


def test_duplicates_merge_lineage_not_linear_scores():
    events = normalize_fixture("duplicate_events.json", "supply_chain")
    deduped = deduplicate_events(events)
    assert len(deduped) == 1
    assert len(deduped[0]["derived_from"]) == 2
    assert deduped[0]["effective_score"] <= 1


def test_convergence_and_unknown_mapping_stay_explicit():
    convergence = normalize_fixture("convergence_signal.json", "convergence")[0]
    unknown = normalize_fixture("unmapped_country.json", "country_risk")[0]
    assert convergence["is_correlated_signal"] is True
    assert unknown["currencies"] == []


def test_remaining_contract_fixtures_cover_short_incident_multi_source_and_unknown_asset():
    brief = normalize_fixture("brief_incident.json", "market_disruption")[0]
    multi = normalize_fixture("multi_source_event.json", "conflict")[0]
    unknown_asset = fixture("asset_unknown.json")
    assert brief["half_life_hours"] == 12
    assert 0 < brief["freshness_decay"] <= 1
    assert multi["source_count"] >= 2
    assert multi["source_diversity"] == 1.0
    assert unknown_asset["asset"]["sector"] is None
    assert unknown_asset["expected"] == "NO_RELIABLE_EXPOSURE_MAPPING"


def test_ag4_fingerprint_is_independent_of_added_entities():
    left = event_fingerprint("Same headline", "2026-08-05T10:00:00Z", [], "https://x.test/a?utm_source=z")
    right = event_fingerprint("Same headline", "2026-08-05T11:00:00Z", ["US"], "https://x.test/a")
    assert left == right


def test_ag4_duplicate_is_auditable_but_excluded_from_ag9_regime():
    event = normalize_fixture("complete_response.json", "chokepoints")[0]
    event["ag4_duplicate"] = True
    built = build_ag9_snapshot(snapshot_id="AG9_DUP", events=[event], source_health=[{
        "capability": "chokepoints", "status": "OK"
    }], now=NOW)
    assert built["snapshot"]["global_risk_score"] is None
    assert built["snapshot"]["critical_events"] == []


def test_secret_redaction():
    secret = "wm_live_abcdefghijklmnopqrstuvwxyz"
    assert secret not in redact(f"failed {secret}", secret)
    assert "[REDACTED]" in redact(f"failed {secret}", secret)


def test_worldmonitor_duckdb_contract(tmp_path):
    db = WorldMonitorDB(str(tmp_path / "world.duckdb"))
    event = normalize_fixture("complete_response.json", "chokepoints")[0]
    event["snapshot_id"] = "AG9_TEST"
    health = [{"run_id": "RUN1", "capability": "chokepoints", "tool_name": "fixture_tool", "status": "OK", "latency_ms": 1, "rows_received": 1, "checked_at": NOW.isoformat()}]
    built = build_ag9_snapshot(snapshot_id="AG9_TEST", events=[event], source_health=health, now=NOW)
    db.persist_refresh({
        "raw_responses": [{"request_id": "REQ1", "run_id": "RUN1", "capability": "chokepoints", "tool_name": "fixture_tool", "tool_contract_hash": "abc", "requested_at": NOW.isoformat(), "received_at": NOW.isoformat(), "status": "OK", "payload_hash": "def", "raw_payload": fixture("complete_response.json")}],
        "events": [event], "source_health": health, "snapshot": built["snapshot"],
        "country_risk": built["country_risk"], "sector_impacts": built["sector_impacts"],
        "chokepoint_status": built["chokepoint_status"], "asset_impacts": [],
        "run_log": {"run_id": "RUN1", "started_at": NOW, "finished_at": NOW, "status": "OK", "tools_discovered": 1, "tools_called": 1, "responses_ok": 1, "events_normalized": 1, "events_deduplicated": 1, "snapshot_id": "AG9_TEST"},
    })
    latest = db.latest_snapshot()
    assert latest["snapshot_id"] == "AG9_TEST"
    assert db.source_health()[0]["status"] == "OK"


def test_observed_catalog_names_are_versioned_in_capability_config():
    config = json.loads((FIXTURES.parents[1] / "config" / "capabilities.json").read_text(encoding="utf-8"))
    candidates = {row["capability"]: row["candidates"] for row in config["capabilities"]}
    assert "get_conflict_events" in candidates["conflicts"]
    assert "get_supply_chain_data" in candidates["supply_chain"]
    assert "get_country_risk" in candidates["country_risk"]
