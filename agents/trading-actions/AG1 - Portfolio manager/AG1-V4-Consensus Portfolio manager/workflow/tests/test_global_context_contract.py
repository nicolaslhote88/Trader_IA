from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

CRITICAL_HASHES = {
    "nodes/post_agent/06_build_consensus_v4.code.js": "c39434c3ff5b484ba2615fa6a0ec7c722387b790c3f83c630070645d611d1316",
    "nodes/post_agent/07_validate_enforce_safety_v5.code.js": "d658f005a41131e175792f5b5dea63e3445fb744f8979f347916dacc9722883d",
    "nodes/post_agent/07b_ibkr_send_orders.js": "060d649426d7ad015e68734fe1cda4909ecdf89503d1158e26d77f3a7e8b5e41",
}


def workflow(name):
    return json.loads((ROOT / name).read_text(encoding="utf-8"))


def node_by_name(payload, name):
    return next(node for node in payload["nodes"] if node["name"] == name)


def targets(payload, source):
    return [target for group in payload["connections"][source].get("main", []) for target in group]


def test_critical_consensus_risk_and_broker_sources_are_unchanged():
    for relative, expected in CRITICAL_HASHES.items():
        text = (ROOT / relative).read_text(encoding="utf-8").replace("\r\n", "\n")
        assert hashlib.sha256(text.encode("utf-8")).hexdigest() == expected


def test_one_shared_context_node_precedes_all_three_models():
    active = workflow("AG1_workflow_v4_consensus.json")
    fetch = node_by_name(active, "AG1.GC — Fetch Advisory Pack")
    assert fetch["parameters"]["url"] == "http://global-context-synthesizer:8083/ag1-pack"
    assert "$env" not in json.dumps(fetch, ensure_ascii=False)
    attach_targets = targets(active, "AG1.GC — Attach Advisory Pack")
    assert attach_targets == [{"node": "AG1.V4 — Liquidity Preflight", "type": "main", "index": 0}]
    fanout = targets(active, "AG1.V4 — Liquidity Preflight")
    assert {row["node"] for row in fanout} == {
        "AG1.V4 — Merge Model Proposals",
        "Agent #1 - Portfolio manager",
        "Agent #1 - Portfolio manager1",
        "Agent #1 - Portfolio manager2",
    }
    prompts = [node_by_name(active, name)["parameters"] for name in (
        "Agent #1 - Portfolio manager", "Agent #1 - Portfolio manager1", "Agent #1 - Portfolio manager2"
    )]
    prompt_texts = [json.dumps(params, ensure_ascii=False) for params in prompts]
    assert all("$json.global_context" in text for text in prompt_texts)
    assert all("AG9_GLOBAL_RISK" in text and "AG4_NEWS_SENTIMENT" in text for text in prompt_texts)


def test_shadow_is_manual_only_and_cannot_reach_broker_or_writer():
    shadow = workflow("AG1_workflow_v4_global_context_shadow.json")
    fetch = node_by_name(shadow, "AG1.GC — Fetch Advisory Pack")
    assert fetch["parameters"]["url"] == "http://global-context-synthesizer:8083/ag1-pack"
    assert "$env" not in json.dumps(fetch, ensure_ascii=False)
    assert shadow["active"] is False
    assert not any(node["type"] == "n8n-nodes-base.scheduleTrigger" for node in shadow["nodes"])
    names = {node["name"] for node in shadow["nodes"]}
    forbidden = {
        "AG1.V4 — Liquidity Preflight", "7 - Validate & Enforce Safety",
        "07b - IBKR Send Orders", "8 - Build DuckDB Bundle",
        "9 - Upsert Run Bundle (DuckDB)", "10 - Post-Run Health (DuckDB)",
    }
    assert not names.intersection(forbidden)
    assert targets(shadow, "AG1.V4 — Build Consensus") == [{"node": "AG1.GC — Shadow Capture (NO BROKER)", "type": "main", "index": 0}]


def test_ledger_schema_persists_exact_context_audit_fields():
    schema = (ROOT / "sql/portfolio_ledger_schema_v4.sql").read_text(encoding="utf-8")
    writer = (ROOT / "nodes/post_agent/duckdb_writer.py").read_text(encoding="utf-8")
    for field in (
        "global_context_snapshot_id", "global_context_payload_hash", "global_context_schema_version",
        "global_context_method_version", "global_context_age", "global_context_status", "global_context_pack_json",
    ):
        assert field in schema
        assert field in writer
    assert 'con.execute("CHECKPOINT")' not in writer
