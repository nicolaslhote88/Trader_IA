#!/usr/bin/env python3
"""Replay hors ligne du contrat AG1 avec 0, 4 ou 5 composants.

Le script est strictement read-only : aucune URL, aucun broker, aucune écriture
DuckDB et aucun appel LLM. Il prépare des variantes déterministes et rapproche,
si fourni, un export JSON des résultats du workflow shadow.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def read_latest_pack(path: str) -> dict:
    if not Path(path).is_file():
        return {}
    with duckdb.connect(path, read_only=True) as con:
        row = con.execute("SELECT ag1_pack_json FROM main.v_ag1_global_context_pack").fetchone()
    return json.loads(row[0]) if row and row[0] else {}


def read_historical_runs(path: str, limit: int) -> list[dict]:
    if not Path(path).is_file():
        return []
    with duckdb.connect(path, read_only=True) as con:
        columns_available = {row[1] for row in con.execute("PRAGMA table_info('core.runs')").fetchall()}
        optional = [
            f"{name}" if name in columns_available else f"NULL AS {name}"
            for name in ("global_context_snapshot_id", "global_context_payload_hash", "global_context_status")
        ]
        cur = con.execute(
            f"""SELECT run_id, CAST(ts_start AS VARCHAR) AS ts_start,
                       CAST(ts_end AS VARCHAR) AS ts_end,
                       decision_summary, prompt_version,
                       {', '.join(optional)}
                FROM core.runs ORDER BY ts_start DESC LIMIT ?""",
            [limit],
        )
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def without_ag9(pack: dict) -> dict:
    variant = json.loads(canonical(pack)) if pack else {}
    variant["geopolitical_risk_regime"] = {"status": "EXCLUDED_FOR_REPLAY"}
    variant["critical_events"] = []
    variant["country_overlays"] = []
    variant["sector_overlays"] = []
    variant["portfolio_exposure_review"] = []
    variant["opportunity_exposure_review"] = []
    variant["source_warnings"] = sorted(set(variant.get("source_warnings") or []) | {"AG9_EXCLUDED_FOR_REPLAY"})
    variant.pop("payload_hash", None)
    variant["payload_hash"] = digest(variant)
    return variant


def baseline_pack() -> dict:
    pack = {
        "schema_version": "AG1_GLOBAL_CONTEXT_LLM_V2",
        "method_version": "GLOBAL_CONTEXT_LLM_COMPACTION_V2",
        "snapshot_id": None,
        "status": "GLOBAL_CONTEXT_DISABLED",
        "use_policy": "IGNORE",
        "advisory_only": True,
        "quality": {
            "freshness_status": "missing",
            "coverage_ratio": 0.0,
            "confidence": 0.0,
            "snapshot_age_hours": None,
        },
        "source_warnings": ["GLOBAL_CONTEXT_DISABLED"],
    }
    pack["payload_hash"] = digest(pack)
    return pack


def describe_variant(name: str, pack: dict) -> dict:
    text = canonical(pack)
    exposure = pack.get("exposure_summary") or {}
    portfolio = exposure.get("portfolio") or {}
    opportunities = exposure.get("opportunities") or {}
    return {
        "variant": name,
        "schema_version": pack.get("schema_version"),
        "use_policy": pack.get("use_policy"),
        "payload_hash": pack.get("payload_hash") or digest(pack),
        "characters": len(text),
        "estimated_tokens": round(len(text) / 4.0),
        "critical_events": len(pack.get("critical_events") or []),
        "currency_signals": len(pack.get("currency_signals") or []),
        "known_asset_overlays": len(pack.get("known_asset_overlays") or []),
        "portfolio_reviews": portfolio.get("total", len(pack.get("portfolio_exposure_review") or [])),
        "opportunity_reviews": opportunities.get("total", len(pack.get("opportunity_exposure_review") or [])),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ag1-db", default="/files/duckdb/ag1_v4_consensus.duckdb")
    parser.add_argument("--global-db", default="/files/duckdb/global_context_v1.duckdb")
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument("--shadow-results", type=Path)
    args = parser.parse_args()

    full = read_latest_pack(args.global_db)
    variants = [baseline_pack(), without_ag9(full), full]
    names = ["historique_sans_contexte", "contexte_ag5_ag8", "contexte_ag5_ag9"]
    shadow = None
    if args.shadow_results and args.shadow_results.is_file():
        shadow = json.loads(args.shadow_results.read_text(encoding="utf-8"))
    report = {
        "mode": "READ_ONLY_NO_ORDER_NO_LLM",
        "variants": [describe_variant(name, pack) for name, pack in zip(names, variants)],
        "historical_runs": read_historical_runs(args.ag1_db, args.runs),
        "shadow_results": shadow,
        "proposal_variation": "AVAILABLE_FROM_SHADOW_RESULTS" if shadow is not None else "NOT_RUN_OFFLINE_NO_LLM",
        "consensus_variation": "AVAILABLE_FROM_SHADOW_RESULTS" if shadow is not None else "NOT_RUN_OFFLINE_NO_LLM",
        "ag9_affected_decisions": "REQUIRES_CAPTURED_SHADOW_RESULTS",
        "extra_latency_ms": None,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
