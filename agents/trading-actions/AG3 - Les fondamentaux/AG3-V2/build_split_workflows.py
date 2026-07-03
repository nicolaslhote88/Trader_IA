#!/usr/bin/env python3
"""Generate the two split AG3-V2 fundamental workflows from the live base export.

Sprint 1 (2026-06-22): AG3 was a single workflow scanning the whole `enabled`
universe (quarantine included) by batches of 50 -> full refresh ~8 trading days
(avg data age 15.7 days). We split it like AG2-V3 into two segment-driven
workflows reading `ag2_v3.duckdb.universe_segments`:

  * Held+Core      -> segments HELD + CORE_AUTO, refreshed at least every 24h.
  * Watchlist      -> segment WATCHLIST (non-quarantine remainder), freshness < 5 days.

Quarantined symbols (segment absent / not active) are excluded by construction.

Base = live export `AG3-V2-workflow.json` (the repo build_workflow.py is stale:
it still emits a Google Sheets "Read Universe" node, whereas the live workflow
reads DuckDB). Run:  python build_split_workflows.py
"""
from __future__ import annotations

import copy
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BASE = ROOT / "AG3-V2-workflow.json"

# --- Read Universe node: original WHERE clause (live) and its segment-filtered form ---
READ_UNIVERSE_NODE = "AG3V2.01 - Read Universe"
DUCKDB_INIT_NODE = "AG3V2.02B - DuckDB Init Run"

ORIG_WHERE = (
    "        FROM universe\n"
    "        WHERE COALESCE(enabled, TRUE) = TRUE\n"
    "        ORDER BY symbol"
)


def segment_where(segments: list[str]) -> str:
    seg_list = ", ".join("'" + s + "'" for s in segments)
    return (
        "        FROM universe\n"
        "        WHERE COALESCE(enabled, TRUE) = TRUE\n"
        "          AND UPPER(TRIM(symbol)) IN (\n"
        "            SELECT UPPER(TRIM(symbol)) FROM universe_segments\n"
        "            WHERE COALESCE(active, TRUE) AND segment IN (" + seg_list + ")\n"
        "          )\n"
        "        ORDER BY symbol"
    )


VARIANTS = [
    {
        "id": "AG3V2HELDCORE20260622",
        "name": "AG3-V2 — Fundamental Held+Core",
        "file": "AG3-V2-Fundamental-Held-Core.workflow.json",
        # Quotidien (week-ends inclus) a 01:00 UTC -> garantit une fraicheur <= 24h
        # sur les positions detenues + le top 50 (CORE_AUTO).
        "cron": "0 1 * * *",
        "segments": ["HELD", "CORE_AUTO"],
        # ~56 symboles aujourd'hui : un seul batch couvre tout (pas de rotation reelle).
        "batch_size": 80,
        "batch_state_key": "ag3_v2_held_core_last_index",
    },
    {
        "id": "AG3V2WATCHNIGHT20260622",
        "name": "AG3-V2 — Fundamental Watchlist Nightly",
        "file": "AG3-V2-Fundamental-Watchlist-Nightly.workflow.json",
        # Quotidien a 02:00 UTC (apres Held+Core, pas de collision lock DuckDB).
        # ~198 symboles / batch 60 -> cycle complet ~4 jours calendaires (< 5 j cible).
        "cron": "0 2 * * *",
        "segments": ["WATCHLIST"],
        "batch_size": 60,
        "batch_state_key": "ag3_v2_watchlist_last_index",
    },
]


def load_base() -> dict:
    raw = json.loads(BASE.read_text(encoding="utf-8-sig"))
    return raw[0] if isinstance(raw, list) else raw


def patch_read_universe(code: str, segments: list[str]) -> str:
    if ORIG_WHERE not in code:
        raise SystemExit(
            "ERREUR: clause WHERE de reference introuvable dans '"
            + READ_UNIVERSE_NODE + "'. Le node live a change, mettre a jour ORIG_WHERE."
        )
    return code.replace(ORIG_WHERE, segment_where(segments), 1)


def patch_duckdb_init(code: str, batch_state_key: str, batch_size: int) -> str:
    out = code
    if 'BATCH_STATE_KEY = "ag3_v2_last_index"' not in out:
        raise SystemExit("ERREUR: BATCH_STATE_KEY de reference introuvable dans " + DUCKDB_INIT_NODE)
    out = out.replace(
        'BATCH_STATE_KEY = "ag3_v2_last_index"',
        'BATCH_STATE_KEY = "' + batch_state_key + '"',
        1,
    )
    out = out.replace("DEFAULT_BATCH_SIZE = 50", "DEFAULT_BATCH_SIZE = " + str(batch_size), 1)
    return out


def configure(base: dict, variant: dict) -> dict:
    wf = copy.deepcopy(base)
    wf["id"] = variant["id"]
    wf["name"] = variant["name"]
    wf["active"] = True
    wf["versionId"] = variant["id"].lower() + "-v1"
    for k in ("updatedAt", "createdAt", "shared", "activeVersionId", "versionCounter", "triggerCount"):
        wf.pop(k, None)
    for node in wf.get("nodes", []):
        if node.get("type") == "n8n-nodes-base.scheduleTrigger":
            node["parameters"] = {
                "rule": {"interval": [{"field": "cronExpression", "expression": variant["cron"]}]}
            }
        if node.get("name") == READ_UNIVERSE_NODE:
            node["parameters"]["pythonCode"] = patch_read_universe(
                node["parameters"]["pythonCode"], variant["segments"]
            )
        if node.get("name") == DUCKDB_INIT_NODE:
            node["parameters"]["pythonCode"] = patch_duckdb_init(
                node["parameters"]["pythonCode"],
                variant["batch_state_key"],
                variant["batch_size"],
            )
        if node.get("type") == "n8n-nodes-base.stickyNote":
            node["parameters"]["content"] = (
                variant["name"] + " — segments " + "+".join(variant["segments"])
                + " ; batch " + str(variant["batch_size"]) + " ; cron " + variant["cron"]
            )
    return wf


def main() -> None:
    base = load_base()
    for variant in VARIANTS:
        wf = configure(base, variant)
        out = ROOT / variant["file"]
        out.write_text(json.dumps([wf], ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print("written:", out)


if __name__ == "__main__":
    main()
