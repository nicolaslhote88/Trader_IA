#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
NODES = ROOT / "nodes"


def code(name: str) -> str:
    return (NODES / name).read_text(encoding="utf-8")


def node(name: str, filename: str, x: int, y: int, lang: str = "pythonNative") -> dict:
    params = {"language": lang, "pythonCode": code(filename)} if lang == "pythonNative" else {"jsCode": code(filename)}
    return {
        "parameters": params,
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [x, y],
        "id": name.lower().replace(" ", "-"),
        "name": name,
    }


def build() -> dict:
    nodes = [
        {
            "parameters": {"rule": {"interval": [{"field": "cronExpression", "expression": "20 4,8,12,16,20 * * 1-5"}]}},
            "type": "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.3,
            "position": [-1140, -120],
            "id": "schedule",
            "name": "Schedule Trigger",
        },
        {"parameters": {}, "type": "n8n-nodes-base.manualTrigger", "typeVersion": 1, "position": [-1140, 80], "id": "manual", "name": "Manual Trigger"},
        node("01 Init Run FX", "01_init_run_fx.js", -900, -20, "javaScript"),
        node("02 Load Universe FX", "02_load_universe_fx.py", -660, -20),
        node("03 Load Latest Technical FX", "03_load_latest_technical_fx.py", -420, -20),
        node("04 Load Macro News FX", "04_load_macro_news_fx.py", -180, -20),
        node("05 Fetch Macro Observations", "05_fetch_macro_observations.py", 60, -20),
        node("06 Compute Currency Scores", "06_compute_currency_scores.py", 300, -20),
        node("07 Compute Pair Scores", "07_compute_pair_scores.py", 540, -20),
        node("08 Compute Equilibrium Targets", "08_compute_equilibrium_targets.py", 780, -20),
        node("09 Build AG1 Summary", "09_build_ag1_summary.py", 1020, -20),
        node("10 Write DuckDB FX", "10_write_duckdb_fx.py", 1260, -20),
        node("11 Log Run FX", "11_log_run_fx.py", 1500, -20),
    ]
    order = [n["name"] for n in nodes[2:]]
    conns = {
        "Schedule Trigger": {"main": [[{"node": order[0], "type": "main", "index": 0}]]},
        "Manual Trigger": {"main": [[{"node": order[0], "type": "main", "index": 0}]]},
    }
    for a, b in zip(order, order[1:]):
        conns[a] = {"main": [[{"node": b, "type": "main", "index": 0}]]}
    return {
        "id": "LKYPNuSv_7HZp928wIAn5",
        "name": "AG3-FX-V1 - Forex Fundamental Analyst",
        "nodes": nodes,
        "connections": conns,
        "settings": {"timezone": "Europe/Paris"},
    }


if __name__ == "__main__":
    out = ROOT / "workflow" / "AG3_FX_workflow_v1.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(build(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {out}")
