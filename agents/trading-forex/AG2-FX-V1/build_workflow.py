#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
NODES = ROOT / "nodes"


def code(name: str) -> str:
    return (NODES / name).read_text(encoding="utf-8")


def node(name, filename, x, y, lang="pythonNative"):
    params = {"language": lang, "pythonCode": code(filename)} if lang == "pythonNative" else {"jsCode": code(filename)}
    return {
        "parameters": params,
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [x, y],
        "id": name.lower().replace(" ", "-"),
        "name": name,
    }


def build():
    nodes = [
        {
            # 6x/day every 4h covering forex 24/5 (Asia/Europe/US). Updated 2026-04-26.
            "parameters": {"rule": {"interval": [{"field": "cronExpression", "expression": "0 0,4,8,12,16,20 * * 1-5"}]}},
            "type": "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.3,
            "position": [-900, -120],
            "id": "schedule",
            "name": "Schedule Trigger",
        },
        {"parameters": {}, "type": "n8n-nodes-base.manualTrigger", "typeVersion": 1, "position": [-900, 80], "id": "manual", "name": "Manual Trigger"},
        node("01 Init Config FX", "01_init_config_fx.js", -660, -20, "javaScript"),
        node("02 Load FX Universe", "02_load_fx_universe.py", -420, -20),
        node("03 Fetch YFinance FX", "03_fetch_yfinance_fx.py", -180, -20),
        node("04 Compute Indicators FX", "04_compute_indicators_fx.py", 60, -20),
        node("05 Compute Levels FX", "05_compute_levels_fx.py", 300, -20),
        node("06 Compute Regime FX", "06_compute_regime_fx.py", 540, -20),
        node("07 Score Signal FX", "07_score_signal_fx.py", 780, -20),
        node("08 Write Universe FX", "08_write_universe_fx.py", 1020, -20),
        node("09 Write Signals FX", "09_write_signals_fx.py", 1260, -20),
        node("10 Log Run FX", "10_log_run_fx.py", 1500, -20),
        node("11 Build Vector Docs FX", "11_build_vector_docs_fx.py", 1740, -20),
    ]
    order = [n["name"] for n in nodes[2:]]
    conns = {
        "Schedule Trigger": {"main": [[{"node": order[0], "type": "main", "index": 0}]]},
        "Manual Trigger": {"main": [[{"node": order[0], "type": "main", "index": 0}]]},
    }
    for a, b in zip(order, order[1:]):
        conns[a] = {"main": [[{"node": b, "type": "main", "index": 0}]]}
    return {"name": "AG2-FX-V1 - Analyse technique Forex", "nodes": nodes, "connections": conns, "settings": {"timezone": "Europe/Paris"}}


if __name__ == "__main__":
    out = ROOT / "workflow" / "AG2_FX_workflow_v1.json"
    out.write_text(json.dumps(build(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {out}")
