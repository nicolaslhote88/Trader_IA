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
    return {"parameters": params, "type": "n8n-nodes-base.code", "typeVersion": 2, "position": [x, y], "id": name.lower().replace(" ", "-"), "name": name}


def build():
    nodes = [
        {
            # 2x/day at 09:15 and 14:15 inside Paris stock exchange opening window. Updated 2026-04-26.
            "parameters": {"rule": {"interval": [{"field": "cronExpression", "expression": "15 9,14 * * 1-5"}]}},
            "type": "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.3,
            "position": [-900, -120],
            "id": "schedule",
            "name": "Schedule Trigger",
        },
        {"parameters": {}, "type": "n8n-nodes-base.manualTrigger", "typeVersion": 1, "position": [-900, 80], "id": "manual", "name": "Manual Trigger"},
        node("01 Init Run FX", "01_init_run_fx.js", -660, -20, "javaScript"),
        node("02 Pull Global FX News", "02_pull_global_fx_news.py", -420, -20),
        node("03 Pull FX Channel News", "03_pull_fx_channel_news.py", -180, -20),
        node("04 Dedupe And Score", "04_dedupe_and_score.py", 60, -20),
        node("05 Compute FX Macro Digest", "05_compute_fx_macro_digest.py", 300, -20),
        node("06 Write Digest FX", "06_write_digest_fx.py", 540, -20),
        node("07 Log Run FX", "07_log_run_fx.py", 780, -20),
    ]
    order = [n["name"] for n in nodes[2:]]
    conns = {
        "Schedule Trigger": {"main": [[{"node": order[0], "type": "main", "index": 0}]]},
        "Manual Trigger": {"main": [[{"node": order[0], "type": "main", "index": 0}]]},
    }
    for a, b in zip(order, order[1:]):
        conns[a] = {"main": [[{"node": b, "type": "main", "index": 0}]]}
    return {"name": "AG4-FX-V1 - Digest macro Forex", "nodes": nodes, "connections": conns, "settings": {"timezone": "Europe/Paris"}}


if __name__ == "__main__":
    out = ROOT / "workflow" / "AG4_FX_workflow_v1.json"
    out.write_text(json.dumps(build(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {out}")
