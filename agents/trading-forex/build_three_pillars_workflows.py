#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent

AGENTS = [
    {
        "id": "AG5FXMacroPillarsV1",
        "name": "AG5-FX-Macro",
        "dir": "AG5-FX-Macro",
        "cron": "5 3 * * 1-5",
        "nodes": [
            ("01 Init Run", "01_init_run.js", "javaScript"),
            ("02 Refresh Macro Data", "02_refresh_macro_data.py", "pythonNative"),
            ("03 Compute Macro Scores", "03_compute_macro_scores.py", "pythonNative"),
            ("04 Log Run", "04_log_run.py", "pythonNative"),
        ],
    },
    {
        "id": "AG6FXValuationPillarsV1",
        "name": "AG6-FX-Valuation",
        "dir": "AG6-FX-Valuation",
        "cron": "15 3 * * 1-5",
        "nodes": [
            ("01 Init Run", "01_init_run.js", "javaScript"),
            ("02 Fetch Valuation Data", "02_fetch_valuation_data.py", "pythonNative"),
            ("03 Compute Valuation Scores", "03_compute_valuation_scores.py", "pythonNative"),
            ("04 Log Run", "04_log_run.py", "pythonNative"),
        ],
    },
    {
        "id": "AG7FXPositioningPillarsV1",
        "name": "AG7-FX-Positioning",
        "dir": "AG7-FX-Positioning",
        "cron": "25 3 * * 1-5",
        "nodes": [
            ("01 Init Run", "01_init_run.js", "javaScript"),
            ("02 Refresh COT Data", "02_refresh_cot_data.py", "pythonNative"),
            ("03 Load Positioning Scores", "03_load_positioning_scores.py", "pythonNative"),
            ("04 Log Run", "04_log_run.py", "pythonNative"),
        ],
    },
    {
        "id": "AG8FXRatesPillarsV1",
        "name": "AG8-FX-Rates",
        "dir": "AG8-FX-Rates",
        "cron": "35 3 * * 1-5",
        "nodes": [
            ("01 Init Run", "01_init_run.js", "javaScript"),
            ("02 Fetch Yield Curves", "02_fetch_yield_curves.py", "pythonNative"),
            ("03 Compute Rates Signals", "03_compute_rates_signals.py", "pythonNative"),
            ("04 Log Run", "04_log_run.py", "pythonNative"),
        ],
    },
]


def read_node(agent_dir: str, filename: str) -> str:
    return (ROOT / agent_dir / "nodes" / filename).read_text(encoding="utf-8")


def code_node(agent_dir: str, label: str, filename: str, language: str, x: int, y: int) -> dict:
    params_key = "jsCode" if language == "javaScript" else "pythonCode"
    return {
        "parameters": {
            "language": language,
            params_key: read_node(agent_dir, filename),
        },
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [x, y],
        "id": label.lower().replace(" ", "-"),
        "name": label,
    }


def build(agent: dict) -> dict:
    nodes = [
        {
            "parameters": {"rule": {"interval": [{"field": "cronExpression", "expression": agent["cron"]}]}},
            "type": "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.3,
            "position": [-920, -120],
            "id": "schedule",
            "name": "Schedule Trigger",
        },
        {
            "parameters": {},
            "type": "n8n-nodes-base.manualTrigger",
            "typeVersion": 1,
            "position": [-920, 80],
            "id": "manual",
            "name": "Manual Trigger",
        },
    ]
    x = -680
    for label, filename, language in agent["nodes"]:
        nodes.append(code_node(agent["dir"], label, filename, language, x, -20))
        x += 260

    chain = [label for label, _, _ in agent["nodes"]]
    connections = {
        "Schedule Trigger": {"main": [[{"node": chain[0], "type": "main", "index": 0}]]},
        "Manual Trigger": {"main": [[{"node": chain[0], "type": "main", "index": 0}]]},
    }
    for source, target in zip(chain, chain[1:]):
        connections[source] = {"main": [[{"node": target, "type": "main", "index": 0}]]}

    return {
        "id": agent["id"],
        "name": agent["name"],
        "nodes": nodes,
        "connections": connections,
        "settings": {"timezone": "Europe/Paris"},
    }


def main() -> None:
    for agent in AGENTS:
        out = ROOT / agent["dir"] / "workflow" / f"{agent['name']}_workflow_v1.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(build(agent), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {out}")


if __name__ == "__main__":
    main()
