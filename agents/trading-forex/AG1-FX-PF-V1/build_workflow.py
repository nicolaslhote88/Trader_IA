#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

DIR = Path(__file__).resolve().parent
NODES_DIR = DIR / "nodes"


def load_code(filename: str) -> str:
    return (NODES_DIR / filename).read_text(encoding="utf-8")


def build() -> dict:
    nodes = [
        {
            "parameters": {
                "rule": {
                    "interval": [
                        {
                            "field": "cronExpression",
                            "expression": "0 0 * * * 1-5",
                        }
                    ]
                }
            },
            "type": "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.3,
            "position": [-420, -80],
            "id": "ag1-fx-pf-schedule",
            "name": "Schedule Trigger",
        },
        {
            "parameters": {},
            "type": "n8n-nodes-base.manualTrigger",
            "typeVersion": 1,
            "position": [-420, 80],
            "id": "ag1-fx-pf-manual",
            "name": "Manual Trigger",
        },
        {
            "parameters": {
                "assignments": {
                    "assignments": [
                        {
                            "id": "ag1fxpf-00-01",
                            "name": "yfinance_api_base",
                            "value": "http://yfinance-api:8080",
                            "type": "string",
                        },
                        {
                            "id": "ag1fxpf-00-02",
                            "name": "interval",
                            "value": "1h",
                            "type": "string",
                        },
                        {
                            "id": "ag1fxpf-00-03",
                            "name": "lookback_days",
                            "value": "5",
                            "type": "string",
                        },
                        {
                            "id": "ag1fxpf-00-04",
                            "name": "max_bars",
                            "value": "10",
                            "type": "string",
                        },
                        {
                            "id": "ag1fxpf-00-05",
                            "name": "min_bars",
                            "value": "1",
                            "type": "string",
                        },
                        {
                            "id": "ag1fxpf-00-06",
                            "name": "allow_stale",
                            "value": True,
                            "type": "boolean",
                        },
                        {
                            "id": "ag1fxpf-00-07",
                            "name": "portfolio_db_paths_json",
                            "value": (
                                "["
                                "\"/files/duckdb/ag1_fx_v1_chatgpt52.duckdb\","
                                "\"/files/duckdb/ag1_fx_v1_grok41_reasoning.duckdb\","
                                "\"/files/duckdb/ag1_fx_v1_gemini30_pro.duckdb\""
                                "]"
                            ),
                            "type": "string",
                        },
                        {
                            "id": "ag1fxpf-00-08",
                            "name": "ag2_fx_path",
                            "value": "/files/duckdb/ag2_fx_v1.duckdb",
                            "type": "string",
                        },
                        {
                            "id": "ag1fxpf-00-09",
                            "name": "schema_path",
                            "value": "/files/AG1-FX-V1-EXPORT/sql/ag1_fx_v1_schema.sql",
                            "type": "string",
                        },
                        {
                            "id": "ag1fxpf-00-10",
                            "name": "request_timeout_seconds",
                            "value": "5",
                            "type": "string",
                        },
                        {
                            "id": "ag1fxpf-00-11",
                            "name": "max_price_workers",
                            "value": "8",
                            "type": "string",
                        },
                        {
                            "id": "ag1fxpf-00-12",
                            "name": "workflow_name",
                            "value": "AG1-FX-PF-V1 hourly portfolio valuation",
                            "type": "string",
                        },
                    ]
                },
                "options": {},
            },
            "type": "n8n-nodes-base.set",
            "typeVersion": 3.4,
            "position": [-160, 0],
            "id": "ag1-fx-pf-config",
            "name": "PF.00 - Config",
        },
        {
            "parameters": {
                "language": "pythonNative",
                "pythonCode": load_code("01_update_fx_portfolio_valuation.py"),
            },
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [120, 0],
            "id": "ag1-fx-pf-update-valuation",
            "name": "PF.01 - Update FX Portfolio Valuation",
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {
                "content": (
                    "AG1-FX-PF-V1: hourly mark-to-market for the three dedicated AG1-FX DuckDB portfolios. "
                    "Reads open lots, refreshes FX prices through yfinance-api with AG2-FX fallback, and writes "
                    "core.portfolio_snapshot rows without asking the LLM for new decisions."
                ),
                "height": 220,
                "width": 760,
                "color": 5,
            },
            "type": "n8n-nodes-base.stickyNote",
            "typeVersion": 1,
            "position": [-460, -300],
            "id": "ag1-fx-pf-note",
            "name": "Note",
        },
    ]

    connections = {
        "Schedule Trigger": {"main": [[{"node": "PF.00 - Config", "type": "main", "index": 0}]]},
        "Manual Trigger": {"main": [[{"node": "PF.00 - Config", "type": "main", "index": 0}]]},
        "PF.00 - Config": {
            "main": [[{"node": "PF.01 - Update FX Portfolio Valuation", "type": "main", "index": 0}]]
        },
    }

    return {
        "name": "AG1-FX-PF-V1 - Hourly Portfolio Valuation",
        "nodes": nodes,
        "connections": connections,
        "settings": {"timezone": "Europe/Paris"},
        "pinData": {},
        "meta": {
            "templateCredsSetupCompleted": True,
            "instanceId": "093ba1266e85fcaaa6abca411d9f7941951737b3920bfaa6e2db89226df2c82d",
        },
    }


def main() -> None:
    wf = build()
    out = DIR / "AG1-FX-PF-V1-workflow.json"
    out.write_text(json.dumps(wf, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
