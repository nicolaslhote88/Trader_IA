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
                            "expression": "0 0 9-17 * * 1-5",
                        }
                    ]
                }
            },
            "type": "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.3,
            "position": [-240, -80],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f101",
            "name": "Schedule Trigger",
        },
        {
            "parameters": {},
            "type": "n8n-nodes-base.manualTrigger",
            "typeVersion": 1,
            "position": [-240, 80],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f102",
            "name": "Manual Trigger",
        },
        {
            "parameters": {
                "assignments": {
                    "assignments": [
                        {
                            "id": "pf00-01",
                            "name": "yfinance_api_base",
                            "value": "http://yfinance-api:8080",
                            "type": "string",
                        },
                        {
                            "id": "pf00-02",
                            "name": "interval",
                            "value": "1d",
                            "type": "string",
                        },
                        {
                            "id": "pf00-03",
                            "name": "lookback_days",
                            "value": "10",
                            "type": "string",
                        },
                        {
                            "id": "pf00-04",
                            "name": "max_bars",
                            "value": "5",
                            "type": "string",
                        },
                        {
                            "id": "pf00-05",
                            "name": "min_bars",
                            "value": "1",
                            "type": "string",
                        },
                        {
                            "id": "pf00-06",
                            "name": "allow_stale",
                            "value": True,
                            "type": "boolean",
                        },
                        {
                            "id": "pf00-07",
                            "name": "portfolio_db_path",
                            "value": "/files/duckdb/ag1_v2.duckdb",
                            "type": "string",
                        },
                        {
                            "id": "pf00-08",
                            "name": "workflow_name",
                            "value": "PF Portfolio MTM Updater",
                            "type": "string",
                        },
                        {
                            "id": "pf00-09",
                            "name": "run_id",
                            "value": "={{ 'PFMTM_' + $now.toFormat('yyyyLLddHHmmss') }}",
                            "type": "string",
                        },
                    ]
                },
                "options": {},
            },
            "type": "n8n-nodes-base.set",
            "typeVersion": 3.4,
            "position": [0, 0],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f103",
            "name": "PF.00 - Config",
        },
        {
            "parameters": {
                "documentId": {
                    "__rl": True,
                    "value": "1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I",
                    "mode": "list",
                    "cachedResultName": "TradingSim_GoogleSheet_Template",
                    "cachedResultUrl": "https://docs.google.com/spreadsheets/d/1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I/edit?usp=drivesdk",
                },
                "sheetName": {
                    "__rl": True,
                    "value": 757810202,
                    "mode": "list",
                    "cachedResultName": "Portefeuille",
                    "cachedResultUrl": "https://docs.google.com/spreadsheets/d/1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I/edit#gid=757810202",
                },
                "options": {},
            },
            "type": "n8n-nodes-base.googleSheets",
            "typeVersion": 4.7,
            "position": [240, 0],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f104",
            "name": "Read Portfolio",
            "credentials": {
                "googleSheetsOAuth2Api": {
                    "id": "aX5iAQEN9HK4UGjr",
                    "name": "Google Sheets account",
                }
            },
        },
        {
            "parameters": {
                "documentId": {
                    "__rl": True,
                    "value": "1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I",
                    "mode": "list",
                    "cachedResultName": "TradingSim_GoogleSheet_Template",
                    "cachedResultUrl": "https://docs.google.com/spreadsheets/d/1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I/edit?usp=drivesdk",
                },
                "sheetName": {
                    "__rl": True,
                    "value": 1078848687,
                    "mode": "list",
                    "cachedResultName": "Universe",
                    "cachedResultUrl": "https://docs.google.com/spreadsheets/d/1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I/edit#gid=1078848687",
                },
                "filtersUI": {
                    "values": [
                        {
                            "lookupColumn": "Symbol",
                            "lookupValue": "={{ $json.Symbol || '' }}",
                        }
                    ]
                },
                "options": {},
            },
            "type": "n8n-nodes-base.googleSheets",
            "typeVersion": 4.7,
            "position": [480, 128],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f105",
            "name": "Get Universe Row",
            "credentials": {
                "googleSheetsOAuth2Api": {
                    "id": "aX5iAQEN9HK4UGjr",
                    "name": "Google Sheets account",
                }
            },
        },
        {
            "parameters": {"jsCode": load_code("09_filter_universe_columns.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [704, 128],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f106",
            "name": "Filter Universe Columns",
        },
        {
            "parameters": {
                "mode": "combine",
                "fieldsToMatchString": "Symbol",
                "joinMode": "enrichInput1",
                "options": {},
            },
            "type": "n8n-nodes-base.merge",
            "typeVersion": 3.2,
            "position": [928, 0],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f107",
            "name": "Merge Portfolio + Universe",
        },
        {
            "parameters": {"jsCode": load_code("03_normalize_positions.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1152, 0],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f108",
            "name": "PF.02 - Normalize Positions",
        },
        {
            "parameters": {"options": {}},
            "type": "n8n-nodes-base.splitInBatches",
            "typeVersion": 3,
            "position": [1376, 0],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f109",
            "name": "Loop Over Items",
        },
        {
            "parameters": {
                "url": "={{ $json.yfinance_api_base }}/history",
                "sendQuery": True,
                "queryParameters": {
                    "parameters": [
                        {"name": "symbol", "value": "={{$json.symbol}}"},
                        {"name": "max_bars", "value": "={{ $json.max_bars }}"},
                        {"name": "min_bars", "value": "={{ $json.min_bars }}"},
                        {"name": "lookback_days", "value": "={{ $json.lookback_days }}"},
                        {"name": "allow_stale", "value": "={{ $json.allow_stale }}"},
                        {"name": "interval", "value": "={{ $json.interval }}"},
                    ]
                },
                "options": {
                    "response": {"response": {"responseFormat": "json"}},
                    "timeout": 60000,
                },
            },
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.3,
            "position": [1616, 112],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f110",
            "name": "PF.05A - Fetch Price 1D",
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {
                "url": "={{ $json.yfinance_api_base }}/history",
                "sendQuery": True,
                "queryParameters": {
                    "parameters": [
                        {"name": "symbol", "value": "={{$json.symbol}}"},
                        {"name": "max_bars", "value": "={{ $json.max_bars }}"},
                        {"name": "min_bars", "value": "={{ $json.min_bars }}"},
                        {"name": "lookback_days", "value": "={{ $json.lookback_days }}"},
                        {"name": "allow_stale", "value": "={{ $json.allow_stale }}"},
                        {"name": "interval", "value": "1h"},
                    ]
                },
                "options": {
                    "response": {"response": {"responseFormat": "json"}},
                    "timeout": 60000,
                },
            },
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.3,
            "position": [1616, 304],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f111",
            "name": "PF.05B - Fetch Price 1H",
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {"jsCode": load_code("05a_wrap_1d.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1824, 112],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f112",
            "name": "PF.05A - Wrap 1D",
        },
        {
            "parameters": {"jsCode": load_code("05b_wrap_1h.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1824, 304],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f113",
            "name": "PF.05B - Wrap 1H",
        },
        {
            "parameters": {
                "mode": "combine",
                "fieldsToMatchString": "symbol",
                "joinMode": "enrichInput1",
                "options": {},
            },
            "type": "n8n-nodes-base.merge",
            "typeVersion": 3.2,
            "position": [2048, 32],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f114",
            "name": "PF.06A - Merge (portfolio + 1D)",
        },
        {
            "parameters": {
                "mode": "combine",
                "fieldsToMatchString": "symbol",
                "joinMode": "enrichInput1",
                "options": {},
            },
            "type": "n8n-nodes-base.merge",
            "typeVersion": 3.2,
            "position": [2272, 208],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f115",
            "name": "PF.06B - Merge (+ 1H)",
        },
        {
            "parameters": {"jsCode": load_code("07_compute_mtm.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [2496, 208],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f116",
            "name": "PF.07 - Compute MTM",
        },
        {
            "parameters": {"jsCode": load_code("08_build_sheet_updates.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1616, -160],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f117",
            "name": "PF.08 - Build Sheet Updates",
        },
        {
            "parameters": {
                "language": "pythonNative",
                "pythonCode": load_code("01_write_positions_mtm_duckdb.py"),
            },
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1840, -160],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f118",
            "name": "PF.08B - Write Positions MTM DuckDB",
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {
                "operation": "update",
                "documentId": {
                    "__rl": True,
                    "value": "1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I",
                    "mode": "list",
                    "cachedResultName": "TradingSim_GoogleSheet_Template",
                    "cachedResultUrl": "https://docs.google.com/spreadsheets/d/1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I/edit?usp=drivesdk",
                },
                "sheetName": {
                    "__rl": True,
                    "value": 757810202,
                    "mode": "list",
                    "cachedResultName": "Portefeuille",
                    "cachedResultUrl": "https://docs.google.com/spreadsheets/d/1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I/edit#gid=757810202",
                },
                "columns": {
                    "mappingMode": "defineBelow",
                    "value": {
                        "row_number": "={{ $json.row_number }}",
                        "LastPrice": "={{ $json.LastPrice }}",
                        "MarketValue": "={{ $json.MarketValue }}",
                        "UnrealizedPnL": "={{ $json.UnrealizedPnL }}",
                        "UpdatedAt": "={{ $json.UpdatedAt }}",
                        "Name": "={{ $json.Name }}",
                        "AssetClass": "={{ $json.AssetClass }}",
                        "Sector": "={{ $json.Sector }}",
                        "Industry": "={{ $json.Industry }}",
                    },
                    "matchingColumns": ["row_number"],
                    "attemptToConvertTypes": False,
                    "convertFieldsToString": False,
                },
                "options": {},
            },
            "type": "n8n-nodes-base.googleSheets",
            "typeVersion": 4.7,
            "position": [2064, -160],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f119",
            "name": "Update Portfolio Row",
            "credentials": {
                "googleSheetsOAuth2Api": {
                    "id": "aX5iAQEN9HK4UGjr",
                    "name": "Google Sheets account",
                }
            },
        },
        {
            "parameters": {
                "content": "PF workflow: DuckDB is persisted at PF.08B, Google Sheets stays updated for manual control.",
                "height": 220,
                "width": 900,
                "color": 5,
            },
            "type": "n8n-nodes-base.stickyNote",
            "typeVersion": 1,
            "position": [-280, -240],
            "id": "f92de8a2-4f5a-4de6-a123-a7eb60a8f120",
            "name": "Note",
        },
    ]

    connections = {
        "Schedule Trigger": {"main": [[{"node": "PF.00 - Config", "type": "main", "index": 0}]]},
        "Manual Trigger": {"main": [[{"node": "PF.00 - Config", "type": "main", "index": 0}]]},
        "PF.00 - Config": {"main": [[{"node": "Read Portfolio", "type": "main", "index": 0}]]},
        "Read Portfolio": {
            "main": [
                [
                    {"node": "Get Universe Row", "type": "main", "index": 0},
                    {"node": "Merge Portfolio + Universe", "type": "main", "index": 0},
                ]
            ]
        },
        "Get Universe Row": {"main": [[{"node": "Filter Universe Columns", "type": "main", "index": 0}]]},
        "Filter Universe Columns": {"main": [[{"node": "Merge Portfolio + Universe", "type": "main", "index": 1}]]},
        "Merge Portfolio + Universe": {"main": [[{"node": "PF.02 - Normalize Positions", "type": "main", "index": 0}]]},
        "PF.02 - Normalize Positions": {"main": [[{"node": "Loop Over Items", "type": "main", "index": 0}]]},
        "Loop Over Items": {
            "main": [
                [{"node": "PF.08 - Build Sheet Updates", "type": "main", "index": 0}],
                [
                    {"node": "PF.05A - Fetch Price 1D", "type": "main", "index": 0},
                    {"node": "PF.05B - Fetch Price 1H", "type": "main", "index": 0},
                    {"node": "PF.06A - Merge (portfolio + 1D)", "type": "main", "index": 0},
                ],
            ]
        },
        "PF.05A - Fetch Price 1D": {"main": [[{"node": "PF.05A - Wrap 1D", "type": "main", "index": 0}]]},
        "PF.05B - Fetch Price 1H": {"main": [[{"node": "PF.05B - Wrap 1H", "type": "main", "index": 0}]]},
        "PF.05A - Wrap 1D": {"main": [[{"node": "PF.06A - Merge (portfolio + 1D)", "type": "main", "index": 1}]]},
        "PF.06A - Merge (portfolio + 1D)": {"main": [[{"node": "PF.06B - Merge (+ 1H)", "type": "main", "index": 0}]]},
        "PF.05B - Wrap 1H": {"main": [[{"node": "PF.06B - Merge (+ 1H)", "type": "main", "index": 1}]]},
        "PF.06B - Merge (+ 1H)": {"main": [[{"node": "PF.07 - Compute MTM", "type": "main", "index": 0}]]},
        "PF.07 - Compute MTM": {"main": [[{"node": "Loop Over Items", "type": "main", "index": 0}]]},
        "PF.08 - Build Sheet Updates": {"main": [[{"node": "PF.08B - Write Positions MTM DuckDB", "type": "main", "index": 0}]]},
        "PF.08B - Write Positions MTM DuckDB": {"main": [[{"node": "Update Portfolio Row", "type": "main", "index": 0}]]},
    }

    return {
        "name": "AG1-PF-V1 - Portfolio MTM (DuckDB + Sheets)",
        "nodes": nodes,
        "connections": connections,
        "pinData": {},
        "meta": {
            "templateCredsSetupCompleted": True,
            "instanceId": "093ba1266e85fcaaa6abca411d9f7941951737b3920bfaa6e2db89226df2c82d",
        },
    }


def main() -> None:
    wf = build()
    out = DIR / "AG1-PF-V1-workflow.json"
    out.write_text(json.dumps(wf, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
