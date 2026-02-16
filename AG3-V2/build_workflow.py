#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

DIR = Path(__file__).resolve().parent
NODES_DIR = DIR / "nodes"


def load_code(filename: str) -> str:
    return (NODES_DIR / filename).read_text(encoding="utf-8")


def build() -> dict:
    doc_id = "1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I"
    creds = {"googleSheetsOAuth2Api": {"id": "aX5iAQEN9HK4UGjr", "name": "Google Sheets account"}}

    nodes = [
        {
            "parameters": {
                "rule": {"interval": [{"field": "cronExpression", "expression": "0 7 * * 1-5"}]}
            },
            "type": "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.3,
            "position": [-1424, -224],
            "id": "dcd6b2a3-cc74-4f03-8588-b8a24fe9908d",
            "name": "Schedule Trigger",
        },
        {
            "parameters": {},
            "type": "n8n-nodes-base.manualTrigger",
            "typeVersion": 1,
            "position": [-1424, -64],
            "id": "092be7cc-16e7-4904-8d73-bf0ae727f25a",
            "name": "Manual Trigger",
        },
        {
            "parameters": {"jsCode": load_code("00_init_context.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-1184, -144],
            "id": "013e013b-4655-4bdb-8f2b-66fdffb09e95",
            "name": "AG3V2.00 - Init Context",
        },
        {
            "parameters": {
                "documentId": {
                    "__rl": True,
                    "mode": "list",
                    "value": doc_id,
                    "cachedResultName": "TradingSim_GoogleSheet_Template",
                    "cachedResultUrl": f"https://docs.google.com/spreadsheets/d/{doc_id}/edit",
                },
                "sheetName": {
                    "__rl": True,
                    "mode": "list",
                    "value": "Universe",
                    "cachedResultName": "Universe",
                    "cachedResultUrl": f"https://docs.google.com/spreadsheets/d/{doc_id}/edit",
                },
                "options": {},
            },
            "type": "n8n-nodes-base.googleSheets",
            "typeVersion": 4.7,
            "position": [-960, -144],
            "id": "8e3a18ed-9e2d-43f1-8b12-a90ff3daa49f",
            "name": "AG3V2.01 - Read Universe",
            "credentials": creds,
        },
        {
            "parameters": {"jsCode": load_code("01_build_queue.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-736, -144],
            "id": "d7dc6bbd-c6b9-402f-b2da-21594941e731",
            "name": "AG3V2.02 - Build Queue",
        },
        {
            "parameters": {"language": "pythonNative", "pythonCode": load_code("06_duckdb_init.py")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-512, -144],
            "id": "d75538b7-502e-463e-ad73-69b940339f2c",
            "name": "AG3V2.02B - DuckDB Init Run",
        },
        {
            "parameters": {"options": {}},
            "type": "n8n-nodes-base.splitInBatches",
            "typeVersion": 3,
            "position": [-304, -144],
            "id": "b3f5f89f-7c85-49ad-907f-f715ea79754a",
            "name": "AG3V2.03 - Split Symbols",
        },
        {
            "parameters": {
                "url": "={{$json.api_base}}/fundamentals",
                "sendQuery": True,
                "queryParameters": {"parameters": [{"name": "symbol", "value": "={{$json.Symbol}}"}]},
                "options": {"timeout": 30000},
            },
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4,
            "position": [-80, -256],
            "id": "3208f39c-278d-4a0d-b3a6-c7f89f2e6f50",
            "name": "AG3V2.04 - HTTP Fundamentals",
            "retryOnFail": True,
            "maxTries": 3,
            "waitBetweenTries": 2000,
            "continueOnFail": True,
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {"mode": "combine", "combineBy": "combineByPosition", "options": {}},
            "type": "n8n-nodes-base.merge",
            "typeVersion": 3.2,
            "position": [128, -144],
            "id": "045b4a91-afca-4c67-b1d9-03b7f0107f3a",
            "name": "AG3V2.05 - Merge Queue + API",
        },
        {
            "parameters": {"jsCode": load_code("02_score_fundamentals.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [336, -144],
            "id": "f21995ee-8c01-43dc-b90a-fcf877539f61",
            "name": "AG3V2.06 - Score Fundamentals",
        },
        {
            "parameters": {"language": "pythonNative", "pythonCode": load_code("07_write_fundamentals_duckdb.py")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [560, -144],
            "id": "568aee73-8511-4db6-885d-f90e85dc7ca8",
            "name": "AG3V2.07 - Write Fundamentals DuckDB",
        },
        {
            "parameters": {"amount": 0.4, "unit": "seconds"},
            "type": "n8n-nodes-base.wait",
            "typeVersion": 1.1,
            "position": [768, -144],
            "id": "51f057cb-5dcb-4bc9-95a7-a8161eb45f7b",
            "name": "AG3V2.08 - Wait Rate Limit",
        },
        {
            "parameters": {"language": "pythonNative", "pythonCode": load_code("08_finalize_run.py")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-80, 64],
            "id": "1a4ac7a0-e2ac-4eb0-bbc5-eb33f2533204",
            "name": "AG3V2.09 - Finalize Run",
        },
        {
            "parameters": {
                "content": "AG3-V2 (DuckDB-first): yfinance fundamentals -> scoring -> DuckDB only (no Google Sheets writes).",
                "height": 160,
                "width": 1160,
                "color": 5,
            },
            "type": "n8n-nodes-base.stickyNote",
            "typeVersion": 1,
            "position": [-1488, -336],
            "id": "f46735a8-4b73-4685-9493-bf170c0afcb6",
            "name": "AG3V2 - Note",
        },
    ]

    connections = {
        "Schedule Trigger": {
            "main": [[{"node": "AG3V2.00 - Init Context", "type": "main", "index": 0}]]
        },
        "Manual Trigger": {
            "main": [[{"node": "AG3V2.00 - Init Context", "type": "main", "index": 0}]]
        },
        "AG3V2.00 - Init Context": {
            "main": [[{"node": "AG3V2.01 - Read Universe", "type": "main", "index": 0}]]
        },
        "AG3V2.01 - Read Universe": {
            "main": [[{"node": "AG3V2.02 - Build Queue", "type": "main", "index": 0}]]
        },
        "AG3V2.02 - Build Queue": {
            "main": [[{"node": "AG3V2.02B - DuckDB Init Run", "type": "main", "index": 0}]]
        },
        "AG3V2.02B - DuckDB Init Run": {
            "main": [[{"node": "AG3V2.03 - Split Symbols", "type": "main", "index": 0}]]
        },
        "AG3V2.03 - Split Symbols": {
            "main": [
                [
                    {"node": "AG3V2.04 - HTTP Fundamentals", "type": "main", "index": 0},
                    {"node": "AG3V2.05 - Merge Queue + API", "type": "main", "index": 1},
                ],
                [
                    {"node": "AG3V2.09 - Finalize Run", "type": "main", "index": 0}
                ],
            ]
        },
        "AG3V2.04 - HTTP Fundamentals": {
            "main": [[{"node": "AG3V2.05 - Merge Queue + API", "type": "main", "index": 0}]]
        },
        "AG3V2.05 - Merge Queue + API": {
            "main": [[{"node": "AG3V2.06 - Score Fundamentals", "type": "main", "index": 0}]]
        },
        "AG3V2.06 - Score Fundamentals": {
            "main": [[{"node": "AG3V2.07 - Write Fundamentals DuckDB", "type": "main", "index": 0}]]
        },
        "AG3V2.07 - Write Fundamentals DuckDB": {
            "main": [[{"node": "AG3V2.08 - Wait Rate Limit", "type": "main", "index": 0}]]
        },
        "AG3V2.08 - Wait Rate Limit": {
            "main": [[{"node": "AG3V2.03 - Split Symbols", "type": "main", "index": 0}]]
        },
    }

    return {
        "name": "AG3-V2 - Fundamental Analyst (DuckDB-first)",
        "nodes": nodes,
        "connections": connections,
        "settings": {"executionOrder": "v1"},
        "pinData": {},
        "meta": {"templateCredsSetupCompleted": True},
    }


def main() -> None:
    wf = build()
    print(json.dumps(wf, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
