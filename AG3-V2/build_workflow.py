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
            "position": [-1520, -240],
            "id": "b96fcb6d-90ab-4dc6-9182-9fc5f5117121",
            "name": "Schedule Trigger",
        },
        {
            "parameters": {},
            "type": "n8n-nodes-base.manualTrigger",
            "typeVersion": 1,
            "position": [-1520, -80],
            "id": "c6376e35-3d35-4827-9267-a9529abf9db9",
            "name": "Manual Trigger",
        },
        {
            "parameters": {"jsCode": load_code("00_init_context.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-1280, -160],
            "id": "7bf94f32-95fb-429a-a09c-1fcfc8d79556",
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
            "position": [-1040, -160],
            "id": "622f053a-f64c-4a56-b4da-880f3a56e440",
            "name": "AG3V2.01 - Read Universe",
            "credentials": creds,
        },
        {
            "parameters": {"jsCode": load_code("01_build_queue.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-816, -160],
            "id": "58f0084f-b69f-47b0-8a95-c705e8067f89",
            "name": "AG3V2.02 - Build Queue",
        },
        {
            "parameters": {"options": {}},
            "type": "n8n-nodes-base.splitInBatches",
            "typeVersion": 3,
            "position": [-608, -160],
            "id": "3516ad96-f981-4a32-81ff-b2938f55a8a8",
            "name": "AG3V2.03 - Split Symbols",
        },
        {
            "parameters": {
                "url": "={{$json.api_base}}/fundamentals",
                "sendQuery": True,
                "queryParameters": {
                    "parameters": [{"name": "symbol", "value": "={{$json.Symbol}}"}]
                },
                "options": {"timeout": 30000},
            },
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4,
            "position": [-384, -272],
            "id": "3ba395fc-657e-49f2-bf9f-f0f6e42c8f6a",
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
            "position": [-176, -160],
            "id": "374f30b0-4952-4164-8e5e-5398574e4f4d",
            "name": "AG3V2.05 - Merge Queue + API",
        },
        {
            "parameters": {"jsCode": load_code("02_score_fundamentals.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [32, -160],
            "id": "b8829d80-2643-4625-9bcf-01035548f38e",
            "name": "AG3V2.06 - Score Fundamentals",
        },
        {
            "parameters": {"jsCode": load_code("03_prepare_triage_row.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [272, -272],
            "id": "ce4f7f3f-e9c2-4538-8e86-e8f823849caa",
            "name": "AG3V2.07 - Prepare Triage Row",
        },
        {
            "parameters": {
                "operation": "appendOrUpdate",
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
                    "value": "AG3_Triage_History",
                    "cachedResultName": "AG3_Triage_History",
                    "cachedResultUrl": f"https://docs.google.com/spreadsheets/d/{doc_id}/edit",
                },
                "columns": {
                    "mappingMode": "autoMapInputData",
                    "matchingColumns": ["RecordId"],
                    "attemptToConvertTypes": False,
                    "convertFieldsToString": False,
                },
                "options": {"handlingExtraData": "ignoreIt"},
            },
            "type": "n8n-nodes-base.googleSheets",
            "typeVersion": 4.7,
            "position": [512, -272],
            "id": "20b5ad8f-df11-4121-b0c2-526e56ea0815",
            "name": "AG3V2.08 - Upsert AG3_Triage_History",
            "credentials": creds,
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {"amount": 0.4, "unit": "seconds"},
            "type": "n8n-nodes-base.wait",
            "typeVersion": 1.1,
            "position": [736, -272],
            "id": "6a7e5f58-eb61-4e54-aed4-fd2f4f30167c",
            "name": "AG3V2.09 - Wait Rate Limit",
        },
        {
            "parameters": {"jsCode": load_code("04_prepare_consensus_row.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [272, -112],
            "id": "7999b87a-f3ec-4d1d-ac2f-276d7094d0a9",
            "name": "AG3V2.10 - Prepare Consensus Row",
        },
        {
            "parameters": {
                "operation": "appendOrUpdate",
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
                    "value": "research_analyst_consensus",
                    "cachedResultName": "research_analyst_consensus",
                    "cachedResultUrl": f"https://docs.google.com/spreadsheets/d/{doc_id}/edit",
                },
                "columns": {
                    "mappingMode": "autoMapInputData",
                    "matchingColumns": ["RecordId"],
                    "attemptToConvertTypes": False,
                    "convertFieldsToString": False,
                },
                "options": {"handlingExtraData": "ignoreIt"},
            },
            "type": "n8n-nodes-base.googleSheets",
            "typeVersion": 4.7,
            "position": [512, -112],
            "id": "84cedd7e-5f26-4f54-adcb-6e8e5d0896ec",
            "name": "AG3V2.11 - Upsert research_analyst_consensus",
            "credentials": creds,
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {"jsCode": load_code("05_prepare_metric_rows.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [272, 48],
            "id": "357701ca-baa0-4fb5-9c8f-e299fbb4e71b",
            "name": "AG3V2.12 - Prepare Metric Rows",
        },
        {
            "parameters": {"fieldToSplitOut": "metricRows", "options": {}},
            "type": "n8n-nodes-base.itemLists",
            "typeVersion": 3.1,
            "position": [512, 48],
            "id": "f5e69c54-765a-422f-865b-3690f85e8ab6",
            "name": "AG3V2.13 - Split Metric Rows",
        },
        {
            "parameters": {
                "operation": "appendOrUpdate",
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
                    "value": "Fundamental_Data",
                    "cachedResultName": "Fundamental_Data",
                    "cachedResultUrl": f"https://docs.google.com/spreadsheets/d/{doc_id}/edit",
                },
                "columns": {
                    "mappingMode": "autoMapInputData",
                    "matchingColumns": ["RecordId"],
                    "attemptToConvertTypes": False,
                    "convertFieldsToString": False,
                },
                "options": {"handlingExtraData": "ignoreIt"},
            },
            "type": "n8n-nodes-base.googleSheets",
            "typeVersion": 4.7,
            "position": [736, 48],
            "id": "8f6676e8-44a1-411b-be2e-f7f34a23ef6e",
            "name": "AG3V2.14 - Upsert Fundamental_Data",
            "credentials": creds,
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {
                "content": "AG3-V2: robust fundamentals pipeline (API-first) for scoring + consensus + raw metrics.",
                "height": 160,
                "width": 980,
                "color": 5,
            },
            "type": "n8n-nodes-base.stickyNote",
            "typeVersion": 1,
            "position": [-1568, -352],
            "id": "0f0ec6f2-6d54-43ea-b929-ce0f8d6f0050",
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
            "main": [[{"node": "AG3V2.03 - Split Symbols", "type": "main", "index": 0}]]
        },
        "AG3V2.03 - Split Symbols": {
            "main": [[
                {"node": "AG3V2.04 - HTTP Fundamentals", "type": "main", "index": 0},
                {"node": "AG3V2.05 - Merge Queue + API", "type": "main", "index": 1},
            ]]
        },
        "AG3V2.04 - HTTP Fundamentals": {
            "main": [[{"node": "AG3V2.05 - Merge Queue + API", "type": "main", "index": 0}]]
        },
        "AG3V2.05 - Merge Queue + API": {
            "main": [[{"node": "AG3V2.06 - Score Fundamentals", "type": "main", "index": 0}]]
        },
        "AG3V2.06 - Score Fundamentals": {
            "main": [[
                {"node": "AG3V2.07 - Prepare Triage Row", "type": "main", "index": 0},
                {"node": "AG3V2.10 - Prepare Consensus Row", "type": "main", "index": 0},
                {"node": "AG3V2.12 - Prepare Metric Rows", "type": "main", "index": 0},
            ]]
        },
        "AG3V2.07 - Prepare Triage Row": {
            "main": [[{"node": "AG3V2.08 - Upsert AG3_Triage_History", "type": "main", "index": 0}]]
        },
        "AG3V2.08 - Upsert AG3_Triage_History": {
            "main": [[{"node": "AG3V2.09 - Wait Rate Limit", "type": "main", "index": 0}]]
        },
        "AG3V2.09 - Wait Rate Limit": {
            "main": [[{"node": "AG3V2.03 - Split Symbols", "type": "main", "index": 0}]]
        },
        "AG3V2.10 - Prepare Consensus Row": {
            "main": [[{"node": "AG3V2.11 - Upsert research_analyst_consensus", "type": "main", "index": 0}]]
        },
        "AG3V2.12 - Prepare Metric Rows": {
            "main": [[{"node": "AG3V2.13 - Split Metric Rows", "type": "main", "index": 0}]]
        },
        "AG3V2.13 - Split Metric Rows": {
            "main": [[{"node": "AG3V2.14 - Upsert Fundamental_Data", "type": "main", "index": 0}]]
        },
    }

    return {
        "name": "AG3-V2 - Fundamental Analyst (API-first)",
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
