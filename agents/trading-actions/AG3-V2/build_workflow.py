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
    sheet_creds = {"googleSheetsOAuth2Api": {"id": "aX5iAQEN9HK4UGjr", "name": "Google Sheets account"}}
    openai_creds = {"openAiApi": {"id": "rILpYjTayqc4jXXZ", "name": "OpenAi account"}}
    qdrant_creds = {"qdrantApi": {"id": "q1CRmg2N6AmW6pC1", "name": "QdrantApi account"}}

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
            "credentials": sheet_creds,
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
            "parameters": {"language": "pythonNative", "pythonCode": load_code("09_build_vector_docs.py")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [128, 64],
            "id": "5e6f56d1-1210-4ab7-9fec-f3aa5ec2f0e6",
            "name": "AG3V2.10 - Build Vector Docs from DuckDB",
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {"options": {"dimensions": 1536}},
            "type": "@n8n/n8n-nodes-langchain.embeddingsOpenAi",
            "typeVersion": 1.2,
            "position": [560, -48],
            "id": "0f4c4e5d-c8c7-41d9-a615-c2bfd2f6f7ba",
            "name": "Embeddings OpenAI",
            "credentials": openai_creds,
        },
        {
            "parameters": {"chunkSize": 30000, "chunkOverlap": 200, "options": {}},
            "type": "@n8n/n8n-nodes-langchain.textSplitterRecursiveCharacterTextSplitter",
            "typeVersion": 1,
            "position": [560, 224],
            "id": "84fe2d6f-40f9-4b25-ab4e-3073958e359f",
            "name": "Text Splitter",
        },
        {
            "parameters": {
                "jsonMode": "expressionData",
                "jsonData": "={{ $json.text }}\n",
                "options": {
                    "metadata": {
                        "metadataValues": [
                            {"name": "id", "value": "={{ $json.metadata.id }}"},
                            {"name": "record_id", "value": "={{ $json.metadata.record_id }}"},
                            {"name": "symbol", "value": "={{ $json.metadata.symbol }}"},
                            {"name": "name", "value": "={{ $json.metadata.name }}"},
                            {"name": "run_id", "value": "={{ $json.metadata.run_id }}"},
                            {"name": "status", "value": "={{ $json.metadata.status }}"},
                            {"name": "horizon", "value": "={{ $json.metadata.horizon }}"},
                            {"name": "score", "value": "={{ $json.metadata.score }}"},
                            {"name": "risk_score", "value": "={{ $json.metadata.risk_score }}"},
                            {"name": "upside_pct", "value": "={{ $json.metadata.upside_pct }}"},
                            {"name": "recommendation", "value": "={{ $json.metadata.recommendation }}"},
                            {"name": "analyst_count", "value": "={{ $json.metadata.analyst_count }}"},
                            {"name": "data_coverage_pct", "value": "={{ $json.metadata.data_coverage_pct }}"},
                            {"name": "as_of_date", "value": "={{ $json.metadata.as_of_date }}"},
                            {"name": "db_path", "value": "={{ $json.metadata.db_path }}"},
                        ]
                    }
                },
            },
            "type": "@n8n/n8n-nodes-langchain.documentDefaultDataLoader",
            "typeVersion": 1,
            "position": [560, 416],
            "id": "cb9b0d2b-9562-4e10-91f8-505eb3b39176",
            "name": "Default Data Loader",
        },
        {
            "parameters": {
                "mode": "insert",
                "qdrantCollection": {"__rl": True, "mode": "list", "value": "fundamental_analysis"},
                "options": {"collectionConfig": {"similarity": "Cosine"}},
            },
            "type": "@n8n/n8n-nodes-langchain.vectorStoreQdrant",
            "typeVersion": 1.1,
            "position": [784, 64],
            "id": "2b96ab39-6361-4763-a677-d1f883533901",
            "name": "Qdrant Upsert",
            "credentials": qdrant_creds,
        },
        {
            "parameters": {"language": "pythonNative", "pythonCode": load_code("10_mark_vectorized.py")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1008, 64],
            "id": "f4f72f66-5833-4ea6-83f6-a4d53e2d068c",
            "name": "AG3V2.11 - Mark Vectorized",
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {
                "content": "AG3-V2 (DuckDB-first): yfinance fundamentals -> scoring -> DuckDB + Qdrant (collection: fundamental_analysis).",
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
                    {"node": "AG3V2.09 - Finalize Run", "type": "main", "index": 0},
                    {"node": "AG3V2.10 - Build Vector Docs from DuckDB", "type": "main", "index": 0},
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
        "AG3V2.10 - Build Vector Docs from DuckDB": {
            "main": [[{"node": "Qdrant Upsert", "type": "main", "index": 0}]]
        },
        "Embeddings OpenAI": {
            "ai_embedding": [[{"node": "Qdrant Upsert", "type": "ai_embedding", "index": 0}]]
        },
        "Text Splitter": {
            "ai_textSplitter": [[{"node": "Default Data Loader", "type": "ai_textSplitter", "index": 0}]]
        },
        "Default Data Loader": {
            "ai_document": [[{"node": "Qdrant Upsert", "type": "ai_document", "index": 0}]]
        },
        "Qdrant Upsert": {
            "main": [[{"node": "AG3V2.11 - Mark Vectorized", "type": "main", "index": 0}]]
        },
        "AG3V2.11 - Mark Vectorized": {
            "main": [[]]
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
