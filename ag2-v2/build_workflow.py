#!/usr/bin/env python3
"""
AG2-V2 workflow generator.

Variants:
- current: mirrors the workflow currently running on n8n (vector nodes present but not wired).
- vector-wired: proposed wiring for vectorization branch (AI + cache paths feed Qdrant pipeline).

Usage:
  python build_workflow.py > AG2-V2-workflow.json
  python build_workflow.py --variant vector-wired > AG2-V2-workflow.vector-wired-proposed.json
  python build_workflow.py --write-files
"""

from __future__ import annotations

import argparse
import json
import os
from copy import deepcopy
from pathlib import Path

DIR = Path(__file__).resolve().parent
NODES_DIR = DIR / "nodes"


def load_code(filename: str) -> str:
    return (NODES_DIR / filename).read_text(encoding="utf-8")


def _find_node_by_name(workflow: dict, name: str) -> dict | None:
    for node in workflow.get("nodes", []):
        if node.get("name") == name:
            return node
    return None


def load_write_node_template() -> dict:
    """Reuse the large Google Sheets mapping/schema from an existing JSON export."""
    candidates = [
        DIR / "AG2-V2-workflow.json",
        DIR.parent / "AG2 - étape 1 - construction des indicateurs Technical Analyst.json",
    ]

    for path in candidates:
        if not path.exists():
            continue
        try:
            wf = json.loads(path.read_text(encoding="utf-8"))
            node = _find_node_by_name(wf, "Write AG2 Sortie")
            if node:
                return {
                    "parameters": deepcopy(node.get("parameters", {})),
                    "credentials": deepcopy(node.get("credentials", {})),
                }
        except Exception:
            continue

    # Minimal fallback (should not happen in this repo)
    return {
        "parameters": {
            "operation": "appendOrUpdate",
            "documentId": {
                "__rl": True,
                "mode": "list",
                "value": "1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I",
            },
            "sheetName": {"__rl": True, "mode": "list", "value": 1444959091},
            "columns": {"mappingMode": "autoMapInputData"},
            "options": {},
        },
        "credentials": {
            "googleSheetsOAuth2Api": {
                "id": "aX5iAQEN9HK4UGjr",
                "name": "Google Sheets account",
            }
        },
    }


def base_nodes() -> list[dict]:
    write_tpl = load_write_node_template()

    ai_system = (
        "Tu es l'Agent #2 (Technical Analyst Validator): tu valides un signal H1 à la lumière du régime D1.\n\n"
        "RÈGLES (ZÉRO HALLUCINATION)\n"
        "- Tu n'utilises QUE les champs du JSON reçu (ai_context).\n"
        "- Si une donnée obligatoire est null/absente => ajoute-la à missing_fields et décide REJECT, validated=false, quality_score<=3.\n"
        "- Tu ne dois JAMAIS écrire \"Prix > SMA200\" si d1.price ou d1.sma200 est null.\n\n"
        "DONNÉES OBLIGATOIRES\n"
        "- d1.price, d1.sma200 (pour bias)\n"
        "- h1.action, h1.score\n"
        "- h1.bars_count\n\n"
        "LOGIQUE\n"
        "1) bias_sma200:\n"
        "   - BULLISH si d1.price > d1.sma200\n"
        "   - BEARISH sinon\n"
        "2) régime D1 = d1.regime_d1 (déjà calculé dans le contexte, tu le recopies et tu le confirmes)\n"
        "3) Alignement:\n"
        "   - WITH_BIAS si (BUY & bias=BULLISH) ou (SELL & bias=BEARISH)\n"
        "   - AGAINST_BIAS sinon (ou MIXED si score H1 faible)\n"
        "4) RR gate (BUY uniquement):\n"
        "   - si rr_theoretical est null: WATCH si WITH_BIAS, sinon REJECT\n"
        "   - rr < 1.2 => REJECT\n"
        "   - 1.2 <= rr < 1.5 => WATCH\n"
        "   - rr >= 1.5 => APPROVE (uniquement si stop_loss_suggestion non null)\n"
        "5) STOP:\n"
        "   - Utilise stop_loss_suggested tel quel (ne l’invente pas). Si null => validated=false.\n"
        "6) validated=true uniquement si decision=APPROVE et stop_loss_suggestion non null.\n\n"
        "REASONING (max 2 phrases)\n"
        "- Doit citer des valeurs: d1.price, d1.sma200, rr_theoretical (si non null), h1.bars_count.\n"
    )

    ai_user = (
        "=Voici le contexte JSON (ai_context). Applique les règles et réponds UNIQUEMENT avec le JSON conforme au schéma.\n\n"
        "DATA:\n"
        "{{ JSON.stringify($json.ai_context, null, 2) }}\n"
    )

    ai_schema = (
        "{\n"
        "  \"type\":\"object\",\n"
        "  \"additionalProperties\":false,\n"
        "  \"properties\":{\n"
        "    \"validated\":{\"type\":\"boolean\"},\n"
        "    \"decision\":{\"type\":\"string\",\"enum\":[\"APPROVE\",\"REJECT\",\"WATCH\"]},\n"
        "    \"quality_score\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":10},\n"
        "    \"bias_sma200\":{\"type\":\"string\",\"enum\":[\"BULLISH\",\"BEARISH\",\"UNKNOWN\"]},\n"
        "    \"regime_d1\":{\"type\":\"string\",\"enum\":[\"BULLISH\",\"BEARISH\",\"NEUTRAL_RANGE\",\"TRANSITION\",\"UNKNOWN\"]},\n"
        "    \"h1_d1_alignment\":{\"type\":\"string\",\"enum\":[\"WITH_BIAS\",\"AGAINST_BIAS\",\"MIXED\",\"UNKNOWN\"]},\n"
        "    \"reasoning\":{\"type\":\"string\"},\n"
        "    \"chart_pattern\":{\"type\":\"string\"},\n"
        "    \"stop_loss_suggestion\":{\"type\":[\"number\",\"null\"]},\n"
        "    \"stop_loss_basis\":{\"type\":\"string\"},\n"
        "    \"missing_fields\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\n"
        "    \"anomalies\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}\n"
        "  },\n"
        "  \"required\":[\"validated\",\"decision\",\"quality_score\",\"reasoning\",\"bias_sma200\",\"regime_d1\",\"h1_d1_alignment\",\"chart_pattern\",\"stop_loss_suggestion\",\"stop_loss_basis\",\"missing_fields\",\"anomalies\"]\n"
        "}\n"
    )

    nodes = [
        {
            "parameters": {
                "rule": {
                    "interval": [
                        {"field": "cronExpression", "expression": "10 9-17 * * 1-5"}
                    ]
                }
            },
            "type": "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.2,
            "position": [-976, 112],
            "id": "bac6ff10-58a0-4d3c-8b84-7bfe9fecfe50",
            "name": "Cron Trigger",
        },
        {
            "parameters": {},
            "type": "n8n-nodes-base.manualTrigger",
            "typeVersion": 1,
            "position": [-976, 304],
            "id": "bb1d0b17-20cb-4c8a-a7a9-56a82b0deaca",
            "name": "Manual Trigger",
        },
        {
            "parameters": {
                "documentId": {
                    "__rl": True,
                    "mode": "list",
                    "value": "1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I",
                    "cachedResultName": "TradingSim_GoogleSheet_Template",
                    "cachedResultUrl": "https://docs.google.com/spreadsheets/d/1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I/edit?usp=drivesdk",
                },
                "sheetName": {
                    "__rl": True,
                    "mode": "list",
                    "value": 1078848687,
                    "cachedResultName": "Universe",
                    "cachedResultUrl": "https://docs.google.com/spreadsheets/d/1l3fmopgQ8jVd__UTyIja3-nQkn7Jaxm19lcC32HQq8I/edit#gid=1078848687",
                },
                "filtersUI": {"values": [{"lookupColumn": "Enabled", "lookupValue": "true"}]},
                "options": {},
            },
            "type": "n8n-nodes-base.googleSheets",
            "typeVersion": 4.5,
            "position": [-800, 208],
            "id": "cf822dc5-78ee-4b92-8ea6-6a2d46c5a7e5",
            "name": "Read Universe",
            "credentials": {
                "googleSheetsOAuth2Api": {
                    "id": "aX5iAQEN9HK4UGjr",
                    "name": "Google Sheets account",
                }
            },
        },
        {
            "parameters": {"jsCode": load_code("01_init_config.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-624, 208],
            "id": "ce685468-3312-44bd-9627-45a359bafe61",
            "name": "Init Config + Batch",
        },
        {
            "parameters": {
                "language": "pythonNative",
                "pythonCode": load_code("02_duckdb_init.py"),
            },
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-448, 208],
            "id": "008d51c9-2c06-4dc7-90bf-eca210725324",
            "name": "DuckDB Init Schema",
        },
        {
            "parameters": {"options": {}},
            "type": "n8n-nodes-base.splitInBatches",
            "typeVersion": 3,
            "position": [-256, 208],
            "id": "325149eb-467a-45a6-8c8f-9f7c5b503776",
            "name": "Loop Symbols",
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {
                "language": "pythonNative",
                "pythonCode": load_code("10_finalize.py"),
            },
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-32, -256],
            "id": "fb1f1f68-0403-4d69-b5ec-2c0f8318db76",
            "name": "Finalize Run",
        },
        {
            "parameters": {
                "url": "={{$json.yfinance_api_base}}/history",
                "sendQuery": True,
                "queryParameters": {
                    "parameters": [
                        {"name": "symbol", "value": "={{$json.symbol}}"},
                        {"name": "interval", "value": "={{ $json.intraday.interval }}"},
                        {"name": "lookback_days", "value": "={{ $json.intraday.lookback_days }}"},
                        {"name": "max_bars", "value": "={{ $json.intraday.max_bars }}"},
                        {"name": "min_bars", "value": "={{ $json.intraday.min_bars }}"},
                        {"name": "allow_stale", "value": "false"},
                    ]
                },
                "options": {
                    "response": {"response": {"responseFormat": "json"}},
                    "timeout": 60000,
                },
            },
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.1,
            "position": [96, 160],
            "id": "e46c0ed3-ec8d-493c-ae67-77e74fb7954d",
            "name": "AG2.10 — HTTP — Fetch Yahoo OHLCV (1H Timing)",
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {
                "url": "={{$json.yfinance_api_base}}/history",
                "sendQuery": True,
                "queryParameters": {
                    "parameters": [
                        {"name": "symbol", "value": "={{$json.symbol}}"},
                        {"name": "interval", "value": "={{ $json.daily.interval }}"},
                        {"name": "lookback_days", "value": "={{ $json.daily.lookback_days }}"},
                        {"name": "max_bars", "value": "={{ $json.daily.max_bars }}"},
                        {"name": "min_bars", "value": "={{ $json.daily.min_bars }}"},
                        {"name": "allow_stale", "value": "false"},
                    ]
                },
                "options": {
                    "response": {"response": {"responseFormat": "json"}},
                    "timeout": 60000,
                },
            },
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.1,
            "position": [96, 336],
            "id": "3ddd3dbe-b741-4239-b13e-48f1866d0e03",
            "name": "AG2.15 — HTTP — Fetch Yahoo OHLCV (1D Strategy)",
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {"jsCode": load_code("03a_wrap_h1.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [320, 160],
            "id": "7b2259fd-0152-4c27-bf4d-711fb818bd7b",
            "name": "AG2.11 — Code — Wrap H1",
        },
        {
            "parameters": {"jsCode": load_code("03b_wrap_d1.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [320, 336],
            "id": "7a0e7adf-6c2a-46ad-b82d-56b3590ad8f8",
            "name": "AG2.16 — Code — Wrap D1",
        },
        {
            "parameters": {"mode": "combine", "combineBy": "combineByPosition", "options": {}},
            "type": "n8n-nodes-base.merge",
            "typeVersion": 3.2,
            "position": [560, 240],
            "id": "36ad40e4-1be2-4550-bccd-d958d2324823",
            "name": "Merge",
        },
        {
            "parameters": {
                "language": "pythonNative",
                "pythonCode": load_code("04_compute.py"),
            },
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [752, 240],
            "id": "d2b65f20-90d8-482e-ab9a-507d756e6931",
            "name": "Compute + Filter + Write",
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {
                "conditions": {
                    "options": {
                        "caseSensitive": True,
                        "leftValue": "",
                        "typeValidation": "strict",
                        "version": 2,
                    },
                    "conditions": [
                        {
                            "id": "1",
                            "operator": {
                                "type": "boolean",
                                "operation": "true",
                                "singleValue": True,
                            },
                            "leftValue": "={{ $json.call_ai.toString().trim().toBoolean() }}",
                            "rightValue": "true",
                        }
                    ],
                    "combinator": "and",
                },
                "options": {},
            },
            "type": "n8n-nodes-base.if",
            "typeVersion": 2.2,
            "position": [976, 320],
            "id": "536bcdab-bdfe-4cf7-a609-336d9311969a",
            "name": "IF Call AI?",
            "alwaysOutputData": False,
        },
        {
            "parameters": {"jsCode": load_code("05_snapshot.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1184, 208],
            "id": "5b209ba4-3895-4131-9a16-ecc1b872b8fb",
            "name": "Snapshot Context",
        },
        {
            "parameters": {
                "modelId": {"__rl": True, "value": "gpt-5-mini", "mode": "list", "cachedResultName": "GPT-5-MINI"},
                "responses": {
                    "values": [
                        {"role": "system", "content": ai_system},
                        {"content": ai_user},
                    ]
                },
                "builtInTools": {},
                "options": {
                    "textFormat": {
                        "textOptions": {
                            "type": "json_schema",
                            "schema": ai_schema,
                            "strict": True,
                        }
                    }
                },
            },
            "type": "@n8n/n8n-nodes-langchain.openAi",
            "typeVersion": 2.1,
            "position": [1408, 208],
            "id": "1893a4e2-30d8-41d6-8192-aab2ea965fb8",
            "name": "AI Validation GPT",
            "credentials": {"openAiApi": {"id": "rILpYjTayqc4jXXZ", "name": "OpenAi account"}},
        },
        {
            "parameters": {"jsCode": load_code("06a_merge_ai.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1728, 208],
            "id": "93a0368f-5346-4997-9d44-1b9968614790",
            "name": "Merge AI + Context",
        },
        {
            "parameters": {
                "language": "pythonNative",
                "pythonCode": load_code("06_extract_ai.py"),
            },
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1904, 496],
            "id": "c77b61d1-0835-4a4a-a58a-96274e341e6c",
            "name": "Extract AI + Write",
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {"jsCode": load_code("08_prep_vector.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [752, -384],
            "id": "c4e39a7f-d45a-4c88-90cf-e0b23b8320c2",
            "name": "Prep Vector Text",
        },
        {
            "parameters": {"options": {"dimensions": 1536}},
            "type": "@n8n/n8n-nodes-langchain.embeddingsOpenAi",
            "typeVersion": 1.2,
            "position": [912, -208],
            "id": "90773e44-24f7-4d33-8a3c-d9c2774aef2e",
            "name": "Embeddings OpenAI",
            "credentials": {"openAiApi": {"id": "rILpYjTayqc4jXXZ", "name": "OpenAi account"}},
        },
        {
            "parameters": {"options": {}},
            "type": "@n8n/n8n-nodes-langchain.documentDefaultDataLoader",
            "typeVersion": 1,
            "position": [960, -80],
            "id": "9ee4bd1e-0beb-4ef4-b73f-2ec9c5919fe3",
            "name": "Default Data Loader",
        },
        {
            "parameters": {"chunkSize": 10000, "chunkOverlap": 200, "options": {}},
            "type": "@n8n/n8n-nodes-langchain.textSplitterRecursiveCharacterTextSplitter",
            "typeVersion": 1,
            "position": [960, 112],
            "id": "9862bdd3-3ba7-415f-9715-163d385839b3",
            "name": "Text Splitter",
        },
        {
            "parameters": {
                "mode": "insert",
                "qdrantCollection": {"__rl": True, "mode": "list", "value": "financial_tech_v1"},
                "options": {"collectionConfig": {"similarity": "Cosine"}},
            },
            "type": "@n8n/n8n-nodes-langchain.vectorStoreQdrant",
            "typeVersion": 1.1,
            "position": [992, -384],
            "id": "e84062e2-9ec5-4747-a9b3-33d767e190d2",
            "name": "Qdrant Upsert",
            "credentials": {"qdrantApi": {"id": "q1CRmg2N6AmW6pC1", "name": "QdrantApi account"}},
        },
        {
            "parameters": {
                "language": "pythonNative",
                "pythonCode": load_code("09_mark_vector.py"),
            },
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1312, -384],
            "id": "d94a67d1-9440-4c21-9f14-fa097d02652a",
            "name": "Mark Vectorized",
        },
        {
            "parameters": {
                "language": "pythonNative",
                "pythonCode": load_code("11_sync_sheets.py"),
            },
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [192, -256],
            "id": "84b876b4-b35a-4942-a915-bc170ca5553b",
            "name": "Sync DuckDB → Sheets",
            "onError": "continueRegularOutput",
        },
        {
            "parameters": write_tpl["parameters"],
            "type": "n8n-nodes-base.googleSheets",
            "typeVersion": 4.5,
            "position": [448, -256],
            "id": "391a30d3-311c-448c-b34e-3a4ec2f7e8e5",
            "name": "Write AG2 Sortie",
            "credentials": write_tpl["credentials"],
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {
                "language": "pythonNative",
                "pythonCode": load_code("07_hydrate_ai_cache.py"),
            },
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1184, 416],
            "id": "3d1d1a86-6ebe-49a4-b5c7-1701383b00d5",
            "name": "Hydrate AI from cache",
        },
    ]

    return nodes


def current_connections() -> dict:
    return {
        "Cron Trigger": {"main": [[{"node": "Read Universe", "type": "main", "index": 0}]]},
        "Manual Trigger": {"main": [[{"node": "Read Universe", "type": "main", "index": 0}]]},
        "Read Universe": {"main": [[{"node": "Init Config + Batch", "type": "main", "index": 0}]]},
        "Init Config + Batch": {"main": [[{"node": "DuckDB Init Schema", "type": "main", "index": 0}]]},
        "DuckDB Init Schema": {"main": [[{"node": "Loop Symbols", "type": "main", "index": 0}]]},
        "Loop Symbols": {
            "main": [
                [{"node": "Finalize Run", "type": "main", "index": 0}],
                [
                    {"node": "AG2.15 — HTTP — Fetch Yahoo OHLCV (1D Strategy)", "type": "main", "index": 0},
                    {"node": "AG2.10 — HTTP — Fetch Yahoo OHLCV (1H Timing)", "type": "main", "index": 0},
                ],
            ]
        },
        "Finalize Run": {"main": [[{"node": "Sync DuckDB → Sheets", "type": "main", "index": 0}]]},
        "AG2.10 — HTTP — Fetch Yahoo OHLCV (1H Timing)": {
            "main": [[{"node": "AG2.11 — Code — Wrap H1", "type": "main", "index": 0}]]
        },
        "AG2.15 — HTTP — Fetch Yahoo OHLCV (1D Strategy)": {
            "main": [[{"node": "AG2.16 — Code — Wrap D1", "type": "main", "index": 0}]]
        },
        "AG2.11 — Code — Wrap H1": {"main": [[{"node": "Merge", "type": "main", "index": 0}]]},
        "AG2.16 — Code — Wrap D1": {"main": [[{"node": "Merge", "type": "main", "index": 1}]]},
        "Merge": {"main": [[{"node": "Compute + Filter + Write", "type": "main", "index": 0}]]},
        "Compute + Filter + Write": {"main": [[{"node": "IF Call AI?", "type": "main", "index": 0}]]},
        "IF Call AI?": {
            "main": [
                [{"node": "Snapshot Context", "type": "main", "index": 0}],
                [{"node": "Hydrate AI from cache", "type": "main", "index": 0}],
            ]
        },
        "Snapshot Context": {"main": [[{"node": "AI Validation GPT", "type": "main", "index": 0}]]},
        "AI Validation GPT": {"main": [[{"node": "Merge AI + Context", "type": "main", "index": 0}]]},
        "Merge AI + Context": {"main": [[{"node": "Extract AI + Write", "type": "main", "index": 0}]]},
        "Extract AI + Write": {"main": [[{"node": "Loop Symbols", "type": "main", "index": 0}]]},
        "Hydrate AI from cache": {"main": [[{"node": "Loop Symbols", "type": "main", "index": 0}]]},
        "Prep Vector Text": {"main": [[{"node": "Qdrant Upsert", "type": "main", "index": 0}]]},
        "Embeddings OpenAI": {"ai_embedding": [[{"node": "Qdrant Upsert", "type": "ai_embedding", "index": 0}]]},
        "Default Data Loader": {"ai_document": [[{"node": "Qdrant Upsert", "type": "ai_document", "index": 0}]]},
        "Text Splitter": {"ai_textSplitter": [[{"node": "Default Data Loader", "type": "ai_textSplitter", "index": 0}]]},
        "Qdrant Upsert": {"main": [[{"node": "Mark Vectorized", "type": "main", "index": 0}]]},
        "Mark Vectorized": {"main": [[]]},
        "Sync DuckDB → Sheets": {"main": [[{"node": "Write AG2 Sortie", "type": "main", "index": 0}]]},
    }


def vector_wired_connections() -> dict:
    conns = current_connections()

    # add node IF Vectorize?
    conns["Extract AI + Write"] = {"main": [[{"node": "IF Vectorize?", "type": "main", "index": 0}]]}
    conns["Hydrate AI from cache"] = {"main": [[{"node": "IF Vectorize?", "type": "main", "index": 0}]]}
    conns["IF Vectorize?"] = {
        "main": [
            [{"node": "Prep Vector Text", "type": "main", "index": 0}],
            [{"node": "Loop Symbols", "type": "main", "index": 0}],
        ]
    }
    conns["Mark Vectorized"] = {"main": [[{"node": "Loop Symbols", "type": "main", "index": 0}]]}
    return conns


def add_vector_if_node(nodes: list[dict]) -> list[dict]:
    out = deepcopy(nodes)
    out.append(
        {
            "parameters": {
                "conditions": {
                    "options": {
                        "caseSensitive": True,
                        "leftValue": "",
                        "typeValidation": "strict",
                        "version": 2,
                    },
                    "conditions": [
                        {
                            "id": "1",
                            "operator": {
                                "type": "boolean",
                                "operation": "true",
                                "singleValue": True,
                            },
                            "leftValue": "={{ $json.should_vectorize.toString().trim().toBoolean() }}",
                            "rightValue": "true",
                        }
                    ],
                    "combinator": "and",
                },
                "options": {},
            },
            "type": "n8n-nodes-base.if",
            "typeVersion": 2.2,
            "position": [2144, 496],
            "id": "9c0d7bd3-4821-451b-8e68-6db2d11d26f0",
            "name": "IF Vectorize?",
            "alwaysOutputData": False,
        }
    )
    return out


def build_workflow(variant: str) -> dict:
    if variant not in {"current", "vector-wired"}:
        raise ValueError(f"Unsupported variant: {variant}")

    nodes = base_nodes()
    if variant == "vector-wired":
        nodes = add_vector_if_node(nodes)
        connections = vector_wired_connections()
    else:
        connections = current_connections()

    return {
        "nodes": nodes,
        "connections": connections,
        "pinData": {},
        "meta": {
            "instanceId": "093ba1266e85fcaaa6abca411d9f7941951737b3920bfaa6e2db89226df2c82d"
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--variant", choices=["current", "vector-wired"], default="current")
    parser.add_argument("--output", default="-")
    parser.add_argument("--write-files", action="store_true")
    args = parser.parse_args()

    if args.write_files:
        current = build_workflow("current")
        vector = build_workflow("vector-wired")
        (DIR / "AG2-V2-workflow.json").write_text(
            json.dumps(current, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        (DIR / "AG2-V2-workflow.vector-wired-proposed.json").write_text(
            json.dumps(vector, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        print("Wrote AG2-V2-workflow.json and AG2-V2-workflow.vector-wired-proposed.json")
        return

    wf = build_workflow(args.variant)
    payload = json.dumps(wf, indent=2, ensure_ascii=False) + "\n"
    if args.output == "-":
        print(payload, end="")
    else:
        Path(args.output).write_text(payload, encoding="utf-8")


if __name__ == "__main__":
    main()
