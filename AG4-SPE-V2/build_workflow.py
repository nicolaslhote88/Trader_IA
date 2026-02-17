#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

DIR = Path(__file__).resolve().parent
NODES_DIR = DIR / "nodes"


def load_code(filename: str) -> str:
    return (NODES_DIR / filename).read_text(encoding="utf-8")


def build() -> dict:
    openai_creds = {"openAiApi": {"id": "rILpYjTayqc4jXXZ", "name": "OpenAi account"}}

    nodes = [
        {
            "parameters": {
                "rule": {"interval": [{"field": "cronExpression", "expression": "0 5 9,12,15 * * 1-5"}]}
            },
            "type": "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.3,
            "position": [-1952, -192],
            "id": "f8a0f0d6-faf7-4c9f-bca1-d7212bd2bcd1",
            "name": "Schedule Trigger",
        },
        {
            "parameters": {},
            "type": "n8n-nodes-base.manualTrigger",
            "typeVersion": 1,
            "position": [-1952, -16],
            "id": "669d6295-6968-4436-b5c7-3f5f6d1b5dce",
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
                "options": {},
            },
            "type": "n8n-nodes-base.googleSheets",
            "typeVersion": 4.7,
            "position": [-1728, -96],
            "id": "0bd3f148-cff3-4dbe-b9b2-9ea242909b1a",
            "name": "S00A - Load Universe (Google Sheets)",
            "credentials": {
                "googleSheetsOAuth2Api": {"id": "aX5iAQEN9HK4UGjr", "name": "Google Sheets account"}
            },
        },
        {
            "parameters": {"jsCode": load_code("01_build_symbol_queue.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-1504, -96],
            "id": "3e1f6147-0f0f-4256-aab3-48af94afcc51",
            "name": "S01 - Build Symbol Queue",
        },
        {
            "parameters": {"language": "pythonNative", "pythonCode": load_code("00_duckdb_prepare.py")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-1280, -96],
            "id": "be2cf27b-c104-4d57-8c06-4fec0129e4be",
            "name": "S00B - DuckDB Init Schema",
        },
        {
            "parameters": {"language": "pythonNative", "pythonCode": load_code("02_start_run.py")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-1072, -96],
            "id": "34c2a416-4c86-4f03-a94e-df5ae6a3f91d",
            "name": "S02 - Start Run",
        },
        {
            "parameters": {"options": {"reset": False}},
            "type": "n8n-nodes-base.splitInBatches",
            "typeVersion": 3,
            "position": [-864, -96],
            "id": "73358bb3-2807-42f2-a72e-98954b35dfdd",
            "name": "S03 - Split Symbols",
        },
        {
            "parameters": {
                "url": "={{$json.actualitesUrl}}",
                "sendHeaders": True,
                "headerParameters": {
                    "parameters": [
                        {
                            "name": "User-Agent",
                            "value": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                        },
                        {"name": "Accept", "value": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
                        {"name": "Accept-Language", "value": "fr-FR,fr;q=0.9,en;q=0.8"},
                        {"name": "Cache-Control", "value": "no-cache"},
                    ]
                },
                "options": {
                    "response": {
                        "response": {
                            "fullResponse": True,
                            "responseFormat": "text",
                            "outputPropertyName": "listingHtml",
                        }
                    }
                },
            },
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.3,
            "position": [-848, 16],
            "id": "3d6f7c6a-6a3b-4c0f-a53d-31b7d2da53cc",
            "name": "S04 - HTTP Listing Page",
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {"mode": "combine", "combineBy": "combineByPosition", "options": {}},
            "type": "n8n-nodes-base.merge",
            "typeVersion": 3.2,
            "position": [-640, -32],
            "id": "34ec95df-6b2c-4752-b1c0-a73de9f6a428",
            "name": "S04M - Merge Symbol + Listing",
        },
        {
            "parameters": {
                "conditions": {
                    "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 3},
                    "conditions": [
                        {
                            "id": "status200",
                            "leftValue": "={{ $json.statusCode }}",
                            "rightValue": 200,
                            "operator": {"type": "number", "operation": "equals"},
                        }
                    ],
                    "combinator": "and",
                },
                "options": {},
            },
            "type": "n8n-nodes-base.if",
            "typeVersion": 2.3,
            "position": [-432, -32],
            "id": "8938cc57-42fb-4ddc-a2ee-bfe2c42986b4",
            "name": "S05 - IF Listing 200?",
        },
        {
            "parameters": {"jsCode": load_code("11_build_error_row.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-224, 224],
            "id": "6bc3533d-96df-4f17-a8ec-0076dcd325ec",
            "name": "S05E - Build Listing Error Row",
        },
        {
            "parameters": {"language": "pythonNative", "pythonCode": load_code("13_write_errors_duckdb.py")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-16, 224],
            "id": "f0db3665-fda8-4322-9ed3-ec08dcff9c40",
            "name": "S23L - Write Listing Errors DuckDB",
        },
        {
            "parameters": {"jsCode": load_code("03_extract_listing_articles.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-224, -32],
            "id": "be1590be-2fef-4f41-9f26-ad70c58d11f2",
            "name": "S06 - Extract Articles",
        },
        {
            "parameters": {"jsCode": load_code("04_normalize_articles.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-16, -32],
            "id": "c10fbcf2-3965-4e31-89d8-cb651ad54baf",
            "name": "S07 - Normalize + Dedupe + Limit",
        },
        {
            "parameters": {
                "conditions": {
                    "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 3},
                    "conditions": [
                        {
                            "id": "hasArticles",
                            "leftValue": "={{ $json.hasArticles }}",
                            "rightValue": True,
                            "operator": {"type": "boolean", "operation": "equals"},
                        }
                    ],
                    "combinator": "and",
                },
                "options": {},
            },
            "type": "n8n-nodes-base.if",
            "typeVersion": 2.3,
            "position": [192, -32],
            "id": "6f3a0de4-769f-4cd8-8f7c-92db8e0f1490",
            "name": "S08 - IF Has Articles?",
        },
        {
            "parameters": {"jsCode": load_code("05_explode_articles.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [400, -32],
            "id": "2b36f448-01dc-4359-b81f-47ef7ef973ef",
            "name": "S09 - Explode Articles",
        },
        {
            "parameters": {"options": {"reset": "={{ $json._articlesLoopReset }}"}},
            "type": "n8n-nodes-base.splitInBatches",
            "typeVersion": 3,
            "position": [608, -32],
            "id": "db2d3cd7-1fb0-4765-bd9f-3f56466de7d6",
            "name": "S10 - Split Articles",
        },
        {
            "parameters": {"language": "pythonNative", "pythonCode": load_code("06_route_new_seen.py")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [816, -32],
            "id": "ed654f58-ed66-473f-990e-27169f86ff96",
            "name": "S11 - Route New vs Seen",
        },
        {
            "parameters": {
                "rules": {
                    "values": [
                        {
                            "conditions": {
                                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 3},
                                "conditions": [
                                    {
                                        "id": "analyze",
                                        "leftValue": "={{ $json._action }}",
                                        "rightValue": "analyze",
                                        "operator": {"type": "string", "operation": "equals"},
                                    }
                                ],
                                "combinator": "and",
                            }
                        },
                        {
                            "conditions": {
                                "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 3},
                                "conditions": [
                                    {
                                        "id": "skip",
                                        "leftValue": "={{ $json._action }}",
                                        "rightValue": "skip",
                                        "operator": {"type": "string", "operation": "equals"},
                                    }
                                ],
                                "combinator": "and",
                            }
                        },
                    ]
                },
                "options": {},
            },
            "type": "n8n-nodes-base.switch",
            "typeVersion": 3.4,
            "position": [1024, -32],
            "id": "0be2ec6f-f5ca-428e-9cd6-c9976ca2dd73",
            "name": "S12 - Router Analyze vs Skip",
        },
        {
            "parameters": {"amount": "={{ Math.floor(Math.random() * (5 - 2 + 1)) + 2 }}"},
            "type": "n8n-nodes-base.wait",
            "typeVersion": 1.1,
            "position": [1232, -128],
            "id": "869fdc69-b2b3-4b3a-abf6-b0b4f3ed9f52",
            "name": "S13 - Wait Article",
            "webhookId": "88cbf6c7-7dcf-4da3-b0ca-5f307ce24395",
        },
        {
            "parameters": {
                "url": "={{$json.articleUrl}}",
                "sendHeaders": True,
                "headerParameters": {
                    "parameters": [
                        {
                            "name": "User-Agent",
                            "value": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                        },
                        {"name": "Accept", "value": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
                        {"name": "Accept-Language", "value": "fr-FR,fr;q=0.9,en;q=0.8"},
                        {"name": "Cache-Control", "value": "no-cache"},
                    ]
                },
                "options": {
                    "response": {
                        "response": {
                            "fullResponse": True,
                            "responseFormat": "text",
                            "outputPropertyName": "articleHtml",
                        }
                    }
                },
            },
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4.3,
            "position": [1440, -128],
            "id": "0ca3f374-fd86-4533-a65e-8d7784ec4d93",
            "name": "S14 - HTTP Article Page",
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {"mode": "combine", "combineBy": "combineByPosition", "options": {}},
            "type": "n8n-nodes-base.merge",
            "typeVersion": 3.2,
            "position": [1648, -176],
            "id": "4d99e026-e09a-4d5e-a9d6-05018ccce237",
            "name": "S14M - Merge Article + Response",
        },
        {
            "parameters": {
                "conditions": {
                    "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 3},
                    "conditions": [
                        {
                            "id": "status200",
                            "leftValue": "={{ $json.statusCode }}",
                            "rightValue": 200,
                            "operator": {"type": "number", "operation": "equals"},
                        }
                    ],
                    "combinator": "and",
                },
                "options": {},
            },
            "type": "n8n-nodes-base.if",
            "typeVersion": 2.3,
            "position": [1856, -176],
            "id": "c8c7a52c-a8d3-4a34-9f88-c00be48a3ff0",
            "name": "S15 - IF Article 200?",
        },
        {
            "parameters": {"jsCode": load_code("11_build_error_row.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [2064, -16],
            "id": "6bfefea8-7ce9-4247-8e14-f45bf2ce92ec",
            "name": "S15E - Build Article Error Row",
        },
        {
            "parameters": {"language": "pythonNative", "pythonCode": load_code("13_write_errors_duckdb.py")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [2272, -16],
            "id": "af8f0f20-07ba-4f58-ac14-efcad758f5a7",
            "name": "S23A - Write Article Errors DuckDB",
        },
        {
            "parameters": {"jsCode": load_code("07_parse_article.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [2064, -176],
            "id": "30f10254-1e03-4448-938d-0b6f0368fdd8",
            "name": "S16 - Parse Article",
        },
        {
            "parameters": {"jsCode": load_code("08_prepare_llm_input.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [2272, -176],
            "id": "2f75a97b-a6cd-43ff-9f45-042b4d11f4c6",
            "name": "S17 - Prepare LLM Input",
        },
        {
            "parameters": {
                "conditions": {
                    "options": {"caseSensitive": True, "leftValue": "", "typeValidation": "strict", "version": 3},
                    "conditions": [
                        {
                            "id": "runai",
                            "leftValue": "={{ $json._runAI }}",
                            "rightValue": True,
                            "operator": {"type": "boolean", "operation": "equals"},
                        }
                    ],
                    "combinator": "and",
                },
                "options": {},
            },
            "type": "n8n-nodes-base.if",
            "typeVersion": 2.3,
            "position": [2480, -176],
            "id": "f385ab13-b63f-44bb-a8eb-c9ca0ec21280",
            "name": "S18 - IF Run AI?",
        },
        {
            "parameters": {
                "modelId": {"__rl": True, "mode": "list", "value": "gpt-5-mini", "cachedResultName": "GPT-5-MINI"},
                "responses": {
                    "values": [
                        {
                            "role": "system",
                            "content": "Tu es un analyste buy-side. Analyse strictement l'impact d'une news sur l'action cible (symbol/company). Reponds uniquement en JSON valide selon le schema impose.",
                        },
                        {
                            "content": "=Analyse cette news specifique a la valeur cible:\n{{ $json.llmInput }}\n\nSi non pertinente pour la societe cible, mets isRelevant=false et impactScore=0."
                        },
                    ]
                },
                "builtInTools": {},
                "options": {
                    "textFormat": {
                        "textOptions": {
                            "type": "json_schema",
                            "name": "specific_stock_news_v2",
                            "schema": "{\n  \"type\": \"object\",\n  \"additionalProperties\": false,\n  \"properties\": {\n    \"isRelevant\": { \"type\": \"boolean\" },\n    \"relevanceReason\": { \"type\": \"string\" },\n    \"impactScore\": { \"type\": \"integer\", \"minimum\": -10, \"maximum\": 10 },\n    \"sentiment\": { \"type\": \"string\", \"enum\": [\"Bullish\", \"Bearish\", \"Neutral\"] },\n    \"category\": { \"type\": \"string\", \"enum\": [\"Earnings\", \"M&A\", \"Contract/Product\", \"Management\", \"Analyst Rating\", \"Macro/Sector\", \"Legal/Reg\", \"Noise\"] },\n    \"summary\": { \"type\": \"string\" }\n  },\n  \"required\": [\"isRelevant\", \"relevanceReason\", \"impactScore\", \"sentiment\", \"category\", \"summary\"]\n}",
                            "strict": True,
                        }
                    }
                },
            },
            "type": "@n8n/n8n-nodes-langchain.openAi",
            "typeVersion": 2.1,
            "position": [2704, -256],
            "id": "31f66369-d4ed-4865-84c4-e78944f4f7d9",
            "name": "S19 - Analyze with OpenAI",
            "credentials": openai_creds,
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {"mode": "combine", "combineBy": "combineByPosition", "options": {}},
            "type": "n8n-nodes-base.merge",
            "typeVersion": 3.2,
            "position": [2928, -176],
            "id": "3f6a478c-f4ef-4f8d-a988-257d813f6ea2",
            "name": "S19M - Merge AI + Context",
        },
        {
            "parameters": {"jsCode": load_code("09_parse_llm_output.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [3152, -176],
            "id": "783d68f2-1df2-40df-a8cb-b1d7dbb51807",
            "name": "S20 - Parse LLM Output",
        },
        {
            "parameters": {"jsCode": load_code("10_build_skip_row.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [3152, 0],
            "id": "52b64b59-ef74-4015-b0ca-ef631748ccf2",
            "name": "S21 - Build Skip Row",
        },
        {
            "parameters": {"language": "pythonNative", "pythonCode": load_code("12_write_news_duckdb.py")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [3376, -80],
            "id": "596f94d7-5f40-40a4-b34f-a718f13cd994",
            "name": "S22 - Upsert News DuckDB",
        },
        {
            "parameters": {"language": "pythonNative", "pythonCode": load_code("14_finalize_run.py")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-848, -256],
            "id": "0a5ecf81-f16e-49e1-aeb2-12096f68e77d",
            "name": "S24 - Finalize Run",
        },
        {
            "parameters": {
                "content": "AG4_Spe-V2: Universe from Google Sheets, specific stock news from Boursorama, dedupe + analysis + storage in DuckDB.",
                "height": 180,
                "width": 1680,
                "color": 5,
            },
            "type": "n8n-nodes-base.stickyNote",
            "typeVersion": 1,
            "position": [-1940, -352],
            "id": "85dd69e0-12f0-4b3a-a7f3-2fbd52a0f67d",
            "name": "Note AG4_Spe-V2",
        },
    ]

    connections = {
        "Schedule Trigger": {"main": [[{"node": "S00A - Load Universe (Google Sheets)", "type": "main", "index": 0}]]},
        "Manual Trigger": {"main": [[{"node": "S00A - Load Universe (Google Sheets)", "type": "main", "index": 0}]]},
        "S00A - Load Universe (Google Sheets)": {"main": [[{"node": "S01 - Build Symbol Queue", "type": "main", "index": 0}]]},
        "S01 - Build Symbol Queue": {"main": [[{"node": "S00B - DuckDB Init Schema", "type": "main", "index": 0}]]},
        "S00B - DuckDB Init Schema": {"main": [[{"node": "S02 - Start Run", "type": "main", "index": 0}]]},
        "S02 - Start Run": {"main": [[{"node": "S03 - Split Symbols", "type": "main", "index": 0}]]},
        "S03 - Split Symbols": {
            "main": [
                [{"node": "S24 - Finalize Run", "type": "main", "index": 0}],
                [
                    {"node": "S04 - HTTP Listing Page", "type": "main", "index": 0},
                    {"node": "S04M - Merge Symbol + Listing", "type": "main", "index": 1},
                ],
            ]
        },
        "S04 - HTTP Listing Page": {
            "main": [[{"node": "S04M - Merge Symbol + Listing", "type": "main", "index": 0}]]
        },
        "S04M - Merge Symbol + Listing": {"main": [[{"node": "S05 - IF Listing 200?", "type": "main", "index": 0}]]},
        "S05 - IF Listing 200?": {
            "main": [
                [{"node": "S06 - Extract Articles", "type": "main", "index": 0}],
                [{"node": "S05E - Build Listing Error Row", "type": "main", "index": 0}],
            ]
        },
        "S05E - Build Listing Error Row": {"main": [[{"node": "S23L - Write Listing Errors DuckDB", "type": "main", "index": 0}]]},
        "S23L - Write Listing Errors DuckDB": {"main": [[{"node": "S03 - Split Symbols", "type": "main", "index": 0}]]},
        "S06 - Extract Articles": {"main": [[{"node": "S07 - Normalize + Dedupe + Limit", "type": "main", "index": 0}]]},
        "S07 - Normalize + Dedupe + Limit": {"main": [[{"node": "S08 - IF Has Articles?", "type": "main", "index": 0}]]},
        "S08 - IF Has Articles?": {
            "main": [
                [{"node": "S09 - Explode Articles", "type": "main", "index": 0}],
                [{"node": "S03 - Split Symbols", "type": "main", "index": 0}],
            ]
        },
        "S09 - Explode Articles": {"main": [[{"node": "S10 - Split Articles", "type": "main", "index": 0}]]},
        "S10 - Split Articles": {
            "main": [
                [{"node": "S03 - Split Symbols", "type": "main", "index": 0}],
                [{"node": "S11 - Route New vs Seen", "type": "main", "index": 0}],
            ]
        },
        "S11 - Route New vs Seen": {"main": [[{"node": "S12 - Router Analyze vs Skip", "type": "main", "index": 0}]]},
        "S12 - Router Analyze vs Skip": {
            "main": [
                [{"node": "S13 - Wait Article", "type": "main", "index": 0}],
                [{"node": "S21 - Build Skip Row", "type": "main", "index": 0}],
            ]
        },
        "S13 - Wait Article": {
            "main": [
                [
                    {"node": "S14 - HTTP Article Page", "type": "main", "index": 0},
                    {"node": "S14M - Merge Article + Response", "type": "main", "index": 1},
                ]
            ]
        },
        "S14 - HTTP Article Page": {
            "main": [[{"node": "S14M - Merge Article + Response", "type": "main", "index": 0}]]
        },
        "S14M - Merge Article + Response": {"main": [[{"node": "S15 - IF Article 200?", "type": "main", "index": 0}]]},
        "S15 - IF Article 200?": {
            "main": [
                [{"node": "S16 - Parse Article", "type": "main", "index": 0}],
                [
                    {"node": "S15E - Build Article Error Row", "type": "main", "index": 0},
                    {"node": "S21 - Build Skip Row", "type": "main", "index": 0},
                ],
            ]
        },
        "S15E - Build Article Error Row": {"main": [[{"node": "S23A - Write Article Errors DuckDB", "type": "main", "index": 0}]]},
        "S16 - Parse Article": {"main": [[{"node": "S17 - Prepare LLM Input", "type": "main", "index": 0}]]},
        "S17 - Prepare LLM Input": {"main": [[{"node": "S18 - IF Run AI?", "type": "main", "index": 0}]]},
        "S18 - IF Run AI?": {
            "main": [
                [
                    {"node": "S19 - Analyze with OpenAI", "type": "main", "index": 0},
                    {"node": "S19M - Merge AI + Context", "type": "main", "index": 0},
                ],
                [{"node": "S21 - Build Skip Row", "type": "main", "index": 0}],
            ]
        },
        "S19 - Analyze with OpenAI": {
            "main": [[{"node": "S19M - Merge AI + Context", "type": "main", "index": 1}]]
        },
        "S19M - Merge AI + Context": {"main": [[{"node": "S20 - Parse LLM Output", "type": "main", "index": 0}]]},
        "S20 - Parse LLM Output": {"main": [[{"node": "S22 - Upsert News DuckDB", "type": "main", "index": 0}]]},
        "S21 - Build Skip Row": {"main": [[{"node": "S22 - Upsert News DuckDB", "type": "main", "index": 0}]]},
        "S22 - Upsert News DuckDB": {"main": [[{"node": "S10 - Split Articles", "type": "main", "index": 0}]]},
    }

    return {
        "name": "AG4_Spé-V2",
        "nodes": nodes,
        "connections": connections,
        "pinData": {},
        "meta": {"instanceId": "093ba1266e85fcaaa6abca411d9f7941951737b3920bfaa6e2db89226df2c82d"},
    }


def main() -> None:
    wf = build()
    out = DIR / "AG4-SPE-V2-workflow.json"
    out.write_text(json.dumps(wf, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
