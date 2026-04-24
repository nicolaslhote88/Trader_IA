#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

DIR = Path(__file__).resolve().parent
NODES_DIR = DIR / "nodes"


def load_code(filename: str) -> str:
    return (NODES_DIR / filename).read_text(encoding="utf-8")


SYSTEM_PROMPT = (
    "Tu es le Chief FX Market Strategist. Tu analyses une news pour decision Forex. "
    "Reponds uniquement en JSON valide selon le schema. "
    "Produis les champs existants de regime/theme/devises et les champs geo: "
    "impact_region CSV parmi {Global, US, EU, France, UK, APAC, Emerging, Other}; "
    "impact_asset_class CSV parmi {Equity, FX, Commodity, Bond, Crypto, Mixed, None}; "
    "impact_magnitude parmi {Low, Medium, High}; "
    "impact_fx_pairs CSV au format XXXYYY sans slash. "
    "Si impact_asset_class contient FX ou Mixed, impact_fx_pairs est non vide. "
    "Paires autorisees: EURUSD, GBPUSD, USDJPY, USDCHF, AUDUSD, NZDUSD, USDCAD, "
    "EURGBP, EURJPY, EURCHF, EURAUD, EURCAD, EURNZD, GBPJPY, GBPCHF, GBPAUD, GBPCAD, "
    "AUDJPY, AUDNZD, AUDCAD, NZDJPY, NZDCAD, CADJPY, CHFJPY, CADCHF, CHFCAD, JPYNZD."
)


def schema() -> str:
    return json.dumps(
        {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "isActionable": {"type": "boolean"},
                "market_regime": {"type": "string", "enum": ["Risk-On", "Risk-Off", "Neutral", "Sector Rotation"]},
                "macro_theme": {
                    "type": "string",
                    "enum": [
                        "Inflation/Taux",
                        "Banques Centrales",
                        "Croissance/Recession",
                        "Geopolitique/Energie",
                        "Tech/AI",
                        "Resultats/Micro",
                    ],
                },
                "currencies_bullish": {"type": "array", "maxItems": 5, "items": {"type": "string"}},
                "currencies_bearish": {"type": "array", "maxItems": 5, "items": {"type": "string"}},
                "impact_region": {"type": "string"},
                "impact_asset_class": {"type": "string"},
                "impact_magnitude": {"type": "string", "enum": ["Low", "Medium", "High"]},
                "impact_fx_pairs": {"type": "string"},
                "fx_directional_hint": {"type": "string"},
                "strategic_summary": {"type": "string"},
                "impact_score": {"type": "integer", "minimum": 0, "maximum": 10},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "urgency": {"type": "string", "enum": ["immediate", "today", "this_week", "low"]},
                "notes": {"type": "string"},
            },
            "required": [
                "isActionable",
                "market_regime",
                "macro_theme",
                "currencies_bullish",
                "currencies_bearish",
                "impact_region",
                "impact_asset_class",
                "impact_magnitude",
                "impact_fx_pairs",
                "fx_directional_hint",
                "strategic_summary",
                "impact_score",
                "confidence",
                "urgency",
                "notes",
            ],
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def build() -> dict:
    nodes = [
        {
            "parameters": {"rule": {"interval": [{"field": "cronExpression", "expression": "*/30 7-20 * * 1-5"}]}},
            "type": "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.3,
            "position": [-1120, -120],
            "id": "d9f75407-81fc-4545-96f6-ec9746fd4c9f",
            "name": "Schedule Trigger",
        },
        {
            "parameters": {},
            "type": "n8n-nodes-base.manualTrigger",
            "typeVersion": 1,
            "position": [-1120, 80],
            "id": "7be73ae9-52dd-45b7-b95e-390b3ce47086",
            "name": "Manual Trigger",
        },
        {
            "parameters": {"language": "pythonNative", "pythonCode": load_code("00_load_fx_sources.py")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-896, -16],
            "id": "a2dfc09a-0c23-40f2-9ff6-588850d0399a",
            "name": "20A - Load FX Sources",
        },
        {
            "parameters": {"options": {"reset": False}},
            "type": "n8n-nodes-base.splitInBatches",
            "typeVersion": 3,
            "position": [-672, -16],
            "id": "d835da36-3337-4f89-9d42-f7456526140b",
            "name": "20D - Split FX Feeds",
        },
        {
            "parameters": {"url": "={{$json.url}}", "options": {}},
            "type": "n8n-nodes-base.rssFeedRead",
            "typeVersion": 1.2,
            "position": [-448, 64],
            "id": "43e68189-ae7f-4127-9906-11a8c307c435",
            "name": "20E - RSS Feed Read",
            "retryOnFail": True,
            "maxTries": 2,
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {"jsCode": load_code("01_normalize_fx_rss_items.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-224, 64],
            "id": "0c44f8d5-fe19-40d8-ac47-30662853e8ac",
            "name": "20F - Normalize FX RSS Items",
        },
        {
            "parameters": {"options": {"reset": False}},
            "type": "n8n-nodes-base.splitInBatches",
            "typeVersion": 3,
            "position": [0, 64],
            "id": "fc5d8e06-e01b-4b3a-8c6d-54ba5e0e1684",
            "name": "20G - Split FX Items",
        },
        {
            "parameters": {"jsCode": load_code("02_add_keys.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [224, 64],
            "id": "4d782a8c-bc9f-4818-8716-97f3b9bcbeea",
            "name": "20G0 - Add Keys",
        },
        {
            "parameters": {"jsCode": load_code("03_prepare_llm_input.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [448, 64],
            "id": "a66d30b6-108c-493e-a686-e52836876c8f",
            "name": "20H0 - Prepare LLM Input",
        },
        {
            "parameters": {
                "modelId": {"__rl": True, "mode": "list", "value": "gpt-5-mini", "cachedResultName": "GPT-5-MINI"},
                "responses": {
                    "values": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"content": "=Analyse cette news FX:\n{{ $json.llmInput }}"},
                    ]
                },
                "builtInTools": {},
                "options": {
                    "textFormat": {
                        "textOptions": {
                            "type": "json_schema",
                            "name": "ag4_forex_news_normalizer_v1",
                            "schema": schema(),
                            "strict": True,
                        }
                    }
                },
            },
            "type": "@n8n/n8n-nodes-langchain.openAi",
            "typeVersion": 2.1,
            "position": [672, 64],
            "id": "c684af9d-1471-458c-8224-cfc03227d129",
            "name": "20H1 - Analyze with OpenAI",
            "credentials": {"openAiApi": {"id": "rILpYjTayqc4jXXZ", "name": "OpenAi account"}},
        },
        {
            "parameters": {"mode": "combine", "combineBy": "combineByPosition", "options": {}},
            "type": "n8n-nodes-base.merge",
            "typeVersion": 3.2,
            "position": [896, 64],
            "id": "151e64e7-a473-4454-b8f2-923e77832577",
            "name": "20H1B - Merge AI + Context",
        },
        {
            "parameters": {"jsCode": load_code("04_parse_llm_output.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1120, 64],
            "id": "09a87adf-ed70-46da-9adf-fd0cc58936f5",
            "name": "20H2 - Parse AI Output",
        },
        {
            "parameters": {"language": "pythonNative", "pythonCode": load_code("05_write_fx_news_duckdb.py")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [1344, 64],
            "id": "a8c43031-7318-4aa6-a10f-b98d12e14ac1",
            "name": "20DBW - Upsert FX News DuckDB",
        },
        {
            "parameters": {"language": "pythonNative", "pythonCode": load_code("06_finalize_fx_run.py")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [-448, -112],
            "id": "5fd1893f-b1c0-4754-b264-18539e91587d",
            "name": "20R1 - Finalize FX Run",
        },
    ]

    connections = {
        "Schedule Trigger": {"main": [[{"node": "20A - Load FX Sources", "type": "main", "index": 0}]]},
        "Manual Trigger": {"main": [[{"node": "20A - Load FX Sources", "type": "main", "index": 0}]]},
        "20A - Load FX Sources": {"main": [[{"node": "20D - Split FX Feeds", "type": "main", "index": 0}]]},
        "20D - Split FX Feeds": {
            "main": [
                [{"node": "20R1 - Finalize FX Run", "type": "main", "index": 0}],
                [{"node": "20E - RSS Feed Read", "type": "main", "index": 0}],
            ]
        },
        "20E - RSS Feed Read": {"main": [[{"node": "20F - Normalize FX RSS Items", "type": "main", "index": 0}]]},
        "20F - Normalize FX RSS Items": {"main": [[{"node": "20G - Split FX Items", "type": "main", "index": 0}]]},
        "20G - Split FX Items": {
            "main": [
                [{"node": "20D - Split FX Feeds", "type": "main", "index": 0}],
                [{"node": "20G0 - Add Keys", "type": "main", "index": 0}],
            ]
        },
        "20G0 - Add Keys": {"main": [[{"node": "20H0 - Prepare LLM Input", "type": "main", "index": 0}]]},
        "20H0 - Prepare LLM Input": {
            "main": [
                [
                    {"node": "20H1 - Analyze with OpenAI", "type": "main", "index": 0},
                    {"node": "20H1B - Merge AI + Context", "type": "main", "index": 0},
                ]
            ]
        },
        "20H1 - Analyze with OpenAI": {"main": [[{"node": "20H1B - Merge AI + Context", "type": "main", "index": 1}]]},
        "20H1B - Merge AI + Context": {"main": [[{"node": "20H2 - Parse AI Output", "type": "main", "index": 0}]]},
        "20H2 - Parse AI Output": {"main": [[{"node": "20DBW - Upsert FX News DuckDB", "type": "main", "index": 0}]]},
        "20DBW - Upsert FX News DuckDB": {"main": [[{"node": "20G - Split FX Items", "type": "main", "index": 0}]]},
    }

    return {
        "name": "AG4-Forex - News Watcher",
        "nodes": nodes,
        "connections": connections,
        "pinData": {},
        "meta": {"instanceId": "093ba1266e85fcaaa6abca411d9f7941951737b3920bfaa6e2db89226df2c82d"},
    }


def main() -> None:
    out = DIR / "AG4-Forex-workflow.json"
    out.write_text(json.dumps(build(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
