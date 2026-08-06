#!/usr/bin/env python3
"""Reconstruit l'artefact AG4_Spé-V2 depuis le scaffold publié et les nœuds canonisés.

Le workflow live contient de nombreux paramètres n8n (positions, versions de nœuds,
credentials et options HTTP). Le JSON publié reste le scaffold structurel ; les
fichiers `nodes/` sont la source canonique du code embarqué.
"""
from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "AG4-SPE-V2-workflow.json"
AG2_READ_UNIVERSE = ROOT.parents[1] / "AG2 - La technique" / "AG2-V3" / "nodes" / "00_read_universe.py"

CODE_NODES = {
    "S00A - Load Universe (Google Sheets)": ("pythonCode", AG2_READ_UNIVERSE),
    "S00B - DuckDB Init Schema": ("pythonCode", ROOT / "nodes" / "00_duckdb_prepare.py"),
    "S01 - Build Symbol Queue": ("pythonCode", ROOT / "nodes" / "01_build_symbol_queue.py"),
    "S02 - Start Run": ("pythonCode", ROOT / "nodes" / "02_start_run.py"),
    "S02B - Reset News Buffer": ("jsCode", ROOT / "nodes" / "02_reset_news_buffer.js"),
    "S05E - Build Listing Error Row": ("jsCode", ROOT / "nodes" / "11_build_error_row.js"),
    "S06 - Extract Articles": ("jsCode", ROOT / "nodes" / "03_extract_listing_articles.js"),
    "S07 - Normalize + Dedupe + Limit": ("jsCode", ROOT / "nodes" / "04_normalize_articles.js"),
    "S09 - Explode Articles": ("jsCode", ROOT / "nodes" / "05_explode_articles.js"),
    "S11 - Route New vs Seen": ("pythonCode", ROOT / "nodes" / "06_route_new_seen.py"),
    "S15E - Build Article Error Row": ("jsCode", ROOT / "nodes" / "11_build_error_row.js"),
    "S16 - Parse Article": ("jsCode", ROOT / "nodes" / "07_parse_article.js"),
    "S17 - Prepare LLM Input": ("jsCode", ROOT / "nodes" / "08_prepare_llm_input.js"),
    "S20 - Parse LLM Output": ("jsCode", ROOT / "nodes" / "09_parse_llm_output.js"),
    "S21 - Build Skip Row": ("jsCode", ROOT / "nodes" / "10_build_skip_row.js"),
    "S21B - Buffer News Rows": ("jsCode", ROOT / "nodes" / "12_buffer_news_rows.js"),
    "S22 - Upsert News DuckDB": ("pythonCode", ROOT / "nodes" / "12_write_news_duckdb.py"),
    "S22B - Flush News Buffer": ("jsCode", ROOT / "nodes" / "12_flush_news_buffer.js"),
    "S23A - Write Article Errors DuckDB": ("pythonCode", ROOT / "nodes" / "13_write_errors_duckdb.py"),
    "S23L - Write Listing Errors DuckDB": ("pythonCode", ROOT / "nodes" / "13_write_errors_duckdb.py"),
    "S24 - Finalize Run": ("pythonCode", ROOT / "nodes" / "14_finalize_run.py"),
}

DEEPSEEK_CHAIN_NAME = "S19 - Analyze with DeepSeek"
DEEPSEEK_MODEL_NAME = "S19A - DeepSeek Chat Model"
DEEPSEEK_PARSER_NAME = "S19B - Structured Output DeepSeek"
DEEPSEEK_MODEL = "deepseek-v4-pro"
DEEPSEEK_CREDENTIAL = {"id": "BlSCC28mzKodkfO5", "name": "DeepSeek account"}


def configure_deepseek_analyzer(workflow: dict) -> None:
    nodes = workflow.get("nodes", [])
    analyzer = next(
        (
            node for node in nodes
            if node.get("name") in {"S19 - Analyze with OpenAI", DEEPSEEK_CHAIN_NAME}
        ),
        None,
    )
    if analyzer is None:
        raise RuntimeError("Nœud d'analyse S19 absent")

    if analyzer.get("type") == "@n8n/n8n-nodes-langchain.openAi":
        values = analyzer["parameters"]["responses"]["values"]
        system_prompt = next(value["content"] for value in values if value.get("role") == "system")
        user_prompt = next(value["content"] for value in values if value.get("role") != "system")
        output_schema = analyzer["parameters"]["options"]["textFormat"]["textOptions"]["schema"]
    else:
        system_prompt = analyzer["parameters"]["messages"]["messageValues"][0]["message"]
        user_prompt = analyzer["parameters"]["text"]
        parser = next(node for node in nodes if node.get("name") == DEEPSEEK_PARSER_NAME)
        output_schema = parser["parameters"]["inputSchema"]

    old_name = analyzer["name"]
    old_position = analyzer.get("position", [10816, 5952])
    chain_id = analyzer.get("id") or "32c21fbf-dda1-44d1-9bd5-c4a536493628"
    model_id = "4d2f3579-114d-4861-956c-64bc6cd134ac"
    parser_id = "b7cf0c18-cc5b-51e2-a374-8c1fd2227346"

    workflow["nodes"] = [
        node for node in nodes
        if node.get("name") not in {
            old_name,
            DEEPSEEK_MODEL_NAME,
            DEEPSEEK_PARSER_NAME,
        }
        and node.get("id") not in {chain_id, model_id, parser_id}
    ]
    workflow["nodes"].extend(
        [
            {
                "parameters": {
                    "promptType": "define",
                    "text": user_prompt,
                    "hasOutputParser": True,
                    "messages": {"messageValues": [{"message": system_prompt}]},
                },
                "type": "@n8n/n8n-nodes-langchain.chainLlm",
                "typeVersion": 1.5,
                "position": old_position,
                "id": chain_id,
                "name": DEEPSEEK_CHAIN_NAME,
                "onError": analyzer.get("onError", "continueRegularOutput"),
            },
            {
                "parameters": {"model": DEEPSEEK_MODEL, "options": {}},
                "type": "@n8n/n8n-nodes-langchain.lmChatDeepSeek",
                "typeVersion": 1,
                "position": [old_position[0] - 112, old_position[1] + 224],
                "id": model_id,
                "name": DEEPSEEK_MODEL_NAME,
                "credentials": {"deepSeekApi": DEEPSEEK_CREDENTIAL},
            },
            {
                "parameters": {"schemaType": "manual", "inputSchema": output_schema},
                "type": "@n8n/n8n-nodes-langchain.outputParserStructured",
                "typeVersion": 1.3,
                "position": [old_position[0] + 112, old_position[1] + 224],
                "id": parser_id,
                "name": DEEPSEEK_PARSER_NAME,
            },
        ]
    )

    connections = workflow.setdefault("connections", {})
    old_main = connections.pop(old_name, {}).get("main") or [[{
        "node": "S19M - Merge AI + Context", "type": "main", "index": 1
    }]]
    connections.pop(DEEPSEEK_MODEL_NAME, None)
    connections.pop(DEEPSEEK_PARSER_NAME, None)
    for source_connections in connections.values():
        for branches in source_connections.values():
            for branch in branches:
                for target in branch:
                    if target.get("node") == old_name:
                        target["node"] = DEEPSEEK_CHAIN_NAME
    connections[DEEPSEEK_CHAIN_NAME] = {"main": old_main}
    connections[DEEPSEEK_MODEL_NAME] = {
        "ai_languageModel": [[{
            "node": DEEPSEEK_CHAIN_NAME, "type": "ai_languageModel", "index": 0
        }]]
    }
    connections[DEEPSEEK_PARSER_NAME] = {
        "ai_outputParser": [[{
            "node": DEEPSEEK_CHAIN_NAME, "type": "ai_outputParser", "index": 0
        }]]
    }


def load_workflow(path: Path) -> dict:
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    workflow = raw[0] if isinstance(raw, list) else raw
    for key in ("nodes", "connections"):
        if isinstance(workflow.get(key), str):
            workflow[key] = json.loads(workflow[key])
    return workflow


def build(source: Path) -> dict:
    workflow = load_workflow(source)
    by_name = {node.get("name"): node for node in workflow.get("nodes", [])}
    missing = sorted(set(CODE_NODES) - set(by_name))
    if missing:
        raise RuntimeError(f"Nœuds absents du scaffold: {missing}")
    for name, (parameter, path) in CODE_NODES.items():
        code = path.read_text(encoding="utf-8")
        # Conserver les terminaisons exactes de la version publiée n8n.
        if name == "S00A - Load Universe (Google Sheets)":
            code = code.rstrip("\r\n")
        elif name == "S16 - Parse Article":
            code = code.rstrip("\r\n") + "\n\n"
        by_name[name].setdefault("parameters", {})[parameter] = code
    configure_deepseek_analyzer(workflow)
    identity = json.dumps(
        {
            "id": workflow.get("id"),
            "nodes": workflow.get("nodes", []),
            "connections": workflow.get("connections", {}),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    workflow["versionId"] = str(uuid.uuid5(uuid.NAMESPACE_URL, identity))
    workflow["activeVersionId"] = None
    return workflow


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=OUTPUT,
                        help="Scaffold n8n (défaut: artefact courant ; accepte un export publié).")
    args = parser.parse_args()
    workflow = build(args.source)
    OUTPUT.write_text(json.dumps([workflow], ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
