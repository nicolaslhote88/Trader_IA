#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
NODES = ROOT / "nodes"
WORKFLOW = ROOT / "workflow"

VARIANTS = {
    "chatgpt52": {
        "model": "gpt-5.5",
        "provider": "openai",
        "cron": "30 4,8,12,16,20 * * 1-5",
        "db_path": "/files/duckdb/ag1_fx_v1_chatgpt52.duckdb",
    },
    "grok41_reasoning": {
        "model": "grok-4.20-0309-reasoning",
        "provider": "grok",
        "cron": "35 4,8,12,16,20 * * 1-5",
        "db_path": "/files/duckdb/ag1_fx_v1_grok41_reasoning.duckdb",
    },
    "gemini30_pro": {
        "model": "models/gemini-3.1-pro-preview",
        "provider": "gemini",
        "cron": "40 4,8,12,16,20 * * 1-5",
        "db_path": "/files/duckdb/ag1_fx_v1_gemini30_pro.duckdb",
    },
}

PROVIDER_META = {
    "openai": {
        "merge": "Merge GPT Output + Brief",
        "model_name": "OpenAI Chat Model - GPT5.5",
        "credential_key": "openAiApi",
        "credential": {"id": "rILpYjTayqc4jXXZ", "name": "OpenAi account"},
    },
    "gemini": {
        "merge": "Merge Gemini Output + Brief",
        "model_name": "Google Gemini Chat Model",
        "credential_key": "googlePalmApi",
        "credential": {"id": "JvctQP9WhDcQkucy", "name": "Google Gemini(PaLM) Api account 2"},
    },
    "grok": {
        "merge": "Merge Grok Output + Brief",
        "model_name": "xAI Grok Chat Model",
        "credential_key": "xAiApi",
        "credential": {"id": "8nKnkigAO1POxfzs", "name": "xAi account"},
    },
}


def read(rel: str) -> str:
    return (NODES / rel).read_text(encoding="utf-8")


def code_node(name: str, rel: str, x: int, y: int, lang: str = "pythonNative") -> dict:
    params = {"language": lang, "pythonCode": read(rel)} if lang == "pythonNative" else {"jsCode": read(rel)}
    return {"parameters": params, "type": "n8n-nodes-base.code", "typeVersion": 2, "position": [x, y], "id": name.lower().replace(" ", "-"), "name": name}


def agent_system_prompt() -> str:
    system_prompt = read("agent_input/07_system_prompt_fx.md")
    system_prompt = system_prompt.replace("{{llm_model}}", "{{ $json.llm_model }}")
    system_prompt = system_prompt.replace("{{leverage_max}}", "{{ $json.brief.config.leverage_max }}")
    return system_prompt + (
        "\n\nResponse schema reminder:\n"
        "- Return valid JSON only, no markdown fences.\n"
        "- Required top-level keys: as_of, decisions.\n"
        "- decisions[] items: pair, decision, conviction, optional size_lots, size_pct_equity, "
        "stop_loss_price, take_profit_price, horizon, rationale, lot_id_to_close.\n"
        "- decision must be one of open_long, open_short, close, partial_close, hold.\n"
    )


def agent_node() -> dict:
    return {
        "parameters": {
            "promptType": "define",
            "text": "={{ $json.user_prompt }}",
            "options": {
                "systemMessage": "=" + agent_system_prompt(),
                "maxIterations": 20,
            },
        },
        "type": "@n8n/n8n-nodes-langchain.agent",
        "typeVersion": 3.1,
        "position": [580, -160],
        "id": "e5c4046e-e795-4c38-89b8-32cc6493ebd6",
        "name": "Agent #1 - Portfolio manager1",
    }


def model_node(variant: str, cfg: dict) -> dict:
    provider = cfg["provider"]
    meta = PROVIDER_META[provider]
    if provider == "openai":
        params = {
            "model": {"__rl": True, "value": cfg["model"], "mode": "list", "cachedResultName": cfg["model"]},
            "builtInTools": {},
            "options": {"timeout": 1500000},
        }
        node_type = "@n8n/n8n-nodes-langchain.lmChatOpenAi"
        type_version = 1.2
    elif provider == "gemini":
        params = {"modelName": cfg["model"], "options": {}}
        node_type = "@n8n/n8n-nodes-langchain.lmChatGoogleGemini"
        type_version = 1
    else:
        params = {"model": cfg["model"], "options": {}}
        node_type = "@n8n/n8n-nodes-langchain.lmChatXAiGrok"
        type_version = 1

    return {
        "parameters": params,
        "type": node_type,
        "typeVersion": type_version,
        "position": [580, 80],
        "id": "2cda413f-27f9-4a0c-9f16-f1d4d5b0b98f",
        "name": meta["model_name"],
        "credentials": {meta["credential_key"]: meta["credential"]},
    }


def merge_node(cfg: dict) -> dict:
    return {
        "parameters": {
            "mode": "combine",
            "combineBy": "combineByPosition",
            "options": {},
        },
        "type": "n8n-nodes-base.merge",
        "typeVersion": 3.2,
        "position": [820, -20],
        "id": "merge-agent-output",
        "name": PROVIDER_META[cfg["provider"]]["merge"],
    }


def llm_segment_nodes(variant: str, cfg: dict) -> list[dict]:
    return [agent_node(), model_node(variant, cfg), merge_node(cfg)]


def connect_linear(connections: dict, names: list[str]) -> None:
    for a, b in zip(names, names[1:]):
        connections[a] = {"main": [[{"node": b, "type": "main", "index": 0}]]}


def build_template(variant: str = "chatgpt52") -> dict:
    cfg = VARIANTS[variant]
    init_code = read("pre_agent/01_init_run_fx.js")
    init_code = init_code.replace("const DEFAULT_VARIANT = 'chatgpt52';", f"const DEFAULT_VARIANT = {variant!r};")
    init_code = init_code.replace("dbPathByVariant[variant] || dbPathByVariant[DEFAULT_VARIANT]", repr(cfg["db_path"]))

    post_x0 = 1060
    nodes = [
        {
            "parameters": {"rule": {"interval": [{"field": "cronExpression", "expression": cfg["cron"]}]}},
            "type": "n8n-nodes-base.scheduleTrigger",
            "typeVersion": 1.3,
            "position": [-1100, -120],
            "id": "schedule",
            "name": "Schedule Trigger",
        },
        {"parameters": {}, "type": "n8n-nodes-base.manualTrigger", "typeVersion": 1, "position": [-1100, 80], "id": "manual", "name": "Manual Trigger"},
        {"parameters": {"jsCode": init_code}, "type": "n8n-nodes-base.code", "typeVersion": 2, "position": [-860, -20], "id": "01-init", "name": "01 Init Run FX"},
        code_node("02 Load Universe FX", "pre_agent/02_load_universe_fx.py", -620, -20),
        code_node("03 Load Portfolio State FX", "pre_agent/03_load_portfolio_state_fx.py", -380, -20),
        code_node("04 Load Technical Signals FX", "pre_agent/04_load_technical_signals_fx.py", -140, -20),
        code_node("05 Load News Macro FX", "pre_agent/05_load_news_macro_fx.py", 100, -20),
        code_node("06 Assemble Brief FX", "pre_agent/06_assemble_brief_fx.js", 340, -20, "javaScript"),
        *llm_segment_nodes(variant, cfg),
        code_node("10 Parse Decision FX", "post_agent/10_parse_decision_fx.js", post_x0, -20, "javaScript"),
        code_node("11 Validate Enforce Safety FX", "post_agent/11_validate_enforce_safety_fx.js", post_x0 + 240, -20, "javaScript"),
        code_node("12 Simulate Fills FX", "post_agent/12_simulate_fills_fx.py", post_x0 + 480, -20),
        code_node("13 Write Orders FX", "post_agent/13_write_orders_fx.py", post_x0 + 720, -20),
        code_node("14 Write Lots FX", "post_agent/14_write_lots_fx.py", post_x0 + 960, -20),
        code_node("15 Close Lots FX", "post_agent/15_close_lots_fx.py", post_x0 + 1200, -20),
        code_node("16 Snapshot Portfolio FX", "post_agent/16_snapshot_portfolio_fx.py", post_x0 + 1440, -20),
        code_node("17 Log Run FX", "post_agent/17_log_run_fx.py", post_x0 + 1680, -20),
    ]

    merge = PROVIDER_META[cfg["provider"]]["merge"]
    model = PROVIDER_META[cfg["provider"]]["model_name"]
    connections = {
        "Schedule Trigger": {"main": [[{"node": "01 Init Run FX", "type": "main", "index": 0}]]},
        "Manual Trigger": {"main": [[{"node": "01 Init Run FX", "type": "main", "index": 0}]]},
    }
    connect_linear(
        connections,
        [
            "01 Init Run FX",
            "02 Load Universe FX",
            "03 Load Portfolio State FX",
            "04 Load Technical Signals FX",
            "05 Load News Macro FX",
            "06 Assemble Brief FX",
        ],
    )
    connections["06 Assemble Brief FX"] = {
        "main": [[
            {"node": "Agent #1 - Portfolio manager1", "type": "main", "index": 0},
            {"node": merge, "type": "main", "index": 0},
        ]]
    }
    connections[model] = {
        "ai_languageModel": [[{"node": "Agent #1 - Portfolio manager1", "type": "ai_languageModel", "index": 0}]]
    }
    connections["Agent #1 - Portfolio manager1"] = {
        "main": [[{"node": merge, "type": "main", "index": 1}]]
    }
    connect_linear(
        connections,
        [
            merge,
            "10 Parse Decision FX",
            "11 Validate Enforce Safety FX",
            "12 Simulate Fills FX",
            "13 Write Orders FX",
            "14 Write Lots FX",
            "15 Close Lots FX",
            "16 Snapshot Portfolio FX",
            "17 Log Run FX",
        ],
    )

    return {
        "name": f"AG1-FX-V1 Portfolio Manager - {variant}",
        "nodes": nodes,
        "connections": connections,
        "settings": {"timezone": "Europe/Paris"},
        "meta": {
            "templateCredsSetupCompleted": True,
            "note": f"{model} variant uses a LangChain agent and merges agent output back with the AG1-FX brief.",
        },
    }


def main() -> None:
    WORKFLOW.mkdir(parents=True, exist_ok=True)
    template = build_template("chatgpt52")
    (WORKFLOW / "AG1_FX_workflow_template_v1.json").write_text(json.dumps(template, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    for key in VARIANTS:
        wf = build_template(key)
        (WORKFLOW / f"AG1_FX_workflow_{key}_v1.json").write_text(json.dumps(wf, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"[OK] {key}")


if __name__ == "__main__":
    main()
