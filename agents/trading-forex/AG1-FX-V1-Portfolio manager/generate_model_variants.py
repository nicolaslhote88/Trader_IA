#!/usr/bin/env python3
from __future__ import annotations

import copy
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
NODES = ROOT / "nodes"
WORKFLOW = ROOT / "workflow"

VARIANTS = {
    "chatgpt52": {
        "model": "gpt-5.2-2025-12-11",
        # 9h30 et 14h30 Paris lun-ven : run matin (après ouverture bourse FR + AG2 8h + AG4 9h15)
        # et run après-midi (avant session US 15h30 + AG2 12h + AG4 14h15).
        "cron": "30 9,14 * * 1-5",
        "db_path": "/files/duckdb/ag1_fx_v1_chatgpt52.duckdb",
    },
    "grok41_reasoning": {
        "model": "grok-4-1-fast-reasoning",
        # +15 min vs chatgpt52 pour étaler la charge runner et éviter conflits lecture concurrente DuckDB.
        "cron": "45 9,14 * * 1-5",
        "db_path": "/files/duckdb/ag1_fx_v1_grok41_reasoning.duckdb",
    },
    "gemini30_pro": {
        "model": "models/gemini-3-pro-preview",
        # +30 min vs chatgpt52 (10h00 / 15h00). Reste dans la fenêtre ouverture bourse FR (9h-17h30).
        "cron": "0 10,15 * * 1-5",
        "db_path": "/files/duckdb/ag1_fx_v1_gemini30_pro.duckdb",
    },
}


def read(rel: str) -> str:
    return (NODES / rel).read_text(encoding="utf-8")


def code_node(name: str, rel: str, x: int, y: int, lang: str = "pythonNative") -> dict:
    params = {"language": lang, "pythonCode": read(rel)} if lang == "pythonNative" else {"jsCode": read(rel)}
    return {"parameters": params, "type": "n8n-nodes-base.code", "typeVersion": 2, "position": [x, y], "id": name.lower().replace(" ", "-"), "name": name}


def build_template(variant: str = "chatgpt52") -> dict:
    cfg = VARIANTS[variant]
    init_code = read("pre_agent/01_init_run_fx.js").replace("'gpt-5.2-2025-12-11'", repr(cfg["model"])).replace("'chatgpt52'", repr(variant))
    init_code = init_code.replace("dbPathByVariant[variant] || dbPathByVariant.chatgpt52", repr(cfg["db_path"]))
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
        {
            "parameters": {
                "jsCode": "const j = $json;\nreturn [{json: {...j, decision_json: {as_of: j.as_of, narrative: 'P3 dry-run placeholder; connect LLM node here after manual review', decisions: (j.brief?.universe?.pairs || []).map((pair) => ({pair, decision: 'hold', conviction: 0.1}))}}}];\n"
            },
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [580, -20],
            "id": "llm-placeholder",
            "name": "LLM Decision Placeholder",
        },
        code_node("10 Parse Decision FX", "post_agent/10_parse_decision_fx.js", 820, -20, "javaScript"),
        code_node("11 Validate Enforce Safety FX", "post_agent/11_validate_enforce_safety_fx.js", 1060, -20, "javaScript"),
        code_node("12 Simulate Fills FX", "post_agent/12_simulate_fills_fx.py", 1300, -20),
        code_node("13 Write Orders FX", "post_agent/13_write_orders_fx.py", 1540, -20),
        code_node("14 Write Lots FX", "post_agent/14_write_lots_fx.py", 1780, -20),
        code_node("15 Close Lots FX", "post_agent/15_close_lots_fx.py", 2020, -20),
        code_node("16 Snapshot Portfolio FX", "post_agent/16_snapshot_portfolio_fx.py", 2260, -20),
        code_node("17 Log Run FX", "post_agent/17_log_run_fx.py", 2500, -20),
    ]
    order = [n["name"] for n in nodes[2:]]
    connections = {
        "Schedule Trigger": {"main": [[{"node": order[0], "type": "main", "index": 0}]]},
        "Manual Trigger": {"main": [[{"node": order[0], "type": "main", "index": 0}]]},
    }
    for a, b in zip(order, order[1:]):
        connections[a] = {"main": [[{"node": b, "type": "main", "index": 0}]]}
    return {
        "name": f"AG1-FX-V1 Portfolio Manager - {variant}",
        "nodes": nodes,
        "connections": connections,
        "settings": {"timezone": "Europe/Paris"},
        "meta": {"note": "LLM placeholder is intentional for P3 manual dry-run; replace with provider node after Nicolas review."},
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
