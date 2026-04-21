import copy
import json
import random
import re
import string
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SOURCE_WORKFLOW = ROOT / "AG1_workflow_template_v3.json"
OUTPUT_DIR = ROOT / "variants"


COMMON_NODES = {
    "1 - Hourly Trigger",
    "7 - Validate & Enforce Safety",
    "merge",
    "8 - Build DuckDB Bundle",
    "4B – Build Portfolio Context",
    "Sticky Note1",
    "Sticky Note3",
    "Sticky Note4",
    "2B - Init Run Context",
    "Sticky Note5",
    "4C — Enrich Portfolio with Market Prices",
    "20J_FINAL — Build MarketNewsPack Final",
    "When clicking ‘Execute workflow’",
    "Sticky Note6",
    "AG4.01 - Récupération des news générales",
    "R8 — Data Prep for Matrix (Fusion Filter)",
    "Calcul Matrice & Briefing",
    "Merge7",
    "AG1.00 — Assemble Input Packs",
    "Sticky Note",
    "9 - Upsert Run Bundle (DuckDB)",
    "10 - Post-Run Health (DuckDB)",
    "0 - SEED Portfolio",
    "FX.00 - Prepare FX Brief Context",
}


VARIANTS = {
    "chatgpt52": {
        "workflow_name": "AG1 - Workflow général - ChatGPT 5.2",
        "db_path": "/files/duckdb/ag1_v3_chatgpt52.duckdb",
        "run_defaults": {
            "strategy_version": "strategy_v3",
            "config_version": "config_v3",
            "prompt_version": "prompt_v3",
            "model": "gpt-5.2-2025-12-11",
        },
        "agent_node": "Agent #1 - Portfolio manager",
        "extractor_node": "Information Extractor",
        "layout_overrides_key": "chatgpt52_current",
        "branch_nodes": {
            "Qdrant Vector Store",
            "OpenAI Chat Model",
            "Qdrant Vector Store1",
            "OpenAI Chat Model1",
            "Qdrant Vector Store2",
            "OpenAI Chat Model2",
            "get_specific_news",
            "get_fundamental_details",
            "get_technical_details",
            "Embeddings OpenAI1",
            "Agent #1 - Portfolio manager",
            "Information Extractor",
            "OpenAI Chat Model - GPT5.2",
            "OpenAI Chat Model3",
            "Embeddings OpenAI2",
            "Embeddings OpenAI3",
            "Sticky Note8",
            "news_web_x_scan (Grok)",
            "OpenAI Chat Model8",
            "Sticky Note11",
        },
    },
    "grok41_reasoning": {
        "workflow_name": "AG1 - Workflow général - Grok 4.1 Reasoning",
        "db_path": "/files/duckdb/ag1_v3_grok41_reasoning.duckdb",
        "run_defaults": {
            "strategy_version": "strategy_v3",
            "config_version": "config_v3",
            "prompt_version": "prompt_v3",
            "model": "grok-4-1-fast-reasoning",
        },
        "agent_node": "Agent #1 - Portfolio manager1",
        "extractor_node": "Information Extractor1",
        "layout_overrides_key": "grok41_reasoning_current",
        "branch_nodes": {
            "Qdrant Vector Store3",
            "OpenAI Chat Model4",
            "Qdrant Vector Store4",
            "OpenAI Chat Model5",
            "Qdrant Vector Store5",
            "OpenAI Chat Model6",
            "get_specific_news1",
            "get_fundamental_details1",
            "get_technical_details1",
            "Embeddings OpenAI",
            "Agent #1 - Portfolio manager1",
            "Information Extractor1",
            "OpenAI Chat Model7",
            "Embeddings OpenAI4",
            "Embeddings OpenAI5",
            "Sticky Note9",
            "news_web_x_scan (Grok)1",
            "xAI Grok Chat Model",
            "Sticky Note2",
            "xAI Grok Chat Model1",
        },
    },
    "gemini30_pro": {
        "workflow_name": "AG1 - Workflow général - Gemini 3.0 Pro",
        "db_path": "/files/duckdb/ag1_v3_gemini30_pro.duckdb",
        "run_defaults": {
            "strategy_version": "strategy_v3",
            "config_version": "config_v3",
            "prompt_version": "prompt_v3",
            "model": "models/gemini-3-pro-preview",
        },
        "agent_node": "Agent #1 - Portfolio manager2",
        "extractor_node": "Information Extractor2",
        "layout_overrides_key": "gemini30_pro_current",
        "branch_nodes": {
            "Qdrant Vector Store6",
            "OpenAI Chat Model10",
            "Qdrant Vector Store7",
            "OpenAI Chat Model11",
            "Qdrant Vector Store8",
            "OpenAI Chat Model12",
            "get_specific_news2",
            "get_fundamental_details2",
            "get_technical_details2",
            "Embeddings OpenAI6",
            "Agent #1 - Portfolio manager2",
            "Information Extractor2",
            "OpenAI Chat Model13",
            "Embeddings OpenAI7",
            "Embeddings OpenAI8",
            "Sticky Note10",
            "news_web_x_scan (Grok)2",
            "OpenAI Chat Model14",
            "Sticky Note7",
            "Google Gemini Chat Model",
        },
    },
}


LAYOUT_OVERRIDES = {
    "chatgpt52_current": {
        "positions": {
            "1 - Hourly Trigger": [43024, 18784],
            "2B - Init Run Context": [43264, 18880],
            "4B – Build Portfolio Context": [43632, 18528],
            "4C — Enrich Portfolio with Market Prices": [43840, 18608],
            "AG4.01 - Récupération des news générales": [43600, 18912],
            "20J_FINAL — Build MarketNewsPack Final": [43856, 18912],
            "When clicking ‘Execute workflow’": [43008, 18992],
            "R8 — Data Prep for Matrix (Fusion Filter)": [43632, 19200],
            "Calcul Matrice & Briefing": [43856, 19200],
            "FX.00 - Prepare FX Brief Context": [43728, 19600],
            "Merge7": [44320, 18704],
            "AG1.00 — Assemble Input Packs": [44480, 18736],
            "merge": [46624, 18608],
            "7 - Validate & Enforce Safety": [46832, 18608],
            "8 - Build DuckDB Bundle": [47056, 18608],
            "9 - Upsert Run Bundle (DuckDB)": [47280, 18608],
            "10 - Post-Run Health (DuckDB)": [47472, 18608],
            "0 - SEED Portfolio": [46944, 18240],
            "Sticky Note1": [44288, 18512],
            "Sticky Note3": [46528, 18512],
            "Sticky Note4": [46944, 18512],
            "Sticky Note5": [43392, 18768],
            "Sticky Note6": [43392, 18480],
            "Sticky Note": [43392, 19136],
            "Sticky Note8": [44800, 19328],
            "Sticky Note11": [44688, 18752],
            "Agent #1 - Portfolio manager": [45232, 18816],
            "Information Extractor": [45968, 18848],
            "news_web_x_scan (Grok)": [45632, 18992],
            "OpenAI Chat Model - GPT5.2": [45088, 19008],
            "OpenAI Chat Model3": [45968, 19040],
            "OpenAI Chat Model8": [45632, 19184],
            "get_specific_news": [45072, 19424],
            "get_fundamental_details": [45408, 19424],
            "get_technical_details": [45728, 19424],
            "Qdrant Vector Store": [44864, 19616],
            "OpenAI Chat Model": [45152, 19616],
            "Qdrant Vector Store1": [45280, 19616],
            "OpenAI Chat Model1": [45568, 19616],
            "Qdrant Vector Store2": [45696, 19616],
            "OpenAI Chat Model2": [45968, 19616],
            "Embeddings OpenAI1": [44864, 19808],
            "Embeddings OpenAI2": [45280, 19808],
            "Embeddings OpenAI3": [45696, 19808],
        },
        "parameter_overrides": {
            "Sticky Note1": {
                "content": "## Agent #1 : Le Portfolio Manager",
                "height": 1552,
                "width": 2240,
                "color": 6,
            },
            "Sticky Note3": {
                "content": "## Agent #5 risk manager",
                "height": 304,
                "width": 416,
                "color": 4,
            },
            "Sticky Note4": {
                "content": "## Agent #6 \"Execution Trader\" (L'Exécuteur)\nApplication des décisions dans la base de donnée (portefeuille virtuel)",
                "height": 304,
                "width": 800,
                "color": 2,
            },
            "Sticky Note5": {
                "content": "## Agent #4 (The News Watcher)\n extraction des news marchés",
                "height": 368,
                "width": 896,
                "color": 5,
            },
            "Sticky Note6": {
                "content": "## Le portefeuille",
                "height": 288,
                "width": 896,
                "color": 7,
            },
            "Sticky Note": {
                "content": "## Préparation pack de brief : Matrice Risk/Reward",
                "height": 720,
                "width": 896,
            },
            "Sticky Note8": {
                "content": "##  Memoires vectorielles",
                "height": 656,
                "width": 1328,
                "color": 3,
            },
            "Sticky Note11": {
                "content": "## GPT",
                "height": 1264,
                "width": 1632,
            },
        },
    },
    "grok41_reasoning_current": {
        "positions": {
            "1 - Hourly Trigger": [51584, 59376],
            "2B - Init Run Context": [51824, 59472],
            "4B \u2013 Build Portfolio Context": [52208, 59120],
            "4C \u2014 Enrich Portfolio with Market Prices": [52416, 59200],
            "AG4.01 - R\u00e9cup\u00e9ration des news g\u00e9n\u00e9rales": [52176, 59504],
            "20J_FINAL \u2014 Build MarketNewsPack Final": [52432, 59504],
            "When clicking \u2018Execute workflow\u2019": [51584, 59584],
            "R8 \u2014 Data Prep for Matrix (Fusion Filter)": [52208, 59792],
            "Calcul Matrice & Briefing": [52416, 59792],
            "FX.00 - Prepare FX Brief Context": [52304, 60144],
            "Merge7": [52880, 59296],
            "AG1.00 \u2014 Assemble Input Packs": [53056, 59328],
            "merge": [55184, 59200],
            "7 - Validate & Enforce Safety": [55392, 59200],
            "8 - Build DuckDB Bundle": [55616, 59200],
            "9 - Upsert Run Bundle (DuckDB)": [55840, 59200],
            "10 - Post-Run Health (DuckDB)": [56032, 59200],
            "0 - SEED Portfolio": [55520, 58832],
            "Sticky Note1": [52848, 59104],
            "Sticky Note3": [55088, 59104],
            "Sticky Note4": [55504, 59104],
            "Sticky Note5": [51952, 59360],
            "Sticky Note6": [51952, 59072],
            "Sticky Note": [51952, 59728],
            "Sticky Note9": [53376, 59824],
            "Sticky Note2": [53280, 59296],
            "Agent #1 - Portfolio manager1": [53808, 59312],
            "Information Extractor1": [54544, 59344],
            "news_web_x_scan (Grok)1": [54208, 59488],
            "xAI Grok Chat Model": [53728, 59552],
            "OpenAI Chat Model7": [54544, 59536],
            "xAI Grok Chat Model1": [54208, 59664],
            "get_specific_news1": [53648, 59920],
            "get_fundamental_details1": [53984, 59920],
            "get_technical_details1": [54304, 59920],
            "Qdrant Vector Store3": [53440, 60112],
            "OpenAI Chat Model4": [53728, 60112],
            "Qdrant Vector Store4": [53856, 60112],
            "OpenAI Chat Model5": [54144, 60112],
            "Qdrant Vector Store5": [54272, 60112],
            "OpenAI Chat Model6": [54544, 60112],
            "Embeddings OpenAI": [53440, 60304],
            "Embeddings OpenAI4": [53856, 60304],
            "Embeddings OpenAI5": [54272, 60304],
        },
        "parameter_overrides": {
            "Sticky Note1": {
                "content": "## Agent #1 : Le Portfolio Manager",
                "height": 1472,
                "width": 2240,
                "color": 6,
            },
            "Sticky Note": {
                "content": "## Pr\u00e9paration pack de brief : Matrice Risk/Reward",
                "height": 608,
                "width": 896,
            },
            "xAI Grok Chat Model": {
                "options": {
                    "timeout": 640000,
                },
            },
        },
    },
    "gemini30_pro_current": {
        "positions": {
            "1 - Hourly Trigger": [1552, 21488],
            "2B - Init Run Context": [1792, 21584],
            "4B \u2013 Build Portfolio Context": [2176, 21232],
            "4C \u2014 Enrich Portfolio with Market Prices": [2384, 21312],
            "AG4.01 - R\u00e9cup\u00e9ration des news g\u00e9n\u00e9rales": [2144, 21616],
            "20J_FINAL \u2014 Build MarketNewsPack Final": [2400, 21616],
            "When clicking \u2018Execute workflow\u2019": [1552, 21696],
            "R8 \u2014 Data Prep for Matrix (Fusion Filter)": [2176, 21904],
            "Calcul Matrice & Briefing": [2384, 21904],
            "FX.00 - Prepare FX Brief Context": [2240, 22176],
            "Merge7": [2848, 21408],
            "AG1.00 \u2014 Assemble Input Packs": [3024, 21440],
            "merge": [5152, 21312],
            "7 - Validate & Enforce Safety": [5360, 21312],
            "8 - Build DuckDB Bundle": [5584, 21312],
            "9 - Upsert Run Bundle (DuckDB)": [5808, 21312],
            "10 - Post-Run Health (DuckDB)": [6000, 21312],
            "Sticky Note1": [2816, 21184],
            "Sticky Note3": [5056, 21216],
            "Sticky Note4": [5472, 21216],
            "Sticky Note5": [1920, 21472],
            "Sticky Note6": [1920, 21184],
            "Sticky Note": [1920, 21840],
            "Sticky Note10": [3360, 22000],
            "Sticky Note7": [3280, 21424],
            "Agent #1 - Portfolio manager2": [3792, 21488],
            "Information Extractor2": [4528, 21520],
            "news_web_x_scan (Grok)2": [4192, 21664],
            "Google Gemini Chat Model": [3728, 21696],
            "OpenAI Chat Model13": [4528, 21712],
            "OpenAI Chat Model14": [4192, 21856],
            "get_specific_news2": [3632, 22096],
            "get_fundamental_details2": [3968, 22096],
            "get_technical_details2": [4288, 22096],
            "Qdrant Vector Store6": [3424, 22288],
            "OpenAI Chat Model10": [3712, 22288],
            "Qdrant Vector Store7": [3840, 22288],
            "OpenAI Chat Model11": [4128, 22288],
            "Qdrant Vector Store8": [4256, 22288],
            "OpenAI Chat Model12": [4528, 22288],
            "Embeddings OpenAI6": [3424, 22480],
            "Embeddings OpenAI7": [3840, 22480],
            "Embeddings OpenAI8": [4256, 22480],
        },
        "parameter_overrides": {
            "Sticky Note1": {
                "content": "## Agent #1 : Le Portfolio Manager",
                "height": 1552,
                "width": 2240,
                "color": 6,
            },
            "Sticky Note": {
                "content": "## Pr\u00e9paration pack de brief : Matrice Risk/Reward",
                "height": 544,
                "width": 896,
            },
        },
    },
}


OLD_PATHS = (
    "/files/duckdb/ag1_v3.duckdb",
    "/local-files/duckdb/ag1_v3.duckdb",
)

VARIANT_TRIGGER_INTERVALS = [
    {"field": "cronExpression", "expression": "0 15 9 * * 1-5"},   # 09:15 Mon-Fri
    {"field": "cronExpression", "expression": "0 30 12 * * 1-5"},  # 12:30 Mon-Fri
    {"field": "cronExpression", "expression": "0 45 16 * * 1-5"},  # 16:45 Mon-Fri
]
VARIANT_TIMEZONE = "Europe/Paris"


def random_n8n_id(length: int = 21) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(length))


def deep_replace_ag1_path(value, new_path: str):
    if isinstance(value, str):
        out = value
        for old in OLD_PATHS:
            out = out.replace(old, new_path)
        return out
    if isinstance(value, list):
        return [deep_replace_ag1_path(v, new_path) for v in value]
    if isinstance(value, dict):
        return {k: deep_replace_ag1_path(v, new_path) for k, v in value.items()}
    return value


def patch_init_run_context_defaults(workflow: dict, cfg: dict) -> None:
    run_defaults = cfg.get("run_defaults") or {}
    if not run_defaults:
        return

    node = next((n for n in (workflow.get("nodes") or []) if n.get("name") == "2B - Init Run Context"), None)
    if node is None:
        raise ValueError("Node '2B - Init Run Context' not found")

    params = node.get("parameters") or {}
    js_code = params.get("jsCode")
    if not isinstance(js_code, str) or not js_code.strip():
        raise ValueError("Node '2B - Init Run Context' has no jsCode")

    def replace_default(src: str, pattern: str, value: str, label: str) -> tuple[str, int]:
        updated, count = re.subn(pattern, rf"\1{value}\2", src)
        return updated, count

    js_code, count = replace_default(
        js_code,
        r'(strategyVersion:\s*String\(cfg\.strategy_version\s*\|\|\s*")[^"]*("\))',
        str(run_defaults.get("strategy_version", "strategy_v3")),
        "strategy_version",
    )
    if count < 1:
        raise ValueError("Init Run Context patch failed, pattern not found for strategy_version")

    js_code, count = replace_default(
        js_code,
        r'(configVersion:\s*String\(cfg\.config_version\s*\|\|\s*")[^"]*("\))',
        str(run_defaults.get("config_version", "config_v3")),
        "config_version",
    )
    if count < 1:
        raise ValueError("Init Run Context patch failed, pattern not found for config_version")

    js_code, count = replace_default(
        js_code,
        r'(promptVersion:\s*String\(cfg\.prompt_version\s*\|\|\s*")[^"]*("\))',
        str(run_defaults.get("prompt_version", "prompt_v3")),
        "prompt_version",
    )
    if count < 1:
        raise ValueError("Init Run Context patch failed, pattern not found for prompt_version")

    js_code, count = replace_default(
        js_code,
        r'(model:\s*String\(cfg\.model\s*\|\|\s*")[^"]*("\))',
        str(run_defaults.get("model", "gpt-5.2")),
        "model",
    )
    if count < 1:
        # Support newer templates where model is hard-coded (no cfg.model fallback).
        js_code, count = replace_default(
            js_code,
            r'(model:\s*")[^"]*(")',
            str(run_defaults.get("model", "gpt-5.2")),
            "model",
        )
    if count < 1:
        raise ValueError("Init Run Context patch failed, pattern not found for model")

    node.setdefault("parameters", {})["jsCode"] = js_code


def patch_start_trigger_schedule(workflow: dict) -> None:
    node = next((n for n in (workflow.get("nodes") or []) if n.get("name") == "1 - Hourly Trigger"), None)
    if node is None:
        raise ValueError("Node '1 - Hourly Trigger' not found")

    node["type"] = "n8n-nodes-base.scheduleTrigger"
    node["typeVersion"] = 1.3
    node["parameters"] = {
        "rule": {
            "interval": copy.deepcopy(VARIANT_TRIGGER_INTERVALS),
        }
    }


def patch_workflow_timezone(workflow: dict) -> None:
    settings = workflow.get("settings")
    if not isinstance(settings, dict):
        settings = {}
        workflow["settings"] = settings
    settings["timezone"] = VARIANT_TIMEZONE


def apply_layout_overrides(workflow: dict, cfg: dict) -> None:
    key = cfg.get("layout_overrides_key")
    if not key:
        return

    profile = LAYOUT_OVERRIDES.get(key) or {}
    if not isinstance(profile, dict):
        return

    nodes = workflow.get("nodes") or []
    node_by_name = {n.get("name"): n for n in nodes if isinstance(n, dict)}

    for name, pos in (profile.get("positions") or {}).items():
        node = node_by_name.get(name)
        if node is None:
            continue
        if not isinstance(pos, (list, tuple)) or len(pos) != 2:
            continue
        node["position"] = [int(pos[0]), int(pos[1])]

    for name, params in (profile.get("parameter_overrides") or {}).items():
        node = node_by_name.get(name)
        if node is None or not isinstance(params, dict):
            continue
        node_params = node.get("parameters")
        if not isinstance(node_params, dict):
            node_params = {}
            node["parameters"] = node_params
        node_params.update(copy.deepcopy(params))


def filter_connections(connections: dict, keep_nodes: set[str]) -> dict:
    out = {}
    for src, src_conns in (connections or {}).items():
        if src not in keep_nodes:
            continue
        if not isinstance(src_conns, dict):
            out[src] = src_conns
            continue
        new_src_conns = {}
        for ctype, slots in src_conns.items():
            if not isinstance(slots, list):
                new_src_conns[ctype] = slots
                continue
            new_slots = []
            for targets in slots:
                if not isinstance(targets, list):
                    new_slots.append(targets)
                    continue
                kept_targets = [t for t in targets if isinstance(t, dict) and t.get("node") in keep_nodes]
                new_slots.append(kept_targets)
            new_src_conns[ctype] = new_slots
        out[src] = new_src_conns
    return out


def ensure_main_connection(connections: dict, src: str, target_node: str, target_index: int) -> None:
    src_conns = connections.setdefault(src, {})
    main = src_conns.setdefault("main", [])
    if not isinstance(main, list):
        src_conns["main"] = main = []
    while len(main) == 0:
        main.append([])
    if not isinstance(main[0], list):
        main[0] = []

    # Remove any existing connection to the same target to keep output deterministic.
    main[0] = [t for t in main[0] if not (isinstance(t, dict) and t.get("node") == target_node and t.get("type") == "main")]
    main[0].append({"node": target_node, "type": "main", "index": target_index})


def validate_workflow(workflow: dict, expected_agent: str, expected_extractor: str, expected_db_path: str):
    nodes = workflow.get("nodes", [])
    node_names = {n.get("name") for n in nodes}
    connections = workflow.get("connections", {})

    # All connection sources/targets exist.
    dangling = []
    for src, src_conns in (connections or {}).items():
        if src not in node_names:
            dangling.append(("missing_source", src))
        if not isinstance(src_conns, dict):
            continue
        for slots in src_conns.values():
            if not isinstance(slots, list):
                continue
            for targets in slots:
                if not isinstance(targets, list):
                    continue
                for t in targets:
                    if isinstance(t, dict) and t.get("node") not in node_names:
                        dangling.append(("missing_target", src, t.get("node")))

    if dangling:
        raise ValueError(f"Dangling connections detected: {dangling[:10]}")

    ag1_out = (((connections.get("AG1.00 — Assemble Input Packs") or {}).get("main") or [[[]]])[0])
    if not any(t.get("node") == expected_agent for t in ag1_out if isinstance(t, dict)):
        raise ValueError(f"AG1.00 is not connected to expected agent '{expected_agent}'")

    extractor_out = (((connections.get(expected_extractor) or {}).get("main") or [[[]]])[0])
    if not any(t.get("node") == "merge" for t in extractor_out if isinstance(t, dict)):
        raise ValueError(f"Extractor '{expected_extractor}' is not connected to 'merge'")

    serialized = json.dumps(workflow, ensure_ascii=False)
    if expected_db_path not in serialized:
        raise ValueError(f"Expected DB path not found: {expected_db_path}")
    if "/files/duckdb/ag1_v3.duckdb" in serialized or "/local-files/duckdb/ag1_v3.duckdb" in serialized:
        raise ValueError("Default AG1 DB path still present after replacement")


def build_variant(base_workflow: dict, key: str, cfg: dict) -> dict:
    keep_nodes = set(COMMON_NODES) | set(cfg["branch_nodes"])
    wf = copy.deepcopy(base_workflow)
    wf["nodes"] = [n for n in wf.get("nodes", []) if n.get("name") in keep_nodes]
    wf["connections"] = filter_connections(wf.get("connections", {}), keep_nodes)

    # Wire the selected branch into the main flow.
    ensure_main_connection(wf["connections"], "AG1.00 — Assemble Input Packs", cfg["agent_node"], 0)
    ensure_main_connection(wf["connections"], cfg["extractor_node"], "merge", 1)

    # Workflow identity/name.
    wf["name"] = cfg["workflow_name"]
    wf["id"] = random_n8n_id()
    wf["versionId"] = str(uuid.uuid4())

    # Keep only pinData entries for retained nodes.
    if isinstance(wf.get("pinData"), dict):
        wf["pinData"] = {k: v for k, v in wf["pinData"].items() if k in keep_nodes}

    # Replace AG1 DB default paths everywhere in the workflow export.
    wf = deep_replace_ag1_path(wf, cfg["db_path"])
    patch_init_run_context_defaults(wf, cfg)
    patch_start_trigger_schedule(wf)
    patch_workflow_timezone(wf)
    apply_layout_overrides(wf, cfg)

    validate_workflow(wf, cfg["agent_node"], cfg["extractor_node"], cfg["db_path"])
    return wf


def main():
    if not SOURCE_WORKFLOW.exists():
        raise FileNotFoundError(f"Source workflow not found: {SOURCE_WORKFLOW}")

    base_workflow = json.loads(SOURCE_WORKFLOW.read_text(encoding="utf-8"))
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for key, cfg in VARIANTS.items():
        wf = build_variant(base_workflow, key, cfg)
        out_path = OUTPUT_DIR / f"AG1_workflow_v3__{key}.json"
        out_path.write_text(json.dumps(wf, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[OK] {out_path.name} | nodes={len(wf.get('nodes', []))} | db={cfg['db_path']}")


if __name__ == "__main__":
    main()
