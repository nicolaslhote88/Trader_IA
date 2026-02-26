import copy
import json
import random
import string
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SOURCE_WORKFLOW = ROOT / "AG1_workflow_general.json"
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
}


VARIANTS = {
    "chatgpt52": {
        "workflow_name": "AG1 - Workflow général - ChatGPT 5.2",
        "db_path": "/files/duckdb/ag1_v2_chatgpt52.duckdb",
        "run_defaults": {
            "strategy_version": "strategy_v2",
            "config_version": "config_v2",
            "model": "gpt-5.2-2025-12-11",
        },
        "agent_node": "Agent #1 - Portfolio manager",
        "extractor_node": "Information Extractor",
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
        "db_path": "/files/duckdb/ag1_v2_grok41_reasoning.duckdb",
        "run_defaults": {
            "strategy_version": "strategy_v2",
            "config_version": "config_v2",
            "model": "grok-4-1-fast-reasoning",
        },
        "agent_node": "Agent #1 - Portfolio manager1",
        "extractor_node": "Information Extractor1",
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
        "db_path": "/files/duckdb/ag1_v2_gemini30_pro.duckdb",
        "run_defaults": {
            "strategy_version": "strategy_v2",
            "config_version": "config_v2",
            "model": "models/gemini-3-pro-preview",
        },
        "agent_node": "Agent #1 - Portfolio manager2",
        "extractor_node": "Information Extractor2",
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


OLD_PATHS = (
    "/files/duckdb/ag1_v2.duckdb",
    "/local-files/duckdb/ag1_v2.duckdb",
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

    replacements = [
        (
            'strategyVersion: String(cfg.strategy_version || "strategy_v1")',
            f'strategyVersion: String(cfg.strategy_version || "{run_defaults.get("strategy_version", "strategy_v1")}")',
        ),
        (
            'configVersion: String(cfg.config_version || "config_v2")',
            f'configVersion: String(cfg.config_version || "{run_defaults.get("config_version", "config_v2")}")',
        ),
        (
            'model: String(cfg.model || "gpt-5.2")',
            f'model: String(cfg.model || "{run_defaults.get("model", "gpt-5.2")}")',
        ),
    ]

    for before, after in replacements:
        if before not in js_code:
            raise ValueError(f"Init Run Context patch failed, snippet not found: {before}")
        js_code = js_code.replace(before, after, 1)

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
    if "/files/duckdb/ag1_v2.duckdb" in serialized or "/local-files/duckdb/ag1_v2.duckdb" in serialized:
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

    validate_workflow(wf, cfg["agent_node"], cfg["extractor_node"], cfg["db_path"])
    return wf


def main():
    if not SOURCE_WORKFLOW.exists():
        raise FileNotFoundError(f"Source workflow not found: {SOURCE_WORKFLOW}")

    base_workflow = json.loads(SOURCE_WORKFLOW.read_text(encoding="utf-8"))
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for key, cfg in VARIANTS.items():
        wf = build_variant(base_workflow, key, cfg)
        out_path = OUTPUT_DIR / f"AG1_workflow_general__{key}.json"
        out_path.write_text(json.dumps(wf, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[OK] {out_path.name} | nodes={len(wf.get('nodes', []))} | db={cfg['db_path']}")


if __name__ == "__main__":
    main()
