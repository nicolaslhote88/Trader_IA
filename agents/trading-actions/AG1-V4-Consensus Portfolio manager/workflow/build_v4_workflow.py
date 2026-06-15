from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parent
TEMPLATE_PATH = ROOT / "AG1_workflow_template_v4.json"
OUTPUT_PATH = ROOT / "AG1_workflow_v4_consensus.json"


MODEL_BRANCHES = {
    "chatgpt52": {
        "agent": "Agent #1 - Portfolio manager",
        "extractor": "Information Extractor",
        "model_name": "OpenAI GPT-5.2",
        "merge_input": 1,
    },
    "grok41_reasoning": {
        "agent": "Agent #1 - Portfolio manager1",
        "extractor": "Information Extractor1",
        "model_name": "xAI Grok 4.1 Reasoning",
        "merge_input": 2,
    },
    "gemini30_pro": {
        "agent": "Agent #1 - Portfolio manager2",
        "extractor": "Information Extractor2",
        "model_name": "Google Gemini 3.0 Pro",
        "merge_input": 3,
    },
}


CODE_MAP = {
    "2B - Init Run Context": ("jsCode", ROOT / "nodes/pre_agent/2B_init_run_context.code.js"),
    "4B – Build Portfolio Context": ("pythonCode", ROOT / "nodes/pre_agent/4B_build_portfolio_context.code.py"),
    "4C — Enrich Portfolio with Market Prices": ("pythonCode", ROOT / "nodes/pre_agent/4C_enrich_portfolio_with_market_prices.code.py"),
    "AG4.01 - Récupération des news générales": ("pythonCode", ROOT / "nodes/pre_agent/AG4_01_fetch_macro_news.code.py"),
    "20J_FINAL — Build MarketNewsPack Final": ("pythonCode", ROOT / "nodes/pre_agent/20J_final_build_market_news_pack.code.py"),
    "R8 — Data Prep for Matrix (Fusion Filter)": ("pythonCode", ROOT / "nodes/pre_agent/R8_data_prep_matrix.code.py"),
    "Calcul Matrice & Briefing": ("pythonCode", ROOT / "nodes/pre_agent/calcul_matrice_briefing.code.py"),
    "AG1.00 — Assemble Input Packs": ("jsCode", ROOT / "nodes/agent_input/ag1_00_assemble_input_packs.code.js"),
    "7 - Validate & Enforce Safety": ("jsCode", ROOT / "nodes/post_agent/07_validate_enforce_safety_v5.code.js"),
    "07b - IBKR Send Orders": ("jsCode", ROOT / "nodes/post_agent/07b_ibkr_send_orders.js"),
    "8 - Build DuckDB Bundle": ("jsCode", ROOT / "nodes/post_agent/08_build_duckdb_bundle.code.js"),
    "9 - Upsert Run Bundle (DuckDB)": ("pythonCode", ROOT / "nodes/post_agent/09_upsert_run_bundle_duckdb.code.py"),
    "10 - Post-Run Health (DuckDB)": ("pythonCode", ROOT / "nodes/post_agent/10_post_run_health.code.py"),
}


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def get_node(workflow: Dict[str, Any], name: str) -> Dict[str, Any]:
    for node in workflow["nodes"]:
        if node.get("name") == name:
            return node
    raise KeyError(f"Node not found: {name}")


def remove_nodes(workflow: Dict[str, Any], names: Iterable[str]) -> None:
    names_set = set(names)
    workflow["nodes"] = [node for node in workflow["nodes"] if node.get("name") not in names_set]
    for name in list(workflow.get("connections", {}).keys()):
        if name in names_set:
            workflow["connections"].pop(name, None)
            continue
        for conn_type, outputs in list(workflow["connections"][name].items()):
            for output in outputs:
                output[:] = [target for target in output if target.get("node") not in names_set]
            workflow["connections"][name][conn_type] = outputs


def set_main_connections(workflow: Dict[str, Any], source: str, targets: List[Tuple[str, int]]) -> None:
    conn = workflow.setdefault("connections", {}).setdefault(source, {})
    conn["main"] = [[{"node": node_name, "type": "main", "index": input_index} for node_name, input_index in targets]]


def read_code(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def patch_code_nodes(workflow: Dict[str, Any]) -> None:
    for name, (param_key, path) in CODE_MAP.items():
        node = get_node(workflow, name)
        node.setdefault("parameters", {})[param_key] = read_code(path)
        if param_key == "pythonCode":
            node["parameters"]["language"] = "pythonNative"

    extractor_template = read_code(ROOT / "nodes/agent_input/information_extractor_v4.code.js")
    for model_key, branch in MODEL_BRANCHES.items():
        node = get_node(workflow, branch["extractor"])
        code = (
            extractor_template
            .replace("__MODEL_KEY__", model_key)
            .replace("__MODEL_NAME__", branch["model_name"])
        )
        node.setdefault("parameters", {})["jsCode"] = code


def patch_agent_prompts(workflow: Dict[str, Any]) -> None:
    intro = (
        "MODE AG1 V4 CONSENSUS\n"
        "Ta sortie est une proposition indépendante. Elle sera comparée aux deux autres modèles; "
        "aucun ordre ne partira sans consensus 2/3 puis validation risk manager.\n\n"
    )
    for branch in MODEL_BRANCHES.values():
        node = get_node(workflow, branch["agent"])
        text = str(node.get("parameters", {}).get("text", ""))
        text = text[1:] if text.startswith("=") else text
        if "MODE AG1 V4 CONSENSUS" not in text:
            text = intro + text
        text = text.replace("\n=Tu dois produire", "\nTu dois produire")
        node.setdefault("parameters", {})["text"] = "=" + text


def add_consensus_nodes(workflow: Dict[str, Any]) -> None:
    merge_node = {
        "parameters": {"numberInputs": 4},
        "type": "n8n-nodes-base.merge",
        "typeVersion": 3.2,
        "position": [4048, 11584],
        "id": "ag1-v4-merge-model-proposals",
        "name": "AG1.V4 — Merge Model Proposals",
    }
    consensus_node = {
        "parameters": {"jsCode": read_code(ROOT / "nodes/post_agent/06_build_consensus_v4.code.js")},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [4304, 11584],
        "id": "ag1-v4-build-consensus",
        "name": "AG1.V4 — Build Consensus",
    }
    workflow["nodes"].append(merge_node)
    workflow["nodes"].append(consensus_node)


def patch_positions(workflow: Dict[str, Any]) -> None:
    positions = {
        "AG1.00 — Assemble Input Packs": [2240, 11584],
        "Agent #1 - Portfolio manager2": [2928, 9920],
        "Information Extractor2": [3712, 9952],
        "Agent #1 - Portfolio manager1": [2928, 11168],
        "Information Extractor1": [3712, 11200],
        "Agent #1 - Portfolio manager": [2928, 12416],
        "Information Extractor": [3712, 12448],
        "7 - Validate & Enforce Safety": [4560, 11584],
        "07b - IBKR Send Orders": [4800, 11584],
        "8 - Build DuckDB Bundle": [5040, 11584],
        "9 - Upsert Run Bundle (DuckDB)": [5264, 11584],
        "10 - Post-Run Health (DuckDB)": [5456, 11584],
    }
    for name, pos in positions.items():
        try:
            get_node(workflow, name)["position"] = pos
        except KeyError:
            pass


def patch_connections(workflow: Dict[str, Any]) -> None:
    # 4B no longer merges directly with a single model output. It still feeds market-price enrichment.
    set_main_connections(workflow, "4B – Build Portfolio Context", [("4C — Enrich Portfolio with Market Prices", 0)])

    set_main_connections(
        workflow,
        "AG1.00 — Assemble Input Packs",
        [
            ("AG1.V4 — Merge Model Proposals", 0),
            (MODEL_BRANCHES["chatgpt52"]["agent"], 0),
            (MODEL_BRANCHES["grok41_reasoning"]["agent"], 0),
            (MODEL_BRANCHES["gemini30_pro"]["agent"], 0),
        ],
    )
    for branch in MODEL_BRANCHES.values():
        set_main_connections(workflow, branch["agent"], [(branch["extractor"], 0)])
        set_main_connections(workflow, branch["extractor"], [("AG1.V4 — Merge Model Proposals", branch["merge_input"])])

    set_main_connections(workflow, "AG1.V4 — Merge Model Proposals", [("AG1.V4 — Build Consensus", 0)])
    set_main_connections(workflow, "AG1.V4 — Build Consensus", [("7 - Validate & Enforce Safety", 0)])
    set_main_connections(workflow, "7 - Validate & Enforce Safety", [("07b - IBKR Send Orders", 0)])
    set_main_connections(workflow, "07b - IBKR Send Orders", [("8 - Build DuckDB Bundle", 0)])
    set_main_connections(workflow, "8 - Build DuckDB Bundle", [("9 - Upsert Run Bundle (DuckDB)", 0)])
    set_main_connections(workflow, "9 - Upsert Run Bundle (DuckDB)", [("10 - Post-Run Health (DuckDB)", 0)])


def main() -> None:
    workflow = load_json(TEMPLATE_PATH)
    workflow = copy.deepcopy(workflow)
    workflow["name"] = "AG1 V4 - Consensus Portfolio Manager"
    workflow["id"] = "AG1V4CONSENSUS"
    workflow["active"] = False
    workflow.pop("versionId", None)

    remove_nodes(workflow, ["merge", "0 - SEED Portfolio"])
    add_consensus_nodes(workflow)
    patch_code_nodes(workflow)
    patch_agent_prompts(workflow)
    patch_positions(workflow)
    patch_connections(workflow)
    write_json(OUTPUT_PATH, workflow)
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
