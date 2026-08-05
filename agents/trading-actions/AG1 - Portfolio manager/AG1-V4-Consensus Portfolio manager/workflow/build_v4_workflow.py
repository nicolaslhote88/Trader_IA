from __future__ import annotations

import argparse
import copy
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parent
TEMPLATE_PATH = ROOT / "AG1_workflow_template_v4.json"
OUTPUT_PATH = ROOT / "AG1_workflow_v4_consensus.json"
SHADOW_OUTPUT_PATH = ROOT / "AG1_workflow_v4_global_context_shadow.json"


MODEL_BRANCHES = {
    "chatgpt52": {
        "agent": "Agent #1 - Portfolio manager",
        "extractor": "Information Extractor",
        "parser": "AG1.V4 — Structured Output GPT",
        "model_node": "OpenAI Chat Model - GPT5.6sol",
        "model_name": "OpenAI GPT-5.6 Sol",
        "model_id": "gpt-5.6-sol",
        "merge_input": 1,
    },
    "grok41_reasoning": {
        "agent": "Agent #1 - Portfolio manager1",
        "extractor": "Information Extractor1",
        "parser": "AG1.V4 — Structured Output DeepSeek",
        "model_node": "DeepSeek Chat Model",
        "model_name": "DeepSeek V4 Pro",
        "model_id": "deepseek-v4-pro",
        "merge_input": 2,
    },
    "claude_sonnet46": {
        "agent": "Agent #1 - Portfolio manager2",
        "extractor": "Information Extractor2",
        "parser": "AG1.V4 — Structured Output Claude",
        "model_node": "Anthropic Chat Model",
        "model_name": "Anthropic Claude Opus 4.8",
        "model_id": "claude-opus-4-8",
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
    "AG1.GC — Attach Advisory Pack": ("jsCode", ROOT / "nodes/agent_input/ag1_gc_attach_advisory_pack.code.js"),
    "AG1.V4 — Liquidity Preflight": ("jsCode", ROOT / "nodes/pre_agent/ag1_v4_liquidity_preflight.code.js"),
    "AG1.V4 — Build Consensus": ("jsCode", ROOT / "nodes/post_agent/06_build_consensus_v4.code.js"),
    "7 - Validate & Enforce Safety": ("jsCode", ROOT / "nodes/post_agent/07_validate_enforce_safety_v5.code.js"),
    "07b - IBKR Send Orders": ("jsCode", ROOT / "nodes/post_agent/07b_ibkr_send_orders.js"),
    "8 - Build DuckDB Bundle": ("jsCode", ROOT / "nodes/post_agent/08_build_duckdb_bundle.code.js"),
    "9 - Upsert Run Bundle (DuckDB)": ("pythonCode", ROOT / "nodes/post_agent/09_upsert_run_bundle_duckdb.code.py"),
    "10 - Post-Run Health (DuckDB)": ("pythonCode", ROOT / "nodes/post_agent/10_post_run_health.code.py"),
}

CLAUDE_OUTPUT_CONTRACT_SUFFIX = """

CONTRAT DE SORTIE — IMPERATIF (lecture obligatoire)
Tu es Claude Opus 4.8 : on attend une sortie strictement conforme, du premier coup.
Retourne EXACTEMENT UN objet JSON unique, sans aucun texte, sans Markdown, sans ``` avant ou apres.
L'objet contient TOUJOURS ces 6 cles, AUCUNE ne peut etre omise (meme vides) :
  "marketRegime"      : une valeur parmi RISK_ON | RISK_OFF | ROTATION | NEUTRAL
  "targetExposurePct" : nombre 0-100 (ou null)
  "maxNewPositions"   : entier 0-15 (ou null)
  "actions"           : TABLEAU, TOUJOURS PRESENT. Si tu ne proposes aucune action, renvoie un tableau VIDE []. Ne JAMAIS omettre cette cle.
  "riskNotes"         : tableau de chaines (mettre [] si rien)
  "dataCaveats"       : tableau de chaines (mettre [] si rien)
Decider de ne rien faire (NO_TRADE) est valide et frequent : dans ce cas renvoie l'objet COMPLET avec "actions": [] et explique en riskNotes.
Interdits absolus : prose hors JSON, objet partiel, cle manquante, valeur d'enum hors liste. Le JSON doit etre parsable sans correction.
"""

DEEPSEEK_OUTPUT_CONTRACT_SUFFIX = """

CONTRAT DE SORTIE DEEPSEEK — IMPERATIF
Retourne EXACTEMENT UN objet JSON unique conforme au schema fourni, sans
Markdown, sans bloc de code, sans second objet et sans texte avant ou apres.
Les six cles marketRegime, targetExposurePct, maxNewPositions, actions,
riskNotes et dataCaveats sont toujours presentes. Si aucune action n'est
proposee, actions vaut [] et les autres tableaux vides valent [].
"""


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def get_node(workflow: Dict[str, Any], name: str) -> Dict[str, Any]:
    for node in workflow["nodes"]:
        if node.get("name") == name:
            return node
    raise KeyError(f"Node not found: {name}")


def has_node(workflow: Dict[str, Any], name: str) -> bool:
    return any(node.get("name") == name for node in workflow["nodes"])


def get_first_node(workflow: Dict[str, Any], names: Iterable[str]) -> Dict[str, Any]:
    for name in names:
        if has_node(workflow, name):
            return get_node(workflow, name)
    raise KeyError(f"None of the nodes exist: {list(names)}")


def rename_node(workflow: Dict[str, Any], old_name: str, new_name: str) -> None:
    if old_name == new_name or not has_node(workflow, old_name):
        return
    if has_node(workflow, new_name):
        raise ValueError(f"Cannot rename {old_name}: {new_name} already exists")
    get_node(workflow, old_name)["name"] = new_name
    connections = workflow.setdefault("connections", {})
    if old_name in connections:
        connections[new_name] = connections.pop(old_name)
    for source_connections in connections.values():
        for outputs in source_connections.values():
            for output in outputs:
                for target in output:
                    if target.get("node") == old_name:
                        target["node"] = new_name


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
            .replace("__MODEL_ID__", branch["model_id"])
        )
        node.setdefault("parameters", {})["jsCode"] = code


def patch_agent_prompts(workflow: Dict[str, Any]) -> None:
    source = load_json(ROOT / "nodes/agent_input/agent_1_portfolio_manager.node.json")
    for model_key, branch in MODEL_BRANCHES.items():
        node = get_node(workflow, branch["agent"])
        node["parameters"] = copy.deepcopy(source["parameters"])
        if model_key == "claude_sonnet46":
            options = node["parameters"].setdefault("options", {})
            options["systemMessage"] = (str(options.get("systemMessage", "")).rstrip() + CLAUDE_OUTPUT_CONTRACT_SUFFIX).rstrip()
        if model_key == "grok41_reasoning":
            # The LangChain Agent binds the structured parser as a tool. DeepSeek
            # can occasionally concatenate tool arguments, which fails before
            # the extractor sees the model text. A Basic LLM Chain keeps the
            # same prompt/schema without the agent tool-call envelope.
            options = node["parameters"].pop("options", {})
            system_message = str(options.get("systemMessage", "")).rstrip()
            node["parameters"]["messages"] = {
                "messageValues": [{
                    "message": (system_message + DEEPSEEK_OUTPUT_CONTRACT_SUFFIX).rstrip(),
                }]
            }
            node["type"] = "@n8n/n8n-nodes-langchain.chainLlm"
            node["typeVersion"] = 1.5
            node["retryOnFail"] = True
            node["maxTries"] = 2
            node["waitBetweenTries"] = 2000
        if branch.get("use_output_parser") is False:
            node["parameters"].pop("hasOutputParser", None)
        node["onError"] = "continueRegularOutput"


def add_input_and_parser_nodes(workflow: Dict[str, Any]) -> None:
    if not has_node(workflow, "AG1.V4 — Liquidity Preflight"):
        workflow["nodes"].append({
            "parameters": {"jsCode": read_code(ROOT / "nodes/pre_agent/ag1_v4_liquidity_preflight.code.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [2496, 11584],
            "id": "ag1-v4-liquidity-preflight",
            "name": "AG1.V4 — Liquidity Preflight",
        })
    schema = json.dumps(load_json(ROOT / "nodes/agent_input/portfolio_manager_output_schema.json"), ensure_ascii=False)
    for idx, branch in enumerate(MODEL_BRANCHES.values()):
        if branch.get("use_output_parser") is False:
            continue
        if not has_node(workflow, branch["parser"]):
            workflow["nodes"].append({
                "parameters": {
                    "schemaType": "manual",
                    "inputSchema": schema,
                    "autoFix": False,
                },
                "type": "@n8n/n8n-nodes-langchain.outputParserStructured",
                "typeVersion": 1.3,
                "position": [3264, 10240 + idx * 1248],
                "id": f"ag1-v4-output-parser-{idx + 1}",
                "name": branch["parser"],
            })
        else:
            node = get_node(workflow, branch["parser"])
            node.setdefault("parameters", {})["inputSchema"] = schema


def add_global_context_nodes(workflow: Dict[str, Any]) -> None:
    nodes = [
        {
            "parameters": {
                "method": "POST",
                "url": "http://global-context-synthesizer:8083/ag1-pack",
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": "={{ { portfolio: $json.portfolio_pack?.positions || [], opportunities: $json.opportunity_pack?.rows || [] } }}",
                "options": {"timeout": 30000},
            },
            "type": "n8n-nodes-base.httpRequest",
            "typeVersion": 4,
            "position": [2336, 11440],
            "id": "ag1-gc-fetch-advisory-pack",
            "name": "AG1.GC — Fetch Advisory Pack",
            "retryOnFail": True,
            "maxTries": 2,
            "waitBetweenTries": 1000,
            "onError": "continueRegularOutput",
        },
        {
            "parameters": {"mode": "append", "numberInputs": 2},
            "type": "n8n-nodes-base.merge",
            "typeVersion": 3.2,
            "position": [2560, 11584],
            "id": "ag1-gc-merge-advisory-pack",
            "name": "AG1.GC — Merge Advisory Pack",
        },
        {
            "parameters": {"jsCode": read_code(ROOT / "nodes/agent_input/ag1_gc_attach_advisory_pack.code.js")},
            "type": "n8n-nodes-base.code",
            "typeVersion": 2,
            "position": [2752, 11584],
            "id": "ag1-gc-attach-advisory-pack",
            "name": "AG1.GC — Attach Advisory Pack",
        },
    ]
    for payload in nodes:
        if not has_node(workflow, payload["name"]):
            workflow["nodes"].append(payload)


def add_anthropic_model_node(workflow: Dict[str, Any]) -> None:
    node_payload = {
        "parameters": {
            "model": {
                "__rl": True,
                "value": "claude-opus-4-8",
                "mode": "list",
                "cachedResultName": "Claude Opus 4.8",
            },
            "options": {"thinking": False},
        },
        "type": "@n8n/n8n-nodes-langchain.lmChatAnthropic",
        "typeVersion": 1.3,
        "position": [2816, 10768],
        "id": "ag1-v4-claude-sonnet46-model",
        "name": "Anthropic Chat Model",
        "credentials": {
            "anthropicApi": {
                "id": "99IcBcafnZl2jhHG",
                "name": "Anthropic account 2",
            }
        },
    }
    try:
        existing = get_node(workflow, "Anthropic Chat Model")
        existing.clear()
        existing.update(node_payload)
    except KeyError:
        workflow["nodes"].append(node_payload)


def patch_model_options(workflow: Dict[str, Any]) -> None:
    rename_node(workflow, "OpenAI Chat Model - GPT5.2", "OpenAI Chat Model - GPT5.6sol")
    openai = get_node(workflow, "OpenAI Chat Model - GPT5.6sol")
    openai["parameters"] = {
        "model": {
            "__rl": True,
            "value": "gpt-5.6-sol",
            "mode": "list",
            "cachedResultName": "gpt-5.6-sol",
        },
        "builtInTools": {},
        "options": {"reasoningEffort": "medium", "timeout": 1500000},
    }

    if has_node(workflow, "xAI Grok Chat Model"):
        rename_node(workflow, "xAI Grok Chat Model", "DeepSeek Chat Model")
    deepseek = get_node(workflow, "DeepSeek Chat Model")
    deepseek_position = deepseek.get("position", [2816, 11216])
    deepseek.clear()
    deepseek.update({
        "parameters": {"model": "deepseek-v4-pro", "options": {}},
        "type": "@n8n/n8n-nodes-langchain.lmChatDeepSeek",
        "typeVersion": 1,
        "position": deepseek_position,
        "id": "d7ebdbd4-83e3-4bf0-94cf-31cd8d8437eb",
        "name": "DeepSeek Chat Model",
        "credentials": {
            "deepSeekApi": {
                "id": "BlSCC28mzKodkfO5",
                "name": "DeepSeek account",
            }
        },
    })

    rename_node(workflow, "AG1.V4 — Structured Output Grok", "AG1.V4 — Structured Output DeepSeek")


def patch_sticky_notes(workflow: Dict[str, Any]) -> None:
    for node in workflow["nodes"]:
        params = node.get("parameters") or {}
        content = str(params.get("content") or "").strip().upper()
        if content == "## GEMINI":
            params["content"] = "## CLAUDE"
        elif content in {"## GROK", "## DEEPSEEK V4"}:
            params["content"] = "## DEEPSEEK V4 PRO"


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
    if not has_node(workflow, "AG1.V4 — Merge Model Proposals"):
        workflow["nodes"].append(merge_node)
    if not has_node(workflow, "AG1.V4 — Build Consensus"):
        workflow["nodes"].append(consensus_node)


def patch_positions(workflow: Dict[str, Any]) -> None:
    if os.getenv("AG1_V4_APPLY_CANONICAL_POSITIONS", "").strip().lower() not in {"1", "true", "yes"}:
        return
    positions = {
        "AG1.00 — Assemble Input Packs": [2240, 11584],
        "AG1.GC — Fetch Advisory Pack": [2464, 11440],
        "AG1.GC — Merge Advisory Pack": [2656, 11584],
        "AG1.GC — Attach Advisory Pack": [2848, 11584],
        "AG1.V4 — Liquidity Preflight": [3040, 11584],
        "Anthropic Chat Model": [3264, 9664],
        "AG1.V4 — Structured Output Claude": [3264, 10176],
        "Agent #1 - Portfolio manager2": [2928, 9920],
        "Information Extractor2": [3712, 9952],
        "Agent #1 - Portfolio manager1": [2928, 11168],
        "DeepSeek Chat Model": [3264, 10912],
        "AG1.V4 — Structured Output DeepSeek": [3264, 11424],
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
        "AG1.V4 — Liquidity Preflight",
        [
            ("AG1.V4 — Merge Model Proposals", 0),
            (MODEL_BRANCHES["chatgpt52"]["agent"], 0),
            (MODEL_BRANCHES["grok41_reasoning"]["agent"], 0),
            (MODEL_BRANCHES["claude_sonnet46"]["agent"], 0),
        ],
    )
    set_main_connections(
        workflow,
        "AG1.00 — Assemble Input Packs",
        [("AG1.GC — Merge Advisory Pack", 0), ("AG1.GC — Fetch Advisory Pack", 0)],
    )
    set_main_connections(workflow, "AG1.GC — Fetch Advisory Pack", [("AG1.GC — Merge Advisory Pack", 1)])
    set_main_connections(workflow, "AG1.GC — Merge Advisory Pack", [("AG1.GC — Attach Advisory Pack", 0)])
    set_main_connections(workflow, "AG1.GC — Attach Advisory Pack", [("AG1.V4 — Liquidity Preflight", 0)])
    for branch in MODEL_BRANCHES.values():
        set_main_connections(workflow, branch["agent"], [(branch["extractor"], 0)])
        set_main_connections(workflow, branch["extractor"], [("AG1.V4 — Merge Model Proposals", branch["merge_input"])])
        if branch.get("model_node"):
            workflow.setdefault("connections", {})[branch["model_node"]] = {
                "ai_languageModel": [[{"node": branch["agent"], "type": "ai_languageModel", "index": 0}]]
            }
        if branch.get("use_output_parser") is not False:
            workflow.setdefault("connections", {})[branch["parser"]] = {
                "ai_outputParser": [[{"node": branch["agent"], "type": "ai_outputParser", "index": 0}]]
            }

    set_main_connections(workflow, "AG1.V4 — Merge Model Proposals", [("AG1.V4 — Build Consensus", 0)])
    set_main_connections(workflow, "AG1.V4 — Build Consensus", [("7 - Validate & Enforce Safety", 0)])
    set_main_connections(workflow, "7 - Validate & Enforce Safety", [("07b - IBKR Send Orders", 0)])
    set_main_connections(workflow, "07b - IBKR Send Orders", [("8 - Build DuckDB Bundle", 0)])
    set_main_connections(workflow, "8 - Build DuckDB Bundle", [("9 - Upsert Run Bundle (DuckDB)", 0)])
    set_main_connections(workflow, "9 - Upsert Run Bundle (DuckDB)", [("10 - Post-Run Health (DuckDB)", 0)])


def build(source_path: Path) -> Dict[str, Any]:
    workflow = load_json(source_path)
    if isinstance(workflow, list):
        if len(workflow) != 1:
            raise ValueError("Expected exactly one workflow")
        workflow = workflow[0]
    workflow = copy.deepcopy(workflow)
    workflow["name"] = "AG1 V4 - Consensus Portfolio Manager"
    workflow["id"] = "AG1V4CONSENSUS"
    workflow["active"] = False
    workflow.pop("versionId", None)
    workflow.pop("activeVersionId", None)

    remove_nodes(workflow, [
        "merge",
        "0 - SEED Portfolio",
        "news_web_x_scan (Grok)",
        "news_web_x_scan (Grok)1",
        "news_web_x_scan (Grok)2",
        "OpenAI Chat Model8",
        "OpenAI Chat Model14",
        "xAI Grok Chat Model1",
        "Google Gemini Chat Model",
    ])
    patch_model_options(workflow)
    add_input_and_parser_nodes(workflow)
    add_global_context_nodes(workflow)
    add_anthropic_model_node(workflow)
    add_consensus_nodes(workflow)
    patch_code_nodes(workflow)
    patch_agent_prompts(workflow)
    patch_sticky_notes(workflow)
    patch_positions(workflow)
    patch_connections(workflow)
    return workflow


def make_shadow(active_candidate: Dict[str, Any]) -> Dict[str, Any]:
    workflow = copy.deepcopy(active_candidate)
    workflow["id"] = "AG1V4GLOBALCONTEXTSHADOW20260805"
    workflow["name"] = "AG1 V4 — Global Context Shadow (NO BROKER)"
    workflow["active"] = False
    schedule_names = [node.get("name") for node in workflow["nodes"] if node.get("type") == "n8n-nodes-base.scheduleTrigger"]
    remove_nodes(workflow, schedule_names + [
        "AG1.V4 — Liquidity Preflight",
        "7 - Validate & Enforce Safety",
        "07b - IBKR Send Orders",
        "8 - Build DuckDB Bundle",
        "9 - Upsert Run Bundle (DuckDB)",
        "10 - Post-Run Health (DuckDB)",
    ])
    workflow["nodes"].append({
        "parameters": {"jsCode": read_code(ROOT / "nodes/agent_input/ag1_gc_shadow_capture.code.js")},
        "type": "n8n-nodes-base.code",
        "typeVersion": 2,
        "position": [4560, 11584],
        "id": "ag1-gc-shadow-capture",
        "name": "AG1.GC — Shadow Capture (NO BROKER)",
    })
    set_main_connections(
        workflow,
        "AG1.GC — Attach Advisory Pack",
        [
            ("AG1.V4 — Merge Model Proposals", 0),
            (MODEL_BRANCHES["chatgpt52"]["agent"], 0),
            (MODEL_BRANCHES["grok41_reasoning"]["agent"], 0),
            (MODEL_BRANCHES["claude_sonnet46"]["agent"], 0),
        ],
    )
    set_main_connections(workflow, "AG1.V4 — Build Consensus", [("AG1.GC — Shadow Capture (NO BROKER)", 0)])
    forbidden = [node.get("name") for node in workflow["nodes"] if "IBKR Send Orders" in str(node.get("name")) or "DuckDB" in str(node.get("name"))]
    if forbidden:
        raise ValueError(f"Shadow contains forbidden nodes: {forbidden}")
    return workflow


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=TEMPLATE_PATH)
    args = parser.parse_args()
    workflow = build(args.source)
    write_json(OUTPUT_PATH, workflow)
    print(f"Wrote {OUTPUT_PATH}")
    shadow = make_shadow(workflow)
    write_json(SHADOW_OUTPUT_PATH, shadow)
    print(f"Wrote {SHADOW_OUTPUT_PATH}")


if __name__ == "__main__":
    main()
