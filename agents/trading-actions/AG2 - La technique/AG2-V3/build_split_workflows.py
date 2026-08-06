import copy
import json
import re
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BASE = ROOT / "AG2-V3-Technical-Watchlist-Nightly.workflow.json"
READ_UNIVERSE_CODE = (ROOT / "nodes" / "00_read_universe.py").read_text(encoding="utf-8")
INIT_CODE = (ROOT / "nodes" / "01_init_config.js").read_text(encoding="utf-8")
DUCKDB_CODE = (ROOT / "nodes" / "02_duckdb_init.py").read_text(encoding="utf-8")
NODE_CODE_FILES = {
    "Read Universe": ("pythonCode", ROOT / "nodes" / "00_read_universe.py"),
    "AG2.11 - Code - Wrap H1": ("jsCode", ROOT / "nodes" / "03a_wrap_h1.js"),
    "AG2.16 - Code - Wrap D1": ("jsCode", ROOT / "nodes" / "03b_wrap_d1.js"),
    "Compute + Filter + Write": ("pythonCode", ROOT / "nodes" / "04_compute.py"),
    "Snapshot Context": ("jsCode", ROOT / "nodes" / "05_snapshot.js"),
    "Merge AI + Context": ("jsCode", ROOT / "nodes" / "06a_merge_ai.js"),
    "Extract AI + Write": ("pythonCode", ROOT / "nodes" / "06_extract_ai.py"),
    "Hydrate AI from cache": ("pythonCode", ROOT / "nodes" / "07_hydrate_ai_cache.py"),
    "Finalize Run": ("pythonCode", ROOT / "nodes" / "10_finalize.py"),
}

VARIANTS = [
    {
        "id": "AG2V3HELDCORE20260619",
        "name": "AG2-V3 — Technical Held+Core",
        "file": "AG2-V3-Technical-Held-Core.workflow.json",
        "cron": "0 9,13,15 * * 1-5",
        "rotation_mode": "HELD_CORE",
        "batch_size": 18,
        "batch_state_key": "last_index_actions_held_core",
        "ai_provider": "deepseek-v4-pro",
        "ai_model_position": [-16, 7312],
        "ai_parser_position": [160, 7312],
    },
    {
        "id": "AG2V3WATCHNIGHT20260619",
        "name": "AG2-V3 — Technical Watchlist Nightly",
        "file": "AG2-V3-Technical-Watchlist-Nightly.workflow.json",
        "cron": "0 22,2 * * *",
        "rotation_mode": "WATCHLIST",
        "batch_size": 40,
        "batch_state_key": "last_index_actions_watchlist",
        "ai_provider": "deepseek-v4-pro",
        "ai_model_position": [-16, 7728],
        "ai_parser_position": [128, 7728],
    },
]


def load_workflow():
    raw = json.loads(BASE.read_text(encoding="utf-8-sig"))
    return raw[0] if isinstance(raw, list) else raw


def configure_init_code(variant):
    substitutions = [
        (r'const DEFAULT_ROTATION_MODE = "[^"]+";', f'const DEFAULT_ROTATION_MODE = "{variant["rotation_mode"]}";'),
        (r"const DEFAULT_BATCH_SIZE = \d+;", f'const DEFAULT_BATCH_SIZE = {variant["batch_size"]};'),
        (r'const DEFAULT_BATCH_STATE_KEY = "[^"]+";', f'const DEFAULT_BATCH_STATE_KEY = "{variant["batch_state_key"]}";'),
    ]
    code = INIT_CODE
    for pattern, replacement in substitutions:
        code, count = re.subn(pattern, replacement, code, count=1)
        if count != 1:
            raise RuntimeError(f"Init config substitution failed: {pattern}")
    return code


def configure_deepseek_validator(wf, variant):
    chain_name = "AI Validation DeepSeek - ACTIONS/ETF"
    model_name = "DeepSeek Chat Model"
    parser_name = "AG2 — Structured Output DeepSeek"
    chain_id = "4b13e55a-c827-4ace-ab94-2f98e589d736"
    model_id = "4d2f3579-114d-4861-956c-64bc6cd134ac"
    parser_id = "77174ed3-84f3-4c81-a0ca-019ac693e1e3"

    old_node = next(
        (
            node for node in wf.get("nodes", [])
            if node.get("name") == "AI Validation GPT - ACTIONS/ETF"
            or node.get("name") == chain_name
        ),
        None,
    )
    if old_node is None:
        raise RuntimeError("AG2 AI validation node not found")

    if old_node.get("type") == "@n8n/n8n-nodes-langchain.openAi":
        response_values = old_node["parameters"]["responses"]["values"]
        system_prompt = next(value["content"] for value in response_values if value.get("role") == "system")
        user_prompt = next(value["content"] for value in response_values if value.get("role") != "system")
        output_schema = old_node["parameters"]["options"]["textFormat"]["textOptions"]["schema"]
    else:
        system_prompt = old_node["parameters"]["messages"]["messageValues"][0]["message"]
        user_prompt = old_node["parameters"]["text"]
        parser_node = next(node for node in wf["nodes"] if node.get("name") == parser_name)
        output_schema = parser_node["parameters"]["inputSchema"]

    old_name = old_node["name"]
    old_position = old_node.get("position", [-16, 7536])
    wf["nodes"] = [
        node for node in wf.get("nodes", [])
        if node.get("name") not in {old_name, model_name, parser_name}
        and node.get("id") not in {chain_id, model_id, parser_id}
    ]
    wf["nodes"].extend([
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
            "name": chain_name,
        },
        {
            "parameters": {"model": "deepseek-v4-pro", "options": {}},
            "type": "@n8n/n8n-nodes-langchain.lmChatDeepSeek",
            "typeVersion": 1,
            "position": variant["ai_model_position"],
            "id": model_id,
            "name": model_name,
            "credentials": {
                "deepSeekApi": {"id": "BlSCC28mzKodkfO5", "name": "DeepSeek account"}
            },
        },
        {
            "parameters": {"schemaType": "manual", "inputSchema": output_schema},
            "type": "@n8n/n8n-nodes-langchain.outputParserStructured",
            "typeVersion": 1.3,
            "position": variant["ai_parser_position"],
            "id": parser_id,
            "name": parser_name,
        },
    ])

    connections = wf.setdefault("connections", {})
    old_main = connections.pop(old_name, {}).get("main") or [[{
        "node": "Merge AI + Context", "type": "main", "index": 0
    }]]
    connections.pop(model_name, None)
    connections.pop(parser_name, None)
    for source_connections in connections.values():
        for branches in source_connections.values():
            for branch in branches:
                for target in branch:
                    if target.get("node") == old_name:
                        target["node"] = chain_name
    connections[chain_name] = {"main": old_main}
    connections[model_name] = {
        "ai_languageModel": [[{"node": chain_name, "type": "ai_languageModel", "index": 0}]]
    }
    connections[parser_name] = {
        "ai_outputParser": [[{"node": chain_name, "type": "ai_outputParser", "index": 0}]]
    }

    by_name = {node["name"]: node for node in wf["nodes"]}
    compute = by_name["Compute + Filter + Write"]["parameters"]["pythonCode"]
    old_hash_line = "sig_hash = compute_sig_hash(dedup_key, h1_sig, h1_ind, d1_ind)"
    new_hash_line = "sig_hash = fnv1a(compute_sig_hash(dedup_key, h1_sig, h1_ind, d1_ind) + '|model=deepseek-v4-pro')"
    if old_hash_line not in compute:
        raise RuntimeError("AG2 model cache namespace hook not found")
    by_name["Compute + Filter + Write"]["parameters"]["pythonCode"] = compute.replace(
        old_hash_line, new_hash_line, 1
    )
    for node_name in ("Extract AI + Write", "Hydrate AI from cache"):
        code = by_name[node_name]["parameters"]["pythonCode"]
        if "gpt-5-mini" not in code:
            raise RuntimeError(f"AG2 model lineage marker missing in {node_name}")
        by_name[node_name]["parameters"]["pythonCode"] = code.replace(
            "gpt-5-mini", "deepseek-v4-pro"
        )


def configure_workflow(base, variant):
    wf = copy.deepcopy(base)
    wf["id"] = variant["id"]
    wf["name"] = variant["name"]
    wf["active"] = True
    wf.pop("updatedAt", None)
    wf.pop("createdAt", None)
    wf.pop("shared", None)
    wf.pop("activeVersionId", None)
    wf.pop("versionCounter", None)
    for node in wf.get("nodes", []):
        if node.get("type") == "n8n-nodes-base.scheduleTrigger":
            node["parameters"] = {
                "rule": {
                    "interval": [
                        {
                            "field": "cronExpression",
                            "expression": variant["cron"],
                        }
                    ]
                }
            }
        if node.get("name") == "Init Config + Batch":
            node.setdefault("parameters", {})["jsCode"] = configure_init_code(variant)
        if node.get("name") == "DuckDB Init Schema":
            node.setdefault("parameters", {})["pythonCode"] = DUCKDB_CODE
        if node.get("name") in (
            "AG2.10 - HTTP - Fetch Yahoo OHLCV (1H Timing)",
            "AG2.15 - HTTP - Fetch Yahoo OHLCV (1D Strategy)",
        ):
            params = node.setdefault("parameters", {}).setdefault("queryParameters", {}).setdefault("parameters", [])
            existing = {str(p.get("name")) for p in params}
            additions = [
                ("exchange", "={{$json.exchange || ''}}"),
                ("asset_class", "={{$json.asset_class || 'EQUITY'}}"),
                ("closed_only", "true"),
                ("validated_only", "true"),
            ]
            for name, value in additions:
                if name not in existing:
                    params.append({"name": name, "value": value})
        if node.get("name") in NODE_CODE_FILES:
            parameter, path = NODE_CODE_FILES[node["name"]]
            node.setdefault("parameters", {})[parameter] = path.read_text(encoding="utf-8")
    if variant.get("ai_provider") == "deepseek-v4-pro":
        configure_deepseek_validator(wf, variant)
    executable = json.dumps(
        {"id": wf["id"], "nodes": wf.get("nodes", []), "connections": wf.get("connections", {})},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    wf["versionId"] = str(uuid.uuid5(uuid.NAMESPACE_URL, executable))
    return wf


base = load_workflow()
for variant in VARIANTS:
    out = ROOT / variant["file"]
    wf = configure_workflow(base, variant)
    out.write_text(json.dumps([wf], ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    print(out)
