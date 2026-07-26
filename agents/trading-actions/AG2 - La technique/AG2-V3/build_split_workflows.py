import copy
import json
import re
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BASE = ROOT / "AG2-V3-Technical-Held-Core.workflow.json"
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
    },
    {
        "id": "AG2V3WATCHNIGHT20260619",
        "name": "AG2-V3 — Technical Watchlist Nightly",
        "file": "AG2-V3-Technical-Watchlist-Nightly.workflow.json",
        "cron": "0 22,2 * * *",
        "rotation_mode": "WATCHLIST",
        "batch_size": 40,
        "batch_state_key": "last_index_actions_watchlist",
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
