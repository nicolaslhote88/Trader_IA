import copy
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BASE = ROOT / "AG2-V3 - Analyse technique actions ETF crypto.json"
INIT_CODE = (ROOT / "nodes" / "01_init_config.js").read_text(encoding="utf-8")
DUCKDB_CODE = (ROOT / "nodes" / "02_duckdb_init.py").read_text(encoding="utf-8")

VARIANTS = [
    {
        "id": "AG2V3HELDCORE20260619",
        "name": "AG2-V3 — Technical Held+Core",
        "file": "AG2-V3-Technical-Held-Core.workflow.json",
        "cron": "10 8,12,14 * * 1-5",
        "rotation_mode": "HELD_CORE",
        "batch_size": 18,
        "batch_state_key": "last_index_actions_held_core",
    },
    {
        "id": "AG2V3WATCHNIGHT20260619",
        "name": "AG2-V3 — Technical Watchlist Nightly",
        "file": "AG2-V3-Technical-Watchlist-Nightly.workflow.json",
        "cron": "20 2 * * 2-6",
        "rotation_mode": "WATCHLIST",
        "batch_size": 40,
        "batch_state_key": "last_index_actions_watchlist",
    },
]


def load_workflow():
    raw = json.loads(BASE.read_text(encoding="utf-8-sig"))
    return raw[0] if isinstance(raw, list) else raw


def configure_init_code(variant):
    code = INIT_CODE
    code = code.replace('const DEFAULT_ROTATION_MODE = "ACTIONS_ONLY";', f'const DEFAULT_ROTATION_MODE = "{variant["rotation_mode"]}";')
    code = code.replace("const DEFAULT_BATCH_SIZE = 10;", f'const DEFAULT_BATCH_SIZE = {variant["batch_size"]};')
    code = code.replace(
        'const DEFAULT_BATCH_STATE_KEY = "last_index_actions";',
        f'const DEFAULT_BATCH_STATE_KEY = "{variant["batch_state_key"]}";',
    )
    return code


def configure_workflow(base, variant):
    wf = copy.deepcopy(base)
    wf["id"] = variant["id"]
    wf["name"] = variant["name"]
    wf["active"] = True
    wf["versionId"] = variant["id"].lower() + "-v1"
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
    return wf


base = load_workflow()
for variant in VARIANTS:
    out = ROOT / variant["file"]
    wf = configure_workflow(base, variant)
    out.write_text(json.dumps([wf], ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    print(out)
