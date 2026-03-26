#!/usr/bin/env python3
"""
Generate AG2-V3 split workflows from the canonical workflow:
- one FX-only workflow (asset_class == FX/CURRENCY universe)
- one non-FX workflow (equity/crypto/etc.)

This script also syncs the canonical workflow code nodes from:
- AG2-V3/nodes/01_init_config.js
- AG2-V3/nodes/02_duckdb_init.py
"""

from __future__ import annotations

import copy
import json
from pathlib import Path


DIR = Path(__file__).resolve().parent
CANONICAL_PATH = DIR / "AG2-V3 - Analyse technique.json"
INIT_CODE_PATH = DIR / "nodes" / "01_init_config.js"
DUCKDB_INIT_CODE_PATH = DIR / "nodes" / "02_duckdb_init.py"

OUT_FX_PATH = DIR / "AG2-V3 - Analyse technique (FX only).json"
OUT_NON_FX_PATH = DIR / "AG2-V3 - Analyse technique (non-FX).json"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def find_node(workflow: dict, name: str) -> dict:
    for node in workflow.get("nodes", []):
        if node.get("name") == name:
            return node
    raise KeyError(f"Node not found: {name}")


def set_node_code(workflow: dict, node_name: str, code_key: str, code_text: str) -> None:
    node = find_node(workflow, node_name)
    params = node.setdefault("parameters", {})
    params[code_key] = code_text.rstrip("\n")


def build_variant(base_workflow: dict, forced_mode: str, workflow_name: str) -> dict:
    wf = copy.deepcopy(base_workflow)
    wf["name"] = workflow_name

    init_node = find_node(wf, "Init Config + Batch")
    js = str(init_node.get("parameters", {}).get("jsCode", ""))
    marker = 'const FORCED_UNIVERSE_MODE = "";'
    replacement = f'const FORCED_UNIVERSE_MODE = "{forced_mode}";'
    if marker not in js:
        raise RuntimeError("Init code marker not found for FORCED_UNIVERSE_MODE.")
    init_node["parameters"]["jsCode"] = js.replace(marker, replacement, 1)
    return wf


def main() -> None:
    workflow = load_json(CANONICAL_PATH)

    init_code = INIT_CODE_PATH.read_text(encoding="utf-8")
    duckdb_init_code = DUCKDB_INIT_CODE_PATH.read_text(encoding="utf-8")

    # Keep canonical workflow aligned with source node files.
    set_node_code(workflow, "Init Config + Batch", "jsCode", init_code)
    set_node_code(workflow, "DuckDB Init Schema", "pythonCode", duckdb_init_code)
    save_json(CANONICAL_PATH, workflow)

    fx_workflow = build_variant(
        workflow,
        forced_mode="FX_ONLY",
        workflow_name="AG2-V3 - Analyse technique (FX only)",
    )
    non_fx_workflow = build_variant(
        workflow,
        forced_mode="NON_FX_ONLY",
        workflow_name="AG2-V3 - Analyse technique (non-FX)",
    )

    save_json(OUT_FX_PATH, fx_workflow)
    save_json(OUT_NON_FX_PATH, non_fx_workflow)

    print(f"Updated canonical: {CANONICAL_PATH}")
    print(f"Generated: {OUT_FX_PATH}")
    print(f"Generated: {OUT_NON_FX_PATH}")


if __name__ == "__main__":
    main()

