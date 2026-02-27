#!/usr/bin/env python3
"""
AG2-V2 workflow sync utility.

Canonical source:
- AG2-V2-workflow.final-loop-vector-test.json

This script can:
1) Print/export the canonical workflow JSON
2) Synchronize node source files (ag2-v2/nodes/*) from code nodes embedded in the canonical workflow

Usage examples:
  python build_workflow.py > AG2-V2-workflow.final-loop-vector-test.json
  python build_workflow.py --sync-nodes
  python build_workflow.py --write-files
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

DIR = Path(__file__).resolve().parent
NODES_DIR = DIR / "nodes"
CANONICAL_FILE = DIR / "AG2-V2-workflow.final-loop-vector-test.json"


def load_workflow(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def find_node(
    workflow: dict,
    *,
    name: str | None = None,
    starts: str | None = None,
    contains: str | None = None,
) -> dict:
    for node in workflow.get("nodes", []):
        nm = node.get("name", "")
        if name is not None and nm == name:
            return node
        if starts is not None and nm.startswith(starts):
            return node
        if contains is not None and contains in nm:
            return node
    raise KeyError(name or starts or contains)


def sync_nodes(workflow: dict) -> None:
    mapping = [
        ({"name": "Init Config + Batch"}, NODES_DIR / "01_init_config.js", "jsCode"),
        ({"name": "DuckDB Init Schema"}, NODES_DIR / "02_duckdb_init.py", "pythonCode"),
        ({"contains": "Wrap H1"}, NODES_DIR / "03a_wrap_h1.js", "jsCode"),
        ({"contains": "Wrap D1"}, NODES_DIR / "03b_wrap_d1.js", "jsCode"),
        ({"name": "Compute + Filter + Write"}, NODES_DIR / "04_compute.py", "pythonCode"),
        ({"name": "Snapshot Context"}, NODES_DIR / "05_snapshot.js", "jsCode"),
        ({"name": "Merge AI + Context"}, NODES_DIR / "06a_merge_ai.js", "jsCode"),
        ({"name": "Extract AI + Write"}, NODES_DIR / "06_extract_ai.py", "pythonCode"),
        ({"name": "Hydrate AI from cache"}, NODES_DIR / "07_hydrate_ai_cache.py", "pythonCode"),
        ({"name": "Build Vector Docs from DuckDB (Final Loop)"}, NODES_DIR / "12_build_vector_docs_final_loop.py", "pythonCode"),
        ({"name": "Mark Vectorized"}, NODES_DIR / "09_mark_vector.py", "pythonCode"),
        ({"name": "Finalize Run"}, NODES_DIR / "10_finalize.py", "pythonCode"),
    ]

    for matcher, filepath, code_key in mapping:
        node = find_node(workflow, **matcher)
        code = node.get("parameters", {}).get(code_key)
        if code is None:
            raise RuntimeError(f"Missing {code_key} in node '{node.get('name')}'")
        filepath.write_text(code.rstrip("\n") + "\n", encoding="utf-8")

    # Legacy file kept intentionally to avoid confusion in existing tooling.
    (NODES_DIR / "08_prep_vector.js").write_text(
        "// Deprecated: legacy prep node not used in final workflow.\n"
        "// Canonical vector build node is:\n"
        "// nodes/12_build_vector_docs_final_loop.py\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default=str(CANONICAL_FILE), help="Canonical workflow JSON path")
    parser.add_argument("--output", default="-", help="Output path or '-' for stdout")
    parser.add_argument(
        "--sync-nodes",
        action="store_true",
        help="Sync ag2-v2/nodes/* from canonical workflow code nodes",
    )
    parser.add_argument("--write-files", action="store_true", help="Shortcut for --sync-nodes")
    args = parser.parse_args()

    workflow = load_workflow(Path(args.source))

    if args.write_files:
        args.sync_nodes = True

    if args.sync_nodes:
        sync_nodes(workflow)
        print("Synced node source files from canonical workflow")

    payload = json.dumps(workflow, indent=2, ensure_ascii=False) + "\n"
    if args.output == "-":
        print(payload, end="")
    else:
        Path(args.output).write_text(payload, encoding="utf-8")


if __name__ == "__main__":
    main()
