#!/usr/bin/env python3
"""Reconstruit AG4_Spé-IBKR-V1 avec l'analyse DeepSeek structurée."""
from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path

from build_workflow import configure_deepseek_analyzer, load_workflow


ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "AG4-SPE-IBKR-V1-workflow.json"

LEGACY_PARSE = 'const raw = j.output?.[0]?.content?.[0]?.text || j.content || "{}";'
DEEPSEEK_PARSE = """// Basic LLM Chain + Structured Output Parser returns { output: <parsed object> }.
// Keep the legacy OpenAI response envelope as a rollback-compatible fallback.
const raw = (j.output && !Array.isArray(j.output))
  ? j.output
  : (j.output?.[0]?.content?.[0]?.text || j.content || j.text || \"{}\");"""


def build(source: Path) -> dict:
    workflow = load_workflow(source)
    configure_deepseek_analyzer(workflow)
    parser = next(
        node for node in workflow.get("nodes", [])
        if node.get("name") == "S20 - Parse LLM Output"
    )
    code = parser["parameters"]["jsCode"]
    if LEGACY_PARSE in code:
        code = code.replace(LEGACY_PARSE, DEEPSEEK_PARSE, 1)
    elif "j.output && !Array.isArray(j.output)" not in code:
        raise RuntimeError("Hook de parsing S20 IBKR absent")
    parser["parameters"]["jsCode"] = code

    identity = json.dumps(
        {
            "id": workflow.get("id"),
            "nodes": workflow.get("nodes", []),
            "connections": workflow.get("connections", {}),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    workflow["versionId"] = str(uuid.uuid5(uuid.NAMESPACE_URL, identity))
    workflow["activeVersionId"] = None
    return workflow


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=OUTPUT)
    args = parser.parse_args()
    workflow = build(args.source)
    OUTPUT.write_text(
        json.dumps([workflow], ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
