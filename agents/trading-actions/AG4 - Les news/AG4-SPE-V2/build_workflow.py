#!/usr/bin/env python3
"""Reconstruit l'artefact AG4_Spé-V2 depuis le scaffold publié et les nœuds canonisés.

Le workflow live contient de nombreux paramètres n8n (positions, versions de nœuds,
credentials et options HTTP). Le JSON publié reste le scaffold structurel ; les
fichiers `nodes/` sont la source canonique du code embarqué.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "AG4-SPE-V2-workflow.json"
AG2_READ_UNIVERSE = ROOT.parents[1] / "AG2 - La technique" / "AG2-V3" / "nodes" / "00_read_universe.py"

CODE_NODES = {
    "S00A - Load Universe (Google Sheets)": ("pythonCode", AG2_READ_UNIVERSE),
    "S00B - DuckDB Init Schema": ("pythonCode", ROOT / "nodes" / "00_duckdb_prepare.py"),
    "S01 - Build Symbol Queue": ("pythonCode", ROOT / "nodes" / "01_build_symbol_queue.py"),
    "S02 - Start Run": ("pythonCode", ROOT / "nodes" / "02_start_run.py"),
    "S02B - Reset News Buffer": ("jsCode", ROOT / "nodes" / "02_reset_news_buffer.js"),
    "S05E - Build Listing Error Row": ("jsCode", ROOT / "nodes" / "11_build_error_row.js"),
    "S06 - Extract Articles": ("jsCode", ROOT / "nodes" / "03_extract_listing_articles.js"),
    "S07 - Normalize + Dedupe + Limit": ("jsCode", ROOT / "nodes" / "04_normalize_articles.js"),
    "S09 - Explode Articles": ("jsCode", ROOT / "nodes" / "05_explode_articles.js"),
    "S11 - Route New vs Seen": ("pythonCode", ROOT / "nodes" / "06_route_new_seen.py"),
    "S15E - Build Article Error Row": ("jsCode", ROOT / "nodes" / "11_build_error_row.js"),
    "S16 - Parse Article": ("jsCode", ROOT / "nodes" / "07_parse_article.js"),
    "S17 - Prepare LLM Input": ("jsCode", ROOT / "nodes" / "08_prepare_llm_input.js"),
    "S20 - Parse LLM Output": ("jsCode", ROOT / "nodes" / "09_parse_llm_output.js"),
    "S21 - Build Skip Row": ("jsCode", ROOT / "nodes" / "10_build_skip_row.js"),
    "S21B - Buffer News Rows": ("jsCode", ROOT / "nodes" / "12_buffer_news_rows.js"),
    "S22 - Upsert News DuckDB": ("pythonCode", ROOT / "nodes" / "12_write_news_duckdb.py"),
    "S22B - Flush News Buffer": ("jsCode", ROOT / "nodes" / "12_flush_news_buffer.js"),
    "S23A - Write Article Errors DuckDB": ("pythonCode", ROOT / "nodes" / "13_write_errors_duckdb.py"),
    "S23L - Write Listing Errors DuckDB": ("pythonCode", ROOT / "nodes" / "13_write_errors_duckdb.py"),
    "S24 - Finalize Run": ("pythonCode", ROOT / "nodes" / "14_finalize_run.py"),
}


def load_workflow(path: Path) -> dict:
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    workflow = raw[0] if isinstance(raw, list) else raw
    for key in ("nodes", "connections"):
        if isinstance(workflow.get(key), str):
            workflow[key] = json.loads(workflow[key])
    return workflow


def build(source: Path) -> dict:
    workflow = load_workflow(source)
    by_name = {node.get("name"): node for node in workflow.get("nodes", [])}
    missing = sorted(set(CODE_NODES) - set(by_name))
    if missing:
        raise RuntimeError(f"Nœuds absents du scaffold: {missing}")
    for name, (parameter, path) in CODE_NODES.items():
        code = path.read_text(encoding="utf-8")
        # Conserver les terminaisons exactes de la version publiée n8n.
        if name == "S00A - Load Universe (Google Sheets)":
            code = code.rstrip("\r\n")
        elif name == "S16 - Parse Article":
            code = code.rstrip("\r\n") + "\n\n"
        by_name[name].setdefault("parameters", {})[parameter] = code
    return workflow


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=OUTPUT,
                        help="Scaffold n8n (défaut: artefact courant ; accepte un export publié).")
    args = parser.parse_args()
    workflow = build(args.source)
    OUTPUT.write_text(json.dumps([workflow], ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
