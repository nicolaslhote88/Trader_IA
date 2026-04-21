#!/usr/bin/env python3
"""
Rebuild the AG1 V3 Portfolio Manager pack — **workflow/-only** layout.

Starting April 2026 the canonical pack lives entirely under
`AG1-V3-Portfolio manager/workflow/`. The parent folder no longer
holds a mirror copy of `nodes/`, `sql/`, or `docs/`. Only this
script, the parent README (a pointer), and `workflow/` remain.

Source of truth: `workflow/AG1_workflow_template_v3.json`.

Generated artifacts:
- `workflow/variants/AG1_workflow_v3__*.json` (via generate_model_variants.py)
- `workflow/nodes/<category>/*` (selected critical nodes + extracted code)
- `workflow/nodes/NODE_SUMMARY.tsv`
- `workflow/sql/portfolio_ledger_schema_v2.sql` (copy from workflow/sql — source)
- `workflow/docs/POST_AGENT_DUCKDB_LEDGER.md` (placeholder if missing)

The script preserves `workflow/nodes/post_agent/duckdb_writer.py` if it
has been edited manually (external runtime dep, not extracted from the
template).
"""
from __future__ import annotations

import json
import re
import shutil
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parent
WORKFLOW_DIR = ROOT / "workflow"
WORKFLOW_TEMPLATE = WORKFLOW_DIR / "AG1_workflow_template_v3.json"


def norm_text(value: str) -> str:
    s = str(value or "")
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s


@dataclass(frozen=True)
class ExportSpec:
    category: str
    key: str
    aliases: tuple[str, ...]


EXPORT_SPECS: tuple[ExportSpec, ...] = (
    ExportSpec("pre_agent", "0_seed_portfolio", ("0 - seed portfolio",)),
    ExportSpec("pre_agent", "2B_init_run_context", ("2b - init run context",)),
    ExportSpec("pre_agent", "4B_build_portfolio_context", ("4b build portfolio context",)),
    ExportSpec("pre_agent", "4C_enrich_portfolio_with_market_prices", ("4c enrich portfolio with market prices",)),
    ExportSpec("pre_agent", "AG4_01_fetch_macro_news", ("ag4 01 recuperation des news generales",)),
    ExportSpec("pre_agent", "20J_final_build_market_news_pack", ("20j final build marketnewspack final",)),
    ExportSpec("pre_agent", "R8_data_prep_matrix", ("r8 data prep for matrix fusion filter",)),
    ExportSpec("pre_agent", "calcul_matrice_briefing", ("calcul matrice briefing",)),
    ExportSpec("pre_agent", "fx_00_prepare_brief_context", ("fx 00 prepare brief context", "fx 00 prepare fx brief context")),
    ExportSpec("pre_agent", "merge7", ("merge7",)),
    ExportSpec("agent_input", "ag1_00_assemble_input_packs", ("ag1 00 assemble input packs",)),
    ExportSpec("agent_input", "agent_1_portfolio_manager", ("agent 1 portfolio manager",)),
    ExportSpec("agent_input", "information_extractor", ("information extractor",)),
    ExportSpec("post_agent", "07_validate_enforce_safety_v5", ("7 validate enforce safety",)),
    ExportSpec("post_agent", "08_build_duckdb_bundle", ("8 build duckdb bundle",)),
    ExportSpec("post_agent", "09_upsert_run_bundle_duckdb", ("9 upsert run bundle duckdb",)),
    ExportSpec("post_agent", "10_post_run_health", ("10 post run health duckdb",)),
)


def load_workflow() -> tuple[Path, dict]:
    if WORKFLOW_TEMPLATE.is_file():
        src = WORKFLOW_TEMPLATE
    else:
        candidates = sorted((WORKFLOW_DIR / "variants").glob("AG1_workflow_v3__*.json"))
        if not candidates:
            raise FileNotFoundError(f"No workflow JSON found in {WORKFLOW_DIR}")
        src = candidates[0]
    wf = json.loads(src.read_text(encoding="utf-8"))
    return src, wf


def find_node(nodes: list[dict], aliases: Iterable[str]) -> dict | None:
    alias_norms = [norm_text(a) for a in aliases]
    # Prefer exact normalized match.
    for alias in alias_norms:
        for n in nodes:
            if norm_text(n.get("name", "")) == alias:
                return n
    # Fallback to substring match.
    for alias in alias_norms:
        for n in nodes:
            if alias in norm_text(n.get("name", "")):
                return n
    return None


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def export_selected_nodes(nodes: list[dict], out_nodes_dir: Path) -> list[dict]:
    summary_rows: list[dict] = []
    # Preserve the external duckdb_writer.py if present (not extracted from template).
    writer_path = out_nodes_dir / "post_agent" / "duckdb_writer.py"
    writer_backup = writer_path.read_bytes() if writer_path.is_file() else None
    # Reset only managed folders/files.
    for sub in ("pre_agent", "agent_input", "post_agent"):
        shutil.rmtree(out_nodes_dir / sub, ignore_errors=True)
    (out_nodes_dir).mkdir(parents=True, exist_ok=True)
    # Restore writer before exporting nodes (so it's back in place regardless of spec list).
    if writer_backup is not None:
        writer_path.parent.mkdir(parents=True, exist_ok=True)
        writer_path.write_bytes(writer_backup)

    missing: list[ExportSpec] = []
    for spec in EXPORT_SPECS:
        node = find_node(nodes, spec.aliases)
        if node is None:
            missing.append(spec)
            continue
        node_path = out_nodes_dir / spec.category / f"{spec.key}.node.json"
        write_json(node_path, node)

        params = node.get("parameters", {}) or {}
        code_len = 0
        if "jsCode" in params:
            code = str(params.get("jsCode") or "")
            write_text(out_nodes_dir / spec.category / f"{spec.key}.code.js", code.rstrip() + "\n")
            code_len = len(code)
        elif "pythonCode" in params:
            code = str(params.get("pythonCode") or "")
            write_text(out_nodes_dir / spec.category / f"{spec.key}.code.py", code.rstrip() + "\n")
            code_len = len(code)

        summary_rows.append(
            {
                "key": spec.key,
                "id": str(node.get("id", "")),
                "name": str(node.get("name", "")),
                "type": str(node.get("type", "")),
                "code_len": code_len,
            }
        )

    if missing:
        missing_keys = ", ".join(m.key for m in missing)
        print(f"[warn] nodes not found in template (skipped): {missing_keys}")

    lines = ["key\tid\tname\ttype\tcode_len"]
    for row in summary_rows:
        lines.append(
            "\t".join(
                [
                    row["key"],
                    row["id"],
                    row["name"].replace("\t", " ").replace("\n", " "),
                    row["type"],
                    str(row["code_len"]),
                ]
            )
        )
    write_text(out_nodes_dir / "NODE_SUMMARY.tsv", "\n".join(lines) + "\n")
    return summary_rows


def ensure_sql_in_workflow() -> None:
    wf_sql = WORKFLOW_DIR / "sql" / "portfolio_ledger_schema_v2.sql"
    if not wf_sql.is_file():
        raise FileNotFoundError(
            f"Missing SQL schema at {wf_sql}. The workflow/sql folder is now the source."
        )
    write_text(
        WORKFLOW_DIR / "sql" / "README.md",
        "AG1 portfolio ledger schema v2 (DuckDB) used by post-agent writer.\n",
    )


def ensure_workflow_pointer_readme() -> None:
    """Parent README is a pointer — the real content lives in workflow/."""
    root_readme = """# AG1 V3 Portfolio Manager Pack

The whole canonical pack now lives in [`workflow/`](workflow/).

- Source of truth : `workflow/AG1_workflow_template_v3.json`
- Nodes extraits : `workflow/nodes/`
- Schema DuckDB : `workflow/sql/portfolio_ledger_schema_v2.sql`
- Variants par modèle : `workflow/variants/`

Utilitaires au niveau parent :

- `rebuild_pack.py` — régénère les fichiers `workflow/nodes/*` et les variants depuis le template.
- `export_to_github.ps1` — helper PowerShell pour commit + push ciblé sur ce dossier.

Voir [`docs/dev/rebuild_pack.md`](../docs/dev/rebuild_pack.md) pour la procédure.
"""
    write_text(ROOT / "README.md", root_readme)


def ensure_post_agent_doc_placeholder() -> None:
    """Write a placeholder only if workflow/docs/POST_AGENT_DUCKDB_LEDGER.md is absent.

    The authoritative content lives at `docs/history/` or `docs/architecture/`
    in the repo root. We only drop a minimal pointer here so a deployment
    mount has the file available if downstream tools expect it.
    """
    wf_doc = WORKFLOW_DIR / "docs" / "POST_AGENT_DUCKDB_LEDGER.md"
    if wf_doc.exists():
        return
    doc = """# AG1 Post-Agent DuckDB Ledger Notes

This file documents the post-agent branch:

1. `7 - Validate & Enforce Safety`
2. `8 - Build DuckDB Bundle`
3. `9 - Upsert Run Bundle (DuckDB)`
4. `10 - Post-Run Health (DuckDB)`

`9 - Upsert Run Bundle (DuckDB)` loads an external `duckdb_writer.py` and requires:

- `AG1_DUCKDB_PATH`
- `AG1_DUCKDB_WRITER_PATH`
- `AG1_LEDGER_SCHEMA_PATH` (optional override)

See `docs/architecture/etat_des_lieux.md` in the repo root for the canonical
description of this branch.
"""
    write_text(wf_doc, doc)


def main() -> None:
    src_workflow_path, workflow_obj = load_workflow()
    nodes = workflow_obj.get("nodes", []) or []
    if not nodes:
        raise RuntimeError("Workflow contains no nodes.")

    # Keep the template JSON normalized (idempotent).
    write_json(WORKFLOW_TEMPLATE, workflow_obj)
    ensure_sql_in_workflow()
    ensure_post_agent_doc_placeholder()
    summary_rows = export_selected_nodes(nodes, WORKFLOW_DIR / "nodes")
    ensure_workflow_pointer_readme()

    print(f"Source workflow: {src_workflow_path.name}")
    print(f"Workflow name: {workflow_obj.get('name')}")
    print(f"Total nodes: {len(nodes)}")
    print(f"Exported nodes: {len(summary_rows)}")
    print(f"Wrote: {WORKFLOW_TEMPLATE}")
    print(f"Wrote: {WORKFLOW_DIR / 'nodes' / 'NODE_SUMMARY.tsv'}")


if __name__ == "__main__":
    main()
