#!/usr/bin/env python3
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
    candidates = sorted(WORKFLOW_DIR.glob("AG1 - Workflow*.json"))
    if not candidates:
        raise FileNotFoundError(f"No workflow JSON found in {WORKFLOW_DIR}")
    src = candidates[0]
    wf = json.loads(src.read_text(encoding="utf-8"))
    return src, wf


def find_node(nodes: list[dict], aliases: Iterable[str]) -> dict:
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
    raise KeyError(f"Node not found for aliases={tuple(aliases)}")


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def export_selected_nodes(nodes: list[dict], out_nodes_dir: Path) -> list[dict]:
    summary_rows: list[dict] = []
    # Reset only managed folders/files.
    for sub in ("pre_agent", "agent_input", "post_agent"):
        shutil.rmtree(out_nodes_dir / sub, ignore_errors=True)
    (out_nodes_dir).mkdir(parents=True, exist_ok=True)

    for spec in EXPORT_SPECS:
        node = find_node(nodes, spec.aliases)
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

    # NODE_SUMMARY.tsv
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


def ensure_sql_mirror() -> None:
    root_sql = ROOT / "sql" / "portfolio_ledger_schema_v2.sql"
    if not root_sql.is_file():
        raise FileNotFoundError(f"Missing SQL schema: {root_sql}")
    wf_sql_dir = WORKFLOW_DIR / "sql"
    wf_sql_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(root_sql, wf_sql_dir / "portfolio_ledger_schema_v2.sql")
    # Keep a tiny README in both locations for consistency.
    readme_text = "AG1 portfolio ledger schema v2 (DuckDB) used by post-agent writer.\n"
    write_text(ROOT / "sql" / "README.md", readme_text)
    write_text(WORKFLOW_DIR / "sql" / "README.md", readme_text)


def ensure_workflow_normalized_copy(src_workflow_path: Path, workflow_obj: dict) -> None:
    write_json(WORKFLOW_DIR / "AG1_workflow_general.json", workflow_obj)

    # Root readme for the pack.
    root_readme = """# AG1 V2 Portfolio Manager Pack

This folder is the canonical AG1 workflow pack rebuilt from the manually maintained n8n workflow.

## Source of truth

- `workflow/AG1 - Workflow général.json` (manual n8n export kept unchanged)

## Generated export artifacts

- `workflow/AG1_workflow_general.json` (normalized UTF-8 copy)
- `workflow/nodes/*` (selected critical nodes and code)
- `workflow/sql/portfolio_ledger_schema_v2.sql`
- `workflow/docs/POST_AGENT_DUCKDB_LEDGER.md`
- `nodes/*` (mirror for direct mounting/reference)
- `sql/portfolio_ledger_schema_v2.sql`
- `docs/POST_AGENT_DUCKDB_LEDGER.md`

## Notes

- Code nodes are extracted from the workflow JSON.
- `duckdb_writer.py` is an external runtime dependency for node `9 - Upsert Run Bundle (DuckDB)`.
"""
    write_text(ROOT / "README.md", root_readme)

    # Root export helper (commit/push current folder only).
    export_ps1 = """param(
    [string]$CommitMessage = \"AG1: rebuild portfolio manager pack from workflow\",
    [switch]$Push
)

$ErrorActionPreference = \"Stop\"

$branch = (git rev-parse --abbrev-ref HEAD).Trim()
if (-not $branch -or $branch -eq \"HEAD\") {
    throw \"Unable to detect current branch.\"
}

$folder = \"AG1-V2-Portfolio manager\"
git add -- $folder

$staged = git diff --cached --name-only
if (-not $staged) {
    throw \"No staged changes.\"
}

git commit -m $CommitMessage

if ($Push) {
    git push origin $branch
    Write-Host \"Pushed to origin/$branch\"
} else {
    Write-Host \"Commit created on branch '$branch'.\"
    Write-Host \"Run: git push origin $branch\"
}
"""
    write_text(ROOT / "export_to_github.ps1", export_ps1)

    # Keep/update workflow README if missing.
    if not (WORKFLOW_DIR / "README.md").exists():
        write_text(
            WORKFLOW_DIR / "README.md",
            "# AG1 workflow subfolder\n\nImport `AG1_workflow_general.json` in n8n.\n",
        )


def ensure_post_agent_doc_placeholders() -> None:
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
"""
    root_doc = ROOT / "docs" / "POST_AGENT_DUCKDB_LEDGER.md"
    wf_doc = WORKFLOW_DIR / "docs" / "POST_AGENT_DUCKDB_LEDGER.md"
    if not root_doc.exists():
        write_text(root_doc, doc)
    if not wf_doc.exists():
        write_text(wf_doc, doc)


def mirror_external_writer_if_present() -> None:
    root_writer = ROOT / "nodes" / "post_agent" / "duckdb_writer.py"
    wf_writer = WORKFLOW_DIR / "nodes" / "post_agent" / "duckdb_writer.py"
    if root_writer.is_file():
        wf_writer.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root_writer, wf_writer)


def mirror_workflow_nodes_to_root() -> None:
    src = WORKFLOW_DIR / "nodes"
    dst = ROOT / "nodes"
    root_writer = dst / "post_agent" / "duckdb_writer.py"
    writer_backup = root_writer.read_bytes() if root_writer.is_file() else None
    # Clean managed tree then mirror.
    for sub in ("pre_agent", "agent_input", "post_agent"):
        shutil.rmtree(dst / sub, ignore_errors=True)
    dst.mkdir(parents=True, exist_ok=True)
    if (dst / "NODE_SUMMARY.tsv").exists():
        (dst / "NODE_SUMMARY.tsv").unlink()
    shutil.copy2(src / "NODE_SUMMARY.tsv", dst / "NODE_SUMMARY.tsv")
    for sub in ("pre_agent", "agent_input", "post_agent"):
        shutil.copytree(src / sub, dst / sub, dirs_exist_ok=True)
    if writer_backup is not None:
        root_writer.parent.mkdir(parents=True, exist_ok=True)
        root_writer.write_bytes(writer_backup)


def main() -> None:
    src_workflow_path, workflow_obj = load_workflow()
    nodes = workflow_obj.get("nodes", []) or []
    if not nodes:
        raise RuntimeError("Workflow contains no nodes.")

    ensure_workflow_normalized_copy(src_workflow_path, workflow_obj)
    ensure_sql_mirror()
    ensure_post_agent_doc_placeholders()
    summary_rows = export_selected_nodes(nodes, WORKFLOW_DIR / "nodes")
    mirror_workflow_nodes_to_root()
    mirror_external_writer_if_present()

    print(f"Source workflow: {src_workflow_path.name}")
    print(f"Workflow name: {workflow_obj.get('name')}")
    print(f"Total nodes: {len(nodes)}")
    print(f"Exported nodes: {len(summary_rows)}")
    print(f"Wrote: {WORKFLOW_DIR / 'AG1_workflow_general.json'}")
    print(f"Wrote: {WORKFLOW_DIR / 'nodes' / 'NODE_SUMMARY.tsv'}")
    print(f"Wrote: {ROOT / 'nodes' / 'NODE_SUMMARY.tsv'}")


if __name__ == "__main__":
    main()
