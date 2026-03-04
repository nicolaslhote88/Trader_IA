import importlib.util
import os
import sys
import traceback
from pathlib import Path

DEFAULT_DB_PATH_FALLBACK = "/files/duckdb/ag1_v3.duckdb"
GENERIC_DB_ENV_CANDIDATES = (
    "AG1_DB_PATH",
    "AG1_DUCKDB_PATH",
)
MODEL_DB_ENV_MAP = (
    (("gpt-5", "chatgpt"), "AG1_CHATGPT52_DUCKDB_PATH"),
    (("grok", "grok-4", "grok41"), "AG1_GROK41_REASONING_DUCKDB_PATH"),
    (("gemini", "gemini-3"), "AG1_GEMINI30_PRO_DUCKDB_PATH"),
)
DEFAULT_WRITER_PATH = os.getenv(
    "AG1_DUCKDB_WRITER_PATH",
    "/files/AG1-V3-EXPORT/nodes/post_agent/duckdb_writer.py",
)
DEFAULT_SCHEMA_PATH = os.getenv(
    "AG1_LEDGER_SCHEMA_PATH",
    "/files/AG1-V3-EXPORT/sql/portfolio_ledger_schema_v2.sql",
)
SCHEMA_ENV = "AG1_LEDGER_SCHEMA_PATH"
WRITER_ENV = "AG1_DUCKDB_WRITER_PATH"

STATIC_WRITER_PATHS = (
    "/files/AG1-V3-EXPORT/nodes/post_agent/duckdb_writer.py",
    "/files/AG1-V3-EXPORT/workflow/nodes/post_agent/duckdb_writer.py",
    "/files/AG1-V3-EXPORT/AG1-V3-Portfolio manager/nodes/post_agent/duckdb_writer.py",
    "/files/AG1-V3-EXPORT/AG1-V3-Portfolio manager/workflow/nodes/post_agent/duckdb_writer.py",
    "/files/AG1-V3-Portfolio manager/nodes/post_agent/duckdb_writer.py",
    "/files/AG1-V3-Portfolio manager/workflow/nodes/post_agent/duckdb_writer.py",
    "/files/AG1-V2-EXPORT/nodes/post_agent/duckdb_writer.py",
    "/files/AG1-V2-EXPORT/workflow/nodes/post_agent/duckdb_writer.py",
    "/files/AG1-V2-Portfolio manager/nodes/post_agent/duckdb_writer.py",
    "/files/AG1-V2-Portfolio manager/workflow/nodes/post_agent/duckdb_writer.py",
)
STATIC_SCHEMA_PATHS = (
    "/files/AG1-V3-EXPORT/sql/portfolio_ledger_schema_v2.sql",
    "/files/AG1-V3-EXPORT/workflow/sql/portfolio_ledger_schema_v2.sql",
    "/files/AG1-V3-EXPORT/AG1-V3-Portfolio manager/sql/portfolio_ledger_schema_v2.sql",
    "/files/AG1-V3-EXPORT/AG1-V3-Portfolio manager/workflow/sql/portfolio_ledger_schema_v2.sql",
    "/files/AG1-V3-Portfolio manager/sql/portfolio_ledger_schema_v2.sql",
    "/files/AG1-V3-Portfolio manager/workflow/sql/portfolio_ledger_schema_v2.sql",
    "/files/AG1-V2-EXPORT/sql/portfolio_ledger_schema_v2.sql",
    "/files/AG1-V2-EXPORT/workflow/sql/portfolio_ledger_schema_v2.sql",
    "/files/AG1-V2-Portfolio manager/sql/portfolio_ledger_schema_v2.sql",
    "/files/AG1-V2-Portfolio manager/workflow/sql/portfolio_ledger_schema_v2.sql",
)


def _clean_path_text(value):
    return str(value or "").strip()


def _resolve_default_db_path(bundle=None):
    run = bundle.get("run") if isinstance(bundle, dict) else None
    model_text = str((run or {}).get("model") or "").strip().lower()
    for tokens, env_key in MODEL_DB_ENV_MAP:
        if any(token in model_text for token in tokens):
            value = _clean_path_text(os.getenv(env_key, ""))
            if value:
                return value
    for key in GENERIC_DB_ENV_CANDIDATES:
        value = _clean_path_text(os.getenv(key, ""))
        if value:
            return value
    return DEFAULT_DB_PATH_FALLBACK


def _is_legacy_ag1_db_path(path_text):
    p = str(path_text or "").strip().lower().replace("\\", "/")
    return p.endswith("/ag1_v3.duckdb")


def _pick_db_path(payload, bundle):
    candidates = [
        payload.get("db_path"),
        payload.get("ag1_db_path"),
        bundle.get("db_path") if isinstance(bundle, dict) else None,
        (bundle.get("run") or {}).get("db_path")
        if isinstance(bundle, dict) and isinstance(bundle.get("run"), dict)
        else None,
        _resolve_default_db_path(bundle),
    ]
    for c in candidates:
        s = _clean_path_text(c)
        if s:
            return s
    return ""


def _iter_candidate_paths(*candidates):
    seen = set()
    for candidate in candidates:
        text = _clean_path_text(candidate)
        if not text:
            continue
        path = Path(text).expanduser()
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        yield path


def _first_existing_file(*candidates):
    attempted = []
    for path in _iter_candidate_paths(*candidates):
        attempted.append(str(path))
        if path.is_file():
            return str(path), attempted
    return "", attempted


def _sample_dir(path_text, limit=20):
    path = Path(path_text)
    if not path.exists():
        return "<missing>"
    if not path.is_dir():
        return "<not_a_directory>"
    try:
        entries = sorted(p.name for p in path.iterdir())
    except Exception as exc:
        return f"<error:{exc}>"
    if len(entries) > limit:
        entries = entries[:limit] + ["..."]
    return ", ".join(entries) if entries else "<empty>"


def _discover_named_file(
    filename,
    roots,
    max_depth=7,
    max_dirs=4000,
):
    seen = set()
    visited = 0
    for root in roots:
        root_path = Path(_clean_path_text(root) or ".").expanduser()
        if not root_path.exists() or not root_path.is_dir():
            continue
        stack = [(root_path, 0)]
        while stack and visited < max_dirs:
            cur, depth = stack.pop()
            try:
                key = str(cur.resolve()) if cur.exists() else str(cur)
            except Exception:
                key = str(cur)
            if key in seen:
                continue
            seen.add(key)
            visited += 1

            candidate = cur / filename
            if candidate.is_file():
                return str(candidate)

            if depth >= max_depth:
                continue
            try:
                children = list(cur.iterdir())
            except Exception:
                continue
            dirs = [p for p in children if p.is_dir()]
            dirs.sort(key=lambda p: p.name.lower())
            for d in reversed(dirs):
                name = d.name.lower()
                if name.startswith(".") or name in {"node_modules", ".git", "__pycache__"}:
                    continue
                stack.append((d, depth + 1))
    return ""


def _resolve_writer_path(preferred_path=""):
    cwd = Path.cwd()
    found, attempted = _first_existing_file(
        preferred_path,
        os.getenv(WRITER_ENV, ""),
        DEFAULT_WRITER_PATH,
        *STATIC_WRITER_PATHS,
        cwd / "nodes/post_agent/duckdb_writer.py",
        cwd / "workflow/nodes/post_agent/duckdb_writer.py",
        cwd / "AG1-V3-EXPORT/nodes/post_agent/duckdb_writer.py",
        cwd / "AG1-V3-EXPORT/workflow/nodes/post_agent/duckdb_writer.py",
        cwd / "AG1-V3-EXPORT/AG1-V3-Portfolio manager/workflow/nodes/post_agent/duckdb_writer.py",
        cwd / "AG1-V3-Portfolio manager/nodes/post_agent/duckdb_writer.py",
        cwd / "AG1-V3-Portfolio manager/workflow/nodes/post_agent/duckdb_writer.py",
        cwd / "AG1-V2-EXPORT/nodes/post_agent/duckdb_writer.py",
        cwd / "AG1-V2-EXPORT/workflow/nodes/post_agent/duckdb_writer.py",
        cwd / "AG1-V2-Portfolio manager/nodes/post_agent/duckdb_writer.py",
        cwd / "AG1-V2-Portfolio manager/workflow/nodes/post_agent/duckdb_writer.py",
        Path("/home/runner/nodes/post_agent/duckdb_writer.py"),
        Path("/home/runner/workflow/nodes/post_agent/duckdb_writer.py"),
        Path("/home/runner/AG1-V3-EXPORT/nodes/post_agent/duckdb_writer.py"),
        Path("/home/runner/AG1-V3-EXPORT/workflow/nodes/post_agent/duckdb_writer.py"),
        Path("/home/runner/AG1-V3-EXPORT/AG1-V3-Portfolio manager/workflow/nodes/post_agent/duckdb_writer.py"),
        Path("/home/runner/AG1-V3-Portfolio manager/nodes/post_agent/duckdb_writer.py"),
        Path("/home/runner/AG1-V3-Portfolio manager/workflow/nodes/post_agent/duckdb_writer.py"),
        Path("/home/runner/AG1-V2-EXPORT/nodes/post_agent/duckdb_writer.py"),
        Path("/home/runner/AG1-V2-EXPORT/workflow/nodes/post_agent/duckdb_writer.py"),
        Path("/home/runner/AG1-V2-Portfolio manager/nodes/post_agent/duckdb_writer.py"),
        Path("/home/runner/AG1-V2-Portfolio manager/workflow/nodes/post_agent/duckdb_writer.py"),
    )
    if found:
        os.environ[WRITER_ENV] = found
        return found

    discovered = _discover_named_file(
        "duckdb_writer.py",
        roots=(
            cwd,
            "/files",
            "/home/runner",
        ),
        max_depth=7,
        max_dirs=4000,
    )
    if discovered:
        os.environ[WRITER_ENV] = discovered
        return discovered

    attempted_msg = ", ".join(attempted) if attempted else "(none)"
    debug = (
        f"cwd={cwd} | "
        f"env_{WRITER_ENV}={_clean_path_text(os.getenv(WRITER_ENV, '')) or '<empty>'} | "
        f"/files=[{_sample_dir('/files')}] | "
        f"/files/AG1-V3-EXPORT=[{_sample_dir('/files/AG1-V3-EXPORT')}] | "
        f"/files/AG1-V2-EXPORT=[{_sample_dir('/files/AG1-V2-EXPORT')}]"
    )
    raise FileNotFoundError(
        "duckdb_writer.py not found. Set AG1_DUCKDB_WRITER_PATH to a valid mounted file. "
        f"Tried: {attempted_msg}. Debug: {debug}"
    )


def _resolve_schema_path(preferred_schema_path="", writer_path_text=""):
    cwd = Path.cwd()
    writer_root = None
    if _clean_path_text(writer_path_text):
        writer_path = Path(writer_path_text)
        if writer_path.name == "duckdb_writer.py":
            try:
                writer_root = writer_path.parents[2]
            except Exception:
                writer_root = None

    found, _ = _first_existing_file(
        preferred_schema_path,
        os.getenv(SCHEMA_ENV, ""),
        DEFAULT_SCHEMA_PATH,
        *STATIC_SCHEMA_PATHS,
        (writer_root / "sql/portfolio_ledger_schema_v2.sql") if writer_root else "",
        (writer_root / "workflow/sql/portfolio_ledger_schema_v2.sql") if writer_root else "",
        cwd / "sql/portfolio_ledger_schema_v2.sql",
        cwd / "workflow/sql/portfolio_ledger_schema_v2.sql",
        cwd / "AG1-V3-EXPORT/sql/portfolio_ledger_schema_v2.sql",
        cwd / "AG1-V3-EXPORT/workflow/sql/portfolio_ledger_schema_v2.sql",
        cwd / "AG1-V3-EXPORT/AG1-V3-Portfolio manager/sql/portfolio_ledger_schema_v2.sql",
        cwd / "AG1-V3-EXPORT/AG1-V3-Portfolio manager/workflow/sql/portfolio_ledger_schema_v2.sql",
        cwd / "AG1-V3-Portfolio manager/sql/portfolio_ledger_schema_v2.sql",
        cwd / "AG1-V3-Portfolio manager/workflow/sql/portfolio_ledger_schema_v2.sql",
        cwd / "AG1-V2-EXPORT/sql/portfolio_ledger_schema_v2.sql",
        cwd / "AG1-V2-EXPORT/workflow/sql/portfolio_ledger_schema_v2.sql",
        cwd / "AG1-V2-Portfolio manager/sql/portfolio_ledger_schema_v2.sql",
        cwd / "AG1-V2-Portfolio manager/workflow/sql/portfolio_ledger_schema_v2.sql",
    )
    if found:
        os.environ[SCHEMA_ENV] = found
        return found
    discovered = _discover_named_file(
        "portfolio_ledger_schema_v2.sql",
        roots=(
            writer_root if writer_root else "",
            cwd,
            "/files",
            "/home/runner",
        ),
        max_depth=7,
        max_dirs=4000,
    )
    if discovered:
        os.environ[SCHEMA_ENV] = discovered
        return discovered
    return ""


def _load_writer_module(writer_path_text):
    writer_path = Path(writer_path_text)
    if not writer_path.is_file():
        raise FileNotFoundError(f"duckdb_writer.py not found at '{writer_path}'.")

    spec = importlib.util.spec_from_file_location("ag1_duckdb_writer", str(writer_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module spec for '{writer_path}'.")

    module = importlib.util.module_from_spec(spec)
    sys.modules["ag1_duckdb_writer"] = module
    spec.loader.exec_module(module)

    try:
        _ = module.init_schema
        _ = module.upsert_run_bundle
        _ = module.compute_snapshots
    except Exception:
        raise RuntimeError(
            "duckdb_writer.py is missing required callables: "
            "init_schema, upsert_run_bundle, compute_snapshots"
        )

    return module, str(writer_path)


try:
    items = _items or []
    if not items:
        return []

    first_json = items[0].get("json", {}) if isinstance(items[0], dict) else {}

    requested_writer_path = _clean_path_text(first_json.get("duckdb_writer_path"))
    writer_path = _resolve_writer_path(requested_writer_path or DEFAULT_WRITER_PATH)

    requested_schema_path = _clean_path_text(
        first_json.get("ledger_schema_path") or first_json.get("schema_path")
    )
    schema_path = _resolve_schema_path(requested_schema_path, writer_path)

    writer, writer_path = _load_writer_module(writer_path)

    out = []
    for idx, it in enumerate(items, start=1):
        incoming = it.get("json", {}) if isinstance(it, dict) else {}
        j = dict(incoming or {})

        bundle = j.get("bundle")
        if not isinstance(bundle, dict):
            bundle = j

        db_path = _pick_db_path(j, bundle)
        if not db_path:
            raise ValueError("Missing db_path and AG1_*_DUCKDB_PATH env vars are empty.")
        if _is_legacy_ag1_db_path(db_path):
            raise ValueError(
                "Refusing legacy AG1 DB path '/files/duckdb/ag1_v3.duckdb'. "
                "Each variant must write to its dedicated DuckDB."
            )

        init_res = writer.init_schema(db_path)
        upsert_res = writer.upsert_run_bundle(db_path, bundle)

        run_id = ""
        if isinstance(upsert_res, dict):
            run_id = str(upsert_res.get("run_id") or "").strip()
        if not run_id and isinstance(bundle, dict):
            run_id = str((bundle.get("run") or {}).get("run_id") or "").strip()

        snap_res = writer.compute_snapshots(db_path, run_id) if run_id else {}

        out.append(
            {
                "json": {
                    "ok": True,
                    "index": idx,
                    "db_path": db_path,
                    "writer_path": writer_path,
                    "schema_path": schema_path or os.getenv(SCHEMA_ENV, ""),
                    "run_id": run_id,
                    "init": init_res,
                    "upsert": upsert_res,
                    "snapshots": snap_res,
                    "bundle_summary": j.get("summary") or {},
                },
                "pairedItem": it.get("pairedItem"),
            }
        )

    return out

except Exception as e:
    return [
        {
            "json": {
                "status": "FATAL_ERROR",
                "error_message": str(e),
                "traceback": traceback.format_exc(),
            }
        }
    ]
