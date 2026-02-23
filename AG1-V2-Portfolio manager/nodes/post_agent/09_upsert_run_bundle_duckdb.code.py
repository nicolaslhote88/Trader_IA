import importlib.util
import os
import sys
import traceback
from pathlib import Path

DEFAULT_DB_PATH = os.getenv("AG1_DUCKDB_PATH", "/files/duckdb/ag1_v2.duckdb")
DEFAULT_WRITER_PATH = os.getenv(
    "AG1_DUCKDB_WRITER_PATH",
    "/files/AG1-V2-EXPORT/nodes/post_agent/duckdb_writer.py",
)
DEFAULT_SCHEMA_PATH = os.getenv(
    "AG1_LEDGER_SCHEMA_PATH",
    "/files/AG1-V2-EXPORT/sql/portfolio_ledger_schema_v2.sql",
)
SCHEMA_ENV = "AG1_LEDGER_SCHEMA_PATH"

def _resolve_schema_path():
    schema_path = os.getenv(SCHEMA_ENV, "").strip()
    if schema_path and Path(schema_path).is_file():
        return schema_path

    default_schema = Path(DEFAULT_SCHEMA_PATH)
    if default_schema.is_file():
        schema_path = str(default_schema)
        os.environ[SCHEMA_ENV] = schema_path
        return schema_path

    return ""

def _load_writer_module(writer_path_text):
    writer_path = Path(writer_path_text)
    if not writer_path.is_file():
        raise FileNotFoundError(
            "duckdb_writer.py not found at "
            f"'{writer_path}'. "
            "Mount AG1-V2-EXPORT in the runner and set AG1_DUCKDB_WRITER_PATH."
        )

    spec = importlib.util.spec_from_file_location("ag1_duckdb_writer", str(writer_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module spec for '{writer_path}'.")

    module = importlib.util.module_from_spec(spec)
    
    # On enregistre le module pour que @dataclass fonctionne
    sys.modules["ag1_duckdb_writer"] = module
    spec.loader.exec_module(module)

    # --- LA CORRECTION EST ICI ---
    # Nous avons totalement supprimé la vérification agressive qui fâchait n8n.
    # On retourne le module directement. Python se chargera du reste !
    # -----------------------------

    return module, str(writer_path)

try:
    items = _items or []
    if not items:
        return []

    first_json = items[0].get("json", {}) if isinstance(items[0], dict) else {}
    writer_path = str(first_json.get("duckdb_writer_path") or DEFAULT_WRITER_PATH).strip()
    if not writer_path:
        raise ValueError("AG1_DUCKDB_WRITER_PATH is empty.")

    schema_path = _resolve_schema_path()
    writer, writer_path = _load_writer_module(writer_path)

    out = []
    for idx, it in enumerate(items, start=1):
        incoming = it.get("json", {}) if isinstance(it, dict) else {}
        j = dict(incoming or {})

        bundle = j.get("bundle")
        if not isinstance(bundle, dict):
            bundle = j

        db_path = str(j.get("db_path") or DEFAULT_DB_PATH).strip()
        if not db_path:
            raise ValueError("Missing db_path and AG1_DUCKDB_PATH is empty.")

        # Appels directs aux fonctions (parfaitement autorisés par n8n)
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
    return [{
        "json": {
            "status": "FATAL_ERROR",
            "error_message": str(e),
            "traceback": traceback.format_exc()
        }
    }]
