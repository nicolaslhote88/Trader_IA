import importlib.util
import json
import os
from pathlib import Path

DEFAULT_DB_PATH = os.getenv("AG1_DUCKDB_PATH", "/files/duckdb/ag1_v2.duckdb")


def load_writer_module():
    env_path = os.getenv("AG1_DUCKDB_WRITER_PATH", "").strip()
    candidates = []
    if env_path:
        candidates.append(Path(env_path))
    candidates.extend(
        [
            Path("AG1-V2-EXPORT/nodes/post_agent/duckdb_writer.py"),
            Path("/files/AG1-V2-EXPORT/nodes/post_agent/duckdb_writer.py"),
            Path("/workspace/AG1-V2-EXPORT/nodes/post_agent/duckdb_writer.py"),
            Path("/data/AG1-V2-EXPORT/nodes/post_agent/duckdb_writer.py"),
        ]
    )

    for path in candidates:
        if not path.exists():
            continue
        spec = importlib.util.spec_from_file_location("ag1_duckdb_writer", str(path))
        if spec is None or spec.loader is None:
            continue
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod, str(path)

    checked = ", ".join(str(p) for p in candidates)
    raise FileNotFoundError(f"duckdb_writer.py not found. Checked: {checked}")


items = _items or []
if not items:
    return []

writer, writer_path = load_writer_module()

out = []
for idx, it in enumerate(items, start=1):
    j = dict(it.get("json", {}) or {})
    bundle = j.get("bundle")
    if not isinstance(bundle, dict):
        bundle = j

    db_path = str(j.get("db_path") or DEFAULT_DB_PATH)

    init_res = writer.init_schema(db_path)
    upsert_res = writer.upsert_run_bundle(db_path, bundle)
    run_id = upsert_res.get("run_id") or (bundle.get("run") or {}).get("run_id")
    snap_res = writer.compute_snapshots(db_path, run_id)

    out.append(
        {
            "json": {
                "ok": True,
                "index": idx,
                "db_path": db_path,
                "writer_path": writer_path,
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
