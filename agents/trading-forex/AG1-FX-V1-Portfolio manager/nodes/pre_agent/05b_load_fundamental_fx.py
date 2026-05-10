import json
import os

import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
path = ctx.get("ag3_fx_path") or "/files/duckdb/ag3_fx_v1.duckdb"

rows = []
if os.path.exists(path):
    try:
        with duckdb.connect(path, read_only=True) as con:
            rows = con.execute(
                """
                SELECT pair, payload_json
                FROM main.v_ag3_fx_ag1_summary
                ORDER BY pair
                """
            ).fetchdf().to_dict("records")
    except Exception as exc:
        ctx["fundamental_fx_error"] = str(exc)
else:
    ctx["fundamental_fx_error"] = "AG3_FX_DB_MISSING"

fundamental_fx = {}
for r in rows:
    pair = str(r.get("pair") or "").upper()
    if not pair:
        continue
    try:
        fundamental_fx[pair] = json.loads(r.get("payload_json") or "{}")
    except Exception:
        fundamental_fx[pair] = {"error": "INVALID_PAYLOAD_JSON"}

return [{"json": {**ctx, "fundamental_fx": fundamental_fx}}]
