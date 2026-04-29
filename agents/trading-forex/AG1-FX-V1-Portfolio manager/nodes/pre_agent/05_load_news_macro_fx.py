import json
import os
import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
path = ctx.get("ag4_fx_path") or "/files/duckdb/ag4_fx_v1.duckdb"
payload = {"top_news": [], "pair_focus": {}, "macro_regime": {}}
if os.path.exists(path):
    try:
        with duckdb.connect(path, read_only=True) as con:
            df = con.execute(
                """
                SELECT section, payload
                FROM main.fx_digest
                WHERE run_id = (SELECT run_id FROM main.run_log ORDER BY finished_at DESC NULLS LAST, started_at DESC LIMIT 1)
                """
            ).fetchdf()
            for rec in df.to_dict("records"):
                section = rec.get("section")
                obj = json.loads(rec.get("payload") or "{}")
                if section == "top_news":
                    payload["top_news"] = obj.get("items") or []
                elif section == "pair_focus":
                    payload["pair_focus"] = obj.get("pairs") or {}
                elif section == "macro_regime":
                    payload["macro_regime"] = obj
    except Exception as exc:
        ctx["macro_news_error"] = str(exc)

return [{"json": {**ctx, "macro_news": payload}}]
