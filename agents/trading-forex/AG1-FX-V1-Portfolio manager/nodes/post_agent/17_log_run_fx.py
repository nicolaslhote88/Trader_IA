import json
import duckdb
from datetime import datetime, timezone

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("db_path") or "/files/duckdb/ag1_fx_v1_chatgpt52.duckdb"
orders = ctx.get("executable_orders") or []
fills = ctx.get("fills") or []
decision_json = json.dumps(ctx.get("decision_json") or {}, ensure_ascii=False)

with duckdb.connect(db_path) as con:
    now_ts = datetime.now(timezone.utc)
    if ctx.get("kill_switch_active_effective"):
        con.execute(
            "UPDATE cfg.portfolio_config SET kill_switch_active=TRUE, updated_at=? WHERE config_key='default'",
            [now_ts],
        )
    con.execute(
        """
        INSERT OR REPLACE INTO core.runs (
          run_id, llm_model, started_at, finished_at, decision_json, decisions_count,
          orders_count, fills_count, errors, leverage_max_used, kill_switch_active, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ctx.get("run_id"), ctx.get("llm_model"), now_ts, now_ts, decision_json,
            len((ctx.get("decision_json") or {}).get("decisions") or []),
            len(orders), len(fills), 0,
            float(((ctx.get("brief") or {}).get("config") or {}).get("leverage_max") or 1),
            bool(ctx.get("kill_switch_active_effective")),
            "AG1-FX-V1 run completed",
        ],
    )

return [{"json": {"run_id": ctx.get("run_id"), "orders": len(orders), "fills": len(fills), "snapshot": ctx.get("snapshot")}}]
