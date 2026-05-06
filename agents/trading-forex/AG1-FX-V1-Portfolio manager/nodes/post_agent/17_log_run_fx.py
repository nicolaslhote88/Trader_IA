import json
import os
import duckdb
from datetime import datetime, timezone

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("db_path") or "/files/duckdb/ag1_fx_v1_chatgpt52.duckdb"
orders = ctx.get("executable_orders") or []
fills = ctx.get("fills") or []
decision_json = json.dumps(ctx.get("decision_json") or {}, ensure_ascii=False)
safety_summary = ctx.get("safety_summary") or {}
rejected_orders_count = int(safety_summary.get("rejected_orders_count") or sum(1 for o in orders if o.get("status") == "rejected"))
risk_rejection_json = json.dumps(safety_summary.get("rejection_reasons") or {}, ensure_ascii=False)
reconciliation = ctx.get("ibkr_reconciliation") or {}

with duckdb.connect(db_path) as con:
    now_ts = datetime.now(timezone.utc)
    con.execute("ALTER TABLE core.runs ADD COLUMN IF NOT EXISTS rejected_orders_count INTEGER")
    con.execute("ALTER TABLE core.runs ADD COLUMN IF NOT EXISTS risk_rejection_json VARCHAR")
    if ctx.get("kill_switch_active_effective"):
        con.execute(
            "UPDATE cfg.portfolio_config SET kill_switch_active=TRUE, updated_at=? WHERE config_key='default'",
            [now_ts],
        )
    con.execute(
        """
        INSERT OR REPLACE INTO core.runs (
          run_id, llm_model, started_at, finished_at, decision_json, decisions_count,
          orders_count, fills_count, rejected_orders_count, risk_rejection_json,
          errors, leverage_max_used, kill_switch_active, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ctx.get("run_id"), ctx.get("llm_model"), now_ts, now_ts, decision_json,
            len((ctx.get("decision_json") or {}).get("decisions") or []),
            len(orders), len(fills), rejected_orders_count, risk_rejection_json, 0,
            float(((ctx.get("brief") or {}).get("config") or {}).get("leverage_max") or 1),
            bool(ctx.get("kill_switch_active_effective")),
            json.dumps(
                {
                    "message": "AG1-FX-V1 run completed",
                    "ibkr_reconciliation_ok": reconciliation.get("ok"),
                    "ibkr_reconciliation_reasons": reconciliation.get("reasons") or [],
                },
                ensure_ascii=False,
            ),
        ],
    )

lock_path = ((ctx.get("ag1_fx_lock") or {}).get("path") or "").strip()
if lock_path:
    try:
        if os.path.exists(lock_path):
            os.remove(lock_path)
    except Exception:
        pass

return [{"json": {"run_id": ctx.get("run_id"), "orders": len(orders), "fills": len(fills), "snapshot": ctx.get("snapshot"), "lock_released": bool(lock_path)}}]
