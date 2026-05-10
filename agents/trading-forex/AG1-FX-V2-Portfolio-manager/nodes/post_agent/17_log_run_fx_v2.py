import json
import duckdb
from datetime import datetime, timezone

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("db_path") or "/files/duckdb/ag1_fx_v2_chatgpt52.duckdb"
orders = ctx.get("executable_orders") or []
fills = ctx.get("fills") or []
decision = ctx.get("decision") or {}
decision_json = json.dumps(decision, ensure_ascii=False)
safety_summary = ctx.get("safety_summary") or {}

rejected_orders_count = int(safety_summary.get("rejected_orders_count") or sum(1 for o in orders if o.get("status") == "rejected"))
pillar_rejected = int(safety_summary.get("pillar_rejected") or 0)
crowded_forced = int(safety_summary.get("crowded_forced_close") or 0)
risk_rejection_json = json.dumps(safety_summary.get("rejection_reasons") or {}, ensure_ascii=False)

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
          orders_count, fills_count, rejected_orders_count, pillar_rejected_count,
          crowded_rejected_count, risk_rejection_json,
          errors, leverage_max_used, kill_switch_active, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ctx.get("run_id"), ctx.get("llm_model"), now_ts, now_ts, decision_json,
            len(decision.get("actions") or []),
            len(orders), len(fills), rejected_orders_count,
            pillar_rejected, crowded_forced,
            risk_rejection_json, 0,
            float(((ctx.get("brief") or {}).get("config") or {}).get("leverage_max") or 2.0),
            bool(ctx.get("kill_switch_active_effective")),
            "AG1-FX-V2 run completed — Framework 3 Piliers Global Macro",
        ],
    )

return [{"json": {"run_id": ctx.get("run_id"), "orders": len(orders), "fills": len(fills), "snapshot": ctx.get("snapshot")}}]
