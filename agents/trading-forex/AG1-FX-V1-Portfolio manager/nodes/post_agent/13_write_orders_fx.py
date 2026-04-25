import json
import duckdb
from datetime import datetime, timezone

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("db_path") or "/files/duckdb/ag1_fx_v1_chatgpt52.duckdb"
orders = ctx.get("executable_orders") or []
alerts = ctx.get("risk_alerts") or []

with duckdb.connect(db_path) as con:
    for o in orders:
        con.execute(
            """
            INSERT OR REPLACE INTO core.orders (
              order_id, client_order_id, run_id, pair, side, order_type, size_lots,
              notional_quote, notional_eur, leverage_used, limit_price, stop_loss_price,
              take_profit_price, requested_at, status, rejection_reason, risk_check_passed, risk_check_notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
            """,
            [
                o.get("order_id"), o.get("client_order_id"), o.get("run_id"), o.get("pair"), o.get("side"),
                o.get("order_type") or "market", float(o.get("size_lots") or 0),
                float(o.get("notional_quote") or 0), float(o.get("notional_eur") or 0),
                float(o.get("leverage_used") or 1), o.get("limit_price"), o.get("stop_loss_price"),
                o.get("take_profit_price"), o.get("status"), o.get("rejection_reason"),
                bool(o.get("risk_check_passed")), o.get("risk_check_notes"),
            ],
        )
    for idx, a in enumerate(alerts, 1):
        con.execute(
            "INSERT OR REPLACE INTO core.alerts VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)",
            [
                f"ALT_{ctx.get('run_id')}_{idx:03d}", ctx.get("run_id"), a.get("severity", "warn"),
                a.get("category", "risk"), a.get("message", ""), json.dumps(a, ensure_ascii=False),
            ],
        )

return [{"json": {**ctx, "orders_written": len(orders), "alerts_written": len(alerts)}}]
