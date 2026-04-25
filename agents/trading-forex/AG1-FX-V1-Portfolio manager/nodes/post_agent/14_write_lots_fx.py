import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("db_path") or "/files/duckdb/ag1_fx_v1_chatgpt52.duckdb"
fills = ctx.get("fills") or []
orders_by_id = {o.get("order_id"): o for o in (ctx.get("executable_orders") or [])}

opened = 0
with duckdb.connect(db_path) as con:
    for f in fills:
        con.execute(
            "INSERT OR REPLACE INTO core.fills VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                f.get("fill_id"), f.get("order_id"), f.get("pair"), f.get("side"),
                f.get("fill_price"), f.get("fill_size_lots"), f.get("fees_eur"),
                f.get("swap_eur"), f.get("filled_at"), f.get("fill_source"),
            ],
        )
        con.execute("UPDATE core.orders SET status='filled' WHERE order_id=?", [f.get("order_id")])
        order = orders_by_id.get(f.get("order_id")) or {}
        if f.get("side") in {"buy_base", "sell_base"}:
            side = "long" if f.get("side") == "buy_base" else "short"
            con.execute(
                """
                INSERT OR REPLACE INTO core.position_lots (
                  lot_id, run_id_open, run_id_close, pair, side, size_lots, open_price, open_at,
                  close_price, close_at, pnl_quote, pnl_eur, fees_eur, swap_eur_total,
                  leverage_used, stop_loss_price, take_profit_price, status, notes
                ) VALUES (?, ?, NULL, ?, ?, ?, ?, CAST(? AS TIMESTAMP), NULL, NULL, NULL, NULL, ?, 0, ?, ?, ?, 'open', ?)
                """,
                [
                    f"LOT_{f.get('fill_id')}", ctx.get("run_id"), f.get("pair"), side,
                    f.get("fill_size_lots"), f.get("fill_price"), f.get("filled_at"), f.get("fees_eur"),
                    order.get("leverage_used") or 1, order.get("stop_loss_price"), order.get("take_profit_price"),
                    order.get("risk_check_notes") or "",
                ],
            )
            opened += 1

return [{"json": {**ctx, "fills_written": len(fills), "lots_opened": opened}}]
