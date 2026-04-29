import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("db_path") or "/files/duckdb/ag1_fx_v1_chatgpt52.duckdb"
fills = [f for f in (ctx.get("fills") or []) if str(f.get("side") or "").startswith("close_")]

closed = 0
with duckdb.connect(db_path) as con:
    for f in fills:
        pair = f.get("pair")
        lots = con.execute(
            "SELECT lot_id, side, size_lots, open_price FROM core.position_lots WHERE pair=? AND status='open' ORDER BY open_at",
            [pair],
        ).fetchall()
        remaining = float(f.get("fill_size_lots") or 999999)
        for lot_id, side, size_lots, open_price in lots:
            if remaining <= 0:
                break
            close_size = min(float(size_lots or 0), remaining)
            if close_size <= 0:
                continue
            direction = 1 if side == "long" else -1
            pnl_quote = close_size * 100000 * (float(f.get("fill_price")) - float(open_price)) * direction
            con.execute(
                """
                UPDATE core.position_lots
                SET run_id_close=?, close_price=?, close_at=CAST(? AS TIMESTAMP),
                    pnl_quote=?, pnl_eur=?, status='closed'
                WHERE lot_id=?
                """,
                [ctx.get("run_id"), f.get("fill_price"), f.get("filled_at"), pnl_quote, pnl_quote, lot_id],
            )
            remaining -= close_size
            closed += 1

return [{"json": {**ctx, "lots_closed": closed}}]
