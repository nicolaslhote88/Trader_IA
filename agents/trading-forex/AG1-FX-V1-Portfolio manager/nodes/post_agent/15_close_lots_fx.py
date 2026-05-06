import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("db_path") or "/files/duckdb/ag1_fx_v1_chatgpt52.duckdb"
fills = [f for f in (ctx.get("fills") or []) if str(f.get("side") or "").startswith("close_")]
orders_by_id = {o.get("order_id"): o for o in (ctx.get("executable_orders") or [])}
brief = ctx.get("brief") or {}
prices = {r.get("pair"): r.get("last_close") for r in brief.get("technical_signals", []) if r.get("pair")}


def quote_to_eur(pair):
    quote = str(pair or "")[3:6].upper()
    if quote == "EUR":
        return 1.0

    direct = prices.get(f"{quote}EUR")
    if direct:
        return float(direct)

    inverse = prices.get(f"EUR{quote}")
    if inverse:
        return 1.0 / float(inverse)

    eurusd = prices.get("EURUSD")
    usd_eur = 1.0 / float(eurusd) if eurusd else 0.0
    if quote == "USD":
        return usd_eur or 1.0

    quote_usd = prices.get(f"{quote}USD")
    if quote_usd and usd_eur:
        return float(quote_usd) * usd_eur

    usd_quote = prices.get(f"USD{quote}")
    if usd_quote and usd_eur:
        return (1.0 / float(usd_quote)) * usd_eur

    return 1.0

closed = 0
close_errors = []
with duckdb.connect(db_path) as con:
    for f in fills:
        order = orders_by_id.get(f.get("order_id")) or {}
        target_lot_id = order.get("lot_id_to_close") or f.get("lot_id_to_close")
        if not target_lot_id:
            close_errors.append({"order_id": f.get("order_id"), "reason": "MISSING_LOT_ID_TO_CLOSE"})
            continue
        pair = f.get("pair")
        row = con.execute(
            """
            SELECT lot_id, run_id_open, pair, side, size_lots, open_price, open_at,
                   fees_eur, swap_eur_total, leverage_used, stop_loss_price,
                   take_profit_price, notes
            FROM core.position_lots
            WHERE lot_id=? AND status='open'
            """,
            [target_lot_id],
        ).fetchone()
        if not row:
            close_errors.append({"order_id": f.get("order_id"), "lot_id": target_lot_id, "reason": "LOT_TO_CLOSE_NOT_FOUND"})
            continue

        (
            lot_id,
            run_id_open,
            lot_pair,
            side,
            size_lots,
            open_price,
            open_at,
            fees_eur,
            swap_eur_total,
            leverage_used,
            stop_loss_price,
            take_profit_price,
            notes,
        ) = row
        expected_close_side = "close_long" if side == "long" else "close_short" if side == "short" else ""
        if f.get("side") != expected_close_side or pair != lot_pair:
            close_errors.append({"order_id": f.get("order_id"), "lot_id": target_lot_id, "reason": "CLOSE_LOT_MISMATCH"})
            continue

        close_size = float(f.get("fill_size_lots") or 0)
        lot_size = float(size_lots or 0)
        if close_size <= 0 or close_size > lot_size + 1e-9:
            close_errors.append({"order_id": f.get("order_id"), "lot_id": target_lot_id, "reason": "CLOSE_SIZE_INVALID"})
            continue

        direction = 1 if side == "long" else -1
        pnl_quote = close_size * 100000 * (float(f.get("fill_price")) - float(open_price)) * direction
        pnl_eur = pnl_quote * quote_to_eur(pair)
        if close_size >= lot_size - 1e-9:
            con.execute(
                """
                UPDATE core.position_lots
                SET run_id_close=?, close_price=?, close_at=CAST(? AS TIMESTAMP),
                    pnl_quote=?, pnl_eur=?, status='closed'
                WHERE lot_id=?
                """,
                [ctx.get("run_id"), f.get("fill_price"), f.get("filled_at"), pnl_quote, pnl_eur, lot_id],
            )
        else:
            con.execute(
                "UPDATE core.position_lots SET size_lots=? WHERE lot_id=?",
                [lot_size - close_size, lot_id],
            )
            con.execute(
                """
                INSERT OR REPLACE INTO core.position_lots (
                  lot_id, run_id_open, run_id_close, pair, side, size_lots, open_price, open_at,
                  close_price, close_at, pnl_quote, pnl_eur, fees_eur, swap_eur_total,
                  leverage_used, stop_loss_price, take_profit_price, status, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS TIMESTAMP), ?, ?, ?, ?, ?, ?, ?, 'closed', ?)
                """,
                [
                    f"{lot_id}::CLOSE::{f.get('order_id')}",
                    run_id_open,
                    ctx.get("run_id"),
                    lot_pair,
                    side,
                    close_size,
                    open_price,
                    open_at,
                    f.get("fill_price"),
                    f.get("filled_at"),
                    pnl_quote,
                    pnl_eur,
                    f.get("fees_eur") or 0,
                    swap_eur_total or 0,
                    leverage_used or 1,
                    stop_loss_price,
                    take_profit_price,
                    notes or "",
                ],
            )
        closed += 1

return [{"json": {**ctx, "lots_closed": closed, "close_errors": close_errors}}]
