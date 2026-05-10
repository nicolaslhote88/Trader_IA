import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("db_path") or "/files/duckdb/ag1_fx_v2_chatgpt52.duckdb"
fills = [f for f in (ctx.get("fills") or []) if str(f.get("side") or "").startswith("close_")]
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
            pnl_eur = pnl_quote * quote_to_eur(pair)
            con.execute(
                """
                UPDATE core.position_lots
                SET run_id_close=?, close_price=?, close_at=CAST(? AS TIMESTAMP),
                    pnl_quote=?, pnl_eur=?, status='closed'
                WHERE lot_id=?
                """,
                [ctx.get("run_id"), f.get("fill_price"), f.get("filled_at"), pnl_quote, pnl_eur, lot_id],
            )
            remaining -= close_size
            closed += 1

return [{"json": {**ctx, "lots_closed": closed}}]
