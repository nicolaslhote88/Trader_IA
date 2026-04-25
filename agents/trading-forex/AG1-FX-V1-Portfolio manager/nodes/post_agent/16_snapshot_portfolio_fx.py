import duckdb
from datetime import datetime, timezone

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("db_path") or "/files/duckdb/ag1_fx_v1_chatgpt52.duckdb"
brief = ctx.get("brief") or {}
prices = {r.get("pair"): r.get("last_close") for r in brief.get("technical_signals", []) if r.get("pair")}

def quote_to_eur(pair):
    quote = pair[3:]
    if quote == "EUR":
        return 1.0
    direct = prices.get(f"{quote}EUR")
    if direct:
        return float(direct)
    inv = prices.get(f"EUR{quote}")
    if inv:
        return 1.0 / float(inv)
    if quote == "USD":
        eurusd = prices.get("EURUSD")
        return 1.0 / float(eurusd) if eurusd else 1.0
    return 1.0

with duckdb.connect(db_path) as con:
    cash = float(con.execute("SELECT COALESCE(SUM(amount_eur), 0) FROM core.cash_ledger").fetchone()[0] or 0)
    lots = con.execute("SELECT pair, side, size_lots, open_price FROM core.position_lots WHERE status='open'").fetchall()
    floating = 0.0
    notional = 0.0
    for pair, side, size_lots, open_price in lots:
        px = float(prices.get(pair) or open_price or 0)
        q2e = quote_to_eur(pair)
        direction = 1 if side == "long" else -1
        floating += float(size_lots) * 100000 * (px - float(open_price)) * direction * q2e
        notional += abs(float(size_lots) * 100000 * px * q2e)
    realized = float(con.execute("SELECT COALESCE(SUM(pnl_eur), 0) FROM core.position_lots WHERE status='closed'").fetchone()[0] or 0)
    fees = float(con.execute("SELECT COALESCE(SUM(fees_eur), 0) FROM core.fills").fetchone()[0] or 0)
    equity = cash + floating + realized - fees - 10000.0
    # Cash ledger keeps the initial deposit; equity is account value, not pure PnL.
    equity = 10000.0 + floating + realized - fees
    pnl_total = equity - 10000.0
    row = con.execute("SELECT MAX(equity_eur) FROM core.portfolio_snapshot").fetchone()
    peak = max(float(row[0] or 10000.0), equity, 10000.0)
    dd_total = equity / peak - 1.0
    lev = notional / equity if equity > 0 else 0.0
    margin_used = notional / max(1.0, float((brief.get("config") or {}).get("leverage_max") or 1))
    con.execute(
        """
        INSERT OR REPLACE INTO core.portfolio_snapshot VALUES (
          ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        [
            f"SNP_{ctx.get('run_id')}", ctx.get("run_id"), cash, equity, margin_used,
            max(0.0, equity - margin_used), lev, len(lots), floating, pnl_total,
            min(0.0, pnl_total / 10000.0), dd_total, "AG1-FX-V1 snapshot",
        ],
    )

return [{"json": {**ctx, "snapshot": {"equity_eur": equity, "pnl_total_eur": pnl_total, "open_lots_count": len(lots), "leverage_effective": lev}}}]
