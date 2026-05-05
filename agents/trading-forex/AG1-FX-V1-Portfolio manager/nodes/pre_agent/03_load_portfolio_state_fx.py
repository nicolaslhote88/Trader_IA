import math
import os
from pathlib import Path

import duckdb


ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("db_path") or "/files/duckdb/ag1_fx_v1_chatgpt52.duckdb"
schema_path = ctx.get("schema_path") or "/files/AG1-FX-V1-EXPORT/sql/ag1_fx_v1_schema.sql"
ag2_path = ctx.get("ag2_fx_path") or "/files/duckdb/ag2_fx_v1.duckdb"


def to_float(value, default=0.0):
    try:
        if value is None:
            return default
        number = float(value)
        return number if math.isfinite(number) else default
    except Exception:
        return default


def split_sql(text):
    buff, out, sq, dq = [], [], False, False
    for ch in text:
        if ch == "'" and not dq:
            sq = not sq
        elif ch == '"' and not sq:
            dq = not dq
        if ch == ";" and not sq and not dq:
            s = "".join(buff).strip()
            if s:
                out.append(s)
            buff = []
        else:
            buff.append(ch)
    s = "".join(buff).strip()
    if s:
        out.append(s)
    return out


def table_exists(con, schema_name, table_name):
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema_name, table_name],
    ).fetchone()
    return bool(row and int(row[0] or 0) > 0)


def fetch_dicts(con, query, params=None):
    cur = con.execute(query, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def load_latest_ag2_prices(path):
    if not os.path.exists(path):
        return {}, "AG2_DB_MISSING"
    try:
        with duckdb.connect(path, read_only=True) as con:
            rows = con.execute(
                """
                SELECT pair, CAST(last_close AS DOUBLE) AS last_close
                FROM main.technical_signals_fx
                WHERE run_id = (
                    SELECT run_id
                    FROM main.run_log
                    ORDER BY finished_at DESC NULLS LAST, started_at DESC NULLS LAST
                    LIMIT 1
                )
                """
            ).fetchall()
        return {str(pair).upper(): to_float(last_close, 0.0) for pair, last_close in rows if pair and to_float(last_close, 0.0) > 0}, ""
    except Exception as exc:
        return {}, str(exc)


def quote_to_eur(pair, prices):
    pair = str(pair or "").upper()
    quote = pair[3:6]
    if quote == "EUR":
        return 1.0

    direct = prices.get(f"{quote}EUR")
    if direct and direct > 0:
        return float(direct)

    inverse = prices.get(f"EUR{quote}")
    if inverse and inverse > 0:
        return 1.0 / float(inverse)

    eurusd = prices.get("EURUSD")
    usd_eur = 1.0 / float(eurusd) if eurusd and eurusd > 0 else 0.0
    if quote == "USD":
        return usd_eur or None

    quote_usd = prices.get(f"{quote}USD")
    if quote_usd and quote_usd > 0 and usd_eur > 0:
        return float(quote_usd) * usd_eur

    usd_quote = prices.get(f"USD{quote}")
    if usd_quote and usd_quote > 0 and usd_eur > 0:
        return (1.0 / float(usd_quote)) * usd_eur

    return None


with duckdb.connect(db_path) as con:
    if os.path.exists(schema_path):
        for stmt in split_sql(Path(schema_path).read_text(encoding="utf-8")):
            con.execute(stmt)

    cfg = fetch_dicts(con, "SELECT * FROM cfg.portfolio_config WHERE config_key='default' LIMIT 1")[0]
    initial = to_float(cfg.get("initial_capital_eur"), 10000.0)
    leverage_max = max(0.01, to_float(cfg.get("leverage_max"), 1.0))
    cash = to_float(con.execute("SELECT COALESCE(SUM(amount_eur), 0) FROM core.cash_ledger").fetchone()[0], initial)
    if cash <= 0:
        cash = initial

    lots = fetch_dicts(
        con,
        """
        SELECT lot_id, pair, side, size_lots, open_price, open_at,
               stop_loss_price, take_profit_price, leverage_used
        FROM core.position_lots
        WHERE status = 'open'
        ORDER BY open_at
        """,
    )
    realized = to_float(con.execute("SELECT COALESCE(SUM(pnl_eur), 0) FROM core.position_lots WHERE status='closed'").fetchone()[0], 0.0)
    fees = to_float(con.execute("SELECT COALESCE(SUM(fees_eur), 0) FROM core.fills").fetchone()[0], 0.0) if table_exists(con, "core", "fills") else 0.0

    snap_rows = fetch_dicts(
        con,
        """
        SELECT *
        FROM core.portfolio_snapshot
        ORDER BY as_of DESC
        LIMIT 1
        """,
    )
    peak_row = con.execute("SELECT MAX(equity_eur) FROM core.portfolio_snapshot").fetchone() if table_exists(con, "core", "portfolio_snapshot") else None
    day_row = con.execute(
        """
        SELECT equity_eur
        FROM core.portfolio_snapshot
        WHERE CAST(as_of AS DATE) = CURRENT_DATE
        ORDER BY as_of ASC
        LIMIT 1
        """
    ).fetchone() if table_exists(con, "core", "portfolio_snapshot") else None

prices, price_error = load_latest_ag2_prices(ag2_path)
floating = 0.0
notional = 0.0
missing_prices = []
missing_conversions = []

for lot in lots:
    pair = str(lot.get("pair") or "").upper()
    px = prices.get(pair) or to_float(lot.get("open_price"), 0.0)
    if pair not in prices:
        missing_prices.append(pair)
    q2e = quote_to_eur(pair, prices)
    if q2e is None:
        q2e = 1.0
        missing_conversions.append(pair)
    direction = 1.0 if str(lot.get("side") or "").lower() == "long" else -1.0
    size_lots = to_float(lot.get("size_lots"), 0.0)
    open_price = to_float(lot.get("open_price"), px)
    pnl_eur = size_lots * 100000.0 * (px - open_price) * direction * q2e
    notional_eur = abs(size_lots * 100000.0 * px * q2e)
    floating += pnl_eur
    notional += notional_eur
    lot["current_price"] = px
    lot["quote_to_eur"] = q2e
    lot["unrealized_pnl_eur"] = pnl_eur
    lot["notional_eur"] = notional_eur

equity = initial + realized + floating - fees
margin_used = notional / leverage_max
margin_free = max(0.0, equity - margin_used)
leverage_effective = notional / equity if equity > 0 else 0.0
day_start = to_float(day_row[0], equity) if day_row else equity
peak = max(initial, equity, to_float(peak_row[0], initial) if peak_row else initial)
drawdown_day = equity / day_start - 1.0 if day_start > 0 else 0.0
drawdown_total = equity / peak - 1.0 if peak > 0 else 0.0
latest_snapshot_equity = to_float(snap_rows[0].get("equity_eur"), equity) if snap_rows else equity

state = {
    "cash_eur": cash,
    "equity_eur": equity,
    "realized_pnl_eur": realized,
    "floating_pnl_eur": floating,
    "fees_eur": fees,
    "margin_used_eur": margin_used,
    "margin_free_eur": margin_free,
    "open_lots": lots,
    "open_lots_count": len(lots),
    "leverage_effective": leverage_effective,
    "drawdown_day_pct": min(0.0, drawdown_day),
    "drawdown_total_pct": min(0.0, drawdown_total),
    "valuation_source": "live_recomputed_from_corrected_lots_and_latest_ag2",
    "latest_snapshot_equity_eur": latest_snapshot_equity,
    "latest_snapshot_delta_eur": equity - latest_snapshot_equity,
    "valuation_warnings": {
        "ag2_price_error": price_error,
        "missing_prices": sorted(set(missing_prices)),
        "missing_conversions": sorted(set(missing_conversions)),
    },
}

cfg["llm_model"] = ctx.get("llm_model") or cfg.get("llm_model") or "unset"
with duckdb.connect(db_path) as con:
    con.execute(
        "UPDATE cfg.portfolio_config SET llm_model = ?, updated_at = CURRENT_TIMESTAMP WHERE config_key = 'default'",
        [cfg["llm_model"]],
    )

return [{"json": {**ctx, "config": cfg, "portfolio_state": state}}]
