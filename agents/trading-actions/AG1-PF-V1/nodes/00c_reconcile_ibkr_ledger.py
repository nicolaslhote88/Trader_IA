import duckdb
import json
import time
import datetime
import math

EXPECTED_ACCOUNT = "U25651155"


def now_utc():
    return datetime.datetime.now(datetime.timezone.utc)


def iso_now():
    return now_utc().replace(microsecond=0).isoformat()


def money(value):
    try:
        return round(float(value), 2)
    except Exception:
        return 0.0


def to_text(value):
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in ("nan", "nat", "none", "null"):
        return ""
    return text


def parse_float(value, default=0.0):
    try:
        if value is None:
            return default
        text = str(value).strip().replace(",", ".")
        if not text:
            return default
        number = float(text)
        return number if math.isfinite(number) else default
    except Exception:
        return default


def norm_symbol(value):
    return to_text(value).upper()


def db_path_from_cfg(cfg):
    path = to_text(cfg.get("portfolio_db_path")) or "/local-files/duckdb/ag1_v4_consensus.duckdb"
    path = path.replace("\\", "/")
    if path.startswith("/local-files/"):
        return "/files/" + path[len("/local-files/"):]
    return path


def ibkr_internal_symbol(row):
    symbol = norm_symbol(
        row.get("symbol")
        or row.get("ticker")
        or row.get("contractDesc")
        or row.get("contract_description_1")
        or row.get("fullName")
    )
    if not symbol:
        return ""
    sec_type = norm_symbol(row.get("sec_type") or row.get("assetClass") or row.get("secType"))
    listing = norm_symbol(row.get("listing_exchange") or row.get("listingExchange") or row.get("exchange"))
    currency = norm_symbol(row.get("currency"))
    if sec_type == "STK" and currency == "EUR" and listing in ("", "SBF", "EUDARK", "ENEXT", "PARIS"):
        return symbol if symbol.endswith(".PA") else symbol + ".PA"
    return symbol


def side_from_ibkr(row):
    side = norm_symbol(row.get("side"))
    if side in ("B", "BUY", "BOT"):
        return "BUY"
    if side in ("S", "SELL", "SLD"):
        return "SELL"
    desc = norm_symbol(row.get("order_description"))
    if desc.startswith("BOT "):
        return "BUY"
    if desc.startswith("SLD ") or desc.startswith("SOLD "):
        return "SELL"
    return side


def is_stock_fill(row):
    return norm_symbol(row.get("sec_type") or row.get("assetClass") or row.get("secType")) == "STK" and side_from_ibkr(row) in ("BUY", "SELL")


def parse_ibkr_trade_time(row):
    raw = to_text(row.get("trade_time"))
    if raw:
        try:
            parsed = datetime.datetime.strptime(raw, "%Y%m%d-%H:%M:%S")
            return parsed.replace(tzinfo=datetime.timezone.utc).isoformat()
        except Exception:
            pass
    ms = parse_float(row.get("trade_time_r"), 0.0)
    if ms > 0:
        try:
            return datetime.datetime.fromtimestamp(ms / 1000.0, tz=datetime.timezone.utc).isoformat()
        except Exception:
            pass
    return iso_now()


def parse_ts(value):
    text = to_text(value)
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed.astimezone(datetime.timezone.utc)
    except Exception:
        return None


def exchange_rates_from_ledger(ledger):
    out = {"EUR": 1.0, "BASE": 1.0}
    if not isinstance(ledger, dict):
        return out
    for ccy, row in ledger.items():
        if isinstance(row, dict):
            rate = parse_float(row.get("exchangerate"), 0.0)
            if rate > 0:
                out[norm_symbol(ccy)] = rate
    return out


def position_market_value_eur(row, rates):
    ccy = norm_symbol(row.get("currency"))
    mv = parse_float(row.get("mktValue") or row.get("marketValue") or row.get("market_value"), 0.0)
    return mv * rates.get(ccy, 1.0)


def position_price_eur(row, rates):
    qty = parse_float(row.get("position") or row.get("quantity") or row.get("qty"), 0.0)
    if abs(qty) <= 1e-12:
        return 0.0
    return position_market_value_eur(row, rates) / qty


def fill_price_eur(row, rates):
    ccy = norm_symbol(row.get("currency"))
    if not ccy:
        listing = norm_symbol(row.get("listing_exchange") or row.get("listingExchange") or row.get("exchange"))
        ccy = "EUR" if listing in ("SBF", "EUDARK", "ENEXT", "PARIS") else "USD"
    return parse_float(row.get("price"), 0.0) * rates.get(ccy, 1.0)


def commission_eur(row, rates):
    ccy = norm_symbol(row.get("currency"))
    if not ccy:
        listing = norm_symbol(row.get("listing_exchange") or row.get("listingExchange") or row.get("exchange"))
        ccy = "EUR" if listing in ("SBF", "EUDARK", "ENEXT", "PARIS") else "USD"
    return abs(parse_float(row.get("commission"), 0.0)) * rates.get(ccy, 1.0)


def fetch_dicts(con, query, params=None):
    cur = con.execute(query, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_existing_execution_ids(con):
    ids = set()
    for row in con.execute("SELECT fill_id FROM core.fills").fetchall():
        text = to_text(row[0])
        if text:
            ids.add(text)
            parts = text.split("_")
            if parts:
                ids.add(parts[-1])
    try:
        for row in con.execute("SELECT broker_execution_id FROM core.fill_costs WHERE broker_execution_id IS NOT NULL").fetchall():
            text = to_text(row[0])
            if text:
                ids.add(text)
    except Exception:
        pass
    return ids


def fetch_existing_orders(con):
    return fetch_dicts(
        con,
        """
        SELECT
          order_id,
          run_id,
          CAST(ts_created AS VARCHAR) AS ts_created,
          symbol,
          side,
          qty,
          limit_price,
          status,
          broker_order_id,
          rationale_json
        FROM core.orders
        """,
    )


def match_order(fill, orders):
    broker_order_id = to_text(fill.get("order_id"))
    symbol = ibkr_internal_symbol(fill)
    side = side_from_ibkr(fill)
    qty = parse_float(fill.get("size"), 0.0)
    order_ref = to_text(fill.get("order_ref"))

    for order in orders:
        if broker_order_id and to_text(order.get("broker_order_id")) == broker_order_id:
            return order

    if order_ref:
        for order in orders:
            if order_ref in to_text(order.get("rationale_json")):
                return order

    fill_ts = parse_ts(parse_ibkr_trade_time(fill))
    candidates = []
    for order in orders:
        if norm_symbol(order.get("symbol")) != symbol:
            continue
        if norm_symbol(order.get("side")) != side:
            continue
        if abs(parse_float(order.get("qty"), 0.0) - qty) > 1e-6:
            continue
        order_ts = parse_ts(order.get("ts_created"))
        if not fill_ts or not order_ts:
            continue
        delta = (fill_ts - order_ts).total_seconds()
        if -60 <= delta <= 21600:
            status = norm_symbol(order.get("status"))
            priority = 0 if status in ("PLANNED", "SUBMITTED") else 1000
            candidates.append((priority + abs(delta), order))
    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]
    return None


def build_missing_fills(con, ibkr_fills, rates):
    orders = fetch_existing_orders(con)
    existing_exec_ids = fetch_existing_execution_ids(con)
    missing = []
    unmatched = []
    for row in ibkr_fills:
        if not isinstance(row, dict) or not is_stock_fill(row):
            continue
        execution_id = to_text(row.get("execution_id"))
        if not execution_id or execution_id in existing_exec_ids:
            continue
        order = match_order(row, orders)
        if not order:
            unmatched.append(row)
            continue
        run_id = to_text(order.get("run_id"))
        order_id = to_text(order.get("order_id"))
        missing.append(
            {
                "fill_id": "FIL_" + run_id + "_" + execution_id,
                "order_id": order_id,
                "run_id": run_id,
                "symbol": norm_symbol(order.get("symbol")) or ibkr_internal_symbol(row),
                "side": side_from_ibkr(row),
                "qty": parse_float(row.get("size"), 0.0),
                "price": fill_price_eur(row, rates),
                "fees_eur": commission_eur(row, rates),
                "ts_fill": parse_ibkr_trade_time(row),
                "broker_execution_id": execution_id,
                "broker_order_id": to_text(row.get("order_id")),
                "raw": row,
            }
        )
    return missing, unmatched


def latest_db_positions(con):
    rows = fetch_dicts(
        con,
        """
        WITH last_run AS (
          SELECT run_id
          FROM core.portfolio_snapshot
          ORDER BY ts DESC
          LIMIT 1
        )
        SELECT symbol, CAST(qty AS DOUBLE) AS qty
        FROM core.positions_snapshot
        WHERE run_id = (SELECT run_id FROM last_run)
        """,
    )
    out = {}
    for row in rows:
        sym = norm_symbol(row.get("symbol"))
        qty = parse_float(row.get("qty"), 0.0)
        if sym:
            out[sym] = out.get(sym, 0.0) + qty
    return out


def latest_db_portfolio(con):
    rows = fetch_dicts(
        con,
        """
        SELECT
          CAST(cash_eur AS DOUBLE) AS cash_eur,
          CAST(equity_eur AS DOUBLE) AS equity_eur,
          CAST(total_value_eur AS DOUBLE) AS total_value_eur
        FROM core.portfolio_snapshot
        ORDER BY ts DESC
        LIMIT 1
        """,
    )
    return rows[0] if rows else {}


def ibkr_positions_map(positions):
    out = {}
    for row in positions:
        if not isinstance(row, dict):
            continue
        sym = ibkr_internal_symbol(row)
        qty = parse_float(row.get("position") or row.get("quantity") or row.get("qty"), 0.0)
        if sym and qty > 0:
            out[sym] = out.get(sym, 0.0) + qty
    return out


def diff_positions(db_pos, ibkr_pos):
    diffs = []
    symbols = sorted(set(db_pos.keys()) | set(ibkr_pos.keys()))
    for sym in symbols:
        db_qty = db_pos.get(sym, 0.0)
        ibkr_qty = ibkr_pos.get(sym, 0.0)
        if abs(db_qty - ibkr_qty) > 1e-6:
            diffs.append({"symbol": sym, "db_qty": db_qty, "ibkr_qty": ibkr_qty})
    return diffs


def upsert_instruments_for_positions(con, positions):
    rows = []
    ts = iso_now()
    for row in positions:
        if not isinstance(row, dict):
            continue
        symbol = ibkr_internal_symbol(row)
        if not symbol:
            continue
        rows.append(
            [
                symbol,
                to_text(row.get("company_name") or row.get("contractDesc") or row.get("contract_description_1") or symbol),
                "EQUITY",
                to_text(row.get("listing_exchange") or row.get("listingExchange") or row.get("exchange")),
                norm_symbol(row.get("currency")) or None,
                None,
                None,
                None,
                True,
                ts,
            ]
        )
    if rows:
        con.executemany(
            """
            INSERT INTO core.instruments (
              symbol, name, asset_class, exchange, currency, isin, sector, industry, is_active, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol) DO UPDATE SET
              name = COALESCE(core.instruments.name, excluded.name),
              asset_class = COALESCE(core.instruments.asset_class, excluded.asset_class),
              exchange = COALESCE(core.instruments.exchange, excluded.exchange),
              currency = COALESCE(core.instruments.currency, excluded.currency),
              is_active = TRUE,
              updated_at = excluded.updated_at
            """,
            rows,
        )


def insert_missing_fills(con, missing):
    if not missing:
        return
    con.executemany(
        """
        INSERT INTO core.fills (
          fill_id, order_id, run_id, ts_fill, qty, price, fees_eur, slippage_bps, liquidity, raw_fill_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
        ON CONFLICT (fill_id) DO NOTHING
        """,
        [
            [
                row["fill_id"],
                row["order_id"],
                row["run_id"],
                row["ts_fill"],
                row["qty"],
                row["price"],
                row["fees_eur"],
                "IBKR_IMPORTED",
                json.dumps({"source": "ibkr_pf_reconcile", "ibkrFill": row["raw"]}, ensure_ascii=False),
            ]
            for row in missing
        ],
    )
    con.executemany(
        """
        INSERT INTO core.fill_costs (
          fill_id, order_id, symbol, pair, broker, broker_execution_id,
          commission_amount, commission_ccy, commission_eur, commission_source, raw_json, recorded_at
        ) VALUES (?, ?, ?, ?, 'IBKR', ?, ?, 'EUR', ?, 'ibkr_pf_reconcile', ?, ?)
        ON CONFLICT (fill_id) DO UPDATE SET
          order_id = excluded.order_id,
          symbol = excluded.symbol,
          pair = excluded.pair,
          broker = excluded.broker,
          broker_execution_id = excluded.broker_execution_id,
          commission_amount = excluded.commission_amount,
          commission_eur = excluded.commission_eur,
          commission_source = excluded.commission_source,
          raw_json = excluded.raw_json,
          recorded_at = excluded.recorded_at
        """,
        [
            [
                row["fill_id"],
                row["order_id"],
                row["symbol"],
                row["symbol"],
                row["broker_execution_id"],
                row["fees_eur"],
                row["fees_eur"],
                json.dumps(row["raw"], ensure_ascii=False),
                iso_now(),
            ]
            for row in missing
        ],
    )
    con.executemany(
        """
        UPDATE core.orders
        SET status = 'FILLED',
            broker_order_id = COALESCE(NULLIF(?, ''), broker_order_id),
            reason = NULL
        WHERE order_id = ?
        """,
        [[row["broker_order_id"], row["order_id"]] for row in missing],
    )


def rebuild_position_lots(con):
    fills = con.execute(
        """
        SELECT
          f.fill_id,
          CAST(f.ts_fill AS VARCHAR) AS ts_fill,
          CAST(f.qty AS DOUBLE) AS qty,
          CAST(f.price AS DOUBLE) AS price,
          CAST(COALESCE(f.fees_eur, 0) AS DOUBLE) AS fees_eur,
          UPPER(COALESCE(o.symbol, '')) AS symbol,
          UPPER(COALESCE(o.side, '')) AS side
        FROM core.fills f
        JOIN core.orders o ON o.order_id = f.order_id
        ORDER BY f.ts_fill, f.fill_id
        """
    ).fetchall()

    lots = []
    open_by_symbol = {}
    for fill_id, ts_fill, qty_raw, price_raw, fees_raw, symbol, side in fills:
        qty = abs(parse_float(qty_raw, 0.0))
        price = parse_float(price_raw, 0.0)
        fees = parse_float(fees_raw, 0.0)
        if not symbol or qty <= 0:
            continue
        if side == "BUY":
            lot = {
                "lot_id": "LOT|" + to_text(fill_id),
                "symbol": symbol,
                "open_fill_id": to_text(fill_id),
                "open_ts": to_text(ts_fill),
                "open_qty": qty,
                "open_price": price,
                "open_fees_eur": fees,
                "remaining_qty": qty,
                "close_ts": None,
                "close_fill_id": None,
                "realized_pnl_eur": 0.0,
                "close_events": [],
            }
            lots.append(lot)
            if symbol not in open_by_symbol:
                open_by_symbol[symbol] = []
            open_by_symbol[symbol].append(lot)
        elif side == "SELL":
            remaining_to_close = qty
            queue = open_by_symbol.get(symbol, [])
            for lot in queue:
                if remaining_to_close <= 1e-9:
                    break
                available = parse_float(lot.get("remaining_qty"), 0.0)
                if available <= 1e-9:
                    continue
                closed = min(available, remaining_to_close)
                lot["remaining_qty"] = available - closed
                open_fee_alloc = parse_float(lot.get("open_fees_eur"), 0.0) * (closed / parse_float(lot.get("open_qty"), 1.0))
                sell_fee_alloc = fees * (closed / qty)
                realized = (price - parse_float(lot.get("open_price"), 0.0)) * closed - open_fee_alloc - sell_fee_alloc
                lot["realized_pnl_eur"] = parse_float(lot.get("realized_pnl_eur"), 0.0) + realized
                lot["close_events"].append(
                    {
                        "close_fill_id": to_text(fill_id),
                        "close_ts": to_text(ts_fill),
                        "qty": closed,
                        "close_price": price,
                        "realized_pnl_eur": realized,
                    }
                )
                if lot["remaining_qty"] <= 1e-9:
                    lot["remaining_qty"] = 0.0
                    lot["close_ts"] = to_text(ts_fill)
                    lot["close_fill_id"] = to_text(fill_id)
                remaining_to_close = remaining_to_close - closed

    con.execute("DELETE FROM core.position_lots")
    rows = []
    for lot in lots:
        remaining = parse_float(lot.get("remaining_qty"), 0.0)
        status = "OPEN" if remaining > 1e-9 else "CLOSED"
        rows.append(
            [
                lot["lot_id"],
                lot["symbol"],
                lot["open_fill_id"],
                lot["open_ts"],
                lot["open_qty"],
                lot["open_price"],
                lot["open_fees_eur"],
                remaining,
                status,
                lot["close_ts"],
                lot["close_fill_id"],
                money(lot["realized_pnl_eur"]) if status == "CLOSED" else None,
                "FIFO",
                json.dumps(
                    {
                        "close_events": lot["close_events"],
                        "realized_pnl_partial": money(lot["realized_pnl_eur"]),
                    },
                    ensure_ascii=False,
                ),
            ]
        )
    if rows:
        con.executemany(
            """
            INSERT INTO core.position_lots (
              lot_id, symbol, open_fill_id, open_ts, open_qty, open_price, open_fees_eur,
              remaining_qty, status, close_ts, close_fill_id, realized_pnl_eur, close_method, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def position_rows_from_ibkr(positions, rates, total_value_eur, run_id, ts):
    rows = []
    for row in positions:
        if not isinstance(row, dict):
            continue
        symbol = ibkr_internal_symbol(row)
        qty = parse_float(row.get("position") or row.get("quantity") or row.get("qty"), 0.0)
        if not symbol or qty <= 0:
            continue
        ccy = norm_symbol(row.get("currency"))
        rate = rates.get(ccy, 1.0)
        avg_cost = parse_float(row.get("avgCost") or row.get("avgPrice") or row.get("avg_price"), 0.0) * rate
        market_value = position_market_value_eur(row, rates)
        last_price = market_value / qty if qty else 0.0
        unrealized = (last_price - avg_cost) * qty
        weight = market_value / total_value_eur if total_value_eur > 0 else 0.0
        rows.append([run_id, ts, symbol, qty, avg_cost, last_price, money(market_value), money(unrealized), weight])
    return rows


def insert_reconciliation_run_and_snapshot(con, positions, ledger, rates, missing, cfg):
    ts = iso_now()
    suffix = to_text(cfg.get("workflow_run_id") or cfg.get("run_id")) or str(int(time.time()))
    suffix = "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in suffix)
    run_id = "RUN_RECON_IBKR_PF_" + suffix
    base = ledger.get("BASE") if isinstance(ledger, dict) else {}
    if not isinstance(base, dict):
        base = {}
    cash_eur = parse_float(base.get("cashbalance"), 0.0)
    equity_eur = parse_float(base.get("stockmarketvalue"), 0.0)
    total_value_eur = parse_float(base.get("netliquidationvalue"), cash_eur + equity_eur)
    initial = 10000.0
    try:
        row = con.execute("SELECT initial_capital_eur FROM cfg.portfolio_config ORDER BY updated_at DESC NULLS LAST LIMIT 1").fetchone()
        if row:
            initial = parse_float(row[0], initial)
    except Exception:
        pass
    cum_fees = parse_float(con.execute("SELECT COALESCE(SUM(CAST(fees_eur AS DOUBLE)), 0) FROM core.fills").fetchone()[0], 0.0)

    con.execute(
        """
        INSERT INTO core.runs (
          run_id, ts_start, ts_end, tz, strategy_version, config_version, prompt_version,
          model, n8n_execution_id, decision_summary, data_ok_for_trading, price_coverage_pct,
          news_count, ai_cost_eur, expected_fees_eur, warnings_json, agent_output_json, risk_gate_json
        ) VALUES (?, ?, ?, 'Europe/Paris', 'ag1_pf_v1_ibkr_reconcile', 'ag1_v4_consensus_v1',
          'pf_ibkr_reconcile', 'ag1_pf_v1', ?, 'PF_IBKR_RECONCILIATION',
          TRUE, NULL, 0, 0, 0, ?, ?, ?)
        ON CONFLICT (run_id) DO NOTHING
        """,
        [
            run_id,
            ts,
            ts,
            to_text(cfg.get("workflow_run_id")),
            json.dumps([], ensure_ascii=False),
            json.dumps({"source": "ibkr_pf_reconcile", "missing_fills": [m["fill_id"] for m in missing]}, ensure_ascii=False),
            json.dumps({"source": "ibkr_pf_reconcile", "account": EXPECTED_ACCOUNT}, ensure_ascii=False),
        ],
    )

    con.execute("DELETE FROM core.positions_snapshot WHERE run_id = ?", [run_id])
    position_rows = position_rows_from_ibkr(positions, rates, total_value_eur, run_id, ts)
    if position_rows:
        con.executemany(
            """
            INSERT INTO core.positions_snapshot (
              run_id, ts, symbol, qty, avg_cost, last_price, market_value_eur, unrealized_pnl_eur, weight_pct
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            position_rows,
        )

    con.execute("DELETE FROM core.portfolio_snapshot WHERE run_id = ?", [run_id])
    con.execute(
        """
        INSERT INTO core.portfolio_snapshot (
          run_id, ts, cash_eur, equity_eur, total_value_eur, cum_fees_eur, cum_ai_cost_eur,
          trades_this_run, total_pnl_eur, roi, drawdown_pct, meta_json
        ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, 0, ?)
        """,
        [
            run_id,
            ts,
            money(cash_eur),
            money(equity_eur),
            money(total_value_eur),
            money(cum_fees),
            len(missing),
            money(total_value_eur - initial),
            (total_value_eur - initial) / initial if initial else 0.0,
            json.dumps({"source": "ibkr_pf_reconcile", "account": EXPECTED_ACCOUNT, "base_ledger": base, "rates": rates}, ensure_ascii=False),
        ],
    )

    cash_pct = cash_eur / total_value_eur if total_value_eur > 0 else 0.0
    top1_value = max((parse_float(row[6], 0.0) for row in position_rows), default=0.0)
    top1 = top1_value / total_value_eur if total_value_eur > 0 else 0.0
    con.execute("DELETE FROM core.risk_metrics WHERE run_id = ?", [run_id])
    con.execute(
        """
        INSERT INTO core.risk_metrics (
          run_id, ts, cash_pct, top1_pos_pct, top1_sector_pct, var95_est_eur,
          positions_count, risk_status, limits_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            ts,
            cash_pct,
            top1,
            top1,
            money(equity_eur * 0.015 * 1.65),
            len(position_rows),
            "BALANCED",
            json.dumps({"source": "ibkr_pf_reconcile"}, ensure_ascii=False),
        ],
    )

    price_rows = []
    for row in positions:
        if not isinstance(row, dict):
            continue
        symbol = ibkr_internal_symbol(row)
        qty = parse_float(row.get("position") or row.get("quantity") or row.get("qty"), 0.0)
        px = position_price_eur(row, rates)
        if symbol and qty > 0 and px > 0:
            price_rows.append([ts, symbol, px, px, px, px, px, None, "ibkr_pf_reconcile", ts])
    if price_rows:
        con.executemany(
            """
            INSERT INTO core.market_prices (
              ts, symbol, open, high, low, close, adj_close, volume, source, "asof"
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (ts, symbol, source) DO NOTHING
            """,
            price_rows,
        )
    return run_id


def upsert_ibkr_positions_latest(con, positions, rates, ts):
    """Ecrit l'etat MTM IBKR live (EUR) dans portfolio_positions_ibkr_latest a CHAQUE run.
    Source de verite du P&L latent affiche par le dashboard, independante de la cadence
    (espacee) des snapshots de reconciliation RECON. Conversion EUR via taux ledger IBKR."""
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_positions_ibkr_latest (
          symbol              VARCHAR PRIMARY KEY,
          quantity            DOUBLE,
          avg_cost_eur        DOUBLE,
          last_price_eur      DOUBLE,
          market_value_eur    DOUBLE,
          unrealized_pnl_eur  DOUBLE,
          currency            VARCHAR,
          fx_rate             DOUBLE,
          run_id              VARCHAR,
          updated_at          VARCHAR
        )
        """
    )
    rows = []
    for row in positions:
        if not isinstance(row, dict):
            continue
        symbol = ibkr_internal_symbol(row)
        qty = parse_float(row.get("position") or row.get("quantity") or row.get("qty"), 0.0)
        if not symbol or qty <= 0:
            continue
        ccy = norm_symbol(row.get("currency"))
        rate = rates.get(ccy, 1.0)
        avg_cost = parse_float(row.get("avgCost") or row.get("avgPrice") or row.get("avg_price"), 0.0) * rate
        market_value = position_market_value_eur(row, rates)
        last_price = market_value / qty if qty else 0.0
        unrealized = (last_price - avg_cost) * qty
        rows.append([
            symbol, qty, avg_cost, last_price, money(market_value), money(unrealized),
            ccy or None, rate, "RUN_IBKR_LIVE_MTM", ts,
        ])
    con.execute("DELETE FROM portfolio_positions_ibkr_latest")
    if rows:
        con.executemany(
            """
            INSERT INTO portfolio_positions_ibkr_latest (
              symbol, quantity, avg_cost_eur, last_price_eur, market_value_eur,
              unrealized_pnl_eur, currency, fx_rate, run_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def should_write(con, positions, ledger, missing):
    ibkr_pos = ibkr_positions_map(positions)
    db_pos = latest_db_positions(con)
    pos_diffs = diff_positions(db_pos, ibkr_pos)
    base = ledger.get("BASE") if isinstance(ledger, dict) else {}
    if not isinstance(base, dict):
        base = {}
    cash = parse_float(base.get("cashbalance"), 0.0)
    equity = parse_float(base.get("stockmarketvalue"), 0.0)
    total = parse_float(base.get("netliquidationvalue"), cash + equity)
    db_portfolio = latest_db_portfolio(con)
    value_diffs = {}
    checks = [("cash_eur", cash, 0.01), ("equity_eur", equity, 1.0), ("total_value_eur", total, 1.0)]
    for key, ibkr_value, threshold in checks:
        db_value = parse_float(db_portfolio.get(key), 0.0)
        if abs(db_value - ibkr_value) > threshold:
            value_diffs[key] = {"db": db_value, "ibkr": ibkr_value}
    return bool(missing or pos_diffs or value_diffs), pos_diffs, value_diffs


def validate_ibkr_state(cfg):
    if not cfg.get("ibkr_reconcile_fetch_ok"):
        return False, "SKIPPED_FETCH_ERROR:" + to_text(cfg.get("ibkr_reconcile_fetch_error"))
    health = cfg.get("ibkr_health") or {}
    if not isinstance(health, dict):
        return False, "SKIPPED_NO_HEALTH"
    if health.get("authenticated") is not True:
        return False, "SKIPPED_NOT_AUTHENTICATED"
    if health.get("dry_run") is True:
        raise RuntimeError("IBKR_RECONCILE_BLOCKED_DRY_RUN_TRUE")
    alignment = health.get("account_alignment") or {}
    selected = norm_symbol(alignment.get("selected_account") or alignment.get("configured_account_id"))
    aligned = alignment.get("aligned")
    gateway_is_paper = alignment.get("gateway_is_paper")
    if selected and selected != EXPECTED_ACCOUNT:
        raise RuntimeError("IBKR_RECONCILE_BLOCKED_ACCOUNT_MISMATCH:" + selected)
    if aligned is False:
        raise RuntimeError("IBKR_RECONCILE_BLOCKED_ACCOUNT_NOT_ALIGNED")
    if gateway_is_paper is True:
        raise RuntimeError("IBKR_RECONCILE_BLOCKED_PAPER_GATEWAY")
    positions = cfg.get("ibkr_positions")
    fills = cfg.get("ibkr_fills")
    ledger = cfg.get("ibkr_ledger")
    if not isinstance(positions, list) or not isinstance(fills, list) or not isinstance(ledger, dict):
        return False, "SKIPPED_INVALID_BROKER_PAYLOAD"
    return True, "OK"


items = _items or [{"json": {}}]
cfg = dict(items[0].get("json") or {})
cfg["ibkr_reconcile_status"] = "NOT_RUN"
cfg["ibkr_reconcile_written"] = False
cfg["ibkr_reconcile_missing_fills"] = []
cfg["ibkr_reconcile_unmatched_stock_fills"] = []
cfg["ibkr_reconcile_position_diffs"] = []
cfg["ibkr_reconcile_value_diffs"] = {}
cfg["ibkr_reconcile_run_id"] = ""

ok, status = validate_ibkr_state(cfg)
if not ok:
    cfg["ibkr_reconcile_status"] = status
    return [{"json": cfg}]

db_path = db_path_from_cfg(cfg)
positions = cfg.get("ibkr_positions") or []
fills = cfg.get("ibkr_fills") or []
ledger = cfg.get("ibkr_ledger") or {}
rates = exchange_rates_from_ledger(ledger)

con = duckdb.connect(db_path)
try:
    # FIX P&L live 2026-06-22 : refresh IBKR-sourced MTM a CHAQUE run (hors gate
    # should_write) -> le dashboard dispose toujours d'un P&L latent IBKR frais
    # (EUR, taux IBKR), independamment de la cadence espacee des snapshots RECON.
    try:
        cfg["ibkr_live_mtm_count"] = upsert_ibkr_positions_latest(con, positions, rates, iso_now())
        cfg["ibkr_live_mtm_written"] = True
    except Exception as _live_err:
        cfg["ibkr_live_mtm_written"] = False
        cfg["ibkr_live_mtm_error"] = str(_live_err)
    missing, unmatched = build_missing_fills(con, fills, rates)
    write_needed, position_diffs, value_diffs = should_write(con, positions, ledger, missing)
    cfg["ibkr_reconcile_missing_fills"] = [row["fill_id"] for row in missing]
    cfg["ibkr_reconcile_unmatched_stock_fills"] = [
        {
            "execution_id": to_text(row.get("execution_id")),
            "symbol": ibkr_internal_symbol(row),
            "side": side_from_ibkr(row),
            "size": parse_float(row.get("size"), 0.0),
        }
        for row in unmatched
    ]
    cfg["ibkr_reconcile_position_diffs"] = position_diffs
    cfg["ibkr_reconcile_value_diffs"] = value_diffs
    if write_needed:
        con.execute("BEGIN TRANSACTION")
        try:
            upsert_instruments_for_positions(con, positions)
            insert_missing_fills(con, missing)
            lot_count = rebuild_position_lots(con)
            run_id = insert_reconciliation_run_and_snapshot(con, positions, ledger, rates, missing, cfg)
            con.execute("COMMIT")
            cfg["ibkr_reconcile_status"] = "WRITTEN"
            cfg["ibkr_reconcile_written"] = True
            cfg["ibkr_reconcile_run_id"] = run_id
            cfg["ibkr_reconcile_lot_count"] = lot_count
        except Exception:
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
            raise
    else:
        cfg["ibkr_reconcile_status"] = "NO_DIFF"
finally:
    try:
        con.execute("CHECKPOINT")
    except Exception:
        pass
    con.close()

return [{"json": cfg}]
