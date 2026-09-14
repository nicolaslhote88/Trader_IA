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


IBKR_SYMBOL_BY_CONID = {}


EXCHANGE_SUFFIX_BY_LISTING = {
    "AEB": ".AS",
    "AMS": ".AS",
    "SBF": ".PA",
    "EUDARK": ".PA",
    "ENEXT": ".PA",
    "PARIS": ".PA",
    "IBIS": ".DE",
    "XETRA": ".DE",
    "LSE": ".L",
    "EBS": ".SW",
}


def row_conid(row):
    try:
        value = row.get("conid") or row.get("conidEx")
        return int(str(value).split("@", 1)[0]) if value not in (None, "") else None
    except Exception:
        return None


def symbol_from_listing(symbol, listing):
    if not symbol or "." in symbol:
        return symbol
    suffix = EXCHANGE_SUFFIX_BY_LISTING.get(norm_symbol(listing))
    return symbol + suffix if suffix else symbol


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
    if "." in symbol:
        return symbol

    conid = row_conid(row)
    if conid is not None and conid in IBKR_SYMBOL_BY_CONID:
        return IBKR_SYMBOL_BY_CONID[conid]

    listing = norm_symbol(row.get("listing_exchange") or row.get("listingExchange") or row.get("exchange"))
    # /positions often omits listing_exchange. Never infer Paris from EUR alone.
    return symbol_from_listing(symbol, listing)


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
    return mv * confirmed_fx_rate(rates, ccy)


def position_price_eur(row, rates):
    qty = parse_float(row.get("position") or row.get("quantity") or row.get("qty"), 0.0)
    if abs(qty) <= 1e-12:
        return 0.0
    return position_market_value_eur(row, rates) / qty


def position_quantity(row):
    return parse_float(row.get("position") or row.get("quantity") or row.get("qty"), 0.0)


def active_position_rows(positions):
    rows = []
    for row in positions:
        if not isinstance(row, dict):
            continue
        symbol = ibkr_internal_symbol(row)
        qty = position_quantity(row)
        if symbol and qty > 1e-9:
            rows.append(row)
    return rows


def open_lot_qty_by_symbol(con):
    out = {}
    try:
        cur = con.execute(
            """
            SELECT UPPER(symbol) AS symbol, CAST(SUM(COALESCE(remaining_qty, 0)) AS DOUBLE) AS qty
            FROM core.position_lots
            WHERE UPPER(COALESCE(status, '')) = 'OPEN'
              AND CAST(COALESCE(remaining_qty, 0) AS DOUBLE) > 1e-9
            GROUP BY UPPER(symbol)
            """
        )
        for symbol, qty in cur.fetchall():
            sym = norm_symbol(symbol)
            if sym:
                out[sym] = parse_float(qty, 0.0)
    except Exception:
        return {}
    return out


def filter_positions_against_ledger(con, positions, rates, ledger):
    """Defend against short-lived IBKR desync: /positions can lag after a full SELL
    while /account/ledger already removed the stock market value. In that case,
    closed lots are the tie-breaker and stale rows are not written to DuckDB."""
    active = active_position_rows(positions)
    base = ledger.get("BASE") if isinstance(ledger, dict) else {}
    if not isinstance(base, dict):
        base = {}
    ledger_equity = parse_float(base.get("stockmarketvalue"), 0.0)
    positions_equity = sum(position_market_value_eur(row, rates) for row in active)
    tolerance = max(1.0, abs(ledger_equity) * 0.0025)
    diag = {
        "ledger_equity_eur": money(ledger_equity),
        "positions_equity_raw_eur": money(positions_equity),
        "positions_equity_filtered_eur": money(positions_equity),
        "tolerance_eur": money(tolerance),
        "removed_stale_closed_symbols": [],
        "applied": False,
    }
    if positions_equity <= ledger_equity + tolerance:
        return active, diag

    open_qty = open_lot_qty_by_symbol(con)
    filtered = []
    removed = []
    for row in active:
        symbol = ibkr_internal_symbol(row)
        if open_qty.get(symbol, 0.0) <= 1e-9:
            removed.append(
                {
                    "symbol": symbol,
                    "qty": position_quantity(row),
                    "market_value_eur": money(position_market_value_eur(row, rates)),
                }
            )
            continue
        filtered.append(row)

    filtered_equity = sum(position_market_value_eur(row, rates) for row in filtered)
    raw_gap = abs(positions_equity - ledger_equity)
    filtered_gap = abs(filtered_equity - ledger_equity)
    if removed and filtered_gap + tolerance < raw_gap:
        diag["positions_equity_filtered_eur"] = money(filtered_equity)
        diag["removed_stale_closed_symbols"] = removed
        diag["applied"] = True
        return filtered, diag

    return active, diag


def fill_currency(row):
    ccy = norm_symbol(row.get("currency"))
    if ccy:
        return ccy
    listing = norm_symbol(row.get("listing_exchange") or row.get("listingExchange") or row.get("exchange"))
    currencies = {
        "SBF": "EUR", "EUDARK": "EUR", "ENEXT": "EUR", "PARIS": "EUR",
        "AEB": "EUR", "ENEXT.BE": "EUR", "BVME": "EUR", "IBIS": "EUR", "IBIS2": "EUR",
        "NASDAQ": "USD", "NASDAQ.NMS": "USD", "NYSE": "USD", "AMEX": "USD", "ARCA": "USD", "BATS": "USD",
        "LSE": "GBP", "EBS": "CHF", "SIX": "CHF", "TSE": "CAD", "VENTURE": "CAD",
        "SEHK": "HKD", "TSEJ": "JPY", "ASX": "AUD", "SGX": "SGD",
    }
    if listing not in currencies:
        raise ValueError("IBKR_FILL_CURRENCY_UNRESOLVED:" + listing)
    return currencies[listing]


def fill_fx_rate(row, rates):
    ccy = fill_currency(row)
    rate = 1.0 if ccy == "EUR" else parse_float(rates.get(ccy), 0.0)
    if rate <= 0:
        raise ValueError("IBKR_FILL_FX_RATE_UNAVAILABLE:" + ccy)
    return rate


def fill_price_eur(row, rates):
    return parse_float(row.get("price"), 0.0) * fill_fx_rate(row, rates)


def commission_eur(row, rates):
    fee_row = dict(row)
    fee_row["currency"] = row.get("commission_currency") or row.get("commissionCurrency") or fill_currency(row)
    return abs(parse_float(row.get("commission"), 0.0)) * fill_fx_rate(fee_row, rates)


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


def _fill_payload(raw_value):
    value = raw_value
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return {}
    if not isinstance(value, dict):
        return {}
    nested = value.get("ibkrFill")
    return nested if isinstance(nested, dict) else value


def build_conid_symbol_map(con, ibkr_fills):
    """Resolve conids from persisted fills/orders before normalizing positions."""
    candidates = {}

    def add(conid, symbol):
        try:
            cid = int(str(conid).split("@", 1)[0])
        except Exception:
            return
        sym = norm_symbol(symbol)
        if cid > 0 and sym:
            candidates.setdefault(cid, set()).add(sym)

    try:
        rows = con.execute(
            """
            SELECT o.symbol, CAST(f.raw_fill_json AS VARCHAR)
            FROM core.fills f
            JOIN core.orders o ON o.order_id = f.order_id
            WHERE o.symbol IS NOT NULL AND f.raw_fill_json IS NOT NULL
            """
        ).fetchall()
        for symbol, raw_fill in rows:
            payload = _fill_payload(raw_fill)
            add(payload.get("conid") or payload.get("conidEx"), symbol)
    except Exception:
        pass

    orders = fetch_existing_orders(con)
    by_broker_order = {
        to_text(order.get("broker_order_id")): norm_symbol(order.get("symbol"))
        for order in orders
        if to_text(order.get("broker_order_id")) and norm_symbol(order.get("symbol"))
    }
    for fill in ibkr_fills:
        if not isinstance(fill, dict):
            continue
        canonical = by_broker_order.get(to_text(fill.get("order_id")))
        if not canonical:
            order_ref = to_text(fill.get("order_ref"))
            if order_ref:
                for order in orders:
                    if order_ref in to_text(order.get("rationale_json")):
                        canonical = norm_symbol(order.get("symbol"))
                        break
        if canonical:
            add(fill.get("conid") or fill.get("conidEx"), canonical)

    resolved = {}
    conflicts = {}
    for conid, symbols in candidates.items():
        if len(symbols) == 1:
            resolved[conid] = next(iter(symbols))
        else:
            conflicts[str(conid)] = sorted(symbols)
    return resolved, conflicts


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
                "currency": fill_currency(row),
                "price_native": parse_float(row.get("price"), 0.0),
                "fx_rate_eur": fill_fx_rate(row, rates),
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
    for row in active_position_rows(positions):
        sym = ibkr_internal_symbol(row)
        qty = position_quantity(row)
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
          fill_id, order_id, run_id, ts_fill, qty, price, fees_eur, slippage_bps, liquidity, raw_fill_json, currency, price_native, fx_rate_eur
        ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)
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
                row["currency"], row["price_native"], row["fx_rate_eur"],
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
    for row in active_position_rows(positions):
        symbol = ibkr_internal_symbol(row)
        qty = position_quantity(row)
        if not symbol or qty <= 0:
            continue
        ccy = norm_symbol(row.get("currency"))
        rate = confirmed_fx_rate(rates, ccy)
        avg_cost = parse_float(row.get("avgCost") or row.get("avgPrice") or row.get("avg_price"), 0.0) * rate
        market_value = position_market_value_eur(row, rates)
        last_price = market_value / qty if qty else 0.0
        unrealized = (last_price - avg_cost) * qty
        weight = market_value / total_value_eur if total_value_eur > 0 else 0.0
        rows.append([run_id, ts, symbol, qty, avg_cost, last_price, money(market_value), money(unrealized), weight])
    return rows


def confirmed_fx_rate(rates, currency):
    rate = 1.0 if currency == "EUR" else parse_float(rates.get(currency), 0.0)
    if rate <= 0:
        raise RuntimeError("RECON_FX_RATE_UNAVAILABLE:" + currency)
    return rate


def measured_snapshot_risk(con, ts, nav, cash, positions):
    peak = parse_float(con.execute("SELECT MAX(total_value_eur) FROM core.portfolio_snapshot WHERE ts <= ?", [ts]).fetchone()[0], nav)
    drawdown = nav / max(nav, peak, 1e-9) - 1.0
    previous = con.execute("SELECT total_value_eur, ts FROM core.portfolio_snapshot WHERE ts < date_trunc('day', CAST(? AS TIMESTAMPTZ)) ORDER BY ts DESC LIMIT 1", [ts]).fetchone()
    daily_return = None
    if previous and parse_float(previous[0], 0) > 0:
        flows = con.execute("SELECT COALESCE(SUM(amount), 0) FROM core.cash_ledger WHERE ts > ? AND ts <= ? AND UPPER(type) IN ('DEPOSIT','WITHDRAWAL','EXTERNAL_DEPOSIT','EXTERNAL_WITHDRAWAL') AND currency='EUR'", [previous[1], ts]).fetchone()[0]
        daily_return = (nav - float(flows)) / float(previous[0]) - 1
    sectors = dict(con.execute("SELECT symbol, COALESCE(sector, 'UNKNOWN') FROM core.instruments").fetchall())
    totals, unknown = {}, 0.0
    for row in positions:
        sector = sectors.get(row[2]) or "UNKNOWN"
        value = parse_float(row[6], 0.0)
        totals[sector] = totals.get(sector, 0.0) + value
        if sector.upper() == "UNKNOWN":
            unknown += value
    top_sector = max(totals.values(), default=0.0) / nav if nav > 0 and unknown == 0 else None
    booked_ai = parse_float(con.execute("SELECT SUM(ABS(amount)) FROM core.cash_ledger WHERE UPPER(type)='AI_COST' AND ts <= ?", [ts]).fetchone()[0], 0.0)
    cash_pct = cash / nav if nav > 0 else 0.0
    status = "DEFENSIVE" if cash_pct >= .8 else "RISK_ON" if cash_pct <= .1 else "BALANCED"
    meta = {"method_version": "performance_contract_v1", "drawdown_method": "observed_NAV_peak_not_flow_adjusted",
            "daily_return_pct": daily_return * 100 if daily_return is not None else None,
            "daily_reference_at": str(previous[1]) if previous else None,
            "daily_method": "previous_UTC_day_last_NAV_external_EUR_flows_at_end",
            "external_flow_coverage": "BOOKED_ONLY_NOT_STATEMENT_RECONCILED",
            "ai_cost_coverage": "BOOKED_ONLY_EXTERNAL_BILLING_UNKNOWN", "var_method": "NOT_ESTIMATED",
            "unknown_sector_value_eur": unknown}
    return {"drawdown": drawdown, "top_sector": top_sector, "booked_ai": booked_ai, "status": status, "meta": meta}


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

    measured = measured_snapshot_risk(con, ts, total_value_eur, cash_eur, position_rows)
    con.execute("DELETE FROM core.portfolio_snapshot WHERE run_id = ?", [run_id])
    con.execute(
        """
        INSERT INTO core.portfolio_snapshot (
          run_id, ts, cash_eur, equity_eur, total_value_eur, cum_fees_eur, cum_ai_cost_eur,
          trades_this_run, total_pnl_eur, roi, drawdown_pct, meta_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            ts,
            money(cash_eur),
            money(equity_eur),
            money(total_value_eur),
            money(cum_fees),
            money(measured["booked_ai"]),
            len(missing),
            money(total_value_eur - initial),
            (total_value_eur - initial) / initial if initial else 0.0,
            measured["drawdown"],
            json.dumps(
                {
                    "source": "ibkr_pf_reconcile",
                    "account": EXPECTED_ACCOUNT,
                    "base_ledger": base,
                    "rates": rates,
                    "position_filter": cfg.get("ibkr_position_filter") or {},
                    **measured["meta"],
                },
                ensure_ascii=False,
            ),
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
            measured["top_sector"],
            None,
            len(position_rows),
            measured["status"],
            json.dumps({"source": "ibkr_pf_reconcile", **measured["meta"]}, ensure_ascii=False),
        ],
    )

    price_rows = []
    for row in active_position_rows(positions):
        symbol = ibkr_internal_symbol(row)
        qty = position_quantity(row)
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
    for row in active_position_rows(positions):
        symbol = ibkr_internal_symbol(row)
        qty = position_quantity(row)
        if not symbol or qty <= 0:
            continue
        ccy = norm_symbol(row.get("currency"))
        rate = confirmed_fx_rate(rates, ccy)
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
    IBKR_SYMBOL_BY_CONID, conid_symbol_conflicts = build_conid_symbol_map(con, fills)
    cfg["ibkr_conid_symbol_map_count"] = len(IBKR_SYMBOL_BY_CONID)
    cfg["ibkr_conid_symbol_conflicts"] = conid_symbol_conflicts
    missing, unmatched = build_missing_fills(con, fills, rates)
    filtered_positions, filter_diag = filter_positions_against_ledger(con, positions, rates, ledger)
    cfg["ibkr_position_filter"] = filter_diag
    write_needed, position_diffs, value_diffs = should_write(con, filtered_positions, ledger, missing)
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
    # The late EOD slot records an observation even if NAV changed by less than EUR 1.
    if write_needed or now_utc().hour >= 21:
        con.execute("BEGIN TRANSACTION")
        try:
            upsert_instruments_for_positions(con, filtered_positions)
            insert_missing_fills(con, missing)
            lot_count = rebuild_position_lots(con)
            filtered_positions, filter_diag = filter_positions_against_ledger(con, positions, rates, ledger)
            cfg["ibkr_position_filter"] = filter_diag
            cfg["ibkr_live_mtm_count"] = upsert_ibkr_positions_latest(con, filtered_positions, rates, iso_now())
            cfg["ibkr_live_mtm_written"] = True
            run_id = insert_reconciliation_run_and_snapshot(con, filtered_positions, ledger, rates, missing, cfg)
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
        # FIX P&L live 2026-06-22 + 2026-07-03 stale-position guard: refresh the
        # IBKR-sourced MTM every run, but only after filtering temporary /positions
        # rows that conflict with ledger NetLiq and closed lots.
        try:
            cfg["ibkr_live_mtm_count"] = upsert_ibkr_positions_latest(con, filtered_positions, rates, iso_now())
            cfg["ibkr_live_mtm_written"] = True
        except Exception as _live_err:
            cfg["ibkr_live_mtm_written"] = False
            cfg["ibkr_live_mtm_error"] = str(_live_err)
        cfg["ibkr_reconcile_status"] = "NO_DIFF"
finally:
    con.close()

return [{"json": cfg}]
