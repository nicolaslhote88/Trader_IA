import math
import os
import json
import time
from pathlib import Path
import urllib.request

import duckdb


ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("db_path") or "/files/duckdb/ag1_fx_v1_chatgpt52.duckdb"
schema_path = ctx.get("schema_path") or "/files/AG1-FX-V1-EXPORT/sql/ag1_fx_v1_schema.sql"
ag2_path = ctx.get("ag2_fx_path") or "/files/duckdb/ag2_fx_v1.duckdb"
broker_url = os.environ.get("IBKR_BROKER_URL", "http://ibkr-broker:8080").rstrip("/")
ibkr_dry_run = os.environ.get("IBKR_DRY_RUN", "true").lower() != "false"
require_paper = os.environ.get("IBKR_REQUIRE_PAPER_ACCOUNT", "true").lower() != "false"
paper_prefixes = tuple(p.strip().upper() for p in os.environ.get("IBKR_PAPER_ACCOUNT_PREFIXES", "DU").split(",") if p.strip())
reconcile_cash_balances_enabled = os.environ.get("IBKR_RECONCILE_CASH_BALANCES", "true").lower() != "false"
block_on_cash_divergence = os.environ.get("IBKR_BLOCK_ON_CASH_DIVERGENCE", "false").lower() == "true"
cash_recon_threshold_units = float(os.environ.get("IBKR_CASH_RECON_THRESHOLD_UNITS", "5") or 5)
portfolio_base_ccy = os.environ.get("AG1_FX_PORTFOLIO_BASE_CCY", "EUR").upper()
lock_ttl_seconds = int(float(os.environ.get("AG1_FX_LOCK_TTL_SECONDS", "2700") or 2700))
lock_path = os.environ.get("AG1_FX_LOCK_PATH", "/files/locks/ag1_fx_active.lock")


def to_float(value, default=0.0):
    try:
        if value is None:
            return default
        number = float(value)
        return number if math.isfinite(number) else default
    except Exception:
        return default


def to_text(value):
    if value is None:
        return ""
    text = str(value)
    return "" if text.lower() in {"nan", "nat", "none", "null"} else text.strip()


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


def acquire_lock(path_text):
    if os.environ.get("AG1_FX_DISABLE_LOCK", "").lower() in {"1", "true", "yes"}:
        return {"enabled": False}
    path = Path(path_text)
    path.parent.mkdir(parents=True, exist_ok=True)
    now = time.time()
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            age = now - float(payload.get("created_ts") or 0)
        except Exception:
            age = now - path.stat().st_mtime
        if age < lock_ttl_seconds:
            raise RuntimeError(f"AG1_FX_PORTFOLIO_LOCK_ACTIVE:{path}:age_seconds={age:.0f}")
        path.unlink(missing_ok=True)
    payload = {
        "run_id": ctx.get("run_id"),
        "variant": ctx.get("variant"),
        "db_path": db_path,
        "created_ts": now,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
    }
    fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(payload, fh)
    return {"enabled": True, "path": str(path), **payload}


def get_broker_json(path, timeout=12):
    req = urllib.request.Request(f"{broker_url}{path}", headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def paper_account_ids(positions_payload):
    ids = []
    for row in positions_payload or []:
        acct = str(row.get("acctId") or row.get("accountId") or row.get("account") or "").strip().upper()
        if acct and acct not in ids:
            ids.append(acct)
    return ids


def norm_pair(text):
    letters = "".join(ch for ch in str(text or "").upper() if ch.isalpha())
    return letters


def ibkr_fx_pair(row, universe_pairs):
    haystack = " ".join(
        str(row.get(k) or "")
        for k in (
            "contractDesc",
            "contract_description_1",
            "description",
            "symbol",
            "ticker",
            "fullName",
            "name",
        )
    ).upper()
    compact = norm_pair(haystack)
    for pair in universe_pairs:
        if pair in compact:
            return pair
        dotted = f"{pair[:3]}.{pair[3:]}"
        if dotted in haystack:
            return pair
    asset = str(row.get("assetClass") or row.get("sec_type") or row.get("secType") or "").upper()
    if asset in {"CASH", "FX", "FOREX", "CURRENCY"}:
        ccy = str(row.get("currency") or "").upper()
        desc = norm_pair(row.get("contractDesc") or row.get("symbol") or row.get("ticker"))
        for pair in universe_pairs:
            if desc in {pair, pair[:3], pair[3:]} or ccy in {pair[:3], pair[3:]}:
                return pair
    return ""


def db_units_by_pair(lots):
    out = {}
    for lot in lots or []:
        pair = str(lot.get("pair") or "").upper()
        sign = -1.0 if str(lot.get("side") or "").lower() == "short" else 1.0
        out[pair] = out.get(pair, 0.0) + sign * to_float(lot.get("size_lots"), 0.0) * 100000.0
    return out


def db_cash_effect_by_currency(lots):
    out = {}

    def add(ccy, value):
        if ccy:
            out[ccy] = out.get(ccy, 0.0) + value

    for lot in lots or []:
        pair = str(lot.get("pair") or "").upper()
        if len(pair) != 6:
            continue
        units = to_float(lot.get("size_lots"), 0.0) * 100000.0
        px = to_float(lot.get("open_price"), 0.0)
        if units <= 0 or px <= 0:
            continue
        sign = -1.0 if str(lot.get("side") or "").lower() == "short" else 1.0
        add(pair[:3], sign * units)
        add(pair[3:6], -sign * units * px)
    return out


def ibkr_units_by_pair(positions, universe_pairs):
    out = {}
    rows = []
    for row in positions or []:
        qty = to_float(row.get("position"), 0.0)
        if abs(qty) <= 1e-9:
            continue
        pair = ibkr_fx_pair(row, universe_pairs)
        if not pair:
            continue
        out[pair] = out.get(pair, 0.0) + qty
        rows.append(row)
    return out, rows


def parse_number_value(value):
    text = to_text(value).upper().replace(",", "")
    if not text:
        return 0.0
    mult = 1.0
    if text.endswith("K"):
        mult = 1000.0
        text = text[:-1]
    elif text.endswith("M"):
        mult = 1000000.0
        text = text[:-1]
    try:
        return float(text) * mult
    except Exception:
        return 0.0


def parse_ibkr_position_value(value):
    return parse_number_value(value)


def iter_ledger_rows(payload):
    if isinstance(payload, list):
        for row in payload:
            if isinstance(row, dict):
                yield "", row
        return
    if not isinstance(payload, dict):
        return
    for key, value in payload.items():
        if isinstance(value, dict):
            yield str(key), value
        elif isinstance(value, list):
            for row in value:
                if isinstance(row, dict):
                    yield str(key), row


def cash_balances_from_ledger(payload):
    balances = {}
    raw_rows = []
    for key, row in iter_ledger_rows(payload):
        ccy = to_text(row.get("currency") or row.get("ccy") or key).upper()
        if not ccy or ccy == "BASE":
            continue
        value = None
        for field in ("cashbalance", "cashBalance", "settledcash", "settledCash", "totalcash", "totalCash"):
            if row.get(field) not in (None, ""):
                value = parse_number_value(row.get(field))
                break
        if value is None:
            continue
        balances[ccy] = balances.get(ccy, 0.0) + value
        raw_rows.append({k: row.get(k) for k in ("currency", "cashbalance", "settledcash", "totalcash") if k in row})
    return balances, raw_rows[:20]


def cash_balance_reconciliation(lots, cfg, ledger_payload):
    db_effects = db_cash_effect_by_currency(lots)
    ibkr_cash, raw_rows = cash_balances_from_ledger(ledger_payload)
    initial = to_float(cfg.get("initial_capital_eur"), 10000.0)
    deltas = {}
    base_delta = None
    for ccy in sorted(set(db_effects) | set(ibkr_cash)):
        broker = ibkr_cash.get(ccy, 0.0)
        expected_effect = db_effects.get(ccy, 0.0)
        if ccy == portfolio_base_ccy:
            base_delta = round(broker - (initial + expected_effect), 4)
            continue
        delta = broker - expected_effect
        if abs(delta) > cash_recon_threshold_units:
            deltas[ccy] = round(delta, 4)
    return {
        "enabled": reconcile_cash_balances_enabled,
        "blocking": block_on_cash_divergence,
        "threshold_units": cash_recon_threshold_units,
        "base_currency": portfolio_base_ccy,
        "db_cash_effect_by_currency": {k: round(v, 4) for k, v in sorted(db_effects.items()) if abs(v) > 1e-9},
        "ibkr_cash_by_currency": {k: round(v, 4) for k, v in sorted(ibkr_cash.items()) if abs(v) > 1e-9},
        "currency_deltas": deltas,
        "base_currency_delta_info": base_delta,
        "raw_rows_sample": raw_rows,
        "ok": not bool(deltas),
    }


def fx_units_from_recent_fills(fills_payload, universe_pairs):
    latest = {}
    for row in fills_payload or []:
        if not isinstance(row, dict):
            continue
        asset = to_text(row.get("assetClass") or row.get("sec_type") or row.get("secType")).upper()
        if asset not in {"CASH", "FX", "FOREX", "CURRENCY"}:
            continue
        pair = ibkr_fx_pair(row, universe_pairs)
        if not pair:
            continue
        qty = parse_ibkr_position_value(row.get("position"))
        ts = to_float(row.get("trade_time_r"), 0.0)
        prev = latest.get(pair)
        if prev is None or ts >= prev[0]:
            latest[pair] = (ts, qty)
    return {pair: qty for pair, (_, qty) in latest.items() if abs(qty) > 1e-9}


def reconcile_ibkr_state(lots, cfg):
    summary = {
        "enabled": not ibkr_dry_run,
        "broker_url": broker_url,
        "ok": True,
        "block_new_orders": False,
        "reasons": [],
        "health": {},
        "paper_account_guard": {"required": require_paper, "prefixes": list(paper_prefixes), "detected_accounts": []},
        "db_units_by_pair": {},
        "ibkr_units_by_pair": {},
        "cash_balances": {"enabled": reconcile_cash_balances_enabled},
        "deltas": {},
    }
    if ibkr_dry_run:
        return summary
    try:
        health = get_broker_json("/health")
        positions = get_broker_json("/positions")
        fills_payload = get_broker_json("/fills")
        summary["health"] = health
        accounts = paper_account_ids(positions)
        summary["paper_account_guard"]["detected_accounts"] = accounts
        if not health.get("authenticated"):
            summary["reasons"].append("IBKR_BROKER_NOT_AUTHENTICATED")
        if health.get("dry_run"):
            summary["reasons"].append("IBKR_BROKER_STILL_IN_DRY_RUN")
        if require_paper and (not accounts or not all(acct.startswith(paper_prefixes) for acct in accounts)):
            summary["reasons"].append(f"IBKR_PAPER_ACCOUNT_GUARD_FAILED:{accounts}")
        universe_pairs = sorted({str(r.get("pair") or "").upper() for r in ctx.get("universe_fx", []) if r.get("pair")})
        db_units = db_units_by_pair(lots)
        ibkr_units, fx_rows = ibkr_units_by_pair(positions, universe_pairs)
        fill_units = fx_units_from_recent_fills(fills_payload, universe_pairs)
        for pair, qty in fill_units.items():
            ibkr_units[pair] = qty
        summary["db_units_by_pair"] = {k: round(v, 4) for k, v in sorted(db_units.items()) if abs(v) > 1e-9}
        summary["ibkr_units_by_pair"] = {k: round(v, 4) for k, v in sorted(ibkr_units.items()) if abs(v) > 1e-9}
        summary["ibkr_units_source"] = "positions_plus_recent_fills"
        summary["ibkr_fx_positions_count"] = len(fx_rows)
        all_pairs = sorted(set(db_units) | set(ibkr_units))
        for pair in all_pairs:
            delta = ibkr_units.get(pair, 0.0) - db_units.get(pair, 0.0)
            if abs(delta) > 1.0:
                summary["deltas"][pair] = round(delta, 4)
        if summary["deltas"]:
            summary["reasons"].append("IBKR_DUCKDB_POSITION_DIVERGENCE")
        if reconcile_cash_balances_enabled:
            try:
                ledger_payload = get_broker_json("/account/ledger")
                cash_summary = cash_balance_reconciliation(lots, cfg, ledger_payload)
                summary["cash_balances"] = cash_summary
                if cash_summary.get("currency_deltas") and block_on_cash_divergence:
                    summary["reasons"].append("IBKR_DUCKDB_CASH_BALANCE_DIVERGENCE")
            except Exception as ledger_exc:
                summary["cash_balances"] = {
                    "enabled": True,
                    "ok": False,
                    "blocking": block_on_cash_divergence,
                    "error": str(ledger_exc),
                }
                if block_on_cash_divergence:
                    summary["reasons"].append(f"IBKR_LEDGER_RECONCILIATION_FAILED:{ledger_exc}")
    except Exception as exc:
        summary["ok"] = False
        summary["reasons"].append(f"IBKR_RECONCILIATION_FAILED:{exc}")

    if summary["reasons"]:
        summary["ok"] = False
        summary["block_new_orders"] = True
        cfg["kill_switch_active"] = True
    return summary


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


lock_info = acquire_lock(lock_path)

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
    reconciliation = reconcile_ibkr_state(lots, cfg)
    if table_exists(con, "core", "reconciliation_log"):
        con.execute(
            """
            INSERT OR REPLACE INTO core.reconciliation_log (
              reconciliation_id, run_id, as_of, source, status, block_new_orders,
              reasons_json, db_positions_json, broker_positions_json, deltas_json, payload_json
            ) VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                f"REC_{ctx.get('run_id')}_PRE",
                ctx.get("run_id"),
                "AG1-FX-V1 pre-run",
                "OK" if reconciliation.get("ok") else "FAILED",
                bool(reconciliation.get("block_new_orders")),
                json.dumps(reconciliation.get("reasons") or [], ensure_ascii=False),
                json.dumps(reconciliation.get("db_units_by_pair") or {}, ensure_ascii=False),
                json.dumps(reconciliation.get("ibkr_units_by_pair") or {}, ensure_ascii=False),
                json.dumps(reconciliation.get("deltas") or {}, ensure_ascii=False),
                json.dumps(reconciliation, ensure_ascii=False),
            ],
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
available_cash = max(0.0, equity - margin_used)
leverage_effective = notional / equity if equity > 0 else 0.0
day_start = to_float(day_row[0], equity) if day_row else equity
peak = max(initial, equity, to_float(peak_row[0], initial) if peak_row else initial)
drawdown_day = equity / day_start - 1.0 if day_start > 0 else 0.0
drawdown_total = equity / peak - 1.0 if peak > 0 else 0.0
latest_snapshot_equity = to_float(snap_rows[0].get("equity_eur"), equity) if snap_rows else equity
max_daily_drawdown = to_float(cfg.get("max_daily_drawdown_pct"), 0.05)
kill_switch_auto_reset = bool(cfg.get("kill_switch_active")) and min(0.0, drawdown_day) > -max_daily_drawdown
if kill_switch_auto_reset:
    cfg["kill_switch_active"] = False

state = {
    "cash_eur": available_cash,
    "available_cash_eur": available_cash,
    "ledger_cash_eur": cash,
    "equity_eur": equity,
    "realized_pnl_eur": realized,
    "floating_pnl_eur": floating,
    "fees_eur": fees,
    "gross_exposure_eur": notional,
    "margin_used_eur": margin_used,
    "margin_free_eur": available_cash,
    "open_lots": lots,
    "open_lots_count": len(lots),
    "leverage_effective": leverage_effective,
    "drawdown_day_frac": min(0.0, drawdown_day),
    "drawdown_total_frac": min(0.0, drawdown_total),
    "drawdown_day_pct_display": min(0.0, drawdown_day) * 100.0,
    "drawdown_total_pct_display": min(0.0, drawdown_total) * 100.0,
    # Backward-compatible legacy names: these are fractions, despite "_pct".
    "drawdown_day_pct": min(0.0, drawdown_day),
    "drawdown_total_pct": min(0.0, drawdown_total),
    "valuation_source": "live_recomputed_from_corrected_lots_and_latest_ag2",
    "latest_snapshot_equity_eur": latest_snapshot_equity,
    "latest_snapshot_delta_eur": equity - latest_snapshot_equity,
    "kill_switch_auto_reset": kill_switch_auto_reset,
    "valuation_warnings": {
        "ag2_price_error": price_error,
        "missing_prices": sorted(set(missing_prices)),
        "missing_conversions": sorted(set(missing_conversions)),
    },
}

cfg["llm_model"] = ctx.get("llm_model") or cfg.get("llm_model") or "unset"
with duckdb.connect(db_path) as con:
    con.execute(
        "UPDATE cfg.portfolio_config SET llm_model = ?, kill_switch_active = ?, updated_at = CURRENT_TIMESTAMP WHERE config_key = 'default'",
        [cfg["llm_model"], bool(cfg.get("kill_switch_active"))],
    )

state["reconciliation"] = reconciliation

return [{
    "json": {
        **ctx,
        "config": cfg,
        "portfolio_state": state,
        "ibkr_reconciliation": reconciliation,
        "reconciliation_block_new_orders": bool(reconciliation.get("block_new_orders")),
        "ag1_fx_lock": lock_info,
    }
}]
