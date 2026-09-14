import gc
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import duckdb

DB_PATH_DEFAULT = os.getenv("AG1_V4_DUCKDB_PATH", "/files/duckdb/ag1_v4_consensus.duckdb")
MAX_IDEAS_IN_BRIEF = 20


def to_num(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        s = str(v).replace("EUR", "").replace("€", "").replace("\u00a0", "").replace("\u202f", "").replace(" ", "")
        s = s.replace(",", ".")
        n = float(s)
        return n if n == n else default
    except Exception:
        return default


def to_int(v, default=None):
    n = to_num(v, None)
    if n is None:
        return default
    try:
        return int(round(n))
    except Exception:
        return default


def round2(v):
    return round(float(v or 0.0), 2)


def norm_text(v):
    return str(v or "").strip().upper()


def is_unknown_text(v):
    s = norm_text(v)
    return (not s) or s in {"UNKNOWN", "N/A", "NA", "NONE", "NULL", "-"}


def normalize_asset_class(asset_class, symbol=None):
    a = norm_text(asset_class)
    if a in {"EQUITY", "STOCK", "ETF", "CRYPTO"}:
        return "EQUITY" if a == "STOCK" else a
    return a or None


def norm_symbol(v, asset_class_hint=None):
    return str(v or "").strip().upper()


def infer_side_from_action_or_signal(action=None, signal=None):
    a = norm_text(action)
    if a in {"OPEN", "INCREASE", "BUY", "PROPOSE_OPEN"}:
        return "BUY"
    if a in {"CLOSE", "DECREASE", "SELL", "PROPOSE_CLOSE"}:
        return "SELL"
    s = norm_text(signal)
    if s == "BUY":
        return "BUY"
    if s == "SELL":
        return "SELL"
    return None


def normalize_risk_values(stop_loss_pct, take_profit_pct, action=None, signal=None):
    sl = to_num(stop_loss_pct, None)
    tp = to_num(take_profit_pct, None)
    if sl is not None:
        sl = -abs(sl)
    if tp is not None:
        tp = abs(tp)
    return sl, tp


def to_iso(v, default=None):
    try:
        if v is None:
            return default
        if isinstance(v, datetime):
            dt = v
        else:
            s = str(v).strip()
            if not s:
                return default
            dt = None
            try:
                n = float(s)
                if n == n:
                    if abs(n) >= 1e11:
                        n = n / 1000.0
                    dt = datetime.fromtimestamp(n, tz=timezone.utc)
            except Exception:
                dt = None
            if dt is None:
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return default


def parse_ts_key(v):
    try:
        s = to_iso(v, default="")
        if not s:
            return 0.0
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return 0.0


def is_cash_row(r):
    sym = norm_text(r.get("Symbol") or r.get("symbol"))
    name = norm_text(r.get("Name") or r.get("name"))
    asset = norm_text(r.get("AssetClass") or r.get("assetClass") or r.get("asset_class"))
    sector = norm_text(r.get("Sector") or r.get("sector"))
    return (
        sym in ("CASH_EUR", "CASH", "EUR_CASH", "LIQUIDITE", "LIQUIDITES")
        or "CASH" in name
        or "LIQUIDITE" in name
        or asset == "CASH"
        or sector == "CASH"
    )


def is_meta_row(r):
    sym = norm_text(r.get("Symbol") or r.get("symbol"))
    name = norm_text(r.get("Name") or r.get("name"))
    return sym == "__META__" or name == "__META__"


def table_exists(con, table_name):
    if "." in table_name:
        schema, name = table_name.split(".", 1)
    else:
        schema, name = "main", table_name
    try:
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE lower(table_schema) = lower(?)
              AND lower(table_name) = lower(?)
            LIMIT 1
            """,
            [schema, name],
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def query_rows(con, sql, params=None):
    try:
        cur = con.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception:
        return []


@contextmanager
def db_con(path, retries=6, delay=0.2):
    con = None
    for i in range(retries):
        try:
            con = duckdb.connect(path, read_only=True)
            break
        except Exception as exc:
            if ("lock" in str(exc).lower() or "busy" in str(exc).lower()) and i < retries - 1:
                time.sleep(delay * (2**i))
            else:
                con = None
                break
    try:
        yield con
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        gc.collect()


def normalize_db_path(path_value):
    s = str(path_value or "").strip().replace("\\", "/")
    if not s:
        s = DB_PATH_DEFAULT
    s = s.replace("/local-files/", "/files/")
    return s


def candidate_db_paths(path_value):
    cands = []

    def add_path_and_mount_aliases(raw_path):
        base = normalize_db_path(raw_path)
        if not base:
            return
        cands.append(base)
        if "/files/" in base:
            cands.append(base.replace("/files/", "/local-files/", 1))
        elif "/local-files/" in base:
            cands.append(base.replace("/local-files/", "/files/", 1))

    add_path_and_mount_aliases(path_value)
    add_path_and_mount_aliases(DB_PATH_DEFAULT)

    out = []
    seen = set()
    for c in cands:
        cc = str(c or "").strip()
        if cc and cc not in seen:
            out.append(cc)
            seen.add(cc)
    return out


def probe_db_memory(path_value):
    probe = {
        "path": str(path_value or ""),
        "ok": False,
        "ai_signals_count": 0,
        "ai_signals_max_ts": None,
        "alerts_count": 0,
        "alerts_max_ts": None,
    }
    with db_con(path_value) as con:
        if con is None:
            return probe
        probe["ok"] = True
        if table_exists(con, "core.ai_signals"):
            rows = query_rows(con, "SELECT COUNT(*) AS c, MAX(epoch_ms(ts)) AS max_ts_ms FROM core.ai_signals")
            if rows:
                probe["ai_signals_count"] = to_int(rows[0].get("c"), 0) or 0
                probe["ai_signals_max_ts"] = to_iso(rows[0].get("max_ts_ms"), None)
        if table_exists(con, "core.alerts"):
            rows = query_rows(con, "SELECT COUNT(*) AS c, MAX(epoch_ms(ts)) AS max_ts_ms FROM core.alerts")
            if rows:
                probe["alerts_count"] = to_int(rows[0].get("c"), 0) or 0
                probe["alerts_max_ts"] = to_iso(rows[0].get("max_ts_ms"), None)
    return probe


def pick_best_probe(probes):
    best = None
    best_key = None
    for p in probes:
        key = (
            1 if p.get("ok") else 0,
            to_int(p.get("ai_signals_count"), 0) or 0,
            parse_ts_key(p.get("ai_signals_max_ts")),
            to_int(p.get("alerts_count"), 0) or 0,
            parse_ts_key(p.get("alerts_max_ts")),
        )
        if best is None or key > best_key:
            best = p
            best_key = key
    return best


def load_fx_ref_map(db_path):
    """IBKR average cost translated at the snapshot FX; never historical EUR cost."""
    out = {}
    with db_con(db_path) as con:
        if con is None or not table_exists(con, "portfolio_positions_ibkr_latest"):
            return out
        rows = query_rows(con, """
            SELECT symbol, currency, quantity, fx_rate, avg_cost_eur, last_price_eur, updated_at
            FROM portfolio_positions_ibkr_latest
        """)
        for row in rows:
            sym = norm_symbol(row.get("symbol"))
            out[sym] = {
                "currency": norm_text(row.get("currency")),
                "fx": to_num(row.get("fx_rate"), None),
                "lp_eur": to_num(row.get("last_price_eur"), None),
                "avg_eur_at_current_fx": to_num(row.get("avg_cost_eur"), None),
                "quantity": to_num(row.get("quantity"), None),
                "updatedAt": to_iso(row.get("updated_at"), None),
            }
    return out


def load_portfolio_reference(db_path):
    with db_con(db_path) as con:
        if con is None:
            return {}
        rows = query_rows(con, "SELECT run_id, cash_eur, total_value_eur, epoch_ms(ts) AS ts_ms FROM core.portfolio_snapshot ORDER BY ts DESC, run_id DESC LIMIT 1")
        if not rows:
            return {}
        row = rows[0]
        previous = query_rows(con, "SELECT total_value_eur, epoch_ms(ts) AS ts_ms FROM core.portfolio_snapshot WHERE ts < date_trunc('day', now()) ORDER BY ts DESC LIMIT 1")
        row["dailyReturnPct"] = None
        if previous and to_num(previous[0].get("total_value_eur"), 0) > 0:
            flows = query_rows(con, "SELECT COALESCE(SUM(amount),0) AS amount FROM core.cash_ledger WHERE ts > to_timestamp(? / 1000.0) AND ts <= to_timestamp(? / 1000.0) AND UPPER(type) IN ('DEPOSIT','WITHDRAWAL','EXTERNAL_DEPOSIT','EXTERNAL_WITHDRAWAL') AND currency='EUR'", [previous[0]["ts_ms"], row["ts_ms"]])
            row["dailyReturnPct"] = ((to_num(row["total_value_eur"], 0) - to_num(flows[0]["amount"], 0)) / to_num(previous[0]["total_value_eur"], 0) - 1) * 100
            row["dailyReferenceAt"] = to_iso(previous[0]["ts_ms"], None)
        return row


def load_position_overrides(db_path, snapshot_run_id=None):
    """One coherent portfolio snapshot. An older MTM must not replace its quantities."""
    out = {}
    with db_con(db_path) as con:
        if con is None:
            return out, None
        if table_exists(con, "core.positions_snapshot"):
            rows = query_rows(con, """
                SELECT p.symbol, p.qty, p.avg_cost, p.last_price, p.market_value_eur,
                       p.unrealized_pnl_eur, epoch_ms(p.ts) AS ts_ms
                FROM core.positions_snapshot p
                WHERE p.run_id = COALESCE(?, (SELECT run_id FROM core.portfolio_snapshot
                                  ORDER BY ts DESC, run_id DESC LIMIT 1))
            """, [snapshot_run_id])
            for row in rows:
                out[norm_symbol(row.get("symbol"))] = {
                    "quantity": to_num(row.get("qty"), None),
                    "avgPrice": to_num(row.get("avg_cost"), None),
                    "lastPrice": to_num(row.get("last_price"), None),
                    "marketValue": to_num(row.get("market_value_eur"), None),
                    "unrealizedPnL": to_num(row.get("unrealized_pnl_eur"), None),
                    "updatedAt": to_iso(row.get("ts_ms"), None),
                }
            if rows or query_rows(con, "SELECT 1 FROM core.portfolio_snapshot LIMIT 1"):
                return out, "core.positions_snapshot"
    return out, None


def lifecycle_from_fills(rows):
    """Only fills establish dates. A flat position starts a new lifecycle on re-entry."""
    out = {}
    for row in rows:
        sym = norm_symbol(row.get("symbol"))
        qty = to_num(row.get("qty"), 0.0) or 0.0
        side = norm_text(row.get("side"))
        ts = to_iso(row.get("ts_ms"), None)
        if not sym or qty <= 0 or not ts or side not in ("BUY", "SELL"):
            continue
        rec = out.setdefault(sym, {"netQty": 0.0, "openedAt": None, "lastBuyAt": None,
                                  "lastSellAt": None, "source": "core.fills", "openingStopLossPct": None,
                                  "openingStopPriceNative": None, "openingFillId": None,
                                  "openingStopSource": None, "openingPriceNative": None})
        if side == "BUY":
            if rec["netQty"] <= 1e-8:
                native = to_num(row.get("price_native"), None)
                stop = to_num(row.get("stop_loss"), None)
                if stop is not None and not (-100 < stop < 0):
                    stop = None
                rec.update({"openedAt": ts, "openingFillId": row.get("fill_id"),
                            "openingPriceNative": native, "openingStopLossPct": stop,
                            "openingStopPriceNative": native * (1 + stop / 100) if native and stop else None,
                            "openingStopSource": "opening_signal_pct_at_fill" if native and stop else None})
            rec["lastBuyAt"] = ts
            rec["netQty"] += qty
        else:
            rec["lastSellAt"] = ts
            rec["netQty"] = max(0.0, rec["netQty"] - qty)
            if rec["netQty"] <= 1e-8:
                rec["openedAt"] = None
                rec["openingFillId"] = None
                rec["openingStopLossPct"] = None
                rec["openingStopPriceNative"] = None
                rec["openingStopSource"] = None
    return out


def load_position_lifecycle(db_path):
    with db_con(db_path) as con:
        if con is None or not table_exists(con, "core.fills"):
            return {}
        signal_join = ""
        stop_select = "NULL AS stop_loss"
        if table_exists(con, "core.ai_signals"):
            signal_join = """LEFT JOIN (SELECT run_id, symbol, stop_loss FROM core.ai_signals
                QUALIFY row_number() OVER(PARTITION BY run_id, symbol ORDER BY ts DESC, signal_id DESC)=1) s
                ON s.run_id=o.run_id AND s.symbol=o.symbol"""
            stop_select = "s.stop_loss"
        rows = query_rows(con, """
            SELECT o.symbol, o.side, f.fill_id, f.qty, f.price_native,
                   epoch_ms(f.ts_fill) AS ts_ms, """ + stop_select + """
            FROM core.fills f JOIN core.orders o ON o.order_id=f.order_id
            """ + signal_join + """ ORDER BY f.ts_fill, f.fill_id
        """)
        result = lifecycle_from_fills(rows)
        if table_exists(con, "core.position_lots"):
            costs = query_rows(con, """
                SELECT symbol, SUM(CAST(remaining_qty AS DOUBLE)) AS remaining_qty,
                       SUM(CAST(remaining_qty AS DOUBLE) * open_price
                           + CASE WHEN open_qty>0 THEN CAST(open_fees_eur AS DOUBLE)
                             * CAST(remaining_qty AS DOUBLE)/CAST(open_qty AS DOUBLE) ELSE 0 END) AS paid_cost_eur
                FROM core.position_lots WHERE remaining_qty>0 GROUP BY symbol
            """)
            for row in costs:
                sym = norm_symbol(row.get("symbol"))
                if sym in result:
                    result[sym]["costBasisQty"] = to_num(row.get("remaining_qty"), None)
                    result[sym]["paidCostEUR"] = to_num(row.get("paid_cost_eur"), None)
        return result


def position_currency_contract(reference, lifecycle, quantity, updated_at, market_value, avg_price_eur, last_price_eur):
    currency = norm_text(reference.get("currency")) or None
    fx = to_num(reference.get("fx"), None)
    reference_qty = to_num(reference.get("quantity"), None)
    coherent = (reference_qty is not None and abs(reference_qty - quantity) < 1e-8
                and parse_ts_key(reference.get("updatedAt")) >= parse_ts_key(updated_at))
    if currency == "EUR":
        fx = 1.0
    avg_native = last_native = None
    if coherent and fx and fx > 0:
        avg_current = to_num(reference.get("avg_eur_at_current_fx"), None)
        last_current = to_num(reference.get("lp_eur"), None)
        avg_native = avg_current / fx if avg_current is not None else None
        last_native = last_current / fx if last_current is not None else None
    elif currency == "EUR":
        avg_native, last_native = avg_price_eur, last_price_eur
    cost_qty = to_num(lifecycle.get("costBasisQty"), None)
    paid_cost = to_num(lifecycle.get("paidCostEUR"), None)
    cost_coherent = cost_qty is not None and abs(cost_qty - quantity) < 1e-8
    if not cost_coherent:
        paid_cost = None
    return {
        "currency": currency, "priceCurrency": "EUR", "fxRateToEUR": fx if coherent else None,
        "fxAsOf": reference.get("updatedAt") if coherent else None,
        "nativePriceSource": "ibkr_snapshot" if coherent else ("eur_snapshot" if currency == "EUR" else None),
        "avgPriceNative": avg_native, "lastPriceNative": last_native,
        "avgPriceEUR": avg_price_eur, "lastPriceEUR": last_price_eur,
        "paidCostEUR": paid_cost,
        "paidCostSource": "remaining_lots_including_allocated_fees" if paid_cost is not None else None,
        "perfLocalPct": (last_native / avg_native - 1) * 100 if avg_native and last_native else None,
        "perfEURPct": (market_value / paid_cost - 1) * 100 if paid_cost and paid_cost > 0 else None,
        "unrealizedPnLEURAtPaidCost": market_value - paid_cost if paid_cost is not None else None,
    }


def load_instrument_overrides(db_path):
    out = {}
    with db_con(db_path) as con:
        if con is None or not table_exists(con, "core.instruments"):
            return out
        rows = query_rows(
            con,
            """
            SELECT
              symbol_key,
              symbol,
              name,
              asset_class,
              sector,
              industry,
              isin
            FROM (
              SELECT
                UPPER(TRIM(symbol)) AS symbol_key,
                symbol,
                name,
                asset_class,
                sector,
                industry,
                isin,
                ROW_NUMBER() OVER (
                  PARTITION BY UPPER(TRIM(symbol))
                  ORDER BY updated_at DESC NULLS LAST
                ) AS rn
              FROM core.instruments
            ) x
            WHERE rn = 1
            """,
        )
        for r in rows:
            sym = norm_symbol(r.get("symbol_key") or r.get("symbol"), r.get("asset_class"))
            if not sym:
                continue
            out[sym] = {
                "name": str(r.get("name") or "").strip(),
                "assetClass": normalize_asset_class(r.get("asset_class"), sym),
                "sector": str(r.get("sector") or "").strip(),
                "industry": str(r.get("industry") or "").strip(),
                "isin": str(r.get("isin") or "").strip(),
            }
    return out


def normalize_last_decision(d, symbol_hint=None):
    d = d or {}
    entry = d.get("entryPlan") if isinstance(d.get("entryPlan"), dict) else {}
    risk = d.get("riskPlan") if isinstance(d.get("riskPlan"), dict) else {}
    action = d.get("action")
    signal = d.get("signal")
    stop_loss_pct, take_profit_pct = normalize_risk_values(
        risk.get("stopLossPct"),
        risk.get("takeProfitPct"),
        action=action,
        signal=signal,
    )
    asset_class = normalize_asset_class(d.get("assetClass"), symbol_hint)
    return {
        "runId": d.get("runId"),
        "ts": to_iso(d.get("ts"), None),
        "action": action,
        "signal": signal,
        "confidence": to_int(d.get("confidence"), None),
        "horizonDays": to_int(d.get("horizonDays"), None),
        "nextReviewDays": to_int(d.get("nextReviewDays"), None),
        "targetQty": to_num(d.get("targetQty"), None),
        "targetWeightPct": to_num(d.get("targetWeightPct"), None),
        "entryPlan": {
            "orderType": entry.get("orderType"),
            "limitPrice": to_num(entry.get("limitPrice"), None),
            "timeInForce": entry.get("timeInForce"),
        },
        "riskPlan": {
            "stopLossPct": stop_loss_pct,
            "takeProfitPct": take_profit_pct,
            "maxLossEUR": to_num(risk.get("maxLossEUR"), None),
        },
        "rationale": d.get("rationale"),
        "dependencies": d.get("dependencies"),
        "assetClass": asset_class,
    }


def normalize_execution_memory(m):
    m = m or {}
    status = norm_text(m.get("lastExecutionStatus")) or "NO_ORDER"
    if status not in {"EXECUTED", "RESIZED", "SKIPPED", "NO_ORDER"}:
        status = "NO_ORDER"
    return {
        "lastOrderRunId": m.get("lastOrderRunId"),
        "lastOrderSide": m.get("lastOrderSide"),
        "lastOrderQtyRequested": to_num(m.get("lastOrderQtyRequested"), None),
        "lastOrderQtyExecuted": to_num(m.get("lastOrderQtyExecuted"), None),
        "lastOrderPrice": to_num(m.get("lastOrderPrice"), None),
        "lastExecutionStatus": status,
        "lastExecutionReason": m.get("lastExecutionReason"),
        "lastExecutionAlertCode": m.get("lastExecutionAlertCode"),
        "lastExecutionAlertMessage": m.get("lastExecutionAlertMessage"),
    }


def normalize_recent_idea(idea):
    idea = idea or {}
    entry = idea.get("entryPlan") if isinstance(idea.get("entryPlan"), dict) else {}
    risk = idea.get("riskPlan") if isinstance(idea.get("riskPlan"), dict) else {}
    symbol = norm_symbol(idea.get("symbol"), idea.get("assetClass"))
    action = idea.get("action")
    stop_loss_pct, take_profit_pct = normalize_risk_values(
        risk.get("stopLossPct"),
        risk.get("takeProfitPct"),
        action=action,
        signal=idea.get("signal"),
    )
    asset_class = normalize_asset_class(idea.get("assetClass"), symbol) or "EQUITY"
    return {
        "symbol": symbol,
        "ts": to_iso(idea.get("ts"), None),
        "action": action,
        "confidence": to_int(idea.get("confidence"), None),
        "targetQty": to_num(idea.get("targetQty"), None),
        "entryPlan": {
            "orderType": entry.get("orderType"),
            "limitPrice": to_num(entry.get("limitPrice"), None),
            "timeInForce": entry.get("timeInForce"),
        },
        "riskPlan": {
            "stopLossPct": stop_loss_pct,
            "takeProfitPct": take_profit_pct,
            "maxLossEUR": to_num(risk.get("maxLossEUR"), None),
        },
        "rationale": idea.get("rationale"),
        "executionStatus": idea.get("executionStatus"),
        "executionReason": idea.get("executionReason"),
        "executionAlertCode": idea.get("executionAlertCode"),
        "executionAlertMessage": idea.get("executionAlertMessage"),
        "requestedQty": to_num(idea.get("requestedQty"), None),
        "executedQty": to_num(idea.get("executedQty"), None),
        "assetClass": asset_class,
    }


def fmt_num(v, nd=2):
    if v is None:
        return "n/a"
    try:
        return f"{float(v):.{nd}f}"
    except Exception:
        return "n/a"


def compute_review_info(decision_ts, next_review_days):
    """Returns (decision_date_str, review_date_str, review_status_str) for display."""
    if not decision_ts:
        return None, None, None
    try:
        from datetime import timedelta
        s = str(decision_ts).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        decision_date = str(dt.date())
        if next_review_days is None:
            return decision_date, None, None
        review_dt = dt + timedelta(days=int(next_review_days))
        review_date = str(review_dt.date())
        today = datetime.now(timezone.utc).date()
        delta = (review_dt.date() - today).days
        if delta < 0:
            status = f"OVERDUE+{abs(delta)}j"
        elif delta == 0:
            status = "AUJOURD_HUI"
        else:
            status = f"dans_{delta}j"
        return decision_date, review_date, status
    except Exception:
        return None, None, None


def days_since(ts):
    if not ts:
        return None
    try:
        s = str(ts).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc).date() - dt.date()).days
    except Exception:
        return None


def compute_horizon_info(opened_ts, decision_ts, horizon_days):
    """Returns (base_label, base_date, horizon_date, horizon_status)."""
    if horizon_days is None:
        return None, None, None, None
    base_ts = opened_ts or decision_ts
    if not base_ts:
        return None, None, None, None
    try:
        from datetime import timedelta
        s = str(base_ts).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        horizon_dt = dt + timedelta(days=int(horizon_days))
        today = datetime.now(timezone.utc).date()
        delta = (horizon_dt.date() - today).days
        if delta < 0:
            status = f"OVERDUE+{abs(delta)}j"
        elif delta == 0:
            status = "AUJOURD_HUI"
        else:
            status = f"dans_{delta}j"
        return ("open" if opened_ts else "decision"), str(dt.date()), str(horizon_dt.date()), status
    except Exception:
        return None, None, None, None


def is_fx_symbol(symbol, asset_class=None):
    if str(symbol or "").upper().startswith("FX:") or str(symbol or "").upper().endswith("=X"):
        return True
    return str(asset_class or "").strip().upper() in {"CURRENCY", "FOREX", "FX"}


def build_agent_brief(summary, positions, recent_ideas):
    lines = [
        "ETAT DU PORTEFEUILLE (memoire decisionnelle + execution):",
        f"- Valeur totale: {fmt_num(summary.get('totalValue'))} EUR",
        f"- Cash: {fmt_num(summary.get('cash'))} EUR",
        f"- Valeur investie: {fmt_num(summary.get('marketValue'))} EUR",
        f"- Exposition actions: {fmt_num(summary.get('exposurePct'), 1)}%",
        f"- Positions: {int(summary.get('positionsCount') or 0)}",
        "",
        "POSITIONS ACTUELLES:",
    ]

    portfolio_symbols = set()

    if not positions:
        lines.append("(Aucune position en portefeuille)")
    else:
        for p in positions:
            d = p.get("lastDecision") or {}
            r = d.get("riskPlan") or {}
            e = p.get("executionMemory") or {}
            entry = d.get("entryPlan") or {}

            sym = p.get("symbol")
            if sym:
                portfolio_symbols.add(sym)

            lines.append(
                f"- {sym} ({p.get('name')}) [{p.get('sector')}]: qty={fmt_num(p.get('quantity'))} "
                f"avg={fmt_num(p.get('avgPrice'))} last={fmt_num(p.get('lastPrice'))} devise={'EUR' if '.' in str(sym) else 'USD'} "
                f"perfLocalPct={fmt_num(p.get('perfLocalPct'), 1)} "
                f"value={fmt_num(p.get('marketValue'))} pnlEUR={fmt_num(p.get('unrealizedPnL'))}"
            )
            opened_at = p.get("openedAt")
            holding_days = p.get("holdingDays")
            decision_date, review_date, review_status = compute_review_info(d.get("ts"), d.get("nextReviewDays"))
            horizon_base, horizon_base_date, horizon_date, horizon_status = compute_horizon_info(opened_at, d.get("ts"), d.get("horizonDays"))
            review_info = ""
            if opened_at:
                review_info += f", openedAt={str(opened_at)[:10]}"
                if holding_days is not None:
                    review_info += f", heldDays={holding_days}"
            if decision_date:
                review_info += f", decisionDt={decision_date}"
            if review_date:
                review_info += f", reviewDt={review_date}[{review_status}]"
            if horizon_date:
                review_info += f", horizonEnd={horizon_date}[{horizon_status};base={horizon_base}:{horizon_base_date}]"
            lines.append(
                "  These IA: "
                f"action={d.get('action') or 'n/a'}, signal={d.get('signal') or 'n/a'}, conf={fmt_num(d.get('confidence'), 0)}, "
                f"horizonDays={d.get('horizonDays') if d.get('horizonDays') is not None else 'n/a'}, "
                f"nextReviewDays={d.get('nextReviewDays') if d.get('nextReviewDays') is not None else 'n/a'}"
                + review_info
            )

            # Parametres: show only if there's an actionable order
            has_params = (
                d.get("targetQty") is not None
                or d.get("targetWeightPct") is not None
                or (entry.get("orderType") and entry.get("orderType") != "n/a")
            )
            if has_params:
                lines.append(
                    "  Parametres: "
                    f"targetQty={fmt_num(d.get('targetQty'))}, targetWeightPct={fmt_num(d.get('targetWeightPct'))}, "
                    f"entry(orderType={entry.get('orderType') or 'n/a'}, limitPrice={fmt_num(entry.get('limitPrice'))}, "
                    f"tif={entry.get('timeInForce') or 'n/a'})"
                )

            # Risk: show only if meaningful values (not all zero/null)
            sl = r.get("stopLossPct")
            tp = r.get("takeProfitPct")
            ml = r.get("maxLossEUR")
            has_risk = (
                (sl is not None and abs(float(sl or 0)) > 0.001)
                or (tp is not None and abs(float(tp or 0)) > 0.001)
                or ml is not None
            )
            if has_risk:
                lines.append(
                    "  Risk: "
                    f"stopLossPct={fmt_num(sl)}, takeProfitPct={fmt_num(tp)}, "
                    f"maxLossEUR={fmt_num(ml)}"
                )

            # Execution: show only if something was actually attempted/done
            exec_status = e.get("lastExecutionStatus") or "NO_ORDER"
            if exec_status != "NO_ORDER":
                lines.append(
                    "  Execution: "
                    f"status={exec_status}, reason={e.get('lastExecutionReason') or 'n/a'}, "
                    f"requested={fmt_num(e.get('lastOrderQtyRequested'))}, executed={fmt_num(e.get('lastOrderQtyExecuted'))}, "
                    f"price={fmt_num(e.get('lastOrderPrice'))}"
                )

            if d.get("rationale"):
                lines.append(f"  Rationale: {str(d.get('rationale')).strip()}")

            # Dependencies: show only if something needs attention
            deps = d.get("dependencies") or {}
            has_deps = isinstance(deps, dict) and any(v is True for v in deps.values())
            if has_deps:
                lines.append(f"  Dependencies: {deps}")

    lines.extend(["", "IDEES RECENTES NON EXECUTEES:"])
    if not recent_ideas:
        lines.append("(Aucune idee non executee recente)")
    else:
        count = 0
        for idea in recent_ideas:
            if count >= MAX_IDEAS_IN_BRIEF:
                break
            sym = idea.get("symbol") or ""
            if is_fx_symbol(sym, idea.get("assetClass")):
                continue
            # Skip stale ideas for symbols already managed in current portfolio
            if sym and sym in portfolio_symbols:
                continue
            lines.append(
                f"- {sym} | action={idea.get('action')} | targetQty={fmt_num(idea.get('targetQty'))}"
            )
            if idea.get("rationale"):
                lines.append(f"  rationale: {str(idea.get('rationale')).strip()}")
            count += 1

    return "\n".join(lines)


incoming = _items or []
input0 = incoming[0].get("json", {}) if incoming else {}

rows_from_4b = input0.get("portfolioRows", []) if isinstance(input0, dict) else []
portfolio_summary_in = input0.get("portfolioSummary", {}) if isinstance(input0, dict) else {}
decision_memory = input0.get("portfolioDecisionMemory", {}) if isinstance(input0, dict) else {}
execution_memory = input0.get("portfolioExecutionMemory", {}) if isinstance(input0, dict) else {}
recent_ideas_in = input0.get("recentUnexecutedIdeas", []) if isinstance(input0, dict) else []
memory_diagnostics_in = input0.get("memoryDiagnostics", {}) if isinstance(input0, dict) else {}

db_path_raw = None
if isinstance(input0, dict):
    db_path_raw = input0.get("db_path") or input0.get("ag1_db_path") or DB_PATH_DEFAULT
db_candidates = candidate_db_paths(db_path_raw)
db_probe_results = [probe_db_memory(p) for p in db_candidates]
db_probe_selected = pick_best_probe(db_probe_results) or {"path": normalize_db_path(db_path_raw)}
db_path = str(db_probe_selected.get("path") or normalize_db_path(db_path_raw))

portfolio_reference = load_portfolio_reference(db_path)
position_overrides, override_source = load_position_overrides(db_path, portfolio_reference.get("run_id"))
instrument_overrides = load_instrument_overrides(db_path)
position_lifecycle = load_position_lifecycle(db_path)
fx_ref_map = load_fx_ref_map(db_path)

base_rows = []
if isinstance(portfolio_summary_in, dict) and isinstance(portfolio_summary_in.get("positions"), list):
    base_rows = portfolio_summary_in.get("positions") or []
if not base_rows and isinstance(rows_from_4b, list):
    base_rows = rows_from_4b

if override_source == "core.positions_snapshot":
    base_by_symbol = {norm_symbol(r.get("symbol") or r.get("Symbol")): r for r in base_rows if isinstance(r, dict)}
    base_rows = [{**base_by_symbol.get(sym, {}), "symbol": sym, "Symbol": sym}
                 for sym, value in position_overrides.items() if (to_num(value.get("quantity"), 0.0) or 0.0) > 0]
positions = []
seen_symbols = set()
for row in base_rows:
    if not isinstance(row, dict):
        continue
    if is_cash_row(row) or is_meta_row(row):
        continue
    symbol = norm_symbol(row.get("Symbol") or row.get("symbol"), row.get("AssetClass") or row.get("assetClass") or row.get("asset_class"))
    if not symbol or symbol in seen_symbols:
        continue
    seen_symbols.add(symbol)

    ov = position_overrides.get(symbol, {})
    meta_ov = instrument_overrides.get(symbol, {})
    quantity = to_num(ov.get("quantity"), to_num(row.get("Quantity"), to_num(row.get("qty"), 0.0)))
    avg_price = to_num(ov.get("avgPrice"), to_num(row.get("AvgPrice"), to_num(row.get("avgPrice"), None)))
    last_price = to_num(ov.get("lastPrice"), to_num(row.get("LastPrice"), to_num(row.get("price"), 0.0)))
    market_value = to_num(ov.get("marketValue"), to_num(row.get("MarketValue"), to_num(row.get("value"), quantity * last_price)))
    unrealized_pnl = to_num(ov.get("unrealizedPnL"), to_num(row.get("UnrealizedPnL"), to_num(row.get("pnl"), 0.0)))
    updated_at = ov.get("updatedAt") or to_iso(row.get("UpdatedAt"), None) or datetime.now(timezone.utc).isoformat()

    last_decision = normalize_last_decision((decision_memory or {}).get(symbol), symbol_hint=symbol)
    exec_mem = normalize_execution_memory((execution_memory or {}).get(symbol))
    asset_class = normalize_asset_class(
        row.get("AssetClass") or row.get("assetClass") or row.get("asset_class") or meta_ov.get("assetClass"),
        symbol,
    ) or normalize_asset_class(last_decision.get("assetClass"), symbol) or "EQUITY"
    if is_fx_symbol(symbol, asset_class):
        continue
    last_decision["assetClass"] = normalize_asset_class(last_decision.get("assetClass"), symbol) or asset_class
    lifecycle = position_lifecycle.get(symbol, {})
    if abs((to_num(lifecycle.get("netQty"), -1.0) or 0.0) - quantity) > 1e-8:
        lifecycle = {}  # a quantity mismatch cannot establish the current lot's age
    opened_at = lifecycle.get("openedAt")
    currency_contract = position_currency_contract(fx_ref_map.get(symbol, {}), lifecycle, quantity,
                                                   updated_at, market_value, avg_price, last_price)
    name = str(row.get("Name") or row.get("name") or "").strip()
    if (is_unknown_text(name) or norm_symbol(name) == symbol) and not is_unknown_text(meta_ov.get("name")):
        name = str(meta_ov.get("name")).strip()
    if is_unknown_text(name):
        name = symbol
    sector = str(row.get("Sector") or row.get("sector") or "").strip()
    if is_unknown_text(sector) and not is_unknown_text(meta_ov.get("sector")):
        sector = str(meta_ov.get("sector")).strip()
    if is_unknown_text(sector):
        sector = "Unknown"
    industry = str(row.get("Industry") or row.get("industry") or "").strip()
    if is_unknown_text(industry) and not is_unknown_text(meta_ov.get("industry")):
        industry = str(meta_ov.get("industry")).strip()
    if is_unknown_text(industry):
        industry = "Unknown"

    positions.append(
        {
            "symbol": symbol,
            "name": name,
            "sector": sector,
            "industry": industry,
            "assetClass": asset_class,
            "quantity": quantity,
            "avgPrice": round2(avg_price) if avg_price is not None else None,
            "lastPrice": round2(last_price),
            **currency_contract,
            "marketValue": round2(market_value),
            "unrealizedPnL": round2(unrealized_pnl),
            "updatedAt": updated_at,
            "openedAt": opened_at,
            "holdingDays": days_since(opened_at),
            "positionLifecycle": lifecycle if lifecycle else None,
            "lastDecision": last_decision,
            "executionMemory": exec_mem,
            # backward-compatible aliases used by prompt/tooling
            "qty": quantity,
            "price": round2(last_price),
            "value": round2(market_value),
            "pnl": round2(unrealized_pnl),
        }
    )

positions.sort(key=lambda p: to_num(p.get("marketValue"), 0.0), reverse=True)

cash_value = to_num((portfolio_summary_in or {}).get("cashEUR"), 0.0) if isinstance(portfolio_summary_in, dict) else 0.0
if portfolio_reference:
    cash_value = to_num(portfolio_reference.get("cash_eur"), cash_value)
market_value = sum(to_num(p.get("marketValue"), 0.0) for p in positions)
computed_total_value = cash_value + market_value
upstream_total_value = to_num((portfolio_summary_in or {}).get("totalPortfolioValueEUR"), None) if isinstance(portfolio_summary_in, dict) else None
if portfolio_reference and to_num(portfolio_reference.get("total_value_eur"), 0) > 0:
    total_value = to_num(portfolio_reference["total_value_eur"], computed_total_value)
elif upstream_total_value is None or upstream_total_value <= 0:
    total_value = computed_total_value
elif abs(upstream_total_value - computed_total_value) > 0.01:
    total_value = computed_total_value
else:
    total_value = upstream_total_value
exposure_pct = (market_value / total_value) * 100 if total_value > 0 else 0.0
# enrich positions with weight % and unrealized PnL % (expected by the LLM prompt)
for _p in positions:
    _mv = to_num(_p.get("marketValue"), 0.0) or 0.0
    _p["weightPct"] = round2((_mv / total_value) * 100.0) if total_value > 0 else 0.0
    _q = to_num(_p.get("quantity"), 0.0) or 0.0
    _avg = to_num(_p.get("avgPrice"), None)
    _cost = (_q * _avg) if (_avg is not None) else None
    _upnl = to_num(_p.get("unrealizedPnL"), None)
    _p["unrealizedPnlPct"] = round2((_upnl / _cost) * 100.0) if (_cost and _cost > 0 and _upnl is not None) else None
portfolio_updated_at = max((p.get("updatedAt") for p in positions), key=parse_ts_key) if positions else None

recent_ideas = []
if isinstance(recent_ideas_in, list):
    for idea in recent_ideas_in:
        if not isinstance(idea, dict):
            continue
        norm = normalize_recent_idea(idea)
        if not norm.get("symbol"):
            continue
        if is_fx_symbol(norm.get("symbol"), norm.get("assetClass")):
            continue
        recent_ideas.append(norm)
recent_ideas.sort(key=lambda x: parse_ts_key(x.get("ts")), reverse=True)
recent_ideas_dedup = []
seen_ideas = set()
for idea in recent_ideas:
    sym_key = norm_symbol(idea.get("symbol"), idea.get("assetClass"))
    if not sym_key or sym_key in seen_ideas:
        continue
    seen_ideas.add(sym_key)
    recent_ideas_dedup.append(idea)
recent_ideas = recent_ideas_dedup

summary = {
    "cash": round2(cash_value),
    "totalValue": round2(total_value),
    "positionsCount": len(positions),
    "marketValue": round2(market_value),
    "navComponentResidualEUR": round2(total_value - computed_total_value),
    "navSource": "core.portfolio_snapshot" if portfolio_reference else "portfolio_components",
    "exposurePct": round2(exposure_pct),
}

brief_text = build_agent_brief(summary, positions, recent_ideas)

source_parts = [str(input0.get("portfolioSource") or "").strip()] if isinstance(input0, dict) else []
if override_source:
    source_parts.append(override_source)
source = "+".join([p for p in source_parts if p]) or "portfolio_brief"
memory_diagnostics = {
    "dbPathRaw": str(db_path_raw),
    "dbPathSelected": str(db_path),
    "dbCandidates": db_probe_results,
    "upstream": memory_diagnostics_in if isinstance(memory_diagnostics_in, dict) else {},
}

return [
    {
        "json": {
            "run": input0.get("run", {}),
            "config": input0.get("config", {}),
            "transfer_pack": input0.get("transfer_pack", {}),
            "db_path": input0.get("db_path"),
            "portfolioBrief": {
                "generatedAt": datetime.now(timezone.utc).isoformat(),
                "dailyReturnPct": portfolio_reference.get("dailyReturnPct"),
                "dailyReferenceAt": portfolio_reference.get("dailyReferenceAt"),
                "dailyRiskAsOf": to_iso(portfolio_reference.get("ts_ms"), None),
                "portfolioUpdatedAt": portfolio_updated_at,
                "summary": summary,
                "cash": summary["cash"],
                "totalValue": summary["totalValue"],
                "positionsCount": summary["positionsCount"],
                "marketValue": summary["marketValue"],
                "exposurePct": summary["exposurePct"],
                "positions": positions,
                "recentUnexecutedIdeas": recent_ideas,
                "agentBriefingText": brief_text,
                "executionNotes": [
                    f"{i.get('symbol')}:{i.get('executionStatus')}:{i.get('executionReason')}"
                    for i in recent_ideas[:MAX_IDEAS_IN_BRIEF]
                ],
                "memoryDiagnostics": memory_diagnostics,
                "source": source,
            }
        }
    }
]
