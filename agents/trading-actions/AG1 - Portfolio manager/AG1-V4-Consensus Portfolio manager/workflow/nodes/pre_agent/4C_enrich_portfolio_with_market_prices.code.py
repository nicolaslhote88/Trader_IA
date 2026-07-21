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
            # CHECKPOINT avant close pour libérer les pages orphelines laissées
            # par les INSERT OR REPLACE / UPDATE. Cf. infra/maintenance/defrag_duckdb.py.
            try:
                con.execute("CHECKPOINT")
            except Exception:
                pass
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
    # FIX 2026-07-13 : taux/devise autoritaires IBKR pour convertir les prix natifs (USD...) en EUR.
    out = {}
    try:
        with db_con(db_path) as con:
            if con is None or not table_exists(con, "portfolio_positions_ibkr_latest"):
                return out
            rows = query_rows(con, """
                SELECT symbol, currency, CAST(fx_rate AS DOUBLE) AS fx_rate,
                       CAST(last_price_eur AS DOUBLE) AS last_price_eur
                FROM portfolio_positions_ibkr_latest
            """)
            for r in rows:
                sym = norm_symbol(r.get("symbol"))
                if not sym:
                    continue
                out[sym] = {
                    "currency": (str(r.get("currency") or "").strip().upper() or None),
                    "fx": to_num(r.get("fx_rate"), None),
                    "lp_eur": to_num(r.get("last_price_eur"), None),
                }
    except Exception:
        return out
    return out


def load_position_overrides(db_path):
    out = {}
    source = None
    with db_con(db_path) as con:
        if con is None:
            return out, source
        if table_exists(con, "core.positions_snapshot"):
            rows = query_rows(
                con,
                """
                SELECT symbol, qty, avg_cost, last_price, market_value_eur, unrealized_pnl_eur, ts_ms
                FROM (
                  SELECT
                    symbol,
                    CAST(qty AS DOUBLE) AS qty,
                    CAST(avg_cost AS DOUBLE) AS avg_cost,
                    CAST(last_price AS DOUBLE) AS last_price,
                    CAST(market_value_eur AS DOUBLE) AS market_value_eur,
                    CAST(unrealized_pnl_eur AS DOUBLE) AS unrealized_pnl_eur,
                    epoch_ms(ts) AS ts_ms,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts DESC, run_id DESC) AS rn
                  FROM core.positions_snapshot
                ) x
                WHERE rn = 1
                """,
            )
            for r in rows:
                sym = norm_symbol(r.get("symbol"))
                if not sym:
                    continue
                out[sym] = {
                    "quantity": to_num(r.get("qty"), None),
                    "avgPrice": to_num(r.get("avg_cost"), None),
                    "lastPrice": to_num(r.get("last_price"), None),
                    "marketValue": to_num(r.get("market_value_eur"), None),
                    "unrealizedPnL": to_num(r.get("unrealized_pnl_eur"), None),
                    "updatedAt": to_iso(r.get("ts_ms"), None),
                }
            source = "core.positions_snapshot"

        if table_exists(con, "portfolio_positions_mtm_latest"):
            rows = query_rows(
                con,
                """
                SELECT symbol, quantity, avg_price, last_price, market_value, unrealized_pnl, CAST(updated_at AS VARCHAR) AS updated_at
                FROM portfolio_positions_mtm_latest
                """,
            )
            for r in rows:
                sym = norm_symbol(r.get("symbol"))
                if not sym:
                    continue
                out[sym] = {
                    "quantity": to_num(r.get("quantity"), None),
                    "avgPrice": to_num(r.get("avg_price"), None),
                    "lastPrice": to_num(r.get("last_price"), None),
                    "marketValue": to_num(r.get("market_value"), None),
                    "unrealizedPnL": to_num(r.get("unrealized_pnl"), None),
                    "updatedAt": to_iso(r.get("updated_at"), None),
                }
            source = "portfolio_positions_mtm_latest"
    return out, source


def load_position_lifecycle(db_path):
    """Best-effort lifecycle dates from executed BUY/SELL orders."""
    out = {}
    with db_con(db_path) as con:
        if con is None or not table_exists(con, "core.orders"):
            return out
        if table_exists(con, "core.fills"):
            rows = query_rows(
                con,
                """
                WITH fills_by_order AS (
                  SELECT
                    order_id,
                    SUM(CAST(qty AS DOUBLE)) AS exec_qty,
                    SUM(CAST(qty AS DOUBLE) * CAST(price AS DOUBLE)) / NULLIF(SUM(CAST(qty AS DOUBLE)), 0) AS avg_price
                  FROM core.fills
                  GROUP BY order_id
                )
                SELECT
                  UPPER(TRIM(o.symbol)) AS symbol,
                  UPPER(TRIM(o.side)) AS side,
                  epoch_ms(o.ts_created) AS ts_ms,
                  COALESCE(CAST(f.exec_qty AS DOUBLE), CAST(o.qty AS DOUBLE)) AS qty,
                  COALESCE(CAST(f.avg_price AS DOUBLE), CAST(o.limit_price AS DOUBLE)) AS price
                FROM core.orders o
                LEFT JOIN fills_by_order f ON f.order_id = o.order_id
                WHERE UPPER(TRIM(o.side)) IN ('BUY', 'SELL')
                ORDER BY o.ts_created ASC
                """,
            )
        else:
            rows = query_rows(
                con,
                """
                SELECT
                  UPPER(TRIM(symbol)) AS symbol,
                  UPPER(TRIM(side)) AS side,
                  epoch_ms(ts_created) AS ts_ms,
                  CAST(qty AS DOUBLE) AS qty,
                  CAST(limit_price AS DOUBLE) AS price
                FROM core.orders
                WHERE UPPER(TRIM(side)) IN ('BUY', 'SELL')
                ORDER BY ts_created ASC
                """,
            )
    for r in rows:
        sym = norm_symbol(r.get("symbol"))
        side = norm_text(r.get("side"))
        qty = to_num(r.get("qty"), 0.0) or 0.0
        ts = to_iso(r.get("ts_ms"), None)
        if not sym or not ts or qty <= 0:
            continue
        rec = out.setdefault(
            sym,
            {
                "openedAt": None,
                "lastBuyAt": None,
                "lastSellAt": None,
                "buyQty": 0.0,
                "buyNotional": 0.0,
                "source": "core.orders",
            },
        )
        if side == "BUY":
            if rec["openedAt"] is None or parse_ts_key(ts) < parse_ts_key(rec["openedAt"]):
                rec["openedAt"] = ts
            if rec["lastBuyAt"] is None or parse_ts_key(ts) > parse_ts_key(rec["lastBuyAt"]):
                rec["lastBuyAt"] = ts
            rec["buyQty"] += qty
            price = to_num(r.get("price"), None)
            if price is not None:
                rec["buyNotional"] += qty * price
        elif side == "SELL":
            if rec["lastSellAt"] is None or parse_ts_key(ts) > parse_ts_key(rec["lastSellAt"]):
                rec["lastSellAt"] = ts
    for rec in out.values():
        rec["avgExecutedBuyPrice"] = round2(rec["buyNotional"] / rec["buyQty"]) if rec["buyQty"] > 0 else None
        rec["buyQty"] = round2(rec["buyQty"])
        rec.pop("buyNotional", None)
    return out


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

position_overrides, override_source = load_position_overrides(db_path)
instrument_overrides = load_instrument_overrides(db_path)
position_lifecycle = load_position_lifecycle(db_path)
fx_ref_map = load_fx_ref_map(db_path)

base_rows = []
if isinstance(portfolio_summary_in, dict) and isinstance(portfolio_summary_in.get("positions"), list):
    base_rows = portfolio_summary_in.get("positions") or []
if not base_rows and isinstance(rows_from_4b, list):
    base_rows = rows_from_4b

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
    _perf_local_pct = ((last_price / avg_price - 1.0) * 100.0) if (avg_price and last_price and avg_price > 0) else None  # perf PRIX devise locale (avant conversion FX)
    market_value = to_num(ov.get("marketValue"), to_num(row.get("MarketValue"), to_num(row.get("value"), quantity * last_price)))
    unrealized_pnl = to_num(ov.get("unrealizedPnL"), to_num(row.get("UnrealizedPnL"), to_num(row.get("pnl"), 0.0)))
    # FIX 2026-07-13 : conversion FX -> EUR des positions non-EUR (MTM/consensus stockent en devise
    # native -> equity gonflee + ecart positions vs snapshot IBKR). Detection d'echelle pour ne
    # jamais double-convertir ; avg_price est deja en EUR. Gardee (try/except) : ne casse jamais le run.
    try:
        _fx = fx_ref_map.get(symbol)
        if _fx and _fx.get("currency") and _fx.get("currency") != "EUR" and _fx.get("fx") and _fx["fx"] > 0:
            _factor = float(_fx["fx"])
            _lp_ref = _fx.get("lp_eur")
            _lp = to_num(last_price, None)
            if _lp is not None and _lp > 0:
                _cand = _lp * _factor
                _do = True
                if _lp_ref is not None and _lp_ref > 0:
                    _do = abs(_cand - _lp_ref) < abs(_lp - _lp_ref)
                if _do:
                    _qty = to_num(quantity, 0.0) or 0.0
                    last_price = _cand
                    market_value = _qty * last_price
                    if avg_price is not None:
                        unrealized_pnl = (last_price - avg_price) * _qty
    except Exception:
        pass
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
    opened_at = lifecycle.get("openedAt")
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
            "perfLocalPct": (round2(_perf_local_pct) if _perf_local_pct is not None else None),
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
market_value = sum(to_num(p.get("marketValue"), 0.0) for p in positions)
computed_total_value = cash_value + market_value
upstream_total_value = to_num((portfolio_summary_in or {}).get("totalPortfolioValueEUR"), None) if isinstance(portfolio_summary_in, dict) else None
if upstream_total_value is None or upstream_total_value <= 0:
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
