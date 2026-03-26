import gc
import json
import os
import re
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import duckdb

DB_PATH_DEFAULT = os.getenv("AG1_DUCKDB_PATH", "/files/duckdb/ag1_v3.duckdb")
SIGNAL_SCAN_LIMIT = 800
ALERT_SCAN_LIMIT = 1500
UNEXECUTED_IDEA_LIMIT = 50

EXECUTABLE_ACTIONS = {"OPEN", "INCREASE", "DECREASE", "CLOSE"}
NON_EXECUTABLE_ACTIONS = {"HOLD", "WATCH"}
VALID_EXECUTION_STATUSES = {"EXECUTED", "RESIZED", "SKIPPED", "NO_ORDER", "REJECTED", "CANCELLED", "SUBMITTED"}
NO_ORDER_REASONS_ALLOWED = {
    "ACTION_NOT_EXECUTABLE",
    "NO_ORDER_GENERATED",
    "FX_WATCH_NOT_EXECUTED",
    "MISSING_EXECUTION_ROW",
    "INVALID_EXECUTION_QTY",
    "INVALID_RESIZE_SHAPE",
    "NO_TRADE",
    "UNKNOWN",
}
STATUS_PRIORITY = {"SKIPPED": 5, "RESIZED": 4, "NO_ORDER": 3, "EXECUTED": 2}
SOURCE_PRIORITY = {"WARNING": 3, "ORDER": 2, "INFERRED": 1}
NOISY_SYMBOL_TOKENS = {
    "ORDER",
    "ORDER_SKIP",
    "ORDER_RESIZED",
    "BUY_SKIPPED",
    "BUY_RESIZED",
    "SELL_SKIPPED",
    "SELL_RESIZED",
    "AGENT_WARNING",
    "NO",
    "TRADE",
    "GLOBAL",
}
CURRENCY_CODES = {
    "AED",
    "ARS",
    "AUD",
    "BRL",
    "CAD",
    "CHF",
    "CLP",
    "CNH",
    "CNY",
    "CZK",
    "DKK",
    "EUR",
    "GBP",
    "HKD",
    "HUF",
    "IDR",
    "ILS",
    "INR",
    "JPY",
    "KRW",
    "MXN",
    "MYR",
    "NOK",
    "NZD",
    "PHP",
    "PLN",
    "RUB",
    "SEK",
    "SGD",
    "THB",
    "TRY",
    "TWD",
    "USD",
    "ZAR",
}


def to_num(v, default=None):
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


def to_qty(v, default=None):
    n = to_num(v, None)
    if n is None:
        return default
    try:
        if abs(n - round(n)) < 1e-9:
            return int(round(n))
    except Exception:
        pass
    return n


def norm_text(v):
    return str(v or "").strip().upper()


def is_unknown_text(v):
    s = norm_text(v)
    return (not s) or s in {"UNKNOWN", "N/A", "NA", "NONE", "NULL", "-"}


def parse_fx_pair(v):
    s = norm_text(v)
    if not s:
        return None
    if s.startswith("FX:"):
        s = s[3:]
    if s.endswith("=X"):
        s = s[:-2]
    s = s.replace("/", "").replace("-", "").replace("_", "")
    pair = "".join(ch for ch in s if ch.isalpha()).upper()[:6]
    if len(pair) != 6:
        return None
    base, quote = pair[:3], pair[3:]
    if base in CURRENCY_CODES and quote in CURRENCY_CODES:
        return pair
    return None


def normalize_asset_class(asset_class, symbol=None):
    a = norm_text(asset_class)
    if a in {"FX", "FOREX", "CURRENCY"}:
        return "FX"
    if parse_fx_pair(symbol):
        return "FX"
    return a or None


def norm_symbol(v, asset_class_hint=None):
    raw = str(v or "").strip()
    s = raw.upper()
    if not s:
        return ""
    pair = parse_fx_pair(s)
    hint = norm_text(asset_class_hint)
    if pair and (hint in {"FX", "FOREX", "CURRENCY"} or s.startswith("FX:") or s.endswith("=X") or "/" in raw or len(s) == 6):
        return f"FX:{pair}"
    return s


def to_iso(v, default="unknown"):
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
        return str(v) if v is not None else default


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


def parse_json_obj(v):
    if v is None:
        return {}
    if isinstance(v, dict):
        return dict(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return {}
        try:
            out = json.loads(s)
            return dict(out) if isinstance(out, dict) else {}
        except Exception:
            return {}
    return {}


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


def infer_action_from_signal(signal):
    s = norm_text(signal)
    if s == "BUY":
        return "OPEN"
    if s == "SELL":
        return "DECREASE"
    if s in {"WATCH", "HOLD", "PROPOSE_OPEN", "PROPOSE_CLOSE"}:
        return s
    return "WATCH"


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


def parse_horizon_days(payload, horizon_txt):
    n = to_int(payload.get("horizonDays"), None)
    if n is not None:
        return n
    s = str(horizon_txt or "").strip().upper()
    if s.startswith("D"):
        return to_int(s[1:], None)
    return None


def normalize_entry_plan(payload, entry_zone):
    src = payload.get("entryPlan") if isinstance(payload.get("entryPlan"), dict) else {}
    return {
        "orderType": src.get("orderType") or entry_zone or None,
        "limitPrice": to_num(src.get("limitPrice"), None),
        "timeInForce": src.get("timeInForce") or None,
    }


def normalize_risk_plan(payload, stop_loss, take_profit, action=None, signal=None):
    src = payload.get("riskPlan") if isinstance(payload.get("riskPlan"), dict) else {}
    stop_loss_raw = to_num(src.get("stopLossPct"), to_num(stop_loss, None))
    take_profit_raw = to_num(src.get("takeProfitPct"), to_num(take_profit, None))
    stop_loss_norm = None
    if stop_loss_raw is not None:
        stop_loss_norm = -abs(stop_loss_raw)
    take_profit_norm = None
    if take_profit_raw is not None:
        take_profit_norm = abs(take_profit_raw)
    return {
        "stopLossPct": stop_loss_norm,
        "takeProfitPct": take_profit_norm,
        "maxLossEUR": to_num(src.get("maxLossEUR"), None),
    }


def normalize_last_decision(signal_row):
    payload = parse_json_obj(signal_row.get("payload_json"))
    signal = norm_text(signal_row.get("signal")) or None
    action = norm_text(payload.get("action")) or infer_action_from_signal(signal)
    confidence = to_int(payload.get("confidence"), to_int(signal_row.get("confidence"), None))
    asset_class = normalize_asset_class(
        payload.get("assetClass") or payload.get("asset_class"),
        payload.get("symbol_internal") or payload.get("symbol") or signal_row.get("symbol"),
    )
    decision = {
        "runId": str(signal_row.get("run_id") or "").strip() or None,
        "ts": to_iso(signal_row.get("ts_ms") if signal_row.get("ts_ms") is not None else signal_row.get("ts")),
        "action": action,
        "signal": signal,
        "confidence": confidence,
        "horizonDays": parse_horizon_days(payload, signal_row.get("horizon")),
        "nextReviewDays": to_int(payload.get("nextReviewDays"), None),
        "targetQty": to_num(payload.get("targetQty"), None),
        "targetWeightPct": to_num(payload.get("targetWeightPct"), None),
        "entryPlan": normalize_entry_plan(payload, signal_row.get("entry_zone")),
        "riskPlan": normalize_risk_plan(payload, signal_row.get("stop_loss"), signal_row.get("take_profit"), action=action, signal=signal),
        "rationale": payload.get("rationale") or signal_row.get("rationale") or None,
        "dependencies": payload.get("dependencies"),
        "assetClass": asset_class,
        "_symbol": norm_symbol(payload.get("symbol_internal") or payload.get("symbol") or signal_row.get("symbol"), asset_class_hint=asset_class),
    }
    return decision


def parse_prefixed_float(tokens, key):
    prefix = str(key or "").upper()
    for t in tokens:
        tt = str(t or "").strip().upper()
        if tt.startswith(prefix):
            return to_num(tt[len(prefix) :], None)
    return None


def parse_symbol_from_tokens(tokens, start_idx):
    if start_idx >= len(tokens):
        return None, start_idx
    tok = norm_symbol(tokens[start_idx])
    if tok == "FX" and start_idx + 1 < len(tokens):
        pair = re.sub(r"[^A-Z]", "", str(tokens[start_idx + 1] or "").upper())[:6]
        if len(pair) == 6:
            return norm_symbol(f"FX:{pair}", asset_class_hint="FX"), start_idx + 2
    return (norm_symbol(tok) or None), start_idx + 1


def parse_symbol_from_warning(message, fallback_symbol=None):
    msg = str(message or "").strip()
    up = msg.upper()
    tokens = [t.strip() for t in up.split(":") if t is not None]

    if len(tokens) >= 3 and tokens[0] in {"ORDER_SKIP", "ORDER_RESIZED", "BUY_SKIPPED", "BUY_RESIZED", "SELL_SKIPPED", "SELL_RESIZED"}:
        sym, _ = parse_symbol_from_tokens(tokens, 2)
        if sym and sym not in NOISY_SYMBOL_TOKENS:
            return norm_symbol(sym)

    fx_match = re.search(r"\bFX:[A-Z]{6}\b", up)
    if fx_match:
        return norm_symbol(fx_match.group(0), asset_class_hint="FX")

    for m in re.finditer(r"\b[A-Z0-9]{1,10}(?:\.[A-Z]{1,4})?\b", up):
        tok = m.group(0).upper()
        if tok in NOISY_SYMBOL_TOKENS:
            continue
        if tok.startswith("RUN_"):
            continue
        return norm_symbol(tok)

    fb = norm_symbol(fallback_symbol)
    if fb and fb not in NOISY_SYMBOL_TOKENS:
        return fb
    return None


def parse_warning_event(alert_row):
    msg = str(alert_row.get("message") or "").strip()
    if not msg:
        return None

    up = msg.upper()
    tokens = [t.strip() for t in up.split(":")]
    payload = parse_json_obj(alert_row.get("payload_json"))
    alert_code = str(alert_row.get("code") or "").strip().upper() or None
    status = None
    reason = None
    symbol = None
    req_qty = to_qty(payload.get("requestedQty"), to_qty(payload.get("qty"), None))
    exec_qty = to_qty(payload.get("executedQty"), to_qty(payload.get("filledQty"), None))
    side = norm_text(payload.get("side")) or None
    if side not in {"BUY", "SELL"}:
        side = None
    price = to_num(payload.get("price"), to_num(payload.get("avgPrice"), None))

    if up.startswith("ORDER_RESIZED:"):
        status = "RESIZED"
        reason = "ORDER_RESIZED"
        if len(tokens) >= 2 and tokens[1]:
            reason = f"ORDER_RESIZED:{tokens[1]}"
        symbol, _ = parse_symbol_from_tokens(tokens, 2)
        req_qty = to_qty(parse_prefixed_float(tokens, "FROM="), req_qty)
        exec_qty = to_qty(parse_prefixed_float(tokens, "TO="), exec_qty)
        price = to_num(parse_prefixed_float(tokens, "PRICE="), price)
    elif up.startswith("ORDER_FILLED") or up.startswith("BUY_FILLED") or up.startswith("SELL_FILLED") or alert_code in {"ORDER_FILLED", "ORDER_EXECUTED"}:
        status = "EXECUTED"
        reason = "ORDER_FILLED"
        if up.startswith("BUY_FILLED"):
            side = "BUY"
        elif up.startswith("SELL_FILLED"):
            side = "SELL"
        if len(tokens) >= 2 and tokens[1] and up.startswith("ORDER_FILLED:"):
            reason = "ORDER_FILLED"
        req_qty = to_qty(parse_prefixed_float(tokens, "QTY="), req_qty)
        exec_qty = to_qty(parse_prefixed_float(tokens, "FILLED="), exec_qty)
        if exec_qty is None:
            exec_qty = to_qty(parse_prefixed_float(tokens, "QTY="), req_qty)
        price = to_num(parse_prefixed_float(tokens, "PRICE="), price)
    elif up.startswith("ORDER_SKIP:"):
        status = "SKIPPED"
        reason = "ORDER_SKIP"
        if len(tokens) >= 2 and tokens[1]:
            reason = f"ORDER_SKIP:{tokens[1]}"
        if len(tokens) >= 3:
            symbol, _ = parse_symbol_from_tokens(tokens, 2)
        req_qty = to_qty(parse_prefixed_float(tokens, "NEED="), req_qty)
    elif up.startswith("BUY_SKIPPED:"):
        status = "SKIPPED"
        reason = "BUY_SKIPPED"
        if len(tokens) >= 2 and tokens[1]:
            reason = f"BUY_SKIPPED:{tokens[1]}"
        if len(tokens) >= 3:
            symbol, _ = parse_symbol_from_tokens(tokens, 2)
        req_qty = to_qty(parse_prefixed_float(tokens, "NEED="), req_qty)
        side = side or "BUY"
    elif up.startswith("SELL_SKIPPED:"):
        status = "SKIPPED"
        reason = "SELL_SKIPPED"
        if len(tokens) >= 2 and tokens[1]:
            reason = f"SELL_SKIPPED:{tokens[1]}"
        if len(tokens) >= 3:
            symbol, _ = parse_symbol_from_tokens(tokens, 2)
        side = side or "SELL"
    elif alert_code == "NO_TRADE" or "NO EXECUTABLE ORDERS FOR THIS RUN" in up:
        status = "NO_ORDER"
        reason = "NO_TRADE"
        symbol = "GLOBAL"
    else:
        return None

    symbol = parse_symbol_from_warning(msg, symbol or alert_row.get("symbol"))
    if not symbol and reason != "NO_TRADE":
        return None

    return {
        "runId": str(alert_row.get("run_id") or "").strip() or None,
        "ts": to_iso(alert_row.get("ts_ms") if alert_row.get("ts_ms") is not None else alert_row.get("ts")),
        "symbol": symbol or "GLOBAL",
        "side": side,
        "requestedQty": req_qty,
        "executedQty": exec_qty,
        "price": price,
        "status": status,
        "reason": reason,
        "alertCode": reason if reason else alert_code,
        "alertMessage": msg,
        "source": "WARNING",
    }


def event_sort_key(evt):
    return (
        parse_ts_key(evt.get("ts")),
        STATUS_PRIORITY.get(str(evt.get("status") or "").upper(), 0),
        SOURCE_PRIORITY.get(str(evt.get("source") or "").upper(), 0),
    )


def pick_better_event(existing, candidate):
    if candidate is None:
        return existing
    if existing is None:
        return candidate
    return candidate if event_sort_key(candidate) >= event_sort_key(existing) else existing


def infer_side_from_action(action):
    return infer_side_from_action_or_signal(action=action, signal=None)


def derive_no_order_reason(decision, symbol=None):
    action = norm_text((decision or {}).get("action"))
    asset_class = normalize_asset_class((decision or {}).get("assetClass"), symbol)
    if action in NON_EXECUTABLE_ACTIONS:
        if action == "WATCH" and asset_class == "FX":
            return "FX_WATCH_NOT_EXECUTED"
        return "ACTION_NOT_EXECUTABLE"
    if action in EXECUTABLE_ACTIONS:
        return "NO_ORDER_GENERATED"
    if action in {"PROPOSE_OPEN", "PROPOSE_CLOSE"}:
        return "ACTION_NOT_EXECUTABLE"
    return "UNKNOWN"


def build_no_order_event(decision, symbol=None, reason=None):
    return {
        "runId": str((decision or {}).get("runId") or "").strip() or None,
        "ts": (decision or {}).get("ts") or datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "side": None,
        "requestedQty": None,
        "executedQty": None,
        "price": None,
        "status": "NO_ORDER",
        "reason": reason or derive_no_order_reason(decision, symbol=symbol),
        "alertCode": None,
        "alertMessage": None,
        "source": "INFERRED",
    }


def sanitize_execution_memory(mem, decision, symbol=None):
    decision = decision or {}
    action = norm_text(decision.get("action"))
    sanitized = dict(mem or {})

    status = norm_text(sanitized.get("lastExecutionStatus")) or "NO_ORDER"
    if status not in VALID_EXECUTION_STATUSES:
        status = "NO_ORDER"

    side = norm_text(sanitized.get("lastOrderSide")) or None
    if side not in {"BUY", "SELL"}:
        side = None

    requested_qty = to_qty(sanitized.get("lastOrderQtyRequested"), None)
    executed_qty = to_qty(sanitized.get("lastOrderQtyExecuted"), None)
    price = to_num(sanitized.get("lastOrderPrice"), None)
    reason = str(sanitized.get("lastExecutionReason") or "").strip() or derive_no_order_reason(decision, symbol=symbol)
    alert_code = sanitized.get("lastExecutionAlertCode")
    alert_message = sanitized.get("lastExecutionAlertMessage")

    if action in NON_EXECUTABLE_ACTIONS:
        status = "NO_ORDER"
        reason = "ACTION_NOT_EXECUTABLE"
        if action == "WATCH" and normalize_asset_class(decision.get("assetClass"), symbol) == "FX":
            reason = "FX_WATCH_NOT_EXECUTED"

    if status == "EXECUTED":
        if side not in {"BUY", "SELL"} or executed_qty is None or executed_qty <= 0:
            status = "NO_ORDER"
            reason = "MISSING_EXECUTION_ROW"
        elif requested_qty is not None and requested_qty + 1e-9 < executed_qty:
            requested_qty = executed_qty
    elif status == "RESIZED":
        valid_shape = requested_qty is not None and executed_qty is not None and requested_qty > executed_qty > 0
        if not valid_shape:
            if executed_qty is not None and executed_qty > 0 and requested_qty is None:
                status = "EXECUTED"
            else:
                status = "NO_ORDER"
                reason = "INVALID_RESIZE_SHAPE"
        else:
            if not alert_code:
                alert_code = "ORDER_RESIZED"
            if not alert_message:
                alert_message = reason or "ORDER_RESIZED"
    elif status == "NO_ORDER":
        if not reason:
            reason = derive_no_order_reason(decision, symbol=symbol)

    if status == "EXECUTED" and (side not in {"BUY", "SELL"} or executed_qty is None or executed_qty <= 0):
        status = "NO_ORDER"
        reason = "MISSING_EXECUTION_ROW"

    if status == "NO_ORDER":
        side = None
        requested_qty = None
        executed_qty = None
        price = None
        alert_code = None
        alert_message = None
        if reason not in NO_ORDER_REASONS_ALLOWED:
            reason = "UNKNOWN"

    sanitized["lastOrderSide"] = side
    sanitized["lastOrderQtyRequested"] = requested_qty
    sanitized["lastOrderQtyExecuted"] = executed_qty
    sanitized["lastOrderPrice"] = price
    sanitized["lastExecutionStatus"] = status
    sanitized["lastExecutionReason"] = reason
    sanitized["lastExecutionAlertCode"] = alert_code
    sanitized["lastExecutionAlertMessage"] = alert_message
    return sanitized


def assert_execution_invariants(symbol, decision, mem):
    decision = decision or {}
    mem = mem or {}
    action = norm_text(decision.get("action"))
    status = norm_text(mem.get("lastExecutionStatus"))
    side = mem.get("lastOrderSide")
    req = to_qty(mem.get("lastOrderQtyRequested"), None)
    exe = to_qty(mem.get("lastOrderQtyExecuted"), None)
    price = to_num(mem.get("lastOrderPrice"), None)
    reason = str(mem.get("lastExecutionReason") or "").strip()

    payload = json.dumps({"symbol": symbol, "decision": decision, "executionMemory": mem}, ensure_ascii=True, default=str)

    if status == "NO_ORDER":
        assert side is None and req is None and exe is None and price is None, f"T1 NO_ORDER non-null fields :: {payload}"
        assert mem.get("lastExecutionAlertCode") is None and mem.get("lastExecutionAlertMessage") is None, f"T1 NO_ORDER alert fields non-null :: {payload}"

    if status == "EXECUTED":
        assert side in {"BUY", "SELL"}, f"T2 EXECUTED invalid side :: {payload}"
        assert exe is not None and exe > 0, f"T2 EXECUTED invalid executedQty :: {payload}"
        if req is not None:
            assert req + 1e-9 >= exe, f"T2 EXECUTED requestedQty < executedQty :: {payload}"

    if status == "RESIZED":
        if side is not None:
            assert side in {"BUY", "SELL"}, f"T3 RESIZED invalid side :: {payload}"
        assert req is not None and exe is not None and req > exe > 0, f"T3 RESIZED invalid qty shape :: {payload}"

    if action in NON_EXECUTABLE_ACTIONS:
        assert status == "NO_ORDER", f"T4 HOLD/WATCH must be NO_ORDER :: {payload}"
        assert reason in {"ACTION_NOT_EXECUTABLE", "FX_WATCH_NOT_EXECUTED"}, f"T4 HOLD/WATCH invalid reason :: {payload}"


def is_cash_row(row):
    sym = norm_text(row.get("Symbol") or row.get("symbol"))
    name = norm_text(row.get("Name") or row.get("name"))
    asset = norm_text(row.get("AssetClass") or row.get("assetClass") or row.get("asset_class"))
    sector = norm_text(row.get("Sector") or row.get("sector"))
    return (
        sym in ("CASH_EUR", "CASH", "EUR_CASH", "LIQUIDITE", "LIQUIDITES")
        or "CASH" in name
        or "LIQUIDITE" in name
        or asset == "CASH"
        or sector == "CASH"
    )


def is_meta_row(row):
    sym = norm_text(row.get("Symbol") or row.get("symbol"))
    name = norm_text(row.get("Name") or row.get("name"))
    return sym == "__META__" or name == "__META__"


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


def load_latest_positions_core(con):
    if not table_exists(con, "core.portfolio_snapshot") or not table_exists(con, "core.positions_snapshot"):
        return None

    summary_row = query_rows(
        con,
        """
        SELECT
          run_id,
          epoch_ms(ts) AS ts_ms,
          CAST(cash_eur AS DOUBLE) AS cash_eur,
          CAST(equity_eur AS DOUBLE) AS equity_eur,
          CAST(total_value_eur AS DOUBLE) AS total_value_eur,
          CAST(cum_fees_eur AS DOUBLE) AS cum_fees_eur,
          CAST(cum_ai_cost_eur AS DOUBLE) AS cum_ai_cost_eur,
          CAST(total_pnl_eur AS DOUBLE) AS total_pnl_eur,
          CAST(roi AS DOUBLE) AS roi,
          CAST(drawdown_pct AS DOUBLE) AS drawdown_pct
        FROM core.portfolio_snapshot
        ORDER BY ts DESC, run_id DESC
        LIMIT 1
        """,
    )
    if not summary_row:
        return None

    s = summary_row[0]
    run_id = str(s.get("run_id") or "").strip()
    if not run_id:
        return None

    rows = query_rows(
        con,
        """
        SELECT
          p.symbol,
          i.name,
          i.asset_class,
          i.sector,
          i.industry,
          i.isin,
          CAST(p.qty AS DOUBLE) AS quantity,
          CAST(p.avg_cost AS DOUBLE) AS avg_price,
          CAST(p.last_price AS DOUBLE) AS last_price,
          CAST(p.market_value_eur AS DOUBLE) AS market_value,
          CAST(p.unrealized_pnl_eur AS DOUBLE) AS unrealized_pnl,
          epoch_ms(p.ts) AS updated_at_ms
        FROM core.positions_snapshot p
        LEFT JOIN core.instruments i ON UPPER(TRIM(i.symbol)) = UPPER(TRIM(p.symbol))
        WHERE p.run_id = ?
        ORDER BY p.market_value_eur DESC NULLS LAST, p.symbol
        """,
        [run_id],
    )

    out_rows = []
    for idx, r in enumerate(rows, start=1):
        sym = norm_symbol(r.get("symbol"), r.get("asset_class"))
        if not sym:
            continue
        asset_class = normalize_asset_class(r.get("asset_class"), sym) or "EQUITY"
        out_rows.append(
            {
                "row_number": idx,
                "Symbol": sym,
                "Name": str(r.get("name") or sym).strip(),
                "AssetClass": asset_class,
                "Sector": str(r.get("sector") or "Unknown").strip() or "Unknown",
                "Industry": str(r.get("industry") or "Unknown").strip() or "Unknown",
                "ISIN": str(r.get("isin") or "").strip(),
                "Quantity": to_num(r.get("quantity"), 0.0),
                "AvgPrice": to_num(r.get("avg_price"), None),
                "LastPrice": to_num(r.get("last_price"), None),
                "MarketValue": to_num(r.get("market_value"), None),
                "UnrealizedPnL": to_num(r.get("unrealized_pnl"), None),
                "UpdatedAt": to_iso(r.get("updated_at_ms") if r.get("updated_at_ms") is not None else r.get("updated_at")),
                "NextReviewDate": None,
            }
        )

    return {
        "rows": out_rows,
        "summary": {
            "runId": run_id,
            "ts": to_iso(s.get("ts_ms") if s.get("ts_ms") is not None else s.get("ts")),
            "cashEUR": to_num(s.get("cash_eur"), 0.0) or 0.0,
            "equityEUR": to_num(s.get("equity_eur"), 0.0) or 0.0,
            "totalPortfolioValueEUR": to_num(s.get("total_value_eur"), 0.0) or 0.0,
            "cumFeesEUR": to_num(s.get("cum_fees_eur"), 0.0) or 0.0,
            "cumAiCostEUR": to_num(s.get("cum_ai_cost_eur"), 0.0) or 0.0,
            "totalPnLEUR": to_num(s.get("total_pnl_eur"), None),
            "roi": to_num(s.get("roi"), None),
            "drawdownPct": to_num(s.get("drawdown_pct"), None),
        },
    }


def load_latest_positions_fallback(con):
    if not table_exists(con, "portfolio_positions_mtm_latest"):
        return []
    rows = query_rows(
        con,
        """
        SELECT
          symbol,
          name,
          asset_class,
          sector,
          industry,
          isin,
          quantity,
          avg_price,
          last_price,
          market_value,
          unrealized_pnl,
          CAST(updated_at AS VARCHAR) AS updated_at
        FROM portfolio_positions_mtm_latest
        ORDER BY market_value DESC NULLS LAST, symbol
        """,
    )
    out = []
    for idx, r in enumerate(rows, start=1):
        sym = norm_symbol(r.get("symbol"), r.get("asset_class"))
        asset_class = normalize_asset_class(r.get("asset_class"), sym) or "EQUITY"
        out.append(
            {
                "row_number": idx,
                "Symbol": sym,
                "Name": str(r.get("name") or sym).strip(),
                "AssetClass": asset_class,
                "Sector": str(r.get("sector") or "Unknown").strip() or "Unknown",
                "Industry": str(r.get("industry") or "Unknown").strip() or "Unknown",
                "ISIN": str(r.get("isin") or "").strip(),
                "Quantity": to_num(r.get("quantity"), None),
                "AvgPrice": to_num(r.get("avg_price"), None),
                "LastPrice": to_num(r.get("last_price"), None),
                "MarketValue": to_num(r.get("market_value"), None),
                "UnrealizedPnL": to_num(r.get("unrealized_pnl"), None),
                "UpdatedAt": to_iso(r.get("updated_at")),
                "NextReviewDate": None,
            }
        )
    return out


def load_signal_rows(con):
    if not table_exists(con, "core.ai_signals"):
        return []
    return query_rows(
        con,
        """
        SELECT
          signal_id,
          run_id,
          epoch_ms(ts) AS ts_ms,
          symbol,
          signal,
          confidence,
          horizon,
          entry_zone,
          stop_loss,
          take_profit,
          rationale,
          payload_json
        FROM core.ai_signals
        ORDER BY ts DESC, run_id DESC, signal_id DESC
        LIMIT ?
        """,
        [SIGNAL_SCAN_LIMIT],
    )


def load_latest_signal_rows(con):
    if not table_exists(con, "core.ai_signals"):
        return []
    return query_rows(
        con,
        """
        SELECT
          signal_id,
          run_id,
          ts_ms,
          symbol,
          signal,
          confidence,
          horizon,
          entry_zone,
          stop_loss,
          take_profit,
          rationale,
          payload_json
        FROM (
          SELECT
            signal_id,
            run_id,
            epoch_ms(ts) AS ts_ms,
            symbol,
            signal,
            confidence,
            horizon,
            entry_zone,
            stop_loss,
            take_profit,
            rationale,
            payload_json,
            ROW_NUMBER() OVER (
              PARTITION BY UPPER(symbol)
              ORDER BY ts DESC, run_id DESC, signal_id DESC
            ) AS rn
          FROM core.ai_signals
        ) x
        WHERE rn = 1
        """,
    )


def load_alert_rows(con):
    if not table_exists(con, "core.alerts"):
        return []
    return query_rows(
        con,
        """
        SELECT
          alert_id,
          run_id,
          epoch_ms(ts) AS ts_ms,
          severity,
          category,
          symbol,
          message,
          code,
          payload_json
        FROM core.alerts
        ORDER BY ts DESC, run_id DESC, alert_id DESC
        LIMIT ?
        """,
        [ALERT_SCAN_LIMIT],
    )


def load_order_execution_rows(con):
    if not table_exists(con, "core.orders"):
        return []
    has_fills = table_exists(con, "core.fills")
    if has_fills:
        sql = """
        WITH fills_by_order AS (
          SELECT
            order_id,
            SUM(CAST(qty AS DOUBLE)) AS exec_qty,
            SUM(CAST(qty AS DOUBLE) * CAST(price AS DOUBLE)) / NULLIF(SUM(CAST(qty AS DOUBLE)), 0) AS avg_price
          FROM core.fills
          GROUP BY order_id
        )
        SELECT
          o.order_id,
          o.run_id,
          epoch_ms(o.ts_created) AS ts_ms,
          o.symbol,
          o.side,
          CAST(o.qty AS DOUBLE) AS qty,
          CAST(o.limit_price AS DOUBLE) AS limit_price,
          CAST(f.exec_qty AS DOUBLE) AS exec_qty,
          CAST(f.avg_price AS DOUBLE) AS avg_price
        FROM core.orders o
        LEFT JOIN fills_by_order f ON f.order_id = o.order_id
        ORDER BY o.ts_created DESC, o.run_id DESC, o.order_id DESC
        LIMIT 2000
        """
        return query_rows(con, sql)
    sql = """
    SELECT
      order_id,
      run_id,
      epoch_ms(ts_created) AS ts_ms,
      symbol,
      side,
      CAST(qty AS DOUBLE) AS qty,
      CAST(limit_price AS DOUBLE) AS limit_price,
      NULL AS exec_qty,
      NULL AS avg_price
    FROM core.orders
    ORDER BY ts_created DESC, run_id DESC, order_id DESC
    LIMIT 2000
    """
    return query_rows(con, sql)


def build_order_event(order_row):
    requested_qty = to_qty(order_row.get("qty"), None)
    executed_qty = to_qty(order_row.get("exec_qty"), None)
    side = norm_text(order_row.get("side")) or None
    status = "EXECUTED"
    reason = "ORDER_FILLED"
    if executed_qty is None:
        executed_qty = requested_qty
    if executed_qty is None or executed_qty <= 0:
        status = "SKIPPED"
        reason = "ORDER_NO_FILL"
    elif requested_qty is not None and executed_qty + 1e-9 < requested_qty:
        status = "RESIZED"
        reason = "ORDER_PARTIAL_FILL"

    return {
        "runId": str(order_row.get("run_id") or "").strip() or None,
        "ts": to_iso(order_row.get("ts_ms") if order_row.get("ts_ms") is not None else order_row.get("ts")),
        "symbol": norm_symbol(order_row.get("symbol")),
        "side": side,
        "requestedQty": requested_qty,
        "executedQty": executed_qty,
        "price": to_num(order_row.get("avg_price"), to_num(order_row.get("limit_price"), None)),
        "status": status,
        "reason": reason,
        "alertCode": None,
        "alertMessage": None,
        "source": "ORDER",
    }


def get_execution_outcome(run_id, symbol, order_events_by_run_symbol, alert_events_by_run_symbol):
    rid = str(run_id or "").strip()
    sym = norm_symbol(symbol)
    if not rid or not sym:
        return None
    key = (rid, sym)
    evt = order_events_by_run_symbol.get(key)
    if evt is None:
        evt = alert_events_by_run_symbol.get(key)
    return dict(evt) if isinstance(evt, dict) else None


def load_instrument_metadata(con):
    if not table_exists(con, "core.instruments"):
        return {}
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
    out = {}
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


def enrich_position_rows_with_instruments(rows, instrument_meta):
    if not isinstance(rows, list) or not rows:
        return rows or []
    out = []
    for r in rows:
        row = dict(r) if isinstance(r, dict) else {}
        sym = norm_symbol(row.get("Symbol") or row.get("symbol"), row.get("AssetClass") or row.get("asset_class"))
        if sym:
            row["Symbol"] = sym

        if not sym or is_cash_row(row) or is_meta_row(row):
            out.append(row)
            continue

        meta = instrument_meta.get(sym) if isinstance(instrument_meta, dict) else None
        current_asset_class = normalize_asset_class(row.get("AssetClass"), sym)
        meta_asset_class = normalize_asset_class((meta or {}).get("assetClass"), sym)
        row["AssetClass"] = current_asset_class or meta_asset_class or "EQUITY"

        current_name = str(row.get("Name") or "").strip()
        if meta and (is_unknown_text(current_name) or norm_symbol(current_name) == sym):
            if not is_unknown_text(meta.get("name")):
                row["Name"] = str(meta.get("name")).strip()
        if is_unknown_text(row.get("Name")):
            row["Name"] = sym

        if meta and is_unknown_text(row.get("Sector")) and not is_unknown_text(meta.get("sector")):
            row["Sector"] = str(meta.get("sector")).strip()
        if is_unknown_text(row.get("Sector")):
            row["Sector"] = "Unknown"

        if meta and is_unknown_text(row.get("Industry")) and not is_unknown_text(meta.get("industry")):
            row["Industry"] = str(meta.get("industry")).strip()
        if is_unknown_text(row.get("Industry")):
            row["Industry"] = "Unknown"

        if meta and is_unknown_text(row.get("ISIN")) and not is_unknown_text(meta.get("isin")):
            row["ISIN"] = str(meta.get("isin")).strip()

        out.append(row)
    return out


def build_recent_unexecuted(decisions_recent, order_events_by_run_symbol, alert_events_by_run_symbol):
    seen = set()
    ideas = []
    for d in decisions_recent:
        symbol = norm_symbol(d.get("_symbol"), d.get("assetClass"))
        run_id = str(d.get("runId") or "").strip()
        if not symbol or not run_id:
            continue
        key = (run_id, symbol)
        if key in seen:
            continue
        seen.add(key)

        event = get_execution_outcome(run_id, symbol, order_events_by_run_symbol, alert_events_by_run_symbol)
        action = norm_text(d.get("action"))
        asset_class = normalize_asset_class(d.get("assetClass"), symbol) or "EQUITY"
        status = "NO_ORDER"
        reason = derive_no_order_reason(d, symbol=symbol)
        alert_code = None
        alert_message = None
        if event:
            status = norm_text(event.get("status")) or "NO_ORDER"
            reason = event.get("reason") or reason
            alert_code = event.get("alertCode")
            alert_message = event.get("alertMessage")

        if status not in {"NO_ORDER", "RESIZED", "REJECTED", "CANCELLED"}:
            continue

        is_fx_watch = asset_class == "FX" and action == "WATCH"
        is_trade_action = action in EXECUTABLE_ACTIONS
        if not (is_trade_action or is_fx_watch):
            continue

        ideas.append(
            {
                "symbol": symbol,
                "ts": d.get("ts"),
                "runId": run_id,
                "assetClass": asset_class,
                "action": d.get("action"),
                "confidence": d.get("confidence"),
                "targetQty": d.get("targetQty"),
                "targetWeightPct": d.get("targetWeightPct"),
                "entryPlan": d.get("entryPlan"),
                "riskPlan": d.get("riskPlan"),
                "rationale": d.get("rationale"),
                "executionStatus": status,
                "executionReason": reason,
                "executionAlertCode": alert_code,
                "executionAlertMessage": alert_message,
                "requestedQty": to_qty(event.get("requestedQty"), None) if event else None,
                "executedQty": to_qty(event.get("executedQty"), None) if event else None,
            }
        )

    ideas.sort(key=lambda x: parse_ts_key(x.get("ts")), reverse=True)
    deduped = []
    seen = set()
    for idea in ideas:
        sym_key = norm_symbol(idea.get("symbol"), idea.get("assetClass"))
        if not sym_key or sym_key in seen:
            continue
        seen.add(sym_key)
        deduped.append(idea)
    return deduped[:UNEXECUTED_IDEA_LIMIT]


def assert_recent_vs_positions_consistency(recent_ideas, positions, decision_by_symbol, portfolio_execution_memory):
    held_symbols = {
        norm_symbol(p.get("Symbol"), p.get("AssetClass"))
        for p in (positions or [])
        if isinstance(p, dict)
    }
    issues = []
    for idea in recent_ideas or []:
        if not isinstance(idea, dict):
            continue
        status = norm_text(idea.get("executionStatus"))
        if status not in {"RESIZED", "EXECUTED"}:
            continue
        symbol = norm_symbol(idea.get("symbol"), idea.get("assetClass"))
        run_id = str(idea.get("runId") or "").strip()
        if not symbol or symbol not in held_symbols or not run_id:
            continue
        decision_run = str((decision_by_symbol.get(symbol) or {}).get("runId") or "").strip()
        if decision_run != run_id:
            continue
        pos_mem = portfolio_execution_memory.get(symbol) or {}
        pos_status = norm_text(pos_mem.get("lastExecutionStatus"))
        pos_reason = str(pos_mem.get("lastExecutionReason") or "").strip()
        idea_reason = str(idea.get("executionReason") or "").strip()
        if pos_status != status or (idea_reason and pos_reason and pos_reason != idea_reason):
            issues.append(
                {
                    "symbol": symbol,
                    "runId": run_id,
                    "ideaStatus": status,
                    "ideaReason": idea_reason,
                    "positionStatus": pos_status,
                    "positionReason": pos_reason,
                }
            )
    assert not issues, f"CROSS_EXECUTION_MISMATCH :: {json.dumps(issues[:10], ensure_ascii=True, default=str)}"


def normalize_db_path(path_value):
    s = str(path_value or "").strip().replace("\\", "/")
    if not s:
        s = DB_PATH_DEFAULT
    s = s.replace("/local-files/", "/files/")
    return s


def candidate_db_paths(path_value):
    base = normalize_db_path(path_value)
    cands = [base]
    if "/files/" in base:
        cands.append(base.replace("/files/", "/local-files/", 1))
    elif "/local-files/" in base:
        cands.append(base.replace("/local-files/", "/files/", 1))
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


def select_db_path(incoming_items):
    db_path = DB_PATH_DEFAULT
    for it in incoming_items:
        j = it.get("json", {}) if isinstance(it, dict) else {}
        if not isinstance(j, dict):
            continue
        cands = [
            j.get("ag1_db_path"),
            j.get("db_path"),
            (j.get("transfer_pack") or {}).get("db_path") if isinstance(j.get("transfer_pack"), dict) else None,
        ]
        for c in cands:
            if c:
                db_path = str(c)
                return db_path
    return str(db_path)


incoming = _items or []
input0 = incoming[0].get("json", {}) if incoming and isinstance(incoming[0], dict) else {}
raw_db_path = select_db_path(incoming)
db_candidates = candidate_db_paths(raw_db_path)
db_probe_results = [probe_db_memory(p) for p in db_candidates]
db_probe_selected = pick_best_probe(db_probe_results) or {"path": normalize_db_path(raw_db_path)}
db_path = str(db_probe_selected.get("path") or normalize_db_path(raw_db_path))

rows = []
portfolio_source = "duckdb_empty"
core_summary = {}
signal_rows = []
signal_rows_latest = []
alert_rows = []
order_rows = []
instrument_meta = {}

with db_con(db_path) as con:
    if con is not None:
        instrument_meta = load_instrument_metadata(con)
        core_payload = load_latest_positions_core(con)
        if core_payload:
            rows = core_payload.get("rows", []) or []
            core_summary = core_payload.get("summary", {}) or {}
            portfolio_source = "core_snapshots"
        else:
            rows = load_latest_positions_fallback(con)
            portfolio_source = "portfolio_positions_mtm_latest" if rows else "duckdb_empty"
        rows = enrich_position_rows_with_instruments(rows, instrument_meta)
        signal_rows = load_signal_rows(con)
        signal_rows_latest = load_latest_signal_rows(con)
        alert_rows = load_alert_rows(con)
        order_rows = load_order_execution_rows(con)

cash_row = next((r for r in rows if is_cash_row(r)), None)
meta_row = next((r for r in rows if is_meta_row(r)), None)

positions = [r for r in rows if not is_cash_row(r) and not is_meta_row(r)]
positions_market_value = sum(to_num(p.get("MarketValue"), 0.0) or 0.0 for p in positions)
cash_eur = to_num(core_summary.get("cashEUR"), None)
if cash_eur is None:
    cash_eur = to_num(cash_row.get("MarketValue"), 0.0) if cash_row else 0.0
cash_eur = cash_eur or 0.0

total_value = to_num(core_summary.get("totalPortfolioValueEUR"), None)
if total_value is None:
    total_value = cash_eur + positions_market_value
total_value = total_value or 0.0

initial_capital = None
if core_summary.get("totalPnLEUR") is not None:
    initial_capital = total_value - (to_num(core_summary.get("totalPnLEUR"), 0.0) or 0.0)
if initial_capital is None or initial_capital <= 0:
    initial_capital = to_num(meta_row.get("MarketValue"), None) if meta_row else None
if initial_capital is None or initial_capital <= 0:
    initial_capital = 50000.0

meta = {
    "startDate": datetime.now(timezone.utc).isoformat(),
    "initialCapitalEUR": initial_capital,
    "cumFeesEUR": to_num(core_summary.get("cumFeesEUR"), 0.0) or 0.0,
    "cumAiCostEUR": to_num(core_summary.get("cumAiCostEUR"), 0.0) or 0.0,
}

portfolio_summary = {
    "cashEUR": cash_eur,
    "positionsCount": len(positions),
    "positionsMarketValueEUR": positions_market_value,
    "totalPortfolioValueEUR": total_value,
    "positions": positions,
}

# ----- Decision memory (latest per symbol + recent stream) -----
decision_by_symbol = {}
decisions_recent = []
for r in signal_rows_latest:
    decision = normalize_last_decision(r)
    symbol = norm_symbol(decision.get("_symbol") or r.get("symbol"), decision.get("assetClass"))
    if not symbol:
        continue
    prev = decision_by_symbol.get(symbol)
    if prev is None or parse_ts_key(decision.get("ts")) >= parse_ts_key(prev.get("ts")):
        decision_by_symbol[symbol] = {k: v for k, v in decision.items() if k != "_symbol"}

for r in signal_rows:
    decision = normalize_last_decision(r)
    symbol = norm_symbol(decision.get("_symbol") or r.get("symbol"), decision.get("assetClass"))
    if not symbol:
        continue
    decision["_symbol"] = symbol
    decision["assetClass"] = normalize_asset_class(decision.get("assetClass"), symbol) or decision.get("assetClass")
    decisions_recent.append(decision)

# ----- Execution memory from orders/fills + warnings -----
order_execution_by_run_symbol = {}
alert_execution_by_run_symbol = {}
latest_execution_by_symbol = {}

for r in order_rows:
    evt = build_order_event(r)
    if not evt.get("symbol") or not evt.get("runId"):
        continue
    key = (evt["runId"], evt["symbol"])
    order_execution_by_run_symbol[key] = pick_better_event(order_execution_by_run_symbol.get(key), evt)
    latest_execution_by_symbol[evt["symbol"]] = pick_better_event(latest_execution_by_symbol.get(evt["symbol"]), evt)

for a in alert_rows:
    evt = parse_warning_event(a)
    if not evt:
        continue
    symbol = evt.get("symbol")
    run_id = evt.get("runId")
    if symbol and symbol != "GLOBAL" and run_id:
        key = (run_id, symbol)
        alert_execution_by_run_symbol[key] = pick_better_event(alert_execution_by_run_symbol.get(key), evt)
        latest_execution_by_symbol[symbol] = pick_better_event(latest_execution_by_symbol.get(symbol), evt)

portfolio_execution_memory = {}
for p in positions:
    symbol = norm_symbol(p.get("Symbol"), p.get("AssetClass"))
    if not symbol:
        continue
    last_decision = decision_by_symbol.get(symbol)
    decision_run = str((last_decision or {}).get("runId") or "").strip() or None
    evt = None
    if decision_run:
        evt = get_execution_outcome(decision_run, symbol, order_execution_by_run_symbol, alert_execution_by_run_symbol)
    mismatch_found = False
    if evt is None:
        if decision_run:
            latest_other = latest_execution_by_symbol.get(symbol)
            if latest_other is not None and str(latest_other.get("runId") or "").strip() != decision_run:
                mismatch_found = True
        evt = build_no_order_event(last_decision, symbol=symbol)

    evt_run = str(evt.get("runId") or "").strip() or None
    if decision_run and evt_run and evt_run != decision_run:
        mismatch_found = True
        evt = build_no_order_event(last_decision, symbol=symbol, reason=derive_no_order_reason(last_decision, symbol=symbol))
        evt_run = decision_run

    execution_row = {
        "lastOrderRunId": evt_run or decision_run,
        "lastOrderSide": evt.get("side"),
        "lastOrderQtyRequested": to_qty(evt.get("requestedQty"), None),
        "lastOrderQtyExecuted": to_qty(evt.get("executedQty"), None),
        "lastOrderPrice": to_num(evt.get("price"), None),
        "lastExecutionStatus": norm_text(evt.get("status")) or "NO_ORDER",
        "lastExecutionReason": evt.get("reason") or derive_no_order_reason(last_decision, symbol=symbol),
        "lastExecutionAlertCode": evt.get("alertCode"),
        "lastExecutionAlertMessage": evt.get("alertMessage"),
    }

    execution_row = sanitize_execution_memory(execution_row, last_decision, symbol=symbol)
    assert_execution_invariants(symbol, last_decision, execution_row)
    if mismatch_found:
        execution_row["debug"] = {"mismatchedOrderFound": True}
    portfolio_execution_memory[symbol] = execution_row

recent_unexecuted = build_recent_unexecuted(decisions_recent, order_execution_by_run_symbol, alert_execution_by_run_symbol)
assert_recent_vs_positions_consistency(recent_unexecuted, positions, decision_by_symbol, portfolio_execution_memory)
memory_diagnostics = {
    "dbPathRaw": str(raw_db_path),
    "dbPathSelected": str(db_path),
    "dbCandidates": db_probe_results,
    "latestSignalsRows": len(signal_rows_latest),
    "signalsLoadedRows": len(signal_rows),
    "alertsLoadedRows": len(alert_rows),
    "signalsMaxTsLoaded": max((d.get("ts") for d in decisions_recent), key=parse_ts_key) if decisions_recent else None,
    "alertsMaxTsLoaded": max(
        (to_iso(a.get("ts_ms") if a.get("ts_ms") is not None else a.get("ts"), None) for a in alert_rows),
        key=parse_ts_key,
    )
    if alert_rows
    else None,
}

transfer_pack = {
    "db_path": str(db_path),
    "run": input0.get("run", {}),
    "config": input0.get("config", {}),
    "feesConfig": input0.get("feesConfig", {}),
    "meta": meta,
    "portfolioSummary": portfolio_summary,
    "portfolioDecisionMemory": decision_by_symbol,
    "portfolioExecutionMemory": portfolio_execution_memory,
    "recentUnexecutedIdeas": recent_unexecuted,
    "memoryDiagnostics": memory_diagnostics,
}

return [
    {
        "json": {
            "portfolioRows": rows,
            "portfolioSummary": portfolio_summary,
            "portfolioDecisionMemory": decision_by_symbol,
            "portfolioExecutionMemory": portfolio_execution_memory,
            "recentUnexecutedIdeas": recent_unexecuted,
            "memoryDiagnostics": memory_diagnostics,
            "meta": meta,
            "portfolioSource": portfolio_source,
            "run": input0.get("run", {}),
            "config": input0.get("config", {}),
            "feesConfig": input0.get("feesConfig", {}),
            "transfer_pack": transfer_pack,
            "db_path": str(db_path),
        }
    }
]
