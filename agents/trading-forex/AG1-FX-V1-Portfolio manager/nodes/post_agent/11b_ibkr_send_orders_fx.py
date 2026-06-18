"""
Node 11b - IBKR Send Orders FX

Insert between:
  11 Validate Enforce Safety FX -> [11b] -> 12 Simulate Fills FX

Dry-run behavior:
  IBKR_DRY_RUN=true keeps the workflow sandbox-only and does not call the
  broker by default. Set IBKR_SEND_DRY_RUN_TO_BROKER=true to exercise the
  ibkr-broker dry-run endpoint while still sending no live IBKR order.

Live behavior:
  IBKR_DRY_RUN=false posts pending orders to ibkr-broker. Orders rejected by
  the broker are marked broker_error, so node 12 will not create simulated
  fills for failed live submissions.
"""

import json
import os
import time
import uuid
import urllib.request
from urllib.error import URLError

ctx = (_items or [{"json": {}}])[0].get("json", {})


def env_bool(name, default):
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default, "context" if name == "IBKR_DRY_RUN" else "default"
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}, "env"


BROKER_URL = os.environ.get("IBKR_BROKER_URL", "http://ibkr-broker:8080")
DRY_RUN, DRY_RUN_SOURCE = env_bool("IBKR_DRY_RUN", bool(ctx.get("dry_run", True)))
SEND_DRY_RUN_TO_BROKER = os.environ.get("IBKR_SEND_DRY_RUN_TO_BROKER", "false").lower() == "true"
REQUIRE_PAPER_ACCOUNT = os.environ.get("IBKR_REQUIRE_PAPER_ACCOUNT", "true").lower() != "false"
PAPER_ACCOUNT_PREFIXES = tuple(
    p.strip().upper()
    for p in os.environ.get("IBKR_PAPER_ACCOUNT_PREFIXES", "DU").split(",")
    if p.strip()
)
FILL_CONFIRM_SECONDS = max(0, int(float(os.environ.get("IBKR_FILL_CONFIRM_SECONDS", "6") or 6)))
FILL_POLL_INTERVAL_SECONDS = max(1, int(float(os.environ.get("IBKR_FILL_POLL_INTERVAL_SECONDS", "2") or 2)))
NODE_TIME_BUDGET_SECONDS = max(15, int(float(os.environ.get("IBKR_SEND_NODE_TIME_BUDGET_SECONDS", "50") or 50)))
NODE_STARTED_AT = time.time()


def remaining_node_seconds(buffer_seconds: float = 4.0) -> float:
    return max(0.0, NODE_TIME_BUDGET_SECONDS - (time.time() - NODE_STARTED_AT) - buffer_seconds)


def deterministic_client_order_id(seed: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


def broker_order_id(result):
    raw = result.get("ibkr_response") or result.get("details") or result
    if isinstance(raw, list) and raw:
        return raw[0].get("order_id") or raw[0].get("orderId") or raw[0].get("id")
    if isinstance(raw, dict):
        return raw.get("order_id") or raw.get("orderId") or raw.get("id")
    return None


def broker_result_error(result):
    raw = result.get("ibkr_response") or result.get("details") or result
    rows = raw if isinstance(raw, list) else [raw] if isinstance(raw, dict) else []
    messages = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for field in ("error", "message", "warning", "text"):
            value = row.get(field)
            if isinstance(value, list):
                messages.extend(str(v) for v in value if str(v).strip())
            elif value:
                messages.append(str(value))
        action = str(row.get("action") or "").lower()
        status = str(row.get("status") or "").lower()
        if "reject" in action or "rejected" in status or row.get("error"):
            return " | ".join(messages) or action or status or "IBKR_ORDER_REJECTED"
    if result.get("error"):
        return str(result.get("error"))
    return ""


def normalize_order_type(value):
    text = str(value or "MKT").strip().upper()
    if text in {"MARKET", "CASH_CONVERSION"}:
        return "MKT"
    if text == "LIMIT":
        return "LMT"
    return text or "MKT"


def get_json(path: str, timeout: int = 8):
    req = urllib.request.Request(f"{BROKER_URL}{path}", headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def post_json(path: str, payload: dict, timeout: int = 10):
    req = urllib.request.Request(
        f"{BROKER_URL}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def paper_account_ids(positions_payload):
    ids = []
    for row in positions_payload or []:
        acct = str(row.get("acctId") or row.get("accountId") or "").strip().upper()
        if acct and acct not in ids:
            ids.append(acct)
    return ids


def account_ids_from_payload(payload):
    ids = []

    def visit(value):
        if isinstance(value, dict):
            candidates = [
                value.get("acctcode"),
                value.get("acctCode"),
                value.get("accountcode"),
                value.get("accountCode"),
                value.get("accountId"),
                value.get("acctId"),
                value.get("account"),
            ]
            for candidate in candidates:
                if isinstance(candidate, dict):
                    candidate = candidate.get("value") or candidate.get("amount")
                acct = str(candidate or "").strip().upper()
                if acct and acct not in ids:
                    ids.append(acct)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(payload)
    return ids


def merge_account_ids(*groups):
    ids = []
    for group in groups:
        for acct in group or []:
            acct = str(acct or "").strip().upper()
            if acct and acct not in ids:
                ids.append(acct)
    return ids


def possible_fill_keys(fill):
    keys = set()
    for field in (
        "order_ref",
        "orderRef",
        "cOID",
        "client_order_id",
        "clientOrderId",
        "order_id",
        "orderId",
    ):
        value = fill.get(field)
        if value is not None and str(value).strip():
            keys.add(str(value).strip())
    return keys


def build_fill_map(fills_payload):
    out = {}
    for fill in fills_payload or []:
        if not isinstance(fill, dict):
            continue
        for key in possible_fill_keys(fill):
            out.setdefault(key, fill)
    return out


def poll_recent_fills(order_keys):
    if not order_keys or FILL_CONFIRM_SECONDS <= 0:
        return {}, []
    poll_seconds = min(float(FILL_CONFIRM_SECONDS), remaining_node_seconds())
    if poll_seconds <= 0:
        return {}, []
    deadline = time.time() + poll_seconds
    last_payload = []
    while True:
        try:
            timeout = max(1, min(6, int(max(1.0, deadline - time.time()))))
            last_payload = get_json("/fills", timeout=timeout)
            fill_map = build_fill_map(last_payload)
            matched = {key: fill_map[key] for key in order_keys if key in fill_map}
            if matched or time.time() >= deadline:
                return matched, last_payload
        except Exception:
            if time.time() >= deadline:
                return {}, last_payload
        time.sleep(min(FILL_POLL_INTERVAL_SECONDS, max(0.0, deadline - time.time())))


def broker_payload_for_orders(orders):
    return {
        "orders": [
            {
                "pair": o["pair"],
                "side": o["side"],
                "size_lots": float(o.get("size_lots", 0)),
                "order_id": o["order_id"],
                "client_order_id": o["client_order_id"],
                "order_type": normalize_order_type(o.get("order_type")),
                "limit_price": o.get("limit_price"),
                "is_currency_conversion": bool(o.get("is_currency_conversion")),
            }
            for o in orders
        ],
        "run_id": run_id,
    }


executable_orders = ctx.get("executable_orders") or []
run_id = ctx.get("run_id") or ""

for idx, order in enumerate(executable_orders, start=1):
    if not order.get("order_id"):
        order["order_id"] = f"ORD_{run_id}_{idx:03d}"
    if not order.get("client_order_id"):
        seed = "|".join(
            [
                str(run_id),
                str(order.get("order_id") or ""),
                str(order.get("pair") or ""),
                str(order.get("side") or ""),
                str(order.get("size_lots") or ""),
            ]
        )
        order["client_order_id"] = deterministic_client_order_id(seed)

ibkr_results = []
ibkr_errors = []
broker_called = False
broker_health = {}
broker_positions = []
broker_fills = []
matched_fills = {}
broker_dry_run = None
broker_account_ids = []

pending_orders = [o for o in executable_orders if o.get("status") == "pending"]

if pending_orders and (not DRY_RUN or SEND_DRY_RUN_TO_BROKER):
    try:
        broker_health = get_json("/health")
        broker_dry_run = bool(broker_health.get("dry_run"))
        authenticated = bool(broker_health.get("authenticated"))
        if not authenticated and not DRY_RUN:
            ibkr_errors.extend(
                {
                    "order_id": o.get("order_id"),
                    "client_order_id": o.get("client_order_id"),
                    "error": "IBKR_BROKER_NOT_AUTHENTICATED",
                }
                for o in pending_orders
            )
        elif not DRY_RUN and broker_dry_run:
            ibkr_errors.extend(
                {
                    "order_id": o.get("order_id"),
                    "client_order_id": o.get("client_order_id"),
                    "error": "IBKR_BROKER_STILL_IN_DRY_RUN",
                }
                for o in pending_orders
            )
        else:
            if REQUIRE_PAPER_ACCOUNT and not DRY_RUN:
                broker_positions = get_json("/positions")
                account_ids = paper_account_ids(broker_positions)
                if not account_ids:
                    try:
                        account_ids = merge_account_ids(account_ids, account_ids_from_payload(get_json("/account/ledger")))
                    except Exception:
                        pass
                if not account_ids:
                    try:
                        account_ids = merge_account_ids(account_ids, account_ids_from_payload(get_json("/account/summary")))
                    except Exception:
                        pass
                if not account_ids or not all(acct.startswith(PAPER_ACCOUNT_PREFIXES) for acct in account_ids):
                    ibkr_errors.extend(
                        {
                            "order_id": o.get("order_id"),
                            "client_order_id": o.get("client_order_id"),
                            "error": f"IBKR_PAPER_ACCOUNT_GUARD_FAILED: accounts={account_ids}",
                        }
                        for o in pending_orders
                    )
                broker_account_ids = account_ids

            if not ibkr_errors:
                broker_called = True
                has_prefunding = any(o.get("is_currency_conversion") or o.get("requires_funding_order_id") for o in pending_orders)
                if has_prefunding:
                    order_state = {o.get("order_id"): o for o in pending_orders}
                    for o in pending_orders:
                        funding_id = o.get("requires_funding_order_id")
                        if funding_id and order_state.get(funding_id, {}).get("status") != "filled":
                            ibkr_errors.append(
                                {
                                    "order_id": o.get("order_id"),
                                    "client_order_id": o.get("client_order_id"),
                                    "error": f"PREFUNDING_NOT_CONFIRMED:{funding_id}",
                                }
                            )
                            continue
                        response_data = post_json("/orders/fx", broker_payload_for_orders([o]))
                        broker_dry_run = bool(response_data.get("dry_run"))
                        ibkr_results.extend(response_data.get("results", []))
                        ibkr_errors.extend(response_data.get("errors", []))
                        if not DRY_RUN and broker_dry_run:
                            ibkr_results = []
                            ibkr_errors.append(
                                {
                                    "order_id": o.get("order_id"),
                                    "client_order_id": o.get("client_order_id"),
                                    "error": "IBKR_BROKER_STILL_IN_DRY_RUN",
                                }
                            )
                            continue
                        order_keys = {str(o.get("order_id") or ""), str(o.get("client_order_id") or "")}
                        order_keys.discard("")
                        new_matches, latest_fills = poll_recent_fills(order_keys)
                        broker_fills = latest_fills or broker_fills
                        matched_fills.update(new_matches)
                        if new_matches:
                            o["status"] = "filled"
                            o["ibkr_status"] = "filled"
                            o["ibkr_fill"] = next(iter(new_matches.values()))
                            o["ibkr_filled_at"] = o["ibkr_fill"].get("trade_time") or o["ibkr_fill"].get("tradeTime") or o["ibkr_fill"].get("time")
                        elif o.get("is_currency_conversion") and not DRY_RUN:
                            # Do not send the dependent target order until the cash
                            # conversion fill is visible in IBKR.
                            o["status"] = "submitted"
                    ibkr_errors.extend(
                        {
                            "order_id": o.get("order_id"),
                            "client_order_id": o.get("client_order_id"),
                            "error": f"PREFUNDING_NOT_CONFIRMED:{o.get('requires_funding_order_id')}",
                        }
                        for o in pending_orders
                        if o.get("requires_funding_order_id") and order_state.get(o.get("requires_funding_order_id"), {}).get("status") != "filled"
                    )
                else:
                    response_data = post_json("/orders/fx", broker_payload_for_orders(pending_orders))
                    broker_dry_run = bool(response_data.get("dry_run"))
                    ibkr_results = response_data.get("results", [])
                    ibkr_errors = response_data.get("errors", [])
                    if not DRY_RUN and broker_dry_run:
                        ibkr_results = []
                        ibkr_errors.extend(
                            {
                                "order_id": o.get("order_id"),
                                "client_order_id": o.get("client_order_id"),
                                "error": "IBKR_BROKER_STILL_IN_DRY_RUN",
                            }
                            for o in pending_orders
                        )
                    elif ibkr_results:
                        order_keys = set()
                        for o in pending_orders:
                            order_keys.add(str(o.get("order_id") or ""))
                            order_keys.add(str(o.get("client_order_id") or ""))
                        order_keys.discard("")
                        matched_fills, broker_fills = poll_recent_fills(order_keys)
    except (URLError, TimeoutError, Exception) as exc:
        ibkr_errors.append({"error": f"ibkr-broker unreachable: {exc}", "fallback": "sandbox_only"})

result_map = {
    (r.get("order_id") or r.get("client_order_id")): r
    for r in ibkr_results
    if r.get("order_id") or r.get("client_order_id")
}
error_map = {
    (e.get("order_id") or e.get("client_order_id")): e
    for e in ibkr_errors
    if e.get("order_id") or e.get("client_order_id")
}

for order in executable_orders:
    oid = order.get("order_id")
    cid = order.get("client_order_id")
    result = result_map.get(oid) or result_map.get(cid)
    error = error_map.get(oid) or error_map.get(cid)

    if result:
        result_error = broker_result_error(result)
        result_broker_order_id = broker_order_id(result)
        order["ibkr_status"] = result.get("status", "unknown")
        order["ibkr_response"] = result
        order["broker"] = "IBKR"
        order["broker_order_id"] = result_broker_order_id
        fill = matched_fills.get(str(oid or "")) or matched_fills.get(str(cid or ""))
        if result_error:
            order["ibkr_status"] = "error"
            order["ibkr_error"] = result_error
            if not DRY_RUN:
                order["status"] = "broker_error"
                order["rejection_reason"] = "IBKR_BROKER_ERROR"
        elif fill:
            order["status"] = "filled"
            order["ibkr_status"] = "filled"
            order["ibkr_fill"] = fill
            order["ibkr_filled_at"] = fill.get("trade_time") or fill.get("tradeTime") or fill.get("time")
        elif not DRY_RUN and not result_broker_order_id:
            order["ibkr_status"] = "error"
            order["ibkr_error"] = "IBKR_SUBMISSION_WITHOUT_ORDER_ID"
            order["status"] = "broker_error"
            order["rejection_reason"] = "IBKR_BROKER_ERROR"
        elif not DRY_RUN:
            order["status"] = "submitted"
    elif error:
        order["ibkr_status"] = "error"
        order["ibkr_error"] = error.get("error", "")
        order["broker"] = "IBKR"
        if not DRY_RUN:
            order["status"] = "broker_error"
            order["rejection_reason"] = "IBKR_BROKER_ERROR"
    elif DRY_RUN:
        order["ibkr_status"] = "dry_run"
        order["broker"] = "SIM"
    elif order.get("status") == "pending":
        order["ibkr_status"] = "not_sent"
        order["broker"] = "IBKR"
        order["status"] = "broker_error"
        order["rejection_reason"] = "IBKR_NOT_SENT"
    else:
        order["ibkr_status"] = "not_sent"

return [
    {
        "json": {
            **ctx,
            "executable_orders": executable_orders,
            "ibkr_send_summary": {
                "dry_run": DRY_RUN,
                "dry_run_source": DRY_RUN_SOURCE,
                "broker_dry_run": broker_dry_run,
                "broker_called": broker_called,
                "orders_considered": len(pending_orders),
                "orders_sent": len(ibkr_results),
                "errors": len(ibkr_errors),
                "fills_matched": len(matched_fills),
                "fills_seen": len(broker_fills),
                "fill_confirm_seconds": FILL_CONFIRM_SECONDS,
                "node_time_budget_seconds": NODE_TIME_BUDGET_SECONDS,
                "node_elapsed_seconds": round(time.time() - NODE_STARTED_AT, 3),
                "broker_url": BROKER_URL,
                "errors_detail": ibkr_errors,
                "health": broker_health,
                "paper_account_guard": {
                    "required": REQUIRE_PAPER_ACCOUNT,
                    "prefixes": list(PAPER_ACCOUNT_PREFIXES),
                    "detected_accounts": broker_account_ids or paper_account_ids(broker_positions),
                },
            },
        }
    }
]
