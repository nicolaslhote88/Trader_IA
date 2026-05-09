"""
Node 11c — IBKR Send Orders (AG1-FX-V2)

Posts pending orders from the three-pillar portfolio manager to ibkr-broker.
Identical behavior to V1's 11b but references V2 env vars and run_id prefix.
"""

import json
import os
import uuid
import urllib.request

BROKER_URL = os.environ.get("IBKR_BROKER_URL", "http://ibkr-broker:8080")
DRY_RUN = os.environ.get("IBKR_DRY_RUN", "true").lower() != "false"
SEND_DRY_RUN_TO_BROKER = os.environ.get("IBKR_SEND_DRY_RUN_TO_BROKER", "false").lower() == "true"


def deterministic_client_order_id(seed: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


def broker_order_id(result):
    raw = result.get("ibkr_response") or result.get("details") or result
    if isinstance(raw, list) and raw:
        return raw[0].get("order_id") or raw[0].get("orderId") or raw[0].get("id")
    if isinstance(raw, dict):
        return raw.get("order_id") or raw.get("orderId") or raw.get("id")
    return None


def normalize_order_type(value):
    text = str(value or "MKT").strip().upper()
    if text == "MARKET":
        return "MKT"
    if text == "LIMIT":
        return "LMT"
    return text or "MKT"


ctx = (_items or [{"json": {}}])[0].get("json", {})
executable_orders = ctx.get("executable_orders") or []
run_id = ctx.get("run_id") or ""

for idx, order in enumerate(executable_orders, start=1):
    if not order.get("order_id"):
        order["order_id"] = f"ORD_{run_id}_{idx:03d}"
    if not order.get("client_order_id"):
        seed = "|".join([
            str(run_id),
            str(order.get("order_id") or ""),
            str(order.get("pair") or ""),
            str(order.get("side") or ""),
            str(order.get("size_lots") or ""),
        ])
        order["client_order_id"] = deterministic_client_order_id(seed)

ibkr_results = []
ibkr_errors = []
broker_called = False

pending_orders = [o for o in executable_orders if o.get("status") == "pending"]

if pending_orders and (not DRY_RUN or SEND_DRY_RUN_TO_BROKER):
    payload = {
        "orders": [
            {
                "pair": o["pair"],
                "side": o["side"],
                "size_lots": float(o.get("size_lots", 0)),
                "order_id": o["order_id"],
                "client_order_id": o["client_order_id"],
                "order_type": normalize_order_type(o.get("order_type")),
                "limit_price": o.get("limit_price"),
            }
            for o in pending_orders
        ],
        "run_id": run_id,
    }

    try:
        broker_called = True
        req = urllib.request.Request(
            f"{BROKER_URL}/orders/fx",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            response_data = json.loads(resp.read().decode("utf-8"))
            ibkr_results = response_data.get("results", [])
            ibkr_errors = response_data.get("errors", [])
    except Exception as exc:
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
        order["ibkr_status"] = result.get("status", "unknown")
        order["ibkr_response"] = result
        order["broker"] = "IBKR"
        order["broker_order_id"] = broker_order_id(result)
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
                "broker_called": broker_called,
                "orders_considered": len(pending_orders),
                "orders_sent": len(ibkr_results),
                "errors": len(ibkr_errors),
                "broker_url": BROKER_URL,
                "errors_detail": ibkr_errors,
            },
        }
    }
]
