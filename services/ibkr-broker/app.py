"""
ibkr-broker — microservice FastAPI passerelle vers IBKR Client Portal API.

Variables d'environnement :
  IBKR_GATEWAY_URL   URL interne du clientportal.gw  (défaut: https://ibkr-gateway:5000)
  IBKR_DRY_RUN       "true" → log sans envoyer        (défaut: "true")
  IBKR_FX_ORDERS_ENABLED  "true" → autorise /orders/fx (défaut: "false")
  IBKR_SSL_VERIFY    "false" → ignore cert auto-signé  (défaut: "false")
  IBKR_ACCOUNT_ID    ID compte IBKR (optionnel, auto-détecté sinon)
  IBKR_AUTO_REAUTH_ENABLED  "true" → reinit /iserver/auth/ssodh/init si possible
  IBKR_AUTO_REAUTH_COMPETE  "false" → true deconnecte une session concurrente
  IBKR_ALERT_WEBHOOK_URL    webhook optionnel quand un relogin navigateur/2FA est requis
  IBKR_ASSISTED_LOGIN_ENABLED  expose si des credentials assistés sont configurés
  IBKR_AUTO_CONFIRM_PRICE_WARNINGS  confirme uniquement certains prompts prix IBKR apres garde-fou
  IBKR_PRICE_GUARD_URL             endpoint quote indépendant (défaut: http://yfinance-api:8080/quote)
  IBKR_PRICE_GUARD_MAX_DEVIATION_PCT  écart max limite/prix de référence
  IBKR_PRICE_GUARD_MAX_QUOTE_AGE_SECONDS  fraîcheur max du prix de référence
  IBKR_AUTO_CONFIRM_MAX_STEPS      nombre max de prompts prix confirmes en chaine

Endpoints exposés à n8n :
  GET  /health                  → statut session IBKR
  GET  /marketdata/fx/snapshot  → snapshot FX bid/ask/mid/spread
  POST /orders/fx               → envoyer ordres FX
  POST /orders/equity           → envoyer ordres actions/ETF
  GET  /fills                   → fills récents
  GET  /positions               → positions actuelles
  GET  /account/summary         → synthèse compte IBKR
  GET  /account/ledger          → cash balances réelles par devise
  POST /auth/tickle             → keepalive manuel
  POST /auth/initialize         → tentative de reinit brokerage session
  POST /auth/recover            → tickle + reinit + action opérateur si besoin
  GET  /auth/operator-action    → instruction relogin/2FA sans envoyer d'ordre
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from contract_cache import (
    FX_CONIDS,
    FX_META,
    fx_ibkr_side,
    get_fx_conid,
    get_stk_conid,
    parse_stk_symbol,
    store_stk_conid,
    stk_ibkr_side,
    yahoo_suffix_to_ibkr_exchanges,
)
from cpapi_client import CPAPIClient, CPAPIError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("ibkr_broker")


def _env_int(name: str, default: int, minimum: int | None = None) -> int:
    raw = os.environ.get(name, "")
    try:
        value = int(raw) if raw else default
    except ValueError:
        logger.warning("Invalid integer for %s=%r, using %s", name, raw, default)
        value = default
    if minimum is not None:
        value = max(minimum, value)
    return value


def _env_float(name: str, default: float, minimum: float | None = None) -> float:
    raw = os.environ.get(name, "")
    try:
        value = float(raw) if raw else default
    except ValueError:
        logger.warning("Invalid float for %s=%r, using %s", name, raw, default)
        value = default
    if minimum is not None:
        value = max(minimum, value)
    return value


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "")
    if not raw:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


# ─── Config ──────────────────────────────────────────────────────────────────
GATEWAY_URL = os.environ.get("IBKR_GATEWAY_URL", "https://ibkr-gateway:5000")
DRY_RUN = os.environ.get("IBKR_DRY_RUN", "true").lower() != "false"
FX_ORDERS_ENABLED = _env_bool("IBKR_FX_ORDERS_ENABLED", False)
SSL_VERIFY = os.environ.get("IBKR_SSL_VERIFY", "false").lower() == "true"
ACCOUNT_ID_OVERRIDE = os.environ.get("IBKR_ACCOUNT_ID", "")
KEEPALIVE_INTERVAL_SECONDS = _env_int("IBKR_KEEPALIVE_INTERVAL_SECONDS", 55, minimum=15)
AUTO_REAUTH_ENABLED = os.environ.get("IBKR_AUTO_REAUTH_ENABLED", "true").lower() != "false"
AUTO_REAUTH_COMPETE = os.environ.get("IBKR_AUTO_REAUTH_COMPETE", "false").lower() == "true"
ALERT_WEBHOOK_URL = os.environ.get("IBKR_ALERT_WEBHOOK_URL", "").strip()
ALERT_COOLDOWN_SECONDS = _env_int("IBKR_ALERT_COOLDOWN_SECONDS", 900, minimum=60)
LOGIN_URL = os.environ.get("IBKR_LOGIN_URL", "https://localhost:5000").strip()
LOGIN_TUNNEL_COMMAND = os.environ.get(
    "IBKR_LOGIN_TUNNEL_COMMAND",
    "ssh -L 5000:127.0.0.1:5000 root@100.104.236.78",
).strip()
ASSISTED_LOGIN_ENABLED = _env_bool("IBKR_ASSISTED_LOGIN_ENABLED", False)
IBKR_USERNAME_CONFIGURED = bool(os.environ.get("IBKR_USERNAME") or os.environ.get("IBEAM_ACCOUNT"))
IBKR_PASSWORD_CONFIGURED = bool(os.environ.get("IBKR_PASSWORD") or os.environ.get("IBEAM_PASSWORD"))
AUTO_CONFIRM_PRICE_WARNINGS = _env_bool("IBKR_AUTO_CONFIRM_PRICE_WARNINGS", False)
PRICE_GUARD_URL = os.environ.get("IBKR_PRICE_GUARD_URL", "http://yfinance-api:8080/quote").strip()
PRICE_GUARD_MAX_DEVIATION_PCT = _env_float("IBKR_PRICE_GUARD_MAX_DEVIATION_PCT", 3.0, minimum=0.0)
PRICE_GUARD_MAX_QUOTE_AGE_SECONDS = _env_int(
    "IBKR_PRICE_GUARD_MAX_QUOTE_AGE_SECONDS",
    28800,
    minimum=60,
)
AUTO_CONFIRM_MAX_STEPS = _env_int("IBKR_AUTO_CONFIRM_MAX_STEPS", 4, minimum=1)

# ─── Global client ───────────────────────────────────────────────────────────
_cpapi: CPAPIClient | None = None
_session_monitor: dict[str, Any] = {
    "last_check_at": None,
    "last_tickle_at": None,
    "last_tickle_ok": None,
    "last_tickle_error": None,
    "last_auth_status": None,
    "last_reauth_at": None,
    "last_reauth_ok": None,
    "last_reauth_error": None,
    "reauth_attempts": 0,
    "manual_login_required": False,
    "manual_login_since": None,
    "last_manual_login_alert_at": None,
    "last_manual_login_alert_error": None,
    "operator_action": None,
    "message": "Session monitor not started",
}


def get_client() -> CPAPIClient:
    if _cpapi is None:
        raise HTTPException(503, "CPAPI client not initialized")
    return _cpapi


def _extract_auth_status(payload: Any) -> dict | None:
    if not isinstance(payload, dict):
        return None
    nested = payload.get("iserver")
    if isinstance(nested, dict):
        auth_status = nested.get("authStatus")
        if isinstance(auth_status, dict):
            return auth_status
    if {"authenticated", "connected"}.intersection(payload.keys()):
        return payload
    return None


def _is_authenticated(status: dict | None) -> bool:
    return bool(status and status.get("authenticated") and status.get("connected"))


def _needs_manual_login(status: dict | None, error: str | None = None) -> bool:
    if error:
        lowered = error.lower()
        return any(marker in lowered for marker in ("401", "unauthorized", "login", "sso", "session"))
    return bool(status and not status.get("connected"))


def _assisted_login_status() -> dict[str, Any]:
    configured = ASSISTED_LOGIN_ENABLED and IBKR_USERNAME_CONFIGURED and IBKR_PASSWORD_CONFIGURED
    return {
        "enabled": ASSISTED_LOGIN_ENABLED,
        "credentials_configured": configured,
        "username_configured": IBKR_USERNAME_CONFIGURED,
        "password_configured": IBKR_PASSWORD_CONFIGURED,
        "mode": "credential_assisted_gateway" if configured else "manual_gateway_login",
        "note": (
            "Credentials are present for an assisted gateway login flow. IBKR 2FA may still require approval."
            if configured
            else "No assisted login credentials are active; use browser login plus IBKR 2FA."
        ),
    }


def _operator_action(reason: str, error: str | None = None) -> dict[str, Any]:
    return {
        "required": True,
        "reason": reason,
        "error": error,
        "login_url": LOGIN_URL,
        "tunnel_command": LOGIN_TUNNEL_COMMAND,
        "assisted_login": _assisted_login_status(),
        "next_steps": [
            "Open the SSH tunnel to the VPS gateway.",
            "Open the IBKR Client Portal Gateway login URL.",
            "Validate IBKR credentials and 2FA.",
            "Call POST /auth/recover or wait for the background keepalive.",
        ],
    }


def _clear_operator_action() -> None:
    _session_monitor["manual_login_required"] = False
    _session_monitor["manual_login_since"] = None
    _session_monitor["operator_action"] = None


async def _send_manual_login_alert(reason: str, error: str | None = None) -> None:
    if not ALERT_WEBHOOK_URL:
        return
    now = datetime.now(timezone.utc)
    last_raw = _session_monitor.get("last_manual_login_alert_at")
    if last_raw:
        try:
            last = datetime.fromisoformat(str(last_raw))
            if (now - last).total_seconds() < ALERT_COOLDOWN_SECONDS:
                return
        except Exception:
            pass

    payload = {
        "event": "IBKR_MANUAL_LOGIN_REQUIRED",
        "severity": "critical",
        "at": now.isoformat(),
        "reason": reason,
        "error": error,
        "gateway_url": GATEWAY_URL,
        "dry_run": DRY_RUN,
        "operator_action": _operator_action(reason, error),
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(ALERT_WEBHOOK_URL, json=payload)
            response.raise_for_status()
        _session_monitor["last_manual_login_alert_at"] = now.isoformat()
        _session_monitor["last_manual_login_alert_error"] = None
    except Exception as exc:
        _session_monitor["last_manual_login_alert_error"] = str(exc)
        logger.warning("IBKR manual-login alert failed: %s", exc)


async def _mark_manual_login_required(reason: str, error: str | None = None) -> None:
    _session_monitor["manual_login_required"] = True
    if not _session_monitor.get("manual_login_since"):
        _session_monitor["manual_login_since"] = now_iso()
    _session_monitor["operator_action"] = _operator_action(reason, error)
    _session_monitor["message"] = "Manual Client Portal login required"
    await _send_manual_login_alert(reason, error)


def _remember_auth_status(status: dict | None) -> None:
    if status is not None:
        _session_monitor["last_auth_status"] = status
        if _is_authenticated(status):
            _clear_operator_action()
            _session_monitor["message"] = "IBKR session authenticated"


async def _initialize_brokerage_session(client: CPAPIClient, reason: str) -> dict:
    _session_monitor["reauth_attempts"] = int(_session_monitor.get("reauth_attempts") or 0) + 1
    _session_monitor["last_reauth_at"] = now_iso()
    try:
        response = await client.initialize_brokerage_session(
            publish=True,
            compete=AUTO_REAUTH_COMPETE,
        )
        status = _extract_auth_status(response) or response
        _remember_auth_status(status if isinstance(status, dict) else None)
        ok = _is_authenticated(status if isinstance(status, dict) else None)
        _session_monitor["last_reauth_ok"] = ok
        _session_monitor["last_reauth_error"] = None
        if ok:
            _clear_operator_action()
            _session_monitor["message"] = f"Brokerage session reinitialized ({reason})"
        else:
            _session_monitor["message"] = f"Brokerage session reinit returned unauthenticated ({reason})"
        logger.info("IBKR brokerage session reinit | reason=%s | ok=%s", reason, ok)
        return {"ok": ok, "response": response}
    except Exception as exc:
        error = str(exc)
        _session_monitor["last_reauth_ok"] = False
        _session_monitor["last_reauth_error"] = error
        if _needs_manual_login(None, error):
            await _mark_manual_login_required("reauth_failed", error)
        else:
            _session_monitor["message"] = f"Brokerage session reinit failed: {error}"
        logger.warning("IBKR brokerage session reinit failed | reason=%s | error=%s", reason, error)
        return {"ok": False, "error": error}


async def _maintain_ibkr_session(client: CPAPIClient, reason: str) -> dict:
    _session_monitor["last_check_at"] = now_iso()
    try:
        tickle_response = await client.tickle()
        _session_monitor["last_tickle_at"] = now_iso()
        _session_monitor["last_tickle_ok"] = True
        _session_monitor["last_tickle_error"] = None
        status = _extract_auth_status(tickle_response)
        if status is None:
            status = await client.auth_status()
        _remember_auth_status(status)
        if _is_authenticated(status):
            return {"ok": True, "authenticated": True, "tickle": tickle_response, "status": status}
        if AUTO_REAUTH_ENABLED and status.get("connected"):
            reauth = await _initialize_brokerage_session(client, reason)
            return {
                "ok": bool(reauth.get("ok")),
                "authenticated": bool(reauth.get("ok")),
                "tickle": tickle_response,
                "status": _session_monitor.get("last_auth_status"),
                "reauth": reauth,
            }
        if _needs_manual_login(status):
            await _mark_manual_login_required("gateway_disconnected", json.dumps(status, ensure_ascii=False))
        else:
            _session_monitor["message"] = "IBKR session is connected but not authenticated"
        return {"ok": False, "authenticated": False, "tickle": tickle_response, "status": status}
    except Exception as exc:
        error = str(exc)
        _session_monitor["last_tickle_ok"] = False
        _session_monitor["last_tickle_error"] = error
        if _needs_manual_login(None, error):
            await _mark_manual_login_required("keepalive_failed", error)
        else:
            _session_monitor["message"] = f"IBKR keepalive failed: {error}"
        logger.warning("IBKR keepalive failed | reason=%s | error=%s", reason, error)
        return {"ok": False, "authenticated": False, "error": error}


# ─── Background keepalive ─────────────────────────────────────────────────────
async def _keepalive_loop():
    """Maintient la session IBKR et tente la reinit brokerage quand possible."""
    while True:
        try:
            client = get_client()
            result = await _maintain_ibkr_session(client, "background_keepalive")
            logger.debug("IBKR keepalive result: %s", result.get("message") or result.get("ok"))
        except Exception as exc:
            logger.warning("IBKR keepalive loop failed: %s", exc)
        await asyncio.sleep(max(15, KEEPALIVE_INTERVAL_SECONDS))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _cpapi
    _cpapi = CPAPIClient(GATEWAY_URL, ssl_verify=SSL_VERIFY)
    if ACCOUNT_ID_OVERRIDE:
        _cpapi._account_id = ACCOUNT_ID_OVERRIDE
    asyncio.create_task(_keepalive_loop())
    logger.info("ibkr-broker started | gateway=%s | dry_run=%s", GATEWAY_URL, DRY_RUN)
    yield
    await _cpapi.close()


import approval  # human-in-the-loop order approval (flag-gated)
from fastapi import Body as ApprovalBody

app = FastAPI(title="ibkr-broker", version="1.0.0", lifespan=lifespan)


# ─── Modèles Pydantic ─────────────────────────────────────────────────────────

class FXOrder(BaseModel):
    pair: str            # ex: "EURUSD"
    side: str            # buy_base | sell_base | close_long | close_short
    size_lots: float     # 1 lot = 100 000 unités base
    order_id: str        # ID interne Trader_IA
    client_order_id: str | None = None  # devient cOID pour idempotence IBKR
    order_type: str = "MKT"
    limit_price: float | None = None
    is_currency_conversion: bool = False


class EquityOrder(BaseModel):
    symbol: str          # ex: "MC.PA"
    side: str            # BUY | SELL
    quantity: float
    order_id: str        # ID interne Trader_IA
    client_order_id: str | None = None  # devient cOID pour idempotence IBKR
    order_type: str = "MKT"
    limit_price: float | None = None
    isin: str | None = None
    exchange: str | None = None


class FXOrdersRequest(BaseModel):
    orders: list[FXOrder]
    run_id: str = ""


class EquityOrdersRequest(BaseModel):
    orders: list[EquityOrder]
    run_id: str = ""


# ─── Helpers ──────────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_order_type(value: str) -> str:
    text = str(value or "MKT").strip().upper()
    if text in {"MARKET", "CASH_CONVERSION"}:
        return "MKT"
    if text == "LIMIT":
        return "LMT"
    return text or "MKT"


def _dry_run_result(order_id: str, details: dict) -> dict:
    return {
        "order_id": order_id,
        "client_order_id": details.get("cOID"),
        "status": "dry_run",
        "ibkr_order_id": None,
        "message": "DRY_RUN — order logged but NOT sent to IBKR",
        "details": details,
        "sent_at": now_iso(),
    }


def _as_list(response: Any) -> list[Any]:
    if response is None:
        return []
    return response if isinstance(response, list) else [response]


def _has_order_identifier(item: dict) -> bool:
    return any(item.get(key) for key in ("order_id", "orderId", "orderid"))


def _reply_required_items(response: Any) -> list[dict]:
    """Return IBKR prompt/reply objects that still need human confirmation."""
    return [
        item for item in _as_list(response)
        if isinstance(item, dict) and item.get("id") and not _has_order_identifier(item)
    ]


def _reply_messages(response: Any) -> list[str]:
    messages = []
    for item in _reply_required_items(response):
        raw_message = item.get("message") or item.get("text") or []
        if isinstance(raw_message, list):
            messages.extend(str(m) for m in raw_message)
        elif raw_message:
            messages.append(str(raw_message))
    return [m.strip() for m in messages if m and m.strip()]


def _reply_required_error(
    order_id: str,
    client_order_id: str,
    response: Any,
    extra: dict | None = None,
) -> dict:
    messages = _reply_messages(response)
    out = {
        "order_id": order_id,
        "client_order_id": client_order_id,
        "status": "needs_confirmation",
        "error": "IBKR_ORDER_NEEDS_CONFIRMATION" + (f": {' | '.join(messages)}" if messages else ""),
        "ibkr_response": response,
    }
    if extra:
        out.update(extra)
    return out


def _ibkr_error_messages(response: Any) -> list[str]:
    messages = []
    for item in _as_list(response):
        if not isinstance(item, dict):
            continue
        for field in ("error", "errorMessage", "error_message"):
            raw = item.get(field)
            if raw:
                messages.append(str(raw))
        raw_message = item.get("message") or item.get("text")
        if raw_message and not item.get("order_id") and not item.get("id"):
            if isinstance(raw_message, list):
                messages.extend(str(m) for m in raw_message)
            else:
                messages.append(str(raw_message))
    return [m.strip() for m in messages if m and m.strip()]


def _ibkr_order_error(
    order_id: str,
    client_order_id: str,
    response: Any,
    extra: dict | None = None,
) -> dict:
    messages = _ibkr_error_messages(response)
    out = {
        "order_id": order_id,
        "client_order_id": client_order_id,
        "status": "error",
        "error": "IBKR_ORDER_REJECTED" + (f": {' | '.join(messages)}" if messages else ""),
        "ibkr_response": response,
    }
    if extra:
        out.update(extra)
    return out


def _is_price_confirmation_prompt(messages: list[str]) -> bool:
    if not messages:
        return False
    joined = " | ".join(messages).lower()
    danger_markers = (
        "margin",
        "insufficient",
        "short sale",
        "shortable",
        "locate",
        "restricted",
        "not allowed",
    )
    if any(marker in joined for marker in danger_markers):
        return False
    return (
        ("price" in joined and "percentage constraint" in joined)
        or "mandatory cap price" in joined
        or (
            "fair and orderly market" in joined
            and "may set a cap" in joined
        )
    )


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _quote_reference_price(quote: dict, side: str) -> tuple[float | None, str]:
    regular = _num(quote.get("regularMarketPrice"))
    if regular and regular > 0:
        return regular, "regularMarketPrice"

    bid = _num(quote.get("bid"))
    ask = _num(quote.get("ask"))
    if bid and ask and bid > 0 and ask > 0 and ask >= bid:
        return (bid + ask) / 2.0, "mid"

    side_u = str(side or "").upper()
    if side_u == "BUY" and ask and ask > 0:
        return ask, "ask"
    if side_u == "SELL" and bid and bid > 0:
        return bid, "bid"

    for field in ("mid", "ask", "bid"):
        value = _num(quote.get(field))
        if value and value > 0:
            return value, field
    return None, ""


async def _fetch_equity_quote(symbol: str, side: str, qty: float) -> dict:
    if not PRICE_GUARD_URL:
        return {"ok": False, "error": "IBKR_PRICE_GUARD_URL_EMPTY"}
    params = {
        "symbols": symbol,
        "side": str(side or "BUY").upper(),
        "qty": qty,
        "max_age_seconds": 300,
    }
    async with httpx.AsyncClient(timeout=10.0) as http:
        response = await http.get(PRICE_GUARD_URL, params=params)
        response.raise_for_status()
        payload = response.json()
    quotes = payload.get("quotes") if isinstance(payload, dict) else None
    if not isinstance(quotes, list) or not quotes:
        return {"ok": False, "error": "NO_QUOTE_ROW", "payload": payload}
    return quotes[0] if isinstance(quotes[0], dict) else {"ok": False, "error": "INVALID_QUOTE_ROW"}


def _parse_ibkr_snapshot_price(snapshot: Any, side: str) -> "float | None":
    try:
        rows = snapshot if isinstance(snapshot, list) else []
        row = rows[0] if rows else {}
        def _n(v):
            if v is None:
                return None
            t = str(v).lstrip("CcHh ").replace(",", "").strip()
            try:
                return float(t)
            except ValueError:
                return None
        last = _n(row.get("31")); bid = _n(row.get("84")); ask = _n(row.get("86"))
        if str(side).upper() == "BUY":
            return ask or last or bid
        return bid or last or ask
    except Exception:
        return None


async def _ibkr_reference_price(client: Any, ibkr_payload: dict, side: str) -> "float | None":
    conid = ibkr_payload.get("conid")
    if not conid:
        return None
    try:
        cid = int(conid)
    except (TypeError, ValueError):
        return None
    try:
        await client.marketdata_snapshot([cid], fields="31,84,86")
        await asyncio.sleep(1.0)
        snap = await client.marketdata_snapshot([cid], fields="31,84,86")
        return _parse_ibkr_snapshot_price(snap, side)
    except Exception:
        return None


def _apply_reference(guard: dict, ref_price: float, limit_price: float, field: str) -> dict:
    guard["reference_price"] = ref_price
    guard["reference_field"] = field
    dev = abs(limit_price - ref_price) / ref_price * 100.0
    guard["deviation_pct"] = dev
    if dev <= PRICE_GUARD_MAX_DEVIATION_PCT:
        guard["ok"] = True
        guard["reason"] = "PRICE_WITHIN_GUARD"
    else:
        guard["reason"] = "PRICE_DEVIATION_TOO_HIGH"
    return guard


async def _price_confirmation_guard(client: Any, order: Any, ibkr_payload: dict, response: Any) -> dict[str, Any]:
    messages = _reply_messages(response)
    guard: dict[str, Any] = {
        "enabled": AUTO_CONFIRM_PRICE_WARNINGS,
        "ok": False,
        "reason": "",
        "prompt_messages": messages,
        "max_deviation_pct": PRICE_GUARD_MAX_DEVIATION_PCT,
        "max_quote_age_seconds": PRICE_GUARD_MAX_QUOTE_AGE_SECONDS,
        "quote_url": PRICE_GUARD_URL,
    }
    if not AUTO_CONFIRM_PRICE_WARNINGS:
        guard["reason"] = "AUTO_CONFIRM_PRICE_WARNINGS_DISABLED"
        return guard
    if not _is_price_confirmation_prompt(messages):
        guard["reason"] = "PROMPT_NOT_PRICE_CONFIRMATION"
        return guard
    if normalize_order_type(ibkr_payload.get("orderType")) != "LMT":
        guard["reason"] = "ORDER_TYPE_NOT_LIMIT"
        return guard

    limit_price = _num(ibkr_payload.get("price"))
    if not limit_price or limit_price <= 0:
        guard["reason"] = "MISSING_LIMIT_PRICE"
        return guard

    symbol = str(getattr(order, "symbol", "") or "").strip()
    if not symbol:
        guard["reason"] = "MISSING_SYMBOL"
        return guard

    try:
        quote = await _fetch_equity_quote(
            symbol,
            str(ibkr_payload.get("side") or "BUY"),
            float(ibkr_payload.get("quantity") or 0),
        )
    except Exception as exc:
        guard["reason"] = f"QUOTE_FETCH_FAILED:{exc}"
        return guard

    ref_price, ref_field = _quote_reference_price(quote, str(ibkr_payload.get("side") or "BUY"))
    guard["quote"] = {
        "symbol": quote.get("symbol"),
        "resolvedSymbol": quote.get("resolvedSymbol"),
        "source": quote.get("source"),
        "regularMarketPrice": quote.get("regularMarketPrice"),
        "bid": quote.get("bid"),
        "ask": quote.get("ask"),
        "mid": quote.get("mid"),
        "marketState": quote.get("marketState"),
        "regularMarketTime": quote.get("regularMarketTime"),
        "lastTradeTime": quote.get("lastTradeTime"),
        "fetchedAt": quote.get("fetchedAt"),
        "isDelayed": quote.get("isDelayed"),
    }
    guard["reference_price"] = ref_price
    guard["reference_field"] = ref_field
    guard["limit_price"] = limit_price

    if not ref_price or ref_price <= 0:
        _ibkr_ref = await _ibkr_reference_price(client, ibkr_payload, str(ibkr_payload.get("side") or "BUY"))
        if _ibkr_ref and _ibkr_ref > 0:
            return _apply_reference(guard, _ibkr_ref, limit_price, "ibkr_snapshot")
        guard["reason"] = "NO_REFERENCE_PRICE"
        return guard

    quote_ts = (
        _parse_iso_datetime(quote.get("regularMarketTime"))
        or _parse_iso_datetime(quote.get("lastTradeTime"))
        or _parse_iso_datetime(quote.get("fetchedAt"))
    )
    if quote_ts:
        age_seconds = (datetime.now(timezone.utc) - quote_ts).total_seconds()
        guard["quote_age_seconds"] = age_seconds
        if age_seconds > PRICE_GUARD_MAX_QUOTE_AGE_SECONDS:
            _ibkr_ref = await _ibkr_reference_price(client, ibkr_payload, str(ibkr_payload.get("side") or "BUY"))
            if _ibkr_ref and _ibkr_ref > 0:
                return _apply_reference(guard, _ibkr_ref, limit_price, "ibkr_snapshot")
            guard["reason"] = "QUOTE_TOO_OLD"
            return guard

    deviation_pct = abs(limit_price - ref_price) / ref_price * 100.0
    guard["deviation_pct"] = deviation_pct
    if deviation_pct <= PRICE_GUARD_MAX_DEVIATION_PCT:
        guard["ok"] = True
        guard["reason"] = "PRICE_WITHIN_GUARD"
    else:
        guard["reason"] = "PRICE_DEVIATION_TOO_HIGH"
    return {
        **guard,
    }


async def _confirm_price_prompt_chain(
    client: CPAPIClient,
    order: Any,
    ibkr_payload: dict,
    initial_response: Any,
) -> dict[str, Any]:
    """Confirm only qualified IBKR price prompts until IBKR returns a terminal response."""
    current_response = initial_response
    reply_responses: list[dict] = []
    attempts: list[dict[str, Any]] = []

    for step in range(1, AUTO_CONFIRM_MAX_STEPS + 1):
        prompt_items = _reply_required_items(current_response)
        if not prompt_items:
            return {
                "ok": True,
                "reason": "CONFIRMATION_CHAIN_COMPLETE",
                "terminal_response": current_response,
                "reply_responses": reply_responses,
                "attempts": attempts,
            }

        guard = await _price_confirmation_guard(client, order, ibkr_payload, current_response)
        attempt = {
            "step": step,
            "reply_ids": [str(item.get("id")) for item in prompt_items],
            "prompt_messages": _reply_messages(current_response),
            "guard": guard,
        }
        attempts.append(attempt)

        if not guard.get("ok"):
            return {
                "ok": False,
                "reason": "CONFIRMATION_GUARD_REJECTED",
                "terminal_response": current_response,
                "reply_responses": reply_responses,
                "attempts": attempts,
            }

        next_response: list[dict] = []
        for item in prompt_items:
            next_response.extend(await client.reply_order(str(item["id"]), confirmed=True))
        reply_responses.extend(next_response)
        current_response = next_response

        if _ibkr_error_messages(current_response):
            return {
                "ok": False,
                "reason": "IBKR_ERROR_AFTER_CONFIRMATION",
                "terminal_response": current_response,
                "reply_responses": reply_responses,
                "attempts": attempts,
            }

    if not _reply_required_items(current_response):
        return {
            "ok": True,
            "reason": "CONFIRMATION_CHAIN_COMPLETE",
            "terminal_response": current_response,
            "reply_responses": reply_responses,
            "attempts": attempts,
        }

    return {
        "ok": False,
        "reason": "MAX_CONFIRMATION_STEPS_EXCEEDED",
        "terminal_response": current_response,
        "reply_responses": reply_responses,
        "attempts": attempts,
    }


async def _account_alignment_status(client: CPAPIClient) -> dict[str, Any]:
    out: dict[str, Any] = {
        "configured_account_id": ACCOUNT_ID_OVERRIDE or None,
        "configured_account_type": (
            "paper" if ACCOUNT_ID_OVERRIDE.upper().startswith("DU")
            else ("live" if ACCOUNT_ID_OVERRIDE else None)
        ),
        "gateway_accounts": [],
        "selected_account": None,
        "gateway_is_paper": None,
        "aligned": None,
        "error": None,
    }
    try:
        payload = await client._get("/v1/api/iserver/accounts")
    except Exception as exc:
        out["aligned"] = False
        out["error"] = str(exc)
        return out

    accounts = payload.get("accounts") if isinstance(payload, dict) else []
    selected = payload.get("selectedAccount") if isinstance(payload, dict) else None
    out["gateway_accounts"] = accounts if isinstance(accounts, list) else []
    out["selected_account"] = selected
    out["gateway_is_paper"] = bool(payload.get("isPaper")) if isinstance(payload, dict) else None
    if ACCOUNT_ID_OVERRIDE:
        out["aligned"] = ACCOUNT_ID_OVERRIDE in out["gateway_accounts"]
    else:
        out["aligned"] = bool(out["gateway_accounts"])
    return out


def _contract_exchanges(contract: dict) -> set[str]:
    exchanges = {
        str(contract.get("listingExchange") or "").upper(),
        str(contract.get("exchange") or "").upper(),
    }
    all_exchanges = str(contract.get("allExchanges") or "")
    exchanges.update(part.strip().upper() for part in all_exchanges.split(",") if part.strip())
    description = str(contract.get("description") or "").strip()
    if description:
        exchanges.add(description.upper())
    company_header = str(contract.get("companyHeader") or "").strip()
    if " - " in company_header:
        suffix = company_header.rsplit(" - ", 1)[-1].strip()
        if suffix:
            exchanges.add(suffix.upper())
    for section in contract.get("sections") or []:
        exchanges.add(str(section.get("exchange") or "").upper())
    return {exchange for exchange in exchanges if exchange}


def _contract_symbol(contract: dict) -> str:
    return str(contract.get("symbol") or contract.get("ticker") or "").strip().upper()


def _position_qty_by_conid(positions: list[dict], conid: int) -> float:
    for position in positions:
        try:
            if int(position.get("conid") or 0) == int(conid):
                return float(position.get("position") or 0)
        except (TypeError, ValueError):
            continue
    return 0.0


async def _resolve_stk_conid(
    client: CPAPIClient,
    symbol: str,
    isin: str | None = None,
    exchange_override: str | None = None,
) -> int:
    """
    Résout le conid IBKR d'un symbole action.
    Utilise le cache en mémoire, sinon appel CPAPI secdef search.
    """
    cached = get_stk_conid(symbol)
    if cached:
        return cached

    ticker, suffix = parse_stk_symbol(symbol)
    exchange_candidates = tuple(
        x.strip().upper()
        for x in str(exchange_override or "").replace(";", ",").split(",")
        if x.strip()
    ) or yahoo_suffix_to_ibkr_exchanges(suffix)

    contracts = []
    if isin:
        try:
            contracts = await client.search_contract(str(isin).strip(), sec_type="STK")
        except Exception:
            contracts = []
    if not contracts:
        contracts = await client.search_contract(ticker, sec_type="STK")
    if not contracts:
        raise HTTPException(404, f"No IBKR contract found for {symbol}")

    exact_symbol_contracts = [c for c in contracts if _contract_symbol(c) == ticker.upper()]
    if exact_symbol_contracts:
        contracts = exact_symbol_contracts

    # Cherche l'exchange correspondant. Pour un symbole suffixe (ex: .PA),
    # on refuse le fallback vers un autre marche afin d'eviter CRI.PA -> CRI US.
    best = None
    for wanted_exchange in exchange_candidates:
        for c in contracts:
            if wanted_exchange in _contract_exchanges(c):
                best = {"conid": int(c["conid"]), "exchange": wanted_exchange}
                break
        if best:
            break

    if not best:
        if suffix:
            # Controlled fallback for suffixed symbols: SMART is acceptable only
            # when CPAPI advertises it for the returned contract. This fixes
            # .PA failures caused by SBF not being listed in CPAPI search output.
            for c in contracts:
                if "SMART" in _contract_exchanges(c):
                    best = {"conid": int(c["conid"]), "exchange": "SMART"}
                    break
            if not best:
                available = sorted({ex for c in contracts for ex in _contract_exchanges(c)})
                raise HTTPException(
                    404,
                    f"No IBKR contract found for {symbol} on exchanges {','.join(exchange_candidates)} "
                    f"(available={','.join(available[:20])})",
                )
        if not best:
            first = contracts[0]
            best = {"conid": int(first["conid"]), "exchange": "SMART"}

    store_stk_conid(symbol, best["conid"])
    return best["conid"]


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> dict:
    """Vérifie l'état de la session IBKR."""
    client = get_client()
    try:
        status = await client.auth_status()
        _remember_auth_status(status)
        authenticated = bool(status.get("authenticated") and status.get("connected"))
        account_alignment = await _account_alignment_status(client)
        return {
            "dry_run": DRY_RUN,
            "fx_orders_enabled": FX_ORDERS_ENABLED,
            "auto_confirm_price_warnings": AUTO_CONFIRM_PRICE_WARNINGS,
            "price_guard": {
                "url": PRICE_GUARD_URL,
                "max_deviation_pct": PRICE_GUARD_MAX_DEVIATION_PCT,
                "max_quote_age_seconds": PRICE_GUARD_MAX_QUOTE_AGE_SECONDS,
                "auto_confirm_max_steps": AUTO_CONFIRM_MAX_STEPS,
            },
            "gateway_url": GATEWAY_URL,
            "authenticated": authenticated,
            "account_alignment": account_alignment,
            "ibkr_status": status,
            "session_monitor": _session_monitor,
            "auto_reauth_enabled": AUTO_REAUTH_ENABLED,
            "auto_reauth_compete": AUTO_REAUTH_COMPETE,
            "assisted_login": _assisted_login_status(),
            "operator_action": _session_monitor.get("operator_action"),
        }
    except Exception as exc:
        if _needs_manual_login(None, str(exc)):
            await _mark_manual_login_required("auth_status_failed", str(exc))
        else:
            _session_monitor["message"] = f"IBKR auth status failed: {exc}"
        return {
            "dry_run": DRY_RUN,
            "fx_orders_enabled": FX_ORDERS_ENABLED,
            "auto_confirm_price_warnings": AUTO_CONFIRM_PRICE_WARNINGS,
            "price_guard": {
                "url": PRICE_GUARD_URL,
                "max_deviation_pct": PRICE_GUARD_MAX_DEVIATION_PCT,
                "max_quote_age_seconds": PRICE_GUARD_MAX_QUOTE_AGE_SECONDS,
                "auto_confirm_max_steps": AUTO_CONFIRM_MAX_STEPS,
            },
            "gateway_url": GATEWAY_URL,
            "authenticated": False,
            "account_alignment": {
                "configured_account_id": ACCOUNT_ID_OVERRIDE or None,
                "aligned": False,
                "error": str(exc),
            },
            "error": str(exc),
            "session_monitor": _session_monitor,
            "auto_reauth_enabled": AUTO_REAUTH_ENABLED,
            "auto_reauth_compete": AUTO_REAUTH_COMPETE,
            "assisted_login": _assisted_login_status(),
            "operator_action": _session_monitor.get("operator_action"),
        }


@app.post("/auth/tickle")
async def manual_tickle() -> dict:
    """Keepalive manuel de la session IBKR."""
    client = get_client()
    try:
        return await _maintain_ibkr_session(client, "manual_tickle")
    except CPAPIError as exc:
        raise HTTPException(502, f"Tickle failed: {exc}") from exc


@app.post("/auth/initialize")
async def manual_initialize_brokerage_session() -> dict:
    """Tente de reinitialiser la session brokerage sans relogin navigateur."""
    client = get_client()
    result = await _initialize_brokerage_session(client, "manual_endpoint")
    if not result.get("ok"):
        raise HTTPException(502, result)
    return result


@app.post("/auth/recover")
async def recover_ibkr_session() -> dict:
    """
    Lance la sequence de recuperation non destructive.

    1. tickle + auth/status
    2. /iserver/auth/ssodh/init si la session Gateway est encore connectee
    3. sinon renvoie l'action operateur attendue pour login navigateur + 2FA
    """
    client = get_client()
    result = await _maintain_ibkr_session(client, "manual_recover_endpoint")
    if result.get("authenticated"):
        return {
            "ok": True,
            "authenticated": True,
            "session_monitor": _session_monitor,
            "operator_action": None,
        }
    return {
        "ok": False,
        "authenticated": False,
        "session_monitor": _session_monitor,
        "operator_action": _session_monitor.get("operator_action")
        or _operator_action("recover_failed", json.dumps(result, default=str, ensure_ascii=False)),
    }


@app.get("/auth/operator-action")
async def ibkr_operator_action() -> dict:
    """Retourne l'action humaine attendue si IBKR impose un relogin/2FA."""
    return {
        "manual_login_required": bool(_session_monitor.get("manual_login_required")),
        "operator_action": _session_monitor.get("operator_action"),
        "assisted_login": _assisted_login_status(),
        "session_monitor": _session_monitor,
    }


@app.get("/fills")
async def get_fills() -> list[dict]:
    """Retourne les fills récents depuis IBKR."""
    client = get_client()
    try:
        return await client.get_recent_trades()
    except CPAPIError as exc:
        raise HTTPException(502, str(exc)) from exc


@app.get("/positions")
async def get_positions() -> list[dict]:
    """Retourne les positions actuelles depuis IBKR."""
    client = get_client()
    try:
        return await client.get_portfolio_positions()
    except CPAPIError as exc:
        raise HTTPException(502, str(exc)) from exc


@app.get("/account/summary")
async def get_account_summary() -> dict:
    """Retourne le résumé de compte IBKR."""
    client = get_client()
    try:
        return await client.get_account_summary()
    except CPAPIError as exc:
        raise HTTPException(502, str(exc)) from exc


@app.get("/account/ledger")
async def get_account_ledger() -> dict:
    """Retourne les cash balances réelles par devise depuis IBKR."""
    client = get_client()
    try:
        return await client.get_account_ledger()
    except CPAPIError as exc:
        raise HTTPException(502, str(exc)) from exc


@app.get("/contracts/equity/resolve")
async def resolve_equity_contracts(
    symbols: str = Query(..., description="Comma-separated symbols, e.g. THEP.PA,ELEC.PA"),
    exchange: str = Query("", description="Optional exchange override, e.g. SBF,SMART"),
) -> dict[str, Any]:
    """Résout des contrats actions sans envoyer d'ordre."""
    client = get_client()
    requested = [s.strip() for s in symbols.split(",") if s.strip()]
    results = []
    errors = []
    for symbol in requested:
        try:
            conid = await _resolve_stk_conid(client, symbol, exchange_override=exchange or None)
            results.append({"symbol": symbol, "conid": conid})
        except HTTPException as exc:
            errors.append({"symbol": symbol, "error": exc.detail})
        except CPAPIError as exc:
            errors.append({"symbol": symbol, "error": str(exc)})
    return {
        "results": results,
        "errors": errors,
        "count": len(requested),
        "resolved": len(results),
        "dry_run": DRY_RUN,
    }


# ─── Market Data ─────────────────────────────────────────────────────────────

def _num(value: Any) -> float | None:
    try:
        if value is None:
            return None
        text = str(value).replace(",", "").strip()
        if not text:
            return None
        mult = 1.0
        suffix = text[-1:].upper()
        if suffix in {"K", "M", "B"}:
            text = text[:-1]
            mult = {"K": 1_000.0, "M": 1_000_000.0, "B": 1_000_000_000.0}[suffix]
        return float(text) * mult
    except Exception:
        return None


def _snapshot_has_price(raw: dict | None) -> bool:
    if not isinstance(raw, dict):
        return False
    return any(_num(raw.get(field)) is not None for field in ("31", "84", "86"))


def _merge_snapshot_rows(*batches: list[dict]) -> dict[int, dict]:
    merged: dict[int, dict] = {}
    for batch in batches:
        for row in batch or []:
            try:
                conid = int(row.get("conid"))
            except Exception:
                continue
            current = merged.get(conid)
            if current is None or (_snapshot_has_price(row) and not _snapshot_has_price(current)):
                merged[conid] = row
            elif current is not None:
                merged[conid] = {**current, **{k: v for k, v in row.items() if v not in (None, "", "N/A")}}
    return merged


def _snapshot_record(pair: str, raw: dict, inverted: bool = False) -> dict:
    bid = _num(raw.get("84"))
    ask = _num(raw.get("86"))
    last = _num(raw.get("31"))
    bid_size = _num(raw.get("88"))
    ask_size = _num(raw.get("85"))

    if inverted:
        inv_bid = (1.0 / ask) if ask and ask > 0 else None
        inv_ask = (1.0 / bid) if bid and bid > 0 else None
        inv_last = (1.0 / last) if last and last > 0 else None
        bid, ask, last = inv_bid, inv_ask, inv_last

    mid = (bid + ask) / 2.0 if bid and ask and bid > 0 and ask > 0 else last
    spread = (ask - bid) if bid and ask and ask >= bid else None
    spread_pct = spread / mid if spread is not None and mid and mid > 0 else None
    return {
        "pair": pair,
        "conid": raw.get("conid"),
        "bid": bid,
        "ask": ask,
        "last": last,
        "mid": mid,
        "bid_size": bid_size,
        "ask_size": ask_size,
        "spread": spread,
        "spread_pct": spread_pct,
        "updated_ms": raw.get("_updated"),
        "market_data_availability": raw.get("6509"),
        "source": "ibkr_cpapi_snapshot_inverted" if inverted else "ibkr_cpapi_snapshot",
        "raw": raw,
    }


@app.get("/marketdata/fx/snapshot")
async def fx_marketdata_snapshot(
    pairs: str = Query(..., description="Comma-separated FX pairs, e.g. EURUSD,USDJPY"),
    fields: str = Query("31,84,85,86,88,7059,6509"),
) -> dict[str, Any]:
    """Retourne bid/ask/mid/spread FX depuis IBKR Client Portal API."""
    client = get_client()
    requested = [p.strip().upper().replace("/", "") for p in pairs.split(",") if p.strip()]
    conid_to_pairs: dict[int, list[tuple[str, bool]]] = {}
    errors = []

    for pair in requested:
        conid = get_fx_conid(pair)
        inverted = False
        if conid is None and len(pair) == 6:
            reverse = pair[3:] + pair[:3]
            conid = get_fx_conid(reverse)
            inverted = conid is not None
        if conid is None:
            errors.append({"pair": pair, "error": "UNKNOWN_FX_CONID"})
            continue
        conid_to_pairs.setdefault(int(conid), []).append((pair, inverted))

    if not conid_to_pairs:
        return {"quotes": [], "errors": errors, "dry_run": DRY_RUN}

    conids = sorted(conid_to_pairs)
    try:
        # CPAPI sometimes requires account initialization before market data,
        # even though snapshots are account-agnostic for our read-only use case.
        try:
            await client.get_account_id()
        except Exception as exc:
            logger.info("IBKR account preflight before snapshot did not complete: %s", exc)

        # CPAPI needs a pre-flight snapshot request to start streams. A short
        # second pass usually returns the requested fields within the same run.
        # Always do the second pass: one liquid pair can answer immediately while
        # other conids are still warming up.
        first = await client.marketdata_snapshot(conids, fields=fields)
        await asyncio.sleep(0.8)
        second = await client.marketdata_snapshot(conids, fields=fields)
        merged_initial = _merge_snapshot_rows(first, second)
        missing = [conid for conid in conids if not _snapshot_has_price(merged_initial.get(conid))]
        third = []
        if missing:
            await asyncio.sleep(0.8)
            third = await client.marketdata_snapshot(missing, fields=fields)
    except CPAPIError as exc:
        raise HTTPException(502, str(exc)) from exc

    by_conid = _merge_snapshot_rows(first, second, third)

    quotes = []
    for conid, pair_specs in conid_to_pairs.items():
        raw = by_conid.get(conid)
        if not raw:
            for pair, _ in pair_specs:
                errors.append({"pair": pair, "conid": conid, "error": "NO_SNAPSHOT_ROW"})
            continue
        for pair, inverted in pair_specs:
            quotes.append(_snapshot_record(pair, raw, inverted=inverted))

    return {
        "quotes": quotes,
        "errors": errors,
        "fields": fields,
        "dry_run": DRY_RUN,
        "known_fx_conids": len(FX_CONIDS),
        "fetched_at": now_iso(),
    }


# ─── FX Orders ────────────────────────────────────────────────────────────────

@app.post("/orders/fx")
async def place_fx_orders(req: FXOrdersRequest) -> dict[str, Any]:
    """
    Envoie des ordres FX vers IBKR IDEALPRO.

    En dry_run=true : logue et retourne des résultats fictifs.
    En dry_run=false : envoie réellement via CPAPI.
    """
    if not FX_ORDERS_ENABLED:
        raise HTTPException(403, "FX order endpoint disabled by IBKR_FX_ORDERS_ENABLED=false")

    results = []
    errors = []

    for order in req.orders:
        pair = order.pair.upper().replace("/", "")
        conid = get_fx_conid(pair)

        if conid is None:
            errors.append({"order_id": order.order_id, "error": f"Unknown FX pair: {pair}"})
            continue

        meta = FX_META[pair]
        ibkr_side = fx_ibkr_side(order.side)
        quantity = round(order.size_lots * 100_000)  # lots → unités base

        ibkr_payload = {
            "conid": conid,
            "orderType": normalize_order_type(order.order_type),
            "side": ibkr_side,
            "quantity": quantity,
            "tif": "DAY",
            "cOID": order.client_order_id or order.order_id,
            # Target AG1-FX orders are speculative spot-FX trades. Prefunding
            # legs are explicit cash conversions so they do not create a
            # leveraged non-base-currency borrow before the target order.
            "isCcyConv": bool(order.is_currency_conversion),
        }
        if ibkr_payload["orderType"] == "LMT" and order.limit_price:
            ibkr_payload["price"] = order.limit_price

        logger.info(
            "FX order | run=%s | pair=%s | side=%s | qty=%s lots | dry=%s",
            req.run_id, pair, ibkr_side, order.size_lots, DRY_RUN,
        )

        if DRY_RUN:
            results.append(_dry_run_result(order.order_id, ibkr_payload))
            continue

        client = get_client()
        try:
            ibkr_resp = await client.place_orders([ibkr_payload])
            client_order_id = order.client_order_id or order.order_id
            if _reply_required_items(ibkr_resp):
                errors.append(_reply_required_error(order.order_id, client_order_id, ibkr_resp))
                continue
            if _ibkr_error_messages(ibkr_resp):
                errors.append(_ibkr_order_error(order.order_id, client_order_id, ibkr_resp))
                continue
            results.append({
                "order_id": order.order_id,
                "client_order_id": client_order_id,
                "status": "submitted",
                "ibkr_response": ibkr_resp,
                "sent_at": now_iso(),
            })
        except CPAPIError as exc:
            logger.error("FX order failed pair=%s: %s", pair, exc)
            errors.append({"order_id": order.order_id, "error": str(exc)})

    return {"results": results, "errors": errors, "dry_run": DRY_RUN}


# ─── Equity Orders ────────────────────────────────────────────────────────────

@app.post("/orders/equity")
async def place_equity_orders(req: EquityOrdersRequest) -> dict[str, Any]:
    """
    Envoie des ordres actions/ETF vers IBKR (SMART routing).

    En dry_run=true : logue et retourne des résultats fictifs.
    En dry_run=false : résout le conid et envoie via CPAPI.
    """
    client = get_client()
    results = []
    errors = []

    for order in req.orders:
        symbol = order.symbol.strip()
        ibkr_side = stk_ibkr_side(order.side)

        try:
            if DRY_RUN:
                conid = 0  # pas besoin en dry-run
            else:
                conid = await _resolve_stk_conid(client, symbol, order.isin, order.exchange)
        except HTTPException as exc:
            errors.append({
                "order_id": order.order_id,
                "client_order_id": order.client_order_id or order.order_id,
                "error": exc.detail,
            })
            continue
        except CPAPIError as exc:
            logger.error("Equity contract resolution failed symbol=%s: %s", symbol, exc)
            errors.append({
                "order_id": order.order_id,
                "client_order_id": order.client_order_id or order.order_id,
                "error": str(exc),
            })
            continue

        if not DRY_RUN and ibkr_side == "SELL":
            try:
                held_qty = _position_qty_by_conid(await client.get_portfolio_positions(), conid)
            except CPAPIError as exc:
                logger.error("Equity position preflight failed symbol=%s: %s", symbol, exc)
                errors.append({
                    "order_id": order.order_id,
                    "client_order_id": order.client_order_id or order.order_id,
                    "error": f"IBKR_POSITION_PREFLIGHT_FAILED: {exc}",
                })
                continue
            if held_qty < float(order.quantity):
                errors.append({
                    "order_id": order.order_id,
                    "client_order_id": order.client_order_id or order.order_id,
                    "error": (
                        "IBKR_SELL_REJECTED_INSUFFICIENT_POSITION:"
                        f"{symbol}:held={held_qty}:sell={order.quantity}"
                    ),
                })
                continue

        ibkr_payload = {
            "conid": conid,
            "orderType": normalize_order_type(order.order_type),
            "side": ibkr_side,
            "quantity": order.quantity,
            "tif": "DAY",
            "cOID": order.client_order_id or order.order_id,
        }
        if ibkr_payload["orderType"] == "LMT" and order.limit_price:
            ibkr_payload["price"] = order.limit_price

        logger.info(
            "Equity order | run=%s | symbol=%s | side=%s | qty=%s | dry=%s",
            req.run_id, symbol, ibkr_side, order.quantity, DRY_RUN,
        )

        if DRY_RUN:
            results.append(_dry_run_result(order.order_id, ibkr_payload))
            continue

        try:
            ibkr_resp = await client.place_orders([ibkr_payload])
            client_order_id = order.client_order_id or order.order_id
            if _reply_required_items(ibkr_resp):
                confirmation = await _confirm_price_prompt_chain(client, order, ibkr_payload, ibkr_resp)
                terminal_response = confirmation.get("terminal_response")
                if confirmation.get("ok"):
                    if _ibkr_error_messages(terminal_response):
                        errors.append(_ibkr_order_error(
                            order.order_id,
                            client_order_id,
                            terminal_response,
                            extra={
                                "confirmation_chain": confirmation,
                                "initial_ibkr_response": ibkr_resp,
                            },
                        ))
                        continue
                    results.append({
                        "order_id": order.order_id,
                        "client_order_id": client_order_id,
                        "status": "submitted_after_confirmation",
                        "ibkr_response": terminal_response,
                        "initial_ibkr_response": ibkr_resp,
                        "confirmation_chain": confirmation,
                        "sent_at": now_iso(),
                    })
                    continue

                if confirmation.get("reason") == "IBKR_ERROR_AFTER_CONFIRMATION":
                    errors.append(_ibkr_order_error(
                        order.order_id,
                        client_order_id,
                        terminal_response,
                        extra={
                            "confirmation_chain": confirmation,
                            "initial_ibkr_response": ibkr_resp,
                        },
                    ))
                    continue

                _appr_result = await approval.maybe_park_for_approval(
                    confirmation=confirmation,
                    order=order,
                    client_order_id=client_order_id,
                    ibkr_payload=ibkr_payload,
                    run_id=req.run_id,
                )
                if _appr_result is not None:
                    results.append(_appr_result)
                    continue
                errors.append(_reply_required_error(
                    order.order_id,
                    client_order_id,
                    terminal_response,
                    extra={
                        "confirmation_chain": confirmation,
                        "initial_ibkr_response": ibkr_resp,
                    },
                ))
                continue
            if _ibkr_error_messages(ibkr_resp):
                errors.append(_ibkr_order_error(order.order_id, client_order_id, ibkr_resp))
                continue
            results.append({
                "order_id": order.order_id,
                "client_order_id": client_order_id,
                "status": "submitted",
                "ibkr_response": ibkr_resp,
                "sent_at": now_iso(),
            })
        except CPAPIError as exc:
            logger.error("Equity order failed symbol=%s: %s", symbol, exc)
            errors.append({"order_id": order.order_id, "error": str(exc)})

    return {"results": results, "errors": errors, "dry_run": DRY_RUN}


# ─── Market Data Historique (pour macro-data-api) ─────────────────────────────

@app.get("/marketdata/history")
async def get_market_data_history(
    conid: int,
    period: str = "1y",
    bar: str = "1d",
    outside_rth: bool = True,
) -> dict:
    """
    Récupère l'historique de prix IBKR pour un contrat (FX, bonds, ETF).
    Utilisé par macro-data-api pour obtenir les yields souverains et données FX IBKR.

    Args:
        conid:       Contract ID IBKR
        period:      Période ("1y", "6m", "3m")
        bar:         Taille barre ("1d", "1h")
        outside_rth: Inclure données hors heures régulières
    """
    client = get_client()
    try:
        data = await client.get_market_data_history(conid, period, bar, outside_rth)
        return {
            "conid": conid,
            "period": period,
            "bar": bar,
            "count": len(data),
            "data": data,
        }
    except CPAPIError as exc:
        logger.warning("market_data_history conid=%s failed: %s", conid, exc)
        # Retourne résultat vide pour ne pas bloquer macro-data-api
        return {"conid": conid, "period": period, "bar": bar, "count": 0, "data": [], "error": str(exc)}


@app.get("/marketdata/snapshot")
async def get_market_data_snapshot(
    conids: str,
    fields: str = "31,7741,55",
) -> list[dict]:
    """
    Snapshot de marché pour plusieurs contrats (yields temps réel, prix).

    Args:
        conids: IDs séparés par virgule (ex: "8297,258")
        fields: Champs IBKR (31=prix, 7741=yield, 55=symbole)
    """
    client = get_client()
    try:
        conid_list = [int(c.strip()) for c in conids.split(",") if c.strip()]
        field_list = [f.strip() for f in fields.split(",") if f.strip()]
        return await client.get_market_data_snapshot(conid_list, field_list)
    except CPAPIError as exc:
        raise HTTPException(502, str(exc)) from exc


# --- Approbation humaine des ordres hors-bande (flag-gated) -------------------

@app.get("/orders/approvals/pending")
async def approvals_pending() -> dict[str, Any]:
    return {
        "enabled": approval.is_enabled(),
        "config": approval.config(),
        "pending": await approval.list_pending(),
    }


@app.post("/orders/approvals/{order_id}/reject")
async def approvals_reject(order_id: str, body: dict = ApprovalBody(default={})) -> dict[str, Any]:
    token = str((body or {}).get("token") or "")
    entry, err = await approval.get_for_decision(order_id, token)
    if err:
        raise HTTPException(status_code=409, detail="APPROVAL_" + err)
    await approval.mark(order_id, "REJECTED")
    return {"order_id": order_id, "status": "REJECTED"}


@app.post("/orders/approvals/{order_id}/approve")
async def approvals_approve(order_id: str, body: dict = ApprovalBody(default={})) -> dict[str, Any]:
    token = str((body or {}).get("token") or "")
    entry, err = await approval.get_for_decision(order_id, token)
    if err:
        raise HTTPException(status_code=409, detail="APPROVAL_" + err)
    client = get_client()
    ibkr_payload = dict(entry["ibkr_payload"])
    side = str(ibkr_payload.get("side") or "BUY")
    symbol = str(entry.get("symbol") or "")
    try:
        quote = await _fetch_equity_quote(symbol, side, float(ibkr_payload.get("quantity") or 0))
        ref_price, _ref_field = _quote_reference_price(quote, side)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail="APPROVAL_QUOTE_FAILED:" + str(exc))
    limit_price = _num(ibkr_payload.get("price"))
    if ref_price and limit_price:
        dev = abs(limit_price - ref_price) / ref_price * 100.0
        if dev > approval.MAX_DEVIATION_PCT:
            await approval.mark(order_id, "REJECTED")
            raise HTTPException(status_code=409, detail="APPROVAL_PRICE_MOVED:%.2fpct" % dev)
    confirmation = entry.get("confirmation") if isinstance(entry.get("confirmation"), dict) else {}
    terminal = confirmation.get("terminal_response")
    if _reply_required_items(terminal):
        next_response: list[dict] = []
        for item in _reply_required_items(terminal):
            next_response.extend(await client.reply_order(str(item["id"]), confirmed=True))
        terminal = next_response
    else:
        terminal = await client.place_orders([ibkr_payload])
    steps = 0
    while _reply_required_items(terminal) and steps < AUTO_CONFIRM_MAX_STEPS:
        steps += 1
        nxt2: list[dict] = []
        for item in _reply_required_items(terminal):
            nxt2.extend(await client.reply_order(str(item["id"]), confirmed=True))
        terminal = nxt2
    if _reply_required_items(terminal) or _ibkr_error_messages(terminal):
        await approval.mark(order_id, "FAILED")
        return {"order_id": order_id, "status": "FAILED", "ibkr_response": terminal}
    await approval.mark(order_id, "FILLED")
    return {"order_id": order_id, "status": "APPROVED_SUBMITTED", "ibkr_response": terminal, "sent_at": now_iso()}
