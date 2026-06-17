"""Human-in-the-loop approval for out-of-band / unverifiable IBKR equity orders.

Flag-gated par IBKR_APPROVAL_ENABLED (defaut false). v2 (2026-06-16) : parque aussi
les cas "prix non verifiable" (QUOTE_TOO_OLD / NO_REFERENCE_PRICE / QUOTE_FETCH_FAILED).
v3 (2026-06-17) : parque le prompt IBKR explicite "without market data" apres garde
prix valide, sans ouvrir l'approbation aux autres confirmations non-prix.
"""
from __future__ import annotations

import asyncio
import logging
import os
import secrets
import time
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

logger = logging.getLogger("ibkr-broker.approval")


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return int(default)


ENABLED = _env_bool("IBKR_APPROVAL_ENABLED", False)
MAX_DEVIATION_PCT = _env_float("IBKR_APPROVAL_MAX_DEVIATION_PCT", 15.0)
TTL_SECONDS = _env_int("IBKR_APPROVAL_TTL_SECONDS", 600)
NOTIFY_WEBHOOK_URL = (os.environ.get("IBKR_APPROVAL_NOTIFY_WEBHOOK_URL") or "").strip()
REPRICE_ON_APPROVE = _env_bool("IBKR_APPROVAL_REPRICE_ON_APPROVE", True)
AUTO_BAND_PCT = _env_float("IBKR_PRICE_GUARD_MAX_DEVIATION_PCT", 5.0)

# Raisons "prix non verifiable" : on parque pour validation humaine plutot que rejeter.
UNVERIFIABLE_REASONS = {"QUOTE_TOO_OLD", "NO_REFERENCE_PRICE", "QUOTE_FETCH_FAILED"}
PROMPT_APPROVAL_REASONS = {"IBKR_PROMPT_WITHOUT_MARKET_DATA"}

_pending: dict[str, dict] = {}
_lock = asyncio.Lock()


def is_enabled() -> bool:
    return ENABLED


def config() -> dict:
    return {
        "enabled": ENABLED,
        "auto_band_pct": AUTO_BAND_PCT,
        "approval_max_deviation_pct": MAX_DEVIATION_PCT,
        "ttl_seconds": TTL_SECONDS,
        "notify_webhook_configured": bool(NOTIFY_WEBHOOK_URL),
        "reprice_on_approve": REPRICE_ON_APPROVE,
        "unverifiable_reasons": sorted(UNVERIFIABLE_REASONS),
        "prompt_approval_reasons": sorted(PROMPT_APPROVAL_REASONS),
    }


def _last_guard(confirmation: dict) -> Optional[dict]:
    attempts = (confirmation or {}).get("attempts") or []
    if attempts:
        return attempts[-1].get("guard") or {}
    return None


def _priced_guard(confirmation: dict) -> Optional[dict]:
    """Retourne le dernier guard avec reference/deviation utile dans la chaine."""
    attempts = (confirmation or {}).get("attempts") or []
    for attempt in reversed(attempts):
        guard = attempt.get("guard") or {}
        if guard.get("reference_price") is not None or guard.get("deviation_pct") is not None:
            return guard
    return None


def _prompt_messages(confirmation: dict, guard: dict) -> list[str]:
    messages = guard.get("prompt_messages")
    if isinstance(messages, list):
        return [str(m) for m in messages if str(m).strip()]
    attempts = (confirmation or {}).get("attempts") or []
    if attempts:
        raw = attempts[-1].get("prompt_messages") or []
        if isinstance(raw, list):
            return [str(m) for m in raw if str(m).strip()]
    return []


def _is_without_market_data_prompt(messages: list[str]) -> bool:
    joined = " | ".join(messages).lower()
    if "without market data" not in joined:
        return False
    # Ne jamais recycler ce chemin pour des confirmations de marge/short/restriction.
    danger_markers = (
        "margin",
        "insufficient",
        "short sale",
        "shortable",
        "locate",
        "restricted",
        "not allowed",
    )
    return not any(marker in joined for marker in danger_markers)


def _verification_reason(confirmation: dict, guard: dict) -> Optional[str]:
    reason = guard.get("reason")
    if reason == "PROMPT_NOT_PRICE_CONFIRMATION" and _is_without_market_data_prompt(
        _prompt_messages(confirmation, guard)
    ):
        return "IBKR_PROMPT_WITHOUT_MARKET_DATA"
    return str(reason) if reason else None


def _eligible(confirmation: dict, guard: dict) -> bool:
    verification = _verification_reason(confirmation, guard)
    if verification in PROMPT_APPROVAL_REASONS:
        priced_guard = _priced_guard(confirmation) or {}
        dev = priced_guard.get("deviation_pct")
        return dev is not None and float(dev) <= AUTO_BAND_PCT
    reason = str(guard.get("reason") or "")
    if reason in UNVERIFIABLE_REASONS or any(reason.startswith(r + ":") for r in UNVERIFIABLE_REASONS):
        return True
    dev = guard.get("deviation_pct")
    if dev is None:
        return False
    return AUTO_BAND_PCT < float(dev) <= MAX_DEVIATION_PCT


def _public(e: dict) -> dict:
    keys = (
        "order_id", "client_order_id", "run_id", "symbol", "side", "qty",
        "limit_price", "reference_price", "deviation_pct", "verification",
        "status", "created_at", "expires_at",
    )
    return {k: e.get(k) for k in keys}


async def _notify(entry: dict) -> None:
    if not NOTIFY_WEBHOOK_URL:
        logger.warning(
            "Approval needed but IBKR_APPROVAL_NOTIFY_WEBHOOK_URL empty; order_id=%s",
            entry.get("order_id"),
        )
        return
    payload = {
        k: entry.get(k)
        for k in (
            "order_id", "client_order_id", "run_id", "symbol", "side", "qty",
            "limit_price", "reference_price", "deviation_pct", "verification",
            "expires_at", "token",
        )
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(NOTIFY_WEBHOOK_URL, json=payload)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Approval notify failed order_id=%s: %s", entry.get("order_id"), exc)


async def maybe_park_for_approval(
    *,
    confirmation: dict,
    order: Any,
    client_order_id: str,
    ibkr_payload: dict,
    run_id: Any,
) -> Optional[dict]:
    """Parque l'ordre + notifie si eligible. Sinon None (le caller continue son rejet normal)."""
    if not ENABLED:
        return None
    if not confirmation or confirmation.get("reason") != "CONFIRMATION_GUARD_REJECTED":
        return None
    guard = _last_guard(confirmation) or {}
    if not _eligible(confirmation, guard):
        return None

    priced_guard = _priced_guard(confirmation) or guard
    verification = _verification_reason(confirmation, guard) or guard.get("reason")
    order_id = str(getattr(order, "order_id", None) or client_order_id)
    ref = priced_guard.get("reference_price")
    lim = priced_guard.get("limit_price")
    dev = priced_guard.get("deviation_pct")
    if dev is None and ref and lim:
        try:
            dev = abs(float(lim) - float(ref)) / float(ref) * 100.0
        except Exception:  # noqa: BLE001
            dev = None
    now = time.time()
    entry = {
        "order_id": order_id,
        "client_order_id": client_order_id,
        "run_id": run_id,
        "symbol": getattr(order, "symbol", None),
        "side": ibkr_payload.get("side"),
        "qty": ibkr_payload.get("quantity"),
        "limit_price": lim,
        "reference_price": ref,
        "deviation_pct": dev,
        "verification": verification,
        "prompt_messages": _prompt_messages(confirmation, guard),
        "ibkr_payload": dict(ibkr_payload),
        "status": "PENDING",
        "token": secrets.token_urlsafe(16),
        "created_at": now,
        "expires_at": now + TTL_SECONDS,
    }
    async with _lock:
        _pending[order_id] = entry
    await _notify(entry)
    logger.info(
        "Order parked for approval order_id=%s symbol=%s reason=%s dev=%s ttl=%ss",
        order_id, entry["symbol"], entry["verification"], entry["deviation_pct"], TTL_SECONDS,
    )
    return {
        "order_id": order_id,
        "client_order_id": client_order_id,
        "status": "needs_approval",
        "approval": {
            k: entry[k]
            for k in ("symbol", "side", "qty", "limit_price", "reference_price", "deviation_pct", "verification", "expires_at")
        },
        "sent_at": datetime.now(timezone.utc).isoformat(),
    }


async def list_pending() -> list[dict]:
    async with _lock:
        return [_public(e) for e in _pending.values()]


async def get_for_decision(order_id: str, token: str) -> tuple[Optional[dict], Optional[str]]:
    async with _lock:
        e = _pending.get(order_id)
        if not e:
            return None, "NOT_FOUND"
        if e.get("token") != token:
            return None, "BAD_TOKEN"
        if e.get("status") != "PENDING":
            return None, "ALREADY_" + str(e.get("status"))
        if time.time() > e.get("expires_at", 0):
            e["status"] = "EXPIRED"
            return None, "EXPIRED"
        return dict(e), None


async def mark(order_id: str, status: str) -> Optional[dict]:
    async with _lock:
        e = _pending.get(order_id)
        if e:
            e["status"] = status
            e["decided_at"] = time.time()
        return e


async def sweep_expired() -> list[str]:
    now = time.time()
    expired: list[str] = []
    async with _lock:
        for e in _pending.values():
            if e.get("status") == "PENDING" and now > e.get("expires_at", 0):
                e["status"] = "EXPIRED"
                expired.append(e["order_id"])
    return expired
