"""
ibkr-broker — microservice FastAPI passerelle vers IBKR Client Portal API.

Variables d'environnement :
  IBKR_GATEWAY_URL   URL interne du clientportal.gw  (défaut: https://ibkr-gateway:5000)
  IBKR_DRY_RUN       "true" → log sans envoyer        (défaut: "true")
  IBKR_SSL_VERIFY    "false" → ignore cert auto-signé  (défaut: "false")
  IBKR_ACCOUNT_ID    ID compte IBKR (optionnel, auto-détecté sinon)

Endpoints exposés à n8n :
  GET  /health                  → statut session IBKR
  POST /orders/fx               → envoyer ordres FX
  POST /orders/equity           → envoyer ordres actions/ETF
  GET  /fills                   → fills récents
  GET  /positions               → positions actuelles
  POST /auth/tickle             → keepalive manuel
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from contract_cache import (
    FX_META,
    fx_ibkr_side,
    get_fx_conid,
    get_stk_conid,
    parse_stk_symbol,
    store_stk_conid,
    stk_ibkr_side,
    yahoo_suffix_to_ibkr_exchange,
)
from cpapi_client import CPAPIClient, CPAPIError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("ibkr_broker")

# ─── Config ──────────────────────────────────────────────────────────────────
GATEWAY_URL = os.environ.get("IBKR_GATEWAY_URL", "https://ibkr-gateway:5000")
DRY_RUN = os.environ.get("IBKR_DRY_RUN", "true").lower() != "false"
SSL_VERIFY = os.environ.get("IBKR_SSL_VERIFY", "false").lower() == "true"
ACCOUNT_ID_OVERRIDE = os.environ.get("IBKR_ACCOUNT_ID", "")

# ─── Global client ───────────────────────────────────────────────────────────
_cpapi: CPAPIClient | None = None


def get_client() -> CPAPIClient:
    if _cpapi is None:
        raise HTTPException(503, "CPAPI client not initialized")
    return _cpapi


# ─── Background keepalive ─────────────────────────────────────────────────────
async def _keepalive_loop():
    """Envoie un tickle IBKR toutes les 55 secondes pour maintenir la session."""
    while True:
        await asyncio.sleep(55)
        try:
            client = get_client()
            await client.tickle()
            logger.debug("Tickle OK")
        except Exception as exc:
            logger.warning("Tickle failed: %s", exc)


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


class EquityOrder(BaseModel):
    symbol: str          # ex: "MC.PA"
    side: str            # BUY | SELL
    quantity: float
    order_id: str        # ID interne Trader_IA
    client_order_id: str | None = None  # devient cOID pour idempotence IBKR
    order_type: str = "MKT"
    limit_price: float | None = None


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
    if text == "MARKET":
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


async def _resolve_stk_conid(client: CPAPIClient, symbol: str) -> int:
    """
    Résout le conid IBKR d'un symbole action.
    Utilise le cache en mémoire, sinon appel CPAPI secdef search.
    """
    cached = get_stk_conid(symbol)
    if cached:
        return cached

    ticker, suffix = parse_stk_symbol(symbol)
    exchange = yahoo_suffix_to_ibkr_exchange(suffix)

    contracts = await client.search_contract(ticker, sec_type="STK")
    if not contracts:
        raise HTTPException(404, f"No IBKR contract found for {symbol}")

    # Cherche l'exchange correspondant, sinon prend le premier
    best = None
    for c in contracts:
        sections = c.get("sections") or []
        for sec in sections:
            if sec.get("exchange", "").upper() == exchange.upper():
                best = {"conid": int(c["conid"]), "exchange": exchange}
                break
        if best:
            break

    if not best:
        # Prend le premier résultat (SMART routing)
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
        authenticated = bool(status.get("authenticated") and status.get("connected"))
        return {
            "dry_run": DRY_RUN,
            "gateway_url": GATEWAY_URL,
            "authenticated": authenticated,
            "ibkr_status": status,
        }
    except Exception as exc:
        return {
            "dry_run": DRY_RUN,
            "gateway_url": GATEWAY_URL,
            "authenticated": False,
            "error": str(exc),
        }


@app.post("/auth/tickle")
async def manual_tickle() -> dict:
    """Keepalive manuel de la session IBKR."""
    client = get_client()
    try:
        r = await client.tickle()
        return {"ok": True, "response": r}
    except CPAPIError as exc:
        raise HTTPException(502, f"Tickle failed: {exc}") from exc


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


# ─── FX Orders ────────────────────────────────────────────────────────────────

@app.post("/orders/fx")
async def place_fx_orders(req: FXOrdersRequest) -> dict[str, Any]:
    """
    Envoie des ordres FX vers IBKR IDEALPRO.

    En dry_run=true : logue et retourne des résultats fictifs.
    En dry_run=false : envoie réellement via CPAPI.
    """
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
            results.append({
                "order_id": order.order_id,
                "client_order_id": order.client_order_id or order.order_id,
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
                conid = await _resolve_stk_conid(client, symbol)
        except HTTPException as exc:
            errors.append({"order_id": order.order_id, "error": exc.detail})
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
            results.append({
                "order_id": order.order_id,
                "client_order_id": order.client_order_id or order.order_id,
                "status": "submitted",
                "ibkr_response": ibkr_resp,
                "sent_at": now_iso(),
            })
        except CPAPIError as exc:
            logger.error("Equity order failed symbol=%s: %s", symbol, exc)
            errors.append({"order_id": order.order_id, "error": str(exc)})

    return {"results": results, "errors": errors, "dry_run": DRY_RUN}
