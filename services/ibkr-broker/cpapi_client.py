"""
IBKR Client Portal API (CPAPI) – client bas niveau.

Documente les appels HTTP au gateway clientportal.gw qui tourne sur le réseau
Docker interne. Le gateway gère l'authentification IBKR ; ce client suppose que
la session est déjà ouverte (vérifiée via /auth/status).
"""

import asyncio
import logging
import time
from typing import Any

import httpx

logger = logging.getLogger("ibkr_cpapi")


class CPAPIError(Exception):
    def __init__(self, status: int, body: str):
        self.status = status
        self.body = body
        super().__init__(f"CPAPI HTTP {status}: {body[:300]}")


class CPAPIClient:
    """
    Wrapper httpx autour du IBKR Client Portal API Gateway.

    Le gateway est supposé tourner sur IBKR_GATEWAY_URL (ex : http://ibkr-gateway:5000).
    SSL : si le gateway utilise son certificat auto-signé, on désactive la vérification.
    """

    def __init__(self, base_url: str, ssl_verify: bool = False, timeout: float = 15.0):
        self.base_url = base_url.rstrip("/")
        # On utilise verify=False car le gateway CPAPI expose un cert auto-signé par défaut.
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            verify=ssl_verify,
            timeout=timeout,
        )
        self._account_id: str | None = None
        self._last_tickle: float = 0.0

    # ──────────────────────────────────────────────────────────────────────────
    # Auth / session
    # ──────────────────────────────────────────────────────────────────────────

    async def auth_status(self) -> dict:
        """Retourne le statut d'authentification IBKR."""
        r = await self._get("/v1/api/iserver/auth/status")
        return r

    async def tickle(self) -> dict:
        """Keepalive de session. À appeler toutes les ~60 s."""
        r = await self._post("/v1/api/tickle", {})
        self._last_tickle = time.monotonic()
        return r

    async def initialize_brokerage_session(
        self,
        publish: bool = True,
        compete: bool = False,
    ) -> dict:
        """
        Reinitialise la session brokerage si la session Gateway/SSO est encore valide.

        IBKR recommande cet endpoint a la place de /iserver/reauthenticate.
        Il ne remplace pas le login navigateur + 2FA quand la session Gateway a
        totalement expire.
        """
        return await self._post(
            "/v1/api/iserver/auth/ssodh/init",
            {"publish": publish, "compete": compete},
        )

    async def logout(self) -> dict:
        """Deconnecte la session Gateway courante."""
        return await self._post("/v1/api/logout", {})

    async def is_authenticated(self) -> bool:
        try:
            status = await self.auth_status()
            return bool(status.get("authenticated") and status.get("connected"))
        except Exception as exc:
            logger.warning("auth_status failed: %s", exc)
            return False

    # ──────────────────────────────────────────────────────────────────────────
    # Compte
    # ──────────────────────────────────────────────────────────────────────────

    async def get_account_id(self) -> str:
        """Récupère le premier compte disponible et le met en cache."""
        if self._account_id:
            return self._account_id
        r = await self._get("/v1/api/iserver/accounts")
        accounts = r.get("accounts") or []
        if not accounts:
            raise CPAPIError(0, "No IBKR accounts found")
        self._account_id = accounts[0]
        logger.info("IBKR account: %s", self._account_id)
        return self._account_id

    async def get_portfolio_positions(self) -> list[dict]:
        account_id = await self.get_account_id()
        return await self._get(f"/v1/api/portfolio/{account_id}/positions/0")

    async def get_account_summary(self) -> dict:
        account_id = await self.get_account_id()
        return await self._get(f"/v1/api/portfolio/{account_id}/summary")

    async def get_account_ledger(self) -> dict:
        account_id = await self.get_account_id()
        return await self._get(f"/v1/api/portfolio/{account_id}/ledger")

    async def get_portfolio_news(self) -> Any:
        """News IBKR des positions detenues. Read-only (spike 2026-06-18).

        /iserver/news/portfolio -> {"news":[...]} headline/source/provider/sentiment.
        L'endpoint par-contrat (/iserver/news?conid=) n'est PAS servi (503).
        """
        return await self._get("/v1/api/iserver/news/portfolio")

    # ──────────────────────────────────────────────────────────────────────────
    # Résolution de contrats
    # ──────────────────────────────────────────────────────────────────────────

    async def search_contract(self, symbol: str, sec_type: str = "STK") -> list[dict]:
        """
        Cherche un contrat par symbole.
        sec_type : STK (action), CASH (FX), ETF, etc.
        """
        r = await self._get(
            "/v1/api/iserver/secdef/search",
            params={"symbol": symbol, "secType": sec_type},
        )
        return r if isinstance(r, list) else r.get("contracts", [])

    async def get_contract_info(self, conid: int) -> dict:
        return await self._get(f"/v1/api/iserver/contract/{conid}/info")

    async def marketdata_snapshot(self, conids: list[int], fields: str = "") -> list[dict]:
        params = {"conids": ",".join(str(c) for c in conids)}
        if fields:
            params["fields"] = fields
        r = await self._get("/v1/api/iserver/marketdata/snapshot", params=params)
        return r if isinstance(r, list) else [r]

    # ──────────────────────────────────────────────────────────────────────────
    # Ordres
    # ──────────────────────────────────────────────────────────────────────────

    async def place_orders(self, orders: list[dict]) -> list[dict]:
        """
        Envoie un ou plusieurs ordres.

        Format IBKR actuel : un tableau JSON de tickets d'ordre.
        Les champs minimum sont conid, orderType, side, tif et quantity.

        Retourne la liste des réponses IBKR (une par ordre).
        Les confirmations IBKR en 2 temps ne sont pas auto-acceptees ici.
        Un item avec "id" doit remonter au caller, qui decide explicitement si
        une confirmation encadree est admissible.
        """
        account_id = await self.get_account_id()
        response = await self._post(
            f"/v1/api/iserver/account/{account_id}/orders", {"orders": orders}
        )
        return response if isinstance(response, list) else [response]

    async def reply_order(self, reply_id: str, confirmed: bool = True) -> list[dict]:
        """
        Repond a un prompt de confirmation IBKR.

        Le caller doit avoir qualifie le prompt avant d'appeler cette methode.
        """
        response = await self._post(
            f"/v1/api/iserver/reply/{reply_id}",
            {"confirmed": bool(confirmed)},
        )
        return response if isinstance(response, list) else [response]

    async def get_recent_trades(self) -> list[dict]:
        """Retourne les fills récents (depuis dernier appel)."""
        r = await self._get("/v1/api/iserver/account/trades")
        return r if isinstance(r, list) else r.get("trades", [])

    # ──────────────────────────────────────────────────────────────────────────
    # Données de marché historiques
    # ──────────────────────────────────────────────────────────────────────────

    async def get_market_data_history(
        self,
        conid: int,
        period: str = "1y",
        bar: str = "1d",
        outside_rth: bool = True,
    ) -> list[dict]:
        """
        Récupère l'historique de prix/yields IBKR (HMDS endpoint).

        Args:
            conid:       Contract ID IBKR (ex: 8297 pour EUR/USD IDEALPRO)
            period:      Période totale ("1y", "6m", "3m", "1w")
            bar:         Taille des barres ("1d", "1h", "30min")
            outside_rth: Inclure les données hors heures de trading régulières

        Returns:
            Liste de dicts avec 't' (timestamp ms), 'o', 'h', 'l', 'c', 'v'
        """
        params = {
            "conid": conid,
            "period": period,
            "bar": bar,
            "outsideRth": str(outside_rth).lower(),
        }
        r = await self._get("/v1/api/hmds/history", params=params)
        if isinstance(r, dict):
            return r.get("data", [])
        return r if isinstance(r, list) else []

    async def get_market_data_snapshot(
        self,
        conids: list[int],
        fields: list[str] | None = None,
    ) -> list[dict]:
        """
        Snapshot de marché pour une liste de conids (prix, yields, etc.).

        fields communs: 31=last price, 7741=yield (bonds), 55=symbol
        """
        fields_str = ",".join(fields or ["31", "7741", "55"])
        conids_str = ",".join(str(c) for c in conids)
        params = {"conids": conids_str, "fields": fields_str}
        r = await self._get("/v1/api/iserver/marketdata/snapshot", params=params)
        return r if isinstance(r, list) else []

    async def unsubscribe_market_data(self, conid: int) -> dict:
        """Désabonne du flux de données pour libérer les ressources."""
        try:
            return await self._get(f"/v1/api/iserver/marketdata/{conid}/unsubscribe")
        except Exception:
            return {}

    async def cancel_order(self, order_id: str) -> dict:
        account_id = await self.get_account_id()
        return await self._delete(
            f"/v1/api/iserver/account/{account_id}/order/{order_id}"
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Helpers HTTP internes
    # ──────────────────────────────────────────────────────────────────────────

    async def _get(self, path: str, params: dict | None = None) -> Any:
        r = await self._client.get(path, params=params)
        return self._parse(r)

    async def _post(self, path: str, body: Any) -> Any:
        r = await self._client.post(path, json=body)
        return self._parse(r)

    async def _delete(self, path: str) -> Any:
        r = await self._client.delete(path)
        return self._parse(r)

    @staticmethod
    def _parse(r: httpx.Response) -> Any:
        if r.status_code >= 400:
            raise CPAPIError(r.status_code, r.text)
        try:
            return r.json()
        except Exception:
            return {"raw": r.text}

    async def close(self):
        await self._client.aclose()
