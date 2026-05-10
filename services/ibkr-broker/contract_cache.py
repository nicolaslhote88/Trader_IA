"""
Résolution et cache des Contract IDs (conids) IBKR.

FX  : symboles de la forme EURUSD → conid sur IDEALPRO.
STK : symboles de la forme MC.PA, AIR.PA, etc. → conid sur l'exchange primaire.

La table FX_CONIDS contient les conids stables IBKR pour les 27 paires du projet.
Les conids actions sont résolus dynamiquement via CPAPI et mis en cache en mémoire.
"""

import logging
from typing import Optional

logger = logging.getLogger("contract_cache")

# ---------------------------------------------------------------------------
# Conids FX stables (IDEALPRO, vérifié 2025-2026)
# Format clé : XXXYYY sans slash
# ---------------------------------------------------------------------------
FX_CONIDS: dict[str, int] = {
    # Majors
    "EURUSD": 12087792,
    "GBPUSD": 12087797,
    "USDJPY": 12087798,
    "USDCHF": 12087800,
    "AUDUSD": 14433401,
    "USDCAD": 12087799,
    "NZDUSD": 14433402,
    # EUR crosses
    "EURGBP": 12087793,
    "EURJPY": 12087794,
    "EURCHF": 12087796,
    "EURAUD": 14433404,
    "EURCAD": 14433405,
    "EURNZD": 14433406,
    # GBP crosses
    "GBPJPY": 12087801,
    "GBPCHF": 12087803,
    "GBPAUD": 14433407,
    "GBPCAD": 14433408,
    "GBPNZD": 14433409,
    # Commodity crosses
    "AUDJPY": 14433403,
    "AUDCHF": 14433410,
    "AUDCAD": 14433411,
    "AUDNZD": 14433412,
    "CADJPY": 14433413,
    "CHFJPY": 14433414,
    "NZDJPY": 14433415,
    "NZDCHF": 14433416,
    "NZDCAD": 14433417,
}

# FX : base + quote pour construction du payload IBKR
FX_META: dict[str, dict] = {
    pair: {"base": pair[:3], "quote": pair[3:], "conid": conid}
    for pair, conid in FX_CONIDS.items()
}


def get_fx_conid(pair: str) -> Optional[int]:
    """Retourne le conid d'une paire FX (ex: 'EURUSD') ou None si inconnue."""
    return FX_CONIDS.get(pair.upper().replace("/", "").replace("_", ""))


def fx_ibkr_side(our_side: str) -> str:
    """
    Convertit notre convention de side FX → side IBKR.

    Notre convention  →  IBKR
    buy_base          →  BUY   (achète la devise base)
    sell_base         →  SELL  (vend la devise base)
    close_long        →  SELL
    close_short       →  BUY
    """
    mapping = {
        "buy_base": "BUY",
        "close_short": "BUY",
        "sell_base": "SELL",
        "close_long": "SELL",
    }
    return mapping.get(our_side.lower(), "BUY")


# ---------------------------------------------------------------------------
# Cache STK dynamique (en mémoire, réinitialisé au redémarrage du service)
# ---------------------------------------------------------------------------
_STK_CACHE: dict[str, int] = {}


def get_stk_conid(symbol: str) -> Optional[int]:
    return _STK_CACHE.get(symbol.upper())


def store_stk_conid(symbol: str, conid: int):
    _STK_CACHE[symbol.upper()] = conid
    logger.info("Cached conid %d for %s", conid, symbol)


def stk_ibkr_side(our_side: str) -> str:
    """Convertit notre convention actions → side IBKR (BUY/SELL)."""
    return "BUY" if our_side.upper() in ("BUY", "OPEN", "INCREASE") else "SELL"


def parse_stk_symbol(symbol: str) -> tuple[str, str]:
    """
    Décompose un symbole Yahoo-style en (ticker, exchange_suffix).
    Ex: 'MC.PA' → ('MC', 'PA') ; 'AAPL' → ('AAPL', '')
    """
    if "." in symbol:
        parts = symbol.split(".", 1)
        return parts[0], parts[1]
    return symbol, ""


SUFFIX_TO_EXCHANGE: dict[str, str] = {
    "PA": "SBF",
    "DE": "IBIS",
    "AS": "AEB",
    "BR": "ENEXT.BE",
    "MC": "BM",
    "MI": "BVME",
    "ST": "SFB",
    "HE": "HEX",
    "CO": "CSE",
    "OL": "OSL",
    "LS": "LSELECT",
    "L":  "LSE",
    "SW": "EBS",
    "VI": "VSE",
    "WA": "WSE",
    "": "SMART",
}


def yahoo_suffix_to_ibkr_exchange(suffix: str) -> str:
    return SUFFIX_TO_EXCHANGE.get(suffix.upper(), "SMART")
