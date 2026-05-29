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
# Conids FX IDEALPRO résolus via CPAPI secdef/info le 2026-05-06.
# Format clé : XXXYYY sans slash
# Certaines paires inverses du projet (ex: CHFCAD, JPYNZD) sont servies par
# inversion du cross direct dans app.py, afin d'éviter de stocker un alias qui
# masquerait le sens du prix.
# ---------------------------------------------------------------------------
FX_CONIDS: dict[str, int] = {
    # Majors
    "EURUSD": 12087792,
    "GBPUSD": 12087797,
    "USDJPY": 15016059,
    "USDCHF": 12087820,
    "AUDUSD": 14433401,
    "USDCAD": 15016062,
    "NZDUSD": 39453441,
    # EUR crosses
    "EURGBP": 12087807,
    "EURJPY": 14321016,
    "EURCHF": 12087817,
    "EURAUD": 15016065,
    "EURCAD": 15016068,
    "EURNZD": 47101302,
    # GBP crosses
    "GBPJPY": 14321015,
    "GBPCHF": 12087826,
    "GBPAUD": 15016075,
    "GBPCAD": 15016078,
    "GBPNZD": 47101305,
    # Commodity crosses
    "AUDJPY": 15016133,
    "AUDCHF": 15016125,
    "AUDCAD": 15016138,
    "AUDNZD": 39453424,
    "CADCHF": 15016234,
    "CADJPY": 15016241,
    "CHFJPY": 14321010,
    "NZDJPY": 39453444,
    "NZDCHF": 46189224,
    "NZDCAD": 46189223,
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

SUFFIX_EXCHANGE_ALIASES: dict[str, tuple[str, ...]] = {
    # CPAPI can expose Paris equities as ENEXT/SMART instead of SBF,
    # depending on the contract family returned by Client Portal.
    "PA": ("SBF", "ENEXT", "EUIBS", "SMART"),
}


def yahoo_suffix_to_ibkr_exchange(suffix: str) -> str:
    return SUFFIX_TO_EXCHANGE.get(suffix.upper(), "SMART")


def yahoo_suffix_to_ibkr_exchanges(suffix: str) -> tuple[str, ...]:
    suffix = suffix.upper()
    if suffix in SUFFIX_EXCHANGE_ALIASES:
        return SUFFIX_EXCHANGE_ALIASES[suffix]
    primary = yahoo_suffix_to_ibkr_exchange(suffix)
    return (primary,) if primary else ("SMART",)
