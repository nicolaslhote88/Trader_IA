"""Normalized, conservative market contracts; CPAPI sizeIncrement is a UI step."""
from datetime import datetime, timezone, timedelta
import math


def equity_quantity_rules(symbol, info, rules):
    suffix = symbol.upper().rsplit(".", 1)[-1] if "." in symbol else ""
    currency = str(info.get("currency") or rules.get("cashCcy") or "").upper()
    step, source = None, "unverified_market_lot"
    if suffix == "T":
        # JPX domestic common shares have 100-share units. ETFs/REITs differ.
        industry = str(info.get("industry") or "").strip()
        category = str(info.get("category") or "").lower()
        if currency == "JPY" and industry and not any(x in category for x in ("etf", "fund", "reit", "trust")):
            step, source = 100, "JPX_domestic_common_shares_100"
    elif suffix in {"", "PA", "DE", "AS", "BR", "MC", "MI", "ST", "HE", "CO", "OL", "SW", "L", "KS", "KQ", "AX", "TO", "V"}:
        step, source = 1, "whole_share_market_policy"
    return {"min_quantity": step, "quantity_increment": step,
            "quantity_rule_known": step is not None, "quantity_rule_source": source}


def quantity_error(quantity, contract, side="BUY"):
    if not math.isfinite(float(quantity)) or quantity <= 0:
        return "INVALID_QUANTITY"
    if not contract.get("quantity_rule_known"):
        return "QUANTITY_RULE_UNKNOWN" if side == "BUY" else None
    step = contract["quantity_increment"]
    if abs(quantity / step - round(quantity / step)) > 1e-8:
        return "BOARD_LOT_MISMATCH"
    return None


def normalize_news_time(raw_ms, now=None):
    """Do not invent a timezone: future/invalid provider dates use observed time."""
    observed = now or datetime.now(timezone.utc)
    candidate = None
    try:
        candidate = datetime.fromtimestamp(float(raw_ms) / 1000, tz=timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        pass
    valid = candidate is not None and datetime(2000, 1, 1, tzinfo=timezone.utc) <= candidate <= observed
    return {"published_at": (candidate if valid else observed).isoformat(),
            "provider_published_at": candidate.isoformat() if candidate else None,
            "provider_time_raw": raw_ms, "observed_at": observed.isoformat(),
            "published_at_source": "provider_epoch" if valid else "observed_at_fallback",
            "timestamp_quality": "UNVERIFIED_PROVIDER_TIME" if valid else "INVALID_PROVIDER_TIME"}
