"""Fill recent missing D1 sessions with observed IBKR OHLCV, never interpolation.

Yahoo-valid sessions are authoritative and never overwritten. The caller must
still enforce the existing closed-bar and OHLCV gates. Cache is separate from
Yahoo's raw history so provenance and rollback remain explicit.
"""

import json
import math
import os
import pathlib
import re
import tempfile
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

_LOCK = threading.Lock()
_CURRENCY = {"PA": "EUR", "DE": "EUR", "AS": "EUR", "MC": "EUR", "SW": "CHF"}


def _get(base, path, params):
    with urllib.request.urlopen(
        base.rstrip("/") + path + "?" + urllib.parse.urlencode(params), timeout=8
    ) as response:
        return json.load(response)


def _number(value):
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def _valid(bar):
    values = [_number(bar.get(k)) for k in ("o", "h", "l", "c", "v")]
    if any(v is None for v in values):
        return False
    o, h, l, c, v = values
    return min(o, h, l, c) > 0 and l <= min(o, c) <= max(o, c) <= h and v >= 0


def fill_daily_gaps(
    bars,
    *,
    symbol,
    asset_class,
    market_timezone,
    market_close,
    now,
    data_dir,
    max_bars,
    base_url
):
    audit = {"provider": "ibkr_cpapi", "status": "NOT_NEEDED", "added": 0}
    ticker, _, suffix = symbol.upper().rpartition(".")
    if (
        not base_url
        or asset_class.upper() not in ("EQUITY", "ETF")
        or suffix not in _CURRENCY
        or not bars
    ):
        return bars, {**audit, "status": "OUT_OF_SCOPE"}
    zone = ZoneInfo(market_timezone)
    today = now.astimezone(zone).date()

    # Only the last seven days may be supplemented; never create a synthetic
    # historical backfill or use a current-session quote as a daily close.
    def day(stamp):
        ts = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
        return ts.date() if ts.hour == ts.minute == 0 else ts.astimezone(zone).date()

    existing = {day(b["t"]): b for b in bars}
    eligible = set()
    for offset in range(7):
        date = today - timedelta(days=offset)
        close = datetime.combine(date, market_close, zone)
        if (
            date.weekday() < 5
            and now >= close + timedelta(minutes=10)
            and date not in existing
        ):
            eligible.add(date)
    if not eligible:
        return bars, audit
    audit["missingSessions"] = sorted(d.isoformat() for d in eligible)
    safe = re.sub("[^A-Z0-9_.-]", "_", symbol.upper())
    directory = pathlib.Path(data_dir) / "ibkr-daily-fallback"
    cache = directory / (safe + ".json")
    try:
        with _LOCK:
            snapshot = None
            if cache.exists():
                saved = json.loads(cache.read_text())
                if time.time() - saved.get("fetchedTs", 0) < 900:
                    snapshot = saved
            if snapshot is None:
                resolved = _get(
                    base_url, "/contracts/equity/resolve", {"symbols": symbol}
                )
                results = resolved.get("results") or []
                if len(results) != 1 or resolved.get("errors"):
                    raise ValueError("CONTRACT_UNRESOLVED")
                contract = results[0]
                if (
                    contract.get("contract_symbol") != ticker
                    or contract.get("currency") != _CURRENCY[suffix]
                    or contract.get("metadata_error")
                ):
                    raise ValueError("CONTRACT_IDENTITY_MISMATCH")
                conid = int(contract["conid"])
                history = _get(
                    base_url,
                    "/marketdata/history",
                    {
                        "conid": conid,
                        "period": "1w",
                        "bar": "1d",
                        "outside_rth": "false",
                    },
                )
                # CPAPI can return an empty first frame while opening a chart.
                # One bounded retry is allowed; an explicit error is never hidden.
                if not history.get("data") and not history.get("error"):
                    time.sleep(0.5)
                    history = _get(
                        base_url,
                        "/marketdata/history",
                        {
                            "conid": conid,
                            "period": "1w",
                            "bar": "1d",
                            "outside_rth": "false",
                        },
                    )
                if (
                    history.get("error")
                    or not history.get("data")
                    or history.get("conid") != conid
                ):
                    raise ValueError("HISTORY_UNAVAILABLE")
                snapshot = {
                    "symbol": symbol,
                    "conid": conid,
                    "currency": contract["currency"],
                    "fetchedTs": time.time(),
                    "bars": history["data"],
                }
                directory.mkdir(parents=True, exist_ok=True)
                fd, tmp = tempfile.mkstemp(
                    dir=directory, prefix=safe + "-", suffix=".tmp"
                )
                try:
                    with os.fdopen(fd, "w") as handle:
                        json.dump(snapshot, handle)
                    os.replace(tmp, cache)
                finally:
                    if os.path.exists(tmp):
                        os.unlink(tmp)
        candidates = {}
        for raw in snapshot["bars"]:
            if not _valid(raw):
                continue
            stamp = datetime.fromtimestamp(float(raw["t"]) / 1000, timezone.utc)
            date = stamp.astimezone(zone).date()
            if now < datetime.combine(date, market_close, zone) + timedelta(minutes=10):
                continue
            candidates[date] = {
                **{k: float(raw[k]) for k in ("o", "h", "l", "c", "v")},
                "t": datetime.combine(date, datetime.min.time(), timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "closed": True,
                "quality": "VALID",
                "source": "ibkr_cpapi_daily",
                "conid": snapshot["conid"],
            }
        overlap = sorted(set(existing) & set(candidates))
        # Two common valid sessions must confirm price units/series coherence.
        # This additional data-quality check does not relax any AG1 gate.
        comparable = [date for date in overlap if _valid(existing[date])]
        if len(comparable) < 2:
            raise ValueError("INSUFFICIENT_OVERLAP")
        deviation = max(
            abs(candidates[d][k] / existing[d][k] - 1)
            for d in comparable
            for k in ("o", "h", "l", "c")
        )
        if deviation > 0.02:
            raise ValueError("PRICE_SERIES_MISMATCH")
        additions = [candidates[d] for d in sorted(eligible & set(candidates))]
        combined = sorted(bars + additions, key=lambda b: b["t"])
        if max_bars:
            combined = combined[-max_bars:]
        return combined, {
            **audit,
            "status": "FILLED" if additions else "NO_CLOSED_SESSION",
            "added": len(additions),
            "conid": snapshot["conid"],
            "currency": snapshot["currency"],
            "overlapSessions": len(comparable),
            "maxOverlapDeviation": round(deviation, 8),
            "outsideRth": False,
            "fetchedAt": datetime.fromtimestamp(
                snapshot["fetchedTs"], timezone.utc
            ).isoformat(),
        }
    except Exception as exc:
        return bars, {
            **audit,
            "status": "UNAVAILABLE",
            "error": type(exc).__name__ + ": " + str(exc)[:160],
        }
