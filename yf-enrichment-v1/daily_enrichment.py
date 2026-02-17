import argparse
import math
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

import duckdb
import pandas as pd
import requests


DEFAULT_YF_ENRICH_DB_PATH = "/files/duckdb/yf_enrichment_v1.duckdb"
DEFAULT_AG2_DB_PATH = "/files/duckdb/ag2_v2.duckdb"
DEFAULT_YF_API_URL = "http://yfinance-api:8080"
DEFAULT_OPTIONS_RECHECK_DAYS = 7
DEFAULT_QUOTE_CHUNK_SIZE = 80
DEFAULT_TARGET_DAYS = 30
DEFAULT_TIMEOUT_SEC = 14.0
WORKFLOW_VERSION = "1.0.0"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def safe_float(v: Any, d: float | None = None) -> float | None:
    try:
        if v is None:
            return d
        n = float(v)
        if math.isnan(n) or math.isinf(n):
            return d
        return n
    except Exception:
        return d


def safe_int(v: Any, d: int | None = None) -> int | None:
    n = safe_float(v, None)
    if n is None:
        return d
    try:
        return int(round(n))
    except Exception:
        return d


def to_text(v: Any) -> str:
    if v is None:
        return ""
    return str(v)


def parse_ts(v: Any) -> datetime | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)
    s = str(v).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        d = datetime.fromisoformat(s)
        if d.tzinfo is None:
            return d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc)
    except Exception:
        return None


@contextmanager
def db_con(path: str, read_only: bool = False, retries: int = 5, delay: float = 0.3):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path, read_only=read_only)
            break
        except Exception as exc:
            if "lock" in str(exc).lower() and attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
            else:
                raise
    try:
        yield con
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS run_log (
          run_id VARCHAR PRIMARY KEY,
          started_at TIMESTAMP NOT NULL,
          finished_at TIMESTAMP,
          status VARCHAR DEFAULT 'RUNNING',
          symbols_total INTEGER DEFAULT 0,
          symbols_ok INTEGER DEFAULT 0,
          symbols_error INTEGER DEFAULT 0,
          quote_ok INTEGER DEFAULT 0,
          options_ok INTEGER DEFAULT 0,
          calendar_ok INTEGER DEFAULT 0,
          options_empty INTEGER DEFAULT 0,
          duration_sec DOUBLE,
          error_detail VARCHAR,
          version VARCHAR DEFAULT '1.0.0'
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS yf_symbol_enrichment_history (
          row_id VARCHAR PRIMARY KEY,
          run_id VARCHAR NOT NULL,
          symbol VARCHAR NOT NULL,
          fetched_at TIMESTAMP,

          quote_ok BOOLEAN DEFAULT FALSE,
          quote_error VARCHAR,
          regular_market_price DOUBLE,
          bid DOUBLE,
          ask DOUBLE,
          bid_size DOUBLE,
          ask_size DOUBLE,
          spread_abs DOUBLE,
          spread_pct DOUBLE,
          slippage_proxy_pct DOUBLE,
          volume DOUBLE,
          market_state VARCHAR,
          regular_market_time TIMESTAMP,
          exchange_data_delayed_by INTEGER,
          quote_source VARCHAR,
          quote_fetched_at TIMESTAMP,

          options_ok BOOLEAN DEFAULT FALSE,
          options_error VARCHAR,
          options_warning VARCHAR,
          expiration_selected VARCHAR,
          days_to_expiration DOUBLE,
          iv_atm DOUBLE,
          iv_atm_call DOUBLE,
          iv_atm_put DOUBLE,
          iv_otm_call_5pct DOUBLE,
          iv_otm_put_5pct DOUBLE,
          skew_put_minus_call_5pct DOUBLE,
          put_call_oi_ratio DOUBLE,
          put_call_volume_ratio DOUBLE,
          options_source VARCHAR,
          options_fetched_at TIMESTAMP,

          calendar_ok BOOLEAN DEFAULT FALSE,
          calendar_error VARCHAR,
          next_earnings_date TIMESTAMP,
          days_to_earnings DOUBLE,
          calendar_source VARCHAR,
          calendar_fetched_at TIMESTAMP,

          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_yf_enrich_symbol ON yf_symbol_enrichment_history(symbol)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_yf_enrich_fetched ON yf_symbol_enrichment_history(fetched_at)")
    con.execute(
        """
        CREATE OR REPLACE VIEW v_latest_symbol_enrichment AS
        SELECT * EXCLUDE(rn)
        FROM (
          SELECT t.*,
                 ROW_NUMBER() OVER (
                   PARTITION BY t.symbol
                   ORDER BY COALESCE(t.fetched_at, t.created_at) DESC, t.created_at DESC
                 ) AS rn
          FROM yf_symbol_enrichment_history t
        )
        WHERE rn = 1
        """
    )


def load_symbols(ag2_db_path: str, symbols_csv: str, max_symbols: int) -> list[str]:
    symbols: list[str] = []

    if symbols_csv:
        symbols = [s.strip().upper() for s in str(symbols_csv).replace(";", ",").split(",") if s.strip()]
    elif os.path.exists(ag2_db_path):
        with db_con(ag2_db_path, read_only=True) as con:
            try:
                df = con.execute(
                    """
                    SELECT symbol
                    FROM universe
                    WHERE COALESCE(enabled, TRUE) = TRUE
                    ORDER BY symbol
                    """
                ).fetchdf()
            except Exception:
                df = con.execute("SELECT symbol FROM universe ORDER BY symbol").fetchdf()
        if not df.empty and "symbol" in df.columns:
            symbols = (
                df["symbol"]
                .fillna("")
                .astype(str)
                .str.strip()
                .str.upper()
                .loc[lambda s: s != ""]
                .drop_duplicates()
                .tolist()
            )

    if max_symbols > 0:
        symbols = symbols[:max_symbols]
    return symbols


def fetch_quote_map(api_url: str, symbols: list[str], timeout_sec: float, chunk_size: int) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not symbols:
        return out

    for i in range(0, len(symbols), max(1, int(chunk_size))):
        chunk = symbols[i : i + max(1, int(chunk_size))]
        try:
            resp = requests.get(
                f"{api_url}/quote",
                params={
                    "symbols": ",".join(chunk),
                    "qty": 100,
                    "side": "BUY",
                    "max_age_seconds": 20,
                },
                timeout=timeout_sec,
            )
            if resp.status_code != 200:
                err = f"http_{resp.status_code}"
                for sym in chunk:
                    out[sym] = {"ok": False, "symbol": sym, "error": err}
                continue

            data = resp.json()
            rows = data.get("quotes", [])
            if not isinstance(rows, list):
                rows = []
            seen = set()
            for row in rows:
                sym = to_text(row.get("symbol", "")).strip().upper()
                if not sym:
                    continue
                seen.add(sym)
                out[sym] = row
            for sym in chunk:
                if sym not in seen:
                    out[sym] = {"ok": False, "symbol": sym, "error": "MISSING_QUOTE_ROW"}
        except Exception as exc:
            err = str(exc)
            for sym in chunk:
                out[sym] = {"ok": False, "symbol": sym, "error": err}
    return out


def should_skip_options(
    con: duckdb.DuckDBPyConnection,
    symbol: str,
    recheck_days: int,
) -> tuple[bool, str]:
    try:
        row = con.execute(
            """
            SELECT options_ok, options_error, options_fetched_at
            FROM v_latest_symbol_enrichment
            WHERE symbol = ?
            """,
            [symbol],
        ).fetchone()
    except Exception:
        return False, ""

    if not row:
        return False, ""

    options_ok = bool(row[0]) if row[0] is not None else False
    options_error = to_text(row[1]).strip().upper()
    options_ts = parse_ts(row[2])

    if options_ok:
        return False, ""
    if options_ts is None:
        return False, ""

    age_days = (utcnow() - options_ts).total_seconds() / 86400.0
    if age_days > max(0, int(recheck_days)):
        return False, ""

    no_options_flags = [
        "NO_EXPIRATIONS_AVAILABLE",
        "REQUESTED_EXPIRATION_NOT_AVAILABLE",
        "EMPTY_OPTION_CHAIN",
        "SKIPPED_RECENT_NO_EXPIRATIONS",
    ]
    if any(flag in options_error for flag in no_options_flags):
        return True, "SKIPPED_RECENT_NO_EXPIRATIONS"
    return False, ""


def fetch_options(api_url: str, symbol: str, target_days: int, timeout_sec: float) -> dict:
    try:
        resp = requests.get(
            f"{api_url}/options",
            params={
                "symbol": symbol,
                "target_days": int(target_days),
                "max_rows_per_side": 120,
                "max_age_seconds": 300,
            },
            timeout=timeout_sec,
        )
        if resp.status_code != 200:
            return {"ok": False, "symbol": symbol, "error": f"http_{resp.status_code}"}
        data = resp.json()
        if not isinstance(data, dict):
            return {"ok": False, "symbol": symbol, "error": "INVALID_OPTIONS_PAYLOAD"}
        return data
    except Exception as exc:
        return {"ok": False, "symbol": symbol, "error": str(exc)}


def fetch_calendar(api_url: str, symbol: str, timeout_sec: float) -> dict:
    try:
        resp = requests.get(
            f"{api_url}/calendar",
            params={"symbol": symbol, "earnings_limit": 8, "max_age_seconds": 1800},
            timeout=timeout_sec,
        )
        if resp.status_code != 200:
            return {"ok": False, "symbol": symbol, "error": f"http_{resp.status_code}"}
        data = resp.json()
        if not isinstance(data, dict):
            return {"ok": False, "symbol": symbol, "error": "INVALID_CALENDAR_PAYLOAD"}
        return data
    except Exception as exc:
        return {"ok": False, "symbol": symbol, "error": str(exc)}


def build_row(
    run_id: str,
    symbol: str,
    quote_row: dict,
    options_row: dict,
    calendar_row: dict,
) -> dict:
    now_ts = utcnow()

    quote_ok = bool(quote_row.get("ok", False))
    quote_error = to_text(quote_row.get("error", ""))
    quote_source = to_text(quote_row.get("source", ""))
    quote_fetched_at = parse_ts(quote_row.get("fetchedAt"))
    regular_market_time = parse_ts(quote_row.get("regularMarketTime"))

    options_ok = bool(options_row.get("ok", False))
    options_error = to_text(options_row.get("error", ""))
    options_source = to_text(options_row.get("source", ""))
    options_fetched_at = parse_ts(options_row.get("fetchedAt"))

    options_metrics = options_row.get("metrics", {}) if isinstance(options_row.get("metrics"), dict) else {}
    options_warnings = options_row.get("warnings", [])
    if isinstance(options_warnings, list):
        options_warning = " | ".join([to_text(x) for x in options_warnings if to_text(x).strip()])
    else:
        options_warning = to_text(options_warnings)

    calendar_ok = bool(calendar_row.get("ok", False))
    calendar_error = to_text(calendar_row.get("error", ""))
    calendar_source = to_text(calendar_row.get("source", ""))
    calendar_fetched_at = parse_ts(calendar_row.get("fetchedAt"))
    next_earnings_date = parse_ts(calendar_row.get("nextEarningsDate"))

    days_to_earnings = None
    if next_earnings_date is not None:
        days_to_earnings = (next_earnings_date - now_ts).total_seconds() / 86400.0

    fetched_candidates = [x for x in [quote_fetched_at, options_fetched_at, calendar_fetched_at] if x is not None]
    fetched_at = max(fetched_candidates) if fetched_candidates else now_ts

    return {
        "row_id": f"{run_id}|{symbol}",
        "run_id": run_id,
        "symbol": symbol,
        "fetched_at": fetched_at,
        "quote_ok": quote_ok,
        "quote_error": quote_error,
        "regular_market_price": safe_float(quote_row.get("regularMarketPrice")),
        "bid": safe_float(quote_row.get("bid")),
        "ask": safe_float(quote_row.get("ask")),
        "bid_size": safe_float(quote_row.get("bidSize")),
        "ask_size": safe_float(quote_row.get("askSize")),
        "spread_abs": safe_float(quote_row.get("spreadAbs")),
        "spread_pct": safe_float(quote_row.get("spreadPct")),
        "slippage_proxy_pct": safe_float(quote_row.get("slippageProxyPct")),
        "volume": safe_float(quote_row.get("volume")),
        "market_state": to_text(quote_row.get("marketState")),
        "regular_market_time": regular_market_time,
        "exchange_data_delayed_by": safe_int(quote_row.get("exchangeDataDelayedBy")),
        "quote_source": quote_source,
        "quote_fetched_at": quote_fetched_at,
        "options_ok": options_ok,
        "options_error": options_error,
        "options_warning": options_warning,
        "expiration_selected": to_text(options_row.get("expirationSelected")),
        "days_to_expiration": safe_float(options_row.get("daysToExpiration")),
        "iv_atm": safe_float(options_metrics.get("ivAtm")),
        "iv_atm_call": safe_float(options_metrics.get("ivAtmCall")),
        "iv_atm_put": safe_float(options_metrics.get("ivAtmPut")),
        "iv_otm_call_5pct": safe_float(options_metrics.get("ivOtmCall5Pct")),
        "iv_otm_put_5pct": safe_float(options_metrics.get("ivOtmPut5Pct")),
        "skew_put_minus_call_5pct": safe_float(options_metrics.get("skewPutMinusCall5Pct")),
        "put_call_oi_ratio": safe_float(options_metrics.get("putCallOiRatio")),
        "put_call_volume_ratio": safe_float(options_metrics.get("putCallVolumeRatio")),
        "options_source": options_source,
        "options_fetched_at": options_fetched_at,
        "calendar_ok": calendar_ok,
        "calendar_error": calendar_error,
        "next_earnings_date": next_earnings_date,
        "days_to_earnings": round(float(days_to_earnings), 3) if days_to_earnings is not None else None,
        "calendar_source": calendar_source,
        "calendar_fetched_at": calendar_fetched_at,
    }


def write_row(con: duckdb.DuckDBPyConnection, row: dict) -> None:
    cols = list(row.keys())
    placeholders = ", ".join(["?"] * len(cols))
    con.execute(
        f"""
        INSERT OR REPLACE INTO yf_symbol_enrichment_history
        ({", ".join(cols)}, updated_at)
        VALUES ({placeholders}, CURRENT_TIMESTAMP)
        """,
        [row[c] for c in cols],
    )


def run_job(
    yf_enrich_db_path: str,
    ag2_db_path: str,
    yf_api_url: str,
    max_symbols: int,
    symbols_csv: str,
    options_recheck_days: int,
    quote_chunk_size: int,
    target_days: int,
    timeout_sec: float,
) -> dict:
    started = utcnow()
    run_id = f"YFENRICH_{started.strftime('%Y%m%d%H%M%S')}"

    with db_con(yf_enrich_db_path, read_only=False) as con:
        init_schema(con)
        symbols = load_symbols(ag2_db_path, symbols_csv, max_symbols=max_symbols)
        con.execute(
            """
            INSERT OR REPLACE INTO run_log (
              run_id, started_at, status, symbols_total, symbols_ok, symbols_error,
              quote_ok, options_ok, calendar_ok, options_empty, duration_sec, error_detail, version
            )
            VALUES (?, ?, 'RUNNING', ?, 0, 0, 0, 0, 0, 0, NULL, NULL, ?)
            """,
            [run_id, started, len(symbols), WORKFLOW_VERSION],
        )

        if not symbols:
            finished = utcnow()
            con.execute(
                """
                UPDATE run_log
                SET finished_at = ?, status = 'NO_DATA', symbols_ok = 0, symbols_error = 0,
                    quote_ok = 0, options_ok = 0, calendar_ok = 0, options_empty = 0,
                    duration_sec = ?, error_detail = 'NO_SYMBOLS'
                WHERE run_id = ?
                """,
                [finished, (finished - started).total_seconds(), run_id],
            )
            return {"ok": False, "run_id": run_id, "error": "NO_SYMBOLS", "symbols_total": 0}

        quote_map = fetch_quote_map(
            api_url=yf_api_url,
            symbols=symbols,
            timeout_sec=timeout_sec,
            chunk_size=quote_chunk_size,
        )

        stats = {
            "symbols_ok": 0,
            "symbols_error": 0,
            "quote_ok": 0,
            "options_ok": 0,
            "calendar_ok": 0,
            "options_empty": 0,
        }

        for sym in symbols:
            quote_row = quote_map.get(sym, {"ok": False, "symbol": sym, "error": "MISSING_QUOTE"})
            skip_options, skip_reason = should_skip_options(con, sym, recheck_days=options_recheck_days)
            if skip_options:
                options_row = {
                    "ok": False,
                    "symbol": sym,
                    "error": skip_reason,
                    "source": "cache_policy",
                    "fetchedAt": utcnow().isoformat(),
                }
            else:
                options_row = fetch_options(
                    api_url=yf_api_url,
                    symbol=sym,
                    target_days=target_days,
                    timeout_sec=timeout_sec,
                )

            calendar_row = fetch_calendar(
                api_url=yf_api_url,
                symbol=sym,
                timeout_sec=timeout_sec,
            )

            row = build_row(
                run_id=run_id,
                symbol=sym,
                quote_row=quote_row,
                options_row=options_row,
                calendar_row=calendar_row,
            )
            write_row(con, row)

            q_ok = bool(row.get("quote_ok", False))
            o_ok = bool(row.get("options_ok", False))
            c_ok = bool(row.get("calendar_ok", False))

            if q_ok:
                stats["quote_ok"] += 1
            if o_ok:
                stats["options_ok"] += 1
            if c_ok:
                stats["calendar_ok"] += 1

            options_err = to_text(row.get("options_error", "")).upper()
            if "NO_EXPIRATIONS_AVAILABLE" in options_err or "SKIPPED_RECENT_NO_EXPIRATIONS" in options_err:
                stats["options_empty"] += 1

            if q_ok or o_ok or c_ok:
                stats["symbols_ok"] += 1
            else:
                stats["symbols_error"] += 1

        # refresh latest view explicitly
        init_schema(con)

        finished = utcnow()
        status = "SUCCESS" if stats["symbols_error"] == 0 else ("PARTIAL" if stats["symbols_ok"] > 0 else "FAILED")
        duration_sec = (finished - started).total_seconds()
        con.execute(
            """
            UPDATE run_log
            SET finished_at = ?, status = ?, symbols_ok = ?, symbols_error = ?,
                quote_ok = ?, options_ok = ?, calendar_ok = ?, options_empty = ?,
                duration_sec = ?, error_detail = NULL
            WHERE run_id = ?
            """,
            [
                finished,
                status,
                stats["symbols_ok"],
                stats["symbols_error"],
                stats["quote_ok"],
                stats["options_ok"],
                stats["calendar_ok"],
                stats["options_empty"],
                duration_sec,
                run_id,
            ],
        )

    return {
        "ok": True,
        "run_id": run_id,
        "status": status,
        "symbols_total": len(symbols),
        **stats,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily YF enrichment into DuckDB")
    parser.add_argument("--yf-enrich-db-path", default=os.getenv("YF_ENRICH_DB_PATH", DEFAULT_YF_ENRICH_DB_PATH))
    parser.add_argument("--ag2-db-path", default=os.getenv("AG2_DUCKDB_PATH", os.getenv("DUCKDB_PATH", DEFAULT_AG2_DB_PATH)))
    parser.add_argument("--yf-api-url", default=os.getenv("YFINANCE_API_URL", DEFAULT_YF_API_URL))
    parser.add_argument("--symbols", default=os.getenv("SYMBOLS", ""))
    parser.add_argument("--max-symbols", type=int, default=int(os.getenv("YF_ENRICH_MAX_SYMBOLS", "0")))
    parser.add_argument("--options-recheck-days", type=int, default=int(os.getenv("YF_OPTIONS_RECHECK_DAYS", str(DEFAULT_OPTIONS_RECHECK_DAYS))))
    parser.add_argument("--quote-chunk-size", type=int, default=int(os.getenv("YF_ENRICH_QUOTE_CHUNK", str(DEFAULT_QUOTE_CHUNK_SIZE))))
    parser.add_argument("--target-days", type=int, default=int(os.getenv("YF_ENRICH_OPTIONS_TARGET_DAYS", str(DEFAULT_TARGET_DAYS))))
    parser.add_argument("--timeout-sec", type=float, default=float(os.getenv("YF_ENRICH_TIMEOUT_SEC", str(DEFAULT_TIMEOUT_SEC))))
    args = parser.parse_args()

    res = run_job(
        yf_enrich_db_path=args.yf_enrich_db_path,
        ag2_db_path=args.ag2_db_path,
        yf_api_url=args.yf_api_url,
        max_symbols=args.max_symbols,
        symbols_csv=args.symbols,
        options_recheck_days=args.options_recheck_days,
        quote_chunk_size=args.quote_chunk_size,
        target_days=args.target_days,
        timeout_sec=args.timeout_sec,
    )
    print(res)


if __name__ == "__main__":
    main()

