import gc
import json
import math
import os
import re
import time
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import duckdb

DEFAULT_TARGETS = [
    "/files/duckdb/ag1_fx_v1_chatgpt52.duckdb",
    "/files/duckdb/ag1_fx_v1_grok41_reasoning.duckdb",
    "/files/duckdb/ag1_fx_v1_gemini30_pro.duckdb",
]

DEFAULT_AG2_FX_PATH = "/files/duckdb/ag2_fx_v1.duckdb"
DEFAULT_SCHEMA_PATH = "/files/AG1-FX-V1-EXPORT/sql/ag1_fx_v1_schema.sql"
DEFAULT_YFINANCE_API_BASE = "http://yfinance-api:8080"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 5
DEFAULT_MAX_PRICE_WORKERS = 8

VARIANT_BY_DB = {
    "ag1_fx_v1_chatgpt52.duckdb": "chatgpt52",
    "ag1_fx_v1_grok41_reasoning.duckdb": "grok41_reasoning",
    "ag1_fx_v1_gemini30_pro.duckdb": "gemini30_pro",
}


def to_text(v):
    if v is None:
        return ""
    s = str(v)
    return "" if s.lower() in ("nan", "nat", "none", "null") else s.strip()


def to_float(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        n = float(v)
        return n if math.isfinite(n) else default
    except Exception:
        return default


def to_bool(v, default=False):
    if isinstance(v, bool):
        return v
    s = to_text(v).lower()
    if not s:
        return default
    return s in {"1", "true", "yes", "y", "on"}


def parse_paths(v):
    if v is None:
        return []
    if isinstance(v, (list, tuple, set)):
        return [to_text(x) for x in v if to_text(x)]
    s = to_text(v)
    if not s:
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [to_text(x) for x in arr if to_text(x)]
        except Exception:
            pass
    if "," in s or ";" in s:
        return [
            to_text(x).strip().strip('"').strip("'")
            for x in re.split(r"[;,]", s.strip().strip("[]"))
            if to_text(x).strip().strip('"').strip("'")
        ]
    return [s]


def dedupe(values):
    out = []
    seen = set()
    for value in values:
        text = to_text(value)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def path_aliases(path_text):
    p = to_text(path_text).replace("\\", "/")
    if not p:
        return []
    out = [p]
    if p.startswith("/files/"):
        out.append("/local-files/" + p[len("/files/"):])
    elif p.startswith("/local-files/"):
        out.append("/files/" + p[len("/local-files/"):])
    return dedupe(out)


def resolve_existing_path(path_text):
    aliases = path_aliases(path_text)
    for p in aliases:
        try:
            if os.path.exists(p):
                return p
        except Exception:
            pass
    return aliases[0] if aliases else ""


def resolve_writable_path(path_text):
    aliases = path_aliases(path_text)
    for p in aliases:
        try:
            if os.path.exists(p):
                return p
        except Exception:
            pass
    for p in aliases:
        try:
            if os.path.isdir(os.path.dirname(p)):
                return p
        except Exception:
            pass
    return aliases[0] if aliases else ""


@contextmanager
def db_con(path, read_only=False, retries=7, base_delay=0.25):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path, read_only=read_only)
            break
        except Exception as exc:
            msg = str(exc).lower()
            if ("lock" in msg or "busy" in msg) and attempt < retries - 1:
                time.sleep(base_delay * (2 ** attempt))
                continue
            raise
    try:
        yield con
    finally:
        if con is not None:
            if not read_only:
                try:
                    con.execute("CHECKPOINT")
                except Exception:
                    pass
            try:
                con.close()
            except Exception:
                pass
        gc.collect()


def split_sql(text):
    buff = []
    out = []
    sq = False
    dq = False
    for ch in text:
        if ch == "'" and not dq:
            sq = not sq
        elif ch == '"' and not sq:
            dq = not dq
        if ch == ";" and not sq and not dq:
            stmt = "".join(buff).strip()
            if stmt:
                out.append(stmt)
            buff = []
        else:
            buff.append(ch)
    stmt = "".join(buff).strip()
    if stmt:
        out.append(stmt)
    return out


def init_schema_if_available(con, schema_path):
    path = resolve_existing_path(schema_path)
    if not path:
        return False
    p = Path(path)
    if not p.is_file():
        return False
    for stmt in split_sql(p.read_text(encoding="utf-8")):
        con.execute(stmt)
    return True


def table_exists(con, table_name, schema="core"):
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table_name],
    ).fetchone()
    return bool(row and int(row[0] or 0) > 0)


def fetch_price_from_yfinance(base_url, pair, cfg):
    symbol = f"{pair}=X"
    params = {
        "symbol": symbol,
        "interval": to_text(cfg.get("interval")) or "1h",
        "lookback_days": int(to_float(cfg.get("lookback_days"), 5)),
        "max_bars": int(to_float(cfg.get("max_bars"), 10)),
        "min_bars": int(to_float(cfg.get("min_bars"), 1)),
        "allow_stale": "true" if to_bool(cfg.get("allow_stale"), True) else "false",
    }
    url = f"{base_url.rstrip('/')}/history?{urlencode(params)}"
    req = Request(url, headers={"Accept": "application/json"})
    timeout = max(1, int(to_float(cfg.get("request_timeout_seconds"), DEFAULT_REQUEST_TIMEOUT_SECONDS)))
    with urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    if payload.get("ok") is False:
        raise RuntimeError(payload.get("error") or "yfinance ok=false")
    if isinstance(payload.get("last"), dict):
        px = to_float(payload["last"].get("c"), 0)
        if px > 0:
            return {
                "pair": pair,
                "price": px,
                "as_of": payload["last"].get("t"),
                "source": payload.get("source") or "yfinance_api",
                "stale": bool(payload.get("stale")),
            }
    bars = payload.get("bars") if isinstance(payload.get("bars"), list) else []
    if bars:
        last = bars[-1]
        px = to_float(last.get("c"), 0)
        if px > 0:
            return {
                "pair": pair,
                "price": px,
                "as_of": last.get("t"),
                "source": payload.get("source") or "yfinance_api",
                "stale": bool(payload.get("stale")),
            }
    raise RuntimeError("No last price in yfinance payload")


def fetch_ag2_prices(ag2_path):
    db_path = resolve_existing_path(ag2_path)
    if not db_path or not os.path.exists(db_path):
        return {}
    try:
        with db_con(db_path, read_only=True) as con:
            rows = con.execute(
                """
                SELECT pair, CAST(last_close AS DOUBLE) AS last_close, CAST(as_of AS VARCHAR) AS as_of
                FROM main.technical_signals_fx
                WHERE run_id = (
                    SELECT run_id
                    FROM main.run_log
                    ORDER BY finished_at DESC NULLS LAST, started_at DESC NULLS LAST
                    LIMIT 1
                )
                """
            ).fetchall()
        return {
            to_text(pair).upper(): {
                "pair": to_text(pair).upper(),
                "price": to_float(last_close, 0),
                "as_of": as_of,
                "source": "ag2_fx_v1",
                "stale": True,
            }
            for pair, last_close, as_of in rows
            if to_text(pair) and to_float(last_close, 0) > 0
        }
    except Exception:
        return {}


def load_universe_pairs(ag2_path):
    db_path = resolve_existing_path(ag2_path)
    fallback = [
        "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD",
        "EURGBP", "EURJPY", "EURCHF", "EURAUD", "EURCAD", "EURNZD",
        "GBPJPY", "GBPCHF", "GBPAUD", "GBPCAD",
        "AUDJPY", "AUDNZD", "AUDCAD",
        "NZDJPY", "NZDCAD", "CADJPY", "CHFJPY", "CADCHF", "CHFCAD", "JPYNZD",
    ]
    if not db_path or not os.path.exists(db_path):
        return fallback
    try:
        with db_con(db_path, read_only=True) as con:
            rows = con.execute(
                """
                SELECT pair
                FROM main.universe_fx
                WHERE enabled = TRUE
                ORDER BY pair
                """
            ).fetchall()
        pairs = [to_text(r[0]).upper() for r in rows if to_text(r[0])]
        return pairs or fallback
    except Exception:
        return fallback


def read_open_lots(con):
    if not table_exists(con, "position_lots", "core"):
        return []
    cur = con.execute(
        """
        SELECT lot_id, pair, side, size_lots, open_price, open_at,
               stop_loss_price, take_profit_price, leverage_used
        FROM core.position_lots
        WHERE status = 'open'
        ORDER BY open_at
        """
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def read_config(con, db_path):
    cfg = {
        "llm_model": "unset",
        "initial_capital_eur": 10000.0,
        "leverage_max": 1.0,
    }
    if table_exists(con, "portfolio_config", "cfg"):
        row = con.execute(
            """
            SELECT llm_model, initial_capital_eur, leverage_max
            FROM cfg.portfolio_config
            WHERE config_key = 'default'
            LIMIT 1
            """
        ).fetchone()
        if row:
            cfg["llm_model"] = to_text(row[0]) or "unset"
            cfg["initial_capital_eur"] = to_float(row[1], 10000.0)
            cfg["leverage_max"] = max(0.01, to_float(row[2], 1.0))
    cfg["variant"] = VARIANT_BY_DB.get(Path(db_path).name.lower(), Path(db_path).stem)
    return cfg


def read_cash_eur(con, fallback):
    if not table_exists(con, "cash_ledger", "core"):
        return fallback
    row = con.execute("SELECT COALESCE(SUM(amount_eur), 0) FROM core.cash_ledger").fetchone()
    val = to_float(row[0], fallback) if row else fallback
    return val if val > 0 else fallback


def read_realized_pnl(con):
    if not table_exists(con, "position_lots", "core"):
        return 0.0
    row = con.execute(
        "SELECT COALESCE(SUM(pnl_eur), 0) FROM core.position_lots WHERE status = 'closed'"
    ).fetchone()
    return to_float(row[0], 0.0) if row else 0.0


def read_fees(con):
    if not table_exists(con, "fills", "core"):
        return 0.0
    row = con.execute("SELECT COALESCE(SUM(fees_eur), 0) FROM core.fills").fetchone()
    return to_float(row[0], 0.0) if row else 0.0


def read_previous_peak(con, fallback):
    if not table_exists(con, "portfolio_snapshot", "core"):
        return fallback
    row = con.execute("SELECT MAX(equity_eur) FROM core.portfolio_snapshot").fetchone()
    return max(fallback, to_float(row[0], fallback) if row else fallback)


def read_day_start_equity(con, fallback):
    if not table_exists(con, "portfolio_snapshot", "core"):
        return fallback
    row = con.execute(
        """
        SELECT equity_eur
        FROM core.portfolio_snapshot
        WHERE CAST(as_of AS DATE) = CURRENT_DATE
        ORDER BY as_of ASC
        LIMIT 1
        """
    ).fetchone()
    return to_float(row[0], fallback) if row else fallback


def quote_to_eur(quote, prices):
    quote = to_text(quote).upper()
    if quote == "EUR":
        return 1.0

    direct = prices.get(f"{quote}EUR", {}).get("price")
    if direct and direct > 0:
        return direct

    inverse = prices.get(f"EUR{quote}", {}).get("price")
    if inverse and inverse > 0:
        return 1.0 / inverse

    if quote == "USD":
        eurusd = prices.get("EURUSD", {}).get("price")
        return 1.0 / eurusd if eurusd and eurusd > 0 else 1.0

    quote_usd = prices.get(f"{quote}USD", {}).get("price")
    usd_quote = prices.get(f"USD{quote}", {}).get("price")
    eurusd = prices.get("EURUSD", {}).get("price")
    usd_eur = 1.0 / eurusd if eurusd and eurusd > 0 else 0.0
    if quote_usd and quote_usd > 0 and usd_eur > 0:
        return quote_usd * usd_eur
    if usd_quote and usd_quote > 0 and usd_eur > 0:
        return (1.0 / usd_quote) * usd_eur

    return 1.0


def conversion_pairs_for_open_pairs(open_pairs):
    if not open_pairs:
        return []
    pairs = {"EURUSD"}
    for pair in open_pairs:
        pair = to_text(pair).upper()
        if len(pair) != 6:
            continue
        base = pair[:3]
        quote = pair[3:]
        pairs.add(pair)
        for ccy in {base, quote}:
            if ccy and ccy not in {"EUR", "USD"}:
                pairs.add(f"EUR{ccy}")
                pairs.add(f"{ccy}EUR")
                pairs.add(f"USD{ccy}")
                pairs.add(f"{ccy}USD")
    return dedupe(pairs)


def build_price_map(cfg, pairs_needed):
    ag2_prices = fetch_ag2_prices(cfg.get("ag2_fx_path") or DEFAULT_AG2_FX_PATH)
    yfinance_base = to_text(cfg.get("yfinance_api_base")) or DEFAULT_YFINANCE_API_BASE
    dry_run = to_bool(cfg.get("dry_run"), False) or os.getenv("AG1_FX_DRY_RUN") == "1"
    prices = {}
    errors = []
    pairs = sorted({p for p in pairs_needed if p})

    if dry_run:
        for pair in pairs:
            if pair in ag2_prices:
                prices[pair] = ag2_prices[pair]
    else:
        max_workers = max(1, int(to_float(cfg.get("max_price_workers"), DEFAULT_MAX_PRICE_WORKERS)))
        max_workers = min(max_workers, max(1, len(pairs)))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(fetch_price_from_yfinance, yfinance_base, pair, cfg): pair
                for pair in pairs
            }
            for fut in as_completed(futures):
                pair = futures[fut]
                try:
                    prices[pair] = fut.result()
                except Exception as exc:
                    if pair in ag2_prices:
                        prices[pair] = ag2_prices[pair]
                    else:
                        errors.append(f"{pair}: {exc}")

    for pair, rec in ag2_prices.items():
        prices.setdefault(pair, rec)

    return prices, errors


def update_one_portfolio(db_path_cfg, cfg, prices):
    db_path = resolve_writable_path(db_path_cfg)
    now = datetime.now(timezone.utc)
    result = {
        "configured_db_path": db_path_cfg,
        "db_path": db_path,
        "status": "FAILED",
        "open_lots_count": 0,
        "equity_eur": None,
        "error": "",
    }

    try:
        with db_con(db_path, read_only=False) as con:
            init_schema_if_available(con, cfg.get("schema_path") or DEFAULT_SCHEMA_PATH)
            if not table_exists(con, "position_lots", "core") or not table_exists(con, "portfolio_snapshot", "core"):
                raise RuntimeError("Missing AG1-FX core tables")

            pcfg = read_config(con, db_path)
            lots = read_open_lots(con)
            cash = read_cash_eur(con, pcfg["initial_capital_eur"])
            realized = read_realized_pnl(con)
            fees = read_fees(con)

            floating = 0.0
            notional = 0.0
            priced_lots = 0
            missing_prices = []

            for lot in lots:
                pair = to_text(lot.get("pair")).upper()
                px = to_float((prices.get(pair) or {}).get("price"), 0.0)
                if px <= 0:
                    px = to_float(lot.get("open_price"), 0.0)
                    missing_prices.append(pair)
                if px <= 0:
                    continue

                quote = pair[3:]
                q2e = quote_to_eur(quote, prices)
                size_lots = to_float(lot.get("size_lots"), 0.0)
                open_price = to_float(lot.get("open_price"), px)
                direction = -1.0 if to_text(lot.get("side")).lower() == "short" else 1.0
                floating += size_lots * 100000.0 * (px - open_price) * direction * q2e
                notional += abs(size_lots * 100000.0 * px * q2e)
                priced_lots += 1

            equity = cash + realized + floating - fees
            pnl_total = equity - pcfg["initial_capital_eur"]
            margin_used = notional / pcfg["leverage_max"]
            leverage_effective = notional / equity if equity > 0 else 0.0
            margin_free = max(0.0, equity - margin_used)
            peak = max(read_previous_peak(con, pcfg["initial_capital_eur"]), equity, pcfg["initial_capital_eur"])
            day_start = read_day_start_equity(con, equity)
            drawdown_total = equity / peak - 1.0 if peak > 0 else 0.0
            drawdown_day = equity / day_start - 1.0 if day_start > 0 else 0.0
            variant = pcfg["variant"]
            run_id = f"AG1FXMTM_{variant}_{now.strftime('%Y%m%d%H%M%S')}"
            snapshot_id = f"SNP_{run_id}"
            notes = {
                "source": "AG1-FX-PF-V1 hourly valuation",
                "priced_lots": priced_lots,
                "missing_price_pairs": sorted(set(missing_prices)),
            }

            con.execute(
                """
                INSERT OR REPLACE INTO core.portfolio_snapshot (
                  snapshot_id, run_id, as_of, cash_eur, equity_eur,
                  margin_used_eur, margin_free_eur, leverage_effective, open_lots_count,
                  pnl_day_eur, pnl_total_eur, drawdown_day_pct, drawdown_total_pct, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    snapshot_id,
                    run_id,
                    now,
                    cash,
                    equity,
                    margin_used,
                    margin_free,
                    leverage_effective,
                    len(lots),
                    equity - day_start,
                    pnl_total,
                    min(0.0, drawdown_day),
                    min(0.0, drawdown_total),
                    json.dumps(notes, ensure_ascii=False),
                ],
            )

            if table_exists(con, "runs", "core"):
                con.execute(
                    """
                    INSERT OR REPLACE INTO core.runs (
                      run_id, llm_model, started_at, finished_at, decision_json, decisions_count,
                      orders_count, fills_count, errors, leverage_max_used, kill_switch_active, notes
                    ) VALUES (?, ?, ?, ?, ?, 0, 0, 0, ?, ?, FALSE, ?)
                    """,
                    [
                        run_id,
                        pcfg["llm_model"],
                        now,
                        now,
                        json.dumps({"type": "valuation_only", "snapshot_id": snapshot_id}, ensure_ascii=False),
                        len(set(missing_prices)),
                        pcfg["leverage_max"],
                        "AG1-FX-PF-V1 hourly valuation",
                    ],
                )

            result.update(
                {
                    "status": "SUCCESS",
                    "run_id": run_id,
                    "snapshot_id": snapshot_id,
                    "variant": variant,
                    "open_lots_count": len(lots),
                    "priced_lots": priced_lots,
                    "missing_price_pairs": sorted(set(missing_prices)),
                    "cash_eur": cash,
                    "floating_pnl_eur": floating,
                    "realized_pnl_eur": realized,
                    "fees_eur": fees,
                    "equity_eur": equity,
                    "pnl_total_eur": pnl_total,
                    "margin_used_eur": margin_used,
                    "margin_free_eur": margin_free,
                    "leverage_effective": leverage_effective,
                    "drawdown_day_pct": min(0.0, drawdown_day),
                    "drawdown_total_pct": min(0.0, drawdown_total),
                }
            )
    except Exception as exc:
        result["error"] = str(exc)

    return result


items = _items or []
cfg = (items[0] or {}).get("json", {}) if items else {}
cfg = cfg if isinstance(cfg, dict) else {}

targets = parse_paths(cfg.get("portfolio_db_paths_json")) or parse_paths(cfg.get("portfolio_db_paths")) or DEFAULT_TARGETS
targets = dedupe(targets)

open_pairs = []
for target in targets:
    db_path = resolve_existing_path(target)
    if not db_path or not os.path.exists(db_path):
        continue
    try:
        with db_con(db_path, read_only=True) as con:
            open_pairs.extend([to_text(l.get("pair")).upper() for l in read_open_lots(con)])
    except Exception:
        pass

pairs_needed = conversion_pairs_for_open_pairs(open_pairs)
prices, price_errors = build_price_map(cfg, pairs_needed)
results = [update_one_portfolio(target, cfg, prices) for target in targets]

status = "SUCCESS"
if any(r.get("status") == "FAILED" for r in results):
    status = "PARTIAL" if any(r.get("status") == "SUCCESS" for r in results) else "FAILED"

return [
    {
        "json": {
            "status": status,
            "workflow": "AG1-FX-PF-V1",
            "as_of": datetime.now(timezone.utc).isoformat(),
            "targets": results,
            "targets_ok": sum(1 for r in results if r.get("status") == "SUCCESS"),
            "targets_failed": sum(1 for r in results if r.get("status") == "FAILED"),
            "prices_count": len(prices),
            "price_errors": price_errors[:50],
        }
    }
]
