import gc
import json
import os
import re
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import duckdb

DEFAULT_TARGETS = [
    "/local-files/duckdb/ag1_v3_chatgpt52.duckdb",
    "/local-files/duckdb/ag1_v3_grok41_reasoning.duckdb",
    "/local-files/duckdb/ag1_v3_gemini30_pro.duckdb",
]
DEFAULT_UNIVERSE_DB_PATH = "/local-files/duckdb/ag2_v3.duckdb"
DEFAULT_UNIVERSE_TABLE = "universe"


def _duckdb_connect_timeout(path, read_only=False, timeout=30):
    """Wrap duckdb.connect() with a timeout to avoid indefinite blocking on file locks."""
    result = [None]
    exc = [None]

    def _connect():
        try:
            result[0] = duckdb.connect(path, read_only=read_only)
        except Exception as e:
            exc[0] = e

    t = threading.Thread(target=_connect, daemon=True)
    t.start()
    t.join(timeout=timeout)
    if t.is_alive():
        raise Exception(f"duckdb lock timeout: {path} verrouille depuis >{timeout}s")
    if exc[0] is not None:
        raise exc[0]
    return result[0]


@contextmanager
def db_con(path, retries=6, base_delay=0.25):
    con = None
    for attempt in range(retries):
        try:
            con = _duckdb_connect_timeout(path, read_only=True, timeout=30)
            break
        except Exception as exc:
            msg = str(exc).lower()
            if ("lock" in msg or "busy" in msg or "timeout" in msg) and attempt < retries - 1:
                time.sleep(base_delay * (2 ** attempt))
                continue
            con = None
            break
    try:
        yield con
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        gc.collect()


def to_text(v):
    if v is None:
        return ""
    s = str(v)
    return "" if s.lower() in ("nan", "nat", "none", "null") else s.strip()


def to_float(v, default=None):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _parse_paths_candidate(v):
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
        parts = re.split(r"[;,]", s.strip().strip("[]"))
        out = []
        for x in parts:
            t = to_text(x).strip().strip('"').strip("'")
            if t:
                out.append(t)
        return out
    return [s]


def dedupe_paths(paths):
    out = []
    seen = set()
    for p in paths:
        t = to_text(p)
        if not t:
            continue
        key = t.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(t)
    return out


def _is_legacy_ag1_db_path(path_text):
    p = to_text(path_text).lower().replace("\\", "/")
    return p.endswith("/ag1_v2.duckdb")


def _path_alias_candidates(path_text):
    p = to_text(path_text).replace("\\", "/")
    if not p:
        return []
    out = [p]
    if p.startswith("/local-files/"):
        out.append("/files/" + p[len("/local-files/"):])
    elif p.startswith("/files/"):
        out.append("/local-files/" + p[len("/files/"):])
    return dedupe_paths(out)


def _resolve_existing_db_path(path_text):
    cands = _path_alias_candidates(path_text)
    if not cands:
        return ""
    for p in cands:
        try:
            if os.path.exists(p):
                return p
        except Exception:
            pass
    return cands[0]


def _norm_symbol(v):
    return to_text(v).upper()


def _is_numeric_like_label(v):
    s = to_text(v)
    if not s:
        return False
    return bool(re.fullmatch(r"[+-]?\d+(?:[.,]\d+)?", s))


def _is_blankish(v):
    s = to_text(v)
    return s == "" or s.lower() in {"nan", "none", "null", "n/a", "na"}


def _needs_name_fill(name_val, symbol):
    s = to_text(name_val)
    if _is_blankish(s):
        return True
    if _is_numeric_like_label(s):
        return True
    return _norm_symbol(s) == _norm_symbol(symbol)


def _needs_taxonomy_fill(v):
    s = to_text(v)
    if _is_blankish(s):
        return True
    if s.strip().lower() in {"unknown", "undef", "undefined"}:
        return True
    if _is_numeric_like_label(s):
        return True
    return False


def resolve_target_paths(cfg):
    paths = []
    paths.extend(_parse_paths_candidate(cfg.get("portfolio_db_paths")))
    paths.extend(_parse_paths_candidate(cfg.get("portfolio_db_paths_json")))
    paths.extend(_parse_paths_candidate(cfg.get("portfolio_db_paths_csv")))
    if not paths:
        paths.extend(_parse_paths_candidate(cfg.get("portfolio_db_path")))
    paths = dedupe_paths(paths)
    paths = [p for p in paths if not _is_legacy_ag1_db_path(p)]
    if not paths:
        paths = list(DEFAULT_TARGETS)
    return paths


def _resolve_universe_db_path(cfg):
    candidates = [
        cfg.get("universe_db_path"),
        cfg.get("ag2_db_path"),
        os.getenv("AG2_DUCKDB_PATH"),
        DEFAULT_UNIVERSE_DB_PATH,
    ]
    for c in candidates:
        s = to_text(c)
        if s:
            return _resolve_existing_db_path(s)
    return _resolve_existing_db_path(DEFAULT_UNIVERSE_DB_PATH)


def _load_universe_map(cfg):
    db_path = _resolve_universe_db_path(cfg)
    table_name = to_text(cfg.get("universe_table")) or DEFAULT_UNIVERSE_TABLE
    if not db_path:
        return {}

    out = {}
    try:
        with db_con(db_path) as con:
            if con is None:
                return {}
            if not _table_exists(con, table_name):
                return {}
            cur = con.execute(
                f"""
                SELECT
                  symbol,
                  name,
                  asset_class,
                  sector,
                  industry,
                  isin
                FROM {table_name}
                """
            )
            cols = [d[0] for d in cur.description]
            for row in cur.fetchall():
                rec = dict(zip(cols, row))
                sym = _norm_symbol(rec.get("symbol"))
                if not sym:
                    continue
                out[sym] = {
                    "name": to_text(rec.get("name")),
                    "asset_class": to_text(rec.get("asset_class")),
                    "sector": to_text(rec.get("sector")),
                    "industry": to_text(rec.get("industry")),
                    "isin": to_text(rec.get("isin")),
                }
    except Exception:
        return {}

    return out


def _enrich_rows_from_universe(rows, universe_map):
    if not rows or not universe_map:
        return rows

    out = []
    for r in rows:
        row = dict(r)
        sym = _norm_symbol(row.get("Symbol"))
        if not sym or sym in {"CASH_EUR", "__META__"}:
            out.append(row)
            continue

        u = universe_map.get(sym) or {}
        if not u:
            out.append(row)
            continue

        if _needs_name_fill(row.get("Name"), sym) and to_text(u.get("name")):
            row["Name"] = to_text(u.get("name"))
        if _needs_taxonomy_fill(row.get("Sector")) and to_text(u.get("sector")):
            row["Sector"] = to_text(u.get("sector"))
        if _needs_taxonomy_fill(row.get("Industry")) and to_text(u.get("industry")):
            row["Industry"] = to_text(u.get("industry"))
        if (_is_blankish(row.get("AssetClass")) or _is_numeric_like_label(row.get("AssetClass"))) and to_text(u.get("asset_class")):
            row["AssetClass"] = to_text(u.get("asset_class"))
        if _is_blankish(row.get("ISIN")) and to_text(u.get("isin")):
            row["ISIN"] = to_text(u.get("isin"))

        out.append(row)

    return out


def _table_exists(con, table_name, schema=None):
    try:
        if schema:
            row = con.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = ? AND table_name = ?
                """,
                [schema, table_name],
            ).fetchone()
        else:
            row = con.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_name = ?
                """,
                [table_name],
            ).fetchone()
        return bool(row and int(row[0] or 0) > 0)
    except Exception:
        return False


def _iso(v):
    try:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        if s.endswith("Z"):
            return s
        if "+" in s[10:] or s.endswith("Z"):
            return s
        # Keep n8n-friendly ISO string, assume UTC if naive.
        return s + "Z" if "T" in s else s
    except Exception:
        return None


def _legacy_rows(con, db_path):
    if not _table_exists(con, "portfolio_positions_mtm_latest"):
        return []
    try:
        cur = con.execute(
            """
            SELECT
              row_number,
              symbol,
              name,
              asset_class,
              sector,
              industry,
              isin,
              quantity,
              avg_price,
              last_price,
              market_value,
              unrealized_pnl,
              updated_at
            FROM portfolio_positions_mtm_latest
            ORDER BY
              CASE
                WHEN UPPER(symbol) = '__META__' THEN 0
                WHEN UPPER(symbol) = 'CASH_EUR' THEN 1
                ELSE 2
              END,
              market_value DESC NULLS LAST,
              symbol
            """
        )
        cols = [d[0] for d in cur.description]
        out = []
        for row in cur.fetchall():
            r = dict(zip(cols, row))
            out.append(
                {
                    "row_number": int(r.get("row_number")) if r.get("row_number") is not None else None,
                    "Symbol": to_text(r.get("symbol")).upper(),
                    "Name": to_text(r.get("name")),
                    "AssetClass": to_text(r.get("asset_class")),
                    "Sector": to_text(r.get("sector")),
                    "Industry": to_text(r.get("industry")),
                    "ISIN": to_text(r.get("isin")),
                    "Quantity": r.get("quantity"),
                    "AvgPrice": r.get("avg_price"),
                    "LastPrice": r.get("last_price"),
                    "MarketValue": r.get("market_value"),
                    "UnrealizedPnL": r.get("unrealized_pnl"),
                    "UpdatedAt": _iso(r.get("updated_at")),
                    "portfolio_db_path": db_path,
                    "portfolio_source": "legacy_mtm_latest",
                }
            )
        return out
    except Exception:
        return []


def _read_initial_capital(con):
    try:
        if _table_exists(con, "portfolio_config", "cfg"):
            row = con.execute(
                """
                SELECT CAST(initial_capital_eur AS DOUBLE)
                FROM cfg.portfolio_config
                WHERE initial_capital_eur IS NOT NULL
                ORDER BY updated_at DESC NULLS LAST
                LIMIT 1
                """
            ).fetchone()
            if row and row[0] is not None:
                return float(row[0])
    except Exception:
        pass

    try:
        if _table_exists(con, "cash_ledger", "core"):
            row = con.execute(
                """
                SELECT CAST(SUM(COALESCE(amount, 0)) AS DOUBLE)
                FROM core.cash_ledger
                WHERE UPPER(COALESCE(type, '')) = 'DEPOSIT'
                """
            ).fetchone()
            if row and row[0] is not None and float(row[0]) > 0:
                return float(row[0])
    except Exception:
        pass

    return 50000.0


def _core_rows(con, db_path):
    needed = (
        _table_exists(con, "portfolio_snapshot", "core")
        and _table_exists(con, "positions_snapshot", "core")
    )
    if not needed:
        return []

    try:
        latest = con.execute(
            """
            SELECT
              ps.run_id,
              ps.ts,
              CAST(ps.cash_eur AS DOUBLE) AS cash_eur
            FROM core.portfolio_snapshot ps
            ORDER BY ps.ts DESC
            LIMIT 1
            """
        ).fetchone()
        if not latest:
            return []
        run_id, snap_ts, cash_eur = latest[0], latest[1], latest[2]
        snap_iso = _iso(snap_ts) or datetime.now(timezone.utc).isoformat()

        pos_cur = con.execute(
            """
            SELECT
              p.symbol,
              COALESCE(i.name, p.symbol) AS name,
              COALESCE(i.asset_class, 'Equity') AS asset_class,
              COALESCE(i.sector, '') AS sector,
              COALESCE(i.industry, '') AS industry,
              COALESCE(i.isin, '') AS isin,
              CAST(p.qty AS DOUBLE) AS quantity,
              CAST(p.avg_cost AS DOUBLE) AS avg_price,
              CAST(p.last_price AS DOUBLE) AS last_price,
              CAST(p.market_value_eur AS DOUBLE) AS market_value,
              CAST(p.unrealized_pnl_eur AS DOUBLE) AS unrealized_pnl
            FROM core.positions_snapshot p
            LEFT JOIN core.instruments i ON i.symbol = p.symbol
            WHERE p.run_id = ?
            ORDER BY p.market_value_eur DESC NULLS LAST, p.symbol
            """,
            [run_id],
        )
        cols = [d[0] for d in pos_cur.description]
        pos_rows = [dict(zip(cols, r)) for r in pos_cur.fetchall()]

        initial_cap = _read_initial_capital(con)

        out = [
            {
                "row_number": 1,
                "Symbol": "__META__",
                "Name": "__META__",
                "AssetClass": "Meta",
                "Sector": "",
                "Industry": "",
                "ISIN": "",
                "Quantity": None,
                "AvgPrice": None,
                "LastPrice": None,
                "MarketValue": initial_cap,
                "UnrealizedPnL": None,
                "UpdatedAt": snap_iso,
                "portfolio_db_path": db_path,
                "portfolio_source": "core_snapshots",
            },
            {
                "row_number": 2,
                "Symbol": "CASH_EUR",
                "Name": "Cash",
                "AssetClass": "Cash",
                "Sector": "Cash",
                "Industry": "Cash",
                "ISIN": "",
                "Quantity": 0.0,
                "AvgPrice": 1.0,
                "LastPrice": 1.0,
                "MarketValue": float(cash_eur or 0.0),
                "UnrealizedPnL": 0.0,
                "UpdatedAt": snap_iso,
                "portfolio_db_path": db_path,
                "portfolio_source": "core_snapshots",
            },
        ]

        idx = 3
        for r in pos_rows:
            out.append(
                {
                    "row_number": idx,
                    "Symbol": to_text(r.get("symbol")).upper(),
                    "Name": to_text(r.get("name")),
                    "AssetClass": to_text(r.get("asset_class")) or "Equity",
                    "Sector": to_text(r.get("sector")),
                    "Industry": to_text(r.get("industry")),
                    "ISIN": to_text(r.get("isin")),
                    "Quantity": r.get("quantity"),
                    "AvgPrice": r.get("avg_price"),
                    "LastPrice": r.get("last_price"),
                    "MarketValue": r.get("market_value"),
                    "UnrealizedPnL": r.get("unrealized_pnl"),
                    "UpdatedAt": snap_iso,
                    "portfolio_db_path": db_path,
                    "portfolio_source": "core_snapshots",
                }
            )
            idx += 1

        return out
    except Exception:
        return []


items = _items or []
cfg = (items[0] or {}).get("json", {}) if items else {}
cfg = cfg if isinstance(cfg, dict) else {}

targets = resolve_target_paths(cfg)
universe_map = _load_universe_map(cfg)
out_rows = []
diag = []

for db_path_cfg in targets:
    db_path = _resolve_existing_db_path(db_path_cfg)
    d = {
        "configured_db_path": db_path_cfg,
        "resolved_db_path": db_path,
        "resolved_path_exists": bool(db_path and os.path.exists(db_path)),
    }
    with db_con(db_path) as con:
        if con is None:
            d["status"] = "CONNECT_FAILED"
            diag.append(d)
            continue

        # AG1-V3 ledger (core.*) is the source of truth for current portfolio state.
        # Legacy MTM table can lag behind after trades, so only use it as fallback.
        rows = _core_rows(con, db_path)
        source = "core_snapshots" if rows else None
        if not rows:
            rows = _legacy_rows(con, db_path)
            if rows:
                source = "legacy_mtm_latest"
        rows = _enrich_rows_from_universe(rows, universe_map)
        d["status"] = "OK" if rows else "NO_ROWS"
        d["rows"] = len(rows)
        d["portfolio_source"] = source or ""
        diag.append(d)

        for r in rows:
            out_rows.append({"json": r})

if not out_rows:
    compact = []
    for d in diag:
        compact.append(
            f"{d.get('configured_db_path')} -> {d.get('resolved_db_path')} "
            f"(exists={d.get('resolved_path_exists')}, status={d.get('status')}, rows={d.get('rows', 0)})"
        )
    hint = " | ".join(compact)[:1800]
    raise RuntimeError(
        "Read Portfolio: aucune ligne lue depuis les bases AG1 dediees. "
        "Verifier les mounts Docker (/local-files vs /files) et la presence des tables core.*. "
        + hint
    )

return out_rows
