import os
import re
import gc
import time
import duckdb
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta, date

DEFAULTS = {
    "ag1_db_path": os.getenv("AG1_DUCKDB_PATH", "/files/duckdb/ag1_v3.duckdb"),
    "ag2_db_path": os.getenv("AG2_DUCKDB_PATH", "/files/duckdb/ag2_v3.duckdb"),
    "ag3_db_path": os.getenv("AG3_DUCKDB_PATH", "/files/duckdb/ag3_v2.duckdb"),
    "ag4_db_path": os.getenv("AG4_DUCKDB_PATH", "/files/duckdb/ag4_v3.duckdb"),
    "ag4_spe_db_path": os.getenv("AG4_SPE_DUCKDB_PATH", "/files/duckdb/ag4_spe_v2.duckdb"),
    "yf_enrich_db_path": os.getenv("YF_ENRICH_DUCKDB_PATH", "/files/duckdb/yf_enrichment_v1.duckdb"),
}
LOOKBACK_NEWS_DAYS = 30


def safe_float(v, default=0.0):
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip().replace(",", ".")
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def parse_dt(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
    elif isinstance(v, date):
        dt = datetime(v.year, v.month, v.day)
    else:
        s = str(v).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = None
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            pass
        if dt is None and re.fullmatch(r"\d{10,13}", s):
            try:
                n = int(s)
                if len(s) == 13:
                    n = n / 1000.0
                dt = datetime.fromtimestamp(n, tz=timezone.utc)
            except Exception:
                dt = None
        if dt is None:
            fmts = (
                "%Y-%m-%d %H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M%z",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
            )
            for fmt in fmts:
                try:
                    dt = datetime.strptime(s, fmt)
                    break
                except Exception:
                    dt = None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def to_iso(v):
    dt = parse_dt(v)
    return dt.isoformat() if dt is not None else ""


def norm_token(v):
    s = str(v or "").strip().lower()
    if s in ("", "n/a", "na", "nan", "none", "unknown", "indefini", "indefinie", "indef"):
        return ""
    return s


def truthy(v):
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "ok")


def normalize_fx_symbol(v):
    s = str(v or "").strip().upper()
    if not s:
        return ""
    if s.startswith("FX:"):
        core = s[3:]
    else:
        core = s.replace("=X", "")
    core = core.replace("/", "").replace("-", "").replace("_", "")
    core = "".join(ch for ch in core if ch.isalpha())
    if len(core) < 6:
        return ""
    return "FX:" + core[:6]


@contextmanager
def db_con(path, retries=5, delay=0.25):
    con = None
    for i in range(retries):
        try:
            con = duckdb.connect(path, read_only=True)
            break
        except Exception as e:
            msg = str(e).lower()
            if ("lock" in msg or "busy" in msg) and i < retries - 1:
                time.sleep(delay * (2 ** i))
            else:
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


def run_query(path, sql, params=None):
    if not path:
        return []
    with db_con(path) as con:
        if con is None:
            return []
        try:
            cur = con.execute(sql, params or [])
            cols = [d[0] for d in (cur.description or [])]
            out = []
            for row in cur.fetchall():
                out.append({cols[i]: row[i] for i in range(len(cols))})
            return out
        except Exception:
            return []


def pick_cfg(items):
    cfg = {}
    for it in (items or []):
        j = it.get("json", {}) if isinstance(it, dict) else {}
        if not isinstance(j, dict):
            continue
        for k in DEFAULTS.keys():
            if j.get(k):
                cfg[k] = str(j.get(k))
    return cfg


items = _items or []
cfg = dict(DEFAULTS)
cfg.update(pick_cfg(items))

now = datetime.now(timezone.utc)
cut30 = now - timedelta(days=LOOKBACK_NEWS_DAYS)
cut7 = now - timedelta(days=7)

universe_rows = run_query(
    cfg["ag2_db_path"],
    """
    SELECT UPPER(TRIM(symbol)) AS symbol,
           UPPER(TRIM(COALESCE(symbol_yahoo, symbol))) AS symbol_yahoo,
           COALESCE(name, '') AS name,
           UPPER(TRIM(COALESCE(asset_class, 'EQUITY'))) AS asset_class,
           COALESCE(sector, '') AS sector,
           COALESCE(industry, '') AS industry,
           UPPER(TRIM(COALESCE(base_ccy, ''))) AS base_ccy,
           UPPER(TRIM(COALESCE(quote_ccy, ''))) AS quote_ccy,
           pip_size
    FROM universe
    WHERE symbol IS NOT NULL AND TRIM(symbol) <> ''
    ORDER BY symbol
    """,
)

tech_rows = run_query(
    cfg["ag2_db_path"],
    """
    SELECT ts.symbol,
           ts.d1_action,
           ts.d1_score,
           ts.d1_rsi14,
           ts.d1_atr_pct,
           ts.d1_resistance,
           ts.d1_support,
           ts.d1_dist_res_pct,
           ts.d1_dist_sup_pct,
           ts.ai_stop_loss,
           ts.ai_rr_theoretical,
           ts.ai_alignment,
           ts.ai_regime_d1,
           ts.last_close,
           ts.data_age_h1_hours,
           ts.data_age_d1_hours,
           COALESCE(ts.workflow_date, ts.updated_at, ts.created_at) AS tech_ts
    FROM technical_signals ts
    INNER JOIN (
      SELECT symbol, MAX(COALESCE(workflow_date, updated_at, created_at)) AS latest_ts
      FROM technical_signals
      GROUP BY symbol
    ) latest
      ON ts.symbol = latest.symbol
     AND COALESCE(ts.workflow_date, ts.updated_at, ts.created_at) = latest.latest_ts
    """,
)

funda_rows = run_query(
    cfg["ag3_db_path"],
    """
    SELECT UPPER(TRIM(symbol)) AS symbol,
           score,
           risk_score,
           upside_pct,
           recommendation,
           target_price,
           horizon,
           COALESCE(updated_at, fetched_at, created_at) AS funda_ts
    FROM v_latest_triage
    ORDER BY symbol
    """,
)

if not funda_rows:
    funda_rows = run_query(
        cfg["ag3_db_path"],
        """
        SELECT * EXCLUDE(rn)
        FROM (
          SELECT UPPER(TRIM(symbol)) AS symbol,
                 score,
                 risk_score,
                 upside_pct,
                 recommendation,
                 target_price,
                 horizon,
                 COALESCE(updated_at, fetched_at, created_at) AS funda_ts,
                 ROW_NUMBER() OVER (
                   PARTITION BY symbol
                   ORDER BY COALESCE(updated_at, fetched_at, created_at) DESC
                 ) AS rn
          FROM fundamentals_triage_history
        )
        WHERE rn = 1
        ORDER BY symbol
        """,
    )

macro_rows = run_query(
    cfg["ag4_db_path"],
    """
    SELECT
      COALESCE(published_at, first_seen_at, analyzed_at, last_seen_at, updated_at, created_at) AS publishedat,
      COALESCE(impact_score, 0) AS impactscore,
      COALESCE(theme, '') AS theme,
      COALESCE(title, '') AS title,
      COALESCE(snippet, '') AS snippet,
      COALESCE(notes, '') AS notes,
      COALESCE(winners, '') AS winners,
      COALESCE(losers, '') AS losers,
      COALESCE(regime, '') AS regime
    FROM news_history
    WHERE COALESCE(type, 'macro') = 'macro'
    ORDER BY publishedat DESC
    """,
)

fx_macro_rows = run_query(
    cfg["ag4_db_path"],
    """
    SELECT
      run_id,
      as_of,
      market_regime,
      drivers,
      confidence,
      usd_bias, eur_bias, jpy_bias, gbp_bias, chf_bias, aud_bias, cad_bias, nzd_bias
    FROM ag4_fx_macro
    ORDER BY as_of DESC
    LIMIT 1
    """,
)

fx_pair_rows = run_query(
    cfg["ag4_db_path"],
    """
    SELECT
      UPPER(TRIM(COALESCE(symbol_internal, pair))) AS symbol_internal,
      UPPER(TRIM(COALESCE(pair, ''))) AS pair,
      UPPER(TRIM(COALESCE(directional_bias, 'NEUTRAL'))) AS directional_bias,
      COALESCE(rationale, '') AS rationale,
      COALESCE(confidence, 0) AS confidence,
      COALESCE(urgent_event_window, FALSE) AS urgent_event_window,
      as_of
    FROM ag4_fx_pairs
    ORDER BY as_of DESC, pair
    """,
)

symbol_news_rows = run_query(
    cfg["ag4_spe_db_path"],
    """
    SELECT
      UPPER(TRIM(symbol)) AS symbol,
      COALESCE(published_at, analyzed_at, fetched_at, updated_at, created_at) AS publishedat,
      COALESCE(impact_score, 0) AS impactscore,
      COALESCE(sentiment, '') AS sentiment,
      COALESCE(urgency, '') AS urgency,
      COALESCE(confidence, 0) AS confidence,
      COALESCE(title, '') AS title,
      COALESCE(summary, '') AS summary
    FROM news_history
    WHERE symbol IS NOT NULL AND TRIM(symbol) <> ''
    ORDER BY publishedat DESC, symbol
    """,
)

if not symbol_news_rows:
    symbol_news_rows = run_query(
        cfg["ag4_db_path"],
        """
        SELECT
          UPPER(TRIM(symbol)) AS symbol,
          COALESCE(published_at, analyzed_at, last_seen_at, updated_at, created_at) AS publishedat,
          COALESCE(impact_score, 0) AS impactscore,
          COALESCE(sentiment, '') AS sentiment,
          COALESCE(urgency, '') AS urgency,
          COALESCE(confidence, 0) AS confidence,
          COALESCE(title, '') AS title,
          COALESCE(summary, '') AS summary
        FROM news_history
        WHERE COALESCE(type, '') = 'symbol'
          AND symbol IS NOT NULL AND TRIM(symbol) <> ''
        ORDER BY publishedat DESC, symbol
        """,
    )

yf_rows = run_query(
    cfg["yf_enrich_db_path"],
    """
    SELECT
      UPPER(TRIM(symbol)) AS symbol,
      regular_market_price,
      spread_pct,
      slippage_proxy_pct,
      iv_atm,
      options_ok,
      options_error,
      options_warning,
      next_earnings_date,
      days_to_earnings,
      fetched_at,
      options_fetched_at
    FROM v_latest_symbol_enrichment
    ORDER BY symbol
    """,
)

if not yf_rows:
    yf_rows = run_query(
        cfg["yf_enrich_db_path"],
        """
        SELECT * EXCLUDE(rn)
        FROM (
          SELECT UPPER(TRIM(symbol)) AS symbol,
                 regular_market_price,
                 spread_pct,
                 slippage_proxy_pct,
                 iv_atm,
                 options_ok,
                 options_error,
                 options_warning,
                 next_earnings_date,
                 days_to_earnings,
                 fetched_at,
                 options_fetched_at,
                 ROW_NUMBER() OVER (
                   PARTITION BY symbol
                   ORDER BY COALESCE(fetched_at, created_at) DESC
                 ) AS rn
          FROM yf_symbol_enrichment_history
        )
        WHERE rn = 1
        ORDER BY symbol
        """,
    )

pf_rows = run_query(
    cfg["ag1_db_path"],
    """
    SELECT UPPER(TRIM(symbol)) AS symbol,
           COALESCE(sector, '') AS sector,
           COALESCE(market_value, 0) AS market_value
    FROM portfolio_positions_mtm_latest
    WHERE symbol IS NOT NULL
      AND UPPER(TRIM(symbol)) NOT IN ('CASH_EUR', '__META__')
    """,
)

sym_weight = {}
sec_weight = {}
total_mv = 0.0
for r in pf_rows:
    total_mv += max(0.0, safe_float(r.get("market_value"), 0.0))
if total_mv > 0:
    for r in pf_rows:
        sym = str(r.get("symbol") or "").strip().upper()
        sec = str(r.get("sector") or "").strip()
        mv = max(0.0, safe_float(r.get("market_value"), 0.0))
        if not sym:
            continue
        pct = (mv / total_mv) * 100.0
        sym_weight[sym] = sym_weight.get(sym, 0.0) + pct
        sec_weight[sec] = sec_weight.get(sec, 0.0) + pct

universe = {}
for r in universe_rows:
    sym = str(r.get("symbol") or "").strip().upper()
    if not sym:
        continue
    universe[sym] = {
        "symbol_yahoo": str(r.get("symbol_yahoo") or sym).strip().upper(),
        "name": str(r.get("name") or "").strip(),
        "asset_class": str(r.get("asset_class") or "EQUITY").strip().upper(),
        "sector": str(r.get("sector") or "").strip(),
        "industry": str(r.get("industry") or "").strip(),
        "base_ccy": str(r.get("base_ccy") or "").strip().upper(),
        "quote_ccy": str(r.get("quote_ccy") or "").strip().upper(),
        "pip_size": safe_float(r.get("pip_size"), 0.0),
    }

tech_map = {}
for r in tech_rows:
    sym = str(r.get("symbol") or "").strip().upper()
    if not sym:
        continue
    prev = tech_map.get(sym)
    cur_ts = parse_dt(r.get("tech_ts"))
    old_ts = parse_dt(prev.get("tech_ts")) if prev else None
    if (prev is None) or (old_ts is None) or (cur_ts is not None and cur_ts >= old_ts):
        tech_map[sym] = dict(r)

funda_map = {}
for r in funda_rows:
    sym = str(r.get("symbol") or "").strip().upper()
    if not sym:
        continue
    prev = funda_map.get(sym)
    cur_ts = parse_dt(r.get("funda_ts"))
    old_ts = parse_dt(prev.get("funda_ts")) if prev else None
    if (prev is None) or (old_ts is None) or (cur_ts is not None and cur_ts >= old_ts):
        funda_map[sym] = dict(r)

yf_map = {}
for r in yf_rows:
    sym = str(r.get("symbol") or "").strip().upper()
    if not sym:
        continue
    prev = yf_map.get(sym)
    cur_ts = parse_dt(r.get("fetched_at"))
    old_ts = parse_dt(prev.get("fetched_at")) if prev else None
    if (prev is None) or (old_ts is None) or (cur_ts is not None and cur_ts >= old_ts):
        yf_map[sym] = dict(r)

sym_news_agg = {}
for r in symbol_news_rows:
    sym = str(r.get("symbol") or "").strip().upper()
    if not sym:
        continue
    dt = parse_dt(r.get("publishedat"))
    impact = safe_float(r.get("impactscore"), 0.0)
    rec = sym_news_agg.get(sym)
    if rec is None:
        rec = {
            "count_7d": 0,
            "count_30d": 0,
            "impact_7d": 0.0,
            "impact_30d": 0.0,
            "last_news_date": None,
        }
        sym_news_agg[sym] = rec
    if dt is not None:
        if rec["last_news_date"] is None or dt > rec["last_news_date"]:
            rec["last_news_date"] = dt
        if dt >= cut30:
            rec["count_30d"] += 1
            rec["impact_30d"] += impact
        if dt >= cut7:
            rec["count_7d"] += 1
            rec["impact_7d"] += impact

macro_recent = []
for r in macro_rows:
    dt = parse_dt(r.get("publishedat"))
    if dt is None or dt < cut30:
        continue
    text = " ".join(
        [
            str(r.get("theme") or ""),
            str(r.get("title") or ""),
            str(r.get("snippet") or ""),
            str(r.get("notes") or ""),
            str(r.get("winners") or ""),
            str(r.get("losers") or ""),
            str(r.get("regime") or ""),
        ]
    ).lower()
    macro_recent.append(
        {
            "publishedat": dt,
            "impactscore": safe_float(r.get("impactscore"), 0.0),
            "theme": str(r.get("theme") or "").strip(),
            "ctx": text,
        }
    )

fx_macro = fx_macro_rows[0] if fx_macro_rows else {}
fx_pair_map = {}
for r in fx_pair_rows:
    sym = normalize_fx_symbol(r.get("symbol_internal") or r.get("pair"))
    if not sym:
        continue
    prev = fx_pair_map.get(sym)
    cur_ts = parse_dt(r.get("as_of"))
    old_ts = parse_dt(prev.get("as_of")) if prev else None
    if prev is None or old_ts is None or (cur_ts is not None and cur_ts >= old_ts):
        fx_pair_map[sym] = dict(r)

symbols = set()
symbols.update(universe.keys())
symbols.update(tech_map.keys())
symbols.update(funda_map.keys())
symbols.update(sym_news_agg.keys())
symbols.update(yf_map.keys())
symbols.update(fx_pair_map.keys())

out = []
for sym in sorted(symbols):
    u = universe.get(sym, {})
    t = tech_map.get(sym, {})
    f = funda_map.get(sym, {})
    sn = sym_news_agg.get(sym, {})
    yf = yf_map.get(sym, {})
    fxp = fx_pair_map.get(normalize_fx_symbol(sym), {})

    asset_class = str(u.get("asset_class") or ("FX" if normalize_fx_symbol(sym) else "EQUITY")).strip().upper()
    symbol_yahoo = str(u.get("symbol_yahoo") or (sym.replace("FX:", "") + "=X" if asset_class == "FX" else sym)).strip().upper()
    base_ccy = str(u.get("base_ccy") or "").strip().upper()
    quote_ccy = str(u.get("quote_ccy") or "").strip().upper()
    pip_size = safe_float(u.get("pip_size"), 0.0)

    if asset_class == "FX":
        pair = normalize_fx_symbol(sym).replace("FX:", "")
        if len(pair) == 6:
            if not base_ccy:
                base_ccy = pair[:3]
            if not quote_ccy:
                quote_ccy = pair[3:]
        if pip_size <= 0:
            pip_size = 0.01 if quote_ccy == "JPY" else 0.0001

    name = str(u.get("name") or "").strip() or (base_ccy + "/" + quote_ccy if asset_class == "FX" and base_ccy and quote_ccy else sym)
    sector = str(u.get("sector") or "").strip()
    industry = str(u.get("industry") or "").strip()

    sec_tok = norm_token(sector)
    ind_tok = norm_token(industry)

    macro_matches = []
    for m in macro_recent:
        ctx = m["ctx"]
        hit = False
        if sec_tok and sec_tok in ctx:
            hit = True
        if ind_tok and ind_tok != sec_tok and ind_tok in ctx:
            hit = True
        if hit:
            macro_matches.append(m)

    macro_impact = sum(m.get("impactscore", 0.0) for m in macro_matches)
    macro_count = len(macro_matches)
    macro_last = max((m.get("publishedat") for m in macro_matches), default=None)

    theme_count = {}
    for m in macro_matches:
        th = str(m.get("theme") or "").strip()
        if not th:
            continue
        theme_count[th] = theme_count.get(th, 0) + 1
    top_themes = [k for k, _ in sorted(theme_count.items(), key=lambda kv: kv[1], reverse=True)[:2]]

    sym_news_last = sn.get("last_news_date")
    last_news = sym_news_last if sym_news_last is not None else macro_last

    out.append(
        {
            "Symbol": sym,
            "Symbol_Yahoo": symbol_yahoo,
            "Name": name,
            "AssetClass": asset_class,
            "Sector": sector,
            "Industry": industry,
            "Base_CCY": base_ccy,
            "Quote_CCY": quote_ccy,
            "Pip_Size": pip_size,
            "Tech_Action": str(t.get("d1_action") or "").upper().strip(),
            "Tech_Confidence": safe_float(t.get("d1_score"), 0.0),
            "Last_Close": safe_float(t.get("last_close"), 0.0),
            "D1_RSI14": safe_float(t.get("d1_rsi14"), 0.0),
            "D1_ATR_Pct": safe_float(t.get("d1_atr_pct"), 0.0),
            "D1_Resistance": safe_float(t.get("d1_resistance"), 0.0),
            "D1_Support": safe_float(t.get("d1_support"), 0.0),
            "D1_Dist_Res_Pct": safe_float(t.get("d1_dist_res_pct"), 0.0),
            "D1_Dist_Sup_Pct": safe_float(t.get("d1_dist_sup_pct"), 0.0),
            "AI_Stop_Loss": safe_float(t.get("ai_stop_loss"), 0.0),
            "AI_RR_Theoretical": safe_float(t.get("ai_rr_theoretical"), 0.0),
            "AI_Alignment": str(t.get("ai_alignment") or "").strip(),
            "AI_Regime_D1": str(t.get("ai_regime_d1") or "").strip(),
            "Data_Age_H1_Hours": safe_float(t.get("data_age_h1_hours"), 0.0),
            "Data_Age_D1_Hours": safe_float(t.get("data_age_d1_hours"), 0.0),
            "Last_Tech_Date": to_iso(t.get("tech_ts")),
            "Funda_Score": safe_float(f.get("score"), 50.0),
            "Funda_Risk": safe_float(f.get("risk_score"), 50.0),
            "Funda_Upside": safe_float(f.get("upside_pct"), 0.0),
            "Recommendation": str(f.get("recommendation") or "").strip(),
            "Target_Price": safe_float(f.get("target_price"), 0.0),
            "Funda_Horizon": str(f.get("horizon") or "").strip(),
            "Last_Funda_Date": to_iso(f.get("funda_ts")),
            "Symbol_News_Count_7d": int(sn.get("count_7d", 0)),
            "Symbol_News_Count_30d": int(sn.get("count_30d", 0)),
            "Symbol_News_Impact_7d": safe_float(sn.get("impact_7d"), 0.0),
            "Symbol_News_Impact_30d": safe_float(sn.get("impact_30d"), 0.0),
            "Symbol_News_Last_Date": to_iso(sym_news_last),
            "Macro_News_Count_30d": int(macro_count),
            "Macro_Impact_30d": safe_float(macro_impact, 0.0),
            "Macro_Last_Date": to_iso(macro_last),
            "Macro_Themes": ", ".join(top_themes),
            "Last_News_Date": to_iso(last_news),
            "Regular_Market_Price": safe_float(yf.get("regular_market_price"), 0.0),
            "SpreadPct": safe_float(yf.get("spread_pct"), 0.0),
            "SlippageProxyPct": safe_float(yf.get("slippage_proxy_pct"), 0.0),
            "IV_ATM": safe_float(yf.get("iv_atm"), 0.0),
            "Options_Ok": truthy(yf.get("options_ok")),
            "Options_Error": str(yf.get("options_error") or "").strip(),
            "Options_Warning": str(yf.get("options_warning") or "").strip(),
            "Next_Earnings_Date": to_iso(yf.get("next_earnings_date")),
            "Days_To_Earnings": yf.get("days_to_earnings"),
            "YF_Fetched_At": to_iso(yf.get("fetched_at")),
            "Sector_Weight_Pct": safe_float(sec_weight.get(sector, 0.0), 0.0),
            "Symbol_Weight_Pct": safe_float(sym_weight.get(sym, 0.0), 0.0),
            "FX_Directional_Bias": str(fxp.get("directional_bias") or "").strip().upper(),
            "FX_Bias_Confidence": safe_float(fxp.get("confidence"), 0.0),
            "FX_Rationale": str(fxp.get("rationale") or "").strip(),
            "FX_Urgent_Event_Window": truthy(fxp.get("urgent_event_window")),
            "FX_As_Of": to_iso(fxp.get("as_of")),
            "FX_Macro_Regime": str(fx_macro.get("market_regime") or "").strip(),
            "FX_Macro_Confidence": safe_float(fx_macro.get("confidence"), 0.0),
            "FX_Macro_As_Of": to_iso(fx_macro.get("as_of")),
        }
    )

if not out:
    return [{"json": {"_empty": True, "_error": "NO_SYMBOLS_AVAILABLE", "config": cfg}}]

return [{"json": r} for r in out]
