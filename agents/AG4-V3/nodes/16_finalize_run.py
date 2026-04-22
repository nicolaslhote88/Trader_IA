import gc
import os
import re
import duckdb
import time
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta

DB_PATH = "/files/duckdb/ag4_v3.duckdb"
AG2_DB_PATH = os.getenv("AG2_DUCKDB_PATH", "/files/duckdb/ag2_v3.duckdb")

CURRENCIES = ["USD", "EUR", "JPY", "GBP", "CHF", "AUD", "CAD", "NZD"]
SAFE_HAVEN = {"USD", "JPY", "CHF"}
RISK_CCY = {"AUD", "NZD", "CAD", "EUR", "GBP"}

KEYWORDS = {
    "USD": ["usd", "dollar", "fed", "treasury", "us yields", "fomc"],
    "EUR": ["eur", "euro", "ecb", "eurozone", "bund"],
    "JPY": ["jpy", "yen", "boj", "japan"],
    "GBP": ["gbp", "sterling", "boe", "uk", "britain"],
    "CHF": ["chf", "franc", "snb", "swiss"],
    "AUD": ["aud", "australia", "rba", "aussie"],
    "CAD": ["cad", "canada", "boc", "loonie", "wticrude", "oil"],
    "NZD": ["nzd", "new zealand", "rbnz", "kiwi"],
}

POS_WORDS = ["strong", "hawkish", "higher", "hot", "beat", "tightening", "resilient", "surprise upside", "rally"]
NEG_WORDS = ["weak", "dovish", "cuts", "slowing", "recession", "downside", "miss", "decline", "selloff"]


@contextmanager
def db_con(path=DB_PATH, retries=5, delay=0.3, read_only=False):
    con = None
    for attempt in range(retries):
        try:
            con = duckdb.connect(path, read_only=read_only)
            break
        except Exception as e:
            if "lock" in str(e).lower() and attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))
            else:
                raise
    try:
        yield con
    finally:
        if con is not None:
            # CHECKPOINT avant close pour libérer les pages orphelines laissées
            # par les INSERT OR REPLACE / UPDATE. Cf. infra/maintenance/defrag_duckdb.py.
            try:
                con.execute("CHECKPOINT")
            except Exception:
                pass
            try:
                con.close()
            except Exception:
                pass
        gc.collect()


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def safe_float(v, d=0.0):
    try:
        if v is None or v == "":
            return d
        return float(v)
    except Exception:
        return d


def parse_ts(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
    else:
        s = str(v).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def norm_regime(v):
    s = str(v or "").strip().lower()
    if "risk-on" in s or "risk on" in s:
        return "Risk-On"
    if "risk-off" in s or "risk off" in s:
        return "Risk-Off"
    return "Neutral"


def normalize_pair6(v):
    s = str(v or "").upper()
    s = s.replace("FX:", "").replace("=X", "").replace("/", "").replace("-", "").replace("_", "")
    s = "".join(ch for ch in s if ch.isalpha())
    if len(s) < 6:
        return ""
    return s[:6]


def score_text_sentiment(text):
    t = text.lower()
    pos = sum(1 for w in POS_WORDS if w in t)
    neg = sum(1 for w in NEG_WORDS if w in t)
    if pos > neg:
        return 1.0
    if neg > pos:
        return -1.0
    return 0.0


def parse_ccy_list(value):
    out = []
    seen = set()
    if value is None:
        return out
    if isinstance(value, list):
        raw = value
    else:
        raw = str(value).split(",")
    for v in raw:
        ccy = str(v or "").strip().upper()
        ccy = "".join(ch for ch in ccy if ch.isalpha())[:3]
        if ccy in CURRENCIES and ccy not in seen:
            seen.add(ccy)
            out.append(ccy)
    return out


def extract_recent_macro(con, run_id):
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    rows = con.execute(
        """
        SELECT
          COALESCE(published_at, analyzed_at, first_seen_at, last_seen_at, updated_at, created_at) AS ts,
          COALESCE(regime, 'Neutral') AS regime,
          COALESCE(theme, '') AS theme,
          COALESCE(impact_score, 0) AS impact_score,
          COALESCE(urgency, 'low') AS urgency,
          COALESCE(title, '') AS title,
          COALESCE(snippet, '') AS snippet,
          COALESCE(notes, '') AS notes,
          COALESCE(currencies_bullish, '') AS currencies_bullish,
          COALESCE(currencies_bearish, '') AS currencies_bearish
        FROM news_history
        WHERE run_id = ?
          AND COALESCE(type, 'macro') = 'macro'
          AND COALESCE(analyzed_at, first_seen_at, published_at, updated_at, created_at) >= ?
        ORDER BY ts DESC
        """,
        [run_id, cutoff],
    ).fetchall()
    out = []
    for r in rows:
        ts = parse_ts(r[0])
        if ts is None:
            continue
        out.append(
            {
                "ts": ts,
                "regime": norm_regime(r[1]),
                "theme": str(r[2] or "").strip(),
                "impact": safe_float(r[3], 0.0),
                "urgency": str(r[4] or "low").strip().lower(),
                "text": " ".join([str(r[5] or ""), str(r[6] or ""), str(r[7] or "")]).strip(),
                "currencies_bullish": parse_ccy_list(r[8]),
                "currencies_bearish": parse_ccy_list(r[9]),
            }
        )
    return out


def derive_fx_macro(rows):
    if not rows:
        now = datetime.now(timezone.utc)
        return {
            "as_of": now,
            "market_regime": "Neutral",
            "drivers": "",
            "confidence": 20.0,
            "bias": {ccy: 0.0 for ccy in CURRENCIES},
            "urgent": False,
        }

    regime_votes = {"Risk-On": 0.0, "Risk-Off": 0.0, "Neutral": 0.0}
    theme_counts = {}
    bias = {ccy: 0.0 for ccy in CURRENCIES}
    urgent = False

    for row in rows:
        impact = clamp(abs(safe_float(row.get("impact"), 0.0)), 0.0, 10.0)
        w = max(0.2, impact / 5.0)
        regime = row.get("regime", "Neutral")
        regime_votes[regime] = regime_votes.get(regime, 0.0) + w

        theme = str(row.get("theme") or "").strip()
        if theme:
            theme_counts[theme] = theme_counts.get(theme, 0) + 1

        txt = str(row.get("text") or "").lower()
        sent = score_text_sentiment(txt)
        if row.get("urgency") in ("high", "immediate"):
            urgent = True

        # Base regime contribution.
        if regime == "Risk-Off":
            for c in SAFE_HAVEN:
                bias[c] += 0.35 * w
            for c in RISK_CCY:
                bias[c] -= 0.2 * w
        elif regime == "Risk-On":
            for c in SAFE_HAVEN:
                bias[c] -= 0.15 * w
            for c in RISK_CCY:
                bias[c] += 0.3 * w

        # Currency-specific mentions.
        for ccy, kws in KEYWORDS.items():
            hits = sum(1 for kw in kws if kw in txt)
            if hits <= 0:
                continue
            delta = hits * (0.3 + 0.15 * sent) * w
            bias[ccy] += delta

        # Direct LLM FX stance from AG4 parse (stronger than keyword hints).
        for ccy in row.get("currencies_bullish", []):
            bias[ccy] += 0.8 * w
        for ccy in row.get("currencies_bearish", []):
            bias[ccy] -= 0.8 * w

    market_regime = max(regime_votes.items(), key=lambda kv: kv[1])[0]
    drivers = ", ".join([k for k, _ in sorted(theme_counts.items(), key=lambda kv: kv[1], reverse=True)[:5]])

    # Normalize to [-2, +2]
    max_abs = max([abs(v) for v in bias.values()] + [1.0])
    for c in list(bias.keys()):
        bias[c] = round(clamp((bias[c] / max_abs) * 2.0, -2.0, 2.0), 3)

    confidence = clamp(30.0 + min(40.0, len(rows) * 4.0) + min(30.0, sum(abs(v) for v in bias.values()) * 2.0), 0.0, 100.0)
    as_of = max([r["ts"] for r in rows])
    return {
        "as_of": as_of,
        "market_regime": market_regime,
        "drivers": drivers,
        "confidence": round(confidence, 2),
        "bias": bias,
        "urgent": urgent,
    }


def load_fx_universe():
    pairs = []
    try:
        with db_con(AG2_DB_PATH, read_only=True) as con:
            rows = con.execute(
                """
                SELECT
                  COALESCE(symbol_internal, symbol) AS symbol_internal,
                  COALESCE(symbol_yahoo, symbol) AS symbol_yahoo,
                  COALESCE(base_ccy, '') AS base_ccy,
                  COALESCE(quote_ccy, '') AS quote_ccy
                FROM universe
                WHERE UPPER(COALESCE(asset_class, '')) = 'FX'
                  AND COALESCE(enabled, TRUE) = TRUE
                ORDER BY symbol
                """
            ).fetchall()
            for r in rows:
                symbol_internal = str(r[0] or "").strip().upper()
                symbol_yahoo = str(r[1] or "").strip().upper()
                base = str(r[2] or "").strip().upper()
                quote = str(r[3] or "").strip().upper()
                pair = normalize_pair6(symbol_internal or symbol_yahoo)
                if not pair:
                    continue
                if not base:
                    base = pair[:3]
                if not quote:
                    quote = pair[3:]
                pairs.append(
                    {
                        "pair": pair,
                        "symbol_internal": f"FX:{pair}",
                        "symbol_yahoo": f"{pair}=X",
                        "base_ccy": base,
                        "quote_ccy": quote,
                    }
                )
    except Exception:
        return []
    return pairs


def build_pair_rows(run_id, macro):
    pairs = load_fx_universe()
    out = []
    bias = macro["bias"]
    for p in pairs:
        base = p["base_ccy"]
        quote = p["quote_ccy"]
        score = safe_float(bias.get(base), 0.0) - safe_float(bias.get(quote), 0.0)
        if score >= 0.4:
            directional_bias = "BUY_BASE"
        elif score <= -0.4:
            directional_bias = "SELL_BASE"
        else:
            directional_bias = "NEUTRAL"

        conf = clamp(abs(score) / 2.0 * macro["confidence"], 0.0, 100.0)
        rationale = f"{base}({bias.get(base, 0):+.2f}) vs {quote}({bias.get(quote, 0):+.2f})"

        out.append(
            {
                "id": f"{run_id}|FX:{p['pair']}",
                "run_id": run_id,
                "pair": p["pair"],
                "symbol_internal": p["symbol_internal"],
                "directional_bias": directional_bias,
                "rationale": rationale,
                "confidence": round(conf, 2),
                "urgent_event_window": bool(macro.get("urgent")),
                "as_of": macro["as_of"],
            }
        )
    return out


items = _items or []
run_id = ""
db_path = DB_PATH

for it in items:
    j = it.get("json", {}) or {}
    if not run_id:
        run_id = str(j.get("run_id", "") or "")
    if j.get("db_path"):
        db_path = str(j.get("db_path"))

with db_con(db_path) as con:
    if not run_id:
        rr = con.execute("SELECT run_id FROM run_log WHERE status='RUNNING' ORDER BY started_at DESC LIMIT 1").fetchone()
        run_id = str(rr[0]) if rr and rr[0] else ""

    if not run_id:
        return [{"json": {"status": "NO_RUN", "run_id": "", "db_path": db_path}}]

    r = con.execute("SELECT COALESCE(sources_total, 0) FROM run_log WHERE run_id = ?", [run_id]).fetchone()
    sources_total = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(DISTINCT COALESCE(feed_url, dedupe_key)) FROM news_errors WHERE run_id = ?", [run_id]).fetchone()
    feeds_error = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(*) FROM news_errors WHERE run_id = ?", [run_id]).fetchone()
    errors_logged = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(*) FROM news_history WHERE run_id = ?", [run_id]).fetchone()
    items_total = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(*) FROM news_history WHERE run_id = ? AND action = 'analyze'", [run_id]).fetchone()
    items_analyzed = int(r[0]) if r else 0

    r = con.execute("SELECT COUNT(*) FROM news_history WHERE run_id = ? AND action = 'skip'", [run_id]).fetchone()
    items_skipped = int(r[0]) if r else 0

    feeds_ok = max(sources_total - feeds_error, 0)

    if items_total == 0 and sources_total == 0:
        status = "NO_DATA"
    elif feeds_error == 0:
        status = "SUCCESS"
    elif items_total > 0:
        status = "PARTIAL"
    else:
        status = "FAILED"

    # Build AG4 FX outputs.
    macro_rows = extract_recent_macro(con, run_id)
    fx_macro = derive_fx_macro(macro_rows)
    pair_rows = build_pair_rows(run_id, fx_macro)

    con.execute(
        """
        INSERT OR REPLACE INTO ag4_fx_macro (
          run_id, as_of, market_regime, drivers, confidence,
          usd_bias, eur_bias, jpy_bias, gbp_bias, chf_bias, aud_bias, cad_bias, nzd_bias,
          bias_json, source_window_days, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 7, CURRENT_TIMESTAMP)
        """,
        [
            run_id,
            fx_macro["as_of"],
            fx_macro["market_regime"],
            fx_macro["drivers"],
            fx_macro["confidence"],
            fx_macro["bias"].get("USD", 0.0),
            fx_macro["bias"].get("EUR", 0.0),
            fx_macro["bias"].get("JPY", 0.0),
            fx_macro["bias"].get("GBP", 0.0),
            fx_macro["bias"].get("CHF", 0.0),
            fx_macro["bias"].get("AUD", 0.0),
            fx_macro["bias"].get("CAD", 0.0),
            fx_macro["bias"].get("NZD", 0.0),
            str(fx_macro["bias"]),
        ],
    )

    for row in pair_rows:
        con.execute(
            """
            INSERT OR REPLACE INTO ag4_fx_pairs (
              id, run_id, pair, symbol_internal, directional_bias, rationale,
              confidence, urgent_event_window, as_of, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                row["id"],
                row["run_id"],
                row["pair"],
                row["symbol_internal"],
                row["directional_bias"],
                row["rationale"],
                row["confidence"],
                row["urgent_event_window"],
                row["as_of"],
            ],
        )

    con.execute(
        """
        UPDATE run_log
        SET finished_at = CURRENT_TIMESTAMP,
            status = ?,
            feeds_ok = ?,
            feeds_error = ?,
            items_total = ?,
            items_analyzed = ?,
            items_skipped = ?,
            errors_logged = ?,
            error_detail = ?
        WHERE run_id = ?
        """,
        [status, feeds_ok, feeds_error, items_total, items_analyzed, items_skipped, errors_logged, None, run_id],
    )

return [
    {
        "json": {
            "run_id": run_id,
            "db_path": db_path,
            "status": status,
            "sources_total": sources_total,
            "feeds_ok": feeds_ok,
            "feeds_error": feeds_error,
            "items_total": items_total,
            "items_analyzed": items_analyzed,
            "items_skipped": items_skipped,
            "errors_logged": errors_logged,
            "fx_macro_generated": True,
            "fx_macro_asof": fx_macro["as_of"].isoformat() if fx_macro.get("as_of") else None,
            "fx_pairs_count": len(pair_rows),
        }
    }
]
