import json
import os
import math
import time
from contextlib import contextmanager

import duckdb

ctx = (_items or [{"json": {}}])[0].get("json", {})
path = ctx.get("ag3_fx_path") or "/files/duckdb/ag3_fx_v1.duckdb"

rows = []


def is_retryable_duckdb_error(exc):
    msg = str(exc).lower()
    return any(token in msg for token in ("lock", "locked", "conflict", "busy", "timeout"))


@contextmanager
def duckdb_connect_retry(db_path, read_only=True, attempts=10, base_delay=0.35):
    con = None
    for attempt in range(attempts):
        try:
            con = duckdb.connect(db_path, read_only=read_only)
            break
        except Exception as exc:
            if is_retryable_duckdb_error(exc) and attempt < attempts - 1:
                time.sleep(base_delay * (1.7 ** attempt))
                continue
            raise
    try:
        yield con
    finally:
        if con is not None:
            con.close()


def json_safe(value):
    if value is None:
        return None
    try:
        if isinstance(value, float) and not math.isfinite(value):
            return None
    except Exception:
        pass
    try:
        isoformat = value.isoformat
    except Exception:
        return value
    try:
        return isoformat()
    except Exception:
        return str(value)


if os.path.exists(path):
    try:
        with duckdb_connect_retry(path, read_only=True) as con:
            cur = con.execute(
                """
                SELECT pair, payload_json
                FROM main.v_ag3_fx_ag1_summary
                ORDER BY pair
                """
            )
            cols = [d[0] for d in cur.description]
            rows = [
                {col: json_safe(value) for col, value in zip(cols, row)}
                for row in cur.fetchall()
            ]
    except Exception as exc:
        ctx["fundamental_fx_error"] = str(exc)
else:
    ctx["fundamental_fx_error"] = "AG3_FX_DB_MISSING"

fundamental_fx = {}
for r in rows:
    pair = str(r.get("pair") or "").upper()
    if not pair:
        continue
    try:
        fundamental_fx[pair] = json.loads(r.get("payload_json") or "{}")
    except Exception:
        fundamental_fx[pair] = {"error": "INVALID_PAYLOAD_JSON"}

macro_db_path = ctx.get("macro_duckdb_path", os.environ.get("MACRO_DUCKDB_PATH", "/files/duckdb/macro_data.duckdb"))
threshold = ctx.get("three_pillars_threshold", 0.20)
core_g8 = ["USD", "EUR", "JPY", "GBP", "CHF", "CAD", "AUD", "NZD"]
extended_watch = ["MXN", "SEK", "NOK", "KRW"]
pillar_scores_by_ccy = {}
yield_curves = {}
cot_by_ccy = {}

if os.path.exists(macro_db_path):
    try:
        with duckdb_connect_retry(macro_db_path, read_only=True) as con:
            try:
                rows = con.execute(
                    """SELECT DISTINCT ON (currency) *
                       FROM pillars.currency_scores
                       ORDER BY currency, as_of DESC"""
                ).fetchdf()
                for _, r in rows.iterrows():
                    ccy = str(r.get("currency") or "").upper()
                    pillar_scores_by_ccy[ccy] = {
                        "macro_score": json_safe(r.get("macro_score")),
                        "valuation_score": json_safe(r.get("valuation_score")),
                        "positioning_score": json_safe(r.get("positioning_score")),
                        "composite_score": json_safe(r.get("composite_score")),
                        "crowded_flag": bool(r.get("crowded_flag", False)),
                        "all_pillars_aligned": bool(r.get("all_pillars_aligned", False)),
                        "data_completeness": json_safe(r.get("data_completeness", "complete")),
                        "score_status": json_safe(r.get("score_status", "scored")),
                        "confidence_floor": json_safe(r.get("confidence_floor")),
                        "missing_inputs": json_safe(r.get("missing_inputs")),
                        "as_of": str(r.get("as_of", "")),
                    }
            except Exception as exc:
                ctx["three_pillars_scores_error"] = str(exc)

            try:
                rate_rows = con.execute(
                    """SELECT DISTINCT ON (currency) *
                       FROM rates.yield_curve
                       ORDER BY currency, as_of DESC"""
                ).fetchdf()
                for _, r in rate_rows.iterrows():
                    ccy = str(r.get("currency") or "").upper()
                    yield_curves[ccy] = {
                        "yield_2y_pct": json_safe(r.get("yield_2y_pct")),
                        "yield_10y_pct": json_safe(r.get("yield_10y_pct")),
                        "slope_10y2y": json_safe(r.get("slope_10y2y")),
                        "slope_change_30d": json_safe(r.get("slope_change_30d")),
                        "steepening": bool(r.get("steepening", False)),
                        "rates_signal": json_safe(r.get("rates_signal", "neutral")),
                    }
            except Exception as exc:
                ctx["three_pillars_rates_error"] = str(exc)

            try:
                cot_rows = con.execute(
                    """SELECT DISTINCT ON (currency) *
                       FROM cot.speculative_positions
                       ORDER BY currency, report_date DESC"""
                ).fetchdf()
                for _, r in cot_rows.iterrows():
                    ccy = str(r.get("currency") or "").upper()
                    cot_by_ccy[ccy] = {
                        "net_spec": json_safe(r.get("net_spec")),
                        "net_z_score": json_safe(r.get("net_z_score")),
                        "crowded_flag": bool(r.get("crowded_flag", False)),
                        "crowded_direction": json_safe(r.get("crowded_direction", "neutral")),
                        "positioning_score": json_safe(r.get("positioning_score")),
                        "source": json_safe(r.get("source", "CFTC_COT")),
                        "confidence": json_safe(r.get("confidence", "high")),
                        "report_date": str(r.get("report_date", "")),
                    }
            except Exception as exc:
                ctx["three_pillars_cot_error"] = str(exc)
    except Exception as exc:
        ctx["three_pillars_load_error"] = str(exc)
else:
    ctx["three_pillars_load_error"] = "MACRO_DATA_DB_MISSING"

three_pillars = {
    "threshold": threshold,
    "by_currency": {},
    "opportunities": [],
    "crowded_alerts": [],
    "data_available": bool(pillar_scores_by_ccy),
}

universe_ccys = set(core_g8 + extended_watch)
for row in ctx.get("universe_fx", []) or []:
    pair = str(row.get("pair") or "").upper()
    if len(pair) >= 6:
        universe_ccys.add(pair[:3])
        universe_ccys.add(pair[3:6])

for ccy in sorted(universe_ccys | set(pillar_scores_by_ccy) | set(yield_curves) | set(cot_by_ccy)):
    p = pillar_scores_by_ccy.get(ccy, {})
    y = yield_curves.get(ccy, {})
    c = cot_by_ccy.get(ccy, {})
    composite = p.get("composite_score")
    aligned = bool(p.get("all_pillars_aligned", False))
    three_pillars["by_currency"][ccy] = {
        "macro_score": p.get("macro_score"),
        "valuation_score": p.get("valuation_score"),
        "positioning_score": p.get("positioning_score") if p.get("positioning_score") is not None else c.get("positioning_score"),
        "composite_score": composite,
        "all_pillars_aligned": aligned,
        "data_completeness": p.get("data_completeness", "data_incomplete" if not p else "complete"),
        "score_status": p.get("score_status", "data_incomplete" if not p else "scored"),
        "confidence_floor": p.get("confidence_floor"),
        "missing_inputs": p.get("missing_inputs"),
        "crowded_flag": c.get("crowded_flag", False),
        "crowded_direction": c.get("crowded_direction", "neutral"),
        "cot_z_score": c.get("net_z_score"),
        "positioning_source": c.get("source"),
        "positioning_confidence": c.get("confidence"),
        "yield_slope": y.get("slope_10y2y"),
        "rates_signal": y.get("rates_signal", "neutral"),
    }
    if aligned and composite is not None:
        three_pillars["opportunities"].append({
            "currency": ccy,
            "direction": "bullish" if float(composite) > 0 else "bearish",
            "composite_score": composite,
        })
    if c.get("crowded_flag"):
        three_pillars["crowded_alerts"].append({
            "currency": ccy,
            "direction": c.get("crowded_direction"),
            "z_score": c.get("net_z_score"),
        })

three_pillars["opportunities"].sort(key=lambda x: abs(float(x.get("composite_score") or 0)), reverse=True)

return [{"json": {**ctx, "fundamental_fx": fundamental_fx, "three_pillars": three_pillars, "yield_curves": yield_curves}}]
