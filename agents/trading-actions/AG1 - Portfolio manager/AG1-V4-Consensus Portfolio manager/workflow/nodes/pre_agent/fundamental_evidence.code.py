"""Shared, read-only AG3 evidence contract; embedded verbatim in the n8n digest.

Scores are screening heuristics. Unknown source periods/publication dates remain
unknown. A collection timestamp must never masquerade as an accounting date.
"""
import json
import math
import duckdb
from datetime import datetime, timezone, timedelta

FUNDAMENTAL_SCHEMA = "AG3_EVIDENCE_V1"
FUNDAMENTAL_METHOD = "ag3_v2_units_nulls_20261003"
FUNDAMENTAL_METRICS = {
    "revenue_growth_pct": ("growth_json", "revenueGrowth", 100, "%"),
    "earnings_growth_pct": ("growth_json", "earningsGrowth", 100, "%"),
    "operating_margin_pct": ("profitability_json", "operatingMargins", 100, "%"),
    "net_margin_pct": ("profitability_json", "profitMargins", 100, "%"),
    "roe_pct": ("profitability_json", "returnOnEquity", 100, "%"),
    "forward_pe": ("valuation_json", "forwardPE", 1, "x"),
    "trailing_pe": ("valuation_json", "trailingPE", 1, "x"),
    "debt_equity_pct": ("financial_health_json", "debtToEquity", 1, "%"),
    "current_ratio": ("financial_health_json", "currentRatio", 1, "x"),
}
FUNDAMENTAL_LEGEND = {
    "schema": FUNDAMENTAL_SCHEMA,
    "use": "Evidence for reasoning; scores are not probabilities. Do not double-count the fundamental contribution already included in the matrix.",
    "risk": "Fundamental balance-sheet/profitability screening, not probability of price loss.",
    "targets": "Analyst opinions, not predictive quantiles or guaranteed bounds; horizon indicative 12 months, not tactical take-profit instructions.",
    "dates": "observed_at is collection time. Accounting periods and publication dates are unknown unless explicitly provided.",
    "units": {k: v[3] for k, v in FUNDAMENTAL_METRICS.items()},
    "changes": "Target revisions compare collected snapshots; score deltas require identical method versions. Missing baselines remain null.",
    "predictive": "NOT_VALIDATED; no predictive probabilities supplied.",
}


def fctx_num(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def fctx_dt(value):
    if value is None:
        return None
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def fctx_json(value):
    if isinstance(value, dict):
        return value
    try:
        result = json.loads(value or "{}")
        return result if isinstance(result, dict) else {}
    except (ValueError, TypeError):
        return {}


def fctx_card(row, snapshot, consensus, history, peers, now=None):
    now = fctx_dt(now) or datetime.now(timezone.utc)
    observed = fctx_dt(row.get("fetched_at") or row.get("updated_at"))
    age = (now - observed).total_seconds() / 3600 if observed else None
    same_snapshot = bool(snapshot) and snapshot.get("run_id") == row.get("run_id")
    if not same_snapshot:
        snapshot = {}
    blocks = {key: fctx_json(snapshot.get(key)) for key in
              ["profile_json", "price_json", "valuation_json", "growth_json", "profitability_json", "financial_health_json"]}
    metrics = {}
    for key, (block, field, scale, unit) in FUNDAMENTAL_METRICS.items():
        value = fctx_num(blocks[block].get(field))
        metrics[key] = round(value * scale, 3) if value is not None else None
    fcf = fctx_num(blocks["profitability_json"].get("freeCashflow"))
    cap = fctx_num(blocks["price_json"].get("marketCap"))
    metrics["fcf_yield_pct"] = round(100 * fcf / cap, 3) if fcf is not None and cap and cap > 0 else None
    scores = {name: fctx_num(row.get(name + "_score")) for name in ["quality", "growth", "valuation", "health", "consensus"]}
    flags = []
    if row.get("status") != "OK":
        flags.append("SOURCE_ERROR")
    if age is None or age > 168 or age < -1:
        flags.append("STALE_OR_INVALID_DATE")
    if not same_snapshot:
        flags.append("SNAPSHOT_UNAVAILABLE")
    if row.get("strategy_version") != FUNDAMENTAL_METHOD:
        flags.append("LEGACY_SCORE_METHOD")
    missing = [key for key, value in metrics.items() if value is None]
    if missing:
        flags.append("MISSING_METRICS")
    coverage = round(100 * (len(metrics) - len(missing)) / len(metrics), 1)
    if coverage < 60:
        flags.append("LOW_METRIC_COVERAGE")
    # All comparisons use observations no later than the subject's observation.
    changes = {}
    for days in [30, 90]:
        cutoff = observed - timedelta(days=days) if observed else None
        candidates = [x for x in history if cutoff and fctx_dt(x.get("updated_at")) and
                      cutoff - timedelta(days=15) <= fctx_dt(x["updated_at"]) <= cutoff]
        base = max(candidates, key=lambda x: fctx_dt(x["updated_at"])) if candidates else None
        change = None
        if base:
            prev = fctx_num(base.get("target_price")); current = fctx_num(row.get("target_price"))
            same_method = base.get("strategy_version") == row.get("strategy_version")
            old_score = fctx_num(base.get("score")); score = fctx_num(row.get("score"))
            change = {"baseline_at": str(base["updated_at"]),
                      "target_revision_pct": round(100 * (current / prev - 1), 2) if prev and prev > 0 and current and current > 0 else None,
                      "score_delta": round(score - old_score, 2) if same_method and score is not None and old_score is not None else None,
                      "same_method": same_method}
        changes[str(days) + "d"] = change
    targets = {}
    if consensus and consensus.get("run_id") == row.get("run_id"):
        for name in ["low", "mean", "high"]:
            value = fctx_num(consensus.get("target_" + name + "_price"))
            targets[name] = value if value and value > 0 else None
    analysts = fctx_num(row.get("analyst_count"))
    if not analysts or analysts < 3:
        flags.append("THIN_OR_MISSING_CONSENSUS")
    valid_peers = [x for x in peers if x.get("symbol") != row.get("symbol") and x.get("sector") == row.get("sector") and
                   x.get("strategy_version") == row.get("strategy_version") and x.get("status") == "OK" and
                   (fctx_num(x.get("data_coverage_pct")) or 0) >= 60 and fctx_dt(x.get("fetched_at")) and
                   0 <= (now - fctx_dt(x["fetched_at"])).total_seconds() <= 168 * 3600 and fctx_num(x.get("score")) is not None]
    score = fctx_num(row.get("score"))
    peer_info = {"n": len(valid_peers), "triage_percentile": None}
    if len(valid_peers) >= 8 and score is not None and "STALE_OR_INVALID_DATE" not in flags:
        peer_info["triage_percentile"] = round(100 * sum(float(x["score"]) <= score for x in valid_peers) / len(valid_peers), 1)
    strengths = [k for k, v in scores.items() if v is not None and v >= 70]
    weaknesses = [k for k, v in scores.items() if v is not None and v < 40]
    return {
        "schema": FUNDAMENTAL_SCHEMA, "method": row.get("strategy_version"),
        "observed_at": observed.isoformat() if observed else None,
        "age_h": round(age, 1) if age is not None else None,
        "accounting_period": None, "published_at": None,
        "source": "yfinance_api", "source_url": row.get("source_url"),
        "currency": blocks["profile_json"].get("currency") or None,
        "scores": scores, "metrics": metrics, "metric_coverage_pct": coverage,
        "missing": missing, "flags": flags, "strong_dimensions": strengths,
        "weak_dimensions": weaknesses, "changes": changes, "sector_reference": peer_info,
        "analyst_targets": targets, "analysts": analysts,
        "predictive_status": "NOT_VALIDATED",
    }


def load_fundamental_context(db_path, symbols=None, now=None):
    """Bounded read-only queries, same-run joins, no artificial defaults for facts."""
    symbols = sorted(set(str(s).strip().upper() for s in (symbols or []) if str(s).strip()))
    con = duckdb.connect(db_path, read_only=True)
    try:
        def rows(sql, params=None):
            cursor = con.execute(sql, params or [])
            names = [x[0] for x in cursor.description]
            return [dict(zip(names, values)) for values in cursor.fetchall()]
        latest = rows("SELECT * FROM v_latest_triage")
        selected = [x for x in latest if not symbols or x["symbol"] in symbols]
        if not selected:
            return {}
        wanted = [x["symbol"] for x in selected]
        where = ",".join("?" for _ in wanted)
        snapshots = rows("SELECT * FROM fundamentals_snapshot WHERE symbol IN (" + where + ") QUALIFY row_number() OVER(PARTITION BY symbol ORDER BY fetched_at DESC)=1", wanted)
        consensus = rows("SELECT * FROM v_latest_consensus WHERE symbol IN (" + where + ")", wanted)
        history = rows("SELECT symbol, CAST(updated_at AS VARCHAR) AS updated_at, score, target_price, strategy_version FROM fundamentals_triage_history WHERE symbol IN (" + where + ") AND updated_at >= CURRENT_TIMESTAMP - INTERVAL '120 days'", wanted)
        smap = {x["symbol"]: x for x in snapshots}; cmap = {x["symbol"]: x for x in consensus}
        hmap = {}
        for item in history:
            hmap.setdefault(item["symbol"], []).append(item)
        return {x["symbol"]: fctx_card(x, smap.get(x["symbol"], {}), cmap.get(x["symbol"], {}), hmap.get(x["symbol"], []), latest, now) for x in selected}
    finally:
        con.close()


items = _items or []
for item in items:
    body = item.get("json", {})
    pack = body.get("opportunity_pack")
    if not isinstance(pack, dict):
        continue
    selected = pack.get("rows") or []
    symbols = [row.get("symbol") for row in selected if row.get("symbol")]
    cards = load_fundamental_context("/files/duckdb/ag3_v2.duckdb", symbols) if symbols else {}
    for row in selected:
        row["fundamentals"] = cards.get(str(row.get("symbol") or "").upper(), {"schema": FUNDAMENTAL_SCHEMA, "flags": ["MISSING_FUNDA"], "predictive_status": "NOT_VALIDATED"})
    pack["fundamental_legend"] = FUNDAMENTAL_LEGEND
return items
