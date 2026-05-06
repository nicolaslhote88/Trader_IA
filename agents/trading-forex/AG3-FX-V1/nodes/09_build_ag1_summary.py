import json
from datetime import datetime, timezone

ctx = (_items or [{"json": {}}])[0].get("json", {})
run_id = ctx.get("run_id") or ""
as_of = ctx.get("as_of") or datetime.now(timezone.utc).isoformat()
pair_scores = {str(x.get("pair") or "").upper(): x for x in (ctx.get("pair_fundamental_scores") or [])}
targets = {str(x.get("pair") or "").upper(): x for x in (ctx.get("equilibrium_targets") or [])}
currency_scores = {str(x.get("currency") or "").upper(): x for x in (ctx.get("currency_scores") or [])}


def parse_json_list(value):
    if isinstance(value, list):
        return value
    try:
        obj = json.loads(value or "[]")
        return obj if isinstance(obj, list) else []
    except Exception:
        return []


def num(value):
    try:
        return None if value is None else float(value)
    except Exception:
        return None


summaries = []
for pair, score in sorted(pair_scores.items()):
    target = targets.get(pair) or {}
    base = str(score.get("base_ccy") or pair[:3]).upper()
    quote = str(score.get("quote_ccy") or pair[3:]).upper()
    drivers = parse_json_list(score.get("rationale"))[:6]
    invalidators = parse_json_list(score.get("invalidators"))[:6]
    base_missing = parse_json_list((currency_scores.get(base) or {}).get("missing_factors"))
    quote_missing = parse_json_list((currency_scores.get(quote) or {}).get("missing_factors"))
    base_stale = parse_json_list((currency_scores.get(base) or {}).get("stale_factors"))
    quote_stale = parse_json_list((currency_scores.get(quote) or {}).get("stale_factors"))
    caveats = {
        "missing_factors": sorted(set(base_missing + quote_missing)),
        "stale_factors": sorted(set(base_stale + quote_stale)),
        "target_status": target.get("target_status"),
        "macro_data_degraded": bool(ctx.get("macro_data_degraded")),
        "macro_fetch_errors": (ctx.get("macro_fetch_errors") or [])[:5],
    }
    payload = {
        "schema_version": "AG3_FX_V1",
        "pair": pair,
        "symbol_yf": score.get("symbol_yf") or target.get("symbol_yf") or f"{pair}=X",
        "as_of": as_of,
        "fundamental": {
            "directional_bias": score.get("directional_bias") or "NEUTRAL",
            "score": num(score.get("pair_score")),
            "confidence": num(score.get("confidence")),
            "base_ccy": base,
            "quote_ccy": quote,
            "base_score": num(score.get("base_score")),
            "quote_score": num(score.get("quote_score")),
            "pair_pressure": num(score.get("pair_pressure")),
        },
        "equilibrium": {
            "spot": num(target.get("spot")),
            "target_mid": num(target.get("equilibrium_target_mid")),
            "target_low": num(target.get("equilibrium_target_low")),
            "target_high": num(target.get("equilibrium_target_high")),
            "band_width_pct": num(target.get("equilibrium_band_width_pct")),
            "mispricing_pct": num(target.get("mispricing_pct")),
            "target_horizon_days": int(target.get("target_horizon_days") or 20),
            "status": target.get("target_status") or "UNKNOWN",
            "method": target.get("method") or "macro_relative_pressure_v1",
        },
        "drivers": drivers,
        "invalidators": invalidators,
        "data_quality": {
            "score": num(score.get("data_quality_score")),
            "missing_factors": caveats["missing_factors"],
            "stale_factors": caveats["stale_factors"],
        },
    }
    summaries.append({
        "summary_id": f"{run_id}_{pair}",
        "run_id": run_id,
        "pair": pair,
        "symbol_yf": payload["symbol_yf"],
        "as_of": as_of,
        "directional_bias": payload["fundamental"]["directional_bias"],
        "score": payload["fundamental"]["score"],
        "confidence": payload["fundamental"]["confidence"],
        "spot": payload["equilibrium"]["spot"],
        "equilibrium_target_mid": payload["equilibrium"]["target_mid"],
        "equilibrium_target_low": payload["equilibrium"]["target_low"],
        "equilibrium_target_high": payload["equilibrium"]["target_high"],
        "mispricing_pct": payload["equilibrium"]["mispricing_pct"],
        "target_horizon_days": payload["equilibrium"]["target_horizon_days"],
        "drivers_json": json.dumps(drivers, ensure_ascii=False),
        "invalidators_json": json.dumps(invalidators, ensure_ascii=False),
        "data_caveats_json": json.dumps(caveats, ensure_ascii=False),
        "payload_json": json.dumps(payload, ensure_ascii=False),
    })

return [{"json": {**ctx, "ag1_summary": summaries}}]
