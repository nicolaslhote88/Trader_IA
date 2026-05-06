import math
from datetime import datetime, timezone

ctx = (_items or [{"json": {}}])[0].get("json", {})
run_id = ctx.get("run_id") or ""
as_of = ctx.get("as_of") or datetime.now(timezone.utc).isoformat()
max_shift = float(ctx.get("max_equilibrium_shift_pct") or 0.06)
technical = {str(x.get("pair") or "").upper(): x for x in (ctx.get("technical_signals") or [])}
pair_focus = ((ctx.get("macro_news") or {}).get("pair_focus") or {})
macro_data_degraded = bool(ctx.get("macro_data_degraded"))
pair_scores = ctx.get("pair_fundamental_scores") or []


def clamp(v, lo=-1.0, hi=1.0):
    try:
        n = float(v)
        if not math.isfinite(n):
            return 0.0
        return max(lo, min(hi, n))
    except Exception:
        return 0.0


def to_float(v, default=0.0):
    try:
        if v is None:
            return default
        n = float(v)
        return n if math.isfinite(n) else default
    except Exception:
        return default


targets = []
for row in pair_scores:
    pair = str(row.get("pair") or "").upper()
    tech = technical.get(pair) or {}
    spot = to_float(row.get("spot"), 0.0) or to_float(tech.get("last_close"), 0.0)
    atr14 = to_float(tech.get("atr14"), 0.0)
    confidence = to_float(row.get("confidence"), 0.25)
    data_quality = to_float(row.get("data_quality_score"), 0.25)
    pressure_norm = clamp(row.get("pair_pressure"))
    confidence_factor = max(0.25, min(1.0, confidence))
    urgent = bool((pair_focus.get(pair) or {}).get("urgent_event_within_4h"))

    if spot <= 0:
        target_mid = None
        target_low = None
        target_high = None
        shift_pct = 0.0
        band_width_pct = None
        atr_pct = None
        mispricing_pct = None
        status = "NO_SPOT"
    else:
        shift_pct = pressure_norm * max_shift * confidence_factor
        target_mid = spot * (1.0 + shift_pct)
        atr_pct = atr14 / spot if atr14 and atr14 > 0 else 0.01
        band_width_pct = max(1.5 * atr_pct, 0.01)
        band_width_pct *= 1.0 + (1.0 - max(0.0, min(1.0, data_quality)))
        if urgent:
            band_width_pct *= 1.5
        target_low = target_mid * (1.0 - band_width_pct)
        target_high = target_mid * (1.0 + band_width_pct)
        mispricing_pct = (spot - target_mid) / target_mid if target_mid else None
        status = "DEGRADED_MACRO_DATA" if macro_data_degraded else "OK"

    targets.append({
        "target_id": f"{run_id}_{pair}",
        "run_id": run_id,
        "pair": pair,
        "symbol_yf": row.get("symbol_yf") or f"{pair}=X",
        "as_of": as_of,
        "spot": spot or None,
        "equilibrium_target_mid": target_mid,
        "equilibrium_target_low": target_low,
        "equilibrium_target_high": target_high,
        "equilibrium_shift_pct": shift_pct,
        "equilibrium_band_width_pct": band_width_pct,
        "mispricing_pct": mispricing_pct,
        "atr_20d_pct": atr_pct,
        "target_horizon_days": 5 if urgent else 20,
        "target_status": status,
        "method": "macro_relative_pressure_v1",
        "confidence": confidence,
        "data_quality_score": data_quality,
    })

return [{"json": {**ctx, "equilibrium_targets": targets, "target_rows": len(targets)}}]
