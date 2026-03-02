from datetime import datetime, timezone
import math
import re


def safe_float(v, default=0.0):
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            x = float(v)
            return x if math.isfinite(x) else default
        s = str(v).strip().replace(",", ".")
        if s == "":
            return default
        x = float(s)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def parse_dt(v):
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
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def quantile(values, q, fallback=50.0):
    arr = []
    for x in values:
        v = safe_float(x, None)
        if v is not None and math.isfinite(v):
            arr.append(v)
    arr.sort()
    if not arr:
        return fallback
    if len(arr) == 1:
        return arr[0]
    pos = (len(arr) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return arr[lo]
    frac = pos - lo
    return arr[lo] + (arr[hi] - arr[lo]) * frac


def score_unit(v):
    return int(round(clamp(safe_float(v, 0.0), 0.0, 100.0)))


def truthy(v):
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "ok")


def fmt_date(v):
    dt = parse_dt(v)
    if dt is None:
        return ""
    return dt.isoformat()


items = _items or []
rows_in = []
for it in items:
    j = it.get("json", {}) if isinstance(it, dict) else {}
    if isinstance(j, dict):
        rows_in.append(j)

if not rows_in:
    return [{"json": {"opportunity_brief": "Aucune donnée reçue pour construire la matrice."}}]

if rows_in and rows_in[0].get("_empty"):
    return [{"json": {"opportunity_brief": f"Pipeline matrice vide: {rows_in[0].get('_error', 'UNKNOWN')}"}}]

rows_by_sym = {}
for r in rows_in:
    sym = str(r.get("Symbol") or r.get("symbol") or "").strip().upper()
    if not sym:
        continue
    prev = rows_by_sym.get(sym)
    if prev is None:
        rows_by_sym[sym] = dict(r)
        continue
    merged = dict(prev)
    for k, v in r.items():
        if (merged.get(k) in (None, "", 0, 0.0, "0")) and (v not in (None, "", "0")):
            merged[k] = v
    for dk in ["Last_Tech_Date", "Last_Funda_Date", "Last_News_Date", "YF_Fetched_At"]:
        d_new = parse_dt(r.get(dk))
        d_old = parse_dt(merged.get(dk))
        if d_old is None or (d_new is not None and d_new > d_old):
            merged[dk] = r.get(dk)
    rows_by_sym[sym] = merged

rows = list(rows_by_sym.values())
now = datetime.now(timezone.utc)

matrix_rows = []
for r in rows:
    symbol = str(r.get("Symbol") or "").strip().upper()
    if not symbol:
        continue

    asset_class = str(r.get("AssetClass") or "EQUITY").strip().upper()
    symbol_yahoo = str(r.get("Symbol_Yahoo") or symbol).strip().upper()
    name = str(r.get("Name") or symbol).strip() or symbol
    sector = str(r.get("Sector") or "N/A").strip() or "N/A"

    entry = safe_float(r.get("Last_Close"), 0.0)
    quote_px = safe_float(r.get("Regular_Market_Price"), 0.0)
    if entry <= 0 and quote_px > 0:
        entry = quote_px
    if entry <= 0:
        entry = safe_float(r.get("Target_Price"), 0.0)

    stop = safe_float(r.get("AI_Stop_Loss"), 0.0)
    d1_support = safe_float(r.get("D1_Support"), 0.0)
    d1_dist_sup = safe_float(r.get("D1_Dist_Sup_Pct"), 0.0)
    d1_atr_pct = safe_float(r.get("D1_ATR_Pct"), 0.0)

    if entry > 0:
        if stop <= 0 or stop >= entry:
            if d1_support > 0 and d1_support < entry:
                stop = d1_support * 0.998
            else:
                fallback_risk = max(2.0, d1_dist_sup if d1_dist_sup > 0 else d1_atr_pct * 2.0)
                stop = entry * (1.0 - fallback_risk / 100.0)

    d1_res = safe_float(r.get("D1_Resistance"), 0.0)
    d1_dist_res = safe_float(r.get("D1_Dist_Res_Pct"), 0.0)
    funda_upside = safe_float(r.get("Funda_Upside"), 0.0)
    target_price = safe_float(r.get("Target_Price"), 0.0)

    tp_candidates = []
    if entry > 0 and d1_res > entry:
        tp_candidates.append(d1_res)
    if entry > 0 and funda_upside > 0:
        tp_candidates.append(entry * (1.0 + funda_upside / 100.0))
    if entry > 0 and target_price > entry:
        tp_candidates.append(target_price)
    if entry > 0 and not tp_candidates and d1_dist_res > 0:
        tp_candidates.append(entry * (1.0 + d1_dist_res / 100.0))
    if entry > 0 and not tp_candidates:
        tp_candidates.append(entry * 1.03)
    tp = min(tp_candidates) if tp_candidates else 0.0

    reward_pct = ((tp - entry) / entry * 100.0) if entry > 0 else 0.0
    risk_pct_raw = ((entry - stop) / entry * 100.0) if entry > 0 else 0.0
    atr_stop_floor_pct = max(0.8 * d1_atr_pct, 0.75 if d1_atr_pct > 0 else 1.2)
    risk_pct_effective = max(0.1, risk_pct_raw, atr_stop_floor_pct)

    r_multiple_raw = reward_pct / max(0.1, risk_pct_raw) if reward_pct > 0 else 0.0
    r_multiple = reward_pct / risk_pct_effective if reward_pct > 0 else 0.0
    r_multiple_capped = clamp(r_multiple, 0.0, 6.0)
    rr_outlier = bool(r_multiple_raw > 6.0 or (risk_pct_raw > 0 and risk_pct_raw < atr_stop_floor_pct * 0.85))

    funda_risk = safe_float(r.get("Funda_Risk"), 50.0)
    spread_pct = safe_float(r.get("SpreadPct"), 0.0)
    slip_pct = safe_float(r.get("SlippageProxyPct"), 0.0)
    symbol_news_impact = safe_float(r.get("Symbol_News_Impact_7d"), 0.0)
    macro_impact = safe_float(r.get("Macro_Impact_30d"), 0.0)
    fx_bias = str(r.get("FX_Directional_Bias") or "").strip().upper()
    fx_bias_conf = safe_float(r.get("FX_Bias_Confidence"), 0.0)
    fx_urgent = truthy(r.get("FX_Urgent_Event_Window"))
    fx_macro_regime = str(r.get("FX_Macro_Regime") or "").strip()
    fx_macro_conf = safe_float(r.get("FX_Macro_Confidence"), 0.0)
    fx_rationale = str(r.get("FX_Rationale") or "").strip()
    sector_weight = safe_float(r.get("Sector_Weight_Pct"), 0.0)
    symbol_weight = safe_float(r.get("Symbol_Weight_Pct"), 0.0)

    raw_days_to_earnings = r.get("Days_To_Earnings")
    next_earnings_ts = parse_dt(r.get("Next_Earnings_Date"))

    days_to_next_earnings = None
    days_since_last_earnings = None
    if next_earnings_ts is not None:
        delta_days = (next_earnings_ts - now).total_seconds() / 86400.0
        if delta_days >= -0.4:
            days_to_next_earnings = max(0.0, round(delta_days, 1))
        else:
            days_since_last_earnings = round(abs(delta_days), 1)
    else:
        d = safe_float(raw_days_to_earnings, None)
        if d is not None:
            if d >= 0:
                days_to_next_earnings = round(d, 1)
            else:
                days_since_last_earnings = round(abs(d), 1)

    if days_to_next_earnings is not None:
        d = clamp(days_to_next_earnings, 0.0, 30.0)
        event_risk = 20.0 + ((30.0 - d) / 30.0) * 75.0
    elif days_since_last_earnings is not None:
        d = clamp(days_since_last_earnings, 0.0, 30.0)
        event_risk = min(45.0, 25.0 + d * 0.5)
    else:
        event_risk = 42.0

    if asset_class == "FX":
        event_risk = max(event_risk, 70.0 if fx_urgent else 35.0)

    vol_risk = clamp(d1_atr_pct * 20.0, 0.0, 100.0)
    liq_risk = clamp(spread_pct * 35.0 + slip_pct * 20.0, 0.0, 100.0)
    news_risk = clamp(max(0.0, -symbol_news_impact) * 8.0 + max(0.0, -macro_impact) * 3.0, 0.0, 100.0)
    concentration_risk = clamp(sector_weight * 1.3 + symbol_weight * 1.1, 0.0, 100.0)

    iv_atm = safe_float(r.get("IV_ATM"), 0.0)
    options_ok = truthy(r.get("Options_Ok"))
    options_error = str(r.get("Options_Error") or "").strip()
    options_warning = str(r.get("Options_Warning") or "").strip()
    options_state_text = (options_error + " " + options_warning).lower()
    invalid_options_state = any(tok in options_state_text for tok in ("_global.json.tmp", "global.json.tmp", "/data/state/", "invalid_options_state"))
    options_missing_known = any(tok in options_state_text for tok in ("no_expirations_available", "skipped_recent_no_expirations"))

    options_has_iv = options_ok and iv_atm > 0
    if options_has_iv:
        iv_as_pct = iv_atm * 100.0 if iv_atm <= 3 else iv_atm
        options_risk = clamp(iv_as_pct * 1.8, 0.0, 100.0)
    else:
        options_risk = 35.0

    if invalid_options_state:
        options_coverage_quality = 0.0
    elif options_has_iv:
        options_coverage_quality = 100.0
    elif options_ok:
        options_coverage_quality = 70.0
    elif options_missing_known:
        options_coverage_quality = 35.0
    else:
        options_coverage_quality = 20.0

    yf_fetched = parse_dt(r.get("YF_Fetched_At"))
    yf_age_h = None
    if yf_fetched is not None:
        yf_age_h = (now - yf_fetched).total_seconds() / 3600.0

    if yf_age_h is not None:
        if yf_age_h <= 24:
            freshness_quality = 100.0
        elif yf_age_h <= 48:
            freshness_quality = 85.0
        elif yf_age_h <= 72:
            freshness_quality = 70.0
        elif yf_age_h <= 120:
            freshness_quality = 50.0
        else:
            freshness_quality = 30.0
    else:
        freshness_quality = 50.0

    stale_penalty = 0.0
    if yf_age_h is not None:
        if yf_age_h > 72:
            stale_penalty = 12.0
        elif yf_age_h > 36:
            stale_penalty = 6.0
        elif yf_age_h > 24:
            stale_penalty = 3.0

    has_quote = entry > 0
    has_days_to_next_earnings = days_to_next_earnings is not None

    core_fields = [
        r.get("Tech_Action"),
        r.get("Tech_Confidence"),
        r.get("Funda_Score"),
        r.get("Funda_Risk"),
        r.get("Funda_Upside"),
        r.get("Symbol_News_Impact_7d"),
        r.get("Macro_Impact_30d"),
        r.get("D1_ATR_Pct"),
        r.get("D1_Dist_Res_Pct"),
        r.get("D1_Dist_Sup_Pct"),
    ]
    core_present = 0
    for fv in core_fields:
        if fv is None:
            continue
        if isinstance(fv, str) and not fv.strip():
            continue
        core_present += 1
    feature_quality = (core_present / len(core_fields)) * 100.0 if core_fields else 50.0

    earnings_quality = 100.0 if has_days_to_next_earnings else 40.0
    data_quality_score = (
        0.20 * (100.0 if has_quote else 30.0)
        + 0.25 * options_coverage_quality
        + 0.15 * earnings_quality
        + 0.20 * freshness_quality
        + 0.20 * feature_quality
    )
    if invalid_options_state:
        data_quality_score = 0.0
    data_quality_score = clamp(data_quality_score, 0.0, 100.0)

    risk_weights = {
        "funda": 0.30,
        "vol": 0.18,
        "liq": 0.14,
        "event": 0.14,
        "news": 0.10,
        "concentration": 0.09,
        "options": 0.05 if options_has_iv else 0.01,
    }
    risk_values = {
        "funda": clamp(funda_risk, 0.0, 100.0),
        "vol": vol_risk,
        "liq": liq_risk,
        "event": event_risk,
        "news": news_risk,
        "concentration": concentration_risk,
        "options": options_risk,
    }
    wsum = sum(risk_weights.values())
    risk_core = sum(risk_values[k] * risk_weights[k] for k in risk_weights) / wsum if wsum > 0 else 50.0
    risk_score_100 = clamp(risk_core + stale_penalty, 0.0, 100.0)

    reward_r = clamp(r_multiple_capped * 35.0, 0.0, 100.0)
    reward_upside = clamp(funda_upside * 3.0, 0.0, 100.0)
    reward_space = clamp(d1_dist_res * 4.0, 0.0, 100.0)
    reward_catalyst = clamp(max(0.0, symbol_news_impact) * 6.0 + max(0.0, macro_impact) * 2.0, 0.0, 100.0)

    tech_action = str(r.get("Tech_Action") or "").upper().strip()
    tech_conf = safe_float(r.get("Tech_Confidence"), 0.0)
    if tech_action == "BUY":
        trend_bonus = min(100.0, 55.0 + tech_conf * 0.45)
    elif tech_action == "SELL":
        trend_bonus = max(0.0, 35.0 - tech_conf * 0.25)
    else:
        trend_bonus = 45.0

    reward_score_100 = clamp(
        0.36 * reward_r
        + 0.22 * reward_upside
        + 0.14 * reward_space
        + 0.18 * reward_catalyst
        + 0.10 * trend_bonus,
        0.0,
        100.0,
    )

    funda_score = safe_float(r.get("Funda_Score"), 50.0)
    tech_prob = 50.0 + (8.0 if tech_action == "BUY" else (-8.0 if tech_action == "SELL" else 0.0)) + (tech_conf - 50.0) * 0.20
    funda_prob = 0.7 * funda_score + 0.3 * (100.0 - funda_risk)
    sentiment_prob = clamp(50.0 + symbol_news_impact * 4.0 + macro_impact * 1.5, 0.0, 100.0)

    regime = str(r.get("AI_Regime_D1") or "").upper().strip()
    alignment = str(r.get("AI_Alignment") or "").upper().strip()
    regime_adj = 8.0 if regime == "BULLISH" else (-6.0 if regime == "BEARISH" else 0.0)
    align_adj = 6.0 if alignment == "WITH_BIAS" else (-6.0 if alignment == "AGAINST_BIAS" else 0.0)

    prob_score = clamp(
        0.36 * tech_prob
        + 0.34 * funda_prob
        + 0.20 * sentiment_prob
        + 0.10 * (50.0 + regime_adj + align_adj),
        0.0,
        100.0,
    )

    p_win = clamp(prob_score / 100.0, 0.05, 0.95)
    ev_r = (p_win * max(0.0, r_multiple_capped)) - (1.0 - p_win)

    risk_u = score_unit(risk_score_100)
    reward_u = score_unit(reward_score_100)

    data_quality_gate_ok = data_quality_score >= 60.0
    earnings_gate_block = days_to_next_earnings is not None and days_to_next_earnings <= 7.0
    liquidity_gate_block = liq_risk >= 85.0
    invalid_options_state_gate = invalid_options_state

    gates = []
    if not data_quality_gate_ok:
        gates.append("DATA_QUALITY_LOW")
    if earnings_gate_block:
        gates.append("EARNINGS_IMMINENT")
    if liquidity_gate_block:
        gates.append("LIQUIDITY_STRESS")
    if invalid_options_state_gate:
        gates.append("INVALID_OPTIONS_STATE")
    if rr_outlier:
        gates.append("RR_OUTLIER")

    matrix_rows.append(
        {
            "symbol": symbol,
            "symbol_yahoo": symbol_yahoo,
            "name": name,
            "asset_class": asset_class,
            "sector": sector,
            "entry_price": entry,
            "stop_price": stop,
            "tp_price": tp,
            "reward_pct": reward_pct,
            "risk_pct": risk_pct_effective,
            "risk_pct_raw": risk_pct_raw,
            "atr_stop_floor_pct": atr_stop_floor_pct,
            "r_multiple_raw": r_multiple_raw,
            "r_multiple": r_multiple_capped,
            "rr_outlier": rr_outlier,
            "risk_score_u": risk_u,
            "reward_score_u": reward_u,
            "prob_score": prob_score,
            "prob_score_for_grade": (0.85 * prob_score + 0.15 * data_quality_score),
            "p_win": p_win,
            "ev_r": ev_r,
            "data_quality_score": data_quality_score,
            "days_to_next_earnings": days_to_next_earnings,
            "days_since_last_earnings": days_since_last_earnings,
            "spread_pct": spread_pct,
            "slippage_proxy_pct": slip_pct,
            "iv_atm": iv_atm if iv_atm > 0 else None,
            "options_ok": options_ok,
            "options_note": ("INVALID_OPTIONS_STATE" if invalid_options_state else (options_error or options_warning or "")),
            "event_risk_score": event_risk,
            "vol_risk_score": vol_risk,
            "liquidity_risk_score": liq_risk,
            "news_risk_score": news_risk,
            "concentration_risk_score": concentration_risk,
            "options_risk_score": options_risk,
            "reward_component_r": reward_r,
            "reward_component_upside": reward_upside,
            "reward_component_space": reward_space,
            "reward_component_catalyst": reward_catalyst,
            "reward_component_trend": trend_bonus,
            "symbol_news_impact_7d": symbol_news_impact,
            "macro_impact_30d": macro_impact,
            "last_tech_date": fmt_date(r.get("Last_Tech_Date")),
            "last_funda_date": fmt_date(r.get("Last_Funda_Date")),
            "last_news_date": fmt_date(r.get("Last_News_Date")),
            "yf_fetched_at": fmt_date(r.get("YF_Fetched_At")),
            "gate_summary": "|".join(gates),
            "tech_action": tech_action,
            "tech_confidence": tech_conf,
            "funda_score": funda_score,
            "funda_risk": funda_risk,
            "funda_upside": funda_upside,
            "recommendation": str(r.get("Recommendation") or "").strip(),
            "target_price": target_price,
            "funda_horizon": str(r.get("Funda_Horizon") or "").strip(),
            "macro_themes": str(r.get("Macro_Themes") or "").strip(),
            "fx_directional_bias": fx_bias,
            "fx_bias_confidence": fx_bias_conf,
            "fx_urgent_event_window": fx_urgent,
            "fx_macro_regime": fx_macro_regime,
            "fx_macro_confidence": fx_macro_conf,
            "fx_rationale": fx_rationale,
            "fx_as_of": fmt_date(r.get("FX_As_Of")),
            "fx_macro_as_of": fmt_date(r.get("FX_Macro_As_Of")),
        }
    )

if not matrix_rows:
    return [{"json": {"opportunity_brief": "Aucune valeur exploitable après normalisation."}}]

risk_thr = int(round(clamp(quantile([r["risk_score_u"] for r in matrix_rows], 0.60, 50.0), 20.0, 85.0)))
reward_thr = int(round(clamp(quantile([r["reward_score_u"] for r in matrix_rows], 0.60, 50.0), 20.0, 85.0)))
grade_a_thr = quantile([r["prob_score_for_grade"] for r in matrix_rows], 0.90, 75.0)
grade_b_thr = quantile([r["prob_score_for_grade"] for r in matrix_rows], 0.50, 55.0)

for r in matrix_rows:
    score_g = safe_float(r.get("prob_score_for_grade"), 50.0)
    grade = "A" if score_g >= grade_a_thr else ("B" if score_g >= grade_b_thr else "C")

    if safe_float(r.get("data_quality_score"), 50.0) < 45.0 and grade == "A":
        grade = "B"
    if safe_float(r.get("data_quality_score"), 50.0) < 45.0 and grade == "B":
        grade = "C"
    if r.get("rr_outlier") and grade == "A":
        grade = "B"
    if r.get("days_to_next_earnings") is not None and safe_float(r.get("days_to_next_earnings"), 99.0) <= 7.0 and grade == "A":
        grade = "B"
    if "INVALID_OPTIONS_STATE" in str(r.get("gate_summary") or ""):
        grade = "C" if grade == "B" else ("B" if grade == "A" else "C")

    risk_u = safe_float(r.get("risk_score_u"), 50.0)
    reward_u = safe_float(r.get("reward_score_u"), 50.0)
    ev_r = safe_float(r.get("ev_r"), 0.0)
    data_quality = safe_float(r.get("data_quality_score"), 50.0)

    quality_block = data_quality < 60.0
    earnings_block = r.get("days_to_next_earnings") is not None and safe_float(r.get("days_to_next_earnings"), 99.0) <= 7.0
    liquidity_block = safe_float(r.get("liquidity_risk_score"), 0.0) >= 85.0
    invalid_options_state = "INVALID_OPTIONS_STATE" in str(r.get("gate_summary") or "")
    rr_outlier = bool(r.get("rr_outlier"))

    if risk_u <= risk_thr and reward_u >= reward_thr:
        quadrant = "Q1 - Priorite"
    elif risk_u > risk_thr and reward_u >= reward_thr:
        quadrant = "Q2 - Speculatif"
    elif risk_u <= risk_thr and reward_u < reward_thr:
        quadrant = "Q3 - Defensif"
    else:
        quadrant = "Q4 - Sortie"

    enter_core = (ev_r >= 0.20 and reward_u >= reward_thr and risk_u <= risk_thr and grade in ("A", "B"))
    reduce_core = (
        ev_r < 0.0
        or (risk_u >= min(95.0, risk_thr + 18.0) and reward_u <= max(5.0, reward_thr - 12.0))
        or (liquidity_block and ev_r < 0.15)
    )

    reasons = []
    if str(r.get("asset_class") or "").upper() == "FX":
        if r.get("fx_directional_bias") == "BUY_BASE":
            action = "Entrer / Renforcer"
            reasons.append("FX_BIAS_BUY_BASE")
        elif r.get("fx_directional_bias") == "SELL_BASE":
            action = "Reduire / Sortir"
            reasons.append("FX_BIAS_SELL_BASE")
        else:
            action = "Surveiller"
            reasons.append("FX_NEUTRAL_BIAS")
    else:
        if enter_core and not (quality_block or earnings_block or rr_outlier or invalid_options_state):
            action = "Entrer / Renforcer"
            reasons.append("SETUP_OK")
        elif reduce_core:
            action = "Reduire / Sortir"
            reasons.append("RISK_REWARD_UNFAVORABLE")
        else:
            action = "Surveiller"
            reasons.append("WAIT_CONFIRMATION")
            if quality_block:
                reasons.append("DATA_QUALITY_GATE")
            if earnings_block:
                reasons.append("EARNINGS_GATE")
            if rr_outlier:
                reasons.append("RR_OUTLIER_GATE")
            if invalid_options_state:
                reasons.append("INVALID_OPTIONS_STATE_GATE")

    ev_component = clamp((ev_r / 1.5) * 100.0, 0.0, 100.0)
    risk_component = clamp(100.0 - risk_u, 0.0, 100.0)
    size_score = 0.55 * ev_component + 0.25 * risk_component + 0.20 * data_quality

    if str(r.get("asset_class") or "").upper() == "FX":
        fx_size = clamp(safe_float(r.get("fx_bias_confidence"), 0.0), 0.0, 100.0)
        if action == "Entrer / Renforcer":
            size_pct = max(10.0, fx_size * 0.7)
        elif action == "Surveiller":
            size_pct = fx_size * 0.25
        else:
            size_pct = 0.0
    else:
        if action == "Entrer / Renforcer":
            size_pct = clamp(size_score, 10.0, 100.0)
        elif action == "Surveiller":
            size_pct = clamp(size_score * 0.50, 0.0, 50.0)
        else:
            size_pct = 0.0
    if earnings_block:
        size_pct = min(size_pct, 30.0)
    if invalid_options_state:
        size_pct = min(size_pct, 20.0)

    r["setup_grade"] = grade
    r["quadrant"] = quadrant
    r["matrix_action"] = action
    r["action_reason"] = "|".join(reasons)
    r["size_reco_pct"] = round(size_pct, 1)

arank = {"Entrer / Renforcer": 0, "Surveiller": 1, "Reduire / Sortir": 2}
krank = {"A": 0, "B": 1, "C": 2}
matrix_rows.sort(
    key=lambda r: (
        arank.get(r.get("matrix_action"), 9),
        krank.get(r.get("setup_grade"), 9),
        -safe_float(r.get("ev_r"), 0.0),
        -safe_float(r.get("reward_score_u"), 0.0),
        safe_float(r.get("risk_score_u"), 50.0),
    )
)

total = len(matrix_rows)
count_enter = sum(1 for r in matrix_rows if r.get("matrix_action") == "Entrer / Renforcer")
count_watch = sum(1 for r in matrix_rows if r.get("matrix_action") == "Surveiller")
count_exit = sum(1 for r in matrix_rows if r.get("matrix_action") == "Reduire / Sortir")
count_grade_a = sum(1 for r in matrix_rows if r.get("setup_grade") == "A")
count_rr_out = sum(1 for r in matrix_rows if r.get("rr_outlier"))
count_dq_low = sum(1 for r in matrix_rows if safe_float(r.get("data_quality_score"), 0.0) < 60.0)

avg_ev = sum(safe_float(r.get("ev_r"), 0.0) for r in matrix_rows) / total if total else 0.0
avg_pwin = (sum(safe_float(r.get("p_win"), 0.0) for r in matrix_rows) / total * 100.0) if total else 0.0


def fmt_row(r):
    gates = r.get("gate_summary") or "OK"
    return (
        f"- {r['symbol']} ({r['name']}) [{r.get('asset_class','EQUITY')}:{r['sector']}] | {r['matrix_action']} | G{r['setup_grade']} | "
        f"Risk {int(r['risk_score_u'])}/100 | Reward {int(r['reward_score_u'])}/100 | "
        f"R {safe_float(r['r_multiple']):.2f} | EV {safe_float(r['ev_r']):.2f} | pWin {safe_float(r['p_win'])*100:.1f}% | "
        f"DataQ {safe_float(r['data_quality_score']):.0f}/100 | size {safe_float(r['size_reco_pct']):.1f}% | "
        f"E {safe_float(r['entry_price']):.2f} S {safe_float(r['stop_price']):.2f} TP {safe_float(r['tp_price']):.2f} | gates={gates}"
    )


enter_rows = [r for r in matrix_rows if r.get("matrix_action") == "Entrer / Renforcer"]
watch_rows = [r for r in matrix_rows if r.get("matrix_action") == "Surveiller"]
exit_rows = [r for r in matrix_rows if r.get("matrix_action") == "Reduire / Sortir"]

TOP_ENTER = 30
TOP_WATCH = 30
TOP_EXIT = 25

brief_lines = []
brief_lines.append("=== MATRICE AG2+AG3+AG4 (PREP AGENT #1) ===")
brief_lines.append(f"GeneratedAt UTC: {datetime.now(timezone.utc).isoformat()}")
brief_lines.append("")
brief_lines.append("Legende:")
brief_lines.append("- Risk (0-100): plus haut = plus risque.")
brief_lines.append("- Reward (0-100): plus haut = potentiel plus attractif.")
brief_lines.append("- R = Reward/Risk cape a 6 pour eviter les faux extremes.")
brief_lines.append("- EV(R) = pWin*R - (1-pWin).")
brief_lines.append("- Gates: DATA_QUALITY_LOW / EARNINGS_IMMINENT / LIQUIDITY_STRESS / RR_OUTLIER / INVALID_OPTIONS_STATE.")
brief_lines.append("")
brief_lines.append(
    f"Seuils dynamiques: Risk p60={risk_thr}, Reward p60={reward_thr}. "
    f"Grades quantiles: A>={grade_a_thr:.1f}, B>={grade_b_thr:.1f}, sinon C."
)
brief_lines.append(
    f"Univers={total} | Entrer={count_enter} | Surveiller={count_watch} | Reduire/Sortir={count_exit} | "
    f"GradeA={count_grade_a} | RR_outliers={count_rr_out} | DataQ<60={count_dq_low}"
)
brief_lines.append(f"EV(R) moyen={avg_ev:.2f} | Prob.win moyenne={avg_pwin:.1f}%")
brief_lines.append("")
brief_lines.append("Lecture des quadrants:")
brief_lines.append(f"- Q1 Priorite: Risk <= {risk_thr} et Reward >= {reward_thr} -> zone prioritaire (si gates ouverts).")
brief_lines.append(f"- Q2 Speculatif: Risk > {risk_thr} et Reward >= {reward_thr} -> execution selective, taille reduite.")
brief_lines.append(f"- Q3 Defensif: Risk <= {risk_thr} et Reward < {reward_thr} -> conservation / surveillance.")
brief_lines.append(f"- Q4 Sortie: Risk > {risk_thr} et Reward < {reward_thr} -> derisquage prioritaire.")
brief_lines.append("")
brief_lines.append(f"TOP {TOP_ENTER} Entrer / Renforcer:")
brief_lines.extend([fmt_row(r) for r in enter_rows[:TOP_ENTER]] or ["- (aucun candidat)"])
brief_lines.append("")
brief_lines.append(f"TOP {TOP_WATCH} Surveiller:")
brief_lines.extend([fmt_row(r) for r in watch_rows[:TOP_WATCH]] or ["- (aucun)"])
brief_lines.append("")
brief_lines.append(f"TOP {TOP_EXIT} Reduire / Sortir:")
brief_lines.extend([fmt_row(r) for r in exit_rows[:TOP_EXIT]] or ["- (aucun)"])
brief_lines.append("")
brief_lines.append("Regles d'usage pour Agent #1:")
brief_lines.append("- Ne proposer OPEN/INCREASE que sur Entrer/Renforcer sans gate bloquant.")
brief_lines.append("- Si EARNINGS_IMMINENT ou INVALID_OPTIONS_STATE: privilegier WATCH/HOLD ou taille reduite.")
brief_lines.append("- Si RR_OUTLIER: considerer setup non fiable tant que stop/target non recalibres.")

brief = "\n".join(brief_lines)

stats = {
    "universe": total,
    "enter_count": count_enter,
    "watch_count": count_watch,
    "exit_count": count_exit,
    "grade_a": count_grade_a,
    "rr_outliers": count_rr_out,
    "data_quality_lt_60": count_dq_low,
    "avg_ev_r": round(avg_ev, 4),
    "avg_pwin_pct": round(avg_pwin, 2),
}
thresholds = {
    "risk_p60": risk_thr,
    "reward_p60": reward_thr,
    "grade_a_min": round(grade_a_thr, 3),
    "grade_b_min": round(grade_b_thr, 3),
}

pack_rows = []
for r in matrix_rows[:200]:
    pack_rows.append(
        {
            "symbol": r["symbol"],
            "symbol_yahoo": r.get("symbol_yahoo") or "",
            "name": r["name"],
            "asset_class": r.get("asset_class") or "EQUITY",
            "sector": r["sector"],
            "decision": r["matrix_action"],
            "grade": r["setup_grade"],
            "risk": int(r["risk_score_u"]),
            "reward": int(r["reward_score_u"]),
            "r": round(safe_float(r["r_multiple"]), 4),
            "ev_r": round(safe_float(r["ev_r"]), 4),
            "p_win_pct": round(safe_float(r["p_win"]) * 100.0, 2),
            "data_quality": round(safe_float(r["data_quality_score"]), 2),
            "size_reco_pct": round(safe_float(r["size_reco_pct"]), 2),
            "gates": r.get("gate_summary") or "OK",
            "entry": round(safe_float(r["entry_price"]), 4),
            "stop": round(safe_float(r["stop_price"]), 4),
            "tp": round(safe_float(r["tp_price"]), 4),
            "days_to_next_earnings": r.get("days_to_next_earnings"),
            "iv_atm": r.get("iv_atm"),
            "spread_pct": round(safe_float(r.get("spread_pct"), 0.0), 4),
            "macro_themes": r.get("macro_themes") or "",
            "action_reason": r.get("action_reason") or "",
            "fx_directional_bias": r.get("fx_directional_bias") or "",
            "fx_bias_confidence": round(safe_float(r.get("fx_bias_confidence"), 0.0), 2),
            "fx_urgent_event_window": bool(r.get("fx_urgent_event_window")),
            "fx_macro_regime": r.get("fx_macro_regime") or "",
            "fx_macro_confidence": round(safe_float(r.get("fx_macro_confidence"), 0.0), 2),
            "fx_rationale": r.get("fx_rationale") or "",
        }
    )

opportunity_pack = {
    "generatedAt": datetime.now(timezone.utc).isoformat(),
    "stats": stats,
    "thresholds": thresholds,
    "rows": pack_rows,
}

def parse_fx_pair6(sym):
    s = str(sym or "").upper().strip()
    if s.startswith("FX:"):
        s = s[3:]
    if s.endswith("=X"):
        s = s[:-2]
    s = re.sub(r"[^A-Z]", "", s)[:6]
    return s if len(s) == 6 else ""


def inverse_pair6(pair6):
    p = parse_fx_pair6(pair6)
    if not p:
        return ""
    return p[3:6] + p[0:3]


def derive_fx_bias(entry_px, tp_px, raw_bias):
    b = str(raw_bias or "").strip().upper()
    if b in ("BUY_BASE", "SELL_BASE", "NEUTRAL"):
        if b != "NEUTRAL":
            return b
    e = safe_float(entry_px, 0.0)
    t = safe_float(tp_px, 0.0)
    if e > 0 and t > 0:
        if t > e:
            return "BUY_BASE"
        if t < e:
            return "SELL_BASE"
    return "NEUTRAL"


def adjust_fx_confidence(conf, bias, base_ccy, quote_ccy, macro_regime):
    c = clamp(round(safe_float(conf, 0.0)), 0, 100)
    regime = str(macro_regime or "").strip().upper()
    safe_haven = {"JPY", "CHF", "USD"}
    beta_ccy = {"AUD", "NZD", "CAD"}
    base_safe = base_ccy in safe_haven
    quote_safe = quote_ccy in safe_haven
    base_beta = base_ccy in beta_ccy
    quote_beta = quote_ccy in beta_ccy

    if "RISK-OFF" in regime:
        if (bias == "BUY_BASE" and base_safe and not quote_safe) or (bias == "SELL_BASE" and quote_safe and not base_safe):
            c += 8
        if (bias == "BUY_BASE" and base_beta and not quote_beta) or (bias == "SELL_BASE" and quote_beta and not base_beta):
            c -= 8
    elif "RISK-ON" in regime:
        if (bias == "BUY_BASE" and base_beta and not quote_beta) or (bias == "SELL_BASE" and quote_beta and not base_beta):
            c += 8
        if (bias == "BUY_BASE" and base_safe and not quote_safe) or (bias == "SELL_BASE" and quote_safe and not base_safe):
            c -= 8
    return int(clamp(c, 0, 100))


def pair_quality_score(p):
    bias_bonus = 1 if str(p.get("directional_bias") or "NEUTRAL") != "NEUTRAL" else 0
    conf = safe_float(p.get("confidence"), 0.0)
    evr = safe_float(p.get("ev_r"), 0.0)
    dq = safe_float(p.get("data_quality"), 0.0)
    has_px = 1 if safe_float(p.get("last_close"), 0.0) > 0 else 0
    return bias_bonus * 1e8 + conf * 1e6 + evr * 1e4 + dq * 1e2 + has_px


raw_fx_pairs = []
for r in matrix_rows:
    if str(r.get("asset_class") or "").upper() != "FX":
        continue
    pair6 = parse_fx_pair6(r.get("symbol"))
    if not pair6:
        continue
    base_ccy, quote_ccy = pair6[:3], pair6[3:6]
    entry_px = safe_float(r.get("entry_price"), 0.0)
    last_close = entry_px if entry_px > 0 else safe_float(r.get("target_price"), 0.0)
    tp_px = safe_float(r.get("tp_price"), 0.0)
    stop_px = safe_float(r.get("stop_price"), 0.0)
    p_win_pct = round(safe_float(r.get("p_win"), 0.0) * 100.0, 2)
    macro_regime = str(r.get("fx_macro_regime") or "Neutral").strip() or "Neutral"
    bias = derive_fx_bias(entry_px, tp_px, r.get("fx_directional_bias"))
    conf = safe_float(r.get("fx_bias_confidence"), 0.0)
    if conf <= 0:
        conf = p_win_pct
    conf = adjust_fx_confidence(conf, bias, base_ccy, quote_ccy, macro_regime)

    raw_fx_pairs.append(
        {
            "pair": pair6,
            "symbol_internal": "FX:" + pair6,
            "symbol_yahoo": pair6 + "=X",
            "base_ccy": base_ccy,
            "quote_ccy": quote_ccy,
            "last_close": round(last_close, 6) if last_close > 0 else None,
            "quote_to_eur": None,
            "eur_per_base": None,
            "directional_bias": bias,
            "confidence": conf,
            "urgent_event_window": bool(r.get("fx_urgent_event_window")),
            "entry": round(entry_px, 6) if entry_px > 0 else None,
            "stop": round(stop_px, 6) if stop_px > 0 else None,
            "tp": round(tp_px, 6) if tp_px > 0 else None,
            "p_win_pct": p_win_pct,
            "risk": round(safe_float(r.get("risk_score_u"), 0.0), 2),
            "reward": round(safe_float(r.get("reward_score_u"), 0.0), 2),
            "ev_r": round(safe_float(r.get("ev_r"), 0.0), 4),
            "data_quality": round(safe_float(r.get("data_quality_score"), 0.0), 2),
            "gates": str(r.get("gate_summary") or "OK"),
            "rationale": str(r.get("fx_rationale") or r.get("action_reason") or ""),
            "asOf": r.get("fx_as_of") or r.get("fx_macro_as_of") or opportunity_pack["generatedAt"],
            "fx_macro_regime": macro_regime,
            "fx_macro_confidence": round(safe_float(r.get("fx_macro_confidence"), 0.0), 2),
        }
    )

# Dedup exact pair duplicates.
fx_pairs_by_pair = {}
for p in raw_fx_pairs:
    prev = fx_pairs_by_pair.get(p["pair"])
    if prev is None or pair_quality_score(p) > pair_quality_score(prev):
        fx_pairs_by_pair[p["pair"]] = p
fx_pairs = list(fx_pairs_by_pair.values())

# Build EUR conversion map: fx_rates[CCY] = EUR_per_1_unit_CCY.
edges = []
for p in fx_pairs:
    px = safe_float(p.get("last_close"), 0.0)
    if px > 0 and p.get("base_ccy") and p.get("quote_ccy"):
        edges.append((p["base_ccy"], p["quote_ccy"], px))

fx_rates = {"EUR": 1.0}
changed = True
guard = 0
while changed and guard < 30:
    changed = False
    guard += 1
    for base, quote, px in edges:
        if fx_rates.get(base) and not fx_rates.get(quote):
            fx_rates[quote] = fx_rates[base] / px
            changed = True
        if fx_rates.get(quote) and not fx_rates.get(base):
            fx_rates[base] = fx_rates[quote] * px
            changed = True

for p in fx_pairs:
    q2e = safe_float(fx_rates.get(p.get("quote_ccy")), None)
    px = safe_float(p.get("last_close"), None)
    p["quote_to_eur"] = round(q2e, 8) if q2e is not None else None
    p["eur_per_base"] = round(px * q2e, 8) if (px is not None and q2e is not None) else None

# Build macro context.
macro_regime = "Neutral"
macro_conf = 0.0
macro_as_of = opportunity_pack["generatedAt"]
if fx_pairs:
    best_macro = max(
        fx_pairs,
        key=lambda p: safe_float(p.get("fx_macro_confidence"), 0.0),
    )
    macro_regime = str(best_macro.get("fx_macro_regime") or "Neutral") or "Neutral"
    macro_conf = safe_float(best_macro.get("fx_macro_confidence"), 0.0)
    macro_as_of = best_macro.get("asOf") or opportunity_pack["generatedAt"]

fx_context = {
    "as_of": macro_as_of,
    "macro_regime": macro_regime,
    "macro_confidence": int(clamp(round(macro_conf), 0, 100)),
    "signals_freshness": {
        "max_age_h1_hours": None,
        "max_age_d1_hours": None,
    },
    "fx_universe_count": len(fx_pairs),
    "fx_sleeve": {
        "target_pct_min": 5,
        "target_pct_max": 10,
        "per_pair_pct_max": 3,
        "default_pair_pct": 1.5,
    },
}

fx_macro = {
    "as_of": fx_context["as_of"],
    "market_regime": fx_context["macro_regime"],
    "confidence": fx_context["macro_confidence"],
}

# Build actionable top FX candidates.
blocked_gates = {"DATA_QUALITY_LOW", "INVALID_OPTIONS_STATE", "LIQUIDITY_STRESS"}
fx_candidates_raw = []
for p in fx_pairs:
    bias = str(p.get("directional_bias") or "NEUTRAL").upper()
    conf = safe_float(p.get("confidence"), 0.0)
    data_q = safe_float(p.get("data_quality"), 0.0)
    gates = [g.strip().upper() for g in str(p.get("gates") or "OK").split("|") if g.strip()]
    if bias == "NEUTRAL":
        continue
    if conf < 50:
        continue
    if data_q < 60:
        continue
    if any(g in blocked_gates for g in gates):
        continue
    fx_candidates_raw.append(dict(p))

# Dedup pair/inverse at candidate stage, keep strongest.
fx_candidates_by_key = {}
for p in fx_candidates_raw:
    inv = inverse_pair6(p.get("pair"))
    key = "|".join(sorted([p.get("pair") or "", inv]))
    prev = fx_candidates_by_key.get(key)
    if prev is None or pair_quality_score(p) > pair_quality_score(prev):
        fx_candidates_by_key[key] = p

fx_candidates = list(fx_candidates_by_key.values())
fx_candidates.sort(
    key=lambda p: (
        -safe_float(p.get("confidence"), 0.0),
        -safe_float(p.get("ev_r"), 0.0),
    )
)
fx_candidates = fx_candidates[:10]

if fx_candidates:
    top_fx = ", ".join(
        [
            f"{p['pair']} {p['directional_bias']} c={int(safe_float(p.get('confidence'), 0))} ev={safe_float(p.get('ev_r'), 0.0):.2f}"
            for p in fx_candidates[:6]
        ]
    )
    brief_lines.append("")
    brief_lines.append("FX top candidates (max 6): " + top_fx)

opportunity_pack["fx_context"] = fx_context
opportunity_pack["fx_rates"] = fx_rates
opportunity_pack["fx_candidates"] = fx_candidates

return [
    {
        "json": {
            "opportunity_brief": brief,
            "opportunity_pack": opportunity_pack,
            "opportunity_stats": stats,
            "matrix_thresholds": thresholds,
            "fx_pairs": fx_pairs,
            "fx_macro": fx_macro,
            "fx_context": fx_context,
            "fx_rates": fx_rates,
            "fx_candidates": fx_candidates,
        }
    }
]
