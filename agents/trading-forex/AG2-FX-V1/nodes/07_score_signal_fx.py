def clamp(v, lo=-1.0, hi=1.0):
    return max(lo, min(hi, v))


out = []
for it in _items or []:
    j = it.get("json", {}) or {}
    score = 0.0
    close = j.get("last_close")
    for key, weight in [("ret_5d", 0.20), ("ret_20d", 0.25), ("macd_hist", 0.20)]:
        v = j.get(key)
        if v is not None:
            score += weight if float(v) > 0 else -weight
    rsi = j.get("rsi14")
    if rsi is not None:
        r = float(rsi)
        if r < 30:
            score += 0.20
        elif r > 70:
            score -= 0.20
        elif 45 <= r <= 60:
            score += 0.05
    if close is not None and j.get("sma50") is not None:
        score += 0.20 if float(close) > float(j["sma50"]) else -0.20
    score = clamp(score)
    if score >= 0.65:
        label = "strong_buy"
    elif score >= 0.25:
        label = "buy"
    elif score <= -0.65:
        label = "strong_sell"
    elif score <= -0.25:
        label = "sell"
    else:
        label = "neutral"
    out.append({"json": {**j, "signal_score": score, "signal_label": label}})

return out
