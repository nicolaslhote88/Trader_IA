out = []
for it in _items or []:
    j = it.get("json", {}) or {}
    close = j.get("last_close")
    sma20 = j.get("sma20")
    sma50 = j.get("sma50")
    sma200 = j.get("sma200")
    bw = j.get("bb_width")
    regime = "range"
    if close is not None and sma50 is not None and sma200 is not None:
        if close > sma50 > sma200:
            regime = "trend_up"
        elif close < sma50 < sma200:
            regime = "trend_down"
    if bw is not None and bw > 0.06 and sma20 is not None and close is not None:
        regime = "breakout"
    out.append({"json": {**j, "regime": regime}})

return out
