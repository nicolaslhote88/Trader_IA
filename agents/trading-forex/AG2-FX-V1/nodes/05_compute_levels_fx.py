out = []
for it in _items or []:
    j = it.get("json", {}) or {}
    h = j.get("_last_high")
    l = j.get("_last_low")
    c = j.get("_last_prev_close") or j.get("last_close")
    if h is not None and l is not None and c is not None:
        pivot = (float(h) + float(l) + float(c)) / 3.0
        r1 = 2 * pivot - float(l)
        s1 = 2 * pivot - float(h)
        r2 = pivot + (float(h) - float(l))
        s2 = pivot - (float(h) - float(l))
    else:
        pivot = r1 = r2 = s1 = s2 = None
    out.append({"json": {**j, "pivot": pivot, "r1": r1, "r2": r2, "s1": s1, "s2": s2}})

return out
