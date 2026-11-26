from calendar import monthrange
from collections import defaultdict
from datetime import date, timedelta


def month_after(day, n):
    m = day.month - 1 + n
    year, month = day.year + m // 12, m % 12 + 1
    return day.replace(year=year, month=month, day=min(day.day, monthrange(year, month)[1]))


def operating_cost(start, end, obs):
    """Charge all evidenced project API costs once to each independent scenario, including weekends."""
    total = 0.
    day = date.fromisoformat(start)
    while day <= date.fromisoformat(end):
        d = day.isoformat()
        if not all(any(a <= d <= b for a, b in obs["coverage"].get(provider, []))
                   for provider in ("OpenAI", "DeepSeek", "Anthropic")):
            return None
        amount = obs["fees"].get(d, 0)
        if amount is None:
            return None
        total += amount
        day += timedelta(days=1)
    return total


def max_drawdown(values):
    if any(x is None or x <= 0 for x in values) or len(values) < 2:
        return None
    peak, worst = values[0], 0.
    for val in values:
        peak = max(peak, val)
        worst = max(worst, 1 - val / peak)
    return worst


def valuation(row, day, lookup, currency_rates):
    nav = row["cash"]
    values, sectors, countries = {}, defaultdict(float), defaultdict(float)
    missing = []
    for symbol, qty in row["positions"].items():
        meta = row["metadata"][symbol]
        ccy = meta["currency"]
        rate = 1 if ccy == "EUR" else currency_rates.get(day) if ccy == "USD" else None
        px = lookup(day, symbol, ccy)
        if not px or not rate:
            missing.append(symbol)
            continue
        values[symbol] = qty * px * rate
        nav += values[symbol]
        sectors[meta["sector"] or "inconnu"] += values[symbol]
        countries[meta["country"] or "inconnu"] += values[symbol]
    return dict(nav=None if missing else nav, values=values, missing=missing,
                sectors={} if missing else {k:v/nav for k,v in sectors.items()},
                countries={} if missing else {k:v/nav for k,v in countries.items()})


def window(series, expected, previous, capital, variant, obs, contract, frozen_runs, wstart):
    empty = dict(sessions=len(expected), complete=False, maturity=0, net=None, drawdown=None,
                 comparisons={}, stress={}, weight_dates={}, missing=[])
    if not expected:
        return empty
    marks = {x["day"]: x for x in series}
    if any(d not in marks for d in expected) or (previous and previous not in marks):
        return dict(empty, missing=["revue absente"])
    bench = {x["date"]: x for x in obs["bench"]}
    anchor = previous or contract["anchor"]
    # Use absolute NAV for the trial and chain the two non-overlapping windows at their common boundary.
    days = [anchor, *expected]
    fees = [0. if d < contract["start"] else operating_cost(contract["start"], d, obs) for d in days]
    costs_ok = all(v is not None for v in fees)
    def curve(mode):
        vals = [capital if not previous else marks[previous]["marked"][variant][mode]["nav"]]
        vals += [marks[d]["marked"][variant][mode]["nav"] for d in expected]
        return [v - fee if v is not None and fee is not None else None for v, fee in zip(vals, fees)]
    def ret(values):
        return values[-1]/values[0]-1 if None not in values and values[0] > 0 else None
    vals = curve("normal")
    net = ret(vals)
    out = dict(empty, net=net, drawdown=max_drawdown(vals), missing=[])
    if not costs_ok:
        out["missing"].append("coûts API incomplets")
    if any(marks[d]["marked"][variant]["normal"]["nav"] is None for d in expected):
        out["missing"].append("clôtures ou changes manquants")
    for key in ("world","matched","basket","account"):
        levels = [(1+bench[d][key]) if d in bench and bench[d].get(key) is not None else None for d in days]
        if key == "account":
            # An errored or unrecorded AG1 session is not a zero-return day of the frozen strategy.
            if not all(frozen_runs.get(d) for d in expected) or not costs_ok or not bench.get(anchor,{}).get("nav"):
                levels = [None] * len(days)
            elif levels[0] and costs_ok and bench[anchor].get("nav"):
                base = bench[anchor]["nav"]
                period_cost = [0, *[operating_cost(wstart, d, obs) for d in expected]]
                levels = [l/levels[0]*base - cost if l is not None and cost is not None else None
                          for l,cost in zip(levels,period_cost)]
        r = ret(levels)
        out["comparisons"][key] = dict(net=r, excess=net-r if net is not None and r is not None else None,
                                      drawdown=max_drawdown(levels))
        if r is None:
            out["missing"].append("témoin " + key)
    for mode in ("cout_double","retard"):
        out["stress"][mode] = ret(curve(mode))
    matched = [1.]
    for before,after in zip(days,days[1:]):
        a=bench.get(before,{}).get("world")
        b=bench.get(after,{}).get("world")
        opening=marks.get(before)
        holding=opening["marked"][variant]["normal"] if opening else None
        exposure=(sum(holding["values"].values())/holding["nav"] if holding and holding["nav"] else
                  0. if before==contract["anchor"] else None)
        matched.append(matched[-1]*(1+exposure*((1+b)/(1+a)-1))
                       if None not in (matched[-1],a,b,exposure) else None)
    shadow=ret(matched)
    out["comparisons"]["variant_matched"]=dict(net=shadow,excess=net-shadow if None not in (net,shadow) else None,
                                               drawdown=max_drawdown(matched))
    if shadow is None:
        out["missing"].append("fonds mondial à l’exposition de la variante")
    contributors = defaultdict(float)
    prior = marks[previous]["marked"][variant]["normal"]["values"] if previous else {}
    dates = {}
    for idx,day in enumerate(expected):
        row = marks[day]
        marked = row["marked"][variant]["normal"]
        trades = row["books"][variant]["normal"]["new_trades"]
        for sym in set(marked["values"]) | set(prior):
            contributors[sym] += marked["values"].get(sym,0) - prior.get(sym,0)
        for t in trades:
            contributors[t["symbol"]] += (1 if t["side"]=="SELL" else -1)*t["nominal"] - t["cost"]
        prior = marked["values"]
        if vals[idx] is not None and vals[idx+1] is not None:
            dates[day] = vals[idx+1]-vals[idx]
    best = max(contributors, key=contributors.get, default=None)
    out["best"] = best
    out["stress"]["sans_meilleur"] = ((vals[-1]-max(0,contributors.get(best,0)))/vals[0]-1
                                     if net is not None else None)
    out["weight_dates"] = dict(sorted(dates.items(),key=lambda x:-abs(x[1]))[:5])
    out["geography"] = marks[expected[-1]]["marked"][variant]["normal"]["countries"]
    out["sectors"] = marks[expected[-1]]["marked"][variant]["normal"]["sectors"]
    out["complete"] = not out["missing"] and all(v is not None for v in out["stress"].values())
    return out
