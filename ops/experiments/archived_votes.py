"""Historical idea-level diagnostic. Aggregate repeated signals on the same symbol, date and side."""

from collections import defaultdict
from statistics import mean, median


def diagnose(h, p):
    by = defaultdict(set)
    models = defaultdict(dict)
    for m in h["models"]:
        models[m["run_id"]][m["model_key"]] = m
    for x in h["votes"]:
        by[(x["run_id"], x["symbol"], x["side"])].add(x["model_key"])
    series = defaultdict(lambda: defaultdict(list))
    dates = set()
    for x in h["ideas"]:
        run = x["run"]
        s = x["symbol"]
        side = x["side"]
        day = x["d"]
        votes = by[(run, s, side)]
        if len(models[run]) != 3 or not all(a["parse_ok"] for a in models[run].values()):
            continue
        first = p["model_key"] in votes
        approved = x["status"] == "CONSENSUS_APPROVED"
        pair = p["second_key"] in votes
        for title, on in [
            ("Premier avis", first),
            ("Accord premier et deuxième", first and pair),
            ("Consensus approuvé", approved),
            ("Premier avis écarté par le consensus", first and not approved),
            ("Consensus sans le premier avis", approved and not first),
        ]:
            if on:
                series[title, side][day, s].append(float(x["signed"]))
                dates.add(day)
    out = []
    for (k, side), g in sorted(series.items()):
        vals = [mean(v) for v in g.values()]
        out.append(
            dict(
                variant=k,
                side=side,
                observations=sum(map(len, g.values())),
                ideas=len(g),
                days=len({d for d, s in g}),
                signed_mean=mean(vals),
                signed_median=median(vals),
                positive_fraction=mean(v > 0 for v in vals),
            )
        )
    return dict(start=min(dates) if dates else None, end=max(dates) if dates else None, rows=out)
