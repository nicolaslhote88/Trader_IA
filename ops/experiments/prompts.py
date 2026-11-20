from pathlib import Path
import sys

sys.path.insert(0, "/opt/trader-ia/research-sim/current")
from common import load, atomic, digest
from news_study import evidence

DST = Path("/var/lib/trader-research-feed/experiments")


def clean(x):
    if isinstance(x, dict):
        return {k: clean(v) for k, v in x.items() if "ag9" not in k.lower()}
    if isinstance(x, list):
        return [
            clean(v)
            for v in x
            if not (
                isinstance(v, dict)
                and any(
                    "ag9" in str(v.get(k, "")).lower()
                    for k in ("agent", "component", "name", "source", "pillar")
                )
            )
        ]
    if isinstance(x, str) and "ag9" in x.lower():
        return None
    return x


def compact(trace, view, arm):
    pf = trace["portfolio"]
    stocks = []
    for r in trace["rows"]:
        s = r["symbol"]
        if arm == "news_v2":
            news = [x["title"][:160] for x in view["pack"].get(s, [])]
        else:
            news = [x["title"][:160] for x in r.get("news", []) if x.get("title")]
        stocks.append(
            dict(
                symbol=s,
                held=r.get("held"),
                sector=r.get("sector"),
                matrix=r.get("decision"),
                rank=r.get("matrix_rank"),
                tech=r.get("ai_decision"),
                fundamental=r.get("fundamentals", {}).get("scores"),
                constraints=r.get("gates"),
                buy=(r.get("buy_feasibility") or {}).get("feasible"),
                sell=(r.get("sell_feasibility") or {}).get("feasible"),
                news=news,
            )
        )
    ctx = trace["ctx"]
    return dict(
        date=trace["at"],
        cash=pf["cashEUR"],
        nav=pf["totalValueEUR"],
        holdings={x["symbol"]: x["quantity"] for x in pf["positions"]},
        rules=dict(max_line_pct=20, max_sector_pct=30, cash_floor_pct=10),
        stocks=stocks,
        advisory_context=None
        if arm == "no_context"
        else {
            k: v
            for k, v in ctx.items()
            if k
            in (
                "status",
                "use_policy",
                "quality",
                "component_summary",
                "exposure_summary",
                "relevant_currencies",
            )
            and k != "AG9"
        },
    )


def make(capture, rows, protocol, paid):
    jobs = []
    for t in capture["traces"]:
        if t["at"][:10] not in paid["only_dates"]:
            continue
        n = evidence(rows, t["at"], protocol)
        for arm in paid["arms"]:
            payload = compact(t, n, arm)
            payload = clean(payload)
            jobs.append(dict(run=t["run"], day=t["at"][:10], arm=arm, hash=digest(payload), payload=payload))
    return jobs


if __name__ == "__main__":
    here = Path(__file__).resolve().parent
    c = load(sorted((DST / "captures").glob("*.gz"))[-1])
    rows = [r for n in c["articles"] for r in load(DST / "articles" / n)]
    atomic(Path(sys.argv[1]), make(c, rows, load(here / "protocol.json"), load(here / "paid_protocol.json")))
