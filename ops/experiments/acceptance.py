from news_study import evidence, evaluate
from opinions import model_state, decision_sets
from engine import book, execute


def check(report, p, cfg, conv, cal):
    checks = []

    def add(k, ok):
        checks.append(dict(test=k, ok=bool(ok)))
        if not ok:
            raise ValueError(k)

    out = report["exits"]
    rows = out["rows"]
    add(
        "Cohorte historique et exclusions",
        out["approved"] == 75 and out["evaluated"] == 64 and all(x["excluded"] or x["arms"] for x in rows),
    )
    add("Panier du 30 octobre", abs(out["basket"]["hold"] - 0.015601052731214442) < 1e-7)
    legs = out["prosus"]
    add(
        "Aller-retour Prosus",
        any(x["side"] == "BUY" and float(x["qty"]) == 28 for x in legs)
        and any(x["side"] == "SELL" and float(x["qty"]) == 28 for x in legs),
    )
    forced = [r["arms"] for r in rows if r["arms"] and r["reason"] in ("risque", "liquidités")]
    add(
        "Même garde en conservation et dans la bande",
        bool(forced) and all(v["conserver"] == v["bande"] == v["réduire"] for v in forced),
    )

    def article(i, channel, time, correct=False):
        return dict(
            id=str(i),
            label=str(i),
            symbol="FIX",
            company="Fixture",
            source=channel,
            title=("Correction: " if correct else "") + "Fixture reports quarterly results Q3",
            pub=time,
            seen=time,
            analyzed=time,
            updated=time,
            created=time,
            in_digest=True,
            impact=4,
            canonical_url="https://example.invalid/issuer/q3-results",
            url=None,
        )

    t = "2026-11-17T10:00:00+00:00"
    close = "2026-11-17T16:40:00+00:00"
    rs = [article(i, ch, t) for i, ch in enumerate(["issuer_rss", "ibkr", "finnhub"])]
    rs.append(article(4, "ibkr", close, True))
    early = evidence(rs, "2026-11-17T16:30:00+00:00", p)
    late = evidence(rs, close, p)
    add(
        "Trois reprises et source officielle",
        len(early["events"]) == 1
        and len(early["events"][0]["sources"]) == 3
        and bool(early["events"][0]["official"]),
    )
    add(
        "Correction après clôture",
        not early["events"][0]["corrections"] and late["events"][0]["corrections"] == ["4"],
    )
    diff = [
        dict(
            rs[0], id="a", canonical_url="https://finnhub.io/api/news?id=a", title="Fixture quarterly results"
        ),
        dict(
            rs[1],
            id="b",
            canonical_url="https://finnhub.io/api/news?id=b",
            title="Fixture files for bankruptcy",
        ),
    ]
    add(
        "Identifiants Finnhub distincts malgré le chemin partagé",
        len(evidence(diff, close, p)["events"]) == 2,
    )
    m = evaluate(late, rs, {}, dict(rows=[], holdings={"SANS-ARTICLE": 1}))
    add("Détenu sans nouvelle visible", m["coverage"][0]["missing"] == ["SANS-ARTICLE"])
    good = dict(requested="fixture", response_hash="hash", size=10, valid=True, actions=[])
    add(
        "Indisponibilité, réponse invalide et entrée trop longue",
        model_state(None, p) == "indisponible"
        and model_state(dict(good, valid=False), p) == "réponse invalide"
        and model_state(dict(good, size=p["max_pack_bytes"] + 1), p) == "paquet trop volumineux",
    )
    tcase = {"models": [], "votes": [], "decisions": []}
    d = {"rule": {("FIX", "BUY")}, "triggers": {"FIX"}}
    add(
        "Repli sur la règle et pas d’appariement inventé",
        decision_sets(tcase, d, p)[0]["B"] == d["rule"] and not decision_sets(tcase, d, p)[1]["paired"],
    )
    r = dict(
        symbol="FIX",
        contract="1",
        place="SBF",
        currency="EUR",
        quote_currency="EUR",
        issuer="NL",
        permission_day="2026-05-06",
        lot=1,
        minimum=1,
        quote_time=t,
        received=t,
        availability="R",
        bid=99.9,
        ask=100,
        fx=1,
        volume=1000000,
    )
    b = book(0, {"FIX": 10})
    sell = execute(b, r, "SELL", 10, t, cfg, conv, cal)
    buy = dict(r, symbol="ALT", contract="2")
    bad = execute(b, buy, "BUY", 9, t, cfg, conv, cal)
    later = "2026-11-19T10:00:00+00:00"
    success = execute(b, dict(buy, quote_time=later, received=later), "BUY", 9, later, cfg, conv, cal)
    missing = execute(book(2000), dict(buy, quote_time=None), "BUY", 2, t, cfg, conv, cal)
    add(
        "Remplacement financé après règlement, deux commissions",
        sell["filled"] == 10
        and bad["filled"] == 0
        and success["filled"] == 9
        and sell["cost"] > 0
        and success["cost"] > 0,
    )
    add(
        "Prix manquant et session absente",
        missing["filled"] == 0
        and not execute(book(2000), r, "BUY", 2, "2026-11-21T15:00:00Z", cfg, conv, cal)["filled"],
    )
    add("Isolement du processus", all(report["network"].values()))
    return checks
