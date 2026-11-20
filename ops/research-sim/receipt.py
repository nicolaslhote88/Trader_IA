"""Acceptance cases, isolated from the prospective books and their returns."""

from copy import deepcopy
from common import digest
from engine import book, execute, replay, settle
from strategies import review


def checks(audit, batch, rows, cfg, conv, cal):
    at = "2026-11-20T16:18:00+00:00"

    def stock(symbol, score, **changes):
        r = dict(
            symbol=symbol,
            contract=symbol,
            sector="Industrie",
            score=score,
            fund_at=at,
            fund_status="OK",
            fund_source="cas de recette",
            tech_at=at,
            ai="APPROVE",
            scoped=True,
            segments=["TEST"],
            product="action",
            base_universe=True,
            permission_day="2026-05-06",
            lot=1,
            minimum=1,
            fx=1,
            quote_time=at,
            received=at,
            availability="R",
            currency="EUR",
            quote_currency="EUR",
            place="SBF",
            issuer="NL",
            bid=99.95,
            ask=100,
            volume=1000000,
        )
        return dict(r, **changes)

    candidates = [
        stock("CHER", 100, bid=4999, ask=5000),
        stock("RARE", 90, sector="Secteur isolé"),
        stock("VETO", 85, ai="REJECT"),
        stock("PERIME", 84, fund_at="2026-10-01T00:00:00Z"),
        *[stock("ACTION" + str(n), 70 - n, sector="Secteur " + str(n % 3)) for n in range(7)],
    ]

    def run():
        return [
            review(book(cfg["capital_eur"]), v, candidates, candidates, at, cfg, conv, cal)
            for v in cfg["variants"]
        ]

    rr = run()
    ranked = {r["symbol"]: r for r in rr[0]["alternatives"]}
    cases = []

    def case(name, ok, detail):
        cases.append(dict(name=name, ok=bool(ok), detail=detail))

    case(
        "Photographie du 18 novembre",
        len(audit["rows"]) == 566
        and sum(bool(r["scoped"]) for r in audit["rows"]) == 364
        and sum(bool(r["pre"]) for r in audit["rows"]) == 260,
        "566 entrées, 364 dans le périmètre et 260 présélectionnées",
    )
    case(
        "Suivi des lignes détenues",
        any("HELD" in r["segments"] and not r["pre"] for r in audit["rows"]),
        "Le titre détenu conserve son segment malgré une présélection négative",
    )
    live = {r["symbol"]: r for r in rows}
    case(
        "SAP en dollars",
        any(r.get("place") == "NYSE" and r.get("currency") == "USD" for r in [live.get("SAP", {})]),
        "Contrat américain et devise de négociation",
    )
    case(
        "Produits CAC à levier",
        all(live.get(s, {}).get("product") == "levier quotidien" for s in ["LVC.PA", "BX4.PA"]),
        "LVC.PA et BX4.PA",
    )
    japan = next((r for r in rows if r["symbol"].endswith(".T") and r.get("lot") == 100), {})
    odd = execute(book(10000), japan, "BUY", 1, at, cfg, conv, cal) if japan else {}
    case(
        "Lot japonais",
        odd.get("reason") == "lot minimal ou pas de quantité",
        (japan.get("symbol") or "absent") + " · ordre fictif de 1 pour un lot de 100",
    )
    r = candidates[1]
    future = execute(book(10000), dict(r, quote_time="2026-11-20T16:18:01Z"), "BUY", 1, at, cfg, conv, cal)
    absent = execute(book(10000), dict(r, quote_time=None), "BUY", 1, at, cfg, conv, cal)
    lim = execute(book(10000), r, "BUY", 1, at, cfg, conv, cal, limit=98)
    case(
        "Rejets des prix futurs, absents et hors limite",
        future["reason"] == "prix ou observation futurs"
        and absent["reason"] == "cotation absente ou périmée"
        and lim["reason"] == "limite non atteinte",
        "Trois cotations contrôlées",
    )
    partial = next(
        (
            p
            for p in batch.get("partial", [])
            if p["symbol"] == "ELIS.PA" and p["date"].replace("-", "") == "20261104"
        ),
        None,
    )
    if partial:
        raw = partial["date"].replace("-", "")
        day = raw[:4] + "-" + raw[4:6] + "-" + raw[6:]
        when = day + "T15:00:00Z"
        p = stock(
            partial["symbol"],
            80,
            bid=partial["price"],
            ask=partial["price"] + 0.01,
            bid_size=partial["sold"],
            received=when,
            quote_time=when,
        )
        wallet = book(0, {p["symbol"]: partial["initial"]})
        noquote = execute(
            deepcopy(wallet), dict(p, quote_time=None), "SELL", partial["sold"], when, cfg, conv, cal
        )
        sale = execute(wallet, p, "SELL", partial["initial"], when, cfg, conv, cal)
        repeat = execute(wallet, p, "SELL", wallet["positions"].get(p["symbol"], 0), when, cfg, conv, cal)
        locked = wallet["cash"] == 0 and bool(wallet["unsettled"])
        due = wallet["unsettled"][0]["day"] if locked else day
        settle(wallet, due + "T12:00:00Z")
        detail = f"{partial['symbol']} · {partial['sold']:g} vendues sur {partial['initial']:g}. Prix du relevé, profondeur de test. Fonds disponibles le {due}."
        case(
            "Vente partielle et règlement",
            sale["status"] == "partiel"
            and sale["filled"] == partial["sold"]
            and repeat["filled"] == 0
            and locked
            and wallet["cash"] > 0
            and noquote["filled"] == 0,
            detail,
        )
    else:
        case("Vente partielle et règlement", False, "Lot Flex ELIS du 4 novembre manquant")
    case(
        "Premier rang trop cher",
        ranked["CHER"]["rank"] == 1
        and "ticket supérieur" in ranked["CHER"]["reason"][0]
        and ranked["RARE"].get("order", {}).get("filled", 0) > 0,
        "Le rang suivant entre dans le budget",
    )
    case(
        "Secteur peu représenté",
        ranked["RARE"].get("order", {}).get("filled", 0) > 0,
        "Un seul titre dans son secteur",
    )
    case(
        "Score périmé et veto AG2",
        all(next(x for x in v["alternatives"] if x["symbol"] == "PERIME")["rank"] is None for v in rr)
        and all(
            (next(x for x in v["alternatives"] if x["symbol"] == "VETO")["rank"] is None)
            == v["variant"].endswith("_veto")
            for v in rr
        ),
        "Score trop ancien écarté dans les quatre variantes. Le veto agit dans les deux variantes prévues.",
    )
    case(
        "Quatre portefeuilles reproductibles",
        digest(rr) == digest(run())
        and len(rr) == 4
        and len({x["at"] for x in rr}) == 1
        and all(v["fees"] > 0 for v in rr),
        "Même jeu de données contrôlé, frais inclus",
    )
    delayed = [dict(x, quote_time="2026-11-20T16:20:00Z") for x in candidates]
    unfilled = review(book(cfg["capital_eur"]), "ag3", candidates, delayed, at, cfg, conv, cal)
    case(
        "Décisions figées avant le prix d’arrivée",
        all(
            a.get("decision") == b.get("decision")
            for a, b in zip(rr[0]["alternatives"], unfilled["alternatives"])
        )
        and not unfilled["positions"],
        "Une arrivée sans prix recevable laisse les mêmes instructions et aucun achat",
    )
    archived = replay(batch, cfg, conv, cal)
    case(
        "Décisions archivées reproductibles",
        archived == replay(batch, cfg, conv, cal)
        and any(not e["orders"] and e["dispatch"] for e in archived["runs"])
        and any(o["actual_qty"] and o.get("side") == "BUY" for e in archived["runs"] for o in e["orders"]),
        "Empreintes des entrées et des réponses conservées. Une session sans ordre et un achat Flex.",
    )
    return dict(cases=cases, fixture_hash=digest(rr), passed=sum(x["ok"] for x in cases), total=len(cases))
