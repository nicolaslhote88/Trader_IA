from bisect import bisect_right, bisect_left
from collections import defaultdict
from datetime import date, timedelta
from math import ceil, floor
from statistics import mean
from common import moment
from engine import fees


def recent(rates, day):
    keys = sorted(rates)
    i = bisect_right(keys, day) - 1
    if i < 0 or (date.fromisoformat(day) - date.fromisoformat(keys[i])).days > 4:
        return None
    return rates[keys[i]]


def settle_day(day):
    t = date.fromisoformat(day)
    for _ in range(2):
        t += timedelta(days=1)
        while t.weekday() > 4:
            t += timedelta(days=1)
    return t.isoformat()


def historical(hist, base, feed, policy, conv):
    px = hist["prices"]
    ccys = hist["currencies"]
    fx = hist["fx"]
    scores = defaultdict(dict)
    for sym, day, score in hist["signal"]:
        scores[sym][day] = score
    perms = {p["symbol"]: p for p in base["permissions"]}
    daily = {r["d"]: r for r in hist["daily"]}
    positions = defaultdict(dict)
    for p in hist["positions"]:
        positions[p["d"]][p["symbol"]] = p

    def eur(sym, day):
        native = px.get(sym, {}).get(day)
        if not native:
            return None
        rate = 1 if ccys.get(sym) == "EUR" else recent(fx, day)
        return native / rate if rate else None

    def stock(sym, day):
        p = perms.get(sym, {})
        if not p.get("day") or p["day"] >= day or ccys.get(sym) != p["currency"]:
            return None
        rate = 1 if ccys[sym] == "EUR" else recent(fx, day)
        return (
            dict(symbol=sym, currency=ccys[sym], fx=1 / rate, place=p["place"], issuer=p.get("country"))
            if rate
            else None
        )

    def prior(mapping, day):
        keys = sorted(mapping)
        i = bisect_left(keys, day) - 1
        return mapping[keys[i]] if i >= 0 else None

    audit = {(r["run"], r["symbol"]): r for r in hist["evaluations"]}
    purchases = defaultdict(list)
    for x in hist["decisions"]:
        if x["side"] == "BUY" and x["status"] == "CONSENSUS_APPROVED":
            purchases[x["run_id"]].append(x)
    output = []
    for x in hist["decisions"]:
        if x["side"] != "SELL" or x["status"] != "CONSENSUS_APPROVED":
            continue
        sym = x["symbol"]
        day = x["ts"][:10]
        run = x["run_id"]
        ses = sorted(d for d in px.get(sym, {}) if d > day)
        av = audit.get((run, sym))
        pday = prior(positions, day) or {}
        acc = prior(daily, day) or {}
        pos = pday.get(sym, {})
        nav = float(acc.get("total_value_eur") or 0)
        cash = float(acc.get("cash_eur") or 0)
        mv = float(pos.get("market_value_eur") or 0)
        sector = hist["universe"].get(sym)
        sector_mv = sum(
            float(r["market_value_eur"])
            for s, r in pday.items()
            if sector and hist["universe"].get(s) == sector
        )
        weight = mv / nav if nav else None
        score = prior(scores[sym], day)
        reason = (
            "risque"
            if nav and (mv / nav > policy["max_line"] or sector_mv / nav > policy["max_sector"])
            else "liquidités"
            if nav and cash / nav < policy["cash_floor"]
            else "fondamental"
            if score is not None and score <= policy["det_score"]
            else "remplacement"
        )
        qty = float(x["selected_qty"] or 0)
        source = "proposition"
        if not qty:
            qty = float(pos.get("qty") or 0)
            source = "position de la veille"
            if x["action"] != "CLOSE":
                qty = floor(qty / 2)
                source = "demi-position de la veille (hypothèse)"
        target = next(
            (
                v
                for v in sorted(purchases[run], key=lambda r: (-r["vote_count"], r["symbol"]))
                if v["symbol"] != sym and v["symbol"] not in pday
            ),
            None,
        )
        out = dict(
            day=day,
            run=run,
            symbol=sym,
            action=x["action"],
            reason=reason,
            qty=qty or None,
            quantity_source=source,
            weight=weight,
            score=score,
            replacement=target["symbol"] if target else None,
            audited=av is not None,
            excess_world=float(av["excess"]) if av else None,
            arms={},
            excluded=None,
        )
        output.append(out)
        if not av:
            out["excluded"] = (
                "20 séances postérieures absentes"
                if len(ses) < 21
                else "série ou écart non retenu dans l’audit"
            )
            continue
        start, end = ses[0], ses[20]
        out.update(start=start, end=end)
        prices = [eur(sym, d) for d in ses[:21]]
        if not all(prices):
            out["excluded"] = "clôture ou change absent"
            continue
        v0, v1 = prices[0], prices[-1]
        out["hold_return"] = v1 / v0 - 1
        bench0 = recent(hist["benchmark"], start)
        bench1 = recent(hist["benchmark"], end)
        if not bench0 or not bench1 or abs((v1 / v0 - bench1 / bench0) - out["excess_world"]) > 1e-6:
            raise ValueError("Parité de l’audit " + sym + " " + day)
        out["hold_worst"] = min(prices) / v0 - 1
        inst = stock(sym, start)
        if not qty or not inst:
            out["excluded"] = "quantité ou accès antérieur au contrat non documenté"
            continue
        if not pos or qty > float(pos.get("qty") or 0):
            out["excluded"] = "position initiale insuffisamment documentée à la veille"
            continue
        sf = fees(inst, "SELL", qty, v0 / inst["fx"], start, conv)
        if not sf:
            out["excluded"] = "barème de vente absent"
            continue
        hold = [qty * p for p in prices]
        proceeds = qty * v0 - sf["cost"]
        out["notional"] = qty * v0

        def arm(path, cost, sold, bought=0, info=None):
            return dict(
                delta=path[-1] - hold[-1],
                drawdown_avoided=(min(path) - min(hold)),
                fees=cost,
                sale_proceeds=proceeds if sold else 0,
                cash_used=bought,
                weight_removed=qty * v0 / nav if sold and nav else 0,
                detail=info,
            )

        sell = [proceeds] * len(prices)
        no = arm(hold, 0, False)
        yes = arm(sell, sf["cost"], True)
        forced = reason in ("risque", "liquidités")
        out["arms"] = {
            "conserver": yes if forced else no,
            "réduire": yes,
            "bande": yes if forced or (nav and qty * v0 / nav >= policy["band_nav"]) else no,
        }
        if not target:
            out["replacement_gap"] = "aucun achat approuvé au même passage"
            continue
        other = target["symbol"]
        eligible = stock(other, day)
        after = next((d for d in ses[:21] if d >= settle_day(start) and eur(other, d)), None)
        # A prior fill attests access. Board lot and an executable quote must be evidenced separately.
        if not eligible or not after:
            out["replacement_gap"] = "contrat antérieur ou prix après règlement absent"
            continue
        rule_events = [e for e in base["events"] if moment(e["end"]) < moment(x["ts"])]
        known = next(
            (q for e in reversed(rule_events) if (q := e["quotes"].get(other, {})).get("lot_known")), None
        )
        step = known["step"] if known and known.get("conid") == perms[other]["conid"] else None
        minimum = known["minimum"] if step else None
        filled_ids = {
            f["order_id"]
            for f in hist["fills"]
            if f["symbol"] == other
            and f["currency"] == ccys.get(other)
            and moment(f["ts_fill"]) < moment(x["ts"])
        }
        past_sizes = sorted(
            {
                float(o["qty"])
                for o in hist.get("buy_orders", [])
                if o["symbol"] == other and o["order_id"] in filled_ids
            },
            reverse=True,
        )
        if not step and not past_sizes:
            out["replacement_gap"] = "aucune taille d’ordre acceptée avant décision"
            continue
        pr = eur(other, after)
        ot = stock(other, after)
        budget = min(
            proceeds,
            max(0, nav * policy["entry_weight"]),
            max(
                0,
                nav * policy["max_sector"]
                - sum(
                    float(r["market_value_eur"])
                    for s, r in pday.items()
                    if hist["universe"].get(s) == hist["universe"].get(other)
                )
                + (qty * v0 if sector == hist["universe"].get(other) else 0),
            ),
        )
        budget = min(budget, max(0, cash + proceeds - nav * policy["cash_floor"]))
        if len(pday) - (qty >= float(pos["qty"])) >= policy["max_positions"]:
            out["replacement_gap"] = "nombre maximal de positions après vente"
            continue
        sizes = (
            (i * step for i in range(floor(budget / pr / step), ceil(minimum / step) - 1, -1))
            if step
            else past_sizes
        )
        n = 0
        fee = None
        for candidate in sizes:
            cost = fees(ot, "BUY", candidate, pr / ot["fx"], after, conv)
            if cost and candidate * pr + cost["cost"] <= budget:
                n = candidate
                fee = cost
                break
        if not n:
            out["replacement_gap"] = "cash, concentration ou tarif insuffisant"
            continue
        out["replacement_size_source"] = (
            "lot de la fiche antérieure" if step else "taille exacte d’un ordre antérieur exécuté"
        )
        path = []
        spent = n * pr + fee["cost"]
        for d in ses[:21]:
            p = eur(other, d)
            if d >= after and not p:
                break
            path.append(proceeds if d < after else proceeds - spent + n * p)
        if len(path) != len(prices):
            out["replacement_gap"] = "valorisation du remplaçant incomplète"
            continue
        out["arms"]["remplacer"] = arm(path, sf["cost"] + fee["cost"], True, spent, after)
    # Independently rebuild the October basket, including its unchanged cash.
    d0, d1, nav = hist["frozen"][:3]
    effects = {
        s: float(p["market_value_eur"]) * (eur(s, d1) / eur(s, d0) - 1) for s, p in positions[d0].items()
    }
    basket = sum(effects.values()) / nav
    if abs(basket - hist["frozen"][6]) > 1e-7:
        raise ValueError("Panier figé")
    prx = [
        dict(day=x["date"], side=x["side"], qty=x["qty"], price=x["price"], fee=x["fee"])
        for x in feed["flex"]
        if x["symbol"] == "PRX.AS" and "20261112" <= x["date"] <= "20261117"
    ]
    return dict(
        rows=output,
        approved=len(output),
        evaluated=sum(x["audited"] for x in output),
        mean_excess=mean(x["excess_world"] for x in output if x["audited"]),
        basket=dict(start=d0, end=d1, hold=basket, account=hist["frozen"][7], constituents=effects),
        prosus=prx,
    )
