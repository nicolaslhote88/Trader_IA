from collections import defaultdict
from datetime import timedelta
from math import ceil, floor
from common import moment, fresh
from engine import book, execute, fees


def sdict(actions):
    return {(a["symbol"], a["side"]) for a in actions if a.get("side") in ("BUY", "SELL")}


def model_state(m, policy):
    if not m or not m.get("requested") or not m.get("response_hash"):
        return "indisponible"
    if m.get("size", 0) > policy["max_pack_bytes"]:
        return "paquet trop volumineux"
    if not m.get("valid") or not isinstance(m.get("actions"), list):
        return "réponse invalide"
    return "valide"


def dossier(t, feed, base, view, policy):
    pf = t["portfolio"]
    nav = float(pf["totalValueEUR"])
    cash = float(pf["cashEUR"])
    held = {p["symbol"]: p for p in pf["positions"]}
    sectors = defaultdict(float)
    for x in held.values():
        sectors[x.get("sector")] += float(x.get("marketValue") or 0)
    scores = {}
    for x in feed["fundamentals"]:
        if fresh(x["ts"], t["at"], 7 * 86400):
            scores[x["symbol"]] = x["score"]
    rows = {r["symbol"]: r for r in t["rows"]}
    forced = {}
    rule = set()
    triggers = set()
    reasons = {}
    required = {}
    projected = sectors.copy()
    remaining = max(0, nav * policy["cash_floor"] - cash)
    available = [s for s in held if s in rows]
    for s in sorted(available, key=lambda s: (-(float(held[s].get("marketValue") or 0)), s)):
        pos = held[s]
        mv = float(pos.get("marketValue") or 0)
        sec = pos.get("sector")
        amount = min(
            mv, max(0, mv - nav * policy["max_line"], projected.get(sec, 0) - nav * policy["max_sector"])
        )
        if amount > 0:
            forced[s] = "risque"
            required[s] = amount
            projected[sec] -= amount
            remaining = max(0, remaining - amount)
    for s in sorted(available, key=lambda s: (scores.get(s, 101), s)):
        if remaining <= 0:
            break
        if s in forced:
            continue
        amount = min(remaining, float(held[s].get("marketValue") or 0))
        if amount > 0:
            forced[s] = "liquidités"
            required[s] = amount
            remaining -= amount
    for s, r in rows.items():
        p = held.get(s)
        score = scores.get(s)
        if p:
            if s not in forced and score is not None and score <= policy["det_score"]:
                reasons[s] = "fondamental"
            if s in forced or s in reasons:
                rule.add((s, "SELL"))
                triggers.add(s)
        elif (
            score is not None
            and score >= policy["score_buy"]
            and (r.get("buy_feasibility") or {}).get("feasible")
        ):
            rule.add((s, "BUY"))
        if any(fresh(e["first"], t["at"], 7 * 86400) for e in view["pack"].get(s, [])):
            triggers.add(s)
    return dict(
        nav=nav,
        cash=cash,
        held=held,
        sectors=sectors,
        rows=rows,
        scores=scores,
        forced=forced,
        required=required,
        rule=rule,
        triggers=triggers,
        reasons=reasons,
    )


def instrument(sym, r, e, base, at):
    liq = r.get("liquidity") or {}
    q = e.get("quotes", {}).get(sym, {})
    cid = str(liq.get("conid") or "")
    p = next((p for p in base["permissions"] if p["conid"] == cid and p["day"] < at[:10]), {})
    result = dict(
        symbol=sym,
        contract=cid,
        place=p.get("place"),
        issuer=p.get("country"),
        currency=p.get("currency"),
        quote_currency=liq.get("currency"),
        fx=liq.get("fxRateToEUR"),
        lot=q.get("step") if q.get("lot_known") else None,
        minimum=q.get("minimum") if q.get("lot_known") else None,
        permission_day=p.get("day"),
        bid=q.get("bid"),
        ask=q.get("ask"),
        quote_time=q.get("quoteTime"),
        received=q.get("received"),
        availability=q.get("ibkrDataAvailability"),
        volume=q.get("volume"),
    )
    frames = [
        x
        for x in base.get("frames", [])
        if x["conid"] == cid and x.get("received") and moment(x["received"]) <= moment(at)
    ]
    if frames:
        f = max(frames, key=lambda x: x["received"])
        if not result["received"] or moment(f["received"]) > moment(result["received"]):
            result.update(
                bid=f["bid"],
                ask=f["ask"],
                quote_time=f["quoteTime"],
                received=f["received"],
                availability=f["ibkrDataAvailability"],
            )
    return result


def fill_arm(t, d, proposal, base, policy, cfg, conv, cal, veto=False):
    at = (moment(t["end"]) + timedelta(seconds=cfg["latency_s"])).isoformat()
    e = next((x for x in base["events"] if x["run"] == t["run"]), {})
    held = d["held"]
    nav = d["nav"]
    wallet = book(d["cash"], {s: float(p["quantity"]) for s, p in held.items()})
    requests = set(proposal) | {(s, "SELL") for s in d["forced"]}
    sectors = d["sectors"].copy()
    available = d["cash"]
    lines = len(held)
    decisions = []
    for s, side in sorted(requests, key=lambda x: (x[1] != "SELL", -(d["scores"].get(x[0]) or 0), x[0])):
        row = dict(
            symbol=s,
            side=side,
            reason=d["forced"].get(s) or d["reasons"].get(s) or "remplacement",
            requested=None,
            filled=0,
        )
        decisions.append(row)
        r = d["rows"].get(s)
        if not r:
            row["reject"] = "absent du pack"
            continue
        inst = instrument(s, r, e, base, at)
        lot = inst["lot"]
        fx = inst["fx"] or 0
        if side == "BUY" and (s in held or (s, "SELL") in requests):
            row["reject"] = "détenu ou vente simultanée"
            continue
        tech_age = [r.get("data_age_h1_hours"), r.get("data_age_d1_hours")]
        tech_ok = (
            bool(r.get("ai_decision"))
            and r["ai_decision"] != "REJECT"
            and all(v is not None and 0 <= float(v) <= cfg["max_tech_age_h"] for v in tech_age)
        )
        if side == "BUY" and veto and not tech_ok:
            row["reject"] = "veto AG2 ou avis absent"
            continue
        if side == "SELL" and s not in held:
            row["reject"] = "position absente"
            continue
        if not lot or not fx or not inst["minimum"]:
            row["reject"] = "lot ou change inconnu"
            continue
        sec = r.get("sector")
        px = (r.get("liquidity") or {}).get("ask") if side == "BUY" else (r.get("liquidity") or {}).get("bid")
        if not px or px <= 0:
            row["reject"] = "prix absent"
            continue
        if side == "BUY":
            if (
                not (r.get("buy_feasibility") or {}).get("feasible")
                or not sec
                or lines >= policy["max_positions"]
            ):
                row["reject"] = "faisabilité d’achat ou nombre de lignes"
                continue
            budget = min(
                nav * policy["entry_weight"],
                nav * policy["max_sector"] - sectors.get(sec, 0),
                available - nav * policy["cash_floor"],
            )
            n = floor(max(0, budget) / (px * fx * lot)) * lot
            while n >= inst["minimum"]:
                fee = fees(inst, "BUY", n, px, at[:10], conv)
                if not fee or n * px * fx + fee["cost"] <= budget:
                    break
                n -= lot
            if n < inst["minimum"] or not fee:
                row["reject"] = "ticket ou tarif"
                continue
            available -= n * px * fx + fee["cost"]
            sectors[sec] = sectors.get(sec, 0) + n * px * fx
            lines += 1
        else:
            qty = float(held[s]["quantity"])
            n = qty
            if s in d["forced"]:
                n = min(qty, max(inst["minimum"], ceil(d["required"][s] / (px * fx * lot)) * lot))
            elif row["reason"] == "liquidités":
                n = min(
                    qty,
                    max(
                        inst["minimum"],
                        ceil(max(0, nav * policy["cash_floor"] - d["cash"]) / (px * fx * lot)) * lot,
                    ),
                )
        row["requested"] = n
        result = execute(
            wallet,
            inst,
            side,
            n,
            at,
            cfg,
            conv,
            cal,
            limit=px if side == "BUY" else None,
            cash_floor=nav * policy["cash_floor"] if side == "BUY" else 0,
        )
        row["filled"] = result["filled"]
        row["fill"] = result
        row["reject"] = result.get("reason")
    return dict(
        decisions=decisions,
        proposed=[list(x) for x in sorted(requests)],
        attempts=sum(x["requested"] is not None for x in decisions),
        fills=sum(x["filled"] > 0 for x in decisions),
        cash=wallet["cash"],
        unsettled=sum(x["amount"] for x in wallet["unsettled"]),
    )


def costs_used(usage, models, run, fx):
    keys = {m["requested"] for m in models if m.get("requested")}
    calls = [
        x
        for x in usage
        if x["run"] == run
        and x["producer"] == "AG1"
        and (x["requested"] in keys or x.get("returned") in keys)
    ]
    complete = (
        bool(keys)
        and keys <= ({x["requested"] for x in calls} | {x.get("returned") for x in calls})
        and all(
            x.get("estimate_usd_high") is not None and x.get("estimate_usd_low") is not None for x in calls
        )
    )
    return dict(
        calls=len(calls),
        tokens_in=sum(x.get("input") or 0 for x in calls),
        tokens_out=sum(x.get("output") or 0 for x in calls),
        low_eur=sum(x["estimate_usd_low"] for x in calls) * fx if complete and fx else None,
        high_eur=sum(x["estimate_usd_high"] for x in calls) * fx if complete and fx else None,
    )


def decision_sets(t, d, p):
    models = {x["key"]: x for x in t["models"]}
    a = d["rule"]
    trigger = d["triggers"]
    f = models.get(p["model_key"])
    opinions = {
        m["key"]: sdict([v for v in t["votes"] if v["model_key"] == m["key"] and v.get("executable")])
        for m in t["models"]
    }
    consensus = sdict([x for x in t["decisions"] if x["status"] == "CONSENSUS_APPROVED"])
    paired = (
        len(models) == 3
        and len({x.get("paired") for x in models.values()}) == 1
        and all(x.get("paired") and model_state(x, p) == "valide" for x in models.values())
    )
    first = opinions.get(p["model_key"], set())
    other = opinions.get(p["second_key"], set())
    b = (
        ({x for x in a if x[0] not in trigger} | {x for x in first if x[0] in trigger})
        if model_state(f, p) == "valide"
        else set(a)
    )
    disputed = {s for s in trigger if {y for x, y in a if x == s} != {y for x, y in first if x == s}}
    b2 = (
        ({x for x in b if x[0] not in disputed} | {x for x in b & other if x[0] in disputed})
        if paired
        else set(b)
    )
    ballot = []
    for sym, side in sorted(set.union(set(), consensus, *opinions.values())):
        ballot.append(
            dict(
                symbol=sym,
                side=side,
                consensus=(sym, side) in consensus,
                supporters=[k for k, v in opinions.items() if (sym, side) in v],
            )
        )
    return dict(
        A=a, B=b, C=consensus, un_avis=first if model_state(f, p) == "valide" else a, deux_avis=b2, A_veto=a
    ), dict(
        paired=paired,
        ballot=ballot,
        triggers=sorted(trigger),
        disputes=sorted(disputed),
        models=[
            dict(
                key=m["key"],
                model=m["requested"],
                status=model_state(m, p),
                pack=m["paired"],
                input=m["input_hash"],
                abstains=not opinions[m["key"]],
            )
            for m in t["models"]
        ],
    )


def outcome(arm, day, closes, base, fee):
    result = {}
    for side in ["BUY", "SELL"]:
        fills = [x for x in arm["decisions"] if x["side"] == side and x["filled"]]
        a = 0
        missing = []
        for x in fills:
            f = x["fill"]
            bars = sorted((d, v) for d, v in closes.get((x["symbol"], f["currency"]), {}).items() if d > day)
            if len(bars) < 20:
                missing.append(x["symbol"])
                continue
            d, pr = bars[19]
            fx = 1 if f["currency"] == "EUR" else base.get("closing_usd", {}).get(d)
            if not fx:
                missing.append(x["symbol"])
                continue
            a += (1 if side == "BUY" else -1) * (pr * fx * x["filled"] - f["nominal"]) - f["cost"]
        result[side] = dict(fills=len(fills), net_eur=None if missing else a, missing=missing)
    gross = (
        None
        if any(x["net_eur"] is None for x in result.values())
        else sum(x["net_eur"] for x in result.values())
    )
    return dict(
        sides=result,
        after_execution=gross,
        after_ai_low=None if gross is None or fee["high_eur"] is None else gross - fee["high_eur"],
        after_ai_high=None if gross is None or fee["low_eur"] is None else gross - fee["low_eur"],
    )
