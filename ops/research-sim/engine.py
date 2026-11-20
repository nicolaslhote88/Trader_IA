"""Pure deterministic execution, no network imports or credentials."""

from datetime import timedelta
from math import floor, isfinite
from common import moment, fresh, digest
from universe import session
from costs import tariff, tax_estimate


def book(cash, positions=None):
    return dict(
        cash=float(cash), positions=dict(positions or {}), unsettled=[], used={}, entries={}, trades=[]
    )


def settle(b, at):
    due = [r for r in b["unsettled"] if r["day"] <= at[:10]]
    b["cash"] = round(b["cash"] + sum(r["amount"] for r in due), 8)
    b["unsettled"] = [r for r in b["unsettled"] if r["day"] > at[:10]]


def fees(r, side, qty, px, day, conv):
    nominal = qty * px * r["fx"]
    fee = tariff(r.get("place"), r["currency"], qty, nominal, r["fx"])
    if fee is None:
        return None
    tax = tax_estimate(r["symbol"], side, day, r.get("issuer"), nominal)
    reserve = nominal * conv["unknown_tax_reserve"] if tax is None else 0
    exchange = nominal * conv["fx_spread_each_conversion"] if r["currency"] != "EUR" else 0
    return dict(
        nominal=round(nominal, 8),
        commission=round(fee, 8),
        tax=tax,
        reserve=round(reserve, 8),
        fx_cost=round(exchange, 8),
        cost=round(fee + (tax or 0) + reserve + exchange, 8),
    )


def execute(b, r, side, qty, at, cfg, conv, cal, limit=None, age_limit=None, cash_floor=0):
    at = moment(at).isoformat()
    settle(b, at)
    result = dict(
        symbol=r["symbol"], side=side, requested=qty, filled=0, at=at, quote=r.get("quote_time"), limit=limit
    )

    def reject(reason):
        return dict(result, status="rejet", reason=reason)

    if side not in ("BUY", "SELL") or qty is None or not isfinite(float(qty)) or qty <= 0:
        return reject("quantité ou sens invalide")
    step = r.get("lot")
    minimum = r.get("minimum")
    if not step or not minimum:
        return reject("lot inconnu")
    if qty < minimum or abs(qty / step - round(qty / step)) > 1e-7:
        return reject("lot minimal ou pas de quantité")
    if not r.get("permission_day") or r["permission_day"] >= at[:10]:
        return reject("accès au contrat non documenté avant décision")
    if not session(r.get("place"), at, cal):
        return reject("séance fermée ou inconnue")
    if any(v and moment(v) > moment(at) for v in (r.get("quote_time"), r.get("received"))):
        return reject("prix ou observation futurs")
    max_age = age_limit if age_limit is not None else cfg["max_quote_age_s"]
    if not fresh(r.get("received"), at, max_age) or not fresh(r.get("quote_time"), at, max_age):
        return reject("cotation absente ou périmée")
    if not str(r.get("availability") or "").startswith("R"):
        return reject("différé ou statut inconnu")
    bid, ask = r.get("bid"), r.get("ask")
    if not bid or not ask or bid <= 0 or bid > ask or r.get("currency") != r.get("quote_currency"):
        return reject("fourchette ou devise invalide")
    if 200 * (ask - bid) / (ask + bid) > cfg["max_spread_pct"]:
        return reject("spread excessif")
    px = ask if side == "BUY" else bid
    if limit is not None and ((side == "BUY" and px > limit) or (side == "SELL" and px < limit)):
        return reject("limite non atteinte")
    if not r.get("fx") or r["fx"] <= 0:
        return reject("change indisponible")
    key = "|".join([str(r.get("contract")), str(r["quote_time"]), side])
    capacity = (r.get("volume") or 0) * cfg["max_daily_volume_fraction"]
    depth = r.get("ask_size" if side == "BUY" else "bid_size")
    if depth is not None:
        capacity = min(capacity, depth)
    available = max(0, capacity - b["used"].get(key, 0))
    amount = floor(min(qty, available) / step) * step
    if amount < minimum:
        return reject("liquidité insuffisante")
    pos = b["positions"].get(r["symbol"], 0)
    if side == "SELL" and qty > pos:
        return reject("position insuffisante")
    fee = fees(r, side, amount, px, at[:10], conv)
    if fee is None:
        return reject("barème absent")
    if side == "BUY":
        debit = fee["nominal"] + fee["cost"]
        if debit > b["cash"] - cash_floor + 1e-8:
            return reject("cash disponible insuffisant")
        b["cash"] = round(b["cash"] - debit, 8)
        b["positions"][r["symbol"]] = pos + amount
        b["entries"].setdefault(r["symbol"], at[:10])
    else:
        when = moment(at).date()
        n = cfg["settlement_weekdays"]
        while n:
            when += timedelta(days=1)
            if when.weekday() < 5:
                n -= 1
        b["unsettled"].append(dict(day=when.isoformat(), amount=round(fee["nominal"] - fee["cost"], 8)))
        if pos == amount:
            del b["positions"][r["symbol"]]
            b["entries"].pop(r["symbol"], None)
        else:
            b["positions"][r["symbol"]] = pos - amount
    b["used"][key] = b["used"].get(key, 0) + amount
    result.update(
        status="partiel" if amount < qty else "exécuté",
        filled=amount,
        price=px,
        **fee,
        currency=r["currency"],
        rate=r["fx"],
    )
    b["trades"].append(result.copy())
    return result


def replay(batch, cfg, conv, cal):
    out = []
    for e in batch["events"]:
        entry = dict(
            execution=e["eid"],
            run=e["run"],
            models=e["model_count"],
            responses=e["responses"],
            verified_graph=e["complete"],
            input=e["input_hash"],
            trace=e["trace_hash"],
            dispatch=e["dispatch_present"],
            orders=[],
        )
        for age in (cfg["max_quote_age_s"], cfg["archived_sensitivity_age_s"]):
            if e["cash"] is None:
                entry["missing"] = "cash initial indisponible"
                continue
            b = book(e["cash"], {s: float(q) for s, q in e["positions"].items()})
            for o in e["orders"]:
                q = e["quotes"].get(o["symbol"], {})
                day = (o["sent"] or e["end"])[:10]
                p = next(
                    (p for p in batch["permissions"] if p["conid"] == q.get("conid") and p["day"] < day), {}
                )
                r = dict(
                    symbol=o["symbol"],
                    contract=q.get("conid"),
                    place=p.get("place"),
                    issuer=p.get("country"),
                    currency=q.get("currency"),
                    quote_currency=q.get("currency"),
                    fx=q.get("fxRateToEUR"),
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
                kind = str(o.get("kind") or "").upper()
                if kind not in ("LMT", "LIMIT", "MKT", "MARKET") or (
                    kind in ("LMT", "LIMIT") and not o["limit"]
                ):
                    result = dict(
                        status="indéterminé",
                        reason="type ou limite non modélisés",
                        filled=0,
                        symbol=o["symbol"],
                    )
                elif o["sent"]:
                    arrival = moment(o["sent"]) + timedelta(seconds=cfg["latency_s"])
                    result = execute(
                        b,
                        r,
                        o["side"],
                        o["qty"],
                        arrival.isoformat(),
                        cfg,
                        conv,
                        cal,
                        limit=o["limit"],
                        age_limit=age,
                    )
                else:
                    result = dict(
                        status="indéterminé", reason="heure d’envoi absente", filled=0, symbol=o["symbol"]
                    )
                actual = o["actual"]
                aq = sum(float(f["qty"]) for f in actual) if actual else None
                ap = sum(float(f["qty"]) * float(f["price"]) for f in actual) / aq if aq else None
                entry["orders"].append(
                    dict(
                        result,
                        age_limit=age,
                        actual_qty=aq,
                        actual_price=ap,
                        qty_delta=result["filled"] - aq if aq else None,
                        price_delta=round(result["price"] - ap, 6) if ap and result.get("price") else None,
                    )
                )
        out.append(entry)
    return dict(runs=out, hash=digest(out), complete_packs=len(out), ledger_runs=batch["ledger_runs"])
