from datetime import date, timedelta
from math import floor
from common import digest, fresh, moment
from universe import reasons
from engine import fees, execute, settle


def marking(b, rows, at):
    values = {}
    for sym, qty in b["positions"].items():
        r = rows.get(sym, {})
        if (
            r.get("bid")
            and r.get("ask")
            and r.get("quote_currency") == r.get("currency")
            and fresh(r.get("quote_time"), at, 96 * 3600)
        ):
            native = (r["bid"] + r["ask"]) / 2
        elif fresh(r.get("price_at"), at, 96 * 3600) and r.get("price_currency") == r.get("currency"):
            native = r.get("price")
        else:
            native = None
        values[sym] = qty * native * r["fx"] if native and r.get("fx") else None
    nav = (
        None
        if None in values.values()
        else b["cash"] + sum(r["amount"] for r in b["unsettled"]) + sum(values.values())
    )
    return nav, values


def review(b, variant, candidates, arrival, at, cfg, conv, cal):
    by = {r["symbol"]: r for r in candidates}
    later = {r["symbol"]: r for r in arrival}
    at = moment(at).isoformat()
    exec_at = (moment(at) + timedelta(seconds=cfg["latency_s"])).isoformat()
    settle(b, at)
    sales = []
    exits = {}
    requests = []
    result = []
    veto = variant.endswith("_veto")
    available_cash = b["cash"]
    planned_positions = set(b["positions"])
    for sym in list(sorted(b["positions"])):
        if (date.fromisoformat(at[:10]) - date.fromisoformat(b["entries"][sym])).days < cfg["horizon_days"]:
            continue
        exits[sym] = b["positions"][sym]
    nav, values = marking(b, by, at)
    rankable = []
    for r in candidates:
        why = reasons(r, at, cfg, cal, veto)
        row = dict(
            symbol=r["symbol"],
            sector=r.get("sector"),
            score=r.get("score"),
            score_at=r.get("fund_at"),
            score_source=r.get("fund_source"),
            tech=r.get("ai"),
            tech_at=r.get("tech_at"),
            quote=r.get("quote_time"),
            rank=None,
            reason=why,
        )
        result.append(row)
        if not why:
            rankable.append((r, row))
    week = str(date.fromisoformat(at[:10]).isocalendar()[:2])

    def key(pair):
        r = pair[0]
        neutral = digest([cfg["neutral_seed"], week, r["symbol"]])
        return (0 if variant.startswith("neutral") else -float(r["score"]), neutral)

    rankable.sort(key=key)
    for rank, (r, row) in enumerate(rankable, 1):
        sym = r["symbol"]
        row["rank"] = rank
        if sym in planned_positions:
            row["reason"] = ["déjà détenu ou réservé à cette revue"]
            continue
        if nav is None or nav <= 0:
            row["reason"] = ["NAV non valorisable"]
            continue
        if len(planned_positions) >= cfg["max_positions"]:
            row["reason"] = ["nombre maximal de lignes"]
            continue
        sector = r["sector"]
        sector_value = sum(v for s, v in values.items() if by.get(s, {}).get("sector") == sector)
        budget = min(
            nav * cfg["target_weight"],
            nav * cfg["max_sector_weight"] - sector_value,
            available_cash - nav * cfg["cash_floor"],
        )
        step = r["lot"]
        px = r["ask"]
        qty = floor(max(0, budget) / (px * r["fx"] * step)) * step
        if qty >= r["minimum"]:
            cost = fees(r, "BUY", qty, px, at[:10], conv)
            if cost is None:
                row["reason"] = ["barème absent"]
                continue
            if cost["nominal"] + cost["cost"] > budget:
                qty = floor(max(0, budget - cost["cost"]) / (px * r["fx"] * step)) * step
        if qty < r["minimum"]:
            row["reason"] = ["ticket supérieur au cash ou au plafond sectoriel"]
            continue
        cost = fees(r, "BUY", qty, px, at[:10], conv)
        available_cash -= cost["nominal"] + cost["cost"]
        values[sym] = cost["nominal"]
        planned_positions.add(sym)
        requests.append((sym, qty, px, row))
        row["decision"] = dict(quantity=qty, limit=px, nominal=cost["nominal"], fees=cost["cost"])
    # Orders and reserves are fixed from the decision pack before consulting arrival prices.
    for sym, qty in exits.items():
        if sym not in later:
            sales.append(dict(symbol=sym, status="rejet", reason="fiche absente"))
            continue
        sales.append(execute(b, later[sym], "SELL", qty, exec_at, cfg, conv, cal))
    for sym, qty, px, row in requests:
        if sym not in later:
            row["reason"] = ["cotation d’arrivée absente"]
            continue
        fill = execute(
            b, later[sym], "BUY", qty, exec_at, cfg, conv, cal, limit=px, cash_floor=nav * cfg["cash_floor"]
        )
        row["order"] = fill
        row["reason"] = [fill.get("reason") or "rang dans le budget et les limites"]
    nav, values = marking(b, later, exec_at)
    sectors = {}
    if nav:
        for s, v in values.items():
            sector = later.get(s, {}).get("sector") or "inconnu"
            sectors[sector] = sectors.get(sector, 0) + v / nav
    return dict(
        variant=variant,
        at=at,
        arrival=exec_at,
        ranked=len(rankable),
        alternatives=result,
        sales=sales,
        nav=nav,
        cash=b["cash"],
        unsettled=sum(x["amount"] for x in b["unsettled"]),
        positions=dict(b["positions"]),
        currencies={s: later.get(s, {}).get("currency") for s in b["positions"]},
        sectors=sectors,
        turnover=sum(t["nominal"] for t in b["trades"]) / cfg["capital_eur"],
        fees=sum(t["cost"] for t in b["trades"]),
        net=nav / cfg["capital_eur"] - 1 if nav is not None else None,
    )
