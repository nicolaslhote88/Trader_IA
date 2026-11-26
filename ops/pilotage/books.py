"""Replay isolated, dated books from the archived decision and arrival frames."""
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import engine
import strategies
from common import digest, load, moment
from engine import book, execute, settle
from universe import prepare
from worker import PARIS


def inputs(day, files, cfg, audit):
    cut = datetime.fromisoformat(day + "T" + cfg["review_paris"]).replace(tzinfo=PARIS).astimezone(timezone.utc)
    arrival = cut + timedelta(seconds=cfg["latency_s"])
    before = [p for ts, p in files if cut - timedelta(hours=1) <= ts <= cut]
    after = [p for ts, p in files if arrival <= ts <= cut + timedelta(hours=1)]
    if not before or not after:
        return None
    src, post = load(before[-1]), load(after[0])
    if moment(src["at"]) > cut or moment(post["at"]) < arrival:
        return None
    joined = dict(src, frames=src["frames"] + post["frames"])
    joined["frames"].sort(key=lambda x: x["received"])
    return (prepare(joined, cut.isoformat(), audit), prepare(joined, arrival.isoformat(), audit),
            cut.isoformat(), arrival.isoformat(), [before[-1].name, after[0].name])


def doubled(rows):
    result = []
    for row in rows:
        r = row.copy()
        if r.get("bid") and r.get("ask") and r["ask"] >= r["bid"]:
            half = (r["ask"] - r["bid"]) / 2
            r["bid"] -= half
            r["ask"] += half
        result.append(r)
    return result


def run_day(state, day, frame, cfg, conv, cal, previous_session=None):
    before, after, cut, arrival, origin = frame
    out = {}
    ordinary_fee = engine.fees

    def double_fee(*args, **kwargs):
        result = ordinary_fee(*args, **kwargs)
        if result is not None:
            for field in ("cost", "commission", "reserve", "fx_cost", "tax"):
                if result[field] is not None:
                    result[field] *= 2
        return result

    stress_before, stress_after = doubled(before), doubled(after)
    by_symbol = {r["symbol"]: r for r in after}
    def bounded_execute(ledger, row, side, qty, at, cfg, conv, cal, **kwargs):
        px = row.get("ask" if side=="BUY" else "bid")
        estimate = engine.fees(row,side,qty,px,day,conv) if px and row.get("fx") else None
        if estimate and "cost_cap" in cfg and (cfg["api_documented"] + sum(t["cost"] for t in ledger["trades"]) +
                                                estimate["cost"] > cfg["cost_cap"]):
            return dict(status="rejet",reason="plafond de coûts du mandat",side=side,symbol=row["symbol"],
                        requested=qty,filled=0,at=at)
        if side == "BUY":
            nav, values = strategies.marking(ledger, by_symbol, at)
            sector = row.get("sector")
            price = row.get("ask")
            fx = row.get("fx")
            sector_value = sum((v or 0) for s, v in values.items() if by_symbol.get(s, {}).get("sector") == sector)
            estimated = engine.fees(row, side, qty, price, day, conv) if price and fx else None
            # Commissions reduce NAV at entry. Check all previously executed buys from the same review.
            nominal = estimated["nominal"] if estimated else 0
            after_nav = nav - estimated["cost"] if nav and estimated else 0
            if (not sector or not after_nav or None in values.values() or
                    sector_value + nominal > after_nav * cfg["max_sector_weight"] + 1e-8 or
                    sum(values.values()) + nominal > after_nav * (1-cfg["cash_floor"]) + 1e-8):
                return dict(status="rejet", reason="limite d’exposition à l’achat", side=side,
                            symbol=row["symbol"], requested=qty, filled=0, at=at)
        return execute(ledger,row,side,qty,at,cfg,conv,cal,**kwargs)
    for variant in cfg["variants"]:
        out[variant] = {}
        for mode in ("normal", "cout_double", "retard"):
            key = variant + "/" + mode
            ledger = state.setdefault(key, book(cfg["capital_eur"]))
            trades_at_start = len(ledger["trades"])
            intents = []
            if mode == "retard":
                settle(ledger, cut)
                # Issue today's instructions before the arrival quote and fills become available.
                due = ledger.get("pending", []) if ledger.get("pending_day") == previous_session else []
                import copy
                planner = copy.deepcopy(ledger)
                plan = strategies.review(planner, variant, before, before, cut, cfg, conv, cal)
                nav, _ = strategies.marking(ledger, {r["symbol"]: r for r in before}, cut)
                floor = (nav or 0) * cfg["cash_floor"]
                ledger["pending_day"] = day
                ledger["pending"] = [
                    (s, "SELL", n, None, 0) for s, n in sorted(ledger["positions"].items())
                    if (datetime.fromisoformat(day) - datetime.fromisoformat(ledger["entries"][s])).days >= cfg["horizon_days"]
                ] + [(r["symbol"], "BUY", r["decision"]["quantity"], r["decision"]["limit"], floor)
                     for r in plan["alternatives"] if r.get("decision")]
                for sym, side, quantity, limit, reserve in due:
                    if sym in by_symbol:
                        bounded_execute(ledger, by_symbol[sym], side, quantity, arrival, cfg, conv, cal,
                                        limit=limit, cash_floor=reserve)
            else:
                with ExitStack() as scope:
                    scope.enter_context(patch.object(strategies, "execute", bounded_execute))
                    if mode == "cout_double":
                        scope.enter_context(patch.object(engine, "fees", double_fee))
                        scope.enter_context(patch.object(strategies, "fees", double_fee))
                    proposal = strategies.review(ledger, variant, stress_before if mode == "cout_double" else before,
                                                 stress_after if mode == "cout_double" else after, cut, cfg, conv, cal)
                    intents = [dict(symbol=row["symbol"],side="BUY",requested=row["decision"]["quantity"],
                                    limit=row["decision"]["limit"],at=cut)
                               for row in proposal["alternatives"] if row.get("decision")]
                    intents.extend(dict(symbol=sale["symbol"],side="SELL",requested=sale["requested"],limit=None,at=cut)
                                   for sale in proposal["sales"] if sale.get("requested"))
            out[variant][mode] = dict(cash=ledger["cash"] + sum(x["amount"] for x in ledger["unsettled"]),
                                      positions=dict(ledger["positions"]),
                                      new_trades=ledger["trades"][trades_at_start:],intents=intents,
                                      metadata={s: {k: by_symbol.get(s, {}).get(k) for k in ("sector","country","currency","place")}
                                                for s in set(ledger["positions"]) | {x["symbol"] for x in ledger["trades"][trades_at_start:]}})
    return dict(day=day, input=digest(origin), sources=origin, books=out)
