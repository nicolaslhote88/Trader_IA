"""Prospective sale and reinvestment, sharing the same admissible dates and opening assets."""

from math import floor
from datetime import timedelta
from engine import book, execute, settle, fees
from common import moment
from exits import settle_day
from opinions import instrument


def simulate(t, d, base, following, p, cfg, conv, cal):
    end = (moment(t["end"]) + timedelta(seconds=cfg["latency_s"])).isoformat()
    event = next((e for e in base["events"] if e["run"] == t["run"]), {})
    buys = sorted(
        (
            x
            for x in t["decisions"]
            if x["side"] == "BUY" and x["status"] == "CONSENSUS_APPROVED" and x["symbol"] not in d["held"]
        ),
        key=lambda x: (-x.get("vote_count", 0), x["symbol"]),
    )
    rows = []
    for x in t["decisions"]:
        if x["side"] != "SELL" or x["status"] != "CONSENSUS_APPROVED":
            continue
        sym = x["symbol"]
        record = d["rows"].get(sym)
        pos = d["held"].get(sym)
        reason = d["forced"].get(sym) or d["reasons"].get(sym) or "remplacement"
        alt = buys[0]["symbol"] if buys else None
        line = dict(symbol=sym, reason=reason, alternative=alt, sale=None, reinvestment=None)
        rows.append(line)
        if not record or not pos:
            line["status"] = "position ou fiche absente"
            continue
        qty = float(
            x.get("selected_qty") or pos["quantity"]
            if x.get("action") == "CLOSE" or x.get("selected_qty")
            else 0
        )
        if not qty:
            line["status"] = "taille de réduction absente"
            continue
        wallet = book(0, {sym: float(pos["quantity"])})
        inst = instrument(sym, record, event, base, end)
        sale = execute(wallet, inst, "SELL", qty, end, cfg, conv, cal)
        line["sale"] = sale
        mandatory = reason in ("risque", "liquidités")
        nominal = qty * float(record.get("price_eur") or 0)
        line["policies"] = {
            "conservation": mandatory,
            "réduction": True,
            "bande": mandatory or nominal >= d["nav"] * p["band_nav"],
        }
        if not sale["filled"]:
            line["status"] = sale["reason"]
            continue
        if not alt:
            line["status"] = "vente simulée, aucun remplacement proposé"
            continue
        due = settle_day(end[:10])
        line["settlement"] = due
        future = next(((v, b) for v, b in following if due <= v["at"][:10]), None)
        if not future:
            line["status"] = "attente de la première revue documentée après règlement"
            continue
        later, b = future
        new = next((r for r in later["rows"] if r["symbol"] == alt), None)
        if not new or not (new.get("buy_feasibility") or {}).get("feasible"):
            line["status"] = "remplaçant indisponible à la première revue"
            continue
        if len(d["held"]) - (sale["filled"] == float(pos["quantity"])) >= p["max_positions"]:
            line["status"] = "nombre maximal de lignes"
            continue
        stamp = (moment(later["end"]) + timedelta(seconds=cfg["latency_s"])).isoformat()
        other = next((e for e in b["events"] if e["run"] == later["run"]), {})
        r = instrument(alt, new, other, b, stamp)
        settle(wallet, stamp)
        if not r["lot"] or not r["ask"] or not r["fx"]:
            line["status"] = "ticket du remplacement inconnu"
            continue
        sec = new.get("sector")
        sector_value = d["sectors"].get(sec, 0) - (sale["nominal"] if sec == pos.get("sector") else 0)
        budget = min(
            wallet["cash"],
            d["nav"] * p["entry_weight"],
            d["nav"] * p["max_sector"] - sector_value,
            wallet["cash"] + d["cash"] - p["cash_floor"] * d["nav"],
        )
        n = floor(max(0, budget) / (r["ask"] * r["fx"] * r["lot"])) * r["lot"]
        while n > 0:
            fee = fees(r, "BUY", n, r["ask"], stamp[:10], conv)
            if not fee or fee["nominal"] + fee["cost"] <= budget:
                break
            n -= r["lot"]
        leg = execute(wallet, r, "BUY", n, stamp, cfg, conv, cal, limit=r["ask"])
        line.update(reinvestment=leg, status=leg.get("reason") or leg["status"])
    return rows
