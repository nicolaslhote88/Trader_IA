"""Operator entry points for rehearsal, live evidence checks and a single reversible retirement plan."""
import argparse
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, "/opt/trader-ia/research-sim/releases/9a528f28")
from common import digest, load, moment
from transition import pilot_gate, reconcile, retirement, retirement_plan, verify_retirement


def order_preview(tickets, mandate, ownership, account):
    """Size an unsigned order sheet. The live owner must quote again and reapply broker safety controls."""
    cap = Decimal(str(mandate["capital"]))
    assigned = [s for s in account["positions"] if ownership["positions"].get(s)=="pilote"]
    invested = sum((Decimal(str(account["values_eur"][s])) for s in assigned),Decimal("0"))
    cash = min(Decimal(str(account["cash"])), cap-invested)
    existing = {k:Decimal(str(v)) for k,v in account["positions"].items()}
    used = set()
    proposals = []
    allocation = Decimal(str(mandate["allocation_ratio"]))
    if not 0 < allocation <= 1 or not 0 <= cash <= cap:
        raise ValueError("Trésorerie ou coefficient du pilote invalide")
    for ticket in tickets:
        sym, side = ticket["symbol"],ticket["side"]
        identity = (sym,side)
        if identity in used or ownership["positions"].get(sym) not in ("pilote",None):
            raise ValueError("Ordre dupliqué ou titre déjà attribué")
        used.add(identity)
        if side not in ("BUY","SELL") or ticket.get("requested",0)<=0:
            raise ValueError("Instruction source invalide")
        lot = Decimal(str(account["lots"][sym]))
        if lot<=0:
            raise ValueError("Pas de quantité invalide")
        qty = ((Decimal(str(ticket["requested"]))*allocation)//lot)*lot
        if qty<=0:
            continue
        # Use a fresh, explicit cost estimate for the resized ticket: fixed fees do not scale with quantity.
        q = account["quotes"][sym]
        if (not q["fresh"] or not q["session_open"] or q["fx"]<=0 or q["bid"]<=0 or
            q["ask"]<q["bid"] or q["fees_eur"]<0 or q.get("estimated_qty")!=float(qty)):
            raise ValueError("Cotation ou estimation des frais à reprendre")
        px = Decimal(str(q["ask"] if side=="BUY" else q["bid"]))
        limit=ticket.get("limit")
        if limit is not None and ((side=="BUY" and px>Decimal(str(limit))) or (side=="SELL" and px<Decimal(str(limit)))):
            continue
        if side=="BUY":
            debit = qty*px*Decimal(str(q["fx"]))+Decimal(str(q["fees_eur"]))
            if debit>cash:
                raise ValueError("Trésorerie attribuée insuffisante")
            cash-=debit
        elif ownership["positions"].get(sym)!="pilote" or qty>existing.get(sym,0):
            raise ValueError("Vente sans position attribuée")
        proposals.append(dict(symbol=sym,side=side,qty=str(qty),limit=str(px),
                              key=digest([mandate["campaign"],mandate["variant"],ticket["at"],sym,side])))
    return proposals


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("action",choices=["passation","rapprocher","retrait","verifier-retrait"])
    parser.add_argument("dossier",type=Path)
    args=parser.parse_args()
    d=load(args.dossier)
    if args.action=="rapprocher":
        result=reconcile(d["initial"],d["orders"],d["fills"],d["observed"],d["tolerance"],d.get("fee_tolerance"))
    elif args.action=="verifier-retrait":
        result=verify_retirement(d["plan"],d["before"],d["after"])
    else:
        report=load("/local-files/duckdb/research_reporting/pilotage/report.json")
        evidence=load(sorted(Path("/var/lib/trader-research-feed/pilotage").glob("*.json.gz"))[-1])
        if args.action=="retrait":
            result=retirement_plan(retirement(evidence),d["decision"],d["review"],d["consumers"])
        else:
            now=datetime.now(timezone.utc)
            gate=pilot_gate(report["formal"],evidence,now,d["mandate"],d["owners"],d["inflight"],d.get("risk"))
            result={"gate":gate,"orders":[]}
            if not gate["blockers"]:
                variant=d["mandate"]["variant"]
                if report["shadow"]["day"] != now.date().isoformat():
                    raise ValueError("La dernière revue concerne une autre journée")
                tickets=report["shadow"]["tickets"][variant]
                if any(not 0<=(now-moment(t["at"])).total_seconds()<=d["mandate"]["max_signal_age_s"] for t in tickets):
                    raise ValueError("Les instructions ont expiré")
                result["orders"]=order_preview(tickets,d["mandate"],d["owners"],d["account"])
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=="__main__":
    main()
