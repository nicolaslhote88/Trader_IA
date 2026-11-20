"""Closing checks using exact contract IDs and the statement's own period boundaries."""
from collections import defaultdict
from datetime import date,timedelta
from decimal import Decimal
from bridge import event_date
from reconcile import ZERO,chosen_section,compare,line
from statements import day,num


def movement_checks(scope,start,end,pp,all_records,selected,issues,period_map,ref):
    rows=[]
    cash=chosen_section(pp,all_records,"CashReportCurrency","CashReport")
    funds=chosen_section(pp,all_records,"StatementOfFundsLine","StmtFunds")
    if cash and funds:
        for bal in cash["v"]["rows"]:
            ccy=bal.get("currency")
            if not ccy or ccy in ("BASE_SUMMARY","BASE") or day(bal.get("toDate",end))!=end or day(bal.get("fromDate",start))!=start:
                continue
            deltas=[]
            uncertain=False
            for r in selected:
                if r["kind"]!="StatementOfFundsLine" or r["v"].get("currency")!=ccy or not start<=event_date(r)<=end:
                    continue
                v=r["v"]
                amount=num(v.get("amount"))
                if amount is None and ("credit" in v or "debit" in v):
                    a,b=num(v.get("credit")),num(v.get("debit"))
                    if b is not None and b>0:
                        uncertain=True  # No inferred debit sign.
                    amount=(a or ZERO)+(b or ZERO)
                if amount is None:uncertain=True
                else:deltas.append(amount)
            unresolved=any(k=="StatementOfFundsLine" and r["v"].get("currency")==ccy and
                           (not event_date(r) or start<=event_date(r)<=end) for k,_,r in issues)
            opening=num(bal.get("startingCash"))
            calc=opening+sum(deltas,ZERO) if opening is not None and not uncertain and not unresolved and deltas else None
            rows.append(compare(scope,"Mouvements d’espèces / clôture · "+ccy,bal.get("endingCash"),calc,
                    "Espèces initiales plus écritures uniques du relevé de fonds en devise native, à la date de transaction.",ccy,cash["source"]))
    current=chosen_section(pp,all_records,"OpenPosition","OpenPositions")
    if not current:return rows
    prev_date=(date.fromisoformat(start)-timedelta(days=1)).isoformat()
    heads=[h for (a,b),hs in period_map.items() if b==prev_date for h in hs]
    previous=chosen_section(heads,all_records,"OpenPosition","OpenPositions")
    if not previous:
        return rows+[line(scope,"Variation de quantité depuis la clôture précédente","ouvert",
                 "Inventaire Flex de la veille du début de période indisponible.",unit="titres",source=current["source"])]
    def inventory(sec,dt):
        pos={}
        for v in sec["v"]["rows"]:
            if v.get("levelOfDetail","SUMMARY").upper() not in ("SUMMARY","SUMMARY_BY_CONID"):continue
            cid=v.get("conid")
            if not cid or cid in pos or num(v.get("position")) is None or day(v.get("reportDate",dt))!=dt:return None
            pos[cid]=num(v["position"])
        return pos if pos or not sec["v"]["rows"] else None
    opening,closing=inventory(previous,prev_date),inventory(current,end)
    if opening is None or closing is None:
        return rows+[line(scope,"Variation des quantités","ouvert","Inventaire par contrat incomplet ou ambigu.",unit="titres")]
    changes=defaultdict(Decimal)
    uncertain=set()
    for f in ref["fills"]:
        if not start<=day(f["ts"][:10])<=end:continue
        cid=str(f["conid"] or "")
        q=num(f.get("qty"))
        if not cid or q is None or f.get("side") not in ("BUY","SELL") or not f.get("execution_id"):
            uncertain.add(cid)
        else:changes[cid]+=abs(q)*(1 if f["side"]=="BUY" else -1)
    ids=defaultdict(list)
    for f in ref["fills"]:
        if start<=day(f["ts"][:10])<=end:ids[f.get("execution_id")].append(f)
    if any(len(g)>1 for g in ids.values()):
        uncertain.update(str(f.get("conid") or "") for g in ids.values() if len(g)>1 for f in g)
    flex_execs=set()
    for r in selected:
        if not start<=event_date(r)<=end:continue
        v=r["v"]
        cid=str(v.get("conid") or "")
        if r["kind"]=="Trade" and v.get("assetCategory")!="CASH":
            ident=v.get("ibExecID")
            flex_execs.add(ident)
            known=ids.get(ident,[])
            q=num(v.get("quantity"))
            if len(known)!=1 or q is None or str(known[0]["conid"])!=cid or num(known[0]["qty"]) is None or abs(num(known[0]["qty"]))!=abs(q) or known[0]["side"]!=v.get("buySell"):
                uncertain.add(cid)
        if r["kind"] in ("CorporateAction","Transfer"):
            q=num(v.get("quantity"))
            if not cid or q is None:uncertain.add(cid)
            else:
                if r["kind"]=="Transfer" and v.get("direction"):
                    direction=v["direction"].upper()
                    if direction in ("IN","INBOUND"):q=abs(q)
                    elif direction in ("OUT","OUTBOUND"):q=-abs(q)
                    else:uncertain.add(cid)
                changes[cid]+=q
    uncertain.update(str(f.get("conid") or "") for eid,fs in ids.items() if eid not in flex_execs for f in fs)
    for k,_,r in issues:
        if k in ("CorporateAction","Transfer","Trade") and (not event_date(r) or start<=event_date(r)<=end):
            uncertain.add(str(r["v"].get("conid") or ""))
    sections=set().union(*(set(h["sections"]) for h in pp))
    for cid in sorted(set(opening)|set(closing)|set(changes)|uncertain):
        if not cid:continue
        complete=cid not in uncertain and "" not in uncertain and {"Trades","CorporateActions","Transfers"}.issubset(sections)
        calc=opening.get(cid,ZERO)+changes[cid]
        rows.append(compare(scope,"Stock reconstitué · contrat "+cid,closing.get(cid,ZERO),calc,
            "Quantité à la clôture précédente, exécutions signées du registre, opérations sur titres et transferts. "+
            ("Sources de mouvements présentes." if complete else "Couverture ou identité de mouvement à vérifier."),
            "titres",current["source"],aligned=complete))
    return rows
