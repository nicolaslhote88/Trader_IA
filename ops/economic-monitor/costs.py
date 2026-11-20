"""Shadow tariff and order-level receipt. All comparisons use independently dated inputs."""
from collections import defaultdict
from datetime import datetime,date,timedelta
from statistics import mean
import json

FRENCH_2026=set("ACA AI BNP DSY ELEC ELIS FGR GTT LR PEUG TKO VIRP VK".split())
VERSION="IBKR-fixed-simulation-20261120.1"

def tariff(venue,ccy,qty,nominal,fx):
    if not qty or not nominal or nominal<=0:return None
    if venue in ("SBF","AEB") and ccy=="EUR":return max(3,nominal*.0005)
    if venue in ("NYSE","NASDAQ") and ccy=="USD" and fx and fx>0:
        return min(max(1,qty*.005)*fx,.01*nominal)
    return None

def tax_estimate(symbol,side,day,country,nominal):
    if side=="SELL":return 0
    if not nominal or day[:4]!="2026":return None
    base=symbol.split(".")[0]
    if country=="FR":
        if base in FRENCH_2026:return nominal*.004
        if base=="MAU":return 0
        return None
    return 0 if country in ("US","NL") else None

def as_fx(fx,day):
    eligible=[k for k in fx if k<day and (date.fromisoformat(day)-date.fromisoformat(k)).days<=5]
    return fx[max(eligible)] if eligible else None

def stamp(s):
    return datetime.fromisoformat(s.replace("Z","+00:00")).timestamp() if s else None

def quality(q,ts):
    if not q or not ts or not q.get("quoteTime"):return False
    age=ts-stamp(q["quoteTime"])
    return (0<=age<=60 and str(q.get("ibkrDataAvailability") or "").startswith("R") and q.get("spreadObserved")
            and 0<(q.get("bid") or 0)<=(q.get("ask") or 0))

def costs(ledger,account,flex,trace,audit):
    flex_ok=sorted({p["source"] for p in account["periods"]})==flex["sources"]
    metadata=flex["meta"] if flex_ok else {}
    verified={v["execution"]:v for v in account["trades"]}
    sends={oid:(eid,json.loads(raw)) for eid,oid,raw in trace.execute("select eid,orderid,body from sends")}
    packs={(eid,c):json.loads(raw) for eid,c,raw in trace.execute("select eid,contract,body from packs")}
    contracts={m["conid"]:m for m in metadata.values()}
    by_order=defaultdict(list)
    for r in ledger:by_order[r["order_id"]].append(r)
    rows=[];tax_groups=defaultdict(list)
    for oid,parts in by_order.items():
        r=parts[0]; fs=[verified.get(p["execution"]) for p in parts]
        mt=metadata.get(parts[0]["execution"]) or contracts.get(parts[0]["conid"],{})
        venue,country=mt.get("listingExchange"),mt.get("issuerCountryCode")
        day=r["at"][:10];cid=parts[0]["conid"] or mt.get("conid")
        eid,send=sends.get(oid,(None,{}));q=packs.get((eid,cid),{})
        ccy=mt.get("currency") or q.get("currency") or send.get("currency")
        fx=1 if ccy=="EUR" else send.get("fxRateToEUR") or (as_fx(flex["fx"],day) if ccy=="USD" else None)
        old=r["expected"];nom=send.get("estNotionalEUR") or (old*1000 if old is not None else None)
        qty=float(r["ordered"]);fee=tariff(venue,ccy,qty,nom,fx);tax=tax_estimate(r["symbol"],r["side"],day,country,nom)
        actual=sum(float(f["fee"]) for f in fs) if all(f and f["fee"] is not None for f in fs) else None
        nominal=sum(float(f["nominal"]) for f in fs) if all(f and f["nominal"] is not None for f in fs) else None
        ref=stamp(send.get("sent_at"));observed=None
        if cid and ref:
            observed=trace.execute("select observed,stamp,bid,ask,availability from quotes where contract=? and observed<=? and observed>=? order by observed desc limit 1",(cid,ref,ref-60)).fetchone()
        usable=observed and observed[4].startswith("R") and observed[1] and 0<=ref-observed[1]<=60 and 0<observed[2]<=observed[3]
        slip=half=native_bps=None
        if usable and all(p["native_price"] and p["filled"] and p["conid"]==cid for p in parts):
            bid,ask=observed[2:4];sign=1 if r["side"]=="BUY" else -1
            touch=ask if sign==1 else bid
            filled=sum(abs(p["filled"]) for p in parts)
            native_extra=sum(abs(p["filled"])*(p["native_price"]-touch)*sign for p in parts)
            native_bps=10000*native_extra/(filled*touch)
            if all(f and f["rate"] for f in fs):
                half=sum(float(f["qty"])*(ask-bid)/2*float(f["rate"]) for f in fs)
                slip=sum(float(f["qty"])*(float(f["price"])-touch)*sign*float(f["rate"]) for f in fs)
        estimate_spread=(q["ask"]-q["bid"])/2*qty*fx if fx and quality(q,stamp(q.get("observed"))) else None
        row=dict(order=oid,date=day,symbol=r["symbol"],side=r["side"],place=venue,currency=ccy,contract=cid,
                 size=("< 300 €" if nom<300 else "300–999 €" if nom<1000 else "≥ 1 000 €") if nom else "inconnue",
                 planned_nominal=nom,quantity=qty,nominal_source="ordre AG1" if send.get("estNotionalEUR") else "prévision historique / 10 pb, arrondie",
                 parts=len(parts),old=old,recorded=sum(float(p["fees"]) for p in parts),fee=fee,actual=actual,tax=tax,actual_tax=None,
                 nominal=nominal,decision_quote=q.get("quoteTime"),decision_received=q.get("observed"),decision_source=q.get("quoteSource"),
                 decision_age=q.get("quoteAgeMinutes"),decision_availability=q.get("ibkrDataAvailability"),estimated_half_spread=estimate_spread,
                 sent=send.get("sent_at"),send_quote_epoch=observed[1] if observed else None,
                 send_quote_received=observed[0] if observed else None,send_availability=observed[4] if observed else None,
                 half_spread=half,slippage_beyond_touch=slip,slippage_bps=native_bps,fx_cost=None,
                 fx_scenario=nom*.0003 if nom and ccy not in (None,"EUR") else 0 if ccy=="EUR" else None,
                 fill_days=len({f["date"] for f in fs if f}),
                 commission_round_trip=2*fee if fee is not None and r["side"]=="BUY" else None,
                 explicit_round_trip=2*fee+tax if fee is not None and tax is not None and r["side"]=="BUY" else None)
        by_day=defaultdict(list)
        for f in fs:
            if f:by_day[f["date"]].append(f)
        replay=[]
        for day_parts in by_day.values():
            if any(f["nominal"] is None for f in day_parts):continue
            replay.append(tariff(venue,ccy,sum(float(f["qty"]) for f in day_parts),sum(float(f["nominal"]) for f in day_parts),fx))
        row["fee_replayed_per_day"]=(sum(replay) if all(fs) and len(replay)==len(by_day) and None not in replay else None)
        rows.append(row)
        for f in fs:
            if f and r["side"]=="BUY":
                fd=f["date"];tax_groups[(fd[:4]+"-"+fd[4:6]+"-"+fd[6:])+ "/" +f["conid"]].append((row,float(f["nominal"]) if f["nominal"] is not None else None))
        if all(fs) and flex_ok:row["actual_tax"]=0
    for k,amount in flex["taxes"].items():
        group=tax_groups.get(k,[])
        if not group:continue
        base=sum(v or 0 for _,v in group)
        for row,v in group:
            if v is not None and base and row["actual_tax"] is not None:
                row["actual_tax"]+=amount*v/base
                row["tax_allocation"]="au prorata des achats du contrat ce jour" if len(group)>1 else "achat unique"
    refids={r[0] for r in audit["fees"]};historical=[r for r in rows if r["order"] in refids]
    groups=[]
    for label,subset in [("Référence",historical),("Réserve oct.–18 nov.",[r for r in historical if r["date"]>="2026-10-01"]),
                         ("Prospectif depuis déploiement",[r for r in rows if r["date"]>="2026-11-20"])]:
        for field in ("total","size","place","side"):
            keys={r.get(field) for r in subset} if field!="total" else {"Tous"}
            for k in sorted(keys,key=str):
                subset2=[r for r in subset if field=="total" or r[field]==k]
                for method in ("old","fee"):
                    errors=[r[method]-r["actual"] for r in subset2 if r[method] is not None and r["actual"] is not None]
                    if errors:groups.append(dict(period=label,group=field,key=k,method=method,n=len(errors),bias=mean(errors),mae=mean(map(abs,errors)),under=sum(e<-.005 for e in errors)))
    return dict(version=VERSION,orders=rows,metrics=groups,count=len(historical),expected=sum(r["old"] for r in historical if r["old"] is not None),
                recorded=sum(r["recorded"] for r in historical), historical_missing=sum(p["slip"] is None for p in ledger if p["order_id"] in refids),
                metadata_aligned=flex_ok,unassigned_tax=round(sum(v for k,v in flex["taxes"].items() if k not in tax_groups),6),
                future_executed=sum(r["date"]>="2026-11-20" for r in rows),
                future_measured=sum(r["date"]>="2026-11-20" and r["slippage_bps"] is not None for r in rows))
