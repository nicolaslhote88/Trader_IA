"""Attribute a historical diagnostic difference only to dated Finance/IBKR evidence."""
from collections import defaultdict
from datetime import date,timedelta
from decimal import Decimal
import re
from statements import day,num
from reconcile import CATEGORIES,LABELS,NAV_FIELDS,META,ZERO,chosen_section,compare,line


def category(v):
    t=re.sub(r"[^a-z]","",v.get("type","").lower())
    if "withholding" in t: return "withholdingTax"
    if "dividend" in t or "paymentinlieu" in t: return "dividends"
    if t in {"deposit","deposits","withdrawal","withdrawals","depositswithdrawals","depositwithdrawal"}: return "depositsWithdrawals"
    if "interest" in t: return "interest"
    if "fee" in t: return "otherFees"
    return ""


def event_date(r):
    v=r["v"]
    return day(v.get("tradeDate") or v.get("dateTime") or v.get("date") or v.get("reportDate"))


def period_details(scope,start,end,pp,records,selected,ref,unresolved):
    nav=chosen_section(pp,records,"ChangeInNAV","ChangeInNAV")
    vals=(nav["v"]["rows"] if nav else [])
    if len(vals)!=1:
        return []
    n=vals[0]
    if (n.get("currency") or nav["v"]["base"])!="EUR" or day(n.get("fromDate",start))!=start or day(n.get("toDate",end))!=end:
        return []
    totals=defaultdict(list)
    rates=defaultdict(set)
    for r in records:
        v=r["v"]
        if r["kind"]=="ConversionRate" and v.get("toCurrency")=="EUR" and v.get("rate") and v.get("reportDate"):
            rates[day(v["reportDate"]),v.get("fromCurrency")].add(num(v["rate"]))
    for r in selected:
        d=event_date(r)
        if not (start<=d<=end):
            continue
        v=r["v"]
        if r["kind"]=="CashTransaction":
            key=category(v)
            field,ccy="amount",v.get("currency")
        elif r["kind"]=="Trade":
            key,field,ccy="commissions","ibCommission",v.get("ibCommissionCurrency")
        elif r["kind"]=="CorporateAction":
            key,field,ccy="corporateActionProceeds","proceeds",v.get("currency")
        else:
            continue
        if not key:continue
        fx=None
        if ccy=="EUR":fx=Decimal(1)
        elif ccy and ccy==v.get("currency"):fx=num(v.get("fxRateToBase"))
        if fx is None and len(rates[d,ccy])==1:fx=next(iter(rates[d,ccy]))
        amount=num(v.get(field))
        totals[key].append(amount*fx if amount is not None and fx is not None and fx>0 else None)
    rows=[]
    for k,values in sorted(totals.items()):
        family={"commissions":"Trade","corporateActionProceeds":"CorporateAction"}.get(k,"CashTransaction")
        complete=not any(kind==family and (not event_date(r) or start<=event_date(r)<=end) for kind,_,r in unresolved)
        total=sum(values,ZERO) if all(x is not None for x in values) and complete else None
        rows.append(compare(scope,"Détail Flex / NAV · "+LABELS.get(k,k),n.get(k),total,
            "Mouvements uniques signés, convertis au taux de la ligne ou du jour. Un champ, un identifiant ou un taux manquant laisse le contrôle ouvert.",
            source=nav["source"]))
    if "depositsWithdrawals" in n:
        flows=[x for x in ref["flows"] if start<=day(x["ts"][:10])<=end and x["type"] in ("DEPOSIT","WITHDRAWAL")]
        subtotal=sum((num(x["amount"]) for x in flows),ZERO) if all(x["currency"]=="EUR" for x in flows) else None
        rows.append(compare(scope,"Dépôts et retraits / registre AG1",n["depositsWithdrawals"],subtotal,
                    "Dates d’écriture du registre interne, qui peuvent différer des dates IBKR.",source=nav["source"]))
    return rows


def attribution(frozen,period_map,records):
    target=frozen["captured"][:10]
    inception=min(n["date"] for n in frozen["nav"])
    eligible=[]
    for (a,b),pp in period_map.items():
        nav=chosen_section(pp,records,"ChangeInNAV","ChangeInNAV")
        if not nav or len(nav["v"]["rows"])!=1 or b>target:
            continue
        v=nav["v"]["rows"][0]
        if (v.get("currency") or nav["v"]["base"])!="EUR" or set(v)-NAV_FIELDS-META:
            continue
        if day(v.get("fromDate",a))!=a or day(v.get("toDate",b))!=b:
            continue
        amounts={k:num(v[k]) for k in NAV_FIELDS if k in v}
        if "depositsWithdrawals" not in amounts or not ("mtm" in amounts or
            {"realized","changeInUnrealized"}.issubset(amounts)) or "startingValue" not in v or "endingValue" not in v:
            continue
        if amounts.get("mtm") and (amounts.get("realized") or amounts.get("changeInUnrealized")):
            continue
        if abs(num(v["endingValue"])-num(v["startingValue"])-sum(amounts.values(),ZERO))>Decimal("0.005"):
            continue
        eligible.append((a,b,num(v["startingValue"]),num(v["endingValue"]),amounts,nav["source"]))
    # Prefer a single exact-period statement. Otherwise use consecutive, balance-continuous periods only.
    paths={}
    for p in sorted(eligible,key=lambda x:(x[1],x[0])):
        a,b,opening,closing,items,src=p
        pre=(date.fromisoformat(a)-timedelta(days=1)).isoformat()
        chain=paths.get((pre,opening))
        if a<=inception<=b:
            cand=[p]
        elif chain:
            cand=chain+[p]
        else:
            continue
        old=paths.get((b,closing))
        if not old or len(cand)<len(old):
            paths[b,closing]=cand
    matches=[v for (end,_),v in paths.items() if end==target]
    if len(matches)!=1:
        return []
    chain=matches[0]
    comps=defaultdict(Decimal)
    for p in chain:
        for k,amount in p[4].items():comps[k]+=amount
    opening,closing=chain[0][2],chain[-1][3]
    sources=", ".join(dict.fromkeys(p[5] for p in chain))
    parts=[
        ("Valorisation instantanée moins clôture Flex",num(frozen["instant"])-closing),
        ("Ouverture et dépôts/retraits moins capital du diagnostic",opening+comps.pop("depositsWithdrawals",ZERO)-num(frozen["capital"]))]
    market=sum((comps.pop(k,ZERO) for k in ("mtm","realized","changeInUnrealized")),ZERO)
    parts.append(("Résultat de marché documenté moins FIFO et latent du diagnostic",market-num(frozen["fifo"])-num(frozen["latent"])))
    parts.extend((LABELS.get(k,k),v) for k,v in sorted(comps.items()))
    result=[line(target,k,"expliqué","Contribution à la différence entre les deux méthodes et dates de valorisation.",v,source=sources) for k,v in parts]
    result.append(compare(target,"Résidu historique à ventiler",frozen["residual"],sum((v for _,v in parts),ZERO),
                          "Résidu initial moins contributions identifiées dans une chaîne de périodes IBKR continue.",source=sources))
    return result
