"""Compare one valuation period at a time. All arithmetic uses original decimal strings."""
from collections import Counter,defaultdict
from decimal import Decimal,ROUND_HALF_UP
from statements import canonical,day,dump,events,load,num

ZERO=Decimal(0)
CENT=Decimal("0.01")
NAV_FIELDS=set("""mtm realized changeInUnrealized costAdjustments transferredPnlAdjustments depositsWithdrawals
internalCashTransfers assetTransfers debitCardActivity billPay dividends withholdingTax withholding871m withholdingTaxCollected
changeInDividendAccruals interest changeInInterestAccruals advisorFees brokerFees changeInBrokerFeeAccruals clientFees
otherFees feesReceivables commissions commissionReceivables forexCommissions transactionTax taxReceivables salesTax
softDollars netFxTrading fxTranslation linkingAdjustments other corporateActionProceeds commissionCreditsRedemption
grantActivity excessFundSweep billableSalesTax mtmAtPaxos carbonCredits donations paxosTransfers commissionsAtPaxos
referralFee changeInIncentiveCouponAccruals otherIncome""".split())
META=set("fromDate toDate currency reportDate startingValue endingValue twr model".split())
LABELS={"mtm":"Variation des prix au marché","realized":"Résultat réalisé","changeInUnrealized":"Variation du latent",
        "commissions":"Commissions","dividends":"Dividendes","withholdingTax":"Retenues à la source",
        "depositsWithdrawals":"Dépôts et retraits","interest":"Intérêts","fxTranslation":"Conversion des soldes en euros",
        "netFxTrading":"Résultat net des opérations de change","corporateActionProceeds":"Produit des opérations sur titres",
        "otherFees":"Autres frais","other":"Autres éléments du relevé"}
CATEGORIES={"Dépôts et retraits":["depositsWithdrawals"],"Dividendes":["dividends"],"Retenues":["withholdingTax","withholding871m","withholdingTaxCollected"],
          "Intérêts":["interest"],"Commissions":["commissions"],"Conversion des devises":["fxTranslation"],"Opérations sur titres":["corporateActionProceeds"]}


def line(scope,item,state,reason,actual=None,reference=None,delta=None,unit="EUR",source=""):
    return dict(periode=scope,rubrique=item,etat=state,montant=actual,comparatif=reference,ecart=delta,devise=unit,motif=reason,source=source)


def compare(scope,name,value,other,reason,unit="EUR",source="",aligned=True,rounded=False):
    value,other=num(value),num(other)
    diff=value-other if value is not None and other is not None else None
    state="ouvert"
    if diff is not None and aligned:
        if diff==0:
            state="rapproché"
        elif rounded and value==other.quantize(CENT,rounding=ROUND_HALF_UP):
            state="expliqué"
            reason+=" · Arrondi du comparatif au centime."
    return line(scope,name,state,reason,value,other,diff,unit,source)


def chosen_section(periods,records,kind,container):
    candidates=[]
    for h in periods:
        section=[r for r in records if r["source"]==h["source"] and r["seq"]==h["seq"] and r["kind"]==kind and not r["v"].get("model")]
        if section or container in h["sections"]:
            candidates.append({"gen":h["generated"],"v":{"base":h["currency"],"rows":sorted((r["v"] for r in section),key=dump)},"source":h["source"]})
    return canonical(candidates) if candidates else None


def nav_lines(scope,start,end,selected,nav,asof):
    if not selected or len(selected["v"]["rows"])!=1:
        return [line(scope,"Variation de la valeur liquidative","ouvert","Rubrique ChangeInNAV absente, multiple ou contradictoire.")]
    v=selected["v"]["rows"][0]
    src=selected["source"]
    currency=v.get("currency") or selected["v"]["base"]
    if currency!="EUR" or day(v.get("fromDate",start))!=start or day(v.get("toDate",end))!=end:
        return [line(scope,"Variation de la valeur liquidative","ouvert","Devise de base EUR ou bornes du relevé à établir.",source=src)]
    known={k:num(v[k]) for k in NAV_FIELDS if k in v}
    opening,closing=num(v.get("startingValue")),num(v.get("endingValue"))
    extra=set(v)-NAV_FIELDS-META
    both=bool(known.get("mtm") and (known.get("realized") or known.get("changeInUnrealized")))
    safe=bool(known) and not extra and not both
    rows=[line(scope,"Ouverture du relevé","expliqué","Solde publié par IBKR.",opening,source=src)] if opening is not None else []
    for k in sorted(known):
        rows.append(line(scope,LABELS.get(k,k),"expliqué","Contribution signée de la rubrique ChangeInNAV.",known[k],source=src))
    for name,fields in CATEGORIES.items():
        if not any(k in known for k in fields):
            rows.append(line(scope,name,"ouvert","Champ absent de ce relevé. Son montant reste indéterminé.",source=src))
    total=opening+sum(known.values(),ZERO) if safe and opening is not None else None
    reason="Clôture publiée moins ouverture et contributions signées."
    if extra or both:
        reason+=" Présentation mixte ou champs non interprétés ("+", ".join(sorted(extra))+")."
    rows.append(compare(scope,"Équilibre du pont de NAV",closing,total,reason,source=src))
    pa=nav.get(end)
    rows.append(compare(scope,"Clôture du relevé / PortfolioAnalyst",closing,pa["value"] if pa else None,
                        "Même date publiée et même devise." if pa and end<asof else "Valorisation finale comparable indisponible.",
                        source=src,aligned=bool(pa and end<asof),rounded=True))
    return rows


def balance_lines(scope,end,periods,records,ref):
    rows=[]
    cash=chosen_section(periods,records,"CashReportCurrency","CashReport")
    if not cash or not cash["v"]["rows"]:
        rows.append(line(scope,"Espèces par devise","ouvert","Cash Report absent ou contradictoire."))
    else:
        seen=set()
        for v in cash["v"]["rows"]:
            c=v.get("currency")
            if not c or c in ("BASE_SUMMARY","BASE"):
                continue
            dt=day(v.get("toDate") or v.get("reportDate") or end)
            if (dt,c) in seen:
                rows.append(line(scope,c+" · espèces","ouvert","Plusieurs soldes du même périmètre.",unit=c))
                continue
            seen.add((dt,c))
            obs=[x for x in ref["cash"] if x["currency"]==c and x["date"]==dt]
            obs.sort(key=lambda x:(x["basis"]=="close",x.get("at_utc","")))
            o=obs[-1] if obs else {}
            for label,k,other in [("espèces à la date de transaction","endingCash","cashbalance"),("espèces réglées","endingSettledCash","settledcash")]:
                rows.append(compare(scope,c+" · "+label,v.get(k),o.get(other),"Source de comparaison "+str(o.get("at_utc","absente"))+
                        (" · clôture attestée." if o.get("basis")=="close" else " · heure de clôture commune non établie."),
                        c,cash["source"],aligned=o.get("basis")=="close"))
        if not seen:
            rows.append(line(scope,"Espèces par devise","ouvert","Aucun solde individuel en devise."))
    pos=chosen_section(periods,records,"OpenPosition","OpenPositions")
    if not pos:
        return rows+[line(scope,"Quantités par contrat","ouvert","Inventaire Flex absent ou contradictoire.")]
    observations=[x for x in ref["positions"] if x["date"]==end]
    observations.sort(key=lambda x:(x["basis"]=="close",x["at"]))
    o=observations[-1] if observations else {}
    expected=defaultdict(list)
    for r in o.get("rows",[]):
        expected[r["conid"]].append(r["qty"])
    found=defaultdict(list)
    for v in pos["v"]["rows"]:
        if v.get("levelOfDetail","SUMMARY").upper() not in ("SUMMARY","SUMMARY_BY_CONID"):
            continue
        if day(v.get("reportDate",end))!=end or not v.get("conid"):
            rows.append(line(scope,"Quantité non attribuable","ouvert","Date ou identifiant de contrat manquant ou incompatible.",unit="titres",source=pos["source"]))
            continue
        found[v["conid"]].append(v.get("position"))
    for cid in sorted(set(found)|set(expected)):
        a,b=found[cid],expected[cid]
        rows.append(compare(scope,"Quantité · contrat "+cid,a[0] if len(a)==1 else None,b[0] if len(b)==1 else None,
                   "Comparaison par conid. Inventaire de référence "+str(o.get("at","absent"))+
                   (" · clôture attestée." if o.get("basis")=="close" else " · clôture commune non établie."),
                   "titres",pos["source"],aligned=o.get("basis")=="close"))
    if not found and not expected:
        rows.append(line(scope,"Quantités par contrat","ouvert","Aucune paire de lignes de clôture à vérifier.",unit="titres"))
    return rows


def by_key(rows,k):
    ix=defaultdict(list)
    for r in rows:
        if r.get(k):
            ix[str(r[k])].append(r)
    return ix


def exec_checks(ref):
    fills=by_key(ref["fills"],"execution_id")
    trades=by_key(ref["trades"],"execution_id")
    stats=Counter()
    evidence=[]
    for eid in sorted(set(fills)|set(trades)):
        a,b=fills[eid],trades[eid]
        state="ouvert"
        reason="Source absente ou identifiant multiple."
        if len(a)==len(b)==1:
            f,t=a[0],b[0]
            same_id=(f.get("conid") is not None and t.get("conid") is not None and str(f["conid"])==str(t["conid"]))
            side={"B":"BUY","S":"SELL"}.get(t["side"],t["side"])
            qf,qt=num(f["qty"]),num(t["size"])
            if same_id and side==f["side"] and qf is not None and qt is not None and abs(qf)==abs(qt):
                state="rapproché"
                reason="Même exécution, contrat, sens et quantité."
            else:
                reason="Contrat, sens ou quantité divergent."
        stats[state]+=1
        evidence.append({"execution":eid,"registre":len(a),"collecteur":len(b),"état":state,"motif":reason,
                         "date_registre":a[0]["ts"][:10] if a and a[0].get("ts") else None,
                         "date_collecteur":b[0]["ts"][:10] if b and b[0].get("ts") else None,
                         "conid":a[0].get("conid") if a else b[0].get("conid"),"qté_registre":a[0]["qty"] if a else None,
                         "qté_collecteur":b[0]["size"] if b else None})
    return dict(stats),evidence


def report(db,frozen,ref):
    from bridge import attribution,period_details
    from balances import movement_checks
    periods,all_rows=load(db)
    selected,issues=events(all_rows)
    lines=[]
    summary,evidence=exec_checks(ref)
    period_map=defaultdict(list)
    for p in periods:
        period_map[(p["start"],p["end"])].append(p)
    asof=ref["captured"][:10]
    for (start,end),pp in sorted(period_map.items()):
        scope=start+" au "+end
        source=chosen_section(pp,all_rows,"ChangeInNAV","ChangeInNAV")
        lines.extend(nav_lines(scope,start,end,source,ref["nav"],asof))
        lines.extend(balance_lines(scope,end,pp,all_rows,ref))
        lines.extend(period_details(scope,start,end,pp,all_rows,selected,ref,issues))
        lines.extend(movement_checks(scope,start,end,pp,all_rows,selected,issues,period_map,ref))
    fills=by_key(ref["fills"],"execution_id")
    for r in selected:
        v=r["v"]
        if r["kind"]!="Trade":
            continue
        eid=v.get("ibExecID","")
        match=fills.get(eid,[])
        f=match[0] if len(match)==1 else {}
        rawday=v.get("tradeDate") or v.get("dateTime")
        scope=day(rawday) or (r["start"]+" au "+r["end"])
        cid=v.get("conid")
        same=bool(cid and f.get("conid") and str(cid)==str(f["conid"]))
        q,qty=num(v.get("quantity")),num(f.get("qty"))
        side=v.get("buySell")
        lines.append(compare(scope,"Exécution "+(eid or "sans référence")+" · quantité",abs(q) if q is not None else None,
                    abs(qty) if qty is not None else None,"Appariement par identifiant d’exécution, contrat et sens.",
                    "titres",r["source"],aligned=same and side==f.get("side")))
        fee=num(v.get("ibCommission"))
        native=v.get("ibCommissionCurrency")
        local_ccy=f.get("commission_ccy")
        # Local register records a charge as positive; the statement convention is a signed cash flow.
        common=bool(native and native==local_ccy)
        reason="Commission en devise d’origine. Provenance du registre "+str(f.get("commission_source","absente"))+"."
        if not common and local_ccy:
            reason+=" Le registre indique "+str(f.get("commission_amount"))+" "+local_ccy+"."
        check=compare(scope,"Exécution "+(eid or "sans référence")+" · commission", -fee if fee is not None else None,
                    f.get("commission_amount") if common else None,reason,native or "inconnue",r["source"],aligned=bool(same and common))
        if check["etat"]=="rapproché" and "fallback" in (f.get("commission_source") or ""):
            check["etat"]="expliqué"
            check["motif"]+=" La devise supposée dans le registre est confirmée par le relevé."
        lines.append(check)
    for kind,why,r in issues:
        lines.append(line(r["start"]+" au "+r["end"],kind,"ouvert",why,source=r["source"]))
    historical=line(frozen["captured"][:10],"Résidu historique à ventiler","ouvert",
            "Valeur IBKR instantanée moins capital, réalisé FIFO interne et latent IBKR. Une clôture Flex est une autre valorisation.",
            num(frozen["residual"]),source=frozen["sources"]["broker"])
    lines.extend(attribution(frozen,period_map,all_rows) or [historical])
    date_audit=frozen["captured"][:10]
    old=next((n for n in frozen["nav"] if n["date"]==date_audit and n.get("value") is not None),None)
    revised=ref["nav"].get(date_audit)
    if old and revised:
        lines.append(line(date_audit,"Révision observée de la NAV PortfolioAnalyst","expliqué",
                    "Deux extractions de la même série datée. Écart mesuré entre publications, cause économique indéterminée. Hors pont comptable.",
                    num(revised["value"]),num(old["value"]),num(revised["value"])-num(old["value"]),source=frozen.get("sources",{}).get("pa","")))
    if not periods:
        for name in ["Clôture du relevé et variation de NAV","Espèces en devise et soldes réglés","Quantités de clôture par contrat",
                     "Opérations sur titres","Commissions en devise d’origine","Dépôts et retraits","Dividendes et retenues","Intérêts","Change et conversion des soldes"]:
            lines.append(line("Mai à novembre 2026",name,"ouvert","Relevé d’activité non reçu."))
    by_month=defaultdict(list)
    for dt in sorted(ref["nav"]):
        if dt<asof:
            by_month[dt[:7]].append(dt)
    coverage={}
    for month,dates in by_month.items():
        covered=sum(any(a<=dt<=b for a,b in period_map) for dt in dates)
        own=any(a[:7]==month and b[:7]==month and a<=dates[0] and b>=dates[-1] and
                chosen_section(pp,all_rows,"ChangeInNAV","ChangeInNAV") for (a,b),pp in period_map.items())
        coverage[month]={"dates_observees":len(dates),"dates_couvertes":covered,"pont_dans_le_mois":bool(own)}
    return {"periods":len(period_map),"imports":db.execute("select count(*) from sources where active=1").fetchone()[0],
            "couverture":coverage,
            "events":selected,"issues":len(issues),"open_events":[dict(r,motif=why) for _,why,r in issues],
            "lines":lines,"exec_summary":summary,"exec_evidence":evidence}
