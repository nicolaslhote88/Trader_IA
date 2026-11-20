"""Rebuild an inception-to-close equity account from signed IBKR cash legs at one closing FX convention."""
from collections import defaultdict
from datetime import date,timedelta
from decimal import Decimal
from statements import day,num
from reconcile import ZERO,chosen_section,line,compare

HALF_CENT=Decimal("0.005")


def equity_check(scope,start,end,heads,records):
    part=chosen_section(heads,records,"EquitySummaryByReportDateInBase","EquitySummaryInBase")
    if not part:return []
    data=[x for x in part["v"]["rows"] if x.get("currency")=="EUR"]
    previous=(date.fromisoformat(start)-timedelta(days=1)).isoformat()
    initial=[x for x in data if day(x.get("reportDate"))==previous]
    final=[x for x in data if day(x.get("reportDate"))==end]
    if len(initial)!=1 or len(final)!=1:return []
    a,b=initial[0],final[0]
    fields=("total","stock","cash")
    if any(num(r.get(k)) is None for r in (a,b) for k in fields):return []
    deltas={k:num(b[k])-num(a[k]) for k in fields}
    if any(abs(num(r["total"])-num(r["stock"])-num(r["cash"]))>=HALF_CENT for r in (a,b)):return []
    return [line(scope,"Variation des actions en EUR","expliqué","Valorisations de clôture consécutives aux bornes du relevé.",deltas["stock"],source=part["source"]),
            line(scope,"Variation des espèces en EUR","expliqué","Espèces en devise de base aux deux bornes du relevé.",deltas["cash"],source=part["source"]),
            compare(scope,"Variation de NAV / actions et espèces",deltas["total"],deltas["stock"]+deltas["cash"],"Contrôle indépendant dans EquitySummaryInBase.",source=part["source"])]


def from_inception(frozen,start,end,heads,records,issues):
    """Require a flat inception, closing equity and cash, stock FIFO and exact trade-to-cash links."""
    if end!=frozen["captured"][:10] or not start<=min(n["date"] for n in frozen["nav"])<=end:return []
    parts={}
    for kind,section in (("ChangeInNAV","ChangeInNAV"),("EquitySummaryByReportDateInBase","EquitySummaryInBase"),
          ("OpenPosition","OpenPositions"),("CashReportCurrency","CashReport"),("StatementOfFundsLine","StmtFunds"),
          ("Trade","Trades"),("ConversionRate","ConversionRates")):
        part=chosen_section(heads,records,kind,section)
        if not part:return []
        parts[kind]=part
    src=parts["ChangeInNAV"]["source"]
    if any(x["source"]!=src for x in parts.values()):return []
    if any(r["source"]==src for _,_,r in issues):return []
    h=next(x for x in heads if x["source"]==src)
    if not {"Transfers","CorporateActions"}.issubset(h["sections"]):return []
    if any(r["source"]==src and r["kind"] in ("Transfer","CorporateAction") for r in records):return []
    def values(kind):
        return parts[kind]["v"]["rows"]
    nav=values("ChangeInNAV")
    if len(nav)!=1 or nav[0].get("currency")!="EUR" or num(nav[0].get("startingValue"))!=ZERO:return []
    closing=num(nav[0].get("endingValue"))
    if closing is None:return []
    equity=equity_check(end,start,end,heads,records)
    if not equity or equity[-1]["etat"]!="rapproché":return []
    previous=(date.fromisoformat(start)-timedelta(days=1)).isoformat()
    equity_by_date={day(x.get("reportDate")):x for x in values("EquitySummaryByReportDateInBase") if x.get("currency")=="EUR"}
    if any(num(equity_by_date[previous].get(k))!=0 for k in ("total","stock","cash")):return []
    terminal=equity_by_date[end]
    if num(terminal["total"])!=closing:return []
    rates={"EUR":Decimal(1)}
    for x in values("ConversionRate"):
        if day(x.get("reportDate"))==end and x.get("toCurrency")=="EUR":
            val=num(x.get("rate"));c=x.get("fromCurrency")
            if val is None or val<=0 or (c in rates and rates[c]!=val):return []
            rates[c]=val
    cash_rows={};mov=defaultdict(Decimal);by_trade=defaultdict(list)
    for v in values("CashReportCurrency"):
        c=v.get("currency")
        if c in ("BASE","BASE_SUMMARY"):continue
        if c not in rates or c in cash_rows or num(v.get("startingCash"))!=0 or num(v.get("endingCash")) is None:return []
        cash_rows[c]=num(v["endingCash"])
    if not cash_rows:return []
    allowed={"BUY","SELL","FOREX","DEP","WITH","DIV","FRTAX","TTAX"}
    seen=set()
    for v in values("StatementOfFundsLine"):
        code=v.get("activityCode");amount=num(v.get("amount"));c=v.get("currency")
        if not code and v.get("activityDescription") in ("Starting Balance","Ending Balance") and amount==0:continue
        if code not in allowed or c not in cash_rows or amount is None or not v.get("transactionID"):return []
        key=c,v["transactionID"]
        if key in seen:return []
        seen.add(key)
        mov[c,code]+=amount
        if code in ("BUY","SELL","FOREX"):
            by_trade[v.get("tradeID")].append((c,code,amount))
    if any(abs(sum((v for (cc,k),v in mov.items() if cc==c),ZERO)-balance)>=HALF_CENT for c,balance in cash_rows.items()):return []
    if abs(sum((v*rates[c] for c,v in cash_rows.items()),ZERO)-num(terminal["cash"]))>=HALF_CENT:return []
    def converted(code):
        return sum((v*rates[c] for (c,k),v in mov.items() if k==code),ZERO)
    realized=defaultdict(Decimal);unrealized=defaultdict(Decimal);stocks=defaultdict(Decimal);qty=defaultdict(Decimal)
    trade_ids=set()
    for v in values("Trade"):
        tid=v.get("tradeID");c=v.get("currency");asset=v.get("assetCategory");legs=by_trade.get(tid,[])
        if not tid or tid in trade_ids or c not in rates or v.get("levelOfDetail")!="EXECUTION":return []
        trade_ids.add(tid)
        if asset=="STK":
            cid=v.get("conid")
            if not cid or any(num(v.get(k)) is None for k in ("netCash","quantity","fifoPnlRealized")):return []
            if len(legs)!=1 or legs[0][:2]!=(c,v.get("buySell")) or abs(legs[0][2]-num(v["netCash"]))>=HALF_CENT:return []
            qty[cid]+=num(v["quantity"])
            realized[c]+=num(v["fifoPnlRealized"])
        elif asset=="CASH":
            pair=v.get("symbol","").split(".")
            native={currency:a for currency,k,a in legs if k=="FOREX"}
            if len(pair)!=2 or len(legs)!=2 or set(pair)!=set(native) or v.get("currency")!=pair[1]:return []
            if any(num(v.get(k)) is None or abs(native[currency]-num(v[k]))>=HALF_CENT for currency,k in zip(pair,("quantity","proceeds"))):return []
        else:return []
    if set(by_trade)!=trade_ids:return []
    held={}
    for v in values("OpenPosition"):
        c=v.get("currency");cid=v.get("conid")
        if v.get("assetCategory")!="STK" or v.get("levelOfDetail")!="SUMMARY" or day(v.get("reportDate"))!=end:return []
        if c not in rates or num(v.get("fxRateToBase"))!=rates[c] or not cid or cid in held:return []
        if any(num(v.get(k)) is None for k in ("positionValue","fifoPnlUnrealized","position")):return []
        held[cid]=num(v["position"]);stocks[c]+=num(v["positionValue"]);unrealized[c]+=num(v["fifoPnlUnrealized"])
    if any(qty[c]!=held.get(c,ZERO) for c in set(qty)|set(held)):return []
    if abs(sum((v*rates[c] for c,v in stocks.items()),ZERO)-num(terminal["stock"]))>=HALF_CENT:return []
    for c in set(realized)|set(stocks)|set(cash_rows):
        if abs(stocks[c]+mov[c,"BUY"]+mov[c,"SELL"]-realized[c]-unrealized[c])>=HALF_CENT:return []
    parts_eur=[
        ("Valorisation instantanée moins clôture Flex",num(frozen["instant"])-closing),
        ("Flux externes moins capital du diagnostic",converted("DEP")+converted("WITH")-num(frozen["capital"])),
        ("Réalisé FIFO actions du relevé au change de clôture moins réalisé AG1",sum((r*rates[c] for c,r in realized.items()),ZERO)-num(frozen["fifo"])),
        ("Latent actions à la clôture moins latent instantané IBKR",sum((r*rates[c] for c,r in unrealized.items()),ZERO)-num(frozen["latent"])),
        ("Conversions de devises valorisées au change de clôture",converted("FOREX")),
        ("Dividendes au change de clôture",converted("DIV")),
        ("Retenues sur dividendes au change de clôture",converted("FRTAX")),
        ("Taxes sur transactions au change de clôture",converted("TTAX"))]
    reason="Reconstitution par les écritures de fonds et les quantités par contrat. Taux de clôture en EUR appliqué à tous les flux et résultats natifs."
    out=[line(end,k,"expliqué",reason,v,source=src) for k,v in parts_eur]
    last=compare(end,"Résidu historique à ventiler",frozen["residual"],sum((v for _,v in parts_eur),ZERO),reason,source=src,rounded=True)
    out.append(last)
    return out
