"""Dated EUR benchmarks. Unknown prices and conversions propagate to the dependent witness."""
from bisect import bisect_right
from calendar import monthrange
from datetime import timedelta
from datetime import date,datetime,timezone
import json
from pathlib import Path
from urllib.request import Request,urlopen
from zoneinfo import ZoneInfo

START="2026-06-11"
FREEZE="2026-10-30"

def last(series,day,lag=None):
    dates=sorted(series);i=bisect_right(dates,day)-1
    if i<0:return None
    key=dates[i]
    if lag is not None and (date.fromisoformat(day)-date.fromisoformat(key)).days>lag:return None
    return series[key]

def prices_eur(series,ccy,fx,day):
    if day not in series:return None
    if ccy=="EUR":return series[day]
    rate=last(fx,day,4) if ccy=="USD" else None
    return series[day]/rate if rate and rate>0 else None

def market_price(symbol):
    request=Request("https://query1.finance.yahoo.com/v8/finance/chart/"+symbol+"?range=1y&interval=1d&events=div%2Csplits",headers={"User-Agent":"Mozilla/5.0"})
    with urlopen(request,timeout=12) as response:
        r=json.load(response)["chart"]["result"][0]
    zone=ZoneInfo(r["meta"]["exchangeTimezoneName"])
    if r["meta"]["symbol"].upper()!=symbol.upper():raise ValueError("Identité cotation")
    quotes=r["indicators"]["adjclose"][0]["adjclose"]
    today=datetime.now(zone).date().isoformat()
    return r["meta"]["currency"],{datetime.fromtimestamp(t,zone).date().isoformat():x for t,x in zip(r["timestamp"],quotes,strict=True)
                                if x and x>0 and datetime.fromtimestamp(t,zone).date().isoformat()<today}

def curves(official,world,fx,cash,holdings,stocks,base,end):
    dates=sorted(d for d in world if START<=d<=end)
    matched={};level=1
    if not dates or dates[0]!=START:raise ValueError("Référence de départ absente")
    matched[START]=1
    for prev,d in zip(dates,dates[1:]):
        snapshot=last(cash,prev)
        if snapshot is None:
            if prev<min(cash):weight=0
            else:weight=None
        else:weight=1-float(snapshot[0])/float(snapshot[1]) if snapshot[1] else None
        level=level*(1+weight*(world[d]/world[prev]-1)) if level is not None and weight is not None else None
        matched[d]=level
    def basket(day):
        if day<FREEZE:return None
        pnl=0
        for sym,value in holdings.items():
            currency,history=stocks[sym]
            a=prices_eur(history,currency,fx,FREEZE);b=prices_eur(history,currency,fx,day)
            if not a or not b:return None
            pnl+=float(value)*(b/a-1)
        return 1+pnl/float(base)
    out=[]
    for d in sorted(official):
        if not START<=d<=end:continue
        nav,twr=official[d]
        fund=last(world,d,4);m=last(matched,d,4)
        out.append(dict(date=d,nav=nav,account=(1+twr)/(1+official[START][1])-1 if twr is not None else None,
                        twr=twr,world=fund/world[START]-1 if fund else None,matched=m-1 if m is not None else None,
                        basket=(b-1) if (b:=basket(d)) is not None else None,
                        fund_date=max((k for k in world if k<=d),default=None)))
    return out

def relative(old,new):
    return (1+new)/(1+old)-1 if old is not None and new is not None else None

def monthly(series):
    last_by_month={}
    for r in series:last_by_month[r["date"][:7]]=r
    rows=[];prior=series[0]
    for m,row in last_by_month.items():
        fields={key:relative(prior[key],row[key]) for key in ("account","world","matched","basket")}
        if prior["date"]<FREEZE<=row["date"] and row["basket"] is not None:fields["basket"]=row["basket"]
        rows.append(dict(month=m,start=prior["date"],end=row["date"],nav=row["nav"],**fields))
        prior=row
    return rows

def calculate(audit,account,cash,market,pa,flows,bills,flex_end):
    funds=audit["market"];hold=audit["holdings"]
    stocks={s:[None,{}] for s in hold}
    for s,d,v,ccy in audit["prices"]:stocks[s][0]=ccy;stocks[s][1][d]=v
    official={d:[None,v] for d,v in audit["nav"].items()}
    ref=curves(official,funds["EUNL.DE"],funds["EURUSD=X"],audit["cash"],hold,stocks,audit["cash"][FREEZE][1],"2026-11-18")
    by_day={r["date"]:r for r in ref}
    rx=by_day["2026-11-16"];rs=by_day[FREEZE]
    check=dict(end=ref[-1],basket=rx["basket"],held_account=relative(rs["account"],rx["account"]),held_world=relative(rs["world"],rx["world"]))
    for r in account["series"]:
        if r.get("twr") is not None:official[r["date"]]=[r["value"],r["twr"]]
    official.update(pa or {})
    world=dict(funds["EUNL.DE"]);fx=dict(funds["EURUSD=X"])
    for s,(ccy,pr) in market.items():
        if s=="EUNL.DE" and ccy=="EUR":world.update(pr)
        elif s=="EURUSD=X" and ccy=="USD":fx.update(pr)
        elif s in stocks and ccy==stocks[s][0]:stocks[s][1].update(pr)
    end=max(d for d,(v,t) in official.items() if v is not None and t is not None)
    live=curves(official,world,fx,{**audit["cash"],**cash},hold,stocks,audit["cash"][FREEZE][1],end)
    monthly_rows=monthly(live)
    # Flows and expenses are grouped by calendar month, including weekends after its last close.
    net_flows=sum(float(v) for d,v in flows if d<=end) if end<=flex_end else None
    gross=float(official[end][0])-net_flows if net_flows is not None else None
    first=min((d for d,_ in flows),default=START)
    # Monthly NAV differences neutralise the external cash flows booked during the period.
    # Only the requested API perimeter is allocated; shared VPS and subscriptions have no assigned amount.
    ranges=bills.get("provider_ranges",{})
    def covered_between(start,end):
        d=date.fromisoformat(start)
        while d.isoformat()<=end:
            if any(not any(a<=d.isoformat()<=b for a,b in ranges.get(p,[])) for p in ("OpenAI","Anthropic","DeepSeek")):
                return False
            d+=timedelta(days=1)
        return True
    def documented(start,end):
        values=[amt for d,amt in bills.get("daily_project_eur",{}).items() if start<=d<=end]
        return sum(values) if None not in values else None
    amount=documented(first,end)
    previous_nav=None
    for row in monthly_rows:
        start=row["month"]+"-01";year,month=map(int,row["month"].split("-"))
        d=min(end,f"{year:04d}-{month:02d}-{monthrange(year,month)[1]:02d}")
        row["spending_to"]=d
        row["flow"]=sum(float(v) for dt,v in flows if start<=dt<=d) if d<=flex_end else None
        row["api_eur"]=documented(start,d)
        row["api_covered"]=covered_between(start,d)
        row["net_eur"]=None
        if previous_nav is not None and row["api_covered"] and row["flow"] is not None and row["api_eur"] is not None:
            row["net_eur"]=row["nav"]-previous_nav-row["flow"]-row["api_eur"]
        previous_nav=row["nav"]
    return dict(start=START,freeze=FREEZE,end=end,series=live,months=monthly_rows,check=check,flows=flows,
                api_documented=amount,api_covered=covered_between(first,end),
                after_documented=gross-amount if gross is not None and amount is not None else None,
                gross=gross,net=(gross-amount if covered_between(first,end) and gross is not None and amount is not None else None),flow_end=flex_end,
                market_dates={s:max(v[1]) for s,v in market.items() if v[1]},holding_symbols=list(hold))
