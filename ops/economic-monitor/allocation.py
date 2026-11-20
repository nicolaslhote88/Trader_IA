from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo
from performance import last

def alias(model):
    return "deepseek-flash" if model in ("deepseek-v4-flash","deepseek-flash") else model

def attach(usage,statement,scope,fx):
    covered=set(scope.get("exclusive",[]))
    by_key=defaultdict(list)
    for r in usage["calls"]:
        timestamp=r["at"]
        if len(timestamp)==10 or r["provider"]!="DeepSeek":continue
        day=datetime.fromisoformat(timestamp.replace("Z","+00:00")).astimezone(ZoneInfo("Europe/Paris")).date().isoformat()
        by_key[day,alias(r["requested"])].append(r)
    monthly=defaultdict(lambda:defaultdict(Decimal))
    matched=[]
    estimates=defaultdict(lambda:defaultdict(Decimal))
    for l in statement["lines"]:
        p,day,model=l["provider"],l["date"],l["model"];usd=Decimal(l["usd"])
        calls=by_key[day,alias(model)] if p=="DeepSeek" else []
        count=l["usage"].get("request_count")
        inputs=sum(l["usage"].get(x,0) for x in ("input_cache_hit_tokens","input_cache_miss_tokens"))
        out=l["usage"].get("output_tokens")
        exact=(calls and count==len(calls) and all(r["input"] is not None and r["output_reliable"] for r in calls)
               and sum(r["input"] for r in calls)==inputs and sum(r["output"] for r in calls)==out
               and l["meter_delta"] is not None and abs(Decimal(l["meter_delta"]))<Decimal(".0001"))
        sources={r["producer"] for r in calls}
        bucket=next(iter(sources)) if exact and len(sources)==1 else "Commun projet" if exact or p in covered else "Périmètre ouvert"
        measured=[r for r in calls if r["input"] is not None and r["output_reliable"] and r["output"] is not None]
        if (p in covered and measured and count and len(measured)<=count and inputs and out
                and sum(r["input"] for r in measured)<=inputs and sum(r["output"] for r in measured)<=out
                and l.get("meter_delta") is not None and abs(Decimal(l["meter_delta"]))<Decimal(".0001")):
            components=l.get("component_usd",{})
            input_cost=sum((Decimal(v) for k,v in components.items() if k.startswith("input_")),Decimal(0))
            output_cost=Decimal(components.get("output_tokens","0"))
            for r in measured:
                estimate=input_cost*r["input"]/inputs+output_cost*r["output"]/out
                r["allocated_estimate_usd"]=float(estimate)
                estimates[day[:7],p][r["producer"]]+=estimate
        if exact:
            matched.append(dict(day=day,model=model,calls=count,usd=float(usd),bucket=bucket))
            if len(calls)==1:calls[0]["cost_export_usd"]=float(usd)
        rate=last(fx,day,5)
        m=monthly[day[:7],p]
        m["total_usd"]+=usd;m[bucket]+=usd
        if rate:m["eur"]+=usd*Decimal(str(rate))
        elif usd:m["missing_fx"]+=1
    days=defaultdict(Decimal)
    provider_day=defaultdict(Decimal)
    for l in statement["lines"]:
        if l["provider"] in covered:
            days[l["date"]]+=Decimal(l["usd"])
            provider_day[l["provider"],l["date"]]+=Decimal(l["usd"])
    for call in usage["calls"]:
        value=provider_day.get((call["provider"],call["at"][:10]))
        call["provider_day_usd"]=float(value) if value is not None else None
    usage["daily_project_usd"]={day:float(v) for day,v in days.items()}
    usage["daily_project_eur"]={day:float(v)*rate if (rate:=last(fx,day,5)) else None for day,v in days.items()}
    totals=[]
    for (m,p),v in sorted(monthly.items()):
        assert sum((v[k] for k in ("AG1","AG2","AG4","Commun projet","Périmètre ouvert")),Decimal(0))==v["total_usd"]
        est=estimates[m,p]
        totals.append(dict(month=m,provider=p,estimate_AG1=float(est["AG1"]),estimate_AG2=float(est["AG2"]),estimate_AG4=float(est["AG4"]),
                           estimate_unallocated=float(v["total_usd"]-sum(est.values(),Decimal(0))),
                           **{k:float(v[k]) for k in ("total_usd","AG1","AG2","AG4","Commun projet","Périmètre ouvert")},
                           reporting_eur=None if v["missing_fx"] else float(v["eur"])))
    usage.update(provider_costs=totals,proofs=statement["manifests"],provider_ranges=statement["ranges"],exact_days=matched,
                 exclusivity=sorted(covered))
