"""Call accounting. A missing invoice, missing output or unallocated vendor scope stays visible."""
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
import json

PROVIDERS=("OpenAI","Anthropic","DeepSeek","Données","VPS")
def price(r):
    model=r["requested"];ts=r.get("at")
    if not ts or not model or not r.get("output_reliable") or None in (r["input"],r["output"]):return None,None
    dt=datetime.fromisoformat(ts.replace("Z","+00:00"))
    if dt.date().isoformat()<"2026-11-20":return None,None
    tariffs={"deepseek-v4-flash":(.003,.15,.60),"deepseek-v4-pro":(.022,.66,1.98)}
    if model not in tariffs:return None,None
    hit,miss,out=tariffs[model]
    factor=2 if dt.weekday()<5 and (1<=dt.hour<4 or 6<=dt.hour<10) else 1
    cache=r.get("cache_read")
    if cache is not None and not 0<=cache<=r["input"]:return None,None
    lo=r["input"] if cache is None else cache
    hi=0 if cache is None else cache
    def cost(h):return (h*hit+(r["input"]-h)*miss+r["output"]*out)*factor/1e6
    return cost(lo),cost(hi)

def read_billing(path):
    if not path.exists():return [],{}
    doc=json.loads(path.read_bytes())
    invoices=[];seen=set()
    for r in doc.get("invoices",[]):
        uid=r["id"]; month=r["month"];datetime.strptime(month,"%Y-%m")
        if uid in seen or r["provider"] not in PROVIDERS:raise ValueError("Pièce dupliquée ou fournisseur inconnu")
        seen.add(uid)
        parts={k:Decimal(str(v)) for k,v in r["allocation_eur"].items()}
        if not parts.keys()<=set(("AG1","AG2","AG4","Données","VPS","Commun")) or any(not v.is_finite() for v in parts.values()):
            raise ValueError("Ventilation invalide")
        total=Decimal(str(r["total_eur"]));other=Decimal(str(r["other_projects_eur"]))
        if sum(parts.values(),other)!=total or not r.get("source") or not r.get("basis"):
            raise ValueError("Pièce ou ventilation non rapprochée")
        invoices.append(dict(id=uid,month=month,provider=r["provider"],total=float(total),other=float(other),
                             project=float(sum(parts.values())),parts={k:float(v) for k,v in parts.items()},basis=r["basis"],source=r["source"]))
    coverage={}
    for month,providers in doc.get("closed_months",{}).items():
        available={r["provider"] for r in invoices if r["month"]==month}
        coverage[month]=set(providers)==set(PROVIDERS) and set(PROVIDERS)<=available
    return invoices,coverage

def usage(db,models,audit,billing):
    invoices,coverage=read_billing(billing)
    calls=[json.loads(r[0]) for r in db.execute("select body from calls order by eid,node,attempt,item")]
    exists={(str(r["execution"]),r["node"]) for r in calls}
    # Pruned n8n traces: recover only counters and identities preserved by the dated audit.
    for eid,nodes in audit["llm"].items():
        if not eid.isdigit():continue
        for node,v in nodes.items():
            if (eid,node) in exists:continue
            run=audit["runs"].get(eid)
            if not run:continue
            anth="Anthropic" in node;ds="DeepSeek" in node
            def first(k):return (v.get(k) or [None])[0]
            calls.append(dict(execution=int(eid),workflow="AG1-AG1V4CONSENSUS",run=run,node=node,producer="AG1",
                              provider="Anthropic" if anth else "DeepSeek" if ds else "OpenAI",
                              requested=(audit["llm"].get(node) or [None])[0],returned=first("model"),attempt=0,item=0,
                              at=datetime.strptime(run[4:12],"%Y%m%d").date().isoformat(),ms=None,status="archive",
                              counter="audit n8n",input=first("input_tokens") if anth else first("promptTokens"),output=first("output_tokens") if anth else first("completionTokens"),
                              cache_read=first("cache_read_input_tokens"),cache_write=first("cache_creation_input_tokens"),
                              output_reliable=anth or ds,prompt=None,version=None,retry_of=None))
    grouped=defaultdict(list)
    for r in calls:
        key={"OpenAI":"chatgpt52","Anthropic":"claude_sonnet46","DeepSeek":"grok41_reasoning"}.get(r["provider"])
        r["registry"]=(models.get((r["run"],key)) or audit["model_names"].get(str(r["run"])+"/"+str(key))) if r["producer"]=="AG1" else None
        r["estimate_usd_low"],r["estimate_usd_high"]=price(r)
        grouped[(r["at"][:7],r["producer"],r["provider"],r["requested"])].append(r)
    groups=[]
    for (m,src,p,model),rs in grouped.items():
        estimates=[r for r in rs if r["estimate_usd_low"] is not None]
        groups.append(dict(month=m,source=src,provider=p,model=model,calls=len(rs),input=sum(r["input"] for r in rs if r["input"] is not None),
                           output_known=sum(r["output"] for r in rs if r["output_reliable"]),missing_input=sum(r["input"] is None for r in rs),
                           uncertain_output=sum(not r["output_reliable"] for r in rs),errors=sum(r["status"] in ("error","crashed","canceled") for r in rs),
                           priced=len(estimates),usd_low=sum(r["estimate_usd_low"] for r in estimates) if estimates else None,
                           usd_high=sum(r["estimate_usd_high"] for r in estimates) if estimates else None))
    return dict(calls=calls,groups=groups,invoices=invoices,coverage=coverage,
                execution_coverage=[dict(workflow=w,first=a,last=b,retained=n) for w,a,b,n in db.execute("select workflow,min(at),max(at),count(*) from seen group by workflow")])
