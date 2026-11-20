"""Retain only keyed accounting counters and observable market evidence from finished n8n runs."""
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys
sys.path.insert(0,"/opt/trader-ia/decision-journal/current")
from evidence import Trace, fingerprint

def db_open(path):
    db=sqlite3.connect(path,timeout=12)
    db.executescript("""
    create table if not exists seen(eid integer primary key, workflow text, status text, at text);
    create table if not exists calls(eid integer,node text,attempt integer,item integer,body text,primary key(eid,node,attempt,item));
    create table if not exists packs(eid integer,contract text,body text,primary key(eid,contract));
    create table if not exists sends(eid integer,orderid text,body text,primary key(eid,orderid));
    create table if not exists quotes(contract text,observed real,stamp real,bid real,ask real,availability text,primary key(contract,observed));
    """)
    return db

def iso(ms):
    return datetime.fromtimestamp(ms/1000,timezone.utc).isoformat() if ms else None

def subset(trace, row, keys):
    return trace.take(row,keys)

def save_run(db,eid,workflow,status,at,workflow_json,raw):
    tr=Trace(raw)
    graph=json.loads(workflow_json)
    producer=workflow.split("-")[0]
    defs={n["name"]:n for n in graph["nodes"]}
    version=graph.get("versionId") or graph.get("activeVersionId")
    run_id=prompt=None
    for name in tr.runs:
        if name.startswith("2B - Init Run Context"):
            for _,row in tr.items(name):
                ctx=tr.follow(row.get("run",{}))
                run_id=tr.follow(ctx.get("runId"));prompt=tr.follow(ctx.get("promptVersion"))
    for name,vals in tr.runs.items():
        n=defs.get(name,{})
        anthropic="Anthropic Messages" in name
        if "lmChat" in n.get("type","") or anthropic:
            typ=n["type"].lower()
            provider="Anthropic" if anthropic or "anthropic" in typ else "OpenAI" if "openai" in typ else "DeepSeek" if "deepseek" in typ else typ.rsplit(".",1)[-1]
            param=n.get("parameters",{})
            model=param.get("model") or param.get("modelName")
            if isinstance(model,dict):model=model.get("value")
            graph_channel="main" if anthropic else "ai_languageModel"
            outputs={}
            for idx,row in tr.items(name,graph_channel):
                outputs.setdefault(idx,[]).append(row)
            for idx,ref in enumerate(tr.follow(vals)):
                node=tr.follow(ref)
                requested=model
                source=[tr.expand(tr.follow(v)) for v in tr.follow(node.get("source",[]))]
                if anthropic and source:
                    previous=source[0].get("previousNode")
                    for ix,r in tr.items(previous):
                        if ix==source[0].get("previousNodeRun",idx) and r.get("model") is not None:
                            requested=tr.follow(r["model"]);break
                rows=outputs.get(idx) or [{}]
                for item,row in enumerate(rows):
                    k=next((field for field in ("usage","tokenUsage","tokenUsageEstimate") if field in row),None)
                    usage=tr.expand(tr.follow(row[k])) if k else {}
                    returned=tr.follow(row.get("model"))
                    data=dict(execution=eid,workflow=workflow,run=run_id,prompt=prompt,version=version,node=name,
                              producer=producer,provider=provider,requested=requested,returned=returned,attempt=idx,item=item,
                              at=iso(node.get("startTime")) or at,ms=node.get("executionTime"),status=node.get("executionStatus") or ("error" if "error" in node else status),
                              counter=k,input=usage.get("input_tokens",usage.get("promptTokens")),output=usage.get("output_tokens",usage.get("completionTokens")),
                              cache_read=usage.get("cache_read_input_tokens",usage.get("prompt_cache_hit_tokens")),
                              cache_write=usage.get("cache_creation_input_tokens"),retry_of=graph.get("retryOf"))
                    # Provider-internal retries are invisible. idx identifies the node invocation, including loops.
                    data["output_reliable"]=not (k=="tokenUsageEstimate" or data["output"] is None)
                    if not prompt: data["prompt_workflow_version"]=version
                    db.execute("insert or replace into calls values(?,?,?,?,?)",(eid,name,idx,item,json.dumps(data)))
        if workflow.startswith("AG1-") and "Liquidity Preflight" in name:
            for _,row in tr.items(name):
                pack=tr.follow(row.get("opportunity_pack",{}))
                for obj in tr.follow(pack.get("rows",[])):
                    r=tr.follow(obj); liquid=tr.follow(r.get("liquidity",{}))
                    out=subset(tr,liquid,"conid quoteTime quoteSource quoteTimestampSource ibkrDataAvailability bid ask price currency fxRateToEUR quoteAgeMinutes spreadObserved estimatedTargetQty")
                    out.update(subset(tr,r,"symbol held decision"))
                    step=tr.follow(tr.follow(vals)[0])
                    out["observed"]=iso(step.get("startTime",0)+step.get("executionTime",0))
                    out["run"]=run_id
                    if out.get("conid"):
                        db.execute("insert or replace into packs values(?,?,?)",(eid,str(out["conid"]),json.dumps(out)))
        if workflow.startswith("AG1-") and "IBKR Send Orders" in name:
            for _,row in tr.items(name):
                for obj in tr.follow(row.get("orders",[])):
                    r=tr.follow(obj);out=subset(tr,r,"orderId symbol side quantity currency expectedFeesEUR estNotionalEUR priceEUR fxRateToEUR ibkrStatus")
                    details=tr.follow(r.get("ibkrResponse",{}))
                    if not isinstance(details,dict):details={}
                    out["sent_at"]=tr.follow(details.get("sent_at"))
                    if out.get("orderId"):db.execute("insert or replace into sends values(?,?,?)",(eid,out["orderId"],json.dumps(out)))
    db.execute("insert into seen values(?,?,?,?)",(eid,workflow,status,at))

def collect(db,path):
    src=sqlite3.connect(Path(path).resolve().as_uri()+"?mode=ro",uri=True,timeout=5)
    mapping={}
    for wid,name in src.execute("select id,name from workflow_entity where active and (id='AG1V4CONSENSUS' or name like 'AG2%' or name like 'AG4%')"):
        if wid=="AG1V4CONSENSUS" or name.startswith(("AG2-V3","AG4-V3","AG4_Spé")) and "Health" not in name:
            mapping[wid]=name[:3]+"-"+wid
    seen={r[0] for r in db.execute("select eid from seen")}
    jobs=src.execute("select e.id,e.workflowId,e.status,e.startedAt,e.retryOf from execution_entity e where e.finished=1 or e.status in ('error','crashed','canceled','success') order by e.id").fetchall()
    failures=[]
    for eid,wid,status,at,retry in jobs:
        if wid not in mapping or eid in seen:continue
        row=src.execute("select workflowData,data from execution_data where executionId=?",(eid,)).fetchone()
        if not row or not all(row):continue
        try:
            with db:
                save_run(db,eid,mapping[wid],status,at,*row)
                if retry:db.execute("update calls set body=json_set(body,'$.retry_of',?) where eid=?",(str(retry),eid))
        except (KeyError,ValueError,IndexError,TypeError) as e:
            failures.append([eid,type(e).__name__])
    src.close()
    return failures
