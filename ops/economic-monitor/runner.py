import argparse
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import ssl
import sys
from tempfile import TemporaryDirectory,NamedTemporaryFile
from urllib.request import Request,urlopen
from zoneinfo import ZoneInfo

from allocation import attach
from costs import costs
from performance import calculate,market_price
from traces import db_open,collect
from usage import usage

N8N="/var/lib/docker/volumes/n8n_data/_data/database.sqlite"
LEDGER="/local-files/duckdb/ag1_v4_consensus.duckdb"
ACCOUNT="/local-files/duckdb/lot03_reporting/compte.json"
STATE=Path("/var/lib/trader-economic")
OUT=Path("/local-files/duckdb/economic_reporting")

def now():return datetime.now(timezone.utc).isoformat()
def read(p):return json.loads(Path(p).read_bytes())
def put(path,data):
    with NamedTemporaryFile(mode="w",encoding="utf8",dir=path.parent,delete=False) as f:
        json.dump(data,f,ensure_ascii=False,allow_nan=False,default=str,separators=(",",":"));f.flush();os.fsync(f.fileno())
    os.chmod(f.name,0o600);os.replace(f.name,path)

def fetch(url,body=None,tls=None):
    req=Request(url,data=json.dumps(body).encode() if body else None,headers={"Content-Type":"application/json"})
    with urlopen(req,timeout=12,context=tls) as r:return json.load(r)

def fetch_pa(account):
    h=fetch("http://127.0.0.1:18080/health");a=h.get("account_alignment") or {};acct=a.get("selected_account")
    if not (h.get("authenticated") and h.get("dry_run") is False and a.get("aligned") and a.get("gateway_is_paper") is False
            and acct and acct==a.get("configured_account_id") and hashlib.sha256(acct.upper().encode()).hexdigest()==account):
        raise ValueError("Compte IBKR non confirmé")
    url="https://127.0.0.1:5000/v1/api/";tls=ssl._create_unverified_context()
    if fetch(url+"iserver/accounts",tls=tls).get("selectedAccount")!=acct:raise ValueError("Compte sélectionné")
    result=fetch(url+"pa/performance",{"acctIds":[acct],"period":"1Y","freq":"D"},tls)
    if fetch(url+"iserver/accounts",tls=tls).get("selectedAccount")!=acct:raise ValueError("Compte modifié")
    def field(k,prop):
        pack=result[k];series=pack["data"]
        if len(series)!=1 or series[0]["baseCurrency"]!="EUR" or pack["freq"]!="D":raise ValueError("Périmètre PortfolioAnalyst")
        return dict(zip(pack["dates"],series[0][prop],strict=True))
    nav=field("nav","navs");twr=field("cps","returns");today=datetime.now(ZoneInfo("Europe/Paris")).date().isoformat()
    return {d:[v,twr[k]] for k,v in nav.items() if (d:=datetime.strptime(k,"%Y%m%d").date().isoformat())<today and v is not None and twr.get(k) is not None}

def ledger():
    sys.path.insert(0,"/opt/trader-ia/decision-journal/current")
    from ledger import reader
    with TemporaryDirectory() as tmp,reader(Path(LEDGER),tmp) as db:
        def query(sql):
            cur=db.execute(sql);names=[c[0] for c in cur.description]
            return [dict(zip(names,r)) for r in cur.fetchall()]
        fills=query("""select f.order_id,o.symbol,o.side,cast(o.ts_created as varchar) as at,o.qty::double ordered,
             try_cast(json_extract_string(o.rationale_json,'$.expectedFeesEUR') as double) expected,
             coalesce(nullif(c.broker_execution_id,''),json_extract_string(f.raw_fill_json,'$.ibkrFill.execution_id')) execution,
             json_extract_string(f.raw_fill_json,'$.ibkrFill.conid') conid,
             try_cast(json_extract_string(f.raw_fill_json,'$.ibkrFill.price') as double) native_price,
             f.qty::double filled, f.fees_eur fees,f.slippage_bps slip from core.fills f left join core.fill_costs c using(fill_id)
             join core.orders o on o.order_id=f.order_id order by f.ts_fill,f.fill_id""")
        cash={d:[v,n] for d,v,n in db.execute("""select cast(ts as date)::varchar,cash_eur,total_value_eur from core.portfolio_snapshot
            qualify row_number() over(partition by ts::date order by ts desc,run_id desc)=1""").fetchall()}
        models={(r,k):(model_id or name) for r,k,model_id,name in db.execute("select run_id,model_key,model_id,model_name from core.model_proposals").fetchall()}
        outcomes=dict(db.execute("""select r.run_id,case when count(o.order_id)>0 then 'Ordre inscrit'
            when r.data_ok_for_trading=false then 'Trading bloqué' else 'Aucun ordre inscrit' end
            from core.runs r left join core.orders o using(run_id) where r.strategy_version='strategy_v4_consensus'
            group by r.run_id,r.data_ok_for_trading""").fetchall())
    return fills,cash,models,outcomes

def quote_capture(db):
    latest=db.execute("select max(eid) from packs").fetchone()[0]
    contracts=[c for c, in db.execute("select contract from packs where eid=? order by contract limit 40",(latest,)) if c.isdigit()]
    if not contracts:return 0
    q=fetch("http://127.0.0.1:18080/marketdata/snapshot?conids="+",".join(contracts)+"&fields=84,86,6509,55")
    ts=datetime.now(timezone.utc).timestamp()
    def px(x):
        try:return float(str(x).replace(",",""))
        except (TypeError,ValueError):return None
    rows=[]
    for r in q:
        bid,ask=px(r.get("84")),px(r.get("86"));stamp=px(r.get("_updated"))
        if bid and ask and bid>0 and ask>=bid:
            rows.append((str(r.get("conid")),ts,stamp/1000 if stamp and stamp>1e12 else stamp,bid,ask,str(r.get("6509") or "")))
    with db:db.executemany("insert or ignore into quotes values(?,?,?,?,?,?)",rows)
    return len(rows)

def refresh_market(audit,account,state):
    cache=state/"market.json";data=read(cache) if cache.exists() else {"market":{},"pa":{}}
    issues=[]
    for s in ["EUNL.DE","EURUSD=X",*audit["holdings"]]:
        try:
            cc,px=market_price(s);prior=data["market"].get(s,[cc,{}])
            if not px or prior[0]!=cc:raise ValueError("Série incompatible")
            data["market"][s]=[cc,{**prior[1],**px}]
        except Exception as e:issues.append(s+" "+type(e).__name__)
    try:
        updates=fetch_pa(account["account"])
        if not updates:raise ValueError("Série vide")
        data["pa"].update(updates);data["pa_at"]=now()
    except Exception as e:issues.append("PortfolioAnalyst "+type(e).__name__)
    data["issues"]=issues;data["attempt"]=now();put(cache,data)
    return data

def run(state,out,mode):
    audit=read(state/"audit.json");account=read(ACCOUNT);db=db_open(state/"trace.sqlite")
    if mode=="quotes":
        count=quote_capture(db)
        from costs import tariff,tax_estimate,as_fx
        batch=db.execute("select max(observed) from quotes").fetchone()[0]
        latest=db.execute("select max(eid) from packs").fetchone()[0]
        prior=read(state/"evidence.json")["flex"] if (state/"evidence.json").exists() else read(state/"flex.json")
        meta={v["conid"]:v for v in prior["meta"].values()}
        estimates=[]
        if count and batch:
            for cid,t,bid,ask,availability,raw in db.execute("""select q.contract,q.stamp,q.bid,q.ask,q.availability,p.body
                    from quotes q join packs p on p.contract=q.contract and p.eid=? where q.observed=?""",(latest,batch)):
                pack=json.loads(raw);qty=pack.get("estimatedTargetQty");m=meta.get(cid,{})
                ccy=m.get("currency");day=now()[:10];fx=1 if ccy=="EUR" else as_fx(prior["fx"],day) if ccy=="USD" else None
                if not qty or qty<=0:continue
                nominal=qty*(bid+ask)/2*fx if fx else None
                fee=tariff(m.get("listingExchange"),ccy,qty,nominal,fx)
                tax=tax_estimate(pack["symbol"],"BUY",day,m.get("issuerCountryCode"),nominal)
                fresh=bool(t and 0<=batch-t<=60 and availability.startswith("R"))
                estimates.append(dict(symbol=pack["symbol"],contract=cid,qty=qty,nominal=nominal,commission=fee,tax=tax,
                                      old=nominal*.001 if nominal else None,spread=(ask-bid)*qty*fx if fresh and fx else None,
                                      age=round(batch-t,1) if t else None,availability=availability,at=now(),fresh=fresh))
        put(out/"quotes.json",dict(at=now(),rows=estimates,count=count))
        print("Contrats cotés",count);return
    if mode=="market":refresh_market(audit,account,state);return
    errors=collect(db,N8N)
    bundle=read(state/"evidence.json") if (state/"evidence.json").exists() else {k:read(state/(k+".json")) for k in ("flex","providers","scope")}
    flex=bundle["flex"];fills,cash,models,outcomes=ledger()
    u=usage(db,models,audit,state/"billing.json")
    exports=bundle["providers"];scope=bundle["scope"]
    attach(u,exports,scope,flex["fx"]);u["trace_errors"]=errors
    for call in u["calls"]:
        if call["producer"]=="AG1":call["decision_outcome"]=outcomes.get(call["run"])
        if not call["prompt"] and call.get("version"):
            call["prompt_configuration"]=call["workflow"]+"/"+call["version"]+"/"+call["node"]
    cost=costs(fills,account,flex,db,audit)
    from costs import tariff,tax_estimate,quality,stamp
    latest=db.execute("select max(eid) from packs").fetchone()[0]
    contracts={r["conid"]:r for r in flex["meta"].values()}
    previews=[]
    for cid,raw in db.execute("select contract,body from packs where eid=?",(latest,)):
        q=json.loads(raw);qty=q.get("estimatedTargetQty");px=q.get("price");fx=q.get("fxRateToEUR")
        if not (qty and qty>0 and px and fx):continue
        prior=contracts.get(cid,{})
        venue=prior.get("listingExchange");ccy=q.get("currency");nom=qty*px*fx
        fee=tariff(venue,ccy,qty,nom,fx)
        tax=tax_estimate(q["symbol"],"BUY",(q.get("observed") or "")[:10],prior.get("issuerCountryCode"),nom)
        spread=(q["ask"]-q["bid"])*qty*fx if quality(q,stamp(q.get("observed"))) else None
        previews.append(dict(symbol=q["symbol"],conid=cid,qty=qty,currency=ccy,place=venue,fx=fx,price=px,nominal=nom,
                             old_estimate=nom*.001,entry_commission=fee,tax=tax,exit_commission=fee,
                             round_trip_spread=spread,known_round_trip=2*fee+tax+spread if None not in (fee,tax,spread) else None,
                             observed=q.get("observed"),quote=q.get("quoteTime"),availability=q.get("ibkrDataAvailability")))
    cost["previews"]=previews
    cost["quote_samples"]=db.execute("select count(*) from quotes").fetchone()[0]
    market=read(state/"market.json") if (state/"market.json").exists() else refresh_market(audit,account,state)
    perf=calculate(audit,account,cash,market["market"],market["pa"],flex["flows"],u,account["end"])
    perf["market_status"]={k:market.get(k) for k in ("issues","attempt","pa_at")}
    for name,value in [("orders",cost),("usage",u),("performance",perf)]:
        value["published"]=now()
    # One snapshot prevents dashboard readers from seeing mismatched cost and performance versions.
    put(out/"report.json",dict(orders=cost,usage=u,performance=perf))
    print("Ordres",len(cost["orders"]),"appels",len(u["calls"]),"clôture",perf["end"],"traces à examiner",len(errors))

if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("mode",choices=["report","market","quotes"]);p.add_argument("--state",type=Path,default=STATE);p.add_argument("--out",type=Path,default=OUT)
    args=p.parse_args();run(args.state,args.out,args.mode)
