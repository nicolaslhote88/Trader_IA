"""Run via SSH on the VPS. Emit only the private, account-scoped reconciliation inputs."""
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import urllib.request

sys.path.insert(0,"/opt/trader-ia/decision-journal/current")
from ledger import reader


def broker(path):
    return json.load(urllib.request.urlopen("http://127.0.0.1:18080/"+path,timeout=20))


def query(db,sql,params=()):
    cur=db.execute(sql,params)
    return [dict(zip([k[0] for k in cur.description],r)) for r in cur.fetchall()]


def main():
    h=broker("health")
    alignment=h.get("account_alignment",{})
    acct=alignment.get("selected_account")
    if not (h.get("authenticated") and h.get("dry_run") is False and alignment.get("aligned") and
            alignment.get("gateway_is_paper") is False and acct and acct==alignment.get("configured_account_id")):
        raise ValueError("Compte de référence indisponible")
    captured=datetime.now(timezone.utc).isoformat()
    out={"captured":captured,"account":hashlib.sha256(acct.upper().encode()).hexdigest(),
         "positions":[],"cash":[],"nav":[],"fills":[],"trades":[]}
    live_ledger=broker("account/ledger")
    if not isinstance(live_ledger,dict) or not live_ledger.get("BASE"):
        raise ValueError("Grand livre indisponible")
    out["broker_nav"]=live_ledger["BASE"].get("netliquidationvalue")
    out["cash_base_currency"]=live_ledger["BASE"].get("currency")
    positions=broker("positions")
    if not isinstance(positions,list):
        raise ValueError("Inventaire IBKR indisponible")
    out["positions"].append({"date":captured[:10],"at":captured,"basis":"intraday","rows":[
        {"conid":str(p["conid"]),"qty":p.get("position"),"symbol":p.get("ticker") or p.get("contractDesc")}
        for p in positions if p.get("conid")]})
    with TemporaryDirectory() as tmp:
        with reader(Path("/local-files/duckdb/broker_costs.duckdb"),tmp) as b:
            out["cash"]=query(b,"select cast(ts_day as varchar) date,cast(ts as varchar) at_utc,currency,cashbalance,settledcash from cash_snapshots order by ts_day,currency")
            for r in out["cash"]:
                r["basis"]="intraday"
            out["trades"]=query(b,"select execution_id,conid,symbol,side,size,price,commission,cast(trade_ts as varchar) ts from broker_trades where account=? order by trade_ts,execution_id",[acct])
        with reader(Path("/local-files/duckdb/ag1_v4_consensus.duckdb"),tmp) as d:
            out["fills"]=query(d,"""select f.fill_id,f.qty,f.currency,f.price_native,c.commission_eur,c.commission_ccy,c.commission_amount,
                    c.commission_source,coalesce(nullif(c.broker_execution_id,''),json_extract_string(f.raw_fill_json,'$.ibkrFill.execution_id')) execution_id,
                    json_extract_string(f.raw_fill_json,'$.ibkrFill.conid') conid,cast(f.ts_fill as varchar) ts,o.side,o.symbol
                    from core.fills f left join core.fill_costs c using(fill_id) left join core.orders o on o.order_id=f.order_id order by f.ts_fill,f.fill_id""")
            out["flows"]=query(d,"select cash_tx_id,currency,amount,type,cast(ts as varchar) ts from core.cash_ledger order by ts")
            out["register_nav"]=query(d,"select cast(ts as date)::varchar date,cast(ts as varchar) at_utc,total_value_eur from core.portfolio_snapshot qualify row_number() over(partition by ts::date order by ts desc,run_id desc)=1 order by date")
    pa_script="""import os,json,httpx
url=os.environ.get('IBKR_GATEWAY_URL','https://ibkr-gateway:5000').rstrip('/')+'/v1/api/'
with httpx.Client(base_url=url,verify=False,timeout=35) as client:
    a=client.get('iserver/accounts').json()['selectedAccount']
    if not a or a!=os.environ.get('IBKR_ACCOUNT_ID'):raise RuntimeError('account')
    resp=client.post('pa/performance',json={'acctIds':[a],'period':'1Y','freq':'D'})
    resp.raise_for_status()
    j=resp.json()
    nav=j['nav'];twr=j['cps'];n=nav['data'][0];c=twr['data'][0]
    if n['baseCurrency']!='EUR' or c['baseCurrency']!='EUR' or nav['freq']!='D' or twr['freq']!='D':raise RuntimeError('scope')
    ret=dict(zip(twr['dates'],c['returns'],strict=True))
    print(json.dumps([{'date':dt,'value':v,'twr':ret.get(dt)} for dt,v in zip(nav['dates'],n['navs'],strict=True)]))
"""
    z=subprocess.run(["docker","exec","-i","ibkr-broker","python","-"],input=pa_script.encode(),capture_output=True,timeout=65)
    if z.returncode:
        raise ValueError("PortfolioAnalyst indisponible")
    out["nav"]=json.loads(z.stdout)
    print(json.dumps(out,default=str,allow_nan=False))


if __name__=="__main__":
    main()
