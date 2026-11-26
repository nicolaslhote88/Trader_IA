"""Independent acceptance using archived market data, without sending any order."""
import copy
import json
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
sys.path.insert(0, "/opt/trader-ia/research-sim/releases/9a528f28")
from common import load, digest
from worker import policy
import campaign
from books import inputs, run_day
from control import order_preview
from transition import pilot_gate, reconcile, retirement, retirement_plan, verify_retirement
from metrics import window, operating_cost, max_drawdown


def check():
    c=load(Path(__file__).with_name("mandate.json"))
    cfg,conv,version=policy()
    cal=load(campaign.FEED/"sessions.json")
    audit=load(campaign.FEED/"audit.json")
    sources=sorted((campaign.FEED/"pilotage").glob("*.json.gz"))
    obs=load(sources[-1])
    days=["2026-11-20","2026-11-23","2026-11-24","2026-11-25"]
    data={}
    for d in days:
        data[d]=[(datetime.strptime(p.name[:19],"%Y-%m-%dT%H-%M-%S").replace(tzinfo=timezone.utc),p)
                 for p in sorted((campaign.FEED/"batches").glob(d+"*.json.gz"))]
    frames=[inputs(d,data[d],cfg,audit) for d in days]
    assert all(frames) and c["strategy"]==version
    def replay():
        state={}; rows=[]
        for i,day in enumerate(days):
            rows.append(run_day(state,day,frames[i],cfg,conv,cal,days[i-1] if i else None))
        return rows
    a=replay()
    assert digest(a)==digest(replay())
    v=a[0]["books"]["ag3"]
    assert v["normal"]["new_trades"] and not v["retard"]["new_trades"]
    for trade in v["cout_double"]["new_trades"]:
        assert trade["commission"]>=6
    previous={x["symbol"] for x in v["normal"]["new_trades"]}
    assert {x["symbol"] for x in a[1]["books"]["ag3"]["retard"]["new_trades"]}.issubset(previous)
    # Controlled arrival at the same limit on the next session, independent of Monday's actual liquidity.
    state={}; run_day(state,days[0],frames[0],cfg,conv,cal)
    copied=copy.deepcopy(frames[0])
    def later(value):
        return (datetime.fromisoformat(value.replace("Z","+00:00"))+timedelta(days=3)).isoformat()
    for rows in copied[:2]:
        for row in rows:
            for field in ("quote_time","received"):
                if isinstance(row.get(field),str):
                    row[field]=later(row[field])
    deferred=run_day(state,days[1],(*copied[:2],later(copied[2]),later(copied[3]),copied[4]),cfg,conv,cal,days[0])
    assert deferred["books"]["ag3"]["retard"]["new_trades"]
    assert not inputs(days[0],[],cfg,audit)
    assert operating_cost("2045-01-01","2045-01-01",obs) is None
    assert max_drawdown([100,120,108])>0.0999 and max_drawdown([100,None]) is None
    witnesses={k:0. for k in ("account","basket","matched","world")}
    example=dict(obs,fees={"2026-11-20":2},coverage={k:[["2026-11-19","2026-11-20"]]
            for k in ("OpenAI","Anthropic","DeepSeek")},
            bench=[dict(witnesses,date="2026-11-19",nav=10000),dict(witnesses,date=days[0],nav=10000)])
    val=dict(nav=10100,values={"SAP":1100},sectors={"Test":0.109},countries={"DE":0.109})
    tx=dict(symbol="SAP",side="BUY",nominal=1000,cost=0)
    frame=dict(day=days[0],marked={"ag3":{k:val for k in ("normal","retard","cout_double")}},
               books={"ag3":{"normal":{"new_trades":[tx]}}})
    win=window([frame],days[:1],None,10000,"ag3",example,dict(c,start=days[0],anchor="2026-11-19"),
               {days[0]:True},days[0])
    assert win["complete"] and abs(win["net"]-0.0098)<1e-10 and abs(win["stress"]["sans_meilleur"]+0.0002)<1e-10
    assert win["comparisons"]["variant_matched"]["net"]==0 and win["comparisons"]["account"]["net"]<0
    assert not window([frame],days[:1],None,10000,"ag3",example,dict(c,start=days[0],anchor="2026-11-19"),
                      {days[0]:False},days[0])["complete"]
    # Reconcile a dated actual purchase against the original, including the dated FX and the actual commission.
    comp=load("/local-files/duckdb/lot03_reporting/compte.json")
    tr=next(x for x in comp["trades"] if x["symbol"]=="SAP" and x["side"]=="BUY" and x["date"].replace("-","")=="20261118")
    from decimal import Decimal as D
    fill=dict(execution=tr["execution"],order="SAP-audit",symbol="SAP",side="BUY",qty=tr["qty"],
              price=tr["price"],fx=tr["rate"],fee_eur=tr["fee"])
    cost=D(str(fill["qty"]))*D(str(fill["price"]))*D(str(fill["fx"]))+D(str(fill["fee_eur"]))
    o=dict(id="SAP-audit",side="BUY",symbol="SAP",qty=tr["qty"])
    initial=dict(cash="10000",positions={})
    observed=dict(cash=str(D("10000")-cost),positions={"SAP":tr["qty"]})
    assert reconcile(initial,[o],[fill,fill],observed,"0.01")["status"]=="rapproché"
    assert reconcile(initial,[o],[fill,dict(fill,qty=1)],observed,"0.01")["status"]=="suspendre"
    assert reconcile(initial,[dict(o,qty=12)],[fill],observed,"0.01")["remaining"]["SAP-audit"] in ("6","6.0","6.00000000")
    assert reconcile(initial,[],[fill],observed,"0.01")["status"]=="suspendre"
    assert reconcile(initial,[],[],initial,"0.01")["status"]=="rapproché"
    fake=dict(results=[{"variant":"ag3","verdict":"poursuivre l’observation"}],id="audit")
    assert pilot_gate(fake,obs,datetime.now(timezone.utc))["blockers"]
    ticket=dict(symbol="SAP",side="BUY",filled=6,at="2026-11-18")
    mandate=dict(capital=2000,allocation_ratio=0.5,campaign="audit",variant="ag3")
    quotes=dict(SAP=dict(bid=100,ask=101,fx=1,fees_eur=3,estimated_qty=3,fresh=True,session_open=True))
    account=dict(cash=1000,positions={},lots={"SAP":1},quotes=quotes)
    orders=order_preview([ticket],mandate,{"positions":{}},account)
    assert len(orders)==1 and orders[0]["qty"]=="3" and orders==order_preview([ticket],mandate,{"positions":{}},account)
    items=retirement(obs); producer=next(x for x in items if x["group"]=="AG4")
    decision=dict(ids=[producer["id"]],approved_by="recette",graph=producer["graph"],backup_sha="fixture",expected_eur=1)
    consumers=set(producer["consumers"]+producer["direct"])
    plan=retirement_plan(items,decision,{"accepted":True},consumers)
    before=dict(coverage=8,stale=0,errors=0,calls=100,billed_eur=7,days=30,scope="producteur-test")
    after=dict(before,complete_cycles=list(consumers),dashboard_ok=True,calls=80,billed_eur=6)
    assert verify_retirement(plan,before,after)["saved_eur"]==1
    assert verify_retirement(plan,before,dict(after,coverage=7)).startswith("rétablir")
    assert verify_retirement(plan,before,dict(after,complete_cycles=[])).startswith("en attente")
    from withdraw import change
    graph={"fixture":"publication test"}; backup=digest(graph)
    record=dict(plan=dict(plan,backup=backup),phase="préparé")
    engine={"active":True,"posts":0}
    def lookup(wid,v):
        return engine["active"],v if engine["active"] else None,graph,0
    def post(wid,op,payload):
        engine["active"]=op=="activate";engine["posts"]+=1
    assert change("retirer",record,lookup,post,lambda x:None)=="retiré"
    assert change("retirer",record,lookup,post,lambda x:None)=="retiré"
    assert change("rétablir",record,lookup,post,lambda x:None)=="rétabli" and engine["posts"]==2
    from handover import apply, prepare
    shadow=sqlite3.connect(":memory:");prepare(shadow)
    now=datetime.now(timezone.utc)
    approved=dict(mandate,max_loss=150,operating_budget=20,review_by=(now+timedelta(days=10)).isoformat(),
                  suspend="perte de rapprochement",approved_by="recette simulée",version=version,
                  starts=(now-timedelta(minutes=1)).isoformat(),fee_tolerance=0.1,cash_tolerance=0.01)
    local=dict(obs,at=now.isoformat(),reconciled=now.isoformat(),
               broker=dict(aligned=True,paper=False,pending=0,positions={},cash_eur=1000))
    simulated=dict(formal=dict(id="audit",strategy=version,results=[dict(variant="ag3",verdict="retenir")],
                               last_day=now.date().isoformat(),tickets={"ag3":[ticket]}))
    doc=dict(mandate=approved,owners=dict(legacy_fenced=True,cash="pilote",positions={}),inflight=[],
             risk=dict(verified=True,drawdown_eur=0,capital_deployed=0,operating_cost_eur=0),account=account)
    assert apply(shadow,"ouvrir",doc,simulated,local,now)["state"]=="préparé"
    assert apply(shadow,"fiche",doc,simulated,local,now)==apply(shadow,"fiche",doc,simulated,local,now)
    assert shadow.execute("select count(*) from sheets").fetchone()[0]==1
    assert apply(shadow,"suspendre",dict(reason="exercice"))["state"]=="suspendu"
    assert apply(shadow,"restituer",dict(observed=account,owners=dict(cash="AG1",positions={}),confirmed_by="recette",inflight=[]),
                 simulated,local,now)["state"]=="restitué"
    try:
        retirement_plan(items,dict(decision,ids=[producer["id"]]*2),{"accepted":True},consumers)
    except ValueError:
        pass
    else:
        raise AssertionError("retrait multiple accepté")
    # Backdated fixture is scoped to this in-memory acceptance DB, independent of the official campaign.
    test=dict(c,start=days[0],anchor="2026-11-19")
    latest=load(sorted((campaign.FEED/"batches").glob("*.json.gz"))[-1])
    db=sqlite3.connect(":memory:")
    db.executescript("""create table days(campaign,day,frame,state,primary key(campaign,day));
                        create table stops(campaign,variant,reason,primary key(campaign,variant));
                        create table fx(day primary key,rate); create table runs(id,day,status,graph);""")
    date=datetime.fromisoformat("2026-11-26T03:00:00+00:00")
    result=campaign.score(db,test,obs,cal,data,latest,date)
    assert result["sessions"]==4 and not result["gaps"]
    assert result==campaign.score(db,test,obs,cal,data,latest,date)
    assert all(x["verdict"]!="retenir" for x in result["results"])
    print(json.dumps(dict(archived_days=days,variants=4,scenarios_per_variant=3,run_hash=digest(a),
                          real_case="SAP 2026-11-18",idempotent=True,
                          commission_stress_fills=len(v["cout_double"]["new_trades"]),
                          delayed_fills=len(a[1]["books"]["ag3"]["retard"]["new_trades"]),
                          delayed_control_fills=len(deferred["books"]["ag3"]["retard"]["new_trades"]),
                          handover_return=True,retirement_return=True,
                          trial_expenses=0),ensure_ascii=False))


if __name__=="__main__":
    check()
