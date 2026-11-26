"""Read-only operational evidence for the prospective trial and its dashboard."""
import json
import hashlib
import os
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen

sys.path.insert(0, "/opt/trader-ia/research-sim/releases/9a528f28")
from common import atomic, digest, load

DATA = Path("/local-files/duckdb")
FEED = Path("/var/lib/trader-research-feed/pilotage")
AG1 = "AG1V4CONSENSUS"
# Logical readers found in the deployed decision and reporting chains. External readers require operator review.
READERS = {
    "AG2": ["matrice AG1", "veto technique du simulateur", "univers du dashboard"],
    "AG3": ["matrice AG1", "classement fondamental simulé", "dashboard"],
    "AG4": ["matrice et résumé de nouvelles AG1", "expériences sur les nouvelles", "dashboard"],
    "AG5": ["contexte consultatif AG1", "dashboard"],
    "AG6": ["contexte consultatif AG1", "dashboard"],
    "AG7": ["contexte consultatif AG1", "dashboard"],
    "AG8": ["contexte consultatif AG1", "dashboard"],
}


def broker(account):
    def get(route):
        with urlopen("http://127.0.0.1:18080/" + route, timeout=8) as reply:
            return json.load(reply)

    def trusted(h):
        a=h.get("account_alignment") or {}
        selected=a.get("selected_account") or ""
        return bool(h.get("authenticated") is True and h.get("dry_run") is False and a.get("aligned") is True
                    and a.get("gateway_is_paper") is False and selected and selected==a.get("configured_account_id")
                    and hashlib.sha256(selected.upper().encode()).hexdigest()==account["account"])
    result = {"aligned": False, "paper": None, "authenticated": False, "pending": None,"positions":None}
    try:
        h = get("health")
        a = h["account_alignment"]
        result.update(authenticated=h["authenticated"], paper=a["gateway_is_paper"],
                      aligned=trusted(h))
        result["pending"] = len(get("orders/approvals/pending")["pending"])
        if result["aligned"]:
            symbols={str(x["conid"]):x["symbol"] for x in account["positions"]}
            pos=get("positions")
            balances=get("account/ledger")
            if not trusted(get("health")):
                result["aligned"]=False
                return result
            base=balances["BASE"]
            eur=base.get("currency")=="EUR" or (base.get("currency")=="BASE" and float(balances.get("EUR",{}).get("exchangerate",0))==1)
            if eur and isinstance(pos,list) and base.get("cashbalance") is not None:
                result["positions"]={symbols.get(str(x["conid"]),"conid:"+str(x["conid"])):float(x["position"])
                                     for x in pos if float(x["position"])}
                result["cash_eur"]=float(base["cashbalance"])
    except (OSError, ValueError, KeyError, TypeError) as exc:
        result["read_error"] = type(exc).__name__
    return result


def workflows(db, since):
    rows = db.execute("""select w.id,w.name,w.active,w.activeVersionId,h.nodes,h.connections
        from workflow_entity w left join workflow_history h on w.activeVersionId=h.versionId
        where w.active=1""").fetchall()
    result = []
    edges = {}
    for wid, name, _, version, nodes, graph in rows:
        if not nodes:
            continue
        body = json.loads(nodes)
        for node in body:
            if node.get("type", "").endswith(".executeWorkflow"):
                target = node.get("parameters", {}).get("workflowId")
                if isinstance(target, dict):
                    target = target.get("value")
                if isinstance(target, str):
                    edges.setdefault(target, set()).add(name)
        group = next((g for g in READERS if name.startswith(g)), None)
        if group or wid == AG1:
            result.append(dict(id=wid, name=name, version=version, group=group or "AG1",
                               graph=digest([body, json.loads(graph)]), readers=READERS.get(group, [])))
    for item in result:
        item["direct_readers"] = sorted(edges.get(item["id"], ()))
    runs = db.execute("""select e.id,e.startedAt,e.status,h.nodes,h.connections from execution_entity e
          left join execution_data d on d.executionId=e.id
          left join workflow_history h on h.versionId=d.workflowVersionId and h.workflowId=e.workflowId
          where e.workflowId=? and e.startedAt>=? order by e.startedAt""", (AG1, since)).fetchall()
    decisions = []
    for eid, at, status, nodes, graph in runs:
        decisions.append(dict(id=eid, day=at[:10], status=status,
                              graph=digest([json.loads(nodes), json.loads(graph)]) if nodes and graph else None))
    return sorted(result, key=lambda x: x["id"]), decisions


def collect():
    now = datetime.now(timezone.utc)
    since = (now.date() - timedelta(days=10)).isoformat()
    dbpath = "/var/lib/docker/volumes/n8n_data/_data/database.sqlite"
    with sqlite3.connect("file:" + dbpath + "?mode=ro", uri=True, timeout=8) as db:
        inventory, runs = workflows(db, since)
    eco = load(DATA / "economic_reporting/report.json")
    u = eco["usage"]
    account = load(DATA / "lot03_reporting/compte.json")
    calls = Counter((x["workflow"], x["at"][:10]) for x in u["calls"] if x.get("at") and x["at"][:10] >= since)
    calls_by_name = [{"workflow": k[0], "day": k[1], "calls": n} for k, n in sorted(calls.items())]
    trials = load(DATA / "research_reporting/experiments/report.json")
    bench = market_benchmarks(eco["performance"]["series"])
    changes = []
    for entry in sorted(Path("/var/lib/trader-pilotage-operations").glob("*.json"),key=lambda p:p.stat().st_mtime_ns):
        if entry.name=="operation.json":
            continue
        record=load(entry)
        changes.append({k:record.get(k) for k in ("phase","plan","result","component")})
    pilot_record=None
    register=Path("/var/lib/trader-pilotage-operations/pilot.sqlite")
    if register.is_file():
        with sqlite3.connect(register.as_uri()+"?mode=ro",uri=True) as pilot:
            state=pilot.execute("select state from pilot where id=1").fetchone()
            if state:
                pilot_record=dict(state=state[0],sheets=pilot.execute("select count(*) from sheets").fetchone()[0],
                                  reconciliations=pilot.execute("select count(*) from reviews").fetchone()[0])
    observation = dict(at=now.isoformat(), broker=broker(account), inventory=inventory, runs=runs,
                       account_day=account["end"], reconciled=account["activity"].get("ag1_pf_v1_ibkr_reconcile"),
                       positions={r["symbol"]: r["qty"] for r in account["positions"]},
                       fees=u["daily_project_eur"], coverage=u["provider_ranges"], costs_date=u["published"],
                       bench=bench, trials_at=trials["at"], blocked=trials.get("unavailable", []),
                       calls=calls_by_name,changes=changes,pilot_record=pilot_record)
    FEED.mkdir(parents=True, exist_ok=True)
    path = FEED / (now.strftime("%Y%m%dT%H%M%S") + "-" + digest(observation)[:16] + ".json.gz")
    atomic(path, observation)
    print("Observation", now.isoformat(), len(inventory), "workflows publiés", flush=True)


def market_benchmarks(official):
    """Extend independently priced witnesses beyond the latest broker statement."""
    sys.path.insert(0, "/opt/trader-ia/economic-monitor/current")
    from performance import START, FREEZE, prices_eur
    state = Path("/var/lib/trader-economic")
    audit, market = load(state/"audit.json"), load(state/"market.json")["market"]
    world = dict(audit["market"]["EUNL.DE"])
    if market.get("EUNL.DE", [None])[0] == "EUR":
        world.update(market["EUNL.DE"][1])
    fx = dict(audit["market"]["EURUSD=X"])
    if market.get("EURUSD=X", [None])[0] == "USD":
        fx.update(market["EURUSD=X"][1])
    byday = {r["date"]:dict(r) for r in official}
    stocks = {}
    for s,d,v,ccy in audit["prices"]:
        stocks.setdefault(s,(ccy,{}))[1][d]=v
    for s,(ccy,history) in stocks.items():
        if s in market and ccy==market[s][0]:
            history.update(market[s][1])
    for d in sorted(world):
        if d in byday or d < FREEZE:
            continue
        pnl=0.
        for s,value in audit["holdings"].items():
            ccy,history=stocks[s]
            a,b=prices_eur(history,ccy,fx,FREEZE),prices_eur(history,ccy,fx,d)
            if not a or not b:
                pnl=None
                break
            pnl+=float(value)*(b/a-1)
        byday[d]=dict(date=d,world=world[d]/world[START]-1,basket=pnl/float(audit["cash"][FREEZE][1]) if pnl is not None else None,
                      matched=None,account=None,nav=None)
    return [byday[d] for d in sorted(byday)]


if __name__ == "__main__":
    collect()
