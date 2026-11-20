"""Passive AG1 archive. All writes go to the separate private archive or private scratch."""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import shutil

from evidence import AG1, WORKFLOWS, collect_n8n, encode, fingerprint
from ledger import collect_ledger, reader


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def atomic(path, data):
    tmp = path.with_suffix(path.suffix+".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


class Journal:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        pagesize=self.db.execute("pragma page_size").fetchone()[0]
        self.db.execute("pragma max_page_count="+str(1024**3//pagesize))
        self.db.executescript("""
            create table if not exists objects(hash text primary key, bytes integer not null, data blob not null);
            create table if not exists refs(kind text, name text, hash text not null, primary key(kind,name));
            create table if not exists revisions(kind text, name text, hash text, seen text not null,
                primary key(kind,name,hash));
            create table if not exists meta(key text primary key,value text not null);
        """)
        self.changed = 0

    def has(self, kind, name):
        return self.db.execute("select 1 from refs where kind=? and name=?",(kind,name)).fetchone() is not None

    def put(self, kind, name, value):
        raw = encode(value)
        h = hashlib.sha256(raw).hexdigest()
        prev = self.db.execute("select hash from refs where kind=? and name=?",(kind,name)).fetchone()
        if prev==(h,):
            return False
        self.db.execute("insert or ignore into objects values(?,?,?)",(h,len(raw),gzip.compress(raw,compresslevel=5,mtime=0)))
        self.db.execute("insert into refs values(?,?,?) on conflict(kind,name) do update set hash=excluded.hash",(kind,name,h))
        self.db.execute("insert or ignore into revisions values(?,?,?,?)",(kind,name,h,now()))
        self.changed += 1
        return True

    def records(self, kind):
        for name, compressed in self.db.execute("""select name,data from refs join objects using(hash)
                where kind=? order by name""",(kind,)):
            yield name,json.loads(gzip.decompress(compressed))

    def get(self, kind, name):
        row = self.db.execute("select data from refs join objects using(hash) where kind=? and name=?",(kind,name)).fetchone()
        return json.loads(gzip.decompress(row[0])) if row else None

    def meta(self,key):
        row=self.db.execute("select value from meta where key=?",(key,)).fetchone()
        return row[0] if row else None

    def mark(self,key,value):
        self.db.execute("insert into meta values(?,?) on conflict(key) do update set value=excluded.value",(key,value))

    def counts(self):
        return {table:self.db.execute("select count(*) from "+table).fetchone()[0] for table in ("objects","refs","revisions")}


def stamp(path):
    s=path.stat()
    return str((s.st_dev,s.st_ino,s.st_ctime_ns,s.st_mtime_ns,s.st_size))


def checks(j, eid, t, d):
    calls=t["calls"]
    outputs=t["outputs"]
    ids={p["model_key"] for p in d["models"] if p["model_id"]}
    called={p.get("ledger_model_key") for p in calls if p.get("model")}
    stages=t["stages"]
    dispatch=stages.get("dispatch") or []
    orders=d["orders"]
    sent=[o for item in dispatch for o in item["orders"]]
    send_ids={str(o.get("brokerOrderId")) for o in sent if o.get("brokerOrderId")}
    actual={str(o["broker_order_id"]) for o in orders if o.get("broker_order_id")}
    with_fills=[(o,f) for o in orders for f in o["fills"]]
    return {
        "execution_reussie":t["status"]=="success",
        "run_et_execution":t.get("context",{}).get("runId")==d["run"]["run_id"] and str(d["run"].get("n8n_execution_id"))==eid and str(t["context"].get("executionId"))==eid,
        "version_publiee":bool(t.get("published_parameters_verified") and t.get("published_graph_sha256") and t["graph"]["version"]),
        "trois_modeles":len(ids)==3 and ids==called,
        "messages_effectifs":bool(calls) and all(p.get("input",{}).get("messages") and p["input_sha256"] for p in calls),
        "reponses_observees":bool(calls) and all(any(x.get("text") or x.get("output") for x in (p.get("response") or [])) for p in calls),
        "propositions":all(p.get("parse_ok") and p.get("decision_json") is not None for p in d["models"]) and len(d["models"])>=3,
        "faisabilite":"input_pack" in stages and any("execution_audit" in x and "opportunity_pack" in x and "portfolio_pack" in x for x in stages["input_pack"]),
        "securite_et_envoi_observes":bool(stages.get("safety") and dispatch and all("orders" in x and "ordersSent" in x.get("broker",{}) for x in dispatch)),
        "issue_des_ordres":all(x.get("link_method") not in ("ambigu","order_id_introuvable") for x in d["decisions"]) and
            all(any(o.get("order_id") in x["linked_order_ids"] for x in d["decisions"]) for o in orders) and
            all(x.get("linked_order_ids") or not orders and bool(stages.get("safety")) and not sent for x in d["decisions"] if x["status"]=="CONSENSUS_APPROVED"),
        "identifiants_ordres":send_ids==actual and all(any((s.get("orderId")==o["order_id"] or s.get("clientOrderId")==o["client_id"] and o["client_id"] or
            str(s.get("brokerOrderId"))==str(o["broker_order_id"]) and o["broker_order_id"]) for s in sent) for o in orders) and (bool(orders) or not sent and all(x.get("broker",{}).get("ordersSent")==0 for x in dispatch)),
        "executions_identifiees":all(o["fills"] or str(o["status"]).upper() not in ("FILLED","PARTIALLY_FILLED","PARTIAL") for o in orders) and all(
            f.get("execution_id") and str(f["broker_order_id"])==str(o["broker_order_id"]) and o["broker_order_id"] for o,f in with_fills),
    }


def receive(j,args):
    since=j.meta("deployed_utc")
    events=[(e["started_utc"], int(k),e) for k,e in j.records("event") if e["mode"]=="trigger" and
            datetime.fromisoformat(e["started_utc"]).replace(tzinfo=timezone.utc)>datetime.fromisoformat(since)]
    if not events:
        return {"etat":"en_attente_du_premier_run_naturel","depuis":since}
    _, n, ev=min(events)
    eid=str(n)
    trace=j.get("trace",eid)
    rid=(trace or {}).get("context",{}).get("runId")
    d=j.get("ledger",rid) if rid else None
    if not d:
        return {"etat":"verification_incomplete","execution":eid,"statut_n8n":ev["status"],"cause":"trace ou registre absent"}
    gates=checks(j,eid,trace,d)
    proof="replay/"+eid
    if not j.meta(proof):
        before=j.counts()
        trace_hash=fingerprint(trace)
        ledger_hash=fingerprint(d)
        with j.db:
            reread=collect_n8n(args.n8n,lambda k,n:n!=eid,j.put)
        with reader(args.duck,args.scratch or args.state) as source, j.db:
            collect_ledger(source,j.put)
        if eid in reread and fingerprint(j.get("trace",eid))==trace_hash and fingerprint(j.get("ledger",rid))==ledger_hash and j.counts()==before:
            j.mark(proof,now())
    gates["relecture_sans_doublon"]=bool(j.meta(proof))
    gaps=[{"branche":c.get("ledger_model_key"),"demande":c["model"],"registre":c.get("declared_model")}
          for c in trace["calls"] if c["model"]!=c.get("declared_model")]
    return {"etat":("valide_avec_ecart_de_version_modele" if gaps else "valide") if all(gates.values()) else "a_verifier","execution":eid,"run":rid,"ecarts_de_version":gaps,
            "controles":gates,"depuis":since}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--state",type=Path,required=True)
    parser.add_argument("--n8n",type=Path,required=True)
    parser.add_argument("--duck",type=Path,required=True)
    parser.add_argument("--baseline",type=Path)
    parser.add_argument("--scratch",type=Path)
    parser.add_argument("--release",required=True)
    parser.add_argument("--replay",action="store_true")
    args=parser.parse_args()
    os.umask(0o077)
    args.state.mkdir(mode=0o700,parents=True,exist_ok=True)
    jp=args.state/"journal.sqlite"
    j=Journal(jp)
    try:
        # Preserve the n8n capture even when a later DuckDB snapshot cannot be taken.
        with j.db:
            if not j.meta("deployed_utc"):
                j.mark("deployed_utc",now())
            if args.baseline:
                j.put("audit","reference",json.loads(args.baseline.read_bytes()))
            collect_n8n(args.n8n, (lambda k,n:False) if args.replay else j.has, j.put)
        version=stamp(args.duck)
        if args.replay or j.meta("duck_stamp")!=version or j.meta("release")!=args.release:
            with reader(args.duck,args.scratch or args.state) as d, j.db:
                collect_ledger(d,j.put)
                j.mark("duck_stamp",version)
                j.mark("release",args.release)
        reception=receive(j,args)
        j.mark("reception",encode(reception).decode())
        j.db.commit()
        from sheets import render
        if j.changed or not (args.state/"fiches"/"index.html").exists() or args.replay or j.meta("render_state")!=fingerprint([reception,j.counts(),args.release]):
            render(j,args.state/"fiches",reception)
            with j.db:
                j.mark("render_state",fingerprint([reception,j.counts(),args.release]))
        sheets_bytes=sum(p.stat().st_size for p in (args.state/"fiches").glob("*.html"))
        status={"collected_utc":now(),"release":args.release,"changed":j.changed,"volume_bytes":jp.stat().st_size,
                "traces_a_verifier":[eid for eid,_ in j.records("capture_issue") if not j.has("trace",eid)],
                "sheets_bytes":sheets_bytes,"storage_review":jp.stat().st_size>=768*1024**2 or sheets_bytes>=1536*1024**2 or shutil.disk_usage(args.state).free<2*1024**3,
                **j.counts(),"reception":reception}
        atomic(args.state/"status.json",encode(status))
        print(json.dumps(status,ensure_ascii=True))
    finally:
        j.db.close()


if __name__=="__main__":
    main()
