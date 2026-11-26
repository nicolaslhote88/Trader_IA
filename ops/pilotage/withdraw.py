"""One operator-approved research producer at a time. No timer invokes this command."""
import argparse
import fcntl
import http.client
import json
import os
import sqlite3
import sys
from pathlib import Path
from urllib.parse import quote

sys.path.insert(0, "/opt/trader-ia/research-sim/releases/9a528f28")
from common import atomic, digest, load
from transition import retirement, retirement_plan, verify_retirement

STATE=Path("/var/lib/trader-pilotage-operations")
N8N=Path("/var/lib/docker/volumes/n8n_data/_data/database.sqlite")
# Scheduled analysis-only producers. Order placement, approvals, quarantine and account reconciliation are excluded.
ALLOWED={"AG2V3HELDCORE20260619","AG2V3WATCHNIGHT20260619","AG3V2HELDCORE20260622",
         "AG3V2WATCHNIGHT20260622","rP2cyaIKt8tiVjYX5AH1e","AG4SPEFINNHUBV1","hSqxVSb8YAO9Nc6A",
         "H0cfY1coMx8dvMuXScMc_","AG5FXMacroPillarsV1","AG6FXValuationPillarsV2",
         "AG7FXPositioningPillarsV2","AG8FXRatesPillarsV2"}


def local_version(wid, version):
    with sqlite3.connect(N8N.as_uri()+"?mode=ro",uri=True,timeout=5) as db:
        key=db.execute("select active,activeVersionId from workflow_entity where id=?",(wid,)).fetchone()
        row=db.execute("select nodes,connections from workflow_history where workflowId=? and versionId=?",(wid,version)).fetchone()
        running=db.execute("select count(*) from execution_entity where workflowId=? and status in ('new','running','waiting')",(wid,)).fetchone()[0]
    if not key or not row:
        raise ValueError("Version publiée introuvable")
    graph=[json.loads(x) for x in row]
    return bool(key[0]),key[1],graph,running


def post(wid, operation, payload):
    token=os.environ.get("N8N_API_KEY")
    if not token:
        raise ValueError("Identifiant API du contexte opérateur indisponible")
    connection=http.client.HTTPConnection("127.0.0.1",5678,timeout=15)
    try:
        connection.request("POST","/api/v1/workflows/"+quote(wid,safe="")+"/"+operation,
                           json.dumps(payload),{"Content-Type":"application/json","X-N8N-API-KEY":token})
        reply=connection.getresponse()
        reply.read()
        if not 200<=reply.status<300:
            raise RuntimeError(f"API n8n HTTP {reply.status}")
    finally:
        connection.close()


def change(action, record, lookup, mutate, save):
    plan=record["plan"]
    wid=plan["producer"]
    if wid not in ALLOWED:
        raise ValueError("Producteur hors périmètre")
    active,v,graph,running=lookup(wid,plan["observed_version"])
    if digest(graph)!=plan["backup"] or running or (active and v!=plan["observed_version"]):
        raise ValueError("Graphe modifié ou exécution en cours")
    if action=="retirer":
        if record["phase"] not in ("préparé","retrait demandé","retiré"):
            raise ValueError("Un retour arrière clôt ce changement")
        if not active and record["phase"]=="préparé":
            raise ValueError("Le producteur a déjà été retiré par un autre opérateur")
        desired=False
    else:
        if record["phase"] not in ("retrait demandé","retiré","réceptionné","reprise demandée","rétabli"):
            raise ValueError("Aucun retrait à rétablir")
        desired=True
    if active!=desired:
        record["phase"]="reprise demandée" if desired else "retrait demandé"
        save(record)
        mutate(wid,"activate" if desired else "deactivate",{"versionId":plan["observed_version"]} if desired else {})
        a,v,graph,_=lookup(wid,plan["observed_version"])
        if a!=desired or (desired and v!=plan["observed_version"]):
            raise RuntimeError("Transition à revérifier auprès de n8n")
    record["phase"]="rétabli" if desired else "retiré"
    save(record)
    return record["phase"]


def main():
    cli=argparse.ArgumentParser()
    cli.add_argument("action",choices=["préparer","retirer","rétablir","réceptionner"])
    cli.add_argument("dossier",type=Path)
    args=cli.parse_args()
    data=load(args.dossier)
    STATE.mkdir(mode=0o700,exist_ok=True)
    with (STATE/"operator.lock").open("a+") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        index=STATE/"operation.json"
        record=load(index) if index.exists() else None
        if args.action=="préparer":
            if record and record["phase"] not in ("rétabli","réceptionné"):
                raise ValueError("Un retrait est déjà ouvert")
            recent=load(sorted(Path("/var/lib/trader-research-feed/pilotage").glob("*.json.gz"))[-1])
            decision=dict(data["decision"])
            if len(decision["ids"])!=1 or decision["ids"][0] not in ALLOWED:
                raise ValueError("Un seul producteur autorisé par changement")
            candidate=next(x for x in retirement(recent) if x["id"]==decision["ids"][0])
            active,version,graph,executing=local_version(candidate["id"],candidate["published"])
            if not active or executing or version!=candidate["published"] or digest(graph)!=candidate["graph"]:
                raise ValueError("La publication a changé")
            decision["backup_sha"]=digest(graph)
            plan=retirement_plan(retirement(recent),decision,data["review"],data["consumers"])
            atomic(STATE/(plan["backup"]+".json.gz"),graph,mode=0o600)
            record={"plan":plan,"phase":"préparé","before":data["before"],"component":candidate}
            atomic(index,record,mode=0o600)
            atomic(STATE/(plan["change_id"]+".json"),record,mode=0o600)
        else:
            if not record or record["plan"]["change_id"]!=data["change_id"]:
                raise ValueError("Identifiant du changement inconnu")
            if args.action=="réceptionner":
                if record["phase"]!="retiré":
                    raise ValueError("Retrait non réalisé")
                result=verify_retirement(record["plan"],record["before"],data["after"])
                record["result"]=result
                if isinstance(result,dict):
                    record["phase"]="réceptionné"
            else:
                if digest(load(STATE/(record["plan"]["backup"]+".json.gz")))!=record["plan"]["backup"]:
                    raise ValueError("Sauvegarde non conforme")
                change(args.action,record,local_version,post,lambda r:atomic(index,r,mode=0o600))
            atomic(index,record,mode=0o600)
            atomic(STATE/(data["change_id"]+".json"),record,mode=0o600)
        print(json.dumps({"phase":record["phase"],"change_id":record["plan"]["change_id"],
                          "result":record.get("result")},ensure_ascii=False))


if __name__=="__main__":
    main()
