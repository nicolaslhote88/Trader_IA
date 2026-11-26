"""Durable pilot staging, suspension and broker reconciliations. The n8n order chain owns dispatch."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import sys

sys.path.insert(0, "/opt/trader-ia/research-sim/releases/9a528f28")
from common import digest, load
from control import order_preview
from transition import pilot_gate, reconcile

STATE=Path("/var/lib/trader-pilotage-operations")
REPORT=Path("/local-files/duckdb/research_reporting/pilotage/report.json")


def prepare(db):
    db.executescript("""create table if not exists pilot(id integer primary key check(id=1), state text, details text);
                      create table if not exists sheets(day text primary key, hash text, sheet text);
                      create table if not exists reviews(id text primary key, digest text, result text);""")


def apply(db, action, document, report=None, evidence=None, now=None):
    now=now or datetime.now(timezone.utc)
    db.execute("begin immediate")
    try:
        saved=db.execute("select state,details from pilot where id=1").fetchone()
        state,details=(saved[0],json.loads(saved[1])) if saved else ("non ouvert",{})
        result={}
        if action=="ouvrir":
            if state!="non ouvert":
                raise ValueError("Une passation existe déjà")
            mandate=document["mandate"]
            gate=pilot_gate(report["formal"],evidence,now,mandate,document["owners"],document["inflight"],document["risk"])
            if gate["blockers"]:
                raise ValueError(" · ".join(gate["blockers"]))
            details={"mandate":mandate,"owners":document["owners"],"opened":now.isoformat()}
            state="préparé"
        elif action=="fiche":
            if state!="préparé":
                raise ValueError("Pilote fermé ou suspendu")
            gate=pilot_gate(report["formal"],evidence,now,details["mandate"],details["owners"],document["inflight"],document["risk"])
            day=now.date().isoformat()
            if gate["blockers"] or report["formal"]["last_day"]!=day:
                raise ValueError("Passation ou revue du jour indisponible")
            m=details["mandate"]
            orders=order_preview(report["formal"]["tickets"][m["variant"]],m,details["owners"],document["account"])
            result={"day":day,"orders":orders,"source":report["formal"]["id"],"version":m["version"]}
            old=db.execute("select hash from sheets where day=?",(day,)).fetchone()
            if old and old[0]!=digest(result):
                raise ValueError("Une fiche différente existe déjà pour cette séance")
            db.execute("insert or ignore into sheets values (?,?,?)",(day,digest(result),json.dumps(result)))
        elif action=="rapprocher":
            if state not in ("préparé","suspendu"):
                raise ValueError("Aucun pilote attribué")
            result=reconcile(document["initial"],document["orders"],document["fills"],document["observed"],
                             details["mandate"]["cash_tolerance"])
            key=document["observation"]
            old=db.execute("select digest from reviews where id=?",(key,)).fetchone()
            if old and old[0]!=digest(document):
                raise ValueError("Observation modifiée sous le même identifiant")
            db.execute("insert or ignore into reviews values (?,?,?)",(key,digest(document),json.dumps(result)))
            if result["errors"]:
                state="suspendu"
        elif action=="suspendre":
            if state not in ("préparé","suspendu") or not document.get("reason"):
                raise ValueError("Une passation et un motif sont requis")
            state="suspendu"
            details["reason"]=document["reason"]
        elif action=="restituer":
            if state!="suspendu" or document.get("inflight")!=[] or not document.get("confirmed_by"):
                raise ValueError("Suspendre, solder les ordres puis confirmer le transfert")
            if not evidence["broker"]["aligned"] or evidence["broker"]["pending"]!=0:
                raise ValueError("Compte ou approbations à rapprocher")
            current=moment_age(now,evidence["reconciled"])
            if not 0<=current<=3600 or not 0<=moment_age(now,evidence["at"])<=300:
                raise ValueError("État courtier trop ancien")
            observed=document["observed"]
            from decimal import Decimal
            holdings=evidence["broker"].get("positions")
            cash=evidence["broker"].get("cash_eur")
            if holdings is None or cash is None or {s:Decimal(str(n)) for s,n in observed["positions"].items()}!={s:Decimal(str(n)) for s,n in holdings.items()} or abs(Decimal(str(cash))-Decimal(str(observed["cash"])))>Decimal(str(details["mandate"]["cash_tolerance"])):
                raise ValueError("Inventaire de restitution différent du courtier")
            owner=document["owners"]
            if set(observed["positions"])!=set(owner["positions"]) or any(v!="AG1" for v in owner["positions"].values()) or owner["cash"]!="AG1":
                raise ValueError("Chaque position et la trésorerie doivent être transférées")
            result={"broker_snapshot":observed,"owners":owner,"confirmed_by":document["confirmed_by"]}
            details["return"]=result
            state="restitué"
        else:
            raise ValueError("Opération inconnue")
        db.execute("insert or replace into pilot values (1,?,?)",(state,json.dumps(details)))
        db.commit()
        return dict(state=state,result=result)
    except Exception:
        db.rollback()
        raise


def moment_age(now,value):
    from common import moment
    return (now-moment(value)).total_seconds() if value else -1


def main():
    p=argparse.ArgumentParser();p.add_argument("action",choices=["ouvrir","fiche","rapprocher","suspendre","restituer"])
    p.add_argument("document",type=Path);a=p.parse_args()
    STATE.mkdir(mode=0o700,exist_ok=True)
    os.umask(0o077)
    with sqlite3.connect(STATE/"pilot.sqlite") as db:
        prepare(db)
        obs=load(sorted(Path("/var/lib/trader-research-feed/pilotage").glob("*.json.gz"))[-1])
        print(json.dumps(apply(db,a.action,load(a.document),load(REPORT),obs),ensure_ascii=False))


if __name__=="__main__":
    main()
