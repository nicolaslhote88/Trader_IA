"""Local operator entry point. Never modifies n8n or the broker. Python 3.11+."""
import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sys
from subprocess import TimeoutExpired
from datetime import datetime,timezone,timedelta
from reference import audit,capture,refs,save
from reconcile import report
from publish import publish
from statements import activate,connect,ingest


@contextmanager
def exclusive(state):
    with (state/".import.lock").open("a+b") as handle:
        if not handle.tell():
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        if os.name=="nt":
            import msvcrt
            msvcrt.locking(handle.fileno(),msvcrt.LK_NBLCK,1)
        else:
            import fcntl
            fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
        yield


def run(args):
    state=args.state.resolve()
    state.mkdir(parents=True,exist_ok=True)
    incoming=state/"entrant"
    incoming.mkdir(exist_ok=True)
    directory=state/"references"
    with exclusive(state):
        if args.audit:
            save(directory,"audit",audit(args.audit))
        anchors=list(directory.glob("audit-*.json"))
        if len(anchors)!=1:
            raise ValueError("Archive de référence non initialisée")
        account=json.loads(anchors[0].read_bytes())["account"]
        db=connect(state/"rapprochement.sqlite")
        notes=[]
        try:
            inputs=list(sorted(p for p in incoming.iterdir() if p.is_file() and not p.name.startswith((".","~"))))
            if args.file:
                inputs.append(args.file)
            others=[p for p in inputs if p.suffix.lower() not in (".xml",".part",".tmp",".crdownload")]
            notes.extend((p.name,"Format à convertir en export Flex Activité XML.") for p in others)
            for p in inputs:
                if p.suffix.lower()!=".xml":
                    continue
                try:
                    ingest(db,p.read_bytes(),account)
                except ValueError as exc:
                    notes.append((p.name,str(exc)))
            if args.withdraw or args.restore:
                activate(db,args.withdraw or args.restore,bool(args.restore))
            obs=list(directory.glob("observation-*.json"))
            last_capture=max((json.loads(p.read_bytes())["captured"] for p in obs),default=None)
            if args.ssh and (args.refresh or not last_capture or datetime.now(timezone.utc)-datetime.fromisoformat(last_capture)>=timedelta(days=1)):
                try:
                    capture(args.ssh,directory,account)
                except (RuntimeError,OSError,ValueError,TimeoutExpired):
                    if not obs:raise
                    notes.append(("Observation VPS","Échec du rafraîchissement. Dernière observation conservée."))
            frozen,ref=refs(directory)
            outcome=report(db,frozen,ref)
            imports=[(r["id"],"actif" if r["active"] else "retiré") for r in db.execute("select id,active from sources order by id")]
            rev_file=Path(__file__).with_name("REVISION")
            status=publish(state,frozen,ref,outcome,notes,imports,rev_file.read_text().strip() if rev_file.exists() else "développement")
            print(json.dumps({k:status[k] for k in ("imports_actifs","periodes","mouvements_uniques","residu_restant_eur","nav","date_derniere_nav_anterieure_au_jour")},ensure_ascii=True))
            return 2 if notes else 0
        finally:
            db.close()


if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--state",type=Path,required=True)
    p.add_argument("--audit",type=Path)
    p.add_argument("--ssh",type=Path)
    p.add_argument("--refresh",action="store_true")
    p.add_argument("--file",type=Path)
    m=p.add_mutually_exclusive_group()
    m.add_argument("--withdraw")
    m.add_argument("--restore")
    sys.exit(run(p.parse_args()))
