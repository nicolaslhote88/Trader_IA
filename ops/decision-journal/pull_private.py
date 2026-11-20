"""Export an internally consistent archive via the configured SSH alias and render locally."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile

from journal import Journal, atomic
from sheets import render


REMOTE = """import sqlite3,sys,tempfile
with tempfile.TemporaryDirectory() as path:
    source=sqlite3.connect('file:/var/lib/trader-ag1-journal/journal.sqlite?mode=ro',uri=True)
    target=sqlite3.connect(path+'/journal.sqlite')
    source.backup(target)
    source.close()
    target.close()
    import shutil
    with open(path+'/journal.sqlite','rb') as f:
        shutil.copyfileobj(f,sys.stdout.buffer)
"""


def refresh(config, dest):
    os.umask(0o077)
    dest.mkdir(exist_ok=True,parents=True)
    with tempfile.TemporaryDirectory(dir=dest) as tmp:
        copy=Path(tmp)/"journal.sqlite"
        with copy.open("wb") as f:
            subprocess.run(["ssh","-T","-o","BatchMode=yes","-o","ConnectTimeout=12","-F",str(config),"vps","python3 -"],
                           input=REMOTE.encode(),stdout=f,check=True,timeout=180)
        j=Journal(copy)
        try:
            if j.db.execute("pragma integrity_check").fetchone()!=("ok",):
                raise ValueError("Intégrité SQLite")
            for key,data in j.db.execute("select hash,data from objects"):
                if hashlib.sha256(gzip.decompress(data)).hexdigest()!=key:
                    raise ValueError("Empreinte d’objet invalide")
            reception=json.loads(j.meta("reception"))
            render(j,Path(tmp)/"fiches",reception)
            counts=j.counts()
        finally:
            j.db.close()
        for sheet in sorted((Path(tmp)/"fiches").glob("run-*.html")):
            (dest/"fiches").mkdir(exist_ok=True)
            os.replace(sheet,dest/"fiches"/sheet.name)
        os.replace(copy,dest/"journal.sqlite")
        os.replace(Path(tmp)/"fiches"/"index.html",dest/"fiches"/"index.html")
        with (dest/"journal.sqlite").open("rb") as archive:
            checksum=hashlib.file_digest(archive,"sha256").hexdigest()
        verified={"integrity":"ok","sha256":checksum,**counts,"reception":reception}
        atomic(dest/"verification.json",json.dumps(verified,ensure_ascii=False,indent=2).encode())
        print(json.dumps(verified,ensure_ascii=True))


if __name__=="__main__":
    cli=argparse.ArgumentParser()
    cli.add_argument("ssh_config",type=Path)
    cli.add_argument("destination",type=Path)
    args=cli.parse_args()
    refresh(args.ssh_config,args.destination)
