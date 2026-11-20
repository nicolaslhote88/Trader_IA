"""Refresh private evidence on the user's reporting host; raw billing and XML stay on Windows."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
from seed import extract
from provider_exports import import_exports

def sync(project,ssh_config):
    state=project/"snapshots/pilotage_prive/lots04-06"
    folder=project/"snapshots/audits_prives/2026-11-18/Coûts API"
    ssh=["ssh","-F",str(ssh_config),"-o","BatchMode=yes","-o","ConnectTimeout=10","vps"]
    with TemporaryDirectory() as tmp:
        p=Path(tmp);extract(project,p)
        (p/"providers.json").write_text(json.dumps(import_exports(folder),ensure_ascii=False,sort_keys=True),encoding="utf8")
        scope=json.loads((state/"scope.json").read_bytes())
        (p/"scope.json").write_text(json.dumps(scope,sort_keys=True),encoding="utf8")
        data={n:json.loads((p/(n+".json")).read_bytes()) for n in ("flex","providers","scope")}
        raw=json.dumps(data,ensure_ascii=False,sort_keys=True,separators=(",",":")).encode()
        hexdigest=hashlib.sha256(raw).hexdigest()
        dest="/var/lib/trader-economic/evidence.json"
        check=subprocess.run(ssh+["test ! -f "+dest+" || sha256sum "+dest],check=True,capture_output=True,timeout=20)
        if check.stdout.decode().startswith(hexdigest):return
        cmd=("set -eu; umask 077; exec 9>/var/lib/trader-economic/sync.lock; flock -x 9; "
             "p=$(mktemp /var/lib/trader-economic/incoming.XXXXXX); trap 'rm -f -- \"$p\"' EXIT; "
             "cat >\"$p\"; echo '"+hexdigest+"  '\"$p\" | sha256sum -c - >/dev/null; "
             "mv -f -- \"$p\" "+dest+"; systemctl start trader-economic@report.service")
        subprocess.run(ssh+[cmd],input=raw,check=True,timeout=220)

if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("project",type=Path);p.add_argument("ssh",type=Path);args=p.parse_args()
    sync(args.project.resolve(),args.ssh)
