"""Publish the latest validated lot 2 evidence to the private VPS reporting service."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

from prepare import prepare


def publish(root, ssh_config):
    evidence = prepare((root/"snapshots/pilotage_prive/lot02").resolve(),
                       root/"snapshots/audits_prives/2026-11-18/preuves_et_calculs.zip")
    raw = json.dumps(evidence, ensure_ascii=False, sort_keys=True, allow_nan=False).encode()
    sha = hashlib.sha256(raw).hexdigest()
    ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-F", str(ssh_config), "vps"]
    dest = "/var/lib/trader-lot03/input.json"
    current = subprocess.check_output(ssh + ["sha256sum " + dest], timeout=25).decode().split()[0]
    if current == sha:
        return
    remote = ("set -eu; umask 077; exec 9>/var/lib/trader-lot03/sync.lock; flock -x 9; "
              "tmp=$(mktemp /var/lib/trader-lot03/input.XXXXXX); trap 'rm -f -- \"$tmp\"' EXIT; "
              "cat >\"$tmp\"; echo '" + sha + "  '\"$tmp\" | sha256sum -c - >/dev/null; "
              "mv -f -- \"$tmp\" " + dest + "; systemctl start trader-lot03-report.service")
    subprocess.run(ssh + [remote], input=raw, check=True, timeout=150)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("project", type=Path)
    p.add_argument("ssh_config", type=Path)
    args = p.parse_args()
    publish(args.project, args.ssh_config)
