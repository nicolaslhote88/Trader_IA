"""Reproduce links on a frozen audit and export only the decision-journal evidence."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import zipfile
from evidence import encode


def calculate(path):
    with zipfile.ZipFile(path) as z:
        manifest = {name:digest for digest,name in (line.split(maxsplit=1) for line in z.read("SHA256.txt").decode().splitlines())}
        def verified(name):
            raw = z.read(name)
            if hashlib.sha256(raw).hexdigest()!=manifest[name]:
                raise ValueError("Empreinte invalide pour "+name)
            return json.loads(raw)
        facts = verified("facts.json")
        packs = verified("packs.json")
    def key(x):
        return tuple(x[k] for k in ("run_id","symbol","side"))
    approved = [d for d in facts["decisions"] if d["status"]=="CONSENSUS_APPROVED"]
    orders = Counter(map(key,facts["orders"]))
    fills = Counter(map(key,facts["fills"]))
    cases = {}
    for eid, stages in packs.items():
        stage = next((v for n,v in stages.items() if "Attach Dated" in n),{})
        cases[eid] = {"selection":stage.get("selection"), "opportunity_rows":stage.get("rows"),
                      "source":"packs.json / Attach Dated Accounts"}
    return {"members_sha256":{k:manifest[k] for k in ("facts.json","packs.json")},
            "approved":len(approved),"with_order":sum(key(d) in orders for d in approved),
            "with_fill":sum(key(d) in fills for d in approved),
            "multi_order_candidates":sum(orders[key(d)]>1 for d in approved),
            "sessions_with_proposals":len({p["run_id"] for p in facts["models"]}),
            "available_pack_summaries":cases, "method":"run_id + symbol + side"}


if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("zip",type=Path)
    parser.add_argument("out",type=Path)
    args=parser.parse_args()
    result=calculate(args.zip)
    print({k:v for k,v in result.items() if isinstance(v,int)})
    args.out.write_bytes(encode(result))
