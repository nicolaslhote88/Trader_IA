"""Keep dated reference captures and reproduce the frozen November audit from its signed-by-hash members."""
from datetime import datetime,timezone
from decimal import Decimal
import json
from pathlib import Path
import subprocess
import zipfile
from statements import day,digest,dump,key_account,num


def audit(path):
    with zipfile.ZipFile(path) as z:
        manifest={name:sha for sha,name in (line.split(maxsplit=1) for line in z.read("SHA256.txt").decode().splitlines())}
        values={}
        for k in ("facts","broker","pa"):
            name=k+".json"
            raw=z.read(name)
            if digest(raw)!=manifest[name]:
                raise ValueError("Pièce d’audit altérée")
            values[k]=json.loads(raw)
    f,b,p=values["facts"],values["broker"],values["pa"]["1Y"]
    a=b["health"]["alignment"]
    if not a["aligned"] or a["gateway_is_paper"] or a["selected_account"]!=a["configured_account_id"]:
        raise ValueError("Identité de l’audit incohérente")
    dep=sum((num(x["amount"]) for x in f["flows"] if x["currency"]=="EUR" and x["type"]=="DEPOSIT"),Decimal(0))
    fifo=sum((num(l["realized_pnl_eur"]) or Decimal(0) for l in f["lots"]),Decimal(0))
    led=b["account/ledger"]["BASE"]
    instant,latent=num(led["netliquidationvalue"]),num(led["unrealizedpnl"])
    twr=dict(zip(p["cps"]["dates"],p["cps"]["data"][0]["returns"],strict=True))
    nav=[{"date":day(dt),"value":v,"twr":twr.get(dt)} for dt,v in zip(p["nav"]["dates"],p["nav"]["data"][0]["navs"],strict=True)]
    return {"account":key_account(a["selected_account"]),"captured":b["utc"],"sources":{k:manifest[k+".json"] for k in values},
            "capital":str(dep),"fifo":str(fifo),"latent":str(latent),"instant":str(instant),"residual":str(instant-dep-fifo-latent),
            "nav":nav,"positions":[{"date":b["utc"][:10],"at":b["utc"],"basis":"intraday","rows":[
                {"conid":str(row["conid"]),"qty":row["position"],"symbol":row.get("ticker") or row.get("contractDesc")} for row in b["positions"]]}]}


def save(dest,name,value):
    data=dump(value).encode()
    dest.mkdir(exist_ok=True,parents=True)
    p=dest/(name+"-"+digest(data)+".json")
    if not p.exists():
        import tempfile,os
        with tempfile.NamedTemporaryFile(dir=dest,delete=False) as f:
            f.write(data)
            temp=f.name
        os.replace(temp,p)
    return p


def capture(config,dest,account):
    result=subprocess.run(["ssh","-T","-F",str(config),"-o","BatchMode=yes","-o","ConnectTimeout=12",
        "vps","/opt/trader-ia/finnhub/venv/bin/python -"],input=Path(__file__).with_name("vps_reference.py").read_bytes(),
        capture_output=True,timeout=100)
    if result.returncode:
        # Do not echo broker response bodies or environment.
        raise RuntimeError("Lecture VPS indisponible (code "+str(result.returncode)+")")
    v=json.loads(result.stdout)
    if v.get("account")!=account:
        raise ValueError("Compte VPS différent de la référence")
    return save(dest,"observation",v)


def refs(directory):
    audits=[]
    obs=[]
    for p in directory.glob("*.json"):
        raw=p.read_bytes()
        if p.stem.rsplit("-",1)[-1]!=digest(raw):
            raise ValueError("Empreinte de référence invalide")
        v=json.loads(raw)
        (audits if p.name.startswith("audit-") else obs).append(v)
    if len(audits)!=1 or not obs:
        raise ValueError("Référence d’audit et observation du compte requises")
    obs.sort(key=lambda v:v["captured"])
    if any(v["account"]!=audits[0]["account"] for v in obs):
        raise ValueError("Compte de comparaison incohérent")
    latest=dict(obs[-1])
    # Daily observations can be revised by PA. Keep source files and prefer the most recent publication.
    nav={}
    positions=[]
    for v in [audits[0]]+obs:
        positions.extend(v["positions"])
        for n in v["nav"]:
            nav[day(n["date"])]=dict(n,date=day(n["date"]),provenance=v["captured"])
    latest["nav"]=nav
    latest["positions"]=positions
    return audits[0],latest
