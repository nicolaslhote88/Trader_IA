"""Build the small immutable audit reference and the active Flex metadata separately."""
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import zipfile


def extract(project, dest):
    private = project / "snapshots/pilotage_prive"
    sys.path.insert(0, str(private/"lot02/application"))
    from statements import load, events
    with zipfile.ZipFile(project/"snapshots/audits_prives/2026-11-18/preuves_et_calculs.zip") as z:
        hashes = {name:h for h,name in (l.split(maxsplit=1) for l in z.read("SHA256.txt").decode().splitlines())}
        def read(key):
            name = key+".json"
            raw = z.read(name)
            assert hashlib.sha256(raw).hexdigest() == hashes[name]
            return json.loads(raw)
        facts,market,pa,research = read("facts"),read("bench"),read("pa")["1Y"],read("research")
        holdings = {r["symbol"]:r["market_value_eur"] for r in facts["exposures"] if r["d"]=="2026-10-30"}
        cash = {r["d"]:[r["cash_eur"],r["total_value_eur"]] for r in facts["daily"]}
        def iso(d): return d[:4]+"-"+d[4:6]+"-"+d[6:] if len(d)==8 else d
        reference=dict(hashes={k:hashes[k+".json"] for k in ["fees","facts","llm","research","bench","pa"]},
                       fees=read("fees"), llm=read("llm"), holdings=holdings,cash=cash,
                       nav={iso(d):v for d,v in zip(pa["cps"]["dates"],pa["cps"]["data"][0]["returns"],strict=True)},
                       market={k:{d:v[0] for d,v in market[k]["prices"].items() if v[0]} for k in ["EURUSD=X","EUNL.DE"]},
                       prices=[r for r in research["recent"] if r[0] in holdings and r[1]>="2026-10-25"],
                       runs={str(r["n8n_execution_id"]):r["run_id"] for r in facts["runs"] if r["n8n_execution_id"]},
                       model_names={r["run_id"]+"/"+r["model_key"]:r["model_name"] for r in facts["models"]})
    db=sqlite3.connect((private/"lot02/rapprochement.sqlite").resolve().as_uri()+"?mode=ro",uri=True)
    db.row_factory=sqlite3.Row
    with db:
        db.execute("begin")
        periods, records=load(db)
        chosen,issues=events(records)
    db.close()
    if any(kind=="Trade" for kind,_,_ in issues):
        raise ValueError("Révision Flex ambiguë")
    flex={"sources":sorted({r["source"] for r in periods}), "meta":{}, "taxes":{}, "flows":[], "fx":{}}
    for r in chosen:
        v=r["v"]
        if r["kind"]=="Trade" and v.get("assetCategory")=="STK":
            flex["meta"][v["ibExecID"]]={k:v.get(k) for k in ("listingExchange","exchange","issuerCountryCode","tradeDate","conid","currency")}
        if r["kind"]=="StatementOfFundsLine" and v.get("activityCode") in ("TTAX","DEP","WITH") and v.get("currency")!="BASE_SUMMARY":
            amt=float(v["amount"])*float(v["fxRateToBase"])
            if v["activityCode"]=="TTAX":
                key=iso(v["date"])+"/"+v["conid"]
                flex["taxes"][key]=flex["taxes"].get(key,0)-amt
            else:
                flex["flows"].append([iso(v["date"]),amt])
    for r in sorted(records,key=lambda r:r["gen"] or ""):
        v=r["v"]
        if r["kind"]=="ConversionRate" and v["fromCurrency"]=="USD" and v["toCurrency"]=="EUR":
            flex["fx"][iso(v["reportDate"])]=float(v["rate"])
    dest.mkdir(exist_ok=True,parents=True)
    for name,value in [("audit",reference),("flex",flex)]:
        (dest/(name+".json")).write_text(json.dumps(value,ensure_ascii=False,sort_keys=True,allow_nan=False),encoding="utf8")


if __name__=="__main__":
    extract(Path(sys.argv[1]).resolve(),Path(sys.argv[2]).resolve())
