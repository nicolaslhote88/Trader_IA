"""Read-only scorer and locked prospective decision journal. All times are UTC in evidence files."""
import json
import hashlib
import sqlite3
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, "/opt/trader-ia/research-sim/releases/9a528f28")
from common import atomic, digest, load, moment
from worker import policy, isolation, PARIS
from universe import PLACES
from books import inputs, run_day
from metrics import month_after, operating_cost, valuation, window, max_drawdown

FEED = Path("/var/lib/trader-research-feed")
STATE = Path("/var/lib/trader-research-sim/pilotage")
REPORTS = Path("/local-files/duckdb/research_reporting")
CODE = Path(__file__).resolve().parent


def sessions(cal):
    return sorted(cal["tables"]["XPAR"])


def method_hash():
    return digest({f:hashlib.sha256((CODE/f).read_bytes().replace(b"\r\n",b"\n")).hexdigest()
                   for f in ("books.py","metrics.py","campaign.py","collect.py")})


def score(db, contract, obs, cal, batches, latest, today, evidence_files=None):
    cfg, conv, version = policy()
    cfg = dict(cfg, capital_eur=contract["capital"], cash_floor=1-contract["exposure"], max_sector_weight=contract["sector"])
    if version != contract["strategy"]:
        raise ValueError("La version des règles diffère du mandat")
    audit = load(FEED/"audit.json")
    key = digest(contract)
    scheduled = sessions(cal)
    due = [d for d in scheduled if contract["start"] <= d < contract["end"] and
           datetime.fromisoformat(d+"T"+cfg["review_paris"]).replace(tzinfo=PARIS).timestamp()+62 <= today.timestamp()]
    frozen = {}
    for day,status,graph in db.execute("select day,status,graph from runs"):
        good = status == "success" and graph == contract["graph"]
        frozen[day] = frozen.get(day,True) and good
    marksdb = sqlite3.connect("file:/var/lib/trader-research-sim/sim.sqlite?mode=ro", uri=True)
    rates = latest.get("closing_usd",{})
    # Older valid FX marks can roll off newer batches.
    with db:
        for day,v in rates.items():
            if isinstance(v,(int,float)) and v>0:
                db.execute("insert or ignore into fx values (?,?)",(day,v))
    rates = dict(db.execute("select day, rate from fx"))
    def price(d,s,c):
        z = marksdb.execute("select price from closes where day=? and symbol=? and currency=?", (d,s,c)).fetchone()
        return z[0] if z else None
    def mark(frame):
        frame["marked"] = {v:{m:valuation(b,frame["day"],price,rates) for m,b in modes.items()}
                          for v,modes in frame["books"].items()}
        return frame
    stored = {d:mark(json.loads(raw)) for d,raw in db.execute("select day,frame from days where campaign=? order by day",(key,))}
    def known_breaks(v, series):
        spent = 0.
        navs = [contract["capital"]]
        reasons = []
        for fr in series:
            b = fr["books"][v]["normal"]
            spent += sum(x["cost"] for x in b["new_trades"])
            fees = operating_cost(contract["start"], fr["day"], obs)
            nav = fr["marked"][v]["normal"]["nav"]
            if nav is not None:
                navs.append(nav - (fees or 0))
            if spent + (fees or 0) > contract["max_cost"]:
                reasons.append("plafond de coûts dépassé")
        if (max_drawdown(navs) or 0) > contract["max_drawdown"]:
            reasons.append("baisse absolue dépassée")
        return sorted(set(reasons))
    paused = dict(db.execute("select variant,reason from stops where campaign=?", (key,)))
    state_row = db.execute("select state from days where campaign=? order by day desc limit 1",(key,)).fetchone()
    state = json.loads(state_row[0]) if state_row else {}
    gaps = []
    for d in due:
        if d in stored:
            continue
        if stored and d < max(stored):
            gaps.append(d)
            continue
        frame = inputs(d,batches.get(d,[]),cfg,audit)
        if frame is None:
            gaps.append(d)
            continue
        prior_day = scheduled[scheduled.index(d)-1]
        # A stop freezes new instructions. Holdings continue to be marked, including unsettled proceeds.
        variants = [v for v in cfg["variants"] if v not in paused]
        dated = obs
        if evidence_files is not None:
            usable=[p for ts,p in evidence_files if ts<=moment(frame[2])]
            if not usable:
                gaps.append(d)
                continue
            dated=load(usable[-1])
            if moment(dated["at"])>moment(frame[2]):
                gaps.append(d)
                continue
        documented=sum(v for k,v in dated["fees"].items() if contract["start"]<=k<=d and v is not None)
        new = run_day(state,d,frame,dict(cfg,variants=variants,cost_cap=contract["max_cost"],
                                         api_documented=documented),conv,cal,prior_day)
        new["cost_evidence_at"]=dated["at"]
        for v in paused:
            prior = stored[max(stored)]["books"][v]
            new["books"][v] = {m:dict(b,new_trades=[],intents=[]) for m,b in prior.items()}
        with db:
            db.execute("insert into days values (?,?,?,?)",(key,d,json.dumps(new),json.dumps(state)))
        stored[d] = mark(new)
        for v in variants:
            reasons = known_breaks(v, list(stored.values()))
            if reasons:
                paused[v] = ", ".join(reasons)
                with db:
                    db.execute("insert or ignore into stops values (?,?,?)",(key,v,paused[v]))
    ordered = [stored[d] for d in sorted(stored)]
    results = []
    for variant in cfg["variants"]:
        spans = []
        for start,end in ((contract["start"],contract["split"]),(contract["split"],contract["end"])):
            days = [d for d in due if start<=d<end]
            prior = scheduled[scheduled.index(start)-1] if start in scheduled else max(d for d in scheduled if d<start)
            summary = window(ordered,days,None if start==contract["start"] else prior,contract["capital"],variant,
                             obs,contract,frozen,start)
            ideas = set()
            for fr in ordered:
                if fr["day"] not in days:
                    continue
                row = fr["books"][variant]["normal"]
                for trade in row["new_trades"]:
                    meta = row["metadata"].get(trade["symbol"],{})
                    venue = PLACES.get(meta.get("place"))
                    dates = cal["tables"].get(venue,{})
                    if sum(fr["day"] < d < today.astimezone(PARIS).date().isoformat() for d in dates) >= 20:
                        ideas.add((fr["day"],trade["symbol"],trade["side"]))
            summary.update(start=start, end_exclusive=end, maturity=len(ideas))
            spans.append(summary)
        verdict = "poursuivre l’observation"
        reasons = known_breaks(variant,ordered)
        full = window(ordered,due,None,contract["capital"],variant,obs,contract,frozen,contract["start"])
        comparison = full["comparisons"].get("variant_matched",{}).get("drawdown")
        if comparison is not None and full["drawdown"] is not None and full["drawdown"]>comparison+contract["relative_drawdown"]:
            reasons.append("baisse relative dépassée sur le mandat")
        for span in spans:
            ref = span["comparisons"].get("variant_matched",{}).get("drawdown")
            if ref is not None and span["drawdown"] is not None and span["drawdown"] > ref+contract["relative_drawdown"]:
                reasons.append("baisse relative dépassée")
        if reasons:
            if variant not in paused:
                paused[variant] = ", ".join(sorted(set(reasons)))
                with db:
                    db.execute("insert or ignore into stops values (?,?,?)",(key,variant,paused[variant]))
        if variant in paused:
            verdict = "abandonner"
        elif today.astimezone(PARIS).date().isoformat() >= contract["end"] and all(x["complete"] and x["maturity"] for x in spans):
            robust = all(min(x["stress"].values()) > max(v["net"] for v in x["comparisons"].values())
                         and all(v["excess"]>0 for v in x["comparisons"].values()) for x in spans)
            verdict = "retenir" if robust else "abandonner"
        results.append(dict(variant=variant,verdict=verdict,reason=paused.get(variant),windows=spans))
    marksdb.close()
    ticket = {}
    if ordered:
        for v in cfg["variants"]:
            ticket[v] = ordered[-1]["books"][v]["normal"]["new_trades"]
    return dict(id=key, strategy=version, sessions=len(ordered), gaps=gaps, last_day=max(stored,default=None),
                results=results, tickets=ticket, due=len(due), current_inputs=latest["at"])


def shadow_after_review(db, contract, cal, batches, now):
    """Continue the four shadow books after the sealed evaluation, without rewriting either test window."""
    key=digest(contract)
    db.execute("create table if not exists shadow(campaign text,day text,state text,intents text,primary key(campaign,day))")
    out={"day":None,"tickets":{}}
    if now.astimezone(PARIS).date().isoformat()<contract["end"]:
        return out
    last=db.execute("select day,state,intents from shadow where campaign=? order by day desc limit 1",(key,)).fetchone()
    baseline=db.execute("select day,state from days where campaign=? order by day desc limit 1",(key,)).fetchone()
    if not baseline:
        return out
    cfg,conv,_=policy()
    cfg=dict(cfg,capital_eur=contract["capital"],cash_floor=1-contract["exposure"],max_sector_weight=contract["sector"])
    state=json.loads(last[1] if last else baseline[1])
    if last:
        out={"day":last[0],"tickets":json.loads(last[2])}
    calendar=sessions(cal)
    audit=load(FEED/"audit.json")
    stops={v for v, in db.execute("select variant from stops where campaign=?",(key,))}
    for day in calendar:
        if day<contract["end"] or (last and day<=last[0]) or day>now.astimezone(PARIS).date().isoformat():
            continue
        frame=inputs(day,batches.get(day,[]),cfg,audit)
        if not frame or moment(frame[3])>now:
            continue
        vcfg=dict(cfg,variants=[v for v in cfg["variants"] if v not in stops])
        result=run_day(state,day,frame,vcfg,conv,cal,calendar[calendar.index(day)-1])
        tickets={v:r["normal"]["intents"] for v,r in result["books"].items()}
        with db:
            db.execute("insert into shadow values (?,?,?,?)",(key,day,json.dumps(state),json.dumps(tickets)))
        out={"day":day,"tickets":tickets}
    return out


def main():
    from transition import pilot_gate, retirement
    guard = isolation()
    if not all(guard.values()):
        raise RuntimeError("Isolement incomplet")
    now = datetime.now(timezone.utc)
    contract = load(CODE/"mandate.json")
    if contract.get("method") != method_hash():
        raise RuntimeError("Le calcul diffère de la méthode scellée")
    if not (contract["confirmed"] and moment(contract["sealed_at"]) <
            datetime.fromisoformat(contract["start"]+"T17:18").replace(tzinfo=PARIS)):
        raise RuntimeError("Mandat non scellé avant la première décision")
    sources = sorted((FEED/"pilotage").glob("*.json.gz"))
    if not sources:
        raise ValueError("La première observation est attendue")
    catalog = sorted((FEED/"batches").glob("*.json.gz"))
    latest = load(catalog[-1])
    batches = defaultdict(list)
    for p in catalog:
        at = datetime.strptime(p.name[:19],"%Y-%m-%dT%H-%M-%S").replace(tzinfo=timezone.utc)
        batches[at.astimezone(PARIS).date().isoformat()].append((at,p))
    STATE.mkdir(exist_ok=True)
    out = REPORTS/"pilotage"
    out.mkdir(exist_ok=True)
    db = sqlite3.connect(STATE/"campaign.sqlite",timeout=15)
    db.executescript("""create table if not exists days(campaign text,day text,frame text,state text,primary key(campaign,day));
                         create table if not exists stops(campaign text,variant text,reason text,primary key(campaign,variant));
                         create table if not exists fx(day text primary key,rate real);
                         create table if not exists captures(name text primary key);
                         create table if not exists runs(id integer primary key,day text,status text,graph text);""")
    seen = set(n for n, in db.execute("select name from captures"))
    with db:
        for p in sources:
            if p.name in seen and p != sources[-1]:
                continue
            obs=load(p)
            if digest(obs)[:16] != p.name.split("-")[-1][:16]:
                raise ValueError("Empreinte de collecte incorrecte")
            for row in obs["runs"]:
                db.execute("insert into runs values (?,?,?,?) on conflict(id) do update set status=excluded.status,"
                           "graph=coalesce(excluded.graph,runs.graph)",(row["id"],row["day"],row["status"],row["graph"]))
            db.execute("insert or ignore into captures values (?)",(p.name,))
    dated=[(datetime.strptime(p.name[:15],"%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc),p) for p in sources]
    cal=load(FEED/"sessions.json")
    campaign = score(db,contract,obs,cal,batches,latest,now,dated)
    shadow=shadow_after_review(db,contract,cal,batches,now)
    db.close()
    gate = pilot_gate(campaign,obs,now)
    gate["register"] = obs.get("pilot_record")
    inventory = retirement(obs)
    historical = load(REPORTS/"report.json")
    exploratory = [dict(day=r["day"],variant=v["variant"],net=v.get("closing_net"))
                   for r in historical["reviews"] if r["day"] < contract["start"] for v in r["results"]]
    atomic(out/"report.json",dict(at=now.isoformat(),observed=obs["at"],mandate=contract,formal=campaign,
                                  pilot=gate,shadow=shadow,retirement=inventory,exploratory=exploratory,
                                  blocked=obs["blocked"],network=guard,experiments_at=obs["trials_at"]))
    print("Séances formelles",campaign["sessions"],"pilote",gate["state"],"inventaire",len(inventory))


if __name__ == "__main__":
    main()
