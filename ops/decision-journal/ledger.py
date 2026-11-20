"""Read a stable independent copy, so analytical queries never take a production DuckDB lock."""
from collections import defaultdict
from contextlib import contextmanager
import json
from pathlib import Path
import shutil
import tempfile

from evidence import fingerprint, protect


@contextmanager
def reader(source, scratch):
    import duckdb
    source = Path(source)
    wal = Path(str(source)+".wal")

    def stamp():
        if wal.exists() and wal.stat().st_size:
            raise RuntimeError("Journal DuckDB ouvert, reprise au prochain passage")
        s = source.stat()
        return s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns

    with tempfile.TemporaryDirectory(dir=scratch) as tmp:
        before = stamp()
        copy = Path(tmp)/"ledger.duckdb"
        shutil.copyfile(source, copy)
        if before != stamp():
            raise RuntimeError("Base modifiée pendant la copie, reprise au prochain passage")
        db = duckdb.connect(str(copy), read_only=True, config={"threads":1})
        try:
            yield db
        finally:
            db.close()


def rows(db, sql):
    result = db.execute(sql)
    cols = [c[0] for c in result.description]
    for row in result.fetchall():
        r = dict(zip(cols, row))
        for k, v in r.items():
            if k.endswith("_json") and isinstance(v,str):
                r[k] = json.loads(v)
        yield r


RUNS = """select run_id,cast(ts_start as varchar) ts_start,n8n_execution_id,strategy_version,prompt_version,
    config_version,model,data_ok_for_trading,ai_cost_eur,risk_gate_json,global_context_payload_hash
    from core.runs where run_id in (select run_id from core.model_proposals) or strategy_version='strategy_v4_consensus'"""
MODELS = """select proposal_id,run_id,model_key,model_name,model_id,parse_ok,extractor_status,decision_json,
    actions_json,warnings_json,latency_ms from core.model_proposals order by run_id,proposal_id"""
DECISIONS = """select consensus_id,run_id,symbol,intent,action,side,vote_count,valid_model_count,model_keys,status,
    reason,selected_qty,selected_weight_pct,selected_limit_price,order_id,confidence
    from core.consensus_decisions order by run_id,consensus_id"""
VOTES = """select vote_id,run_id,consensus_id,model_key,symbol,intent,action,side,executable,confidence,
    target_qty,target_weight_pct,limit_price,rationale from core.consensus_votes order by run_id,vote_id"""
ORDERS = """select order_id,run_id,cast(ts_created as varchar) ts_created,symbol,side,intent,order_type,qty,
    limit_price,status,broker,broker_order_id,reason,json_extract_string(rationale_json,'$.clientOrderId') client_id
    from core.orders order by ts_created,order_id"""
FILLS = """select f.fill_id,f.order_id,f.run_id,cast(ts_fill as varchar) ts_fill,f.qty,f.price,f.fees_eur,
    f.slippage_bps,f.currency,f.price_native,f.fx_rate_eur,c.commission_eur,c.commission_source,
    coalesce(nullif(c.broker_execution_id,''),json_extract_string(f.raw_fill_json,'$.ibkrFill.execution_id')) execution_id,
    json_extract_string(f.raw_fill_json,'$.ibkrFill.order_id') broker_order_id,
    json_extract_string(f.raw_fill_json,'$.ibkrFill.order_ref') client_id
    from core.fills f left join core.fill_costs c using(fill_id) order by ts_fill,fill_id"""


def stable_id(r):
    return r.get("consensus_id") or "decision-"+fingerprint([r.get(k) for k in ("run_id","symbol","intent","side","action")])[:24]


def order_links(decisions, orders):
    for d in decisions:
        ident = d.get("order_id")
        if ident:
            matched = [o for o in orders if o["order_id"]==ident]
            method = "order_id" if matched else "order_id_introuvable"
        elif d["status"] == "CONSENSUS_APPROVED":
            matched = [o for o in orders if (o["symbol"],o["side"])==(d["symbol"],d["side"])]
            method = {0:"aucun_ordre_correspondant",1:"run_symbole_sens"}.get(len(matched),"ambigu")
            if matched and sum(other["status"]=="CONSENSUS_APPROVED" and (other["symbol"],other["side"])==(d["symbol"],d["side"]) for other in decisions)>1:
                method = "ambigu"
        else:
            matched,method = [],"aucun_ordre_attribué"
        d["decision_id"] = stable_id(d)
        d["link_method"] = method
        d["candidate_order_ids"] = [o["order_id"] for o in matched]
        d["linked_order_ids"] = [] if method=="ambigu" else list(d["candidate_order_ids"])


def fill_links(orders, fills):
    lookup = {k:defaultdict(set) for k in ("order_id","broker_order_id","client_id")}
    by_id = {o["order_id"]:o for o in orders}
    for o in orders:
        o["fills"] = []
        for k, m in lookup.items():
            if o.get(k):
                m[str(o[k])].add(o["order_id"])
    unresolved = []
    executions = {}
    for f in fills:
        matches = {k:m[str(f[k])] for k,m in lookup.items() if f.get(k) and str(f[k]) in m}
        candidates = set().union(*matches.values()) if matches else set()
        if len(candidates)!=1:
            unresolved.append(dict(f, candidates=sorted(candidates), reason="ambiguous_or_missing_order"))
            continue
        oid = next(iter(candidates))
        if f.get("broker_order_id") and by_id[oid].get("broker_order_id") and str(f["broker_order_id"])!=str(by_id[oid]["broker_order_id"]):
            unresolved.append(dict(f, candidates=[oid], reason="broker_order_id_conflict"))
            continue
        key = ("broker",f["execution_id"]) if f.get("execution_id") else ("local",f["fill_id"])
        if key in executions:
            unresolved.append(dict(f, candidates=[oid], reason="duplicate_execution_reference"))
            continue
        executions[key] = oid
        by_id[oid]["fills"].append(dict(f, matched_on=sorted(matches)))
    return unresolved


def collect_ledger(db, put):
    runs = {}
    def dossier(rid):
        return runs.setdefault(rid, {"run":{"run_id":rid}, "models":[], "votes":[], "decisions":[], "orders":[]})

    for r in rows(db,RUNS):
        dossier(r["run_id"])["run"] = r
    for key, query in (("models",MODELS),("decisions",DECISIONS),("votes",VOTES)):
        for r in rows(db,query):
            dossier(r["run_id"])[key].append(r)
    orders = list(rows(db,ORDERS))
    unresolved = fill_links(orders, rows(db,FILLS))
    for o in orders:
        if o["run_id"] in runs:
            dossier(o["run_id"])["orders"].append(o)
    # Keep orders originating in a PF reconciliation visible, never attribute by symbol alone.
    put("reconciliation","unassigned",protect({"unlinked_fills":unresolved,
        "other_orders":[o for o in orders if o["run_id"] not in runs]}))
    for rid, d in sorted(runs.items()):
        order_links(d["decisions"],d["orders"])
        put("ledger",rid,protect(d))
