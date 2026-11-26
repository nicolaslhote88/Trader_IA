"""Read-only sources. The calculation service receives immutable, filtered inputs."""

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import gzip, json, sys

sys.path.insert(0, "/opt/trader-ia/research-sim/current")
from common import load, atomic, digest, moment
from collect import sqlread, query, archive

FEED = Path("/var/lib/trader-research-feed")
DST = FEED / "experiments"
DB = Path("/local-files/duckdb")


def common_input(call):
    pieces = []
    for v in call.get("input", {}).get("messages", []):
        s = v.get("content", "") if isinstance(v, dict) else str(v)
        if "RUN\n" in s and "OPPORTUNITES ELIGIBLES\n" in s:
            head = s[s.index("RUN\n") :]
            pref, tail = head.split("OPPORTUNITES ELIGIBLES\n", 1)
            try:
                obj, end = json.JSONDecoder().raw_decode(tail.lstrip())
                pieces.append(digest([pref.strip(), obj]))
            except ValueError:
                pass
    return pieces[0] if len(pieces) == 1 else None


def trace_data():
    with sqlread("/var/lib/trader-ag1-journal/journal.sqlite") as db:

        def docs(kind):
            return {
                str(k): (h, json.loads(gzip.decompress(raw)))
                for k, h, raw in db.execute(
                    "select name,hash,data from refs join objects using(hash) where kind=? order by name",
                    (kind,),
                )
            }

        traces = docs("trace")
        ledgers = docs("ledger")
    result = []
    unavailable = []
    for eid, (hash_, t) in traces.items():
        packs = t["stages"].get("input_pack", [])
        run = (t.get("context") or {}).get("runId")
        if not packs or run not in ledgers:
            unavailable.append(dict(execution=eid, at=t.get("started_utc"), status=t.get("status"),
                                    reason="dossier de décision ou registre absent", trace=hash_))
            continue
        _, l = ledgers[run]
        p = packs[0]
        calls = {x["ledger_model_key"]: x for x in t["calls"] if x.get("ledger_model_key")}
        models = []
        for m in l["models"]:
            call = calls.get(m["model_key"], {})
            models.append(
                dict(
                    key=m["model_key"],
                    requested=call.get("model") or m.get("model_id"),
                    valid=m["parse_ok"],
                    status=m["extractor_status"],
                    paired=common_input(call),
                    input_hash=call.get("input_sha256"),
                    response_hash=digest(call["response"]) if call.get("response") else None,
                    size=len(json.dumps(call.get("input"), ensure_ascii=False).encode()),
                    actions=m.get("actions_json") or [],
                )
            )
        if not p.get("opportunity_pack"):
            continue
        result.append(
            dict(
                eid=eid,
                run=run,
                trace=hash_,
                at=moment(t["started_utc"]).isoformat(),
                end=moment(t["ended_utc"]).isoformat(),
                portfolio=p["portfolio_pack"],
                ctx=p.get("global_context", {}),
                rows=p["opportunity_pack"]["rows"],
                votes=l["votes"],
                models=models,
                decisions=l["decisions"],
                orders=[o for x in t["stages"].get("dispatch", []) for o in x.get("orders", [])],
                pack_hash=digest(p),
                graph=t.get("published_graph_sha256"),
                verified=t.get("published_parameters_verified"),
            )
        )
    return sorted(result, key=lambda r: r["at"]), unavailable


def articles():
    # Archive full source text for private inspection, publish only the selected event fields.
    sql = """select h.news_id id,substr(md5(h.news_id),1,12) as "label",h.symbol,h.company_name company,h.source,
      h.title,h.canonical_url,h.url,h.snippet,h.text,h.summary,h.impact_score impact,h.sentiment,
      cast(h.published_at as varchar) pub,cast(h.first_seen_at as varchar) seen,
      cast(h.analyzed_at as varchar) analyzed,cast(h.updated_at as varchar) updated,
      cast(h.fetched_at as varchar) fetched,cast(h.created_at as varchar) created,
      a.news_id is not null in_digest from news_history h
      left join (select distinct news_id from news_analyzed) a on a.news_id=h.news_id
      where coalesce(h.first_seen_at,h.published_at)>=current_date-interval 45 day
      order by h.news_id,h.symbol"""
    groups = defaultdict(list)
    for r in query("ag4_spe_v2", sql):
        for k in ("pub", "seen", "analyzed", "updated", "fetched", "created"):
            if r[k]:
                r[k] = moment(r[k]).isoformat()
        groups[(r["seen"] or r["pub"])[:10]].append(r)
    paths = []
    for day, rows in sorted(groups.items()):
        name = day + "-" + digest(rows)[:20] + ".json.gz"
        p = DST / "articles" / name
        if not p.exists():
            atomic(p, rows)
        paths.append(name)
    return paths


def main():
    sets = articles()
    traces, unavailable = trace_data()
    paths = sorted(FEED.joinpath("batches").glob("*.json*"))
    prior = load(paths[-1])
    eco = load(DB / "economic_reporting/report.json")
    act = load(DB / "lot03_reporting/compte.json")
    fees = eco["usage"]["calls"]
    closes = query(
        "ag2_v3",
        """select symbol,cast(cast(d1_date as date) as varchar) d,currency,d1_last_close px
      from technical_signals where d1_closed_only and d1_status='OK' and d1_last_close>0
      and cast(workflow_date as date)>cast(d1_date as date) and d1_date>=current_date-interval 120 day
      qualify row_number() over(partition by symbol,cast(d1_date as date) order by workflow_date,id)=1 order by symbol,d""",
    )
    fundamentals = query(
        "ag3_v2",
        """select symbol,cast(fetched_at as varchar) ts,score from fundamentals_triage_history
      where fetched_at>=current_date-interval 120 day and status='OK' and score is not null order by symbol,fetched_at""",
    )
    at = datetime.now(timezone.utc).isoformat()
    journal = archive()
    data = dict(
        at=at,
        articles=sets,
        traces=traces,
        unavailable=unavailable,
        base_file=paths[-1].name,
        base_at=prior["at"],
        closes=closes,
        market={k: journal[k] for k in ("events", "frames", "permissions", "ledger_runs")},
        fundamentals=fundamentals,
        usage=[
            {
                k: r.get(k)
                for k in (
                    "execution",
                    "run",
                    "at",
                    "requested",
                    "returned",
                    "provider",
                    "node",
                    "input",
                    "output",
                    "estimate_usd_low",
                    "estimate_usd_high",
                    "attempt",
                    "producer",
                )
            }
            for r in fees
            if r["producer"] in ("AG1", "AG4")
        ],
        flex=act["trades"],
        control=eco["performance"]["check"],
    )
    dest = at[:19].replace(":", "-") + "-" + digest(data)[:16] + ".json.gz"
    atomic(DST / "captures" / dest, data)
    print("Capture", at, len(traces), "décisions", len(sets), "journées de nouvelles", flush=True)


if __name__ == "__main__":
    main()
