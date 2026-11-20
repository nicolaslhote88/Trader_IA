"""Offline worker: makes no network calls. Every publication is reproducible from its input hash."""

from datetime import datetime, timezone, timedelta, date
from pathlib import Path
import json
import os
import sqlite3
from zoneinfo import ZoneInfo
from common import atomic, digest, load, moment
from engine import book, replay
from strategies import review
from universe import prepare, reasons, ticket, type_of, PLACES, session

CODE = Path(__file__).resolve().parent
FEED = Path("/var/lib/trader-research-feed")
STATE = Path("/var/lib/trader-research-sim")
OUT = Path("/local-files/duckdb/research_reporting")
PARIS = ZoneInfo("Europe/Paris")


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


def policy():
    cfg = load(CODE / "scenario.json")
    conv = load(CODE / "execution.json")
    import hashlib

    import costs

    files = [CODE / n for n in ("engine.py", "strategies.py", "universe.py", "worker.py", "common.py")]
    files.append(Path(costs.__file__))
    src = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    return cfg, conv, digest([cfg, conv, src])


def isolation():
    import socket

    probe = {}
    for name, family in [("ipv4", socket.AF_INET), ("unix", socket.AF_UNIX)]:
        try:
            s = socket.socket(family, socket.SOCK_STREAM)
            s.close()
            probe[name] = False
        except OSError:
            probe[name] = True
    probe["no_operations_files"] = not any(
        os.access(p, os.R_OK)
        for p in [
            "/run/docker.sock",
            "/var/run/docker.sock",
            "/root/.ssh",
            "/var/lib/docker",
            "/local-files/duckdb/ag1_v4_consensus.duckdb",
            "/var/lib/trader-economic",
            "/opt/trading-dashboard/secrets",
        ]
    )
    probe["no_trade_credentials"] = not any(
        k.startswith(("IBKR", "N8N", "OPENAI_API", "ANTHROPIC_API", "DEEPSEEK_API")) for k in os.environ
    )
    return probe


def display(rows, at, cfg, conv, cal, audit=False):
    result = []
    for r in rows:
        s = r["symbol"]
        product = r.get("product") or type_of(s, r["asset"])
        held = r.get("held", "HELD" in r["segments"])
        blockers = r.get("block", []) if audit else reasons(r, at, cfg, cal)
        nominal, total = (None, None) if audit else ticket(r, at, conv)
        if not audit and total is None and not blockers:
            blockers.append("tarif ou coût du ticket absent")
        item = {
            k: r.get(k)
            for k in (
                "symbol",
                "name",
                "sector",
                "asset",
                "country",
                "place",
                "currency",
                "contract",
                "segments",
                "enabled",
                "quarantine",
                "reason",
                "scoped",
                "pre",
                "base_universe",
                "score",
                "fund_at",
                "fund_source",
                "ai",
                "tech_at",
                "lot",
                "minimum",
                "lot_source",
                "permission_day",
                "fx",
                "fx_day",
                "quote_time",
                "received",
                "availability",
                "bid",
                "ask",
                "spread",
                "volume",
                "held_asof",
            )
        }
        if audit:
            state = "présélection du soir" if r.get("pre") else "étude" if r.get("scoped") else "écarté"
        elif not blockers and total and total <= cfg["capital_eur"] * (1 - cfg["cash_floor"]):
            state = "achat simulable"
        elif held:
            state = "suivi détenu"
        elif r["quarantine"] or not r["scoped"]:
            state = "écarté"
        elif product != "action":
            state = "suivi spécialisé"
        else:
            state = "étude"
        item.update(
            product=product,
            held=held,
            state=state,
            block=blockers,
            minimum_nominal=nominal,
            minimum_cost=total,
            calendar=PLACES.get(r.get("place")),
            session_utc=cal["tables"].get(PLACES.get(r.get("place")) or "", {}).get(at[:10]),
            session_open=None if audit else session(r.get("place"), at, cal),
        )
        result.append(item)
    if not any(r["symbol"] == "EUNL.DE" for r in result):
        result.append(
            dict(
                symbol="EUNL.DE",
                name="iShares Core MSCI World UCITS ETF",
                product="fonds mondial",
                state="témoin de recherche",
                currency="EUR",
                place="XETRA",
                asset="ETF",
                calendar="XETR",
                segments=[],
                held=False,
                pre=False,
                scoped=False,
                enabled=False,
                quarantine=False,
                block=["référence de comparaison hors mandat actions"],
            )
        )
    return result


def membership(rows):
    return {
        r["symbol"]: [r.get("enabled"), r.get("segments"), r.get("quarantine"), r.get("held")] for r in rows
    }


def revision(db, at, rows, key):
    if db.execute("select 1 from revisions where source=?", (key,)).fetchone():
        return
    previous = db.execute("select file from revisions where at<? order by at desc limit 1", (at,)).fetchone()
    old = membership(load(OUT / previous[0])) if previous else {}
    new = membership(rows)
    changes = []
    if previous:
        for s in sorted(old.keys() | new.keys()):
            if old.get(s) != new.get(s):
                changes.append(dict(symbol=s, before=old.get(s), after=new.get(s)))
    h = digest(rows)
    file = "univers/" + h + ".json.gz"
    if not (OUT / file).exists():
        atomic(OUT / file, rows)
    counts = dict(
        sources=sum(r["state"] != "témoin de recherche" for r in rows),
        scoped=sum(bool(r.get("scoped")) for r in rows),
        pre=sum(bool(r.get("pre")) for r in rows),
        held=sum(bool(r["held"]) for r in rows),
        paper=sum(r["state"] == "achat simulable" for r in rows),
    )
    db.execute(
        "insert into revisions values(?,?,?,?,?)", (at, key, file, json.dumps(counts), json.dumps(changes))
    )


def run_reviews(db, files, now, cfg, conv, cal, audit, version):
    bydate = {}
    for ts, p in files:
        bydate.setdefault(moment(ts).astimezone(PARIS).date().isoformat(), []).append((ts, p))
    # Rebuild four independent books from their locked reviews. Re-running never spends cash twice.
    last = db.execute(
        "select day,books from reviews where version=? order by day desc limit 1", (version,)
    ).fetchone()
    books = json.loads(last[1]) if last else {k: book(cfg["capital_eur"]) for k in cfg["variants"]}
    pauses = load(FEED / "pauses.json") if (FEED / "pauses.json").exists() else {}
    if any(k not in cfg["variants"] for k in pauses):
        raise ValueError("Identifiant de variante suspendue inconnu")
    day_cursor = date.fromisoformat(cfg["start"])
    while day_cursor <= moment(now).astimezone(PARIS).date():
        day_key = day_cursor.isoformat()
        cutoff = datetime.fromisoformat(day_key + "T" + cfg["review_paris"]).replace(
            tzinfo=PARIS
        ) + timedelta(minutes=6)
        if (
            day_cursor.weekday() in cfg["weekdays"]
            and moment(now) >= cutoff
            and not (last and day_key <= last[0])
        ):
            db.execute(
                "insert or ignore into gaps values(?,?,?)",
                (version, day_key, "En attente de photographies encadrant la revue"),
            )
        day_cursor += timedelta(days=1)
    for day, entries in sorted(bydate.items()):
        if (
            day < cfg["start"]
            or (last and day <= last[0])
            or date.fromisoformat(day).weekday() not in cfg["weekdays"]
        ):
            continue
        t = datetime.fromisoformat(day + "T" + cfg["review_paris"]).replace(tzinfo=PARIS)
        arrival = t + timedelta(seconds=cfg["latency_s"])
        cut = iso(t)
        arr = iso(arrival)
        if moment(now) < arrival:
            continue
        before = [
            (ts, p) for ts, p in entries if moment(ts) <= t and (t - moment(ts)).total_seconds() <= 3600
        ]
        after = [(ts, p) for ts, p in entries if moment(ts) >= arrival]
        if not after:
            continue
        if not before:
            db.execute(
                "insert or ignore into gaps values(?,?,?)",
                (version, day, "Photographie de présélection absente dans l’heure précédant la revue"),
            )
            continue
        src = load(before[-1][1])
        post = load(after[0][1])
        if moment(src["at"]) > t or moment(post["at"]) < arrival:
            db.execute(
                "insert or ignore into gaps values(?,?,?)",
                (version, day, "Heure exacte des observations incompatible avec la décision"),
            )
            continue
        # Only already-received quote frames cross this boundary. Post-decision fundamentals, universe and fills never enter a decision.
        joined = {**src, "frames": sorted(src["frames"] + post["frames"], key=lambda x: x["received"])}
        dec = prepare(joined, cut, audit)
        exe = prepare(joined, arr, audit)
        inputs = digest([dec, exe, cut, arr, version, pauses])
        daily = []
        for name in cfg["variants"]:
            if name in pauses and day >= pauses[name]:
                continue
            daily.append(review(books[name], name, dec, exe, cut, cfg, conv, cal))
        file = "revues/" + digest([inputs, daily]) + ".json.gz"
        atomic(OUT / file, dict(input=inputs, version=version, results=daily))
        db.execute(
            "insert into reviews values(?,?,?,?,?,?)",
            (
                version,
                day,
                inputs,
                file,
                json.dumps(books),
                json.dumps(
                    [{k: v for k, v in x.items() if k not in ("alternatives", "sales")} for x in daily]
                ),
            ),
        )
        db.execute("delete from gaps where version=? and day=?", (version, day))


def run():
    restrictions = isolation()
    if not all(restrictions.values()):
        raise RuntimeError("Isolement du simulateur incomplet " + str(restrictions))
    cfg, conv, version = policy()
    audit = load(FEED / "audit.json")
    cal = load(FEED / "sessions.json")
    now = datetime.now(timezone.utc).isoformat()
    for name in ("univers", "revues"):
        (OUT / name).mkdir(exist_ok=True)
    db = sqlite3.connect(STATE / "sim.sqlite")
    db.executescript("""create table if not exists revisions(at text,source text primary key,file text,counts text,changes text);
        create table if not exists reviews(version text,day text,input text,file text,books text,result text,primary key(version,day));
        create table if not exists gaps(version text,day text,reason text,primary key(version,day));
        create table if not exists closes(day text,symbol text,currency text,price real,seen text,primary key(day,symbol,currency));""")
    files = []
    for path in sorted((FEED / "batches").glob("*.json*")):
        # The collection timestamp is stored at full precision in the batch; timestamps in names are only for sorting.
        s = path.name[:19]
        at = s[:10] + "T" + s[11:].replace("-", ":") + "+00:00"
        files.append((at, path))
    if not files:
        raise ValueError("Aucun lot de données")
    with db:
        revision(db, audit["at"], display(audit["rows"], audit["at"], cfg, conv, cal, True), digest(audit))
        for i, (ts, p) in enumerate(files):
            key = p.name
            if db.execute("select 1 from revisions where source=?", (key,)).fetchone() and i < len(files) - 1:
                continue
            batch = load(p)
            at = batch["at"]
            if key.split("-")[-1].split(".")[0] != digest(batch)[:16]:
                raise ValueError("Empreinte du lot de données")
            for x in batch.get("closes", []):
                # Only a bar archived after its session's close may serve as a closing mark.
                if x["day"] < at[:10] and x.get("currency") and x["seen"][:10] > x["day"]:
                    db.execute(
                        "insert or ignore into closes values(?,?,?,?,?)",
                        (x["day"], x["symbol"], x["currency"], x["price"], x["seen"]),
                    )
            rows = prepare(batch, at, audit)
            revision(db, at, display(rows, at, cfg, conv, cal), key)
        run_reviews(db, files, now, cfg, conv, cal, audit, version)
    census = [
        dict(at=at, file=f, counts=json.loads(c), changes=json.loads(ch))
        for at, f, c, ch in db.execute("select at,file,counts,changes from revisions order by at")
    ]
    summaries = [
        dict(day=d, input=h, file=f, results=json.loads(r))
        for d, h, f, r in db.execute(
            "select day,input,file,result from reviews where version=? order by day", (version,)
        )
    ]
    rep = replay(batch, cfg, conv, cal)
    if rep != replay(batch, cfg, conv, cal):
        raise RuntimeError("Rejeu non déterministe")
    gaps = [
        dict(day=d, reason=r)
        for d, r in db.execute("select day,reason from gaps where version=? order by day", (version,))
    ]
    # Align each witness to the closing date of the paper portfolio's launch. Never use future observations.
    benches = batch["bench"]
    bbase = next((x for x in reversed(benches) if x["date"] < cfg["start"]), None)
    for summary in summaries:
        end = next((x for x in benches if x["date"] == summary["day"]), None)
        for result in summary["results"]:
            day = summary["day"]
            result["excess"] = {}
            acc = result["cash"] + result["unsettled"]
            missing = []
            for sym, qty in result["positions"].items():
                ccy = result.get("currencies", {}).get(sym)
                closing = db.execute(
                    "select price from closes where day=? and symbol=? and currency=?", (day, sym, ccy)
                ).fetchone()
                fx = 1 if ccy == "EUR" else batch.get("closing_usd", {}).get(day) if ccy == "USD" else None
                if not closing or not fx:
                    missing.append(sym)
                else:
                    acc += qty * closing[0] * fx
            result["closing_missing"] = missing
            result["closing_net"] = acc / cfg["capital_eur"] - 1 if not missing and day < now[:10] else None
            for k in ("world", "matched", "basket", "account"):
                if (
                    end
                    and bbase
                    and end.get(k) is not None
                    and bbase.get(k) is not None
                    and result["closing_net"] is not None
                ):
                    result["excess"][k] = result["closing_net"] - ((1 + end[k]) / (1 + bbase[k]) - 1)
    from receipt import checks

    acceptance = checks(audit, batch, rows, cfg, conv, cal)
    report = dict(
        created=now,
        observation=batch["at"],
        version=version,
        scenario=cfg,
        execution=conv,
        calendar=cal["version"],
        census=census,
        replay=rep,
        checks=acceptance,
        network=restrictions,
        reviews=summaries,
        gaps=gaps,
        signal=audit["historical_signal"],
        benchmark_base=bbase["date"] if bbase else None,
        partial=batch.get("partial", []),
        pauses=load(FEED / "pauses.json") if (FEED / "pauses.json").exists() else {},
    )
    atomic(OUT / "report.json", report)
    print("Univers", census[-1]["counts"], "rejeux", rep["complete_packs"], "revues", len(summaries))


if __name__ == "__main__":
    run()
