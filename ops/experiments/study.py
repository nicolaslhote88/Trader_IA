from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict
import sys

sys.path.insert(0, "/opt/trader-ia/research-sim/current")
from common import load, atomic, digest, moment
from worker import isolation
from exits import historical, recent
from news_study import evidence, evaluate
from archived_votes import diagnose
from opinions import dossier, decision_sets, fill_arm, costs_used, outcome, sdict

CODE = Path(__file__).resolve().parent
ROOT = Path("/var/lib/trader-research-feed")
FEED = ROOT / "experiments"
OUT = Path("/local-files/duckdb/research_reporting/experiments")
STATE = Path("/var/lib/trader-research-sim/experiments")
BASE_CODE = Path("/opt/trader-ia/research-sim/current")


def display_pack(view):
    return dict(
        r8=view["r8"],
        v2=view["v2"],
        raw=view["raw"],
        analyzed=view["analyzed"],
        events=view["events"],
        pack={k: [e["id"] for e in rows] for k, rows in view["pack"].items()},
        old={
            s: [{k: r.get(k) for k in ("id", "title", "source", "pub", "seen", "analyzed")} for r in rows]
            for s, rows in view["old"].items()
        },
    )


def receipt_cost(receipt, fx):
    all_records = receipt["records"]
    usd = sum(x["usd"] for x in all_records if x.get("usd") is not None)
    return dict(
        requested=receipt["requested"],
        structured=receipt.get("structured", False),
        workflow=receipt["workflow"],
        count=len(all_records),
        received=sum(x["received"] for x in all_records),
        valid=sum(x["valid"] for x in all_records),
        complete=all(x.get("usd") is not None for x in all_records),
        reserved=receipt["reserved_eur"],
        usd=usd,
        eur=usd * fx if fx else None,
    )


def paid_results(t, d, paid, base, p, cfg, conv, cal, closes, fx, view, mark_fx):
    from prompts import compact, clean

    records = []
    for batch in [*paid.get("earlier", []), paid]:
        if not batch.get("structured"):
            continue
        for x in batch.get("records", []):
            if x["run"] == t["run"] and x["input"] == digest(clean(compact(t, view, x["arm"]))):
                records.append(x)
    arms = {}
    for x in records:
        proposed = sdict(x["actions"]) if x["valid"] else d["rule"]
        arm = fill_arm(t, d, proposed, base, p, cfg, conv, cal)
        cost = x["usd"] * fx if x.get("usd") is not None and fx else None
        arms[x["arm"]] = dict(
            arm=arm,
            valid=x["valid"],
            hash=x["input"],
            model=x["model"],
            usd=x["usd"],
            outcome=outcome(
                arm, t["at"][:10], closes, {"closing_usd": mark_fx}, dict(low_eur=cost, high_eur=cost)
            ),
        )
    diffs = []
    if "original" in arms:
        original = arms["original"]
        for k, b in arms.items():
            if k == "original":
                continue
            aa = {tuple(i) for i in original["arm"]["proposed"]}
            bb = {tuple(i) for i in b["arm"]["proposed"]}

            def signature(a, filled):
                return {
                    (x["symbol"], x["side"], x["filled"] if filled else x["requested"])
                    for x in a["arm"]["decisions"]
                    if (x["filled"] if filled else x["requested"])
                }

            aa_net = original["outcome"]["after_ai_low"]
            bb_net = b["outcome"]["after_ai_low"]
            diffs.append(
                dict(
                    arm=k,
                    valid_pair=bool(original["valid"] and b["valid"]),
                    changed_decisions=len(aa ^ bb),
                    changed_orders=len(signature(b, False) ^ signature(original, False)),
                    changed_fills=len(signature(b, True) ^ signature(original, True)),
                    net_delta=bb_net - aa_net if aa_net is not None and bb_net is not None else None,
                    sides={
                        s: (b["outcome"]["sides"][s]["net_eur"] - original["outcome"]["sides"][s]["net_eur"])
                        if b["outcome"]["sides"][s]["net_eur"] is not None
                        and original["outcome"]["sides"][s]["net_eur"] is not None
                        else None
                        for s in ("BUY", "SELL")
                    },
                )
            )
    return dict(arms=arms, differences=diffs)


def measure(t, feed, base, rows, p, cfg, conv, cal, closes, paid, fx, mark_fx, usage):
    view = evidence(rows, t["at"], p)
    d = dossier(t, feed, base, view, p)
    variants, meta = decision_sets(t, d, p)
    m = {x["key"]: x for x in t["models"]}
    outputs = {}
    day = t["at"][:10]
    for name, actions in variants.items():
        used = (
            list(m.values())
            if name == "C"
            else [m[k] for k in (p["model_key"], p["second_key"]) if k in m]
            if name == "deux_avis" and meta["disputes"]
            else [m[p["model_key"]]]
            if p["model_key"] in m and (name == "un_avis" or name in ("B", "deux_avis") and meta["triggers"])
            else []
        )
        cost = costs_used(usage, used, t["run"], recent(mark_fx, day))
        if not used:
            cost.update(low_eur=0, high_eur=0)
        a = fill_arm(t, d, actions, base, p, cfg, conv, cal, veto=name == "A_veto")
        outputs[name] = dict(**a, ai=cost, outcome=outcome(a, day, closes, {"closing_usd": mark_fx}, cost))
    diff = []
    for reference, name in [("A", "B"), ("B", "deux_avis"), ("B", "C"), ("un_avis", "C"), ("A", "A_veto")]:
        a, b = outputs[reference], outputs[name]
        for side in ("BUY", "SELL"):
            aa = {s for s, k in a["proposed"] if k == side}
            bb = {s for s, k in b["proposed"] if k == side}
            pa = a["outcome"]["sides"][side]["net_eur"]
            pb = b["outcome"]["sides"][side]["net_eur"]
            diff.append(
                dict(
                    reference=reference,
                    variant=name,
                    side=side,
                    added=sorted(bb - aa),
                    removed=sorted(aa - bb),
                    fills=b["outcome"]["sides"][side]["fills"] - a["outcome"]["sides"][side]["fills"],
                    delta_eur=pb - pa if pa is not None and pb is not None else None,
                    identical_content=meta["paired"],
                )
            )
    after_costs = []
    for a, b in (("A", "B"), ("B", "deux_avis"), ("un_avis", "C"), ("A", "C"), ("A", "A_veto")):
        va, vb = outputs[a]["outcome"], outputs[b]["outcome"]
        lo = (
            vb["after_ai_low"] - va["after_ai_high"]
            if vb["after_ai_low"] is not None and va["after_ai_high"] is not None
            else None
        )
        hi = (
            vb["after_ai_high"] - va["after_ai_low"]
            if vb["after_ai_high"] is not None and va["after_ai_low"] is not None
            else None
        )
        after_costs.append(dict(reference=a, variant=b, low_eur=lo, high_eur=hi))
    # The actual digest transmitted to the three production models is retained separately.
    hist_digest = {r["symbol"]: r.get("news", []) for r in t["rows"]}
    news = dict(old=hist_digest, rebuilt=display_pack(view), source_hash=t["pack_hash"])
    return dict(
        day=day,
        run=t["run"],
        time=t["at"],
        observed_end=t["end"],
        start_cash=d["cash"],
        start_nav=d["nav"],
        meta=meta,
        arms=outputs,
        differences=diff,
        after_costs=after_costs,
        news=news,
        actual_dispatch=[{"side": o.get("side"), "symbol": o.get("symbol")} for o in t["orders"]],
        paid=paid_results(t, d, paid, base, p, cfg, conv, cal, closes, fx, view, mark_fx),
    )


def persist(name, data):
    filename = name + "-" + digest(data)[:24] + ".json.gz"
    if not (OUT / filename).exists():
        atomic(OUT / filename, data)
    return filename


def run():
    guard = isolation()
    if not all(guard.values()):
        raise RuntimeError("Isolement incomplet")
    OUT.mkdir(exist_ok=True)
    STATE.mkdir(exist_ok=True)
    p = load(CODE / "protocol.json")
    labels = load(CODE / "labels.json")
    cfg = load(BASE_CODE / "scenario.json")
    conv = load(BASE_CODE / "execution.json")
    cal = load(ROOT / "sessions.json")
    hist = load(FEED / "historical.json.gz")
    captures = sorted((FEED / "captures").glob("*.json.gz"))
    path = captures[-1]

    def checked(path):
        x = load(path)
        if digest(x)[:16] != path.name.split("-")[-1].split(".")[0]:
            raise ValueError("Capture")
        return x

    latest = checked(path)
    base = load(ROOT / "batches" / latest["base_file"])
    articles = [r for f in latest["articles"] for r in load(FEED / "articles" / f)]
    paid = load(FEED / "paid.json") if (FEED / "paid.json").exists() else {}
    fx_date = paid.get("at", latest["at"])[:10]
    fx = recent(base.get("closing_usd", {}), fx_date)
    lot10 = historical(hist, base, latest, p, conv)
    lookup = STATE / "index.json"
    idx = load(lookup) if lookup.exists() else {}
    unseen = [f for f in captures if f.name > idx.get("last", "")]
    refs = idx.get("refs", {})
    for f in unseen:
        snap = latest if f == path else checked(f)
        for t in snap["traces"]:
            refs.setdefault(t["trace"], f.name)
    atomic(lookup, dict(last=path.name, refs=refs))
    import sqlite3

    closes = defaultdict(dict)
    with sqlite3.connect(STATE / "closing.sqlite") as db:
        db.execute(
            "create table if not exists marks(symbol text,currency text,day text,px real,primary key(symbol,currency,day))"
        )
        db.executemany(
            "insert or ignore into marks values(?,?,?,?)",
            [(x["symbol"], x["currency"], x["d"], x["px"]) for x in latest["closes"]],
        )
        for sym, ccy, day, px in db.execute("select * from marks"):
            closes[(sym, ccy)][day] = px
    cases = []
    anchors = {}
    cache = {}
    for t in latest["traces"]:
        ref = refs[t["trace"]]
        if ref not in cache:
            snap = latest if ref == path.name else checked(FEED / "captures" / ref)
            anchor = {**load(ROOT / "batches" / snap["base_file"]), **snap.get("market", {})}
            art = (
                articles
                if ref == path.name
                else [r for f in snap["articles"] for r in load(FEED / "articles" / f)]
            )
            cache[ref] = (snap, anchor, art)
        snap, anchor, art = cache[ref]
        anchors[t["run"]] = anchor
        x = measure(
            t,
            snap,
            anchor,
            art,
            p,
            cfg,
            conv,
            cal,
            closes,
            paid,
            fx,
            base.get("closing_usd", {}),
            latest["usage"],
        )
        # Retain the complete matched-day file, including rejected and unexecuted instructions.
        news = x.pop("news")
        x["news_file"] = persist("pack", news)
        cases.append(x)
    from forward_exits import simulate

    trace_by = {t["run"]: t for t in latest["traces"]}
    for x in cases:
        t = trace_by[x["run"]]
        ref = refs[t["trace"]]
        snap, anchor, art = cache[ref]
        d = dossier(t, snap, anchor, {"pack": {}}, p)
        followers = [(v, anchors[v["run"]]) for v in latest["traces"] if v["at"] > t["at"]]
        x["exit_review"] = simulate(t, d, anchor, followers, p, cfg, conv, cal)
    view = evidence(articles, latest["at"], p)
    # Freeze the annotated test at a common date before its labels fall out of the rolling news window.
    lp = STATE / ("labels-" + digest([labels, (CODE / "news_study.py").read_text()])[:16] + ".json")
    if not lp.exists():
        first = checked(captures[0])
        label_source = [row for file in first["articles"] for row in load(FEED / "articles" / file)]
        val = evidence(label_source, "2026-11-19T23:59:59+00:00", p)
        annotated = evaluate(val, label_source, labels["labels"], base)
        if annotated["missing_labels"]:
            raise ValueError("Échantillon annoté incomplet")
        atomic(lp, annotated)
    metrics = evaluate(view, articles, {}, base)
    reference = load(lp)
    metrics.update({k: reference[k] for k in ("labels", "missed", "false_positive", "missing_labels")})
    calls = [
        x
        for x in latest["usage"]
        if x["producer"] == "AG4"
        and x.get("at")
        and (moment(latest["at"]) - moment(x["at"])).total_seconds() <= 14 * 86400
    ]
    priced = [x for x in calls if x["estimate_usd_high"] is not None]
    metrics.update(
        cost=dict(
            n_calls=len(calls),
            priced=len(priced),
            usd_high=sum(x["estimate_usd_high"] for x in priced),
            covered_usd_per_event=sum(x["estimate_usd_high"] for x in priced) / len(view["events"])
            if priced and view["events"]
            else None,
            full_usd_per_event=None,
            incremental_paid_usd=0,
        )
    )
    report = dict(
        created=datetime.now(timezone.utc).isoformat(),
        at=latest["at"],
        input=path.name,
        network=guard,
        protocol=digest(p),
        exits=lot10,
        news=dict(
            metrics=metrics,
            latest=persist("pack", display_pack(view)),
            events=len(view["events"]),
            raw=view["raw"],
        ),
        cases=cases,
        ledger_runs=base.get("ledger_runs"),
        historical_votes=diagnose(hist, p),
        billing=[receipt_cost(v, fx) for v in [*paid.get("earlier", []), paid] if v.get("records")],
        fx_eur_per_usd=fx,
        fx_asof=max((k for k in base.get("closing_usd", {}) if k <= fx_date), default=None),
    )
    from acceptance import check

    report["checks"] = check(report, p, cfg, conv, cal)
    atomic(OUT / "report.json", report)
    print(
        "Sorties", lot10["evaluated"], "événements", len(view["events"]), "sessions", len(cases), flush=True
    )


if __name__ == "__main__":
    run()
