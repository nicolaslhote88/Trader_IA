"""Read-only AG1 readiness audit, using bar dates rather than workflow timestamps.

Run in a compatible DuckDB runtime. The report keeps the original non-quarantine
denominator and separates upstream data defects, liquidity and AI exclusions.
"""

import duckdb, json, pathlib, datetime, collections, time, argparse

parser = argparse.ArgumentParser()
parser.add_argument("--db-dir", default="/files/duckdb")
parser.add_argument("--output", type=pathlib.Path, required=True)
args = parser.parse_args()
B = args.output.parent
B.mkdir(parents=True, exist_ok=True)


def read(db, sql):
    for i in range(20):
        try:
            c = duckdb.connect(
                str(pathlib.Path(args.db_dir) / (db + ".duckdb")), read_only=True
            )
            break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("locked " + db)
    try:
        q = c.execute(sql)
        return [dict(zip([x[0] for x in q.description], r)) for r in q.fetchall()]
    finally:
        c.close()


u = read(
    "ag2_v3",
    "select u.*,coalesce(q.active,false) quarantine_active,q.reason quarantine_reason,(select string_agg(segment,',') from universe_segments s where s.symbol=u.symbol and coalesce(s.active,true)) segments from universe u left join universe_quarantine q using(symbol) where coalesce(u.enabled,true)",
)
t = {r["symbol"]: r for r in read("ag2_v3", "select * from v_latest_signals")}
y = {
    r["symbol"]: r
    for r in read("yf_enrichment_v1", "select * from v_latest_symbol_enrichment")
}
f = {
    r["symbol"]: r
    for r in read(
        "ag3_v2",
        "select symbol,max(fetched_at) fetched_at from fundamentals_snapshot group by symbol",
    )
}
now = datetime.datetime.now(datetime.timezone.utc)


def age(v):
    if v is None:
        return 1e9
    if isinstance(v, str):
        v = datetime.datetime.fromisoformat(v.replace("Z", "+00:00"))
    if v.tzinfo is None:
        v = v.replace(tzinfo=datetime.timezone.utc)
    return max(0, (now - v).total_seconds() / 3600)


rows = []
counts = collections.Counter()
loss = collections.Counter()
for r in u:
    sym = r["symbol"]
    s = t.get(sym, {})
    q = y.get(sym, {})
    stages = [
        True,
        r["asset_class"] in ("EQUITY", "ETF")
        and not str(r["symbol_yahoo"]).endswith("=X"),
        not r["quarantine_active"],
        bool(r["segments"]),
    ]
    h = max(float(s.get("data_age_h1_hours") or 0), age(s.get("h1_date")))
    d = max(float(s.get("data_age_d1_hours") or 0), age(s.get("d1_date")))
    tech = bool(
        s
        and s.get("h1_status") == "OK"
        and s.get("d1_status") == "OK"
        and s.get("h1_closed_only")
        and s.get("d1_closed_only")
        and h <= 96
        and d <= 96
    )
    ui_h = max(float(s.get("data_age_h1_hours") or 0), age(s.get("workflow_date")))
    ui_d = max(float(s.get("data_age_d1_hours") or 0), age(s.get("workflow_date")))
    ui_tech = bool(
        s
        and s.get("h1_status") == "OK"
        and s.get("d1_status") == "OK"
        and s.get("h1_closed_only")
        and s.get("d1_closed_only")
        and ui_h <= 96
        and ui_d <= 96
    )
    quote = bool(
        q
        and age(q.get("fetched_at")) <= 72
        and q.get("quote_ok")
        and (q.get("regular_market_price") or 0) > 0
    )
    liquid = quote and (
        q.get("spread_pct") is not None or (q.get("volume") or 0) >= 5000
    )
    stages += [tech, liquid, str(s.get("ai_decision") or "").upper() != "REJECT"]
    names = [
        "universe",
        "supported",
        "non_quarantine",
        "segmented",
        "tech",
        "quote_liquidity",
        "pretradable",
    ]
    ok = True
    for n, v in zip(names, stages):
        ok = ok and v
        counts[n] += int(ok)
    reasons = []
    if all(stages[:3]):
        if not s:
            reasons.append("MISSING_TECH")
        if s.get("h1_status") != "OK":
            reasons.append("H1_" + str(s.get("h1_status")))
        if s.get("d1_status") != "OK":
            reasons.append("D1_" + str(s.get("d1_status")))
        if h > 96:
            reasons.append("STALE_H1")
        if d > 96:
            reasons.append("STALE_D1")
        if not quote:
            reasons.append("QUOTE")
        if quote and not liquid:
            reasons.append("LIQUIDITY")
        if not stages[-1]:
            reasons.append("REJECT")
        loss.update(reasons)
        counts["legacy_ui_tech"] += int(ui_tech)
        counts["legacy_ui_pretradable"] += int(
            stages[3] and ui_tech and liquid and stages[-1]
        )
        counts["funda_fresh"] += int(age(f.get(sym, {}).get("fetched_at")) <= 168)
    rows.append(
        dict(
            symbol=sym,
            segments=r["segments"],
            quarantine=r["quarantine_active"],
            scope=all(stages[:3]),
            tech=tech,
            ui_tech=ui_tech,
            pretradable=ok,
            reasons=reasons,
            h1_age=h,
            d1_age=d,
            signal_age=age(s.get("workflow_date")),
            h1_status=s.get("h1_status"),
            d1_status=s.get("d1_status"),
            h1_date=s.get("h1_date"),
            d1_date=s.get("d1_date"),
            filter_reason=s.get("filter_reason"),
            ai_decision=s.get("ai_decision"),
            quote_ok=q.get("quote_ok"),
            quote_error=q.get("quote_error"),
            spread=q.get("spread_pct"),
            volume=q.get("volume"),
            funda_age=age(f.get(sym, {}).get("fetched_at")),
        )
    )
out = dict(
    at=now.isoformat(),
    counts=counts,
    losses=loss,
    rows=rows,
    ag2_runs=read("ag2_v3", "select * from run_log order by started_at desc limit 35"),
    yf_runs=read(
        "yf_enrichment_v1", "select * from run_log order by started_at desc limit 3"
    ),
    raw={"universe": u, "tech": list(t.values()), "yf": list(y.values())},
)
args.output.write_text(json.dumps(out, default=str, indent=2))
print(
    json.dumps(
        {"at": out["at"], "counts": counts, "losses": loss, "report": str(args.output)},
        default=str,
    )
)
