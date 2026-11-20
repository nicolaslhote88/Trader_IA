"""Read-only extract. Expose only market, decision and outcome fields to the offline worker."""

from datetime import datetime, timezone
import gzip
import json
from pathlib import Path
import sqlite3
import duckdb
from common import atomic, digest, load, moment, fresh

DB = Path("/local-files/duckdb")
FEED = Path("/var/lib/trader-research-feed")


def query(name, sql):
    c = duckdb.connect(str(DB / (name + ".duckdb")), read_only=True, config={"threads": 1})
    try:
        q = c.execute(sql)
        cols = [x[0] for x in q.description]
        return [dict(zip(cols, r)) for r in q.fetchall()]
    finally:
        c.close()


def sqlread(path):
    return sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=8)


def subset(r, keys):
    return {k: r.get(k) for k in keys.split()}


def archive():
    j = sqlread("/var/lib/trader-ag1-journal/journal.sqlite")
    e = sqlread("/var/lib/trader-economic/trace.sqlite")
    reports = load(DB / "economic_reporting/report.json")
    account = load(DB / "lot03_reporting/compte.json")
    sends = {r["order"]: r for r in reports["orders"]["orders"]}
    facts = {r["execution"]: r for r in account["trades"]}
    packs = {(str(i), c): json.loads(b) for i, c, b in e.execute("select eid,contract,body from packs")}

    def docs(k):
        return {
            n: (h, json.loads(gzip.decompress(raw)))
            for n, h, raw in j.execute(
                "select name,hash,data from refs join objects using(hash) where kind=?", (k,)
            )
        }

    ledgers = docs("ledger")
    events = []
    for eid, (h, t) in docs("trace").items():
        run = t.get("context", {}).get("runId")
        ledger = ledgers.get(run, (None, {}))[1]
        stage = t.get("stages", {})
        inputs = stage.get("input_pack", [])
        if not inputs:
            continue
        pack = inputs[0]
        positions = pack["portfolio_pack"]
        observations = {}
        for r in pack["opportunity_pack"]["rows"]:
            liq = r.get("liquidity") or {}
            cid = str(liq.get("conid") or "")
            old = packs.get((eid, cid), {})
            x = subset(liq, "quoteTime quoteSource bid ask currency volume fxRateToEUR ibkrDataAvailability")
            x.update(
                conid=cid,
                received=old.get("observed"),
                step=r.get("quantity_increment"),
                minimum=r.get("min_quantity"),
                lot_known=(r.get("execution_constraints") or {}).get("quantityRuleKnown"),
                symbol=r["symbol"],
            )
            observations[r["symbol"]] = x
        placed = [a for group in stage.get("dispatch", []) for a in group.get("orders", [])]
        orders = []
        for o in placed:
            oid = o.get("orderId")
            report = sends.get(oid, {})
            linked = next((x for x in ledger.get("orders", []) if x["order_id"] == oid), {})
            fills = [facts.get(f.get("execution_id")) for f in linked.get("fills", [])]
            fills = [subset(f, "qty price fee currency rate") for f in fills if f]
            orders.append(
                dict(
                    id=oid,
                    symbol=o.get("symbol"),
                    side=o.get("side"),
                    qty=o.get("quantity"),
                    limit=o.get("limitPrice"),
                    kind=o.get("orderType"),
                    sent=report.get("sent"),
                    place=report.get("place"),
                    actual=fills,
                )
            )
        events.append(
            dict(
                eid=eid,
                run=run,
                end=t["ended_utc"],
                input_hash=digest(inputs),
                trace_hash=h,
                ledger_hash=ledgers.get(run, (None,))[0],
                responses=digest(t.get("outputs", {})),
                model_count=len(ledger.get("models", [])),
                complete=bool(t.get("published_parameters_verified")),
                cash=positions.get("cashEUR"),
                positions={x["symbol"]: x["quantity"] for x in positions.get("positions", [])},
                quotes=observations,
                orders=orders,
                dispatch_present="dispatch" in stage,
            )
        )
    reg = {str(r["contract"]): r["place"] for r in sends.values() if r.get("contract") and r.get("place")}
    bundle = load("/var/lib/trader-economic/evidence.json")["flex"]
    permissions = {}
    for f in account["trades"]:
        if f["side"] != "BUY":
            continue
        cid = str(f["conid"])
        meta = bundle["meta"].get(f["execution"], {})
        date = f["date"]
        day = date[:4] + "-" + date[4:6] + "-" + date[6:]
        # The evidence establishes historical market access after the trade day, not an account permission today.
        v = dict(
            conid=cid,
            symbol=f["symbol"],
            currency=f["currency"],
            place=meta.get("listingExchange") or reg.get(cid),
            country=meta.get("issuerCountryCode"),
            day=day,
        )
        if cid not in permissions or permissions[cid]["day"] > day:
            permissions[cid] = v
    qrows = [
        dict(
            conid=c,
            received=moment(ts).isoformat(),
            quoteTime=moment(t).isoformat() if t else None,
            bid=b,
            ask=a,
            ibkrDataAvailability=av,
        )
        for c, ts, t, b, a, av in e.execute(
            "select contract,observed,stamp,bid,ask,availability from quotes where observed>=strftime('%s','now','-4 days')+0 order by observed,contract"
        )
    ]
    j.close()
    e.close()
    partial = []
    for lot in account["lot_comparison"]:
        if float(lot["remaining"]) <= 0 or not lot["matched_sales"]:
            continue
        for match in account["closed"]:
            if match["buy"] != lot["buy"]:
                continue
            trade = facts.get(match["sell"], {})
            buy = facts.get(match["buy"], {})
            partial.append(
                dict(
                    symbol=lot["symbol"],
                    date=match["date"],
                    sold=float(match["qty"]),
                    initial=float(buy["qty"]),
                    price=float(trade["price"]),
                    currency=trade["currency"],
                    remaining=float(lot["remaining"]),
                )
            )
    return dict(
        events=sorted(events, key=lambda x: (x["end"], x["eid"])),
        partial=sorted(partial, key=lambda x: (x["date"], x["symbol"])),
        ledger_runs=sum(bool(d.get("models")) for _, d in ledgers.values()),
        permissions=[permissions[k] for k in sorted(permissions)],
        holdings={r["symbol"]: r["qty"] for r in account["positions"]},
        holdings_at=account["end"],
        nav=float(account["close"]["total"]),
        fx=bundle["fx"],
        frames=qrows,
        bench=[subset(r, "date world matched basket account") for r in reports["performance"]["series"]],
    )


def current():
    master = query(
        "ag2_v3",
        """select u.symbol,u.name,u.asset_class asset,u.exchange place,u.country,u.currency,u.sector,u.enabled,
        u.symbol_yahoo alias,coalesce(q.active,false) quarantine,q.reason,
        (select string_agg(distinct segment,',' order by segment) from universe_segments s where s.symbol=u.symbol and s.active) seg
        from universe u left join universe_quarantine q using(symbol) order by u.symbol""",
    )
    tech = {
        r["symbol"]: r
        for r in query(
            "ag2_v3",
            """select symbol,h1_status,d1_status,h1_closed_only,d1_closed_only,
        cast(h1_date as varchar) h1,cast(d1_date as varchar) d1,cast(workflow_date as varchar) tech_at,
        data_age_h1_hours ha,data_age_d1_hours da,ai_decision ai from v_latest_signals""",
        )
    }
    quotes = {
        r["symbol"]: r
        for r in query(
            "yf_enrichment_v1",
            """select symbol,cast(fetched_at as varchar) yf_at,quote_ok,
        regular_market_price price,spread_pct spread,volume,cast(regular_market_time as varchar) price_at,
        bid,ask,market_state from v_latest_symbol_enrichment""",
        )
    }
    scores = {
        r["symbol"]: r
        for r in query(
            "ag3_v2",
            """select symbol,score,cast(fetched_at as varchar) fund_at,status fund_status,
        source fund_source from fundamentals_triage_history where fetched_at is not null
        qualify row_number() over(partition by symbol order by fetched_at desc,updated_at desc,record_id desc)=1""",
        )
    }
    at = datetime.now(timezone.utc).isoformat()
    for r in master:
        s = r["symbol"]
        r.update(tech.get(s, {}))
        r.update(quotes.get(s, {}))
        r.update(scores.get(s, {}))
        r["segments"] = (r.pop("seg") or "").split(",")
        r["segments"] = [x for x in r["segments"] if x]
        scope = bool(
            r["enabled"]
            and r["asset"] in ("EQUITY", "ETF")
            and not str(r.get("alias")).endswith("=X")
            and not r["quarantine"]
        )
        tech_ok = (
            r.get("h1_status") == r.get("d1_status") == "OK"
            and r.get("h1_closed_only")
            and r.get("d1_closed_only")
            and fresh(r.get("h1"), at, 96 * 3600)
            and fresh(r.get("d1"), at, 96 * 3600)
            and max(r.get("ha") or 0, r.get("da") or 0) <= 96
        )
        liq = bool(
            fresh(r.get("yf_at"), at, 72 * 3600)
            and r.get("quote_ok")
            and (r.get("price") or 0) > 0
            and (r.get("spread") is not None or (r.get("volume") or 0) >= 5000)
        )
        r.update(
            scoped=scope,
            tech=bool(tech_ok),
            base_universe=bool(scope and r["segments"] and tech_ok and liq),
            pre=bool(scope and r["segments"] and tech_ok and liq and str(r.get("ai")).upper() != "REJECT"),
        )
    return at, master


def main():
    extra = archive()
    at, rows = current()
    closing = query(
        "ag2_v3",
        """select symbol,cast(cast(d1_date as date) as varchar) as "day",currency,d1_last_close price,
        cast(workflow_date as varchar) seen from technical_signals where d1_date>=date '2026-11-20'
        and d1_date>=current_date-interval 10 day and cast(workflow_date as date)>cast(d1_date as date)
        and d1_closed_only and d1_status='OK' and d1_last_close>0
        qualify row_number() over(partition by symbol,cast(d1_date as date) order by workflow_date, id)=1""",
    )
    market = load("/var/lib/trader-economic/market.json")["market"]
    fx_market = market.get("EURUSD=X", [None, {}])[1]
    local_fx = {}
    for x in query(
        "ag2_v3",
        """select t.quote_ccy ccy,cast(cast(t.d1_date as date) as varchar) as "day",t.d1_last_close px
         from technical_signals t join universe u on u.symbol=t.symbol
         where t.base_ccy='EUR' and u.symbol_yahoo like '%=X' and t.d1_closed_only and t.d1_status='OK'
         and t.d1_last_close>0 and t.d1_date>=current_date-interval 10 day and cast(t.d1_date as date)<current_date
         qualify row_number() over(partition by t.quote_ccy,cast(t.d1_date as date) order by t.workflow_date,t.id)=1""",
    ):
        local_fx.setdefault(x["ccy"], {})[x["day"]] = 1 / x["px"]
    data = dict(
        at=at,
        rows=rows,
        closes=closing,
        local_fx=local_fx,
        closing_usd={k: 1 / v for k, v in fx_market.items() if k >= "2026-11-13" and v and v > 0},
        **extra,
    )
    path = FEED / "batches" / (at[:19].replace(":", "-") + "-" + digest(data)[:16] + ".json.gz")
    atomic(path, data)
    print("Observation", at, len(rows), "sources", len(extra["events"]), "traces")


if __name__ == "__main__":
    main()
