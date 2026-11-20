"""Extract minimised evidence from the active lot 2 import, keeping revisions and dates."""
import argparse
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import zipfile


def fields(row, names):
    return {k: row.get(k) for k in names.split()}


def prepare(directory, archive):
    sys.path.insert(0, str(directory / "application"))
    from statements import load, events, canonical, day
    from reference import refs
    from reconcile import chosen_section
    frozen, ref = refs(directory / "references")
    state = json.loads((directory / "etat.json").read_bytes())
    with sqlite3.connect((directory / "rapprochement.sqlite").as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        db.execute("begin")
        periods, rows = load(db)
        sources = sorted({h["source"] for h in periods})
        for s in db.execute("select id,raw from sources where active=1"):
            if hashlib.sha256(s["raw"]).hexdigest() != s["id"]:
                raise ValueError("Empreinte du relevé invalide")
        from reconcile import report
        outcome = report(db, frozen, ref)
        imports = [(s["id"], "actif" if s["active"] else "retiré") for s in db.execute("select id,active from sources order by id")]
        generation = hashlib.sha256(json.dumps([state["version"], ref["captured"], imports, outcome, state["incidents_import"]], sort_keys=True, default=str, ensure_ascii=False).encode()).hexdigest()
    if (generation != state["generation"] or state["observation"] != ref["captured"]
            or state["imports_actifs"] != len(sources) or any(p["currency"] != "EUR" for p in periods)):
        raise ValueError("Références et publication du lot 2 désynchronisées")
    selected, issues = events(rows)
    if any(t == "Trade" for t, _, _ in issues):
        raise ValueError("Révision d’exécution ambiguë")
    trades = []
    for r in selected:
        v = r["v"]
        if r["kind"] == "Trade" and v.get("assetCategory") != "CASH":
            if v.get("assetCategory") != "STK" or Decimal(v.get("multiplier") or "1") != 1:
                raise ValueError("Instrument non pris en charge par le FIFO actions")
            trades.append(dict(fields(v, "tradeID ibExecID conid symbol buySell quantity tradePrice tradeDate dateTime currency fxRateToBase ibCommission ibCommissionCurrency"), proof=r["source"], line=r["line"]))
    with zipfile.ZipFile(archive) as z:
        fn = "one_transaction.json"
        sha = {name: digest for digest, name in (l.split(maxsplit=1) for l in z.read("SHA256.txt").decode().splitlines())}
        raw = z.read(fn)
        if hashlib.sha256(raw).hexdigest() != sha[fn] or sha["broker.json"] != frozen["sources"]["broker"]:
            raise ValueError("Archive de transactions incohérente")
        pa = json.loads(raw)
        if pa["currency"] != "EUR" or pa["http"] != 200:
            raise ValueError("Réponse PortfolioAnalyst invalide")
    rates, equity = defaultdict(list), defaultdict(list)
    for r in rows:
        v = r["v"]
        if r["kind"] == "ConversionRate" and v.get("toCurrency") == "EUR":
            rates[v["reportDate"] + "/" + v["fromCurrency"]].append(r)
        if r["kind"] == "EquitySummaryByReportDateInBase" and v.get("currency") == "EUR" and not v.get("model"):
            equity[day(v["reportDate"])].append(r)
    closes = {}
    for d, choices in equity.items():
        record = canonical(choices)
        if record and d < datetime.now().astimezone().date().isoformat():
            closes[d] = fields(record["v"], "cash stock total")
    end = max(closes)
    trades = [v for v in trades if day(v["tradeDate"]) <= end]
    intervals = sorted((p["start"],p["end"]) for p in periods)
    from datetime import date, timedelta
    if intervals[0][0] > min(ref["nav"]):
        raise ValueError("Historique Flex incomplet depuis le premier point du compte")
    covered = date.fromisoformat(intervals[0][1])
    for begin, finish in intervals[1:]:
        if date.fromisoformat(begin) > covered + timedelta(days=1):
            # Non-trading dates need no statement when the daily PortfolioAnalyst series has no new observation.
            if any(covered.isoformat() < dt < begin for dt in ref["nav"]):
                raise ValueError("Séance manquante dans les relevés Flex")
        covered = max(covered, date.fromisoformat(finish))
    final = [p for p in periods if p["end"] == end]
    hold = chosen_section(final, rows, "OpenPosition", "OpenPositions")
    if not hold:
        raise ValueError("Inventaire Flex de clôture indisponible")
    positions = [fields(v, "conid symbol position currency markPrice positionValue fxRateToBase") for v in hold["v"]["rows"]]
    rates = {k: entry["v"]["rate"] for k, group in rates.items() if (entry := canonical(group))}
    # External cash flows only, from active broker movements.
    from bridge import category
    flows = [r["v"] for r in selected if r["kind"] == "CashTransaction" and category(r["v"]) == "depositsWithdrawals"]
    net = sum((Decimal(v["amount"]) * Decimal(v["fxRateToBase"]) for v in flows if day(v["reportDate"]) <= end), Decimal(0))
    taxes = [r["v"] for r in selected if r["kind"] == "StatementOfFundsLine" and r["v"].get("activityCode") == "TTAX" and r["v"].get("currency") != "BASE_SUMMARY"]
    tax = sum((Decimal(v["amount"]) * Decimal(v["fxRateToBase"]) for v in taxes if day(v["reportDate"]) <= end), Decimal(0))
    return dict(account=ref["account"], periods=[fields(h,"start end source") for h in periods], trades=trades,
                rates=rates, precise=pa["trades"], precise_hash=sha[fn], closes=closes, end=end, positions=positions,
                observation=ref["positions"][-1], series=[v for k,v in sorted(ref["nav"].items()) if k <= end],
                external=str(net), taxes=str(tax), state=fields(state,"generation residu_historique_eur residu_restant_eur etat_residu"),
                observation_at=ref["captured"])


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("lot02", type=Path)
    ap.add_argument("archive", type=Path)
    ap.add_argument("output", type=Path)
    args = ap.parse_args()
    data = prepare(args.lot02.resolve(), args.archive)
    args.output.write_text(json.dumps(data, ensure_ascii=False, allow_nan=False), encoding="utf-8")
