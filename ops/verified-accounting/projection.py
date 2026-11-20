"""Passive accounting projection. All currency arithmetic uses Decimal until UI presentation."""
from collections import defaultdict, deque
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory, NamedTemporaryFile


def dec(x):
    return Decimal(str(x)) if x not in (None, "") else None


def difference(a, b):
    return a - b if a is not None and b is not None else None


def positive(x):
    n = dec(x)
    return n if n is not None and n.is_finite() and n > 0 else None


def precise_key(r):
    _, month, day, _, _, year = r["date"].split()
    month_num = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split().index(month) + 1
    return (f"{year}{month_num:02d}{int(day):02d}", str(r["conid"]), r["type"].upper(), r["cur"], abs(dec(r["qty"])), dec(r["pr"]))


def trade_key(v):
    return (v["tradeDate"], v["conid"], v["buySell"], v["currency"], abs(dec(v["quantity"])), dec(v["tradePrice"]))


def normalize(v, old, data, pa):
    date, conid, side, currency, qty, price = trade_key(v)
    native = qty * price
    rate = dec(1) if currency == "EUR" else positive(v.get("fxRateToBase"))
    source = "Identité EUR" if currency == "EUR" else "Flex journalier"
    evidence = v["proof"]
    if pa:
        better = positive(pa["fxRate"])
        signed = -native if side == "BUY" else native
        if better and abs(signed * better - dec(pa["amt"])) < Decimal("0.01"):
            rate, source, evidence = better, "PortfolioAnalyst transaction", data["precise_hash"]
    if not rate:
        source = "À rapprocher"
    fee_currency = v["ibCommissionCurrency"]
    fee_native = dec(v["ibCommission"])
    fee_rate = dec(1) if fee_currency == "EUR" else rate if fee_currency == currency else positive(data["rates"].get(date + "/" + (fee_currency or "")))
    # Commission debits are negative in Flex. Rebates keep their opposite sign.
    fee = -fee_native * fee_rate if fee_native is not None and fee_rate else None
    nominal = native * rate if rate else None
    old_price = dec(old.get("price"))
    old_nominal = old_price * qty if old_price is not None else None
    old_fee = dec(old.get("fees_eur"))
    return dict(execution=v["ibExecID"], trade=v["tradeID"], conid=conid, symbol=old.get("symbol") or v["symbol"],
                date=date, time=v["dateTime"], side=side, qty=qty, currency=currency, account_currency="EUR",
                price=price, native=native, rate=rate, rate_date=date, source=source, evidence=evidence,
                flex=v["proof"], line=v["line"], fee_currency=fee_currency, commission_signed=fee_native,
                fee_rate=fee_rate, fee_rate_date=date,
                fee_rate_source=("À rapprocher" if not fee_rate else "Identité EUR" if fee_currency == "EUR"
                                 else source if fee_currency == currency else "Flex ConversionRate journalier"),
                fee=fee, nominal=nominal, old_nominal=old_nominal, old_rate=dec(old.get("fx_rate_eur")),
                old_fee=old_fee, old_fee_currency=old.get("commission_ccy"), delta=difference(nominal, old_nominal))


def unit_cost(r, old=False):
    nominal, fee = (r["old_nominal"], r["old_fee"]) if old else (r["nominal"], r["fee"])
    if nominal is None or fee is None:
        return None
    return (nominal + fee if r["side"] == "BUY" else nominal - fee) / r["qty"]


def fifo(trades):
    book = defaultdict(deque)
    closed = []
    for r in trades:
        lots = book[r["conid"]]
        qty = r["qty"]
        if qty <= 0 or r["side"] not in ("BUY", "SELL"):
            raise ValueError("Sens ou quantité FIFO invalide")
        unit, old = unit_cost(r), unit_cost(r, True)
        if r["side"] == "BUY":
            lots.append(dict(buy=r["execution"], date=r["date"], symbol=r["symbol"], qty=qty, unit=unit, old_unit=old))
            continue
        while qty > 0:
            if not lots:
                raise ValueError("Vente sans achat couvert par les relevés")
            lot = lots[0]
            part = min(qty, lot["qty"])
            gain, old_gain = difference(unit, lot["unit"]), difference(old, lot["old_unit"])
            closed.append(dict(symbol=r["symbol"], date=r["date"], buy=lot["buy"], sell=r["execution"],
                               qty=part, realized=gain*part if gain is not None else None,
                               old_realized=old_gain*part if old_gain is not None else None))
            qty -= part
            lot["qty"] -= part
            if not lot["qty"]:
                lots.popleft()
    return book, closed


def total(values):
    values = list(values)
    return None if None in values else sum(values, Decimal(0))


def summarize(trades, closed, field):
    groups = defaultdict(list)
    gains = defaultdict(list)
    for r in trades:
        groups[r["date"][:6] if field == "month" else r["symbol"]].append(r)
    for r in closed:
        gains[r["date"][:6] if field == "month" else r["symbol"]].append(r)
    result = []
    for k, rows in sorted(groups.items()):
        realized = total(x["realized"] for x in gains[k])
        old_realized = total(x["old_realized"] for x in gains[k])
        result.append(dict(key=k, count=len(rows), turnover_delta=total(r["delta"] for r in rows),
                           fees=total(r["fee"] for r in rows), realized=realized, old_realized=old_realized,
                           realized_delta=difference(realized, old_realized)))
    return result


def build(data, ledger, actual_lots, activity):
    groups = defaultdict(list)
    for p in data["precise"]:
        groups[precise_key(p)].append(p)
    frequencies = defaultdict(int)
    for t in data["trades"]:
        frequencies[trade_key(t)] += 1
    by_id = {r["execution"]: r for r in ledger if r["execution"]}
    if len(by_id) != sum(bool(r["execution"]) for r in ledger):
        raise ValueError("Identifiant du registre ambigu")
    trades, paired, mismatches = [], set(), []
    for t in sorted(data["trades"], key=lambda t: (t["dateTime"], int(t["tradeID"]))):
        k = trade_key(t)
        old = by_id.get(t["ibExecID"], {})
        if old and (old["conid"] != k[1] or old["side"] != k[2] or abs(dec(old["qty"])) != k[4]
                    or (old.get("raw_price") is not None and dec(old["raw_price"]) != k[5])):
            mismatches.append(t["ibExecID"])
            old = {}
        if old:
            paired.add(t["ibExecID"])
        exact = groups[k][0] if len(groups[k]) == frequencies[k] == 1 else None
        trades.append(normalize(t, old, data, exact))
    book, closed = fifo(trades)
    flex_pos = {r["conid"]:r for r in data["positions"] if dec(r["position"])}
    observed = {r["conid"]:dec(r["qty"]) for r in data["observation"]["rows"] if dec(r["qty"])}
    positions = []
    for conid in sorted(set(flex_pos) | {c for c,lots in book.items() if lots}):
        lots = book[conid]
        fp = flex_pos.get(conid, {})
        qty = total(l["qty"] for l in lots)
        cost = total(l["unit"]*l["qty"] if l["unit"] is not None else None for l in lots)
        old_cost = total(l["old_unit"]*l["qty"] if l["old_unit"] is not None else None for l in lots)
        mark, fx = dec(fp.get("markPrice")), dec(fp.get("fxRateToBase"))
        positions.append(dict(conid=conid, symbol=lots[0]["symbol"] if lots else fp["symbol"], qty=qty, flex_qty=dec(fp.get("position")),
                              observed_qty=observed.get(conid), cost=cost, pru=cost/qty if cost is not None and qty else None,
                              old_cost=old_cost, cost_delta=difference(cost, old_cost), currency=fp.get("currency"), mark=mark,
                              value=dec(fp["positionValue"])*fx if fp.get("positionValue") is not None and fx else None))
    known_currencies = defaultdict(set)
    for t in trades:
        known_currencies[t["conid"]].add(t["currency"])
    pending=[]
    for r in ledger:
        if r["execution"] not in paired:
            price, qty = dec(r["raw_price"]), dec(r["qty"])
            currencies = known_currencies[r["conid"]]
            currency = r["raw_currency"] or (next(iter(currencies)) if len(currencies) == 1 else None)
            source = "Exécution broker" if r["raw_currency"] else "Dernier contrat Flex" if currency else "Devise inconnue"
            pending.append(dict(date=r["ts"], symbol=r["symbol"], side=r["side"], qty=qty, execution=r["execution"],
                                currency=currency, currency_source=source, price=price, native=abs(qty)*price if qty is not None and price is not None else None,
                                status="À rapprocher"))
    id_map = {row["fill_id"]:row["execution"] for row in ledger}
    matched_by_buy = defaultdict(list)
    for match in closed:
        matched_by_buy[match["buy"]].append(match)
    register_totals = defaultdict(Decimal)
    lot_comparison = []
    for registered in actual_lots:
        buy = id_map.get(registered["open_fill_id"])
        matches = matched_by_buy.get(buy, [])
        amount = dec(registered["realized_pnl_eur"])
        replay = total(x["old_realized"] for x in matches)
        corrected = total(x["realized"] for x in matches)
        register_totals[registered["symbol"]] += amount or Decimal(0)
        lot_comparison.append(dict(buy=buy, symbol=registered["symbol"], remaining=registered["remaining_qty"],
                                   recorded=amount, old_fifo=replay, corrected_fifo=corrected,
                                   delta_vs_replay=difference(corrected,replay), matched_sales=len(matches)))
    return dict(schema=1, generated=datetime.now(timezone.utc).isoformat(), account=data["account"], periods=data["periods"],
                end=data["end"], close=data["closes"][data["end"]], series=data["series"], external=data["external"], taxes=data["taxes"],
                state=data["state"], observed=data["observation"]["at"], positions=positions, trades=trades, pending=pending, mismatches=mismatches,
                paired=len(paired), qty_ok=set(flex_pos)=={c for c,lots in book.items() if lots} and all(r["qty"]==r["flex_qty"] for r in positions),
                observed_ok=observed == {k:dec(v["position"]) for k,v in flex_pos.items()}, closed=closed,
                open_lots=[dict(lot,conid=c) for c,lots in book.items() for lot in lots], registered=register_totals,
                lot_comparison=lot_comparison, activity=activity,
                months=summarize(trades,closed,"month"), symbols=summarize(trades,closed,"symbol"))


def snapshot(source):
    sys.path.insert(0, "/opt/trader-ia/decision-journal/current")
    from ledger import reader
    with TemporaryDirectory() as temp, reader(Path(source), temp) as db:
        cur = db.execute("""select coalesce(nullif(c.broker_execution_id,''),json_extract_string(f.raw_fill_json,'$.ibkrFill.execution_id')) execution,
            json_extract_string(f.raw_fill_json,'$.ibkrFill.conid') conid,json_extract_string(f.raw_fill_json,'$.ibkrFill.price') raw_price,
            json_extract_string(f.raw_fill_json,'$.ibkrFill.currency') raw_currency,f.qty,f.price,f.fx_rate_eur,f.fees_eur,c.commission_ccy,
            f.fill_id,o.side,o.symbol,cast(f.ts_fill as varchar) ts from core.fills f left join core.fill_costs c using(fill_id)
            left join core.orders o on o.order_id=f.order_id order by f.ts_fill,f.fill_id""")
        keys = [c[0] for c in cur.description]
        rows = [dict(zip(keys,r)) for r in cur.fetchall()]
        cur = db.execute("select symbol,open_fill_id,remaining_qty,realized_pnl_eur from core.position_lots order by open_ts,lot_id")
        keys = [c[0] for c in cur.description]
        registered = [dict(zip(keys,r)) for r in cur.fetchall()]
        activity = dict(db.execute("""select strategy_version,cast(max(ts_start) as varchar) from core.runs
                    where strategy_version in ('strategy_v4_consensus','ag1_pf_v1_ibkr_reconcile') group by strategy_version""").fetchall())
        activity["latest_fill"] = rows[-1]["ts"] if rows else None
    return rows, registered, activity


def publish(inp, dest, ledger):
    raw = Path(inp).read_bytes()
    data = json.loads(raw)
    rows, lots, activity = snapshot(ledger)
    result = build(data, rows, lots, activity)
    result["input_hash"] = hashlib.sha256(raw).hexdigest()
    with NamedTemporaryFile(mode="w", encoding="utf8", dir=dest.parent, delete=False) as tmp:
        json.dump(result, tmp, ensure_ascii=False, default=str, allow_nan=False)
        tmp.flush()
        os.fsync(tmp.fileno())
    os.chmod(tmp.name, 0o600)
    os.replace(tmp.name, dest)
    print(f"{len(rows)} exécutions du registre, {result['paired']} appariées, {len(result['pending'])} à rapprocher")


if __name__ == "__main__":
    publish(sys.argv[1], Path(sys.argv[2]), "/local-files/duckdb/ag1_v4_consensus.duckdb")
