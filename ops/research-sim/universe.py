from bisect import bisect_left
from datetime import date
from common import fresh, moment
from costs import tariff, tax_estimate

PLACES = {
    "NYSE": "XNYS",
    "NASDAQ": "XNYS",
    "SBF": "XPAR",
    "PAR": "XPAR",
    "Euronext Paris": "XPAR",
    "AEB": "XAMS",
    "Euronext Amsterdam": "XAMS",
    "Tokyo": "XTKS",
    "TSE": "XTKS",
    "XETRA": "XETR",
}


def session(place, when, cal):
    key = PLACES.get(place)
    if not key:
        return None
    t = moment(when)
    rows = cal["tables"][key]
    d = t.date().isoformat()
    if not min(rows) <= d <= max(rows):
        return None
    a = rows.get(d)
    if not a:
        return False
    return moment(a[0]) <= t < moment(a[3]) and not (a[1] and moment(a[1]) <= t < moment(a[2]))


def fx_before(rates, day):
    keys = sorted(rates)
    i = bisect_left(keys, day) - 1
    if i < 0:
        return None, None
    k = keys[i]
    return (float(rates[k]), k) if (date.fromisoformat(day) - date.fromisoformat(k)).days <= 5 else (None, k)


def type_of(symbol, asset):
    if symbol == "EUNL.DE":
        return "fonds mondial"
    if symbol in ("LVC.PA", "BX4.PA"):
        return "levier quotidien"
    return "fonds sectoriel" if asset == "ETF" else "action" if asset == "EQUITY" else "hors actions"


def prepare(batch, at, audit):
    prior = sorted(
        (e for e in batch["events"] if moment(e["end"]) <= moment(at)), key=lambda e: moment(e["end"])
    )
    quotes = {}
    rules = {}
    ids = {}
    for e in prior:
        for sym, q in e["quotes"].items():
            if q.get("conid"):
                ids[sym] = q["conid"]
                if q.get("lot_known") and q.get("step") and q.get("minimum"):
                    rules[q["conid"]] = (q["step"], q["minimum"], e["end"])
                if q.get("received") and moment(q["received"]) <= moment(at):
                    quotes[q["conid"]] = q
    perms = {p["symbol"]: p for p in batch["permissions"] if p["day"] < at[:10]}
    frames = {}
    for q in batch["frames"]:
        if moment(q["received"]) <= moment(at):
            c = q["conid"]
            if c not in frames or moment(q["received"]) > moment(frames[c]["received"]):
                frames[c] = q
    rows = []
    for master in batch["rows"]:
        r = dict(master)
        s = r["symbol"]
        p = perms.get(s, {})
        c = ids.get(s) or p.get("conid")
        perm = bool(c and p.get("conid") == c)
        q = quotes.get(c, {})
        frame = frames.get(c)
        if frame and (not q.get("received") or moment(frame["received"]) > moment(q["received"])):
            q = {**q, **frame}
        place = p["place"] if perm else r["place"]
        ccy = p["currency"] if perm else q.get("currency") or r["currency"]
        rates = (
            {**batch.get("closing_usd", {}), **batch["fx"]}
            if ccy == "USD"
            else audit.get("closing_fx", {}).get(ccy, {})
        )
        rates = {**batch.get("local_fx", {}).get(ccy, {}), **rates}
        rate, rate_day = (1, at[:10]) if ccy == "EUR" else fx_before(rates, at[:10])
        lot = rules.get(c)
        if not lot and r["country"] == "Japan" and r["asset"] == "EQUITY" and r["symbol"].endswith(".T"):
            lot = (100, 100, "JPX standard domestique")
        r.update(
            product=type_of(s, r["asset"]),
            contract=c,
            place=place,
            currency=ccy,
            price_currency=master.get("currency"),
            issuer=p.get("country") if perm else None,
            permission_day=p.get("day") if perm else None,
            lot=lot[0] if lot else None,
            minimum=lot[1] if lot else None,
            lot_source=lot[2] if lot else None,
            fx=rate,
            fx_day=rate_day,
            held=s in batch["holdings"],
            held_asof=batch["holdings_at"],
            bid=q.get("bid"),
            ask=q.get("ask"),
            quote_time=q.get("quoteTime"),
            received=q.get("received"),
            availability=q.get("ibkrDataAvailability"),
            volume=(r.get("volume") if fresh(r.get("yf_at"), at, 72 * 3600) else None),
            observed_at=batch["at"],
        )
        r["quote_currency"] = q.get("currency") or (ccy if frame and perm else None)
        rows.append(r)
    return rows


def reasons(r, at, cfg, cal, veto=False):
    out = []
    if not r.get("scoped") or not r.get("segments"):
        out.append("hors univers ou quarantaine")
    if r.get("product") != "action":
        out.append("mandat actions")
    if not r.get("base_universe"):
        out.append("présélection technique ou liquidité")
    if veto and (
        r.get("ai") in (None, "", "REJECT") or not fresh(r.get("tech_at"), at, 3600 * cfg["max_tech_age_h"])
    ):
        out.append("veto AG2 ou verdict absent/périmé")
    if not r.get("sector"):
        out.append("secteur absent")
    if (
        r.get("score") is None
        or r.get("fund_status") != "OK"
        or not fresh(r.get("fund_at"), at, cfg["max_score_age_h"] * 3600)
    ):
        out.append("score AG3 absent ou périmé")
    if not r.get("permission_day"):
        out.append("accès historique au contrat non attesté")
    if not r.get("lot") or not r.get("minimum"):
        out.append("pas ou minimum inconnu")
    if not r.get("fx"):
        out.append("change absent")
    if not session(r.get("place"), at, cal):
        out.append("séance fermée ou horaire inconnu")
    if (
        not fresh(r.get("quote_time"), at, cfg["max_quote_age_s"])
        or not fresh(r.get("received"), at, cfg["max_quote_age_s"])
        or not str(r.get("availability") or "").startswith("R")
    ):
        out.append("fourchette temps réel absente ou périmée")
    if (
        not r.get("bid")
        or not r.get("ask")
        or r["bid"] > r["ask"]
        or r.get("quote_currency") != r.get("currency")
    ):
        out.append("fourchette ou devise non vérifiée")
    elif 200 * (r["ask"] - r["bid"]) / (r["ask"] + r["bid"]) > cfg["max_spread_pct"]:
        out.append("spread excessif")
    if not r.get("volume") or r["volume"] * cfg["max_daily_volume_fraction"] < (r.get("minimum") or 1):
        out.append("capacité insuffisante")
    return out


def ticket(r, at, settings):
    # A dated last market price can value a lot indicatively outside a session.
    native = (
        r.get("ask")
        if fresh(r.get("quote_time"), at, 60) and r.get("quote_currency") == r.get("currency")
        else (
            r.get("price")
            if r.get("price_currency") == r.get("currency") and fresh(r.get("price_at"), at, 96 * 3600)
            else None
        )
    )
    if not native or not r.get("minimum") or not r.get("fx"):
        return None, None
    nominal = float(native) * r["fx"] * r["minimum"]
    fee = tariff(r["place"], r["currency"], r["minimum"], nominal, r["fx"])
    tax = tax_estimate(r["symbol"], "BUY", at[:10], r.get("issuer"), nominal)
    if fee is None:
        return nominal, None
    return nominal, nominal + fee + (
        tax if tax is not None else settings["unknown_tax_reserve"] * nominal
    ) + (settings["fx_spread_each_conversion"] * nominal if r["currency"] != "EUR" else 0)
