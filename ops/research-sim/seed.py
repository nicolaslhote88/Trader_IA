"""Prepare the frozen census and exchange sessions. No provider credentials or trading account IDs."""

import hashlib
import json
from pathlib import Path
import sys
import zipfile


def make(archive, directory):
    import exchange_calendars as xc

    with zipfile.ZipFile(archive) as z:
        checks = {n: h for h, n in (s.split(maxsplit=1) for s in z.read("SHA256.txt").decode().splitlines())}

        def read(name):
            raw = z.read(name + ".json")
            if hashlib.sha256(raw).hexdigest() != checks[name + ".json"]:
                raise ValueError("Empreinte audit")
            return json.loads(raw)

        facts = read("facts")
        funnel = read("funnel")
        stats = read("pillar_stats")
        by = {r["symbol"]: r for r in funnel["rows"]}
        rows = []
        for s in facts["segments"]:
            r = by.get(s["symbol"], {})
            rows.append(
                dict(
                    symbol=s["symbol"],
                    name=s["name"],
                    country=s["country"],
                    sector=s["sector"],
                    place=s["exchange"],
                    currency=s["currency"],
                    asset=s["asset_class"],
                    enabled=bool(s["enabled"]),
                    quarantine=bool(s["quarantine"]),
                    reason=s["reason"] if s["quarantine"] else None,
                    segments=sorted((s["segment"] or "").split(",")) if s["segment"] else [],
                    scoped=r.get("scope", False),
                    pre=r.get("pretradable", False),
                    tech=r.get("tech"),
                    block=r.get("reasons", []),
                )
            )
        frozen = dict(
            at=funnel["at"],
            rows=rows,
            expected=funnel["counts"],
            historical_signal=stats["funda"],
            hashes={k: checks[k + ".json"] for k in ("facts", "funnel", "pillar_stats")},
        )
    import sqlite3
    from collections import defaultdict

    folder = archive.parents[2] / "pilotage_prive" / "lot02"
    sys.path.insert(0, str(folder / "application"))
    from statements import load as active_records, canonical

    with sqlite3.connect((folder / "rapprochement.sqlite").resolve().as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        db.execute("begin")
        _, records = active_records(db)
    groups = defaultdict(list)
    for record in records:
        v = record["v"]
        if record["kind"] == "ConversionRate" and v["toCurrency"] == "EUR":
            groups[(v["fromCurrency"], v["reportDate"])].append(record)
    rates = {}
    for (ccy, dt), group in groups.items():
        entry = canonical(group)
        if entry:
            rates.setdefault(ccy, {})[dt[:4] + "-" + dt[4:6] + "-" + dt[6:]] = float(entry["v"]["rate"])
    frozen["closing_fx"] = rates
    tables = {}
    for code in ("XNYS", "XPAR", "XAMS", "XTKS", "XETR"):
        cal = xc.get_calendar(code, start="2026-01-01", end="2028-12-31")
        tables[code] = {
            str(idx.date()): [None if str(v) == "NaT" else v.isoformat() for v in row]
            for idx, row in cal.schedule[["open", "break_start", "break_end", "close"]].iterrows()
        }
    for name, data in [("audit", frozen), ("sessions", dict(version=xc.__version__, tables=tables))]:
        (directory / (name + ".json")).write_text(
            json.dumps(data, ensure_ascii=False, allow_nan=False), encoding="utf8"
        )


if __name__ == "__main__":
    dest = Path(sys.argv[2])
    dest.mkdir(parents=True, exist_ok=True)
    make(Path(sys.argv[1]), dest)
