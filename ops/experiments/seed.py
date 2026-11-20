"""Extract the November audit without exposing the source archive to the experiment service."""

import csv
import hashlib
import io
import json
import sys
import zipfile
from collections import defaultdict
from pathlib import Path
import gzip


def build(source):
    with zipfile.ZipFile(source) as z:
        manifest = {
            n: h for h, n in (s.split(maxsplit=1) for s in z.read("SHA256.txt").decode().splitlines())
        }

        def read(n):
            raw = z.read(n)
            if hashlib.sha256(raw).hexdigest() != manifest[n]:
                raise ValueError(n)
            return raw

        data = json.loads(read("facts.json"))
        market = json.loads(read("research.json"))["recent"]
        bench = json.loads(read("bench.json"))
        observations = list(csv.DictReader(io.StringIO(read("mesure_votes.csv").decode("utf-8-sig"))))
        signal = json.loads(read("pillars.json"))["ag3_v2"]
        frozen = json.loads(read("metrics.json"))["frozen"]
    prices = defaultdict(dict)
    currencies = {}
    for sym, day, px, currency in market:
        if currency in ("EUR", "USD") and px and px > 0:
            prices[sym][day] = px
            currencies[sym] = currency
    return dict(
        decisions=data["decisions"],
        votes=[
            {k: v[k] for k in ("run_id", "symbol", "side", "model_key")}
            for v in data["votes"]
            if v["executable"] and v["side"] in ("BUY", "SELL")
        ],
        models=[
            {k: v.get(k) for k in ("run_id", "model_key", "model_name", "model_id", "parse_ok")}
            for v in data["models"]
        ],
        ideas=[v for v in observations if v["h"] == "20"],
        positions=data["exposures"],
        daily=data["daily"],
        fills=data["fills"],
        buy_orders=[
            {k: o[k] for k in ("order_id", "symbol", "qty", "ts_created")}
            for o in data["orders"]
            if o["side"] == "BUY"
        ],
        prices=prices,
        currencies=currencies,
        fx={d: x[0] for d, x in bench["EURUSD=X"]["prices"].items()},
        benchmark={d: x[0] for d, x in bench["EUNL.DE"]["prices"].items()},
        signal=signal,
        universe={r["symbol"]: r["sector"] for r in data["segments"]},
        frozen=frozen,
        evaluations=[
            r
            for r in observations
            if r["side"] == "SELL" and r["status"] == "CONSENSUS_APPROVED" and r["h"] == "20"
        ],
        audit_end="2026-11-18",
        hashes={
            n: manifest[n]
            for n in [
                "facts.json",
                "research.json",
                "bench.json",
                "mesure_votes.csv",
                "pillars.json",
                "metrics.json",
            ]
        },
    )


if __name__ == "__main__":
    Path(sys.argv[2]).write_bytes(
        gzip.compress(
            json.dumps(
                build(sys.argv[1]), ensure_ascii=False, allow_nan=False, separators=(",", ":")
            ).encode(),
            mtime=0,
        )
    )
