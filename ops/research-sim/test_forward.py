"""Exercise prospective scheduling with dated, controlled quotes. Uses only a temporary directory."""

from pathlib import Path
import sqlite3
import sys
import tempfile
from copy import deepcopy
import unittest

import worker
from common import atomic, load


class Prospective(unittest.TestCase):
    def test_first_review_retry_and_cutoff(self):
        cfg, conv, version = worker.policy()
        cal = load(Path(sys.argv[1]) / "sessions.json")
        audit = {"closing_fx": {}}
        before = "2026-11-20T16:16:30+00:00"
        after = "2026-11-20T16:19:00+00:00"
        rate = "2026-11-20T16:17:53+00:00"
        sym = "EXEMPLE"
        c = "TEST"
        master = dict(
            symbol=sym,
            name=sym,
            place="AEB",
            currency="EUR",
            country="Netherlands",
            asset="EQUITY",
            sector="Industrie",
            scoped=True,
            segments=["core"],
            base_universe=True,
            fund_status="OK",
            fund_at=before,
            fund_source="contrôle",
            tech_at=before,
            ai="APPROVE",
            score=72,
            volume=900000,
            yf_at=before,
            price_at=before,
            price=99,
        )
        src = dict(
            rows=[master],
            at=before,
            permissions=[
                dict(symbol=sym, day="2026-05-18", conid=c, country="NL", place="AEB", currency="EUR")
            ],
            events=[
                dict(
                    end="2026-11-19T16:20:00Z", quotes={sym: dict(conid=c, step=1, minimum=1, lot_known=True)}
                )
            ],
            frames=[],
            fx={},
            holdings={},
            holdings_at="2026-11-19",
        )
        post = deepcopy(src)
        post["at"] = after
        post["rows"][0]["score"] = 0
        post["frames"] = [
            dict(conid=c, received=rate, quoteTime=rate, bid=99.95, ask=100, ibkrDataAvailability="R"),
            dict(conid=c, received=after, quoteTime=after, bid=20, ask=20.01, ibkrDataAvailability="R"),
        ]
        original = worker.FEED, worker.OUT
        with tempfile.TemporaryDirectory() as folder:
            worker.FEED = worker.OUT = Path(folder)
            try:
                (worker.OUT / "revues").mkdir()
                files = []
                for i, batch in enumerate([src, post]):
                    f = Path(folder) / f"{i}.json.gz"
                    atomic(f, batch)
                    files.append((batch["at"], f))
                db = sqlite3.connect(":memory:")
                db.executescript("""create table reviews(version,day,input,file,books,result,primary key(version,day));
                                    create table gaps(version,day,reason,primary key(version,day));""")

                def run(fs):
                    worker.run_reviews(db, fs, after, cfg, conv, cal, audit, version)

                run(files[:1])
                self.assertIsNone(db.execute("select day from reviews").fetchone())
                run(files)

                def stored():
                    return db.execute("select input,file,books from reviews").fetchall()

                initial = stored()
                self.assertEqual(len(initial), 1)
                report = load(worker.OUT / initial[0][1])
                for v in report["results"]:
                    alt = v["alternatives"][0]
                    self.assertEqual(v["positions"], {sym: 14})
                    self.assertEqual(alt["score"], 72)
                    self.assertEqual(alt["order"]["price"], 100)
                    self.assertEqual(alt["order"]["at"], "2026-11-20T16:18:02+00:00")
                run(files)
                self.assertEqual(initial, stored())
                self.assertEqual(len(list((worker.OUT / "revues").iterdir())), 1)
                db.execute("delete from reviews")
                atomic(worker.FEED / "pauses.json", {"ag3": "2026-11-20"})
                run(files)
                only = load(worker.OUT / stored()[0][1])
                self.assertEqual(len(only["results"]), 3)
                self.assertNotIn("ag3", [v["variant"] for v in only["results"]])
            finally:
                worker.FEED, worker.OUT = original


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
