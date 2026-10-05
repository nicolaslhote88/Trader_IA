import unittest
from datetime import datetime, timedelta, timezone
from test_compute_closed_bars import FUNCS


class HeldReviewTests(unittest.TestCase):
    def test_neutral_held_is_reviewed_but_candidate_stays_filtered(self):
        h = {"status": "OK", "signal": {"action": "NEUTRAL"}}
        d = {"status": "OK"}
        self.assertEqual((True, "HELD_PERIODIC_REVIEW"), FUNCS["pre_filter"](h, d, True))
        self.assertEqual((False, "NEUTRAL"), FUNCS["pre_filter"](h, d))
        d["status"] = "STALE"
        self.assertFalse(FUNCS["pre_filter"](h, d, True)[0])

    def test_session_reference_accepts_weekend_not_missed_sessions(self):
        now = datetime(2026, 10, 5, 7, 3, tzinfo=timezone.utc)
        last = datetime(2026, 10, 2, 19, 30, tzinfo=timezone.utc)
        ref = {"method": "REGULAR_SESSION_V1", "asOf": now.isoformat(), "latestExpectedBarTime": last.isoformat()}
        self.assertTrue(FUNCS["check_freshness"](last, "1h", now, ref)[0])
        self.assertFalse(FUNCS["check_freshness"](last-timedelta(days=1), "1h", now, ref)[0])
        self.assertEqual("STALE", FUNCS["check_freshness"](last-timedelta(days=2), "1h", now, ref)[2])
        ref["asOf"] = (now-timedelta(hours=1)).isoformat()
        self.assertFalse(FUNCS["check_freshness"](last, "1h", now, ref)[0])

    def test_held_review_cache_is_bounded_and_reject_not_reused(self):
        class C:
            def __init__(self, decision, age): self.decision, self.age = decision, age
            def execute(self, sql, args): self.sql = sql; return self
            def fetchone(self):
                if "sig_hash" in self.sql:
                    return ("same", "2026-10-05", 60, '{"decision":"'+self.decision+'"}')
                return (self.age,)
        check = FUNCS["check_dedup"]
        self.assertEqual((False, "UNCHANGED_WITHIN_TTL"), check("TXN", "same", "NEUTRAL", C("WATCH", 120), True))
        self.assertEqual((True, "TTL_EXPIRED"), check("TXN", "same", "NEUTRAL", C("WATCH", 241), True))
        self.assertEqual((True, "REJECT_NOT_REUSED"), check("TXN", "same", "NEUTRAL", C("REJECT", 1), True))
