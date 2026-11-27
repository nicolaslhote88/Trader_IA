import unittest
from datetime import datetime, timezone
from app_modules.ui_kit import (
    AMBER, GREEN, GREY, RED, Signal, account_signal, campaign_signal, decision_signal,
    freshness_signal, lab_signal, limit_signal, official_signal, reconcile_signal,
)


class StatusTests(unittest.TestCase):
    now = datetime(2026, 11, 23, 13, tzinfo=timezone.utc)
    now_at = now.isoformat()

    def test_paper_and_stale_portfolio_on_monday(self):
        account = account_signal((self.now_at, "Compte réel non confirmé", None, None),
                                 [], {"paper": True}, self.now_at, self.now)
        self.assertEqual(RED, account.state)
        self.assertIn("simulation", account.detail)
        self.assertEqual(RED, decision_signal(self.now, "2026-11-20", account).state)
        self.assertEqual(RED, reconcile_signal(self.now, "2026-11-20T22:15:04+00:00").state)

    def test_missing_and_partial(self):
        self.assertEqual(GREY, official_signal(self.now, {}, {}).state)
        self.assertEqual(GREY, campaign_signal({"formal": {"sessions": 0}}).state)
        self.assertEqual(GREY, limit_signal(None, .1))
        self.assertEqual(AMBER, limit_signal(.09, .1))
        self.assertEqual(RED, limit_signal(.11, .1))
        self.assertEqual(GREEN, limit_signal(.04, .1))
        self.assertEqual(GREY, freshness_signal(self.now, [None], 75, "Source").state)
        self.assertEqual(RED, lab_signal(self.now, {"checks": {"passed": 2, "total": 3}}, {}, {}).state)

    def test_expected_previous_session_and_holiday(self):
        broker = Signal(GREEN, "Connecté")
        self.assertEqual(GREEN, decision_signal(self.now, "2026-11-20T16:10:00Z", broker).state)
        holiday = datetime(2026, 12, 25, 13, tzinfo=timezone.utc)
        self.assertEqual(GREY, decision_signal(holiday, None, broker).state)
