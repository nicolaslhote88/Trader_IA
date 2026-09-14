import sys
import textwrap
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


def load_compute_functions():
    try:
        __import__("duckdb")
    except ImportError:
        sys.modules.setdefault("duckdb", types.ModuleType("duckdb"))
    source_path = Path(__file__).resolve().parents[1] / "nodes" / "04_compute.py"
    source = source_path.read_text(encoding="utf-8").split("items = _items", 1)[0]
    namespace = {}
    exec(compile(source, str(source_path), "exec"), namespace)
    return namespace


FUNCS = load_compute_functions()


def bar(i, close=100.0, *, closed=True, high=None, low=None):
    return {
        "t": f"2026-01-{(i % 28) + 1:02d}T00:00:00Z",
        "o": close,
        "h": close + 1 if high is None else high,
        "l": close - 1 if low is None else low,
        "c": close,
        "v": 1000,
        "closed": closed,
    }


class ComputeClosedBarsTests(unittest.TestCase):
    def test_flat_series_is_neutral(self):
        result = FUNCS["compute_indicators"]([bar(i) for i in range(220)], "1d")
        self.assertEqual("OK", result["status"])
        self.assertEqual(50.0, result["indicators"]["rsi14"])
        self.assertEqual("NEUTRAL", result["signal"]["action"])
        self.assertEqual(0, result["signal"]["score"])

    def test_unclosed_bars_are_rejected(self):
        result = FUNCS["compute_indicators"]([bar(i, closed=False) for i in range(220)], "1d")
        self.assertEqual("NO_DATA", result["status"])

    def test_invalid_ohlc_is_rejected(self):
        result = FUNCS["compute_indicators"]([bar(i, high=99, low=98) for i in range(220)], "1d")
        self.assertEqual("NO_DATA", result["status"])

    def test_prefilter_requires_ok_daily_context(self):
        h1 = {"status": "OK", "signal": {"action": "BUY", "score": 3}, "indicators": {"rsi14": 40}}
        d1 = {"status": "STALE", "indicators": {"last_close": 110, "sma200": 100}}
        self.assertEqual((False, "NO_OR_STALE_D1_DATA"), FUNCS["pre_filter"](h1, d1))

    def test_volatility_annualization_uses_market_session(self):
        periods = FUNCS["annualization_periods"]
        self.assertEqual(252.0 * 6.5, periods("1h", "NYSE", "EQUITY"))
        self.assertEqual(252.0 * 8.5, periods("1h", "Euronext Paris", "EQUITY"))
        self.assertEqual(365.0 * 24.0, periods("1h", "", "CRYPTO"))
        self.assertEqual(365.0, periods("1d", "", "CRYPTO"))

    def test_intraday_ai_window_is_soft_but_96h_gate_is_hard(self):
        check = FUNCS["check_freshness"]
        now = datetime(2026, 8, 6, 11, 0, tzinfo=timezone.utc)

        self.assertEqual(
            (False, 4.0, "SOFT_STALE"),
            check(now - timedelta(hours=4), "1h", now),
        )
        self.assertEqual(
            (False, 15.0, "SOFT_STALE"),
            check(now - timedelta(hours=15), "1h", now),
        )
        self.assertEqual(
            (False, 97.0, "STALE"),
            check(now - timedelta(hours=97), "1h", now),
        )

    def test_daily_96h_gate_is_unchanged(self):
        check = FUNCS["check_freshness"]
        now = datetime(2026, 8, 6, 11, 0, tzinfo=timezone.utc)

        self.assertEqual((True, 95.0, "FRESH"), check(now - timedelta(hours=95), "1d", now))
        self.assertEqual((False, 97.0, "STALE"), check(now - timedelta(hours=97), "1d", now))

    def test_node_runtime_uses_current_item_context(self):
        class FakeConnection:
            def execute(self, *_args, **_kwargs):
                return self

            def close(self):
                return None

        duckdb_module = sys.modules["duckdb"]
        old_connect = getattr(duckdb_module, "connect", None)
        duckdb_module.connect = lambda *_args, **_kwargs: FakeConnection()
        source_path = Path(__file__).resolve().parents[1] / "nodes" / "04_compute.py"
        source = source_path.read_text(encoding="utf-8")
        wrapped = "def node_main(_items):\n" + textwrap.indent(source, "    ")
        namespace = {}
        try:
            exec(compile(wrapped, str(source_path), "exec"), namespace)

            payload = {
                "run_id": "test_run",
                "symbol": "AIR.PA",
                "symbol_internal": "AIR.PA",
                "symbol_yahoo": "AIR.PA",
                "asset_class": "EQUITY",
                "exchange": "Euronext Paris",
                "currency": "EUR",
                "batch_info": {
                    "start": 40,
                    "size": 1,
                    "state_key": "last_index_actions_watchlist",
                    "next_index": 80,
                },
                "h1_response": {"bars": [], "interval": "1h", "closedOnly": True},
                "d1_response": {"bars": [], "interval": "1d", "closedOnly": True},
            }
            result = namespace["node_main"]([{"json": payload}])
        finally:
            if old_connect is None:
                delattr(duckdb_module, "connect")
            else:
                duckdb_module.connect = old_connect

        self.assertEqual(1, len(result))
        self.assertEqual("ok", result[0]["json"]["_status"])
        self.assertEqual("EQUITY", result[0]["json"]["asset_class"])
        self.assertEqual("Euronext Paris", result[0]["json"]["exchange"])
        self.assertEqual(payload["batch_info"], result[0]["json"]["batch_info"])


if __name__ == "__main__":
    unittest.main()
