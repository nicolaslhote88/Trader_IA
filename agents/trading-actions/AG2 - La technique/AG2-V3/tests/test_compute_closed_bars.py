import sys
import types
import unittest
from pathlib import Path


def load_compute_functions():
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


if __name__ == "__main__":
    unittest.main()
