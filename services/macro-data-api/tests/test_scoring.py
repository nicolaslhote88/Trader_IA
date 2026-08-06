"""Tests des contrats canoniques AG5-AG8."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scoring import (
    bounded_weighted_composite,
    compute_ag5_macro,
    compute_fx_valuation,
    cot_positioning_score,
    effective_age_hours,
    rates_regime,
    score_current_account_pct_gdp,
    synthetic_usd_positioning,
)


NOW = datetime(2026, 8, 5, 12, tzinfo=timezone.utc)


def metric(value, confidence=0.9, freshness="fresh"):
    return {"value": value, "confidence": confidence, "freshness_status": freshness}


def test_missing_never_becomes_neutral_zero():
    result = bounded_weighted_composite({"a": None, "b": 0.8}, {"a": 0.5, "b": 0.5})
    assert result["score"] == 0.8
    assert result["coverage_ratio"] == 0.5
    assert result["confidence"] < 1.0
    assert result["all_aligned"] is False


def test_current_account_uses_pct_gdp_units():
    assert score_current_account_pct_gdp(5.0) > 0
    assert score_current_account_pct_gdp(-5.0) < 0
    assert score_current_account_pct_gdp(None) is None


def test_ag5_has_no_usd_directional_prior_and_excludes_stale():
    inputs = {
        "growth": metric(2.0), "growth_momentum": metric(0.2),
        "inflation": metric(2.4), "policy_rate": metric(3.5),
        "current_account_pct_gdp": metric(-2.0),
        "fiscal_balance_pct_gdp": metric(None), "unemployment_change_pp": metric(-0.1),
    }
    result = compute_ag5_macro("USD", inputs, neutral_rate={"rate_pct": 2.5, "uncertainty_pct": 1.0, "confidence": 0.35})
    assert result["entity_id"] == "USD"
    assert -1 <= result["macro_score"] <= 1
    assert "fiscal" in result["missing_inputs"]
    stale = dict(inputs)
    stale["growth"] = metric(10.0, freshness="stale")
    stale_result = compute_ag5_macro("USD", stale, neutral_rate={"rate_pct": 2.5, "uncertainty_pct": 1.0, "confidence": 0.35})
    assert "growth" in stale_result["stale_inputs"]
    assert stale_result["macro_score"] != result["macro_score"]


def test_fx_ppp_requires_spot_and_fair_value():
    missing_spot = compute_fx_valuation("EUR", nominal_carry_pct=1, real_carry_pct=0.5, spot_reference=None, ppp_fair_value=1.2)
    assert missing_spot["ppp_gap"] is None
    assert "ppp" in missing_spot["missing_inputs"]
    valid = compute_fx_valuation("EUR", nominal_carry_pct=1, real_carry_pct=0.5, spot_reference=1.1, ppp_fair_value=1.2)
    assert valid["ppp_gap"] > 0
    assert valid["valuation_score"] is not None


def test_cot_score_is_contrarian_and_usd_proxy_is_capped():
    assert cot_positioning_score(2.0) < 0
    assert cot_positioning_score(-2.0) > 0
    synthetic = synthetic_usd_positioning({"EUR": {"z_score": 1}, "JPY": {"z_score": -1}}, {"EUR": 0.6, "JPY": 0.4})
    assert synthetic["is_proxy"] is True
    assert synthetic["source"] == "CFTC_SYNTHETIC_USD_BASKET"
    assert synthetic["confidence"] <= 0.60


def test_rates_regime_uses_direction_to_name_steepening():
    bull = rates_regime(yield_2y=3.0, yield_10y=3.5, slope_change=0.3, yield_2y_change=-0.5, yield_10y_change=-0.2)
    bear = rates_regime(yield_2y=3.0, yield_10y=3.5, slope_change=0.3, yield_2y_change=0.1, yield_10y_change=0.4)
    unknown = rates_regime(yield_2y=3.0, yield_10y=3.5, slope_change=0.3)
    assert bull["curve_regime"] == "bull_steepening"
    assert bear["curve_regime"] == "bear_steepening"
    assert unknown["curve_regime"] == "unknown"


def test_effective_age_uses_oldest_truth():
    assert effective_age_hours("2026-08-05T10:00:00Z", 8, now=NOW) == 8
    assert effective_age_hours("2026-08-04T00:00:00Z", 1, now=NOW) == 36
