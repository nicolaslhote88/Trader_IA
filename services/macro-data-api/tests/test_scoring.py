"""Tests unitaires pour le module de scoring des 3 piliers."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scoring import (
    score_gdp_growth,
    score_inflation,
    score_cb_policy,
    score_current_account,
    compute_macro_score,
    compute_carry_score,
    compute_all_pillar_scores,
    clamp,
)


def test_clamp():
    assert clamp(2.0) == 1.0
    assert clamp(-2.0) == -1.0
    assert clamp(0.5) == 0.5


def test_score_gdp_growth():
    assert score_gdp_growth(4.0, 0.5) > 0.8   # forte croissance
    assert score_gdp_growth(-2.0, -0.3) < -0.7  # récession
    assert score_gdp_growth(1.0, 0.0) > 0       # croissance modérée
    assert score_gdp_growth(None, None) == 0.0


def test_score_inflation():
    assert score_inflation(2.0, 4.5) > 0       # parfaitement à la cible
    assert score_inflation(8.0, 5.0) < -0.5    # inflation élevée
    assert score_inflation(-0.5, 0.0) < 0      # déflation légère
    assert score_inflation(None, None) == 0.0


def test_score_cb_policy():
    assert score_cb_policy(4.5, "USD", 3.0) > 0  # au-dessus du neutre (2.5)
    assert score_cb_policy(0.1, "JPY", 0.5) >= 0  # Japon, neutre = 0
    assert score_cb_policy(-0.5, "EUR", 1.0) < 0  # en dessous du neutre
    assert score_cb_policy(None, "USD", None) == 0.0


def test_score_current_account():
    assert score_current_account(100.0) == 1.0   # gros excédent (Japon)
    assert score_current_account(25.0) > 0
    assert score_current_account(-200.0) == -1.0  # gros déficit (US)
    assert score_current_account(-50.0) < 0
    assert score_current_account(None) == 0.0


def test_compute_macro_score_usd_bearish():
    """USD avec déficit massif et croissance faible doit scorer négatif."""
    indicators = [
        {"currency": "USD", "indicator": "gdp_growth_qoq", "value": -0.5},
        {"currency": "USD", "indicator": "gdp_momentum", "value": -0.3},
        {"currency": "USD", "indicator": "cpi_yoy", "value": 4.5},
        {"currency": "USD", "indicator": "current_account_bn_usd", "value": -200.0},
    ]
    policy_rates = [{"currency": "USD", "rate_pct": 4.5}]
    result = compute_macro_score(indicators, policy_rates)
    assert "USD" in result
    # Déficit massif et récession doivent dominer → score négatif
    assert result["USD"]["macro_score"] < 0.0


def test_compute_macro_score_jpy_surplus():
    """JPY avec gros excédent courant doit scorer positivement sur CA."""
    indicators = [
        {"currency": "JPY", "indicator": "current_account_bn_usd", "value": 60.0},
        {"currency": "JPY", "indicator": "cpi_yoy", "value": 2.0},
        {"currency": "JPY", "indicator": "gdp_growth_qoq", "value": 2.0},
    ]
    policy_rates = [{"currency": "JPY", "rate_pct": 0.5}]
    result = compute_macro_score(indicators, policy_rates)
    assert "JPY" in result
    assert result["JPY"]["macro_ca_score"] > 0.5  # CA excédent fort


def test_compute_carry_score():
    """Devise avec taux le plus élevé doit avoir le meilleur carry score."""
    policy_rates = [
        {"currency": "USD", "rate_pct": 5.0},
        {"currency": "JPY", "rate_pct": 0.1},
        {"currency": "EUR", "rate_pct": 3.0},
        {"currency": "GBP", "rate_pct": 4.5},
    ]
    result = compute_carry_score(policy_rates)
    assert result.get("USD", 0) > result.get("JPY", 1)  # USD meilleur carry que JPY
    assert result.get("JPY", 0) < 0  # JPY carry négatif


def test_all_pillar_scores_range():
    """Tous les scores doivent être dans [-1, +1]."""
    from scoring import compute_macro_score, compute_valuation_scores
    indicators = [
        {"currency": "USD", "indicator": "gdp_growth_qoq", "value": 2.5},
        {"currency": "USD", "indicator": "cpi_yoy", "value": 3.0},
        {"currency": "USD", "indicator": "current_account_bn_usd", "value": -150.0},
    ]
    policy_rates = [{"currency": "USD", "rate_pct": 5.25}]
    cpi_history = [
        {"currency": "USD", "indicator": "cpi_yoy", "value": 3.0},
        {"currency": "EUR", "indicator": "cpi_yoy", "value": 2.5},
    ]
    macro = compute_macro_score(indicators, policy_rates)
    valuation = compute_valuation_scores(policy_rates, cpi_history)
    for ccy, scores in macro.items():
        assert -1.0 <= scores["macro_score"] <= 1.0, f"Macro score out of range for {ccy}"
    for ccy, scores in valuation.items():
        assert -1.0 <= scores["valuation_score"] <= 1.0, f"Valuation score out of range for {ccy}"


class FakeMacroDB:
    def __init__(self, *, include_mxn=True, include_sek_positioning=False):
        self.include_mxn = include_mxn
        self.include_sek_positioning = include_sek_positioning

    def get_indicators(self, currency=None, indicator=None):
        rows = [
            {"currency": "USD", "indicator": "gdp_growth_qoq", "value": 2.0},
            {"currency": "USD", "indicator": "gdp_momentum", "value": 0.1},
            {"currency": "USD", "indicator": "cpi_yoy", "value": 3.0},
            {"currency": "USD", "indicator": "current_account_bn_usd", "value": -120.0},
            {"currency": "MXN", "indicator": "gdp_growth_qoq", "value": 2.5},
            {"currency": "MXN", "indicator": "gdp_momentum", "value": 0.2},
            {"currency": "MXN", "indicator": "cpi_yoy", "value": 4.0},
            {"currency": "MXN", "indicator": "unemployment_pct", "value": 3.0},
            {"currency": "SEK", "indicator": "gdp_growth_qoq", "value": 1.0},
            {"currency": "SEK", "indicator": "cpi_yoy", "value": 2.2},
        ]
        if indicator:
            rows = [r for r in rows if r["indicator"] == indicator]
        if currency:
            rows = [r for r in rows if r["currency"] == currency]
        return rows

    def get_latest_policy_rates(self):
        return [
            {"currency": "USD", "rate_pct": 5.0},
            {"currency": "EUR", "rate_pct": 2.0},
            {"currency": "JPY", "rate_pct": 0.1},
            {"currency": "GBP", "rate_pct": 4.0},
            {"currency": "MXN", "rate_pct": 11.0},
            {"currency": "SEK", "rate_pct": 2.5},
        ]

    def get_latest_cot(self):
        rows = [
            {
                "currency": "MXN",
                "positioning_score": 0.4,
                "net_z_score": -0.8,
                "crowded_flag": False,
                "confidence": "high",
            }
        ]
        if self.include_sek_positioning:
            rows.append({
                "currency": "SEK",
                "positioning_score": 0.1,
                "net_z_score": 0.0,
                "crowded_flag": False,
                "confidence": "medium",
            })
        return rows

    def get_latest_yield_curve(self):
        return [
            {"currency": "MXN", "yield_2y_pct": 10.5, "yield_10y_pct": 9.5},
            {"currency": "SEK", "yield_2y_pct": 2.2, "yield_10y_pct": 2.7},
        ]


def test_mxn_scores_when_all_input_families_are_complete():
    scores = compute_all_pillar_scores(FakeMacroDB())
    mxn = next(s for s in scores if s["currency"] == "MXN")
    assert mxn["score_status"] == "scored"
    assert mxn["data_completeness"] == "complete"
    assert mxn["confidence_floor"] in ("medium", "high")
    assert mxn["composite_score"] is not None


def test_sek_is_data_incomplete_without_positioning_proxy():
    scores = compute_all_pillar_scores(FakeMacroDB())
    sek = next(s for s in scores if s["currency"] == "SEK")
    assert sek["score_status"] == "data_incomplete"
    assert sek["composite_score"] is None
    assert "positioning" in sek["missing_inputs"]
