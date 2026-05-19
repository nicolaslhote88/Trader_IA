"""Tests pour le client COT CFTC."""

import sys
import os
import pandas as pd
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cot_client import COTClient


def test_compute_z_scores_crowded_long():
    """Une position très longue doit être détectée comme crowded long."""
    client = COTClient()
    records = []
    # Simuler 60 semaines avec net_spec ~0, puis une semaine à +100000
    for i in range(60):
        records.append({
            "report_date": f"2025-{(i % 12) + 1:02d}-01",
            "currency": "EUR",
            "net_spec": 1000 + (i * 100),
            "lev_money_long": 50000, "lev_money_short": 49000,
            "asset_mgr_long": 0, "asset_mgr_short": 0,
            "open_interest": 500000,
        })
    # Ajouter une position extrême
    records.append({
        "report_date": "2026-01-01",
        "currency": "EUR",
        "net_spec": 150000,  # très long
        "lev_money_long": 100000, "lev_money_short": 10000,
        "asset_mgr_long": 0, "asset_mgr_short": 0,
        "open_interest": 500000,
    })
    result = client.compute_z_scores(records, lookback_weeks=52)
    latest = [r for r in result if r["currency"] == "EUR" and r["report_date"] == "2026-01-01"]
    assert latest, "No result for latest EUR record"
    assert latest[0]["net_z_score"] > 1.5, "Should be crowded long"
    assert latest[0]["crowded_flag"] is True
    assert latest[0]["crowded_direction"] == "long"
    assert latest[0]["positioning_score"] < 0, "Crowded long = bearish signal"


def test_compute_z_scores_hated_short():
    """Une position très courte doit être détectée comme 'hated' → opportunité positive."""
    client = COTClient()
    records = []
    for i in range(60):
        records.append({
            "report_date": f"2025-{(i % 12) + 1:02d}-01",
            "currency": "JPY",
            "net_spec": -5000 - (i * 50),
            "lev_money_long": 10000, "lev_money_short": 15000,
            "asset_mgr_long": 0, "asset_mgr_short": 0,
            "open_interest": 300000,
        })
    # Ajouter une position extrêmement courte
    records.append({
        "report_date": "2026-01-01",
        "currency": "JPY",
        "net_spec": -120000,  # très short (tout le monde est short JPY)
        "lev_money_long": 5000, "lev_money_short": 85000,
        "asset_mgr_long": 0, "asset_mgr_short": 40000,
        "open_interest": 300000,
    })
    result = client.compute_z_scores(records, lookback_weeks=52)
    latest = [r for r in result if r["currency"] == "JPY" and r["report_date"] == "2026-01-01"]
    assert latest, "No result for latest JPY record"
    assert latest[0]["net_z_score"] < -1.5, "Should be crowded short"
    assert latest[0]["crowded_flag"] is True
    assert latest[0]["positioning_score"] > 0, "Hated = contrarian bullish signal"


def test_cot_market_mapping():
    """Vérifier que les noms de marchés CFTC sont bien mappés."""
    from cot_client import CFTC_MARKET_TO_CURRENCY
    assert "EUR" in CFTC_MARKET_TO_CURRENCY.values()
    assert "JPY" in CFTC_MARKET_TO_CURRENCY.values()
    assert "GBP" in CFTC_MARKET_TO_CURRENCY.values()
    assert "EURO FX" in CFTC_MARKET_TO_CURRENCY
    assert CFTC_MARKET_TO_CURRENCY["NZ DOLLAR"] == "NZD"


def test_parse_tff_underscored_columns_and_exchange_suffix():
    """Le rapport TFF annuel CFTC utilise des colonnes underscorees et un suffixe exchange."""
    client = COTClient()
    df = pd.DataFrame([
        {
            "Market_and_Exchange_Names": "EURO FX - CHICAGO MERCANTILE EXCHANGE",
            "Report_Date_as_YYYY-MM-DD": "2026-05-12",
            "Open_Interest_All": 1000,
            "Asset_Mgr_Positions_Long_All": 250,
            "Asset_Mgr_Positions_Short_All": 100,
            "Lev_Money_Positions_Long_All": 300,
            "Lev_Money_Positions_Short_All": 450,
        }
    ])

    records = client._parse_df(df)

    assert len(records) == 1
    assert records[0]["currency"] == "EUR"
    assert records[0]["net_spec"] == 0
