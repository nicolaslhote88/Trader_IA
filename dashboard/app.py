import json
import hashlib
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import gspread
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
from oauth2client.service_account import ServiceAccountCredentials

# ============================================================
# CONFIGURATION
# ============================================================

st.set_page_config(page_title="AI Trading Executor", layout="wide", page_icon="AI")

SHEET_ID = os.getenv("SHEET_ID")
CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/secrets/service_account.json")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/files/duckdb/ag2_v2.duckdb")
AG3_DUCKDB_PATH = os.getenv("AG3_DUCKDB_PATH", "/files/duckdb/ag3_v2.duckdb")
AG4_DUCKDB_PATH = os.getenv("AG4_DUCKDB_PATH", "/files/duckdb/ag4_v2.duckdb")
AG4_SPE_DUCKDB_PATH = os.getenv("AG4_SPE_DUCKDB_PATH", "/files/duckdb/ag4_spe_v2.duckdb")
YF_ENRICH_DUCKDB_PATH = os.getenv("YF_ENRICH_DUCKDB_PATH", "/files/duckdb/yf_enrichment_v1.duckdb")
YFINANCE_API_URL = os.getenv("YFINANCE_API_URL", "http://yfinance-api:8080")

# ============================================================
# CSS PERSONNALISE
# ============================================================

st.markdown(
    """
<style>
    .dataframe-wrap {
        width: 100% !important;
        border-collapse: collapse !important;
        font-family: sans-serif;
        font-size: 0.9rem;
    }
    .dataframe-wrap th {
        background-color: #262730;
        color: white;
        padding: 12px !important;
        text-align: left !important;
        border-bottom: 2px solid #444 !important;
    }
    .dataframe-wrap td {
        padding: 12px !important;
        border-bottom: 1px solid #333 !important;
        vertical-align: top !important;
        white-space: normal !important;
        line-height: 1.5 !important;
    }
    .badge-risk-off { background-color: #dc3545; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.8em; }
    .badge-risk-on { background-color: #28a745; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.8em; }
    .badge-neutral { background-color: #6c757d; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.8em; }
    .badge-buy { background-color: #28a745; color: white; padding: 3px 8px; border-radius: 4px; font-weight: bold; }
    .badge-sell { background-color: #dc3545; color: white; padding: 3px 8px; border-radius: 4px; font-weight: bold; }
    .badge-neutral-v2 { background-color: #6c757d; color: white; padding: 3px 8px; border-radius: 4px; font-weight: bold; }
    .badge-approve { background-color: #28a745; color: white; padding: 3px 8px; border-radius: 4px; font-weight: bold; }
    .badge-reject { background-color: #dc3545; color: white; padding: 3px 8px; border-radius: 4px; font-weight: bold; }
    .badge-watch { background-color: #fd7e14; color: white; padding: 3px 8px; border-radius: 4px; font-weight: bold; }
    .badge-running { background-color: #0d6efd; color: white; padding: 3px 8px; border-radius: 4px; font-weight: bold; }
    .badge-success { background-color: #28a745; color: white; padding: 3px 8px; border-radius: 4px; font-weight: bold; }
    .badge-partial { background-color: #fd7e14; color: white; padding: 3px 8px; border-radius: 4px; font-weight: bold; }
    .badge-failed { background-color: #dc3545; color: white; padding: 3px 8px; border-radius: 4px; font-weight: bold; }
    .v2-card {
        background-color: #1e1e2e;
        border: 1px solid #333;
        border-radius: 8px;
        padding: 16px;
        margin-bottom: 12px;
    }
    .v2-card h4 { margin-top: 0; color: #ccc; }
</style>
""",
    unsafe_allow_html=True,
)

# Ensure local imports resolve whether app is launched from repo root or dashboard/.
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app_modules.core import (
    calculate_sector_sentiment,
    calculate_symbol_momentum,
    check_freshness,
    clean_research_text,
    clean_text,
    enrich_df_with_name,
    extract_valuation_scenarios,
    format_impact_html,
    norm_symbol,
    normalize_cols,
    safe_float,
    safe_float_series,
    safe_json_parse,
    truthy_series,
)
from app_modules.tables import render_interactive_table
from app_modules.visualizations import render_portfolio_sparklines

# ============================================================
# HELPERS GENERAUX (modules externes)
# ============================================================
# ============================================================
# LOAD DATA - Google Sheets
# ============================================================


def validate_configuration() -> bool:
    missing = []
    if not SHEET_ID:
        missing.append("SHEET_ID")
    if not CREDENTIALS_FILE:
        missing.append("GOOGLE_APPLICATION_CREDENTIALS")

    if missing:
        return False

    if not os.path.exists(CREDENTIALS_FILE):
        return False

    return True


@st.cache_resource
def get_gspread_client() -> gspread.Client:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    return gspread.authorize(creds)


@st.cache_data(ttl=30)
def load_data() -> dict[str, pd.DataFrame]:
    if not validate_configuration():
        return {}

    client = get_gspread_client()

    try:
        sh = client.open_by_key(SHEET_ID)
    except gspread.SpreadsheetNotFound as exc:
        st.error(f"Sheet introuvable: {exc}")
        return {}
    except Exception as exc:
        st.error(f"Erreur Sheet: {exc}")
        return {}

    tabs_mapping = {
        "Performance": "Performance",
        "Risk_Metrics": "Risk_Metrics",
        "Portefeuille": "Portefeuille",
        "Transactions": "Transactions",
        "AI_Runs": "AI_Runs",
        "AI_Signals": "AI_Signals",
        "Market_Prices": "Market_Prices",
        "Market_News": "Market_News",
        "Alerts": "Alerts",
        "Backfill_Queue": "Backfill_Queue",
        "Universe": "Universe",
    }

    data = {}

    for key, tab_name in tabs_mapping.items():
        try:
            ws = sh.worksheet(tab_name)
            records = ws.get_all_records()
            df = pd.DataFrame(records)

            df = normalize_cols(df)

            date_c = next(
                (
                    c
                    for c in [
                        "timestamp",
                        "date",
                        "publishedat",
                        "updatedat",
                        "fetchedat",
                        "created_at",
                    ]
                    if c in df.columns
                ),
                None,
            )
            if date_c and not df.empty:
                df[date_c] = pd.to_datetime(df[date_c], errors="coerce")
                df = df.sort_values(by=date_c, ascending=False)

            data[key] = df
        except gspread.WorksheetNotFound:
            data[key] = pd.DataFrame()
        except Exception:
            data[key] = pd.DataFrame()

    return data


# ============================================================
# LOAD DATA - DuckDB (AG2-V2)
# ============================================================


@st.cache_data(ttl=30)
def load_duckdb_data() -> dict[str, pd.DataFrame]:
    """Charge les donnees DuckDB AG2/AG3/AG4/AG4-SPE + enrichissement daily YF (quote/options/calendar)."""
    result = {
        "df_universe": pd.DataFrame(),
        "df_signals": pd.DataFrame(),
        "df_runs": pd.DataFrame(),
        "df_signals_all": pd.DataFrame(),
        "df_funda_latest": pd.DataFrame(),
        "df_funda_runs": pd.DataFrame(),
        "df_funda_history": pd.DataFrame(),
        "df_funda_consensus": pd.DataFrame(),
        "df_funda_metrics": pd.DataFrame(),
        "df_news_macro_history": pd.DataFrame(),
        "df_news_macro_runs": pd.DataFrame(),
        "df_news_symbol_history": pd.DataFrame(),
        "df_news_symbol_latest": pd.DataFrame(),
        "df_news_symbol_runs": pd.DataFrame(),
        "df_yf_enrichment_latest": pd.DataFrame(),
        "df_yf_enrichment_runs": pd.DataFrame(),
        "df_yf_enrichment_history": pd.DataFrame(),
    }

    max_retries = 3
    delay = 0.5

    def _connect_readonly(path: str):
        conn = None
        for attempt in range(max_retries):
            try:
                conn = duckdb.connect(path, read_only=True)
                return conn
            except duckdb.IOException:
                if attempt < max_retries - 1:
                    time.sleep(delay)
                else:
                    return None
            except Exception:
                return None
        return conn

    # -------------------------------
    # AG2-V2 (Technique)
    # -------------------------------
    if os.path.exists(DUCKDB_PATH):
        conn = _connect_readonly(DUCKDB_PATH)
        if conn is not None:
            try:
                try:
                    result["df_universe"] = conn.execute("""
                        SELECT * FROM universe ORDER BY symbol
                    """).fetchdf()
                except Exception:
                    result["df_universe"] = pd.DataFrame()

                try:
                    result["df_signals"] = conn.execute("""
                        SELECT ts.*
                        FROM technical_signals ts
                        INNER JOIN (
                            SELECT symbol, MAX(workflow_date) AS max_date
                            FROM technical_signals
                            GROUP BY symbol
                        ) latest ON ts.symbol = latest.symbol AND ts.workflow_date = latest.max_date
                        ORDER BY ts.symbol
                    """).fetchdf()
                except Exception:
                    result["df_signals"] = pd.DataFrame()

                try:
                    result["df_runs"] = conn.execute("""
                        SELECT * FROM run_log ORDER BY started_at DESC
                    """).fetchdf()
                except Exception:
                    result["df_runs"] = pd.DataFrame()

                try:
                    result["df_signals_all"] = conn.execute("""
                        SELECT * FROM technical_signals ORDER BY workflow_date DESC, symbol
                    """).fetchdf()
                except Exception:
                    result["df_signals_all"] = pd.DataFrame()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    # -------------------------------
    # AG3-V2 (Fondamentale)
    # -------------------------------
    if os.path.exists(AG3_DUCKDB_PATH):
        conn = _connect_readonly(AG3_DUCKDB_PATH)
        if conn is not None:
            try:
                try:
                    result["df_funda_latest"] = conn.execute("""
                        SELECT * FROM v_latest_triage ORDER BY symbol
                    """).fetchdf()
                except Exception:
                    try:
                        result["df_funda_latest"] = conn.execute("""
                            SELECT * EXCLUDE(rn)
                            FROM (
                              SELECT t.*,
                                     ROW_NUMBER() OVER (
                                       PARTITION BY t.symbol
                                       ORDER BY COALESCE(t.updated_at, t.created_at) DESC, t.created_at DESC
                                     ) AS rn
                              FROM fundamentals_triage_history t
                            )
                            WHERE rn = 1
                            ORDER BY symbol
                        """).fetchdf()
                    except Exception:
                        result["df_funda_latest"] = pd.DataFrame()

                try:
                    result["df_funda_runs"] = conn.execute("""
                        SELECT * FROM run_log ORDER BY started_at DESC
                    """).fetchdf()
                except Exception:
                    result["df_funda_runs"] = pd.DataFrame()

                try:
                    result["df_funda_history"] = conn.execute("""
                        SELECT * FROM fundamentals_triage_history
                        ORDER BY COALESCE(updated_at, created_at) DESC, symbol
                    """).fetchdf()
                except Exception:
                    result["df_funda_history"] = pd.DataFrame()

                try:
                    result["df_funda_consensus"] = conn.execute("""
                        SELECT * FROM v_latest_consensus ORDER BY symbol
                    """).fetchdf()
                except Exception:
                    try:
                        result["df_funda_consensus"] = conn.execute("""
                            SELECT * EXCLUDE(rn)
                            FROM (
                              SELECT c.*,
                                     ROW_NUMBER() OVER (
                                       PARTITION BY c.symbol
                                       ORDER BY COALESCE(c.updated_at, c.created_at) DESC, c.created_at DESC
                                     ) AS rn
                              FROM analyst_consensus_history c
                            )
                            WHERE rn = 1
                            ORDER BY symbol
                        """).fetchdf()
                    except Exception:
                        result["df_funda_consensus"] = pd.DataFrame()

                try:
                    result["df_funda_metrics"] = conn.execute("""
                        SELECT m.*
                        FROM fundamental_metrics_history m
                        INNER JOIN (
                          SELECT symbol, metric, MAX(COALESCE(extracted_at, created_at)) AS latest_ts
                          FROM fundamental_metrics_history
                          GROUP BY symbol, metric
                        ) x
                          ON m.symbol = x.symbol
                         AND m.metric = x.metric
                         AND COALESCE(m.extracted_at, m.created_at) = x.latest_ts
                        ORDER BY m.symbol, m.section, m.metric
                    """).fetchdf()
                except Exception:
                    result["df_funda_metrics"] = pd.DataFrame()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    # -------------------------------
    # AG4-V2 (News Macro)
    # -------------------------------
    if os.path.exists(AG4_DUCKDB_PATH):
        conn = _connect_readonly(AG4_DUCKDB_PATH)
        if conn is not None:
            try:
                try:
                    result["df_news_macro_history"] = conn.execute("""
                        SELECT *
                        FROM news_history
                        WHERE COALESCE(type, 'macro') = 'macro'
                        ORDER BY COALESCE(published_at, analyzed_at, last_seen_at, updated_at) DESC
                    """).fetchdf()
                except Exception:
                    result["df_news_macro_history"] = pd.DataFrame()

                try:
                    result["df_news_macro_runs"] = conn.execute("""
                        SELECT * FROM run_log ORDER BY started_at DESC
                    """).fetchdf()
                except Exception:
                    result["df_news_macro_runs"] = pd.DataFrame()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    # -------------------------------
    # AG4-SPE-V2 (News Symbole)
    # -------------------------------
    if os.path.exists(AG4_SPE_DUCKDB_PATH):
        conn = _connect_readonly(AG4_SPE_DUCKDB_PATH)
        if conn is not None:
            try:
                try:
                    result["df_news_symbol_history"] = conn.execute("""
                        SELECT *
                        FROM news_history
                        ORDER BY COALESCE(published_at, analyzed_at, fetched_at, updated_at) DESC, symbol
                    """).fetchdf()
                except Exception:
                    result["df_news_symbol_history"] = pd.DataFrame()

                try:
                    result["df_news_symbol_latest"] = conn.execute("""
                        SELECT * EXCLUDE(rn)
                        FROM (
                          SELECT n.*,
                                 ROW_NUMBER() OVER (
                                   PARTITION BY n.symbol
                                   ORDER BY COALESCE(n.published_at, n.analyzed_at, n.fetched_at, n.updated_at, n.created_at) DESC
                                 ) AS rn
                          FROM news_history n
                        )
                        WHERE rn = 1
                        ORDER BY symbol
                    """).fetchdf()
                except Exception:
                    result["df_news_symbol_latest"] = pd.DataFrame()

                try:
                    result["df_news_symbol_runs"] = conn.execute("""
                        SELECT * FROM run_log ORDER BY started_at DESC
                    """).fetchdf()
                except Exception:
                    result["df_news_symbol_runs"] = pd.DataFrame()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    # Fallback: si AG4-SPE vide, reutiliser les news "type=symbol" depuis AG4-V2.
    if result["df_news_symbol_history"].empty and os.path.exists(AG4_DUCKDB_PATH):
        conn = _connect_readonly(AG4_DUCKDB_PATH)
        if conn is not None:
            try:
                try:
                    result["df_news_symbol_history"] = conn.execute("""
                        SELECT *
                        FROM news_history
                        WHERE COALESCE(type, '') = 'symbol'
                        ORDER BY COALESCE(published_at, analyzed_at, last_seen_at, updated_at) DESC
                    """).fetchdf()
                except Exception:
                    result["df_news_symbol_history"] = pd.DataFrame()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    # -------------------------------
    # YF ENRICHMENT DAILY (quote/options/calendar)
    # -------------------------------
    if os.path.exists(YF_ENRICH_DUCKDB_PATH):
        conn = _connect_readonly(YF_ENRICH_DUCKDB_PATH)
        if conn is not None:
            try:
                try:
                    result["df_yf_enrichment_latest"] = conn.execute("""
                        SELECT * FROM v_latest_symbol_enrichment ORDER BY symbol
                    """).fetchdf()
                except Exception:
                    try:
                        result["df_yf_enrichment_latest"] = conn.execute("""
                            SELECT * EXCLUDE(rn)
                            FROM (
                              SELECT t.*,
                                     ROW_NUMBER() OVER (
                                       PARTITION BY t.symbol
                                       ORDER BY COALESCE(t.fetched_at, t.created_at) DESC, t.created_at DESC
                                     ) AS rn
                              FROM yf_symbol_enrichment_history t
                            )
                            WHERE rn = 1
                            ORDER BY symbol
                        """).fetchdf()
                    except Exception:
                        result["df_yf_enrichment_latest"] = pd.DataFrame()

                try:
                    result["df_yf_enrichment_runs"] = conn.execute("""
                        SELECT * FROM run_log ORDER BY started_at DESC
                    """).fetchdf()
                except Exception:
                    result["df_yf_enrichment_runs"] = pd.DataFrame()

                try:
                    result["df_yf_enrichment_history"] = conn.execute("""
                        SELECT * FROM yf_symbol_enrichment_history
                        ORDER BY COALESCE(fetched_at, created_at) DESC, symbol
                    """).fetchdf()
                except Exception:
                    result["df_yf_enrichment_history"] = pd.DataFrame()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    return result


# ============================================================
# HELPERS V2 - Analyse Technique V2 page
# ============================================================


def _action_badge(action: object) -> str:
    """Retourne un badge HTML colore selon l'action (BUY/SELL/NEUTRAL)."""
    s = str(action).strip().upper() if action else ""
    if s == "BUY":
        return '<span class="badge-buy">BUY</span>'
    elif s == "SELL":
        return '<span class="badge-sell">SELL</span>'
    elif s == "NEUTRAL":
        return '<span class="badge-neutral-v2">NEUTRAL</span>'
    return f'<span class="badge-neutral-v2">{s if s else "—"}</span>'


def _ai_badge(decision: object) -> str:
    """Retourne un badge HTML colore selon la decision IA."""
    s = str(decision).strip().upper() if decision else ""
    if s == "APPROVE":
        return '<span class="badge-approve">APPROVE</span>'
    elif s == "REJECT":
        return '<span class="badge-reject">REJECT</span>'
    elif s == "WATCH":
        return '<span class="badge-watch">WATCH</span>'
    return f'<span class="badge-neutral-v2">{s if s else "—"}</span>'


def _status_badge(status: object) -> str:
    """Retourne un badge HTML colore selon le statut du run."""
    s = str(status).strip().upper() if status else ""
    if s == "SUCCESS":
        return '<span class="badge-success">SUCCESS</span>'
    elif s == "PARTIAL":
        return '<span class="badge-partial">PARTIAL</span>'
    elif s == "FAILED":
        return '<span class="badge-failed">FAILED</span>'
    elif s == "RUNNING":
        return '<span class="badge-running">RUNNING</span>'
    return f'<span class="badge-neutral-v2">{s if s else "—"}</span>'


def _make_rsi_gauge(rsi_val: float, title: str = "RSI") -> go.Figure:
    """Cree une jauge Plotly pour le RSI avec zones colorees."""
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=rsi_val,
            title={"text": title, "font": {"size": 14, "color": "#ccc"}},
            number={"font": {"size": 24, "color": "#fff"}},
            gauge={
                "axis": {"range": [0, 100], "tickcolor": "#666"},
                "bar": {"color": "#ffffff", "thickness": 0.3},
                "steps": [
                    {"range": [0, 30], "color": "#28a745"},
                    {"range": [30, 70], "color": "#444"},
                    {"range": [70, 100], "color": "#dc3545"},
                ],
                "threshold": {
                    "line": {"color": "#ffc107", "width": 2},
                    "thickness": 0.8,
                    "value": rsi_val,
                },
            },
        )
    )
    fig.update_layout(
        height=200,
        margin=dict(t=40, b=10, l=30, r=30),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#ccc"),
    )
    return fig


def _sma_alignment_text(price: float, sma20: float, sma50: float, sma200: float) -> str:
    """Genere le texte d'alignement SMA avec checkmarks/crosses."""
    if price == 0:
        return "Donnees indisponibles"

    parts = [f"Prix({price:.2f})"]

    sma_list = [("SMA20", sma20), ("SMA50", sma50), ("SMA200", sma200)]
    all_above = True

    for name, val in sma_list:
        if val > 0:
            if price > val:
                parts.append(f"> {name}({val:.2f}) ✅")
            else:
                parts.append(f"< {name}({val:.2f}) X")
                all_above = False
        else:
            parts.append(f"{name}(N/A)")
            all_above = False

    alignment = " ".join(parts)

    if all_above and sma20 > 0 and sma50 > 0 and sma200 > 0:
        if price > sma20 > sma50 > sma200:
            alignment += " → **BULLISH ALIGNMENT**"
        else:
            alignment += " → BULLISH (prix au-dessus)"
    elif price > 0 and sma200 > 0 and price < sma200:
        alignment += " → **BEARISH**"
    else:
        alignment += " → MIXTE"

    return alignment


# ============================================================
# INDICATEUR INTERPRETATION - contexte visuel pour non-experts
# ============================================================

# Chaque indicateur : (label, min, max, zones, description)
# zones: liste de (borne_basse, borne_haute, couleur, label_zone)
INDICATOR_META = {
    "rsi14": {
        "label": "RSI (14)",
        "min": 0, "max": 100,
        "zones": [(0, 30, "#28a745", "Survendu"), (30, 70, "#6c757d", "Neutre"), (70, 100, "#dc3545", "Suracheté")],
        "desc": "Mesure la vitesse des variations de prix. <30 = survendu (opportunité achat), >70 = suracheté (risque correction).",
    },
    "macd_hist": {
        "label": "MACD Histogramme",
        "min": -2, "max": 2,
        "zones": [(-2, -0.1, "#dc3545", "Baissier"), (-0.1, 0.1, "#6c757d", "Neutre"), (0.1, 2, "#28a745", "Haussier")],
        "desc": "Différence entre signal MACD et sa moyenne. Positif = momentum haussier, négatif = momentum baissier.",
    },
    "stoch_k": {
        "label": "Stochastique %K",
        "min": 0, "max": 100,
        "zones": [(0, 20, "#28a745", "Survendu"), (20, 80, "#6c757d", "Neutre"), (80, 100, "#dc3545", "Suracheté")],
        "desc": "Position du prix dans son range récent. <20 = bas du range, >80 = haut du range.",
    },
    "stoch_d": {
        "label": "Stochastique %D",
        "min": 0, "max": 100,
        "zones": [(0, 20, "#28a745", "Survendu"), (20, 80, "#6c757d", "Neutre"), (80, 100, "#dc3545", "Suracheté")],
        "desc": "Moyenne lissée de %K. Croisement %K/%D génère des signaux.",
    },
    "adx": {
        "label": "ADX (Force tendance)",
        "min": 0, "max": 100,
        "zones": [(0, 20, "#6c757d", "Pas de tendance"), (20, 40, "#ffc107", "Tendance modérée"), (40, 100, "#28a745", "Tendance forte")],
        "desc": "Force de la tendance (pas sa direction). >25 = tendance significative, <20 = marché sans direction.",
    },
    "atr_pct": {
        "label": "ATR %",
        "min": 0, "max": 10,
        "zones": [(0, 1, "#28a745", "Faible volatilité"), (1, 3, "#ffc107", "Volatilité normale"), (3, 10, "#dc3545", "Haute volatilité")],
        "desc": "Average True Range en % du prix. Mesure la volatilité quotidienne moyenne.",
    },
    "bb_width": {
        "label": "Bollinger Width",
        "min": 0, "max": 0.2,
        "zones": [(0, 0.03, "#0d6efd", "Compression (squeeze)"), (0.03, 0.08, "#6c757d", "Normal"), (0.08, 0.2, "#dc3545", "Expansion")],
        "desc": "Largeur des bandes de Bollinger. Compression = explosion imminente, expansion = mouvement en cours.",
    },
    "volatility": {
        "label": "Volatilité RSI",
        "min": 0, "max": 2,
        "zones": [(0, 0.3, "#28a745", "Calme"), (0.3, 0.8, "#ffc107", "Modérée"), (0.8, 2, "#dc3545", "Élevée")],
        "desc": "Volatilité normalisée. Plus elle est basse, plus le prix est stable.",
    },
    "obv_slope": {
        "label": "OBV Slope",
        "min": -5, "max": 5,
        "zones": [(-5, -0.5, "#dc3545", "Volume sortant"), (-0.5, 0.5, "#6c757d", "Neutre"), (0.5, 5, "#28a745", "Volume entrant")],
        "desc": "Pente du On-Balance Volume. Positif = accumulation (acheteurs), négatif = distribution (vendeurs).",
    },
}


def _indicator_bar(key: str, value: float, tf_label: str = "") -> str:
    """Génère une barre de progression HTML colorée avec contexte pour un indicateur."""
    meta = INDICATOR_META.get(key)
    if not meta:
        return f"<span>{value:.4f}</span>"

    vmin, vmax = meta["min"], meta["max"]
    # Clamp value pour le positionnement
    clamped = max(vmin, min(vmax, value))
    pct = ((clamped - vmin) / (vmax - vmin)) * 100 if vmax != vmin else 50

    # Trouver la zone active
    zone_color = "#6c757d"
    zone_label = ""
    for z_lo, z_hi, z_col, z_lbl in meta["zones"]:
        if z_lo <= value <= z_hi:
            zone_color = z_col
            zone_label = z_lbl
            break
    else:
        # Hors bornes
        if value < vmin:
            zone_color = meta["zones"][0][2]
            zone_label = meta["zones"][0][3]
        else:
            zone_color = meta["zones"][-1][2]
            zone_label = meta["zones"][-1][3]

    # Formater la valeur
    if abs(value) >= 10:
        val_str = f"{value:.1f}"
    elif abs(value) >= 1:
        val_str = f"{value:.2f}"
    else:
        val_str = f"{value:.4f}"

    # Construire les segments de la barre
    bar_segments = ""
    for z_lo, z_hi, z_col, z_lbl in meta["zones"]:
        seg_start = max(0, ((z_lo - vmin) / (vmax - vmin)) * 100) if vmax != vmin else 0
        seg_end = min(100, ((z_hi - vmin) / (vmax - vmin)) * 100) if vmax != vmin else 100
        seg_width = seg_end - seg_start
        bar_segments += f'<div style="position:absolute;left:{seg_start}%;width:{seg_width}%;height:100%;background:{z_col};opacity:0.25;"></div>'

    uid = f"ind_{key}_{tf_label}"
    html = f"""
    <div style="margin-bottom:12px;" title="{meta['desc']}">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:2px;">
        <span style="color:#ccc;font-size:0.85em;font-weight:600;">{meta['label']}</span>
        <span style="color:{zone_color};font-size:0.85em;font-weight:bold;">{val_str} — {zone_label}</span>
      </div>
      <div style="position:relative;height:10px;background:#222;border-radius:5px;overflow:hidden;">
        {bar_segments}
        <div style="position:absolute;left:{pct}%;top:0;width:3px;height:100%;background:#fff;border-radius:1px;transform:translateX(-1px);z-index:2;"></div>
      </div>
      <div style="display:flex;justify-content:space-between;margin-top:1px;">
        <span style="color:#666;font-size:0.7em;">{vmin}</span>
        <span style="color:#666;font-size:0.7em;">{vmax}</span>
      </div>
    </div>"""
    return html


# ============================================================
# FETCH PRICE DATA FROM YFINANCE-API
# ============================================================


@st.cache_data(ttl=120)
def fetch_yfinance_history(symbol: str, interval: str = "1d", lookback_days: int = 90) -> pd.DataFrame:
    """Récupère l'historique OHLCV depuis yfinance-api. Retourne un DataFrame vide si indisponible."""
    try:
        resp = requests.get(
            f"{YFINANCE_API_URL}/history",
            params={"symbol": symbol, "interval": interval, "lookback_days": lookback_days, "allow_stale": "true"},
            timeout=10,
        )
        if resp.status_code != 200:
            return pd.DataFrame()
        data = resp.json()
        if not data.get("ok") or not data.get("bars"):
            return pd.DataFrame()

        df = pd.DataFrame(data["bars"])
        df.rename(columns={"t": "time", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}, inplace=True)
        df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True)
        df = df.dropna(subset=["time"]).sort_values("time")
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=20)
def fetch_yfinance_quote_batch(symbols: tuple[str, ...], qty: float = 100.0, side: str = "BUY") -> pd.DataFrame:
    """Fetch quote snapshots for multiple symbols from yfinance-api /quote endpoint."""
    if not symbols:
        return pd.DataFrame()

    cleaned = [str(s).strip().upper() for s in symbols if str(s).strip()]
    if not cleaned:
        return pd.DataFrame()

    try:
        resp = requests.get(
            f"{YFINANCE_API_URL}/quote",
            params={
                "symbols": ",".join(cleaned),
                "qty": float(qty),
                "side": str(side or "BUY").upper(),
                "max_age_seconds": 20,
            },
            timeout=12,
        )
        if resp.status_code != 200:
            return pd.DataFrame()
        data = resp.json()
        rows = data.get("quotes", [])
        if not isinstance(rows, list) or not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        if "symbol" in df.columns:
            df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
            df = df[df["symbol"] != ""]
        for c in [
            "regularMarketPrice",
            "bid",
            "ask",
            "bidSize",
            "askSize",
            "spreadAbs",
            "spreadPct",
            "slippageProxyPct",
            "volume",
            "exchangeDataDelayedBy",
        ]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_yfinance_options_snapshot(symbol: str, target_days: int = 30) -> dict:
    """Fetch options snapshot from yfinance-api /options endpoint with robust empty handling."""
    sym = str(symbol or "").strip().upper()
    out = {
        "symbol": sym,
        "options_ok": False,
        "options_error": "",
        "expiration_selected": "",
        "days_to_expiration": pd.NA,
        "iv_atm": pd.NA,
        "iv_atm_call": pd.NA,
        "iv_atm_put": pd.NA,
        "iv_skew_put_minus_call_5pct": pd.NA,
        "put_call_oi_ratio": pd.NA,
        "put_call_volume_ratio": pd.NA,
        "options_warning": "",
    }
    if not sym:
        return out

    def _num_or_na(v: object):
        try:
            n = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
            return float(n) if pd.notna(n) else pd.NA
        except Exception:
            return pd.NA

    try:
        resp = requests.get(
            f"{YFINANCE_API_URL}/options",
            params={"symbol": sym, "target_days": int(target_days), "max_rows_per_side": 120, "max_age_seconds": 300},
            timeout=14,
        )
        if resp.status_code != 200:
            out["options_error"] = f"http_{resp.status_code}"
            return out

        data = resp.json()
        out["options_ok"] = bool(data.get("ok", False))
        out["options_error"] = str(data.get("error", "") or "")
        out["expiration_selected"] = str(data.get("expirationSelected", "") or "")
        out["days_to_expiration"] = _num_or_na(data.get("daysToExpiration", pd.NA))

        metrics = data.get("metrics", {}) if isinstance(data.get("metrics"), dict) else {}
        out["iv_atm"] = _num_or_na(metrics.get("ivAtm", pd.NA))
        out["iv_atm_call"] = _num_or_na(metrics.get("ivAtmCall", pd.NA))
        out["iv_atm_put"] = _num_or_na(metrics.get("ivAtmPut", pd.NA))
        out["iv_skew_put_minus_call_5pct"] = _num_or_na(metrics.get("skewPutMinusCall5Pct", pd.NA))
        out["put_call_oi_ratio"] = _num_or_na(metrics.get("putCallOiRatio", pd.NA))
        out["put_call_volume_ratio"] = _num_or_na(metrics.get("putCallVolumeRatio", pd.NA))

        warnings = data.get("warnings", [])
        if isinstance(warnings, list):
            out["options_warning"] = " | ".join([str(w) for w in warnings if str(w).strip()])
        elif warnings:
            out["options_warning"] = str(warnings)

        return out
    except Exception as exc:
        out["options_error"] = str(exc)
        return out


@st.cache_data(ttl=1800)
def fetch_yfinance_calendar_snapshot(symbol: str) -> dict:
    """Fetch earnings/calendar snapshot from yfinance-api /calendar endpoint."""
    sym = str(symbol or "").strip().upper()
    out = {
        "symbol": sym,
        "calendar_ok": False,
        "calendar_error": "",
        "next_earnings_date": pd.NaT,
        "days_to_earnings": pd.NA,
    }
    if not sym:
        return out

    try:
        resp = requests.get(
            f"{YFINANCE_API_URL}/calendar",
            params={"symbol": sym, "earnings_limit": 8, "max_age_seconds": 1800},
            timeout=10,
        )
        if resp.status_code != 200:
            out["calendar_error"] = f"http_{resp.status_code}"
            return out
        data = resp.json()
        out["calendar_ok"] = bool(data.get("ok", False))
        out["calendar_error"] = str(data.get("error", "") or "")
        nxt = pd.to_datetime(data.get("nextEarningsDate"), errors="coerce", utc=True)
        out["next_earnings_date"] = nxt
        if pd.notna(nxt):
            days = (nxt - pd.Timestamp.now(tz="UTC")).total_seconds() / 86400.0
            out["days_to_earnings"] = round(float(days), 1)
        return out
    except Exception as exc:
        out["calendar_error"] = str(exc)
        return out


def _portfolio_exposure_maps(df_port: pd.DataFrame) -> tuple[dict[str, float], dict[str, float]]:
    """Return symbol and sector weights (%) from portfolio table."""
    if df_port is None or df_port.empty:
        return {}, {}

    wk = normalize_cols(df_port.copy())
    if "symbol" not in wk.columns:
        return {}, {}
    if "marketvalue" not in wk.columns:
        wk["marketvalue"] = 0.0
    if "sector" not in wk.columns:
        wk["sector"] = ""

    wk["symbol"] = wk["symbol"].astype(str).str.strip().str.upper()
    wk["sector"] = wk["sector"].fillna("").astype(str).str.strip()
    wk["mv"] = safe_float_series(wk["marketvalue"]).fillna(0.0)
    wk = wk[~wk["symbol"].isin(["CASH_EUR", "__META__", ""])]
    wk = wk[wk["mv"] > 0]
    if wk.empty:
        return {}, {}

    total = float(wk["mv"].sum())
    if total <= 0:
        return {}, {}

    sym_map = (wk.groupby("symbol")["mv"].sum() / total * 100.0).to_dict()
    sec_map = (wk.groupby("sector")["mv"].sum() / total * 100.0).to_dict()
    return sym_map, sec_map


def _score_to_1_5(v: float) -> int:
    x = safe_float(v)
    x = max(0.0, min(100.0, x))
    return int(min(5, max(1, int((x // 20) + 1))))


def _grade_from_prob(prob_score: float) -> str:
    p = safe_float(prob_score)
    if p >= 70:
        return "A"
    if p >= 55:
        return "B"
    return "C"


def _score_unit(v: float) -> int:
    return int(round(max(0.0, min(100.0, safe_float(v)))))


def _stable_jitter(symbol: str, salt: str, amplitude: float = 0.45) -> float:
    seed = f"{symbol}|{salt}".encode("utf-8")
    h = hashlib.sha1(seed).hexdigest()
    unit = int(h[:8], 16) / 0xFFFFFFFF
    return (unit * 2.0 - 1.0) * amplitude


def _build_multi_agent_matrix(
    consolidated: pd.DataFrame,
    df_portfolio: pd.DataFrame,
    df_yf_enrichment_latest: pd.DataFrame,
) -> pd.DataFrame:
    """Build mechanical Risk/Reward matrix from AG2+AG3+AG4 with daily YF enrichment DuckDB."""
    if consolidated is None or consolidated.empty:
        return pd.DataFrame()

    base = consolidated.copy()
    base["symbol"] = base.get("symbol", pd.Series("", index=base.index)).astype(str).str.strip().str.upper()
    base = base[base["symbol"] != ""].copy()
    if base.empty:
        return pd.DataFrame()

    enrich = normalize_cols(df_yf_enrichment_latest.copy()) if df_yf_enrichment_latest is not None and not df_yf_enrichment_latest.empty else pd.DataFrame()
    if not enrich.empty and "symbol" in enrich.columns:
        enrich["symbol"] = enrich["symbol"].astype(str).str.strip().str.upper()
        enrich = enrich[enrich["symbol"] != ""]

        col_map = {
            "regular_market_price": "regularMarketPrice",
            "bid_size": "bidSize",
            "ask_size": "askSize",
            "spread_pct": "spreadPct",
            "spread_abs": "spreadAbs",
            "slippage_proxy_pct": "slippageProxyPct",
            "market_state": "marketState",
            "exchange_data_delayed_by": "exchangeDataDelayedBy",
            "iv_atm": "iv_atm",
            "iv_atm_call": "iv_atm_call",
            "iv_atm_put": "iv_atm_put",
            "skew_put_minus_call_5pct": "iv_skew_put_minus_call_5pct",
            "put_call_oi_ratio": "put_call_oi_ratio",
            "put_call_volume_ratio": "put_call_volume_ratio",
            "days_to_expiration": "days_to_expiration",
            "expiration_selected": "expiration_selected",
            "options_ok": "options_ok",
            "options_error": "options_error",
            "options_warning": "options_warning",
            "next_earnings_date": "next_earnings_date",
            "days_to_earnings": "days_to_earnings",
            "calendar_ok": "calendar_ok",
            "calendar_error": "calendar_error",
            "fetched_at": "yf_fetched_at",
        }
        for src, dst in col_map.items():
            if src in enrich.columns and dst not in enrich.columns:
                enrich[dst] = enrich[src]

        if "yf_fetched_at" in enrich.columns:
            enrich["yf_fetched_at"] = pd.to_datetime(enrich["yf_fetched_at"], errors="coerce", utc=True)
            enrich["yf_age_h"] = (pd.Timestamp.now(tz="UTC") - enrich["yf_fetched_at"]).dt.total_seconds() / 3600.0
        else:
            enrich["yf_age_h"] = pd.NA

        keep_enrich = [
            "symbol",
            "regularMarketPrice",
            "bid",
            "ask",
            "bidSize",
            "askSize",
            "spreadAbs",
            "spreadPct",
            "slippageProxyPct",
            "marketState",
            "exchangeDataDelayedBy",
            "iv_atm",
            "iv_atm_call",
            "iv_atm_put",
            "iv_skew_put_minus_call_5pct",
            "put_call_oi_ratio",
            "put_call_volume_ratio",
            "days_to_expiration",
            "expiration_selected",
            "options_ok",
            "options_error",
            "options_warning",
            "next_earnings_date",
            "days_to_earnings",
            "calendar_ok",
            "calendar_error",
            "yf_fetched_at",
            "yf_age_h",
        ]
        keep_enrich = [c for c in keep_enrich if c in enrich.columns]
        base = base.merge(enrich[keep_enrich].drop_duplicates(subset=["symbol"], keep="first"), on="symbol", how="left")
    else:
        base["options_ok"] = False
        base["options_error"] = "MISSING_ENRICHMENT_DATA"
        base["options_warning"] = ""
        base["days_to_earnings"] = pd.NA
        base["iv_atm"] = pd.NA
        base["spreadPct"] = pd.NA
        base["slippageProxyPct"] = pd.NA
        base["regularMarketPrice"] = pd.NA
        base["yf_age_h"] = pd.NA

    sym_w_map, sec_w_map = _portfolio_exposure_maps(df_portfolio)

    rows = []
    for _, r in base.iterrows():
        symbol = str(r.get("symbol", "")).strip().upper()
        if not symbol:
            continue

        entry = safe_float(r.get("last_close", 0))
        quote_px = safe_float(r.get("regularMarketPrice", 0))
        if entry <= 0 and quote_px > 0:
            entry = quote_px
        if entry <= 0:
            entry = safe_float(r.get("target_price", 0))
        if entry <= 0:
            entry = 0.0

        stop = safe_float(r.get("ai_stop_loss", 0))
        d1_support = safe_float(r.get("d1_support", 0))
        d1_dist_sup = safe_float(r.get("d1_dist_sup_pct", 0))
        d1_atr_pct = safe_float(r.get("d1_atr_pct", 0))

        if entry > 0:
            if stop <= 0 or stop >= entry:
                if d1_support > 0 and d1_support < entry:
                    stop = d1_support * 0.998
                else:
                    fallback_risk = max(2.0, d1_dist_sup if d1_dist_sup > 0 else d1_atr_pct * 2.0)
                    stop = entry * (1.0 - fallback_risk / 100.0)
        else:
            stop = 0.0

        d1_res = safe_float(r.get("d1_resistance", 0))
        d1_dist_res = safe_float(r.get("d1_dist_res_pct", 0))
        funda_upside = safe_float(r.get("funda_upside", 0))
        target_price = safe_float(r.get("target_price", 0))

        tp_candidates = []
        if entry > 0 and d1_res > entry:
            tp_candidates.append(d1_res)
        if entry > 0 and funda_upside > 0:
            tp_candidates.append(entry * (1.0 + funda_upside / 100.0))
        if entry > 0 and target_price > entry:
            tp_candidates.append(target_price)
        if entry > 0 and not tp_candidates and d1_dist_res > 0:
            tp_candidates.append(entry * (1.0 + d1_dist_res / 100.0))
        if entry > 0 and not tp_candidates:
            tp_candidates.append(entry * 1.03)
        tp = min(tp_candidates) if tp_candidates else 0.0

        reward_pct = ((tp - entry) / entry * 100.0) if entry > 0 else 0.0
        risk_pct = ((entry - stop) / entry * 100.0) if entry > 0 else 0.0
        risk_pct = max(0.1, risk_pct)
        r_multiple = reward_pct / risk_pct if risk_pct > 0 else 0.0

        funda_risk = safe_float(r.get("funda_risk", 50.0))
        spread_pct = safe_float(r.get("spreadPct", 0))
        slip_pct = safe_float(r.get("slippageProxyPct", 0))
        symbol_news_impact = safe_float(r.get("symbol_news_impact_7d", 0))
        macro_impact = safe_float(r.get("macro_impact_30d", 0))
        sector = str(r.get("sector", "")).strip()
        sector_weight = safe_float(sec_w_map.get(sector, 0.0))
        symbol_weight = safe_float(sym_w_map.get(symbol, 0.0))

        days_to_earnings = pd.to_numeric(pd.Series([r.get("days_to_earnings", pd.NA)]), errors="coerce").iloc[0]
        if pd.notna(days_to_earnings):
            d = max(0.0, min(30.0, float(days_to_earnings)))
            # Linear scale: 0 day -> 95 risk, 30+ days -> 20 risk
            event_risk = 20.0 + ((30.0 - d) / 30.0) * 75.0
        else:
            event_risk = 40.0

        vol_risk = min(100.0, max(0.0, d1_atr_pct * 20.0))
        liq_risk = min(100.0, max(0.0, spread_pct * 35.0 + slip_pct * 20.0))
        news_risk = min(100.0, max(0.0, max(0.0, -symbol_news_impact) * 8.0 + max(0.0, -macro_impact) * 3.0))
        concentration_risk = min(100.0, max(0.0, sector_weight * 1.3 + symbol_weight * 1.1))

        iv_atm = pd.to_numeric(pd.Series([r.get("iv_atm", pd.NA)]), errors="coerce").iloc[0]
        options_raw = str(r.get("options_ok", "")).strip().lower()
        options_ok = options_raw in ("1", "true", "yes", "y")
        options_has_iv = options_ok and pd.notna(iv_atm) and float(iv_atm) > 0
        if options_has_iv:
            iv_val = float(iv_atm)
            iv_as_pct = iv_val * 100.0 if iv_val <= 3 else iv_val
            options_risk = min(100.0, max(0.0, iv_as_pct * 1.8))
        else:
            # Keep a neutral baseline when options are missing (common for FR symbols).
            options_risk = 35.0

        yf_age_h = pd.to_numeric(pd.Series([r.get("yf_age_h", pd.NA)]), errors="coerce").iloc[0]
        stale_penalty = 0.0
        if pd.notna(yf_age_h):
            if float(yf_age_h) > 72:
                stale_penalty = 12.0
            elif float(yf_age_h) > 36:
                stale_penalty = 6.0
            elif float(yf_age_h) > 24:
                stale_penalty = 3.0

        risk_weights = {
            "funda": 0.30,
            "vol": 0.18,
            "liq": 0.14,
            "event": 0.14,
            "news": 0.10,
            "concentration": 0.09,
            "options": 0.05 if options_has_iv else 0.01,
        }
        risk_values = {
            "funda": min(100.0, max(0.0, funda_risk)),
            "vol": vol_risk,
            "liq": liq_risk,
            "event": event_risk,
            "news": news_risk,
            "concentration": concentration_risk,
            "options": options_risk,
        }
        wsum = sum(risk_weights.values())
        risk_core = sum(risk_values[k] * risk_weights[k] for k in risk_weights) / wsum if wsum > 0 else 50.0
        risk_score_100 = risk_core + stale_penalty
        risk_score_100 = max(0.0, min(100.0, risk_score_100))

        reward_r = min(100.0, max(0.0, r_multiple * 35.0))
        reward_upside = min(100.0, max(0.0, funda_upside * 3.0))
        reward_space = min(100.0, max(0.0, d1_dist_res * 4.0))
        reward_catalyst = min(100.0, max(0.0, max(0.0, symbol_news_impact) * 6.0 + max(0.0, macro_impact) * 2.0))
        tech_action = str(r.get("tech_action", "")).upper().strip()
        tech_conf = safe_float(r.get("tech_confidence", 0))
        if tech_action == "BUY":
            trend_bonus = min(100.0, 55.0 + tech_conf * 0.45)
        elif tech_action == "SELL":
            trend_bonus = max(0.0, 35.0 - tech_conf * 0.25)
        else:
            trend_bonus = 45.0

        reward_score_100 = (
            0.36 * reward_r
            + 0.22 * reward_upside
            + 0.14 * reward_space
            + 0.18 * reward_catalyst
            + 0.10 * trend_bonus
        )
        reward_score_100 = max(0.0, min(100.0, reward_score_100))

        funda_score = safe_float(r.get("funda_score", 50))
        tech_prob = 50.0 + (8.0 if tech_action == "BUY" else (-8.0 if tech_action == "SELL" else 0.0)) + (tech_conf - 50.0) * 0.20
        funda_prob = 0.7 * funda_score + 0.3 * (100.0 - funda_risk)
        sentiment_prob = min(100.0, max(0.0, 50.0 + symbol_news_impact * 4.0 + macro_impact * 1.5))
        regime = str(r.get("ai_regime_d1", "")).upper().strip()
        alignment = str(r.get("ai_alignment", "")).upper().strip()
        regime_adj = 8.0 if regime == "BULLISH" else (-6.0 if regime == "BEARISH" else 0.0)
        align_adj = 6.0 if alignment == "WITH_BIAS" else (-6.0 if alignment == "AGAINST_BIAS" else 0.0)

        prob_score = (
            0.36 * tech_prob
            + 0.34 * funda_prob
            + 0.20 * sentiment_prob
            + 0.10 * (50.0 + regime_adj + align_adj)
        )
        prob_score = max(0.0, min(100.0, prob_score))

        p_win = max(0.05, min(0.95, prob_score / 100.0))
        ev_r = (p_win * max(0.0, r_multiple)) - (1.0 - p_win)

        risk_1_5 = _score_to_1_5(risk_score_100)
        reward_1_5 = _score_to_1_5(reward_score_100)
        risk_u = _score_unit(risk_score_100)
        reward_u = _score_unit(reward_score_100)
        grade = _grade_from_prob(prob_score)

        if risk_u <= 50 and reward_u >= 50:
            quadrant = "Q1 - Priorite"
        elif risk_u > 50 and reward_u >= 50:
            quadrant = "Q2 - Speculatif"
        elif risk_u <= 50 and reward_u < 50:
            quadrant = "Q3 - Defensif"
        else:
            quadrant = "Q4 - Sortie"

        if ev_r >= 0.30 and reward_score_100 >= 60.0 and risk_score_100 <= 40.0 and grade in ("A", "B"):
            matrix_action = "Entrer / Renforcer"
        elif ev_r < 0 or (risk_score_100 >= 70.0 and reward_score_100 <= 45.0):
            matrix_action = "Reduire / Sortir"
        else:
            matrix_action = "Surveiller"

        options_note = ""
        if not options_ok:
            err = str(r.get("options_error", "") or "").strip()
            warn = str(r.get("options_warning", "") or "").strip()
            options_note = err or warn or "Aucune option disponible"

        rows.append(
            {
                **r.to_dict(),
                "regularMarketPrice": quote_px if quote_px > 0 else pd.NA,
                "spreadPct": spread_pct if spread_pct > 0 else pd.NA,
                "slippageProxyPct": slip_pct if slip_pct > 0 else pd.NA,
                "days_to_earnings": float(days_to_earnings) if pd.notna(days_to_earnings) else pd.NA,
                "iv_atm": float(iv_atm) if pd.notna(iv_atm) else pd.NA,
                "options_ok": options_ok,
                "options_error": str(r.get("options_error", "") or ""),
                "options_warning": str(r.get("options_warning", "") or ""),
                "has_enrichment": bool(pd.notna(r.get("yf_fetched_at"))) if "yf_fetched_at" in r.index else False,
                "entry_price": entry,
                "stop_price": stop,
                "tp_price": tp,
                "reward_pct": reward_pct,
                "risk_pct": risk_pct,
                "r_multiple": r_multiple,
                "risk_score_100": risk_score_100,
                "reward_score_100": reward_score_100,
                "risk_score_u": risk_u,
                "reward_score_u": reward_u,
                "risk_score_plot": max(0.0, min(100.0, risk_u + _stable_jitter(symbol, "risk"))),
                "reward_score_plot": max(0.0, min(100.0, reward_u + _stable_jitter(symbol, "reward"))),
                "prob_score": prob_score,
                "p_win": p_win,
                "ev_r": ev_r,
                "risk_score_1_5": risk_1_5,
                "reward_score_1_5": reward_1_5,
                "setup_grade": grade,
                "matrix_action": matrix_action,
                "quadrant": quadrant,
                "event_risk_score": event_risk,
                "vol_risk_score": vol_risk,
                "liquidity_risk_score": liq_risk,
                "news_risk_score": news_risk,
                "concentration_risk_score": concentration_risk,
                "options_risk_score": options_risk,
                "sector_weight_pct": sector_weight,
                "symbol_weight_pct": symbol_weight,
                "options_note": options_note,
            }
        )

    out_df = pd.DataFrame(rows)
    if out_df.empty:
        return out_df
    out_df = out_df.sort_values(
        ["matrix_action", "ev_r", "reward_score_u", "risk_score_u"],
        ascending=[True, False, False, True],
        na_position="last",
    ).reset_index(drop=True)
    return out_df


def _make_candlestick_chart(df: pd.DataFrame, title: str, sma20: float = 0, sma50: float = 0, sma200: float = 0, support: float = 0, resistance: float = 0) -> go.Figure:
    """Crée un graphique chandelier avec SMA et niveaux S/R optionnels."""
    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=df["time"], open=df["open"], high=df["high"], low=df["low"], close=df["close"],
        name="OHLC",
        increasing_line_color="#28a745", decreasing_line_color="#dc3545",
    ))

    # Volume en barres secondaires
    if "volume" in df.columns and df["volume"].notna().any():
        vol_colors = ["rgba(40,167,69,0.4)" if c >= o else "rgba(220,53,69,0.4)" for c, o in zip(df["close"], df["open"])]
        fig.add_trace(go.Bar(
            x=df["time"], y=df["volume"], name="Volume",
            marker_color=vol_colors, opacity=0.4, yaxis="y2",
        ))

    # SMA lines (valeur unique = ligne horizontale, pour simplifier ici on ne les ajoute que si > 0)
    if support > 0:
        fig.add_hline(y=support, line_dash="dash", line_color="#28a745", annotation_text=f"S: {support:.2f}", annotation_position="bottom left")
    if resistance > 0:
        fig.add_hline(y=resistance, line_dash="dash", line_color="#dc3545", annotation_text=f"R: {resistance:.2f}", annotation_position="top left")

    fig.update_layout(
        title=title,
        height=400,
        xaxis_rangeslider_visible=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#ccc"),
        margin=dict(t=40, b=30, l=50, r=20),
        yaxis=dict(title="Prix", side="left", gridcolor="#333"),
        yaxis2=dict(title="Volume", side="right", overlaying="y", showgrid=False, range=[0, df["volume"].max() * 4] if "volume" in df.columns and df["volume"].notna().any() else None),
        xaxis=dict(gridcolor="#333"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    return fig


def _first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _prepare_performance_timeseries(df_perf: pd.DataFrame) -> pd.DataFrame:
    cols = ["timestamp", "total_value", "cash_value", "equity_value", "invested_value"]
    if df_perf is None or df_perf.empty:
        return pd.DataFrame(columns=cols)

    df = df_perf.copy()
    ts_col = _first_existing_column(df, ["timestamp", "date", "updatedat", "created_at"])
    if not ts_col:
        return pd.DataFrame(columns=cols)

    df["timestamp"] = pd.to_datetime(df[ts_col], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    if df.empty:
        return pd.DataFrame(columns=cols)

    total_col = _first_existing_column(df, ["totalvalue", "totalvalueeur", "total_value", "portfolio_value"])
    cash_col = _first_existing_column(df, ["cash", "casheur", "cash_eur"])
    equity_col = _first_existing_column(df, ["equity", "equityeur", "equity_value", "invested"])
    if not any([total_col, cash_col, equity_col]):
        return pd.DataFrame(columns=cols)

    total_value = safe_float_series(df[total_col]) if total_col else pd.Series(0.0, index=df.index)
    cash_value = safe_float_series(df[cash_col]) if cash_col else pd.Series(0.0, index=df.index)
    equity_value = safe_float_series(df[equity_col]) if equity_col else pd.Series(0.0, index=df.index)

    if not total_col and (cash_col or equity_col):
        total_value = cash_value + equity_value
    if total_col and not cash_col and equity_col:
        cash_value = total_value - equity_value
    if total_col and cash_col and not equity_col:
        equity_value = total_value - cash_value
    if total_col and not cash_col and not equity_col:
        equity_value = total_value
        cash_value = pd.Series(0.0, index=df.index)

    out = pd.DataFrame(
        {
            "timestamp": df["timestamp"],
            "total_value": total_value,
            "cash_value": cash_value,
            "equity_value": equity_value,
        }
    )
    out = out.replace([float("inf"), float("-inf")], pd.NA).dropna(subset=["total_value"])
    out = out.groupby("timestamp", as_index=False).last().sort_values("timestamp")
    out["invested_value"] = out["equity_value"]
    if (out["invested_value"].abs().sum() == 0) and ("cash_value" in out.columns):
        out["invested_value"] = out["total_value"] - out["cash_value"]
    return out


def _append_current_efficiency_point(
    eff_df: pd.DataFrame,
    *,
    total_value: float,
    cash_value: float,
    invested_value: float,
) -> pd.DataFrame:
    cols = ["timestamp", "total_value", "cash_value", "equity_value", "invested_value"]
    now_ts = pd.Timestamp.now()
    cur = pd.DataFrame(
        [
            {
                "timestamp": now_ts,
                "total_value": float(total_value),
                "cash_value": float(cash_value),
                "equity_value": float(invested_value),
                "invested_value": float(invested_value),
            }
        ]
    )

    if eff_df is None or eff_df.empty:
        return cur[cols]

    base = eff_df.copy()
    for c in cols:
        if c not in base.columns:
            base[c] = pd.NA

    out = pd.concat([base[cols], cur[cols]], ignore_index=True)
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    out = out.dropna(subset=["timestamp"]).sort_values("timestamp")
    out = out.drop_duplicates(subset=["timestamp"], keep="last")
    return out.reset_index(drop=True)


def _prepare_transactions(df_trans: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "timestamp",
        "symbol",
        "side",
        "quantity",
        "notional",
        "realized_pnl",
        "agent_label",
    ]
    if df_trans is None or df_trans.empty:
        return pd.DataFrame(columns=cols)

    df = df_trans.copy()

    ts_col = _first_existing_column(df, ["timestamp", "date", "created_at", "updatedat"])
    side_col = _first_existing_column(df, ["side", "action", "signal"])
    symbol_col = _first_existing_column(df, ["symbol", "ticker"])
    qty_col = _first_existing_column(df, ["quantity", "qty"])
    notional_col = _first_existing_column(df, ["notional", "tradevalue", "value", "amount"])
    realized_col = _first_existing_column(df, ["realizedpnl", "realized_pnl", "pnl_realized", "pnl"])
    agent_col = _first_existing_column(
        df,
        ["agent", "agent_id", "agentname", "agent_name", "source_agent", "sourceagent"],
    )
    rationale_col = _first_existing_column(df, ["rationale", "commentary", "notes"])

    df["timestamp"] = pd.to_datetime(df[ts_col], errors="coerce") if ts_col else pd.NaT
    df["symbol"] = df[symbol_col].astype(str).str.strip().str.upper() if symbol_col else ""
    df["side"] = df[side_col].astype(str).str.strip().str.upper() if side_col else ""
    df["quantity"] = safe_float_series(df[qty_col]) if qty_col else 0.0
    df["notional"] = safe_float_series(df[notional_col]) if notional_col else 0.0
    df["realized_pnl"] = safe_float_series(df[realized_col]) if realized_col else 0.0

    if agent_col:
        agent_series = df[agent_col].fillna("").astype(str).str.strip()
    else:
        agent_series = pd.Series("", index=df.index, dtype=object)

    parsed_agent = pd.Series("", index=df.index, dtype=object)
    if rationale_col:
        extracted = df[rationale_col].fillna("").astype(str).str.extract(r"(?i)agent[\s_-]*([0-9]+)")[0]
        parsed_agent = extracted.fillna("").astype(str).str.strip()
        parsed_agent = parsed_agent.where(parsed_agent == "", "Agent " + parsed_agent)

    df["agent_label"] = agent_series.where(agent_series != "", parsed_agent)
    df["agent_label"] = df["agent_label"].fillna("").astype(str).str.strip().replace("", "Unknown")

    out = df[cols].copy()
    return out.sort_values("timestamp", na_position="last")


def _build_realized_vs_total_curve(df_perf_ts: pd.DataFrame, df_tx: pd.DataFrame, init_cap: float) -> pd.DataFrame:
    cols = ["timestamp", "realized_equity", "total_equity"]
    if df_perf_ts is None or df_perf_ts.empty:
        return pd.DataFrame(columns=cols)

    curve = pd.DataFrame(
        {
            "timestamp": df_perf_ts["timestamp"],
            "total_equity": df_perf_ts["total_value"],
        }
    ).sort_values("timestamp")

    if df_tx is None or df_tx.empty:
        curve["realized_equity"] = init_cap
        return curve

    realized = df_tx[(df_tx["realized_pnl"] != 0) & (df_tx["timestamp"].notna())][["timestamp", "realized_pnl"]].copy()
    realized = realized.sort_values("timestamp")

    if realized.empty:
        curve["realized_equity"] = init_cap
        return curve

    realized["cum_realized"] = realized["realized_pnl"].cumsum()
    merged = pd.merge_asof(
        curve,
        realized[["timestamp", "cum_realized"]],
        on="timestamp",
        direction="backward",
    )
    merged["cum_realized"] = merged["cum_realized"].fillna(0.0)
    merged["realized_equity"] = init_cap + merged["cum_realized"]
    return merged[cols]


def _build_trade_quality_dataframe(df_tx: pd.DataFrame) -> pd.DataFrame:
    out_cols = ["timestamp", "symbol", "agent_label", "duration_days", "trade_return_pct", "realized_pnl"]
    if df_tx is None or df_tx.empty:
        return pd.DataFrame(columns=out_cols)

    trades = df_tx.copy()
    trades = trades[~trades["symbol"].isin(["", "__RUN__"])].sort_values("timestamp", na_position="last")
    if trades.empty:
        return pd.DataFrame(columns=out_cols)

    inventory: dict[str, list[dict[str, object]]] = {}
    rows: list[dict[str, object]] = []

    for rec in trades.itertuples(index=False):
        side = str(rec.side or "").upper()
        symbol = str(rec.symbol or "").upper()
        qty = float(rec.quantity or 0)
        ts = rec.timestamp

        if not symbol or qty <= 0:
            continue

        if side == "BUY":
            inventory.setdefault(symbol, []).append({"qty": qty, "ts": ts})
            continue

        if side != "SELL":
            continue

        qty_left = qty
        matched_qty = 0.0
        weighted_days = 0.0

        for _ in range(10000):
            if qty_left <= 1e-9 or symbol not in inventory or not inventory[symbol]:
                break

            lot = inventory[symbol][0]
            lot_qty = float(lot.get("qty", 0) or 0)
            buy_ts = lot.get("ts")
            take_qty = min(qty_left, lot_qty)
            if take_qty <= 0:
                inventory[symbol].pop(0)
                continue

            if pd.notna(ts) and pd.notna(buy_ts):
                delta_days = (ts - buy_ts).total_seconds() / 86400
                weighted_days += max(delta_days, 0) * take_qty
                matched_qty += take_qty

            lot["qty"] = lot_qty - take_qty
            qty_left -= take_qty
            if float(lot["qty"]) <= 1e-9:
                inventory[symbol].pop(0)

        duration_days = (weighted_days / matched_qty) if matched_qty > 0 else pd.NA
        denom = abs(float(rec.notional or 0))
        trade_return_pct = (float(rec.realized_pnl or 0) / denom * 100) if denom > 0 else pd.NA

        if float(rec.realized_pnl or 0) != 0 or pd.notna(duration_days):
            rows.append(
                {
                    "timestamp": ts,
                    "symbol": symbol,
                    "agent_label": rec.agent_label,
                    "duration_days": duration_days,
                    "trade_return_pct": trade_return_pct,
                    "realized_pnl": float(rec.realized_pnl or 0),
                }
            )

    if not rows:
        return pd.DataFrame(columns=out_cols)
    return pd.DataFrame(rows).sort_values("timestamp", na_position="last")


def _build_underwater_dataframe(df_perf_ts: pd.DataFrame) -> pd.DataFrame:
    cols = ["timestamp", "drawdown_pct"]
    if df_perf_ts is None or df_perf_ts.empty:
        return pd.DataFrame(columns=cols)

    df = df_perf_ts[["timestamp", "total_value"]].copy().sort_values("timestamp")
    rolling_peak = df["total_value"].cummax().replace(0, pd.NA)
    df["drawdown_pct"] = ((df["total_value"] / rolling_peak) - 1.0) * 100
    df["drawdown_pct"] = df["drawdown_pct"].fillna(0.0)
    return df[cols]


def _compute_risk_scorecards(df_perf_ts: pd.DataFrame, df_tx: pd.DataFrame) -> dict[str, float]:
    sharpe = 0.0
    max_drawdown = 0.0

    if df_perf_ts is not None and not df_perf_ts.empty:
        rets = df_perf_ts["total_value"].pct_change().replace([float("inf"), float("-inf")], pd.NA).dropna()
        if len(rets) >= 2:
            std = float(rets.std(ddof=0))
            if std > 0:
                step_days = df_perf_ts["timestamp"].diff().dt.total_seconds().div(86400)
                step_days = step_days[step_days > 0]
                median_days = float(step_days.median()) if not step_days.empty else 1.0
                periods_per_year = 252 / median_days if median_days > 0 else 252
                sharpe = float((rets.mean() / std) * (periods_per_year ** 0.5))

        underwater = _build_underwater_dataframe(df_perf_ts)
        if not underwater.empty:
            max_drawdown = float(underwater["drawdown_pct"].min())

    profit_factor = 0.0
    win_rate = 0.0

    if df_tx is not None and not df_tx.empty:
        pnl = df_tx[df_tx["realized_pnl"] != 0]["realized_pnl"]
        if not pnl.empty:
            gross_profit = float(pnl[pnl > 0].sum())
            gross_loss = abs(float(pnl[pnl < 0].sum()))
            wins = int((pnl > 0).sum())
            losses = int((pnl < 0).sum())
            total_closed = wins + losses
            win_rate = (wins / total_closed * 100) if total_closed > 0 else 0.0
            if gross_loss > 0:
                profit_factor = gross_profit / gross_loss
            elif gross_profit > 0:
                profit_factor = float("inf")

    return {
        "sharpe": sharpe,
        "profit_factor": profit_factor,
        "win_rate": win_rate,
        "max_drawdown_pct": max_drawdown,
    }


def _make_return_gauge(value_pct: float, title: str, axis_limit: float) -> go.Figure:
    axis_limit = max(10.0, float(axis_limit))
    color = "#28a745" if value_pct >= 0 else "#dc3545"
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=value_pct,
            number={"suffix": "%", "font": {"size": 24, "color": "#fff"}},
            title={"text": title, "font": {"size": 14, "color": "#ccc"}},
            gauge={
                "axis": {"range": [-axis_limit, axis_limit], "tickcolor": "#666"},
                "bar": {"color": color, "thickness": 0.35},
                "steps": [
                    {"range": [-axis_limit, 0], "color": "rgba(220,53,69,0.25)"},
                    {"range": [0, axis_limit], "color": "rgba(40,167,69,0.25)"},
                ],
            },
        )
    )
    fig.update_layout(
        height=220,
        margin=dict(t=40, b=10, l=30, r=30),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#ccc"),
    )
    return fig


# ============================================================
# HELPERS V2 - Analyse Fondamentale V2 page
# ============================================================

FUNDAMENTAL_META = {
    "score": {
        "label": "Score de triage",
        "desc": "Score composite de conviction fondamentale (qualite, croissance, valorisation, sante financiere, consensus).",
        "higher_is_better": True,
        "good": 75,
        "warn": 60,
    },
    "risk_score": {
        "label": "Score de risque",
        "desc": "Niveau de risque fondamental. Plus le score est bas, meilleur est le profil de risque.",
        "higher_is_better": False,
        "good": 35,
        "warn": 55,
    },
    "quality_score": {
        "label": "Qualite du business",
        "desc": "Qualite du business (marges, ROE/ROA, capacite a generer du cash).",
        "higher_is_better": True,
        "good": 70,
        "warn": 55,
    },
    "growth_score": {
        "label": "Croissance",
        "desc": "Dynamique de croissance des revenus et benefices.",
        "higher_is_better": True,
        "good": 65,
        "warn": 50,
    },
    "valuation_score": {
        "label": "Valorisation",
        "desc": "Attractivite du prix relatif aux fondamentaux et a l'upside consensus.",
        "higher_is_better": True,
        "good": 65,
        "warn": 50,
    },
    "health_score": {
        "label": "Sante financiere",
        "desc": "Solidite du bilan (dette, liquidite, ratio de couverture).",
        "higher_is_better": True,
        "good": 70,
        "warn": 55,
    },
    "consensus_score": {
        "label": "Consensus analystes",
        "desc": "Qualite du support sell-side (recommandation, couverture, objectif de cours).",
        "higher_is_better": True,
        "good": 60,
        "warn": 45,
    },
    "data_coverage_pct": {
        "label": "Couverture de donnees",
        "desc": "Couverture des donnees disponibles. Faible couverture = confiance plus faible.",
        "higher_is_better": True,
        "good": 70,
        "warn": 45,
    },
}


def _funda_eval(key: str, value: float) -> tuple[str, str]:
    meta = FUNDAMENTAL_META.get(key, {})
    hib = bool(meta.get("higher_is_better", True))
    good = float(meta.get("good", 70))
    warn = float(meta.get("warn", 50))

    if hib:
        if value >= good:
            return ("Bon", "#28a745")
        if value >= warn:
            return ("Moyen", "#ffc107")
        return ("Faible", "#dc3545")

    if value <= good:
        return ("Bon", "#28a745")
    if value <= warn:
        return ("Moyen", "#ffc107")
    return ("Eleve", "#dc3545")


def _make_funda_gauge(value: float, title: str, inverse: bool = False) -> go.Figure:
    v = max(0.0, min(100.0, float(value)))
    if inverse:
        # Low = good for inverse metrics (e.g., risk score)
        steps = [
            {"range": [0, 35], "color": "rgba(40,167,69,0.25)"},
            {"range": [35, 60], "color": "rgba(255,193,7,0.25)"},
            {"range": [60, 100], "color": "rgba(220,53,69,0.25)"},
        ]
    else:
        steps = [
            {"range": [0, 35], "color": "rgba(220,53,69,0.25)"},
            {"range": [35, 60], "color": "rgba(255,193,7,0.25)"},
            {"range": [60, 100], "color": "rgba(40,167,69,0.25)"},
        ]

    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=v,
            number={"suffix": "/100", "font": {"size": 22, "color": "#fff"}},
            title={"text": title, "font": {"size": 13, "color": "#ccc"}},
            gauge={
                "axis": {"range": [0, 100], "tickcolor": "#666"},
                "bar": {"color": "#ffffff", "thickness": 0.28},
                "steps": steps,
            },
        )
    )
    fig.update_layout(
        height=190,
        margin=dict(t=35, b=8, l=24, r=24),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#ccc"),
    )
    return fig


def _safe_series(df: pd.DataFrame, candidates: list[str], default: float = 0.0) -> pd.Series:
    col = _first_existing_column(df, candidates)
    if col:
        return safe_float_series(df[col])
    return pd.Series(default, index=df.index, dtype=float)


def _clamp_pct(v: float) -> float:
    return max(0.0, min(100.0, float(v)))


def _estimate_scenario_probabilities(score: float, risk: float, upside_pct: float) -> dict[str, int]:
    """Heuristique locale (sans IA) pour afficher une probabilité relative des scénarios."""
    s = max(0.0, min(100.0, float(score)))
    r = max(0.0, min(100.0, float(risk)))
    u = float(upside_pct)

    bull_raw = max(5.0, 0.8 * s - 0.55 * r + max(u, 0.0) * 0.9 + 25.0)
    bear_raw = max(5.0, 0.95 * r - 0.45 * s + max(-u, 0.0) * 1.1 + 18.0)
    base_raw = max(5.0, 100.0 - abs(s - 60.0) - abs(u) * 0.45 + 10.0)

    total = bull_raw + bear_raw + base_raw
    bull = int(round((bull_raw / total) * 100))
    bear = int(round((bear_raw / total) * 100))
    base = 100 - bull - bear

    return {"baissier": bear, "central": base, "haussier": bull}


def _normalize_macro_news_df(df_macro: pd.DataFrame) -> pd.DataFrame:
    cols = ["publishedat", "impactscore", "winners", "losers", "theme", "regime", "title", "snippet", "notes", "source", "action", "reason"]
    if df_macro is None or df_macro.empty:
        return pd.DataFrame(columns=cols)

    wk = normalize_cols(df_macro.copy())
    ts_col = _first_existing_column(
        wk,
        ["published_at", "publishedat", "analyzed_at", "analyzedat", "last_seen_at", "updated_at", "updatedat", "created_at"],
    )
    impact_col = _first_existing_column(wk, ["impact_score", "impactscore"])

    wk["publishedat"] = pd.to_datetime(wk[ts_col], errors="coerce", utc=True) if ts_col else pd.NaT
    wk["impactscore"] = safe_float_series(wk[impact_col]) if impact_col else 0.0

    for c in ["winners", "losers", "theme", "regime", "title", "snippet", "notes", "source", "action", "reason"]:
        if c not in wk.columns:
            wk[c] = ""
        wk[c] = wk[c].fillna("").astype(str)

    keep = [c for c in cols if c in wk.columns]
    out = wk[keep].copy()
    if "publishedat" in out.columns:
        out = out.sort_values("publishedat", ascending=False, na_position="last")
    return out


def _normalize_symbol_news_df(df_symbol_news: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "symbol",
        "publishedat",
        "impactscore",
        "companyname",
        "title",
        "summary",
        "snippet",
        "sentiment",
        "urgency",
        "confidence",
        "action",
        "reason",
    ]
    if df_symbol_news is None or df_symbol_news.empty:
        return pd.DataFrame(columns=cols)

    wk = normalize_cols(df_symbol_news.copy())
    symbol_col = _first_existing_column(wk, ["symbol", "ticker", "symbols"])
    ts_col = _first_existing_column(
        wk,
        ["published_at", "publishedat", "analyzed_at", "analyzedat", "fetched_at", "fetchedat", "updated_at", "updatedat", "created_at"],
    )
    impact_col = _first_existing_column(wk, ["impact_score", "impactscore"])
    company_col = _first_existing_column(wk, ["company_name", "companyname", "name"])
    conf_col = _first_existing_column(wk, ["confidence_score", "confidence"])

    if symbol_col:
        raw_symbol = wk[symbol_col].astype(str)
        if symbol_col == "symbols":
            raw_symbol = raw_symbol.str.replace(r"[\[\]'\" ]", "", regex=True).str.split(",").str[0]
        wk["symbol"] = raw_symbol.str.strip().str.upper()
    else:
        wk["symbol"] = ""
    wk["publishedat"] = pd.to_datetime(wk[ts_col], errors="coerce", utc=True) if ts_col else pd.NaT
    wk["impactscore"] = safe_float_series(wk[impact_col]) if impact_col else 0.0
    wk["companyname"] = wk[company_col].fillna("").astype(str) if company_col else ""
    wk["confidence"] = safe_float_series(wk[conf_col]) if conf_col else 0.0

    for c in ["title", "summary", "snippet", "sentiment", "urgency", "action", "reason"]:
        if c not in wk.columns:
            wk[c] = ""
        wk[c] = wk[c].fillna("").astype(str)

    out = wk[[c for c in cols if c in wk.columns]].copy()
    out = out[out["symbol"] != ""]
    if "publishedat" in out.columns:
        out = out.sort_values("publishedat", ascending=False, na_position="last")
    return out


def _load_fundamentals_for_dashboard(duckdb_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    df = duckdb_data.get("df_funda_latest", pd.DataFrame())
    if df is None or df.empty:
        hist = duckdb_data.get("df_funda_history", pd.DataFrame())
        if hist is None or hist.empty:
            return pd.DataFrame()
        wk = normalize_cols(hist.copy())
        if "symbol" not in wk.columns:
            return pd.DataFrame()
        ts_col = _first_existing_column(wk, ["updated_at", "fetched_at", "created_at"])
        if ts_col:
            wk[ts_col] = pd.to_datetime(wk[ts_col], errors="coerce", utc=True)
            wk = wk.dropna(subset=[ts_col]).sort_values(ts_col, ascending=False)
        wk["symbol"] = wk["symbol"].astype(str).str.strip().str.upper()
        df = wk.drop_duplicates(subset=["symbol"], keep="first")
    else:
        df = normalize_cols(df.copy())
        if "symbol" in df.columns:
            df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()

    ren = {
        "updated_at": "updatedat",
        "fetched_at": "fetchedat",
        "current_price": "lastprice",
        "next_steps": "nextsteps",
    }
    for src, dst in ren.items():
        if src in df.columns and dst not in df.columns:
            df[dst] = df[src]
    return df


def _clean_context_token(v: object) -> str:
    s = str(v or "").strip().lower()
    if s in ("", "n/a", "na", "nan", "none", "unknown", "indefini", "indefinie", "indef"):
        return ""
    return s


def _synthesis_conclusion(score: float, tech_action: str) -> str:
    action = str(tech_action or "").upper().strip()
    if score >= 75 and action == "BUY":
        return "Conviction forte haussiere"
    if score >= 65:
        return "Biais positif (selectionnable)"
    if score >= 50:
        return "Neutre / Watch"
    if score >= 35:
        return "Prudence (risque eleve)"
    return "Defensif / a eviter"


def _prepare_multi_agent_view(
    df_universe: pd.DataFrame,
    df_tech_latest: pd.DataFrame,
    df_funda_latest: pd.DataFrame,
    df_macro_news: pd.DataFrame,
    df_symbol_news: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Universe
    u = normalize_cols(df_universe.copy()) if df_universe is not None and not df_universe.empty else pd.DataFrame()
    if "symbol" in u.columns:
        u["symbol"] = u["symbol"].astype(str).str.strip().str.upper()
        u = u[u["symbol"] != ""]

    # Tech latest
    tech = normalize_cols(df_tech_latest.copy()) if df_tech_latest is not None and not df_tech_latest.empty else pd.DataFrame()
    if "symbol" in tech.columns:
        tech["symbol"] = tech["symbol"].astype(str).str.strip().str.upper()
    action_col = _first_existing_column(tech, ["d1_action", "action", "signal"]) if not tech.empty else None
    conf_col = _first_existing_column(tech, ["d1_confidence", "confidence", "d1_score"]) if not tech.empty else None
    ts_col_tech = _first_existing_column(tech, ["workflow_date", "d1_date", "updated_at", "created_at"]) if not tech.empty else None
    if ts_col_tech:
        tech["last_tech_date"] = pd.to_datetime(tech[ts_col_tech], errors="coerce", utc=True)
    else:
        tech["last_tech_date"] = pd.NaT
    if action_col:
        tech["tech_action"] = tech[action_col].fillna("").astype(str).str.upper().str.strip()
    else:
        tech["tech_action"] = ""
    tech["tech_confidence"] = safe_float_series(tech[conf_col]) if conf_col else 0.0
    keep_tech = [
        c
        for c in [
            "symbol",
            "tech_action",
            "tech_confidence",
            "last_close",
            "d1_rsi14",
            "d1_macd_hist",
            "d1_atr_pct",
            "d1_resistance",
            "d1_support",
            "d1_dist_res_pct",
            "d1_dist_sup_pct",
            "ai_stop_loss",
            "ai_rr_theoretical",
            "ai_decision",
            "ai_alignment",
            "ai_regime_d1",
            "data_age_h1_hours",
            "data_age_d1_hours",
            "last_tech_date",
        ]
        if c in tech.columns
    ]
    tech = tech[keep_tech].drop_duplicates(subset=["symbol"], keep="first") if "symbol" in keep_tech else pd.DataFrame()

    # Funda latest
    funda = normalize_cols(df_funda_latest.copy()) if df_funda_latest is not None and not df_funda_latest.empty else pd.DataFrame()
    if "symbol" in funda.columns:
        funda["symbol"] = funda["symbol"].astype(str).str.strip().str.upper()
    ts_col_funda = _first_existing_column(funda, ["updated_at", "fetched_at", "created_at", "updatedat"]) if not funda.empty else None
    if ts_col_funda:
        funda["last_funda_date"] = pd.to_datetime(funda[ts_col_funda], errors="coerce", utc=True)
    else:
        funda["last_funda_date"] = pd.NaT
    score_col = _first_existing_column(funda, ["score", "funda_conf"]) if not funda.empty else None
    risk_col = _first_existing_column(funda, ["risk_score"]) if not funda.empty else None
    upside_col = _first_existing_column(funda, ["upside_pct"]) if not funda.empty else None
    horizon_col = _first_existing_column(funda, ["horizon"]) if not funda.empty else None
    funda["funda_score"] = safe_float_series(funda[score_col]) if score_col else 50.0
    funda["funda_risk"] = safe_float_series(funda[risk_col]) if risk_col else 50.0
    funda["funda_upside"] = safe_float_series(funda[upside_col]) if upside_col else 0.0
    funda["funda_horizon"] = funda[horizon_col].fillna("").astype(str) if horizon_col else ""
    if "name" in funda.columns:
        funda["funda_name"] = funda["name"].fillna("").astype(str)
    if "sector" in funda.columns:
        funda["funda_sector"] = funda["sector"].fillna("").astype(str)
    if "industry" in funda.columns:
        funda["funda_industry"] = funda["industry"].fillna("").astype(str)
    keep_funda = [
        c
        for c in [
            "symbol",
            "funda_name",
            "funda_sector",
            "funda_industry",
            "funda_score",
            "funda_risk",
            "funda_upside",
            "funda_horizon",
            "recommendation",
            "target_price",
            "current_price",
            "analyst_count",
            "quality_score",
            "growth_score",
            "valuation_score",
            "health_score",
            "consensus_score",
            "last_funda_date",
        ]
        if c in funda.columns
    ]
    funda = funda[keep_funda].drop_duplicates(subset=["symbol"], keep="first") if "symbol" in keep_funda else pd.DataFrame()

    # News
    macro = _normalize_macro_news_df(df_macro_news)
    sym_news = _normalize_symbol_news_df(df_symbol_news)

    symbol_pool: set[str] = set()
    for df, col in [(u, "symbol"), (tech, "symbol"), (funda, "symbol"), (sym_news, "symbol")]:
        if df is not None and not df.empty and col in df.columns:
            vals = df[col].dropna().astype(str).str.strip().str.upper()
            symbol_pool.update([v for v in vals.tolist() if v])

    base = pd.DataFrame({"symbol": sorted(symbol_pool)})
    if base.empty:
        return pd.DataFrame(), macro, sym_news

    if not u.empty and "symbol" in u.columns:
        cols_u = [c for c in ["symbol", "name", "sector", "industry"] if c in u.columns]
        base = base.merge(u[cols_u].drop_duplicates(subset=["symbol"], keep="first"), on="symbol", how="left")

    for c in ["name", "sector", "industry"]:
        if c not in base.columns:
            base[c] = ""
        base[c] = base[c].fillna("").astype(str)

    if not tech.empty:
        base = base.merge(tech, on="symbol", how="left")
    if not funda.empty:
        base = base.merge(funda, on="symbol", how="left")
        for src, dst in [("funda_name", "name"), ("funda_sector", "sector"), ("funda_industry", "industry")]:
            if src in base.columns:
                current = base[dst].fillna("").astype(str) if dst in base.columns else pd.Series("", index=base.index, dtype=object)
                fallback = base[src].fillna("").astype(str)
                base[dst] = current.where(current.str.strip() != "", fallback)
                base = base.drop(columns=[src])

    # Aggregate symbol news by recency.
    sym_agg = pd.DataFrame(columns=["symbol", "symbol_news_last_date", "symbol_news_count_7d", "symbol_news_count_30d", "symbol_news_impact_7d", "symbol_news_impact_30d"])
    if not sym_news.empty and "symbol" in sym_news.columns and "publishedat" in sym_news.columns:
        cut7 = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=7)
        cut30 = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=30)
        wk = sym_news.copy()
        wk["is_7d"] = wk["publishedat"] >= cut7
        wk["is_30d"] = wk["publishedat"] >= cut30
        wk["impact_7d"] = wk["impactscore"].where(wk["is_7d"], 0.0)
        wk["impact_30d"] = wk["impactscore"].where(wk["is_30d"], 0.0)
        sym_agg = (
            wk.groupby("symbol", as_index=False)
            .agg(
                symbol_news_last_date=("publishedat", "max"),
                symbol_news_count_7d=("is_7d", "sum"),
                symbol_news_count_30d=("is_30d", "sum"),
                symbol_news_impact_7d=("impact_7d", "sum"),
                symbol_news_impact_30d=("impact_30d", "sum"),
            )
        )

    base = base.merge(sym_agg, on="symbol", how="left")

    # Aggregate macro context by sector/industry matching.
    base["sector_token"] = base["sector"].map(_clean_context_token)
    base["industry_token"] = base["industry"].map(_clean_context_token)

    macro_map_rows = []
    if not macro.empty:
        cut30 = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=30)
        macro_recent = macro[macro["publishedat"] >= cut30].copy() if "publishedat" in macro.columns else macro.copy()
        if not macro_recent.empty:
            text_cols = [c for c in ["theme", "title", "snippet", "notes", "winners", "losers", "regime", "source"] if c in macro_recent.columns]
            if text_cols:
                macro_recent["_ctx"] = macro_recent[text_cols].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
            else:
                macro_recent["_ctx"] = ""
        else:
            macro_recent["_ctx"] = ""
    else:
        macro_recent = pd.DataFrame(columns=["publishedat", "impactscore", "_ctx", "theme"])

    for sec, ind in base[["sector_token", "industry_token"]].drop_duplicates().itertuples(index=False):
        if macro_recent.empty or (not sec and not ind):
            macro_map_rows.append(
                {
                    "sector_token": sec,
                    "industry_token": ind,
                    "macro_news_count_30d": 0,
                    "macro_impact_30d": 0.0,
                    "macro_last_date": pd.NaT,
                    "macro_themes": "",
                }
            )
            continue

        mask = pd.Series(False, index=macro_recent.index)
        if sec:
            mask = mask | macro_recent["_ctx"].str.contains(re.escape(sec), regex=True, na=False)
        if ind and ind != sec:
            mask = mask | macro_recent["_ctx"].str.contains(re.escape(ind), regex=True, na=False)

        matched = macro_recent[mask]
        if matched.empty:
            macro_map_rows.append(
                {
                    "sector_token": sec,
                    "industry_token": ind,
                    "macro_news_count_30d": 0,
                    "macro_impact_30d": 0.0,
                    "macro_last_date": pd.NaT,
                    "macro_themes": "",
                }
            )
            continue

        themes = (
            matched.get("theme", pd.Series(dtype=str))
            .fillna("")
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .value_counts()
            .head(2)
            .index
            .tolist()
        )
        macro_map_rows.append(
            {
                "sector_token": sec,
                "industry_token": ind,
                "macro_news_count_30d": int(len(matched)),
                "macro_impact_30d": float(matched["impactscore"].sum()) if "impactscore" in matched.columns else 0.0,
                "macro_last_date": matched["publishedat"].max() if "publishedat" in matched.columns else pd.NaT,
                "macro_themes": ", ".join(themes),
            }
        )

    macro_map = pd.DataFrame(macro_map_rows)
    if not macro_map.empty:
        base = base.merge(macro_map, on=["sector_token", "industry_token"], how="left")

    # Fill defaults.
    for c in [
        "tech_action",
        "funda_horizon",
        "recommendation",
        "macro_themes",
    ]:
        if c not in base.columns:
            base[c] = ""
        base[c] = base[c].fillna("").astype(str)

    for c in [
        "tech_confidence",
        "last_close",
        "d1_rsi14",
        "d1_macd_hist",
        "d1_atr_pct",
        "d1_resistance",
        "d1_support",
        "d1_dist_res_pct",
        "d1_dist_sup_pct",
        "ai_stop_loss",
        "ai_rr_theoretical",
        "funda_score",
        "funda_risk",
        "funda_upside",
        "target_price",
        "current_price",
        "analyst_count",
        "quality_score",
        "growth_score",
        "valuation_score",
        "health_score",
        "consensus_score",
        "symbol_news_count_7d",
        "symbol_news_count_30d",
        "symbol_news_impact_7d",
        "symbol_news_impact_30d",
        "macro_news_count_30d",
        "macro_impact_30d",
    ]:
        if c not in base.columns:
            base[c] = 0.0
        base[c] = pd.to_numeric(base[c], errors="coerce").fillna(0.0)

    if "symbol_news_last_date" not in base.columns:
        base["symbol_news_last_date"] = pd.NaT
    if "macro_last_date" not in base.columns:
        base["macro_last_date"] = pd.NaT

    # Composite score and conclusion.
    action_upper = base["tech_action"].str.upper()
    tech_component = action_upper.map({"BUY": 18.0, "SELL": -18.0}).fillna(0.0)
    funda_component = ((base["funda_score"] - 50.0) / 50.0) * 22.0
    risk_penalty = (base["funda_risk"] - 55.0).clip(lower=0) * 0.35
    symbol_news_component = (base["symbol_news_impact_7d"].clip(-8, 8) * 1.7) + (base["symbol_news_count_7d"].clip(0, 6) * 0.8)
    macro_component = base["macro_impact_30d"].clip(-10, 10) * 1.2

    base["conviction_score"] = (50.0 + tech_component + funda_component + symbol_news_component + macro_component - risk_penalty).clip(0, 100).round(1)
    base["conclusion"] = base.apply(
        lambda r: _synthesis_conclusion(float(r.get("conviction_score", 50.0)), str(r.get("tech_action", ""))),
        axis=1,
    )
    base["last_news_date"] = base["symbol_news_last_date"].combine_first(base["macro_last_date"])

    base = base.sort_values("conviction_score", ascending=False, na_position="last").reset_index(drop=True)
    return base, macro, sym_news


# ============================================================
# MAIN APP
# ============================================================

st.sidebar.title("TradingSim AI")
page = st.sidebar.radio(
    "Navigation",
    [
        "Dashboard Trading",
        "System Health (Monitoring)",
        "Vue consolidee Multi-Agents",
        "Analyse Technique V2",
        "Analyse Fondamentale V2",
    ],
)

data_dict = load_data()
if not data_dict:
    st.warning("Donnees Google Sheets indisponibles. Les vues basees DuckDB (System Health, Vue consolidee, Analyse V2) restent disponibles.")

# Load DuckDB data (non-blocking)
duckdb_data = load_duckdb_data()

# ------------------------------------------------------------
# PRE-CALCULS (ROBUSTES)
# ------------------------------------------------------------

df_univ = data_dict.get("Universe", pd.DataFrame()) if data_dict else pd.DataFrame()
if (df_univ is None or df_univ.empty) and duckdb_data:
    df_univ = duckdb_data.get("df_universe", pd.DataFrame())
df_port = enrich_df_with_name(data_dict.get("Portefeuille", pd.DataFrame()), df_univ) if data_dict else pd.DataFrame()
df_perf = data_dict.get("Performance", pd.DataFrame()) if data_dict else pd.DataFrame()
df_trans = enrich_df_with_name(data_dict.get("Transactions", pd.DataFrame()), df_univ) if data_dict else pd.DataFrame()
df_prices = data_dict.get("Market_Prices", pd.DataFrame()) if data_dict else pd.DataFrame()

total_val = 0.0
cash = 0.0
invest = 0.0
init_cap = 50000.0

if df_port is not None and not df_port.empty:
    if "symbol" not in df_port.columns:
        df_port["symbol"] = ""
    if "marketvalue" not in df_port.columns:
        df_port["marketvalue"] = 0

    df_port["mv_num"] = df_port["marketvalue"].apply(safe_float)
    sym_up = df_port["symbol"].astype(str).str.strip().str.upper()

    cash = df_port[sym_up == "CASH_EUR"]["mv_num"].sum()
    invest = df_port[~sym_up.isin(["CASH_EUR", "__META__"])]["mv_num"].sum()
    total_val = cash + invest

    meta = df_port[sym_up == "__META__"]
    if not meta.empty:
        val_meta = meta.iloc[0].get("marketvalue")
        if val_meta:
            init_cap = safe_float(val_meta)
        else:
            try:
                notes = safe_json_parse(meta.iloc[0].get("notes", ""))

                # Robust: accepter plusieurs variantes de clés
                init_cap = None
                for k in ["initialCapitalEUR", "initialcapitaleur", "initial_capital_eur"]:
                    if k in notes:
                        init_cap = safe_float(notes.get(k))
                        break
                if init_cap is None:
                    init_cap = 50000.0

            except Exception:
                init_cap = 50000.0

roi = (total_val - init_cap) / init_cap if init_cap else 0
cash_pct = (cash / total_val) * 100 if total_val else 0


# ============================================================
# PAGE 1: DASHBOARD
# ============================================================

if page == "Dashboard Trading":
    if not data_dict:
        st.error("Donnees Google Sheets requises pour cette page.")
        st.stop()

    st.title("AI Trading Executor Dashboard")

    if st.button("Rafraichir"):
        load_data.clear()
        load_duckdb_data.clear()
        st.rerun()

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Capital Depart", f"{init_cap:,.0f} EUR")
    c2.metric("Valeur Totale", f"{total_val:,.2f} EUR", delta=f"{total_val - init_cap:,.2f} EUR")
    c3.metric("Cash", f"{cash:,.2f} EUR")
    c4.metric("Investi", f"{invest:,.2f} EUR")
    c5.metric("ROI", f"{roi * 100:.2f} %")
    c6.metric("% Cash", f"{cash_pct:.1f} %")

    t1, t2, t3, t4 = st.tabs(["Portefeuille", "Performance", "Cerveau IA", "Marche & Recherche"])

    # TAB 1: PORTEFEUILLE
    with t1:
        if df_port is not None and not df_port.empty:
            df_clean = df_port[~df_port["symbol"].astype(str).str.upper().isin(["__META__"])].copy()

            for c in ["marketvalue", "unrealizedpnl", "quantity", "avgprice", "lastprice"]:
                if c in df_clean.columns:
                    df_clean[c] = df_clean[c].apply(safe_float)

            for col in ["sector", "industry", "name", "assetclass"]:
                if col not in df_clean.columns:
                    df_clean[col] = ""

            df_clean.loc[
                df_clean["symbol"].astype(str).str.upper() == "CASH_EUR",
                ["sector", "industry", "name", "assetclass"],
            ] = "Cash"

            st.subheader("📊 Allocation")
            cc1, cc2, cc3 = st.columns(3)

            def pie(df: pd.DataFrame, col: str) -> px.pie:
                if col not in df.columns or "marketvalue" not in df.columns:
                    return None
                fig = px.pie(df, values="marketvalue", names=col, hole=0.4)
                fig.update_layout(margin=dict(t=0, b=20, l=0, r=0), height=300)
                return fig

            with cc1:
                st.caption("Secteur")
                fig = pie(df_clean, "sector")
                if fig:
                    st.plotly_chart(fig, use_container_width=True)

            with cc2:
                st.caption("Industrie")
                fig = pie(df_clean, "industry")
                if fig:
                    st.plotly_chart(fig, use_container_width=True)

            with cc3:
                st.caption("Classe")
                fig = pie(df_clean, "assetclass")
                if fig:
                    st.plotly_chart(fig, use_container_width=True)

            st.divider()
            st.subheader("Portfolio Sparklines (90j)")
            render_portfolio_sparklines(
                df_clean,
                df_trans,
                yfinance_api_url=YFINANCE_API_URL,
                lookback_days=90,
                columns_per_row=3,
            )

            st.divider()
            st.subheader("Positions")
            qty_basis = (
                df_clean.get("avgprice", pd.Series(0.0, index=df_clean.index))
                * df_clean.get("quantity", pd.Series(0.0, index=df_clean.index))
            )
            mv_basis = (
                df_clean.get("marketvalue", pd.Series(0.0, index=df_clean.index))
                - df_clean.get("unrealizedpnl", pd.Series(0.0, index=df_clean.index))
            )
            cost_basis = qty_basis.where(qty_basis > 0, mv_basis)
            df_clean["unrealizedpnl_pct"] = 0.0
            valid_basis = cost_basis != 0
            df_clean.loc[valid_basis, "unrealizedpnl_pct"] = (
                df_clean.loc[valid_basis, "unrealizedpnl"] / cost_basis[valid_basis] * 100
            )
            df_clean["unrealizedpnl_pct"] = df_clean["unrealizedpnl_pct"].round(2)

            cols_show = [
                "name",
                "symbol",
                "sector",
                "industry",
                "quantity",
                "avgprice",
                "lastprice",
                "marketvalue",
                "unrealizedpnl",
                "unrealizedpnl_pct",
            ]
            cols_exist = [c for c in cols_show if c in df_clean.columns]
            df_view = df_clean[cols_exist].copy()
            if "marketvalue" in df_view.columns:
                df_view = df_view.sort_values("marketvalue", ascending=False)
            render_interactive_table(df_view, key_suffix="positions", hide_index=True)
        else:
            st.info("Portefeuille vide.")

    # TAB 2: PERFORMANCE
    with t2:
        perf_ts = _prepare_performance_timeseries(df_perf)
        tx_norm = _prepare_transactions(df_trans)

        latent_pnl = 0.0
        if df_port is not None and not df_port.empty and "unrealizedpnl" in df_port.columns:
            sym = df_port.get("symbol", pd.Series("", index=df_port.index)).astype(str).str.upper().str.strip()
            latent_pnl = float(safe_float_series(df_port[~sym.isin(["CASH_EUR", "__META__"])]["unrealizedpnl"]).sum())

        realized_pnl = float(tx_norm["realized_pnl"].sum()) if not tx_norm.empty else 0.0
        total_gain = total_val - init_cap

        v_pnl, v_eff, v_quality, v_risk = st.tabs(
            [
                "1) Performance Financiere",
                "2) Efficacite du Capital",
                "3) Qualite du Trading",
                "4) Risque",
            ]
        )

        with v_pnl:
            m1, m2, m3 = st.columns(3)
            m1.metric("P&L realise", f"{realized_pnl:,.2f} EUR")
            m2.metric("P&L latent", f"{latent_pnl:,.2f} EUR")
            m3.metric("Gain total", f"{total_gain:,.2f} EUR")

            fig_wf = go.Figure(
                go.Waterfall(
                    orientation="v",
                    measure=["absolute", "relative", "relative", "total"],
                    x=["Capital initial", "P&L realise", "P&L latent", "Total equity"],
                    y=[init_cap, realized_pnl, latent_pnl, 0],
                    increasing={"marker": {"color": "#28a745"}},
                    decreasing={"marker": {"color": "#dc3545"}},
                    totals={"marker": {"color": "#0d6efd"}},
                    connector={"line": {"color": "#666"}},
                )
            )
            fig_wf.update_layout(title="Cascade de valeur", height=360, margin=dict(t=50, b=20, l=20, r=20))
            st.plotly_chart(fig_wf, use_container_width=True)

            curve = _build_realized_vs_total_curve(perf_ts, tx_norm, init_cap)
            if curve.empty:
                st.info("Donnees insuffisantes pour la courbe Realise vs Totale.")
            else:
                fig_curve = go.Figure()
                fig_curve.add_trace(
                    go.Scatter(
                        x=curve["timestamp"],
                        y=curve["realized_equity"],
                        mode="lines",
                        name="Capital + gains realises",
                        line=dict(width=2.5, color="#00c2ff"),
                    )
                )
                fig_curve.add_trace(
                    go.Scatter(
                        x=curve["timestamp"],
                        y=curve["total_equity"],
                        mode="lines",
                        name="Valeur liquidative",
                        line=dict(width=2.5, color="#ffffff", dash="dash"),
                    )
                )
                fig_curve.update_layout(
                    title="Equity realisee vs equity totale",
                    height=360,
                    margin=dict(t=50, b=20, l=20, r=20),
                )
                st.plotly_chart(fig_curve, use_container_width=True)

        with v_eff:
            if perf_ts.empty:
                st.info("Donnees insuffisantes pour l'efficacite du capital.")
            else:
                eff = _append_current_efficiency_point(
                    perf_ts.copy(),
                    total_value=total_val,
                    cash_value=cash,
                    invested_value=invest,
                )
                eff["total_value"] = eff["total_value"].replace(0, pd.NA)
                eff["cash_pct"] = (eff["cash_value"] / eff["total_value"] * 100).fillna(0.0).clip(lower=0, upper=100)
                eff["invested_pct"] = (eff["invested_value"] / eff["total_value"] * 100).fillna(0.0).clip(lower=0, upper=100)
                eff["roi_pct"] = ((eff["total_value"] / init_cap) - 1.0) * 100 if init_cap else 0.0
                inv_base = eff["invested_value"].replace(0, pd.NA)
                eff["roic_pct"] = ((eff["total_value"] - init_cap) / inv_base) * 100
                eff = eff.fillna(0.0)

                fig_eff = go.Figure()
                fig_eff.add_trace(
                    go.Scatter(
                        x=eff["timestamp"],
                        y=eff["cash_pct"],
                        mode="lines",
                        name="% Cash",
                        stackgroup="alloc",
                        line=dict(width=0.7, color="#4e79a7"),
                    )
                )
                fig_eff.add_trace(
                    go.Scatter(
                        x=eff["timestamp"],
                        y=eff["invested_pct"],
                        mode="lines",
                        name="% Investi",
                        stackgroup="alloc",
                        line=dict(width=0.7, color="#59a14f"),
                    )
                )
                fig_eff.add_trace(
                    go.Scatter(
                        x=eff["timestamp"],
                        y=eff["roi_pct"],
                        mode="lines",
                        name="ROI global",
                        yaxis="y2",
                        line=dict(width=2.5, color="#ff9d00"),
                    )
                )
                fig_eff.add_trace(
                    go.Scatter(
                        x=eff["timestamp"],
                        y=eff["roic_pct"],
                        mode="lines",
                        name="ROIC",
                        yaxis="y2",
                        line=dict(width=2.3, color="#00d084", dash="dot"),
                    )
                )
                fig_eff.update_layout(
                    title="Exposition capital vs performance (ROI + ROIC)",
                    height=380,
                    margin=dict(t=50, b=20, l=20, r=20),
                    yaxis=dict(title="Allocation (%)", range=[0, 100]),
                    yaxis2=dict(title="ROI / ROIC (%)", overlaying="y", side="right", showgrid=False),
                )
                st.plotly_chart(fig_eff, use_container_width=True)

                roi_global_pct = (total_gain / init_cap * 100) if init_cap else 0.0
                roic_pct = (total_gain / invest * 100) if invest > 0 else 0.0
                gauge_axis = max(20.0, abs(roi_global_pct), abs(roic_pct)) * 1.25

                g1, g2 = st.columns(2)
                with g1:
                    st.plotly_chart(
                        _make_return_gauge(roi_global_pct, "ROI Global", gauge_axis),
                        use_container_width=True,
                    )
                with g2:
                    st.plotly_chart(
                        _make_return_gauge(roic_pct, "ROIC", gauge_axis),
                        use_container_width=True,
                    )
                st.caption(
                    "ROI = (Valeur totale - Capital initial) / Capital initial. "
                    f"ROIC = (Valeur totale - Capital initial) / Capital investi a date ({invest:,.2f} EUR). "
                    "La courbe est ancree avec un point courant portefeuille pour aligner la derniere valeur avec les KPI."
                )

        with v_quality:
            quality_df = _build_trade_quality_dataframe(tx_norm)
            hist_df = quality_df.dropna(subset=["trade_return_pct"]) if not quality_df.empty else pd.DataFrame()
            dur_df = quality_df.dropna(subset=["duration_days"]) if not quality_df.empty else pd.DataFrame()

            if hist_df.empty:
                st.info("Pas assez de trades closes pour analyser la distribution des returns.")
            else:
                fig_hist = px.histogram(
                    hist_df,
                    x="trade_return_pct",
                    nbins=28,
                    title="Distribution des returns par trade (%)",
                )
                fig_hist.add_vline(x=0, line_dash="dash", line_color="#999")
                fig_hist.update_layout(height=340, margin=dict(t=50, b=20, l=20, r=20))
                st.plotly_chart(fig_hist, use_container_width=True)

            if dur_df.empty:
                st.info("Pas assez d'historique pour la distribution des durees de detention.")
            else:
                bins = [-0.001, 1, 3, 7, 14, 21, 30, 45, 60, 90, 120, 180, 365, float("inf")]
                labels = [
                    "<=1j", "2-3j", "4-7j", "8-14j", "15-21j", "22-30j",
                    "31-45j", "46-60j", "61-90j", "91-120j", "121-180j", "181-365j", ">365j",
                ]
                dur_work = dur_df.copy()
                dur_work["duration_bucket"] = pd.cut(
                    dur_work["duration_days"].astype(float),
                    bins=bins,
                    labels=labels,
                    include_lowest=True,
                )
                dist = (
                    dur_work.groupby("duration_bucket", observed=False)
                    .size()
                    .reindex(labels, fill_value=0)
                    .reset_index(name="count")
                )
                fig_dur = px.bar(
                    dist,
                    x="duration_bucket",
                    y="count",
                    text="count",
                    title="Distribution des durees de detention (classes)",
                    labels={"duration_bucket": "Classe de duree", "count": "Nombre de positions closes"},
                )
                fig_dur.update_layout(height=360, margin=dict(t=50, b=20, l=20, r=20))
                st.plotly_chart(fig_dur, use_container_width=True)
                st.caption("Objectif: visualiser la forme de distribution des durees de detention (profil court, swing, long).")

        with v_risk:
            st.markdown(
                "Lecture rapide: le drawdown mesure l'ecart (%) entre la valeur du portefeuille et son plus-haut historique."
            )
            st.markdown(
                "- `0%` : nouveau plus-haut.\n"
                "- valeur negative : perte temporaire depuis le dernier pic.\n"
                "- `Max Drawdown` : pire creux observe sur la periode."
            )

            cards = _compute_risk_scorecards(perf_ts, tx_norm)
            underwater = _build_underwater_dataframe(perf_ts)
            if underwater.empty:
                st.info("Pas d'historique suffisant pour le drawdown.")
            else:
                fig_dd = go.Figure()
                fig_dd.add_trace(
                    go.Scatter(
                        x=underwater["timestamp"],
                        y=underwater["drawdown_pct"],
                        mode="lines",
                        fill="tozeroy",
                        line=dict(color="#dc3545", width=2),
                        name="Drawdown",
                        hovertemplate="%{x|%Y-%m-%d}<br>Drawdown: %{y:.2f}%<extra></extra>",
                    )
                )
                fig_dd.add_hline(
                    y=float(cards.get("max_drawdown_pct", 0.0)),
                    line_dash="dot",
                    line_color="#ff9d00",
                    annotation_text=f"Max DD: {float(cards.get('max_drawdown_pct', 0.0)):.2f}%",
                    annotation_position="bottom right",
                )
                fig_dd.update_layout(
                    title="Drawdown du portefeuille (ecart au plus-haut)",
                    height=360,
                    margin=dict(t=50, b=20, l=20, r=20),
                    yaxis=dict(title="Drawdown", ticksuffix="%"),
                )
                st.plotly_chart(fig_dd, use_container_width=True)

            if perf_ts is not None and not perf_ts.empty:
                ret = (
                    perf_ts["total_value"]
                    .pct_change()
                    .replace([float("inf"), float("-inf")], pd.NA)
                    .dropna()
                    * 100
                )
                if not ret.empty:
                    fig_ret = px.histogram(
                        ret.to_frame(name="ret"),
                        x="ret",
                        nbins=30,
                        title="Distribution des variations periodiques (%)",
                        labels={"ret": "Variation (%)"},
                    )
                    fig_ret.add_vline(x=0, line_dash="dash", line_color="#999")
                    fig_ret.update_layout(height=280, margin=dict(t=45, b=20, l=20, r=20))
                    st.plotly_chart(fig_ret, use_container_width=True)

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Sharpe Ratio", f"{cards['sharpe']:.2f}")
            pf_val = cards["profit_factor"]
            c2.metric("Profit Factor", "inf" if pf_val == float("inf") else f"{pf_val:.2f}")
            c3.metric("Win Rate", f"{cards['win_rate']:.1f}%")
            c4.metric("Max Drawdown", f"{cards['max_drawdown_pct']:.2f}%")
            st.caption(
                "Repere: Sharpe > 1 = rendement ajuste du risque correct, Profit Factor > 1.5 = robustesse des trades, "
                "Max Drawdown proche de 0 = profondeur des creux limitee."
            )

    # TAB 3: CERVEAU IA
    with t3:
        df_sig = enrich_df_with_name(data_dict.get("AI_Signals", pd.DataFrame()), df_univ)
        df_alt = enrich_df_with_name(data_dict.get("Alerts", pd.DataFrame()), df_univ)

        st.subheader("🚦 Signaux")
        if df_sig is not None and not df_sig.empty:
            if "rationale" in df_sig.columns:
                df_sig["rationale"] = df_sig["rationale"].apply(clean_text)
            render_interactive_table(df_sig, key_suffix="sig")
        else:
            st.caption("Aucun signal.")

        st.subheader("Alertes")
        if df_alt is not None and not df_alt.empty:
            render_interactive_table(df_alt, key_suffix="alt")
        else:
            st.caption("RAS")

    # TAB 4: MARCHE & RECHERCHE
    with t4:
        df_news = _normalize_macro_news_df(duckdb_data.get("df_news_macro_history", pd.DataFrame()))
        df_news_sym = _normalize_symbol_news_df(duckdb_data.get("df_news_symbol_history", pd.DataFrame()))
        df_res = _load_fundamentals_for_dashboard(duckdb_data)
        df_res = enrich_df_with_name(df_res, df_univ)

        st_macro, st_research = st.tabs(["Macro & Buzz", "Recherche"])

        with st_macro:
            st.subheader("Meteo Secteurs (30j)")
            if df_news is not None and not df_news.empty:
                df_sec = calculate_sector_sentiment(df_news)
                if df_sec is not None and not df_sec.empty:
                    fig = px.bar(df_sec, x="NetScore", y="Sector", orientation="h", title="Momentum Sectoriel", text="NetScore")
                    fig.update_traces(marker_color=df_sec["Color"])
                    st.plotly_chart(fig, use_container_width=True)

            st.divider()
            st.subheader("Palmares Actions (30j)")
            if df_news_sym is not None and not df_news_sym.empty:
                df_sym = calculate_symbol_momentum(df_news_sym)
                if df_sym is not None and not df_sym.empty:
                    fig = px.bar(df_sym, x="NetScore", y="Label", orientation="h", title="Momentum Actions", text="NetScore")
                    fig.update_traces(marker_color=df_sym["Color"])
                    st.plotly_chart(fig, use_container_width=True)

        with st_research:
            if df_res is None or df_res.empty:
                st.info("Aucune note de recherche disponible.")
            else:
                df_viz = df_res.copy()

                if "score" in df_viz.columns:
                    df_viz["score_num"] = df_viz["score"].apply(safe_float)
                else:
                    df_viz["score_num"] = 0.0

                if "sector" not in df_viz.columns:
                    df_viz["sector"] = "Indefini"
                if "name" not in df_viz.columns:
                    if "symbol" in df_viz.columns:
                        df_viz["name"] = df_viz["symbol"]
                    else:
                        df_viz["name"] = "N/A"

                top_picks = df_viz[df_viz["score_num"] >= 70]

                k1, k2, k3, k4 = st.columns(4)
                k1.metric("Dossiers Analyses", len(df_viz))
                k2.metric("Top Convictions", len(top_picks))
                k3.metric("Qualite Moyenne", f"{df_viz['score_num'].mean():.1f}/100" if len(df_viz) else "0/100")

                if len(df_viz) and "sector" in df_viz.columns:
                    try:
                        leader_sector = df_viz.groupby("sector")["score_num"].mean().idxmax()
                        k4.metric("Secteur Leader", leader_sector)
                    except Exception:
                        k4.metric("Secteur Leader", "N/A")
                else:
                    k4.metric("Secteur Leader", "N/A")

                st.divider()

                c_chart, c_top = st.columns([2, 1])

                with c_chart:
                    st.subheader("Carte des Opportunites")
                    if not df_viz.empty:
                        df_tree = df_viz.copy()
                        if "sector" not in df_tree.columns:
                            df_tree["sector"] = "Non defini"
                        df_tree["sector"] = df_tree["sector"].fillna("").astype(str).str.strip().replace("", "Non defini")

                        if "symbol" in df_tree.columns:
                            df_tree["symbol"] = df_tree["symbol"].fillna("").astype(str).str.strip().str.upper()
                            df_tree = df_tree[df_tree["symbol"] != ""].copy()
                            path = [px.Constant("Univers"), "sector", "symbol"]
                        else:
                            path = [px.Constant("Univers"), "sector"]

                        if df_tree.empty:
                            st.caption("Donnees insuffisantes pour la carte des opportunites.")
                        else:
                            fig_tree = px.treemap(
                                df_tree,
                                path=path,
                                values="score_num",
                                color="score_num",
                                color_continuous_scale=["#d73027", "#fee08b", "#1a9850"],
                                range_color=[30, 90],
                                hover_data=["name"] if "name" in df_tree.columns else None,
                                title="Taille = Score",
                            )
                            st.plotly_chart(fig_tree, use_container_width=True)

                with c_top:
                    st.subheader("Top 3")
                    if "symbol" in df_viz.columns:
                        for _, row in df_viz.sort_values("score_num", ascending=False).head(3).iterrows():
                            with st.container(border=True):
                                st.markdown(f"**{row.get('symbol','')}** — {row.get('score_num',0):.0f}/100")
                                st.caption(f"{row.get('name','')}")
                                if st.button(f"Voir {row.get('symbol','')}", key=f"btn_{row.get('symbol','NA')}"):
                                    st.session_state["filter_res"] = row.get("symbol", "")

                st.divider()

                st.subheader("🔬 Analyse Détaillée & Scénarios")

                def_sym = 0
                sym_options = sorted(df_viz["symbol"].unique().tolist()) if "symbol" in df_viz.columns else []
                if "filter_res" in st.session_state and st.session_state["filter_res"] in sym_options:
                    def_sym = sym_options.index(st.session_state["filter_res"])

                sel_sym = st.selectbox(
                    "Sélectionner une action pour voir les scénarios :",
                    [""] + sym_options,
                    index=(def_sym + 1) if def_sym else 0,
                )

                if sel_sym and "symbol" in df_viz.columns:
                    row_det = df_viz[df_viz["symbol"] == sel_sym].iloc[0]

                    txt_source = str(row_det.get("valuation", "")) + " " + str(row_det.get("why", ""))
                    scenarios = extract_valuation_scenarios(txt_source)

                    c_d1, c_d2 = st.columns([1, 2])

                    with c_d1:
                        st.markdown(f"### {row_det.get('symbol','')}")
                        st.info(f"**Thèse:** {clean_research_text(row_det.get('why',''))[:400]}...")
                        st.error(f"**Risques:** {clean_research_text(row_det.get('risks',''))[:300]}...")

                    with c_d2:
                        fig_scen = go.Figure()

                        last_price = safe_float(row_det.get("lastprice", 0))
                        last_date = datetime.now()

                        if df_prices is not None and not df_prices.empty and "symbol" in df_prices.columns:
                            df_h = df_prices[df_prices["symbol"].astype(str) == sel_sym].copy()
                            if not df_h.empty:
                                if "date" in df_h.columns:
                                    df_h["date"] = pd.to_datetime(df_h["date"], errors="coerce")
                                    df_h = df_h.dropna(subset=["date"]).sort_values("date")
                                else:
                                    df_h = pd.DataFrame()

                                if not df_h.empty and "close" in df_h.columns:
                                    fig_scen.add_trace(
                                        go.Scatter(
                                            x=df_h["date"],
                                            y=df_h["close"],
                                            mode="lines",
                                            name="Prix",
                                            line=dict(color="white"),
                                        )
                                    )
                                    last_price = safe_float(df_h.iloc[-1]["close"])
                                    last_dt = df_h.iloc[-1]["date"]
                                    last_date = last_dt.to_pydatetime() if hasattr(last_dt, "to_pydatetime") else last_dt

                        if scenarios and last_price > 0:
                            future = last_date + timedelta(days=365)
                            colors = {"Bear": "#dc3545", "Base": "#ffc107", "Bull": "#28a745"}

                            for k, v in scenarios.items():
                                fig_scen.add_trace(
                                    go.Scatter(
                                        x=[last_date, future],
                                        y=[last_price, v],
                                        mode="lines+markers+text",
                                        name=f"{k} Case",
                                        line=dict(color=colors.get(k, "white"), dash="dash"),
                                        text=[None, f"{v:.1f}€"],
                                        textposition="top right",
                                    )
                                )

                            fig_scen.update_layout(
                                title="Cône de Valorisation (12 mois)",
                                height=350,
                                margin=dict(l=0, r=0, t=30, b=0),
                            )
                            st.plotly_chart(fig_scen, use_container_width=True)
                        else:
                            st.warning(f"Pas de scénarios extraits ou prix indisponible. (Scenarios trouvés : {scenarios})")

                st.markdown("#### Liste Complète")
                cols_res = ["updatedat", "score", "symbol", "name", "why", "risks", "nextsteps"]
                cols_final = [c for c in cols_res if c in df_viz.columns]

                if cols_final:
                    df_list = df_viz[cols_final].copy()
                    if "score" in df_list.columns:
                        df_list["_score_num_tmp"] = df_list["score"].apply(safe_float)
                        df_list = df_list.sort_values("_score_num_tmp", ascending=False).drop(
                            columns=["_score_num_tmp"]
                        )
                    render_interactive_table(df_list, key_suffix="res_list")
                else:
                    st.info("Aucune colonne exploitable pour afficher la liste complète.")


# ============================================================
# PAGE 2: SYSTEM HEALTH (MONITORING)
# ============================================================

elif page == "System Health (Monitoring)":
    st.title("System Health - Fraicheur des donnees")
    st.caption("Controle par symbole (AG2/AG3/AG4-SPE) + controle macro global (AG4) + verification des derniers runs.")

    if st.button("Rafraichir", key="refresh_system_health"):
        load_data.clear()
        load_duckdb_data.clear()
        st.rerun()

    # Sources
    tech_latest = normalize_cols(duckdb_data.get("df_signals", pd.DataFrame()).copy())
    funda_latest = _load_fundamentals_for_dashboard(duckdb_data)
    symbol_news = _normalize_symbol_news_df(duckdb_data.get("df_news_symbol_history", pd.DataFrame()))
    macro_news = _normalize_macro_news_df(duckdb_data.get("df_news_macro_history", pd.DataFrame()))

    # Base symboles
    universe = normalize_cols(df_univ.copy()) if df_univ is not None and not df_univ.empty else pd.DataFrame()
    if "symbol" in universe.columns:
        universe["symbol"] = universe["symbol"].astype(str).str.strip().str.upper()
        universe = universe[universe["symbol"] != ""]

    if universe.empty:
        symbol_pool: set[str] = set()
        for df, col in [(tech_latest, "symbol"), (funda_latest, "symbol"), (symbol_news, "symbol")]:
            if df is not None and not df.empty and col in df.columns:
                vals = df[col].dropna().astype(str).str.strip().str.upper()
                symbol_pool.update([v for v in vals.tolist() if v])
        universe = pd.DataFrame({"symbol": sorted(symbol_pool)})

    if universe.empty:
        st.warning("Aucune donnee disponible pour etablir la page de health-check.")
        st.stop()

    if "name" not in universe.columns:
        universe["name"] = ""
    if "sector" not in universe.columns:
        universe["sector"] = ""
    if "industry" not in universe.columns:
        universe["industry"] = ""

    health_df = universe[["symbol", "name", "sector", "industry"]].drop_duplicates(subset=["symbol"]).copy()

    # Dates AG2 (technique)
    if tech_latest is not None and not tech_latest.empty and "symbol" in tech_latest.columns:
        tk = tech_latest.copy()
        tk["symbol"] = tk["symbol"].astype(str).str.strip().str.upper()
        tech_ts_col = _first_existing_column(tk, ["workflow_date", "d1_date", "h1_date", "updated_at", "created_at"])
        if tech_ts_col:
            tk["last_tech_date"] = pd.to_datetime(tk[tech_ts_col], errors="coerce", utc=True)
            tk = tk.sort_values("last_tech_date", ascending=False)
            tk = tk.drop_duplicates(subset=["symbol"], keep="first")
            health_df = health_df.merge(tk[["symbol", "last_tech_date"]], on="symbol", how="left")

    # Dates AG3 (fondamentale)
    if funda_latest is not None and not funda_latest.empty and "symbol" in funda_latest.columns:
        fd = normalize_cols(funda_latest.copy())
        fd["symbol"] = fd["symbol"].astype(str).str.strip().str.upper()
        funda_ts_col = _first_existing_column(fd, ["updated_at", "fetched_at", "created_at", "updatedat", "fetchedat"])
        if funda_ts_col:
            fd["last_funda_date"] = pd.to_datetime(fd[funda_ts_col], errors="coerce", utc=True)
            fd = fd.sort_values("last_funda_date", ascending=False)
            fd = fd.drop_duplicates(subset=["symbol"], keep="first")
            health_df = health_df.merge(fd[["symbol", "last_funda_date"]], on="symbol", how="left")

    # Dates AG4-SPE (news symbole)
    if symbol_news is not None and not symbol_news.empty and "symbol" in symbol_news.columns:
        ns = symbol_news.copy()
        ns["symbol"] = ns["symbol"].astype(str).str.strip().str.upper()
        if "publishedat" in ns.columns:
            ns["publishedat"] = pd.to_datetime(ns["publishedat"], errors="coerce", utc=True)
            ns = ns.sort_values("publishedat", ascending=False)
            ns = ns.drop_duplicates(subset=["symbol"], keep="first")
            health_df = health_df.merge(
                ns[["symbol", "publishedat"]].rename(columns={"publishedat": "last_news_date"}),
                on="symbol",
                how="left",
            )

    # Macro AG4 (global, non liee aux symboles)
    macro_last_date = pd.NaT
    if macro_news is not None and not macro_news.empty and "publishedat" in macro_news.columns:
        macro_news["publishedat"] = pd.to_datetime(macro_news["publishedat"], errors="coerce", utc=True)
        macro_last_date = macro_news["publishedat"].dropna().max() if not macro_news["publishedat"].dropna().empty else pd.NaT

    # Freshness / statuts FR
    now_utc = pd.Timestamp.now(tz="UTC")

    def _age_days(dt_series: pd.Series) -> pd.Series:
        s = pd.to_datetime(dt_series, errors="coerce", utc=True)
        age = (now_utc - s).dt.total_seconds() / 86400.0
        # Evite les ages negatifs (dates futures / timezone)
        return age.clip(lower=0)

    health_df["tech_age_days"] = _age_days(health_df.get("last_tech_date", pd.Series(pd.NaT, index=health_df.index)))
    health_df["funda_age_days"] = _age_days(health_df.get("last_funda_date", pd.Series(pd.NaT, index=health_df.index)))
    health_df["news_age_days"] = _age_days(health_df.get("last_news_date", pd.Series(pd.NaT, index=health_df.index)))

    def _status_fr(age_series: pd.Series, date_series: pd.Series, ok_days: int, warn_days: int) -> pd.Series:
        out = pd.Series("Manquant", index=age_series.index, dtype=object)
        valid = pd.to_datetime(date_series, errors="coerce", utc=True).notna()
        out.loc[valid & (age_series <= ok_days)] = "A jour"
        out.loc[valid & (age_series > ok_days) & (age_series <= warn_days)] = "A surveiller"
        out.loc[valid & (age_series > warn_days)] = "En retard"
        return out

    health_df["tech_statut"] = _status_fr(health_df["tech_age_days"], health_df.get("last_tech_date"), ok_days=3, warn_days=7)
    health_df["funda_statut"] = _status_fr(health_df["funda_age_days"], health_df.get("last_funda_date"), ok_days=30, warn_days=90)
    health_df["news_statut"] = _status_fr(health_df["news_age_days"], health_df.get("last_news_date"), ok_days=2, warn_days=7)

    sev_map = {"A jour": 0, "A surveiller": 1, "En retard": 2, "Manquant": 3}
    inv_sev = {0: "A jour", 1: "A surveiller", 2: "En retard", 3: "Manquant"}
    sev_cols = []
    for src in ["tech", "funda", "news"]:
        sev_col = f"{src}_sev"
        health_df[sev_col] = health_df[f"{src}_statut"].map(sev_map).fillna(3).astype(int)
        sev_cols.append(sev_col)
    health_df["global_sev"] = health_df[sev_cols].max(axis=1)
    health_df["statut_global"] = health_df["global_sev"].map(inv_sev).fillna("Manquant")

    # KPI symboles
    total_symbols = len(health_df)
    k_ok = int((health_df["statut_global"] == "A jour").sum())
    k_warn = int((health_df["statut_global"] == "A surveiller").sum())
    k_late = int((health_df["statut_global"] == "En retard").sum())
    k_miss = int((health_df["statut_global"] == "Manquant").sum())

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Symboles suivis", total_symbols)
    c2.metric("A jour", k_ok)
    c3.metric("A surveiller", k_warn)
    c4.metric("En retard", k_late)
    c5.metric("Manquant", k_miss)

    # Macro globale (independante des symboles)
    st.markdown("### Macro globale (AG4)")
    if pd.notna(macro_last_date):
        macro_age = max(0.0, (now_utc - macro_last_date).total_seconds() / 86400.0)
        if macro_age <= 2:
            macro_statut = "A jour"
        elif macro_age <= 7:
            macro_statut = "A surveiller"
        else:
            macro_statut = "En retard"
    else:
        macro_age = float("nan")
        macro_statut = "Manquant"

    m1, m2, m3 = st.columns(3)
    m1.metric("Statut macro", macro_statut)
    m2.metric("Age macro (jours)", f"{macro_age:.1f}" if pd.notna(macro_age) else "n/a")
    m3.metric("Derniere date macro", str(macro_last_date)[:19] if pd.notna(macro_last_date) else "n/a")

    # Runs workflows (robuste contre RUNNING stale)
    st.markdown("### Statut des workflows (dernier run)")

    def _workflow_status_fr(raw_status: str) -> str:
        s = str(raw_status or "").upper().strip()
        mapping = {
            "SUCCESS": "Termine",
            "PARTIAL": "Partiel",
            "FAILED": "Echec",
            "RUNNING": "En cours",
            "NO_DATA": "Aucune donnee",
            "BLOQUE": "Bloque",
            "UNKNOWN": "Inconnu",
            "": "Inconnu",
        }
        return mapping.get(s, s.title())

    def _latest_run_snapshot(df_runs: pd.DataFrame, workflow: str) -> dict[str, object]:
        out = {
            "workflow": workflow,
            "status_raw": "NO_DATA",
            "status": "Aucune donnee",
            "last_start": pd.NaT,
            "last_finish": pd.NaT,
            "age_h": pd.NA,
        }
        if df_runs is None or df_runs.empty:
            return out

        wk = normalize_cols(df_runs.copy())
        if "started_at" not in wk.columns:
            return out

        wk["started_at"] = pd.to_datetime(wk["started_at"], errors="coerce", utc=True)
        if "finished_at" in wk.columns:
            wk["finished_at"] = pd.to_datetime(wk["finished_at"], errors="coerce", utc=True)
        else:
            wk["finished_at"] = pd.NaT

        wk["status_u"] = wk.get("status", pd.Series("", index=wk.index)).fillna("").astype(str).str.upper().str.strip()
        wk = wk.dropna(subset=["started_at"]).copy()
        if wk.empty:
            return out

        wk["end_ts"] = wk["finished_at"].where(wk["finished_at"].notna(), wk["started_at"])
        final_statuses = {"SUCCESS", "PARTIAL", "FAILED", "NO_DATA"}
        finals = wk[(wk["finished_at"].notna()) | (wk["status_u"].isin(final_statuses))].copy()
        finals = finals.sort_values("end_ts", ascending=False)
        latest_final = finals.iloc[0] if not finals.empty else None

        runnings = wk[(wk["status_u"] == "RUNNING") & (wk["finished_at"].isna())].copy().sort_values("started_at", ascending=False)
        latest_running = runnings.iloc[0] if not runnings.empty else None

        chosen = None
        chosen_status = "NO_DATA"

        if latest_running is not None:
            running_age_h = (now_utc - latest_running["started_at"]).total_seconds() / 3600.0
            latest_final_end = latest_final["end_ts"] if latest_final is not None else pd.NaT
            really_running = pd.isna(latest_final_end) or (latest_running["started_at"] > latest_final_end)
            if really_running and running_age_h <= 1.5:
                chosen = latest_running
                chosen_status = "RUNNING"
            elif latest_final is not None:
                chosen = latest_final
                chosen_status = str(latest_final["status_u"] or "UNKNOWN")
            else:
                chosen = latest_running
                chosen_status = "BLOQUE" if running_age_h > 1.5 else "RUNNING"
        elif latest_final is not None:
            chosen = latest_final
            chosen_status = str(latest_final["status_u"] or "UNKNOWN")

        if chosen is None:
            return out

        ref_dt = chosen["finished_at"] if pd.notna(chosen["finished_at"]) else chosen["started_at"]
        age_h = (now_utc - ref_dt).total_seconds() / 3600.0 if pd.notna(ref_dt) else pd.NA

        out["status_raw"] = chosen_status
        out["status"] = _workflow_status_fr(chosen_status)
        out["last_start"] = chosen["started_at"]
        out["last_finish"] = chosen["finished_at"]
        out["age_h"] = round(float(age_h), 1) if pd.notna(age_h) else pd.NA
        return out

    run_rows = [
        _latest_run_snapshot(duckdb_data.get("df_runs", pd.DataFrame()), "AG2 Technique"),
        _latest_run_snapshot(duckdb_data.get("df_funda_runs", pd.DataFrame()), "AG3 Fondamentale"),
        _latest_run_snapshot(duckdb_data.get("df_news_macro_runs", pd.DataFrame()), "AG4 Macro"),
        _latest_run_snapshot(duckdb_data.get("df_news_symbol_runs", pd.DataFrame()), "AG4 SPE News Symbole"),
    ]
    runs_df = pd.DataFrame(run_rows)

    r1, r2, r3, r4 = st.columns(4)
    run_cards = [r1, r2, r3, r4]
    for idx, rec in enumerate(run_rows):
        delta_txt = f"{rec['age_h']}h" if pd.notna(rec.get("age_h")) else "n/a"
        run_cards[idx].metric(rec["workflow"], str(rec["status"]), delta=delta_txt)

    render_interactive_table(
        runs_df[["workflow", "status", "status_raw", "last_start", "last_finish", "age_h"]].rename(
            columns={
                "workflow": "Workflow",
                "status": "Statut",
                "status_raw": "Statut brut",
                "last_start": "Dernier demarrage",
                "last_finish": "Derniere fin",
                "age_h": "Age (heures)",
            }
        ),
        key_suffix="system_health_runs",
        height=220,
    )

    st.divider()
    st.markdown("### Fraicheur par source (symboles)")
    status_counts = []
    for src, label in [("tech_statut", "Technique AG2"), ("funda_statut", "Fondamentale AG3"), ("news_statut", "News Symbole AG4-SPE")]:
        vc = health_df[src].value_counts(dropna=False)
        for stt in ["A jour", "A surveiller", "En retard", "Manquant"]:
            status_counts.append({"Source": label, "Statut": stt, "Count": int(vc.get(stt, 0))})
    df_counts = pd.DataFrame(status_counts)
    fig_counts = px.bar(
        df_counts,
        x="Source",
        y="Count",
        color="Statut",
        barmode="stack",
        color_discrete_map={"A jour": "#28a745", "A surveiller": "#ffc107", "En retard": "#fd7e14", "Manquant": "#dc3545"},
        title="Repartition des statuts de fraicheur",
    )
    fig_counts.update_layout(height=320, margin=dict(t=40, b=20, l=20, r=20))
    st.plotly_chart(fig_counts, use_container_width=True)

    st.divider()
    st.markdown("### Detail par symbole")

    f1, f2 = st.columns([1, 1])
    view_mode = f1.selectbox(
        "Filtre statut",
        ["Tous", "Critiques (En retard/Manquant)", "A surveiller et plus", "A jour uniquement"],
        index=0,
        key="health_filter_mode",
    )
    sectors = sorted([s for s in health_df["sector"].dropna().astype(str).str.strip().unique().tolist() if s != ""])
    sector_sel = f2.selectbox("Secteur", ["Tous"] + sectors, index=0, key="health_filter_sector")

    show_df = health_df.copy()
    if view_mode == "Critiques (En retard/Manquant)":
        show_df = show_df[show_df["global_sev"] >= 2]
    elif view_mode == "A surveiller et plus":
        show_df = show_df[show_df["global_sev"] >= 1]
    elif view_mode == "A jour uniquement":
        show_df = show_df[show_df["global_sev"] == 0]

    if sector_sel != "Tous":
        show_df = show_df[show_df["sector"].astype(str) == sector_sel]

    for age_col in ["tech_age_days", "funda_age_days", "news_age_days"]:
        show_df[age_col] = pd.to_numeric(show_df[age_col], errors="coerce").round(1)

    cols_detail = [
        "symbol", "name", "sector", "industry", "statut_global",
        "tech_statut", "tech_age_days", "last_tech_date",
        "funda_statut", "funda_age_days", "last_funda_date",
        "news_statut", "news_age_days", "last_news_date",
    ]
    cols_detail = [c for c in cols_detail if c in show_df.columns]
    show_df = show_df.sort_values(["global_sev", "symbol"], ascending=[False, True], na_position="last")

    render_interactive_table(
        show_df[cols_detail].rename(
            columns={
                "symbol": "Symbole",
                "name": "Nom",
                "sector": "Secteur",
                "industry": "Industrie",
                "statut_global": "Statut global",
                "tech_statut": "Statut Tech",
                "tech_age_days": "Age Tech (j)",
                "last_tech_date": "Derniere date Tech",
                "funda_statut": "Statut Funda",
                "funda_age_days": "Age Funda (j)",
                "last_funda_date": "Derniere date Funda",
                "news_statut": "Statut News",
                "news_age_days": "Age News (j)",
                "last_news_date": "Derniere date News",
            }
        ),
        key_suffix="system_health_symbols",
        height=520,
    )

# ============================================================
# PAGE 3: VUE CONSOLIDEE MULTI-AGENTS
# ============================================================

elif page == "Vue consolidee Multi-Agents":
    st.title("Vue consolidee AG2 + AG3 + AG4")

    if st.button("Rafraichir", key="refresh_multi_agents"):
        load_data.clear()
        load_duckdb_data.clear()
        st.rerun()

    df_funda_for_view = _load_fundamentals_for_dashboard(duckdb_data)
    consolidated, macro_news_norm, symbol_news_norm = _prepare_multi_agent_view(
        df_universe=df_univ,
        df_tech_latest=duckdb_data.get("df_signals", pd.DataFrame()),
        df_funda_latest=df_funda_for_view,
        df_macro_news=duckdb_data.get("df_news_macro_history", pd.DataFrame()),
        df_symbol_news=duckdb_data.get("df_news_symbol_history", pd.DataFrame()),
    )

    if consolidated is None or consolidated.empty:
        st.warning("Aucune vue consolidee disponible. Verifiez les bases DuckDB AG2/AG3/AG4.")
        st.stop()

    matrix_df = _build_multi_agent_matrix(
        consolidated,
        df_port,
        duckdb_data.get("df_yf_enrichment_latest", pd.DataFrame()),
    )
    use_matrix = matrix_df is not None and not matrix_df.empty
    view_df = matrix_df if use_matrix else consolidated

    tab_global, tab_symbol = st.tabs(["Vue globale", "Vue par valeur"])

    with tab_global:
        if use_matrix:
            total_values = len(view_df)
            enter_count = int(view_df.get("matrix_action", pd.Series(dtype=str)).astype(str).eq("Entrer / Renforcer").sum())
            watch_count = int(view_df.get("matrix_action", pd.Series(dtype=str)).astype(str).eq("Surveiller").sum())
            exit_count = int(view_df.get("matrix_action", pd.Series(dtype=str)).astype(str).eq("Reduire / Sortir").sum())
            avg_ev = float(pd.to_numeric(view_df.get("ev_r", pd.Series(dtype=float)), errors="coerce").fillna(0).mean()) if total_values else 0.0
            grade_a = int(view_df.get("setup_grade", pd.Series(dtype=str)).astype(str).eq("A").sum())
            options_ok_series = view_df.get("options_ok", pd.Series(False, index=view_df.index)).fillna(False).astype(bool)
            opt_missing = int((~options_ok_series).sum())
            enrich_cov = (
                float(view_df.get("has_enrichment", pd.Series(False, index=view_df.index)).fillna(False).astype(bool).mean()) * 100.0
                if total_values
                else 0.0
            )

            g1, g2, g3, g4, g5, g6 = st.columns(6)
            g1.metric("Valeurs suivies", total_values)
            g2.metric("Entrer / Renforcer", enter_count)
            g3.metric("Surveiller", watch_count)
            g4.metric("Reduire / Sortir", exit_count)
            g5.metric("EV(R) moyen", f"{avg_ev:.2f}")
            g6.metric("Setups grade A", grade_a, delta=f"Options indispo: {opt_missing}")
            if enrich_cov < 85:
                st.warning(f"Couverture enrichissement YF insuffisante: {enrich_cov:.1f}% des symboles ont une ligne daily.")
            else:
                st.caption(f"Couverture enrichissement YF: {enrich_cov:.1f}%")

            st.caption(
                "Legende matrice: Axe X = Score RISQUE (0 faible -> 100 eleve), "
                "Axe Y = Score REWARD (0 faible -> 100 fort), couleur = grade setup (A/B/C), "
                "taille = valeur absolue EV(R), forme = action recommandee."
            )

            plot_df = view_df.copy()
            plot_df["bubble_size"] = (plot_df.get("ev_r", pd.Series(0.0, index=plot_df.index)).abs() * 24.0 + 10.0).clip(lower=10, upper=60)
            plot_df["p_win_pct"] = plot_df.get("p_win", pd.Series(0.0, index=plot_df.index)) * 100.0
            plot_df["risk_score_u"] = safe_float_series(plot_df.get("risk_score_u", pd.Series(50.0, index=plot_df.index))).fillna(50.0)
            plot_df["reward_score_u"] = safe_float_series(plot_df.get("reward_score_u", pd.Series(50.0, index=plot_df.index))).fillna(50.0)
            plot_df["risk_score_plot"] = safe_float_series(plot_df.get("risk_score_plot", plot_df["risk_score_u"])).fillna(50.0)
            plot_df["reward_score_plot"] = safe_float_series(plot_df.get("reward_score_plot", plot_df["reward_score_u"])).fillna(50.0)

            fig_rr = px.scatter(
                plot_df,
                x="risk_score_plot",
                y="reward_score_plot",
                color="setup_grade",
                symbol="matrix_action",
                size="bubble_size",
                hover_name="symbol",
                hover_data={
                    "name": True,
                    "sector": True,
                    "risk_score_u": ":.0f",
                    "reward_score_u": ":.0f",
                    "r_multiple": ":.2f",
                    "ev_r": ":.2f",
                    "p_win_pct": ":.1f",
                    "reward_pct": ":.2f",
                    "risk_pct": ":.2f",
                    "spreadPct": ":.2f",
                    "iv_atm": ":.3f",
                    "days_to_earnings": ":.1f",
                    "bubble_size": False,
                },
                color_discrete_map={"A": "#28a745", "B": "#ffc107", "C": "#dc3545"},
                labels={
                    "risk_score_plot": "Risque (0-100)",
                    "reward_score_plot": "Reward (0-100)",
                    "risk_score_u": "Risque unitaire",
                    "reward_score_u": "Reward unitaire",
                    "setup_grade": "Probabilite",
                    "matrix_action": "Decision",
                },
                title="Matrice Risk / Reward / Probabilite (echelle 0-100)",
            )
            fig_rr.update_traces(marker=dict(opacity=0.62))
            fig_rr.add_vline(x=50, line_dash="dot", line_color="#dc3545")
            fig_rr.add_hline(y=50, line_dash="dot", line_color="#28a745")
            fig_rr.update_xaxes(range=[0, 100], dtick=10)
            fig_rr.update_yaxes(range=[0, 100], dtick=10)
            fig_rr.update_layout(height=540)
            st.plotly_chart(fig_rr, use_container_width=True)

            st.markdown(
                """
**Lecture des 4 cadrants (investissement)**
1. `Haut gauche` (Risque <= 50, Reward >= 50): Zone de priorite. Candidats a l'entree/renforcement avec sizing normal.
2. `Haut droite` (Risque > 50, Reward >= 50): Opportunites speculatives. Taille reduite, stop strict, suivi plus frequent.
3. `Bas gauche` (Risque <= 50, Reward < 50): Profil defensif. Conservation possible, mais faible upside a court terme.
4. `Bas droite` (Risque > 50, Reward < 50): Zone a risque negatif. Reduction/sortie prioritaire sauf raison tactique forte.
"""
            )

            st.markdown("#### Priorisation matrice")
            matrix_cols = [
                "symbol",
                "name",
                "sector",
                "matrix_action",
                "quadrant",
                "setup_grade",
                "risk_score_u",
                "reward_score_u",
                "r_multiple",
                "ev_r",
                "p_win",
                "reward_pct",
                "risk_pct",
                "spreadPct",
                "iv_atm",
                "days_to_earnings",
                "options_note",
            ]
            show_cols = [c for c in matrix_cols if c in view_df.columns]
            table_show = view_df[show_cols].copy()
            if "p_win" in table_show.columns:
                table_show["p_win"] = (pd.to_numeric(table_show["p_win"], errors="coerce").fillna(0.0) * 100.0).round(1)
            render_interactive_table(
                table_show.rename(
                    columns={
                        "symbol": "Symbole",
                        "name": "Nom",
                        "sector": "Secteur",
                        "matrix_action": "Decision",
                        "quadrant": "Quadrant",
                        "setup_grade": "Grade",
                        "risk_score_u": "Risque (0-100)",
                        "reward_score_u": "Reward (0-100)",
                        "r_multiple": "R",
                        "ev_r": "EV(R)",
                        "p_win": "Prob. win %",
                        "reward_pct": "Reward %",
                        "risk_pct": "Risk %",
                        "spreadPct": "Spread %",
                        "iv_atm": "IV ATM",
                        "days_to_earnings": "Jours earnings",
                        "options_note": "Note options",
                    }
                ),
                key_suffix="multi_agents_matrix_global",
                height=460,
            )
        else:
            avg_conv = float(consolidated["conviction_score"].mean()) if "conviction_score" in consolidated.columns else 0.0
            bullish = int(consolidated.get("tech_action", pd.Series(dtype=str)).astype(str).str.upper().eq("BUY").sum())
            bearish = int(consolidated.get("tech_action", pd.Series(dtype=str)).astype(str).str.upper().eq("SELL").sum())
            hot = int((consolidated.get("conviction_score", pd.Series(dtype=float)) >= 70).sum())
            at_risk = int((consolidated.get("conviction_score", pd.Series(dtype=float)) < 40).sum())

            g1, g2, g3, g4, g5 = st.columns(5)
            g1.metric("Valeurs suivies", len(consolidated))
            g2.metric("Conviction moyenne", f"{avg_conv:.1f}/100")
            g3.metric("Biais BUY", bullish)
            g4.metric("Biais SELL", bearish)
            g5.metric("Alerte (<40)", at_risk, delta=f"{hot} >= 70")

            plot_df = consolidated.copy()
            if "sector" not in plot_df.columns:
                plot_df["sector"] = "N/A"
            plot_df["bubble_size"] = (
                plot_df.get("symbol_news_count_30d", pd.Series(0, index=plot_df.index))
                + plot_df.get("macro_news_count_30d", pd.Series(0, index=plot_df.index))
            ).clip(lower=1)

            fig = px.scatter(
                plot_df,
                x="funda_risk",
                y="conviction_score",
                color="sector",
                size="bubble_size",
                hover_name="symbol",
                hover_data={
                    "name": True,
                    "tech_action": True,
                    "funda_score": ":.1f",
                    "funda_upside": ":.1f",
                    "symbol_news_impact_7d": ":.1f",
                    "macro_impact_30d": ":.1f",
                    "bubble_size": False,
                },
                labels={"funda_risk": "Risque fondamental", "conviction_score": "Conviction consolidee"},
                title="Carte globale conviction vs risque",
            )
            fig.add_hline(y=70, line_dash="dot", line_color="#28a745")
            fig.add_hline(y=40, line_dash="dot", line_color="#dc3545")
            fig.add_vline(x=60, line_dash="dot", line_color="#dc3545")
            fig.update_layout(height=520)
            st.plotly_chart(fig, use_container_width=True)

            st.markdown("#### Priorisation globale")
            table_cols = [
                "symbol",
                "name",
                "sector",
                "tech_action",
                "funda_score",
                "funda_risk",
                "funda_upside",
                "symbol_news_impact_7d",
                "macro_impact_30d",
                "conviction_score",
                "conclusion",
            ]
            show_cols = [c for c in table_cols if c in consolidated.columns]
            render_interactive_table(
                consolidated[show_cols].rename(
                    columns={
                        "symbol": "Symbole",
                        "name": "Nom",
                        "sector": "Secteur",
                        "tech_action": "Signal Tech",
                        "funda_score": "Score Funda",
                        "funda_risk": "Risque",
                        "funda_upside": "Upside %",
                        "symbol_news_impact_7d": "Impact News 7j",
                        "macro_impact_30d": "Impact Macro 30j",
                        "conviction_score": "Conviction",
                        "conclusion": "Conclusion",
                    }
                ),
                key_suffix="multi_agents_global",
                height=460,
            )

    with tab_symbol:
        labels = []
        label_to_symbol = {}
        for _, row in view_df.iterrows():
            sym = str(row.get("symbol", "")).strip()
            if not sym:
                continue
            name = str(row.get("name", "")).strip()
            lbl = f"{sym} - {name}" if name else sym
            labels.append(lbl)
            label_to_symbol[lbl] = sym

        if not labels:
            st.info("Aucune valeur disponible.")
        else:
            selected_label = st.selectbox("Selectionner une valeur", labels, key="multi_agents_symbol")
            selected_symbol = label_to_symbol[selected_label]
            row = view_df[view_df["symbol"] == selected_symbol].iloc[0]

            if use_matrix:
                s1, s2, s3, s4, s5, s6, s7, s8 = st.columns(8)
                s1.metric("Decision", str(row.get("matrix_action", "N/A")))
                s2.metric("Grade", str(row.get("setup_grade", "N/A")))
                s3.metric("Risque", f"{safe_float(row.get('risk_score_u', row.get('risk_score_100', 0))):.0f}/100")
                s4.metric("Reward", f"{safe_float(row.get('reward_score_u', row.get('reward_score_100', 0))):.0f}/100")
                s5.metric("R", f"{safe_float(row.get('r_multiple', 0)):.2f}")
                s6.metric("EV(R)", f"{safe_float(row.get('ev_r', 0)):.2f}")
                s7.metric("Prob. win", f"{safe_float(row.get('p_win', 0) * 100):.1f}%")
                s8.metric("Conviction", f"{safe_float(row.get('conviction_score', 0)):.1f}/100")

                st.markdown("#### Lecture mecanique")
                st.write(
                    f"{selected_symbol}: entree={safe_float(row.get('entry_price', 0)):.2f}, "
                    f"stop={safe_float(row.get('stop_price', 0)):.2f}, "
                    f"tp={safe_float(row.get('tp_price', 0)):.2f}, "
                    f"reward={safe_float(row.get('reward_pct', 0)):.2f}%, "
                    f"risk={safe_float(row.get('risk_pct', 0)):.2f}%, "
                    f"R={safe_float(row.get('r_multiple', 0)):.2f}, "
                    f"EV(R)={safe_float(row.get('ev_r', 0)):.2f}."
                )

                rd_left, rd_right = st.columns(2)
                with rd_left:
                    st.markdown("#### Decomposition risque")
                    risk_parts = pd.DataFrame(
                        [
                            {"Composant": "Funda risk", "Score": safe_float(row.get("funda_risk", 0))},
                            {"Composant": "Volatilite (ATR%)", "Score": safe_float(row.get("vol_risk_score", 0))},
                            {"Composant": "Liquidite (spread/slippage)", "Score": safe_float(row.get("liquidity_risk_score", 0))},
                            {"Composant": "Event risk (earnings)", "Score": safe_float(row.get("event_risk_score", 0))},
                            {"Composant": "News pressure", "Score": safe_float(row.get("news_risk_score", 0))},
                            {"Composant": "Concentration portefeuille", "Score": safe_float(row.get("concentration_risk_score", 0))},
                            {"Composant": "Options IV risk", "Score": safe_float(row.get("options_risk_score", 0))},
                        ]
                    )
                    fig_risk = px.bar(
                        risk_parts.sort_values("Score", ascending=True),
                        x="Score",
                        y="Composant",
                        orientation="h",
                        title="Composants du score risque (0-100)",
                        color="Score",
                        color_continuous_scale=["#28a745", "#ffc107", "#dc3545"],
                    )
                    fig_risk.update_layout(height=320, margin=dict(t=40, b=20, l=20, r=20), coloraxis_showscale=False)
                    st.plotly_chart(fig_risk, use_container_width=True)

                with rd_right:
                    st.markdown("#### Donnees marche externes")
                    st.write(
                        f"Spread: {safe_float(row.get('spreadPct', 0)):.2f}% | "
                        f"Slippage proxy: {safe_float(row.get('slippageProxyPct', 0)):.2f}% | "
                        f"IV ATM: {safe_float(row.get('iv_atm', 0)):.3f} | "
                        f"Jours avant earnings: {safe_float(row.get('days_to_earnings', 0)):.1f}"
                    )
                    if not bool(row.get("options_ok", False)):
                        note = str(row.get("options_note", "") or "Aucune option Yahoo disponible (cas courant sur titres FR).")
                        st.info(note)
                    elif str(row.get("options_warning", "")).strip():
                        st.caption(f"Options warning: {row.get('options_warning')}")
            else:
                s1, s2, s3, s4, s5, s6 = st.columns(6)
                s1.metric("Conviction", f"{safe_float(row.get('conviction_score', 0)):.1f}/100", delta=str(row.get("conclusion", "")))
                s2.metric("Tech", str(row.get("tech_action", "N/A")))
                s3.metric("Funda", f"{safe_float(row.get('funda_score', 0)):.0f}/100")
                s4.metric("Risque", f"{safe_float(row.get('funda_risk', 0)):.0f}/100")
                s5.metric("Upside", f"{safe_float(row.get('funda_upside', 0)):.1f}%")
                s6.metric("News 7j", f"{safe_float(row.get('symbol_news_impact_7d', 0)):.1f}")

                st.markdown("#### Conclusion de synthese")
                st.write(
                    f"{selected_symbol}: {row.get('conclusion', '')}. "
                    f"Macro 30j={safe_float(row.get('macro_impact_30d', 0)):.1f}, "
                    f"News symbole 7j={safe_float(row.get('symbol_news_impact_7d', 0)):.1f}, "
                    f"themes macro dominants={row.get('macro_themes', '') or 'N/A'}."
                )

            st.divider()
            c_left, c_right = st.columns(2)

            with c_left:
                st.markdown("#### News macro reliees (30j)")
                sec = _clean_context_token(row.get("sector", ""))
                ind = _clean_context_token(row.get("industry", ""))
                macro_show = macro_news_norm.copy()
                if not macro_show.empty:
                    text_cols = [c for c in ["theme", "title", "snippet", "notes", "winners", "losers", "regime"] if c in macro_show.columns]
                    macro_show["_ctx"] = macro_show[text_cols].fillna("").astype(str).agg(" ".join, axis=1).str.lower() if text_cols else ""
                    mask = pd.Series(False, index=macro_show.index)
                    if sec:
                        mask = mask | macro_show["_ctx"].str.contains(re.escape(sec), regex=True, na=False)
                    if ind and ind != sec:
                        mask = mask | macro_show["_ctx"].str.contains(re.escape(ind), regex=True, na=False)
                    cut30 = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=30)
                    macro_show = macro_show[(macro_show["publishedat"] >= cut30) & mask].copy()
                if macro_show.empty:
                    st.caption("Aucune news macro reliee sur 30 jours.")
                else:
                    cols = [c for c in ["publishedat", "theme", "title", "impactscore", "regime", "winners", "losers"] if c in macro_show.columns]
                    render_interactive_table(
                        macro_show[cols].rename(
                            columns={
                                "publishedat": "Date",
                                "theme": "Theme",
                                "title": "Titre",
                                "impactscore": "Impact",
                                "regime": "Regime",
                                "winners": "Winners",
                                "losers": "Losers",
                            }
                        ),
                        key_suffix=f"macro_symbol_{selected_symbol}",
                        height=320,
                    )

            with c_right:
                st.markdown("#### News specifiques symbole (30j)")
                sym_show = symbol_news_norm.copy()
                if not sym_show.empty:
                    cut30 = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=30)
                    sym_show = sym_show[(sym_show["symbol"] == selected_symbol) & (sym_show["publishedat"] >= cut30)].copy()
                if sym_show.empty:
                    st.caption("Aucune news specifique sur 30 jours.")
                else:
                    cols = [c for c in ["publishedat", "title", "impactscore", "sentiment", "urgency", "confidence", "summary"] if c in sym_show.columns]
                    render_interactive_table(
                        sym_show[cols].rename(
                            columns={
                                "publishedat": "Date",
                                "title": "Titre",
                                "impactscore": "Impact",
                                "sentiment": "Sentiment",
                                "urgency": "Urgence",
                                "confidence": "Confiance",
                                "summary": "Resume",
                            }
                        ),
                        key_suffix=f"symbol_news_{selected_symbol}",
                        height=320,
                    )


# ============================================================
# PAGE 4: ANALYSE TECHNIQUE V2
# ============================================================

elif page == "Analyse Technique V2":
    st.title("Analyse Technique V2 (AG2)")

    if st.button("Rafraichir", key="refresh_v2"):
        load_data.clear()
        load_duckdb_data.clear()
        st.rerun()

    if not duckdb_data:
        st.info(
            "Base DuckDB non disponible. Vérifiez que le fichier existe "
            f"à l'emplacement : `{DUCKDB_PATH}`"
        )
        st.stop()

    df_signals = duckdb_data.get("df_signals", pd.DataFrame())
    df_runs = duckdb_data.get("df_runs", pd.DataFrame())
    df_signals_all = duckdb_data.get("df_signals_all", pd.DataFrame())

    if df_signals is None or df_signals.empty:
        st.warning("Aucun signal technique V2 disponible dans DuckDB.")
        st.stop()

    # Enrich with universe names if available
    if df_univ is not None and not df_univ.empty:
        df_signals = enrich_df_with_name(df_signals, df_univ)

    tab_overview, tab_detail, tab_runs = st.tabs(
        ["Vue d'ensemble", "Vue détaillée", "Historique Runs"]
    )

    # ================================================================
    # TAB 1: VUE D'ENSEMBLE
    # ================================================================
    with tab_overview:
        # KPI row
        total_symbols = len(df_signals)
        buy_count = int((df_signals.get("d1_action", pd.Series(dtype=str)).astype(str).str.upper() == "BUY").sum())
        sell_count = int((df_signals.get("d1_action", pd.Series(dtype=str)).astype(str).str.upper() == "SELL").sum())
        neutral_count = total_symbols - buy_count - sell_count
        ai_calls = int(truthy_series(df_signals.get("call_ai", pd.Series(dtype=object))).sum()) if "call_ai" in df_signals.columns else 0
        ai_approvals = int(df_signals.get("ai_decision", pd.Series(dtype=str)).astype(str).str.upper().eq("APPROVE").sum()) if "ai_decision" in df_signals.columns else 0

        kc1, kc2, kc3, kc4, kc5, kc6 = st.columns(6)
        kc1.metric("Symboles analysés", total_symbols)
        kc2.metric("BUY", buy_count)
        kc3.metric("SELL", sell_count)
        kc4.metric("NEUTRAL", neutral_count)
        kc5.metric("Appels IA", ai_calls)
        kc6.metric("IA Approuvés", ai_approvals)

        st.divider()

        # Build styled overview dataframe
        overview_cols = {
            "symbol": "Symbol",
        }

        df_ov = df_signals.copy()

        # Ensure columns exist with defaults
        for col_name in [
            "name", "sector", "last_close",
            "h1_action", "h1_score", "h1_rsi14",
            "d1_action", "d1_score", "d1_rsi14",
            "filter_reason", "ai_decision", "ai_quality",
            "workflow_date",
        ]:
            if col_name not in df_ov.columns:
                df_ov[col_name] = ""

        # Format for display
        df_display = pd.DataFrame()
        df_display["Symbol"] = df_ov["symbol"]
        df_display["Name"] = df_ov["name"].fillna("")
        df_display["Sector"] = df_ov["sector"].fillna("")
        close_num = safe_float_series(df_ov["last_close"])
        h1_score_num = safe_float_series(df_ov["h1_score"])
        h1_rsi_num = safe_float_series(df_ov["h1_rsi14"])
        d1_score_num = safe_float_series(df_ov["d1_score"])
        d1_rsi_num = safe_float_series(df_ov["d1_rsi14"])
        ai_quality_num = safe_float_series(df_ov["ai_quality"])

        df_display["Close"] = close_num.apply(lambda v: f"{v:.2f}" if v > 0 else "—")
        df_display["H1 Action"] = df_ov["h1_action"].fillna("").astype(str).str.upper().replace("", "—")
        df_display["H1 Score"] = h1_score_num.apply(lambda v: f"{v:.0f}" if v != 0 else "—")
        df_display["H1 RSI"] = h1_rsi_num.apply(lambda v: f"{v:.1f}" if v > 0 else "—")
        df_display["D1 Action"] = df_ov["d1_action"].fillna("").astype(str).str.upper().replace("", "—")
        df_display["D1 Score"] = d1_score_num.apply(lambda v: f"{v:.0f}" if v != 0 else "—")
        df_display["D1 RSI"] = d1_rsi_num.apply(lambda v: f"{v:.1f}" if v > 0 else "—")
        df_display["Filtre"] = df_ov["filter_reason"].fillna("—")
        df_display["IA"] = df_ov["ai_decision"].fillna("").astype(str).str.upper().replace("", "—")
        df_display["Qualité IA"] = ai_quality_num.apply(lambda v: f"{v:.0f}/10" if v > 0 else "—")
        df_display["Date"] = df_ov["workflow_date"].apply(
            lambda x: str(x)[:10] if pd.notna(x) and str(x).strip() not in ("", "nan", "NaT") else "—"
        )

        # Apply RSI coloring via Styler on a numeric version for conditional formatting
        # Since we use HTML badges, we display via HTML table
        render_interactive_table(df_display, key_suffix="v2_overview")

    # ================================================================
    # TAB 2: VUE DETAILLEE
    # ================================================================
    with tab_detail:
        signals_by_symbol = (
            df_signals.dropna(subset=["symbol"])
            .drop_duplicates(subset=["symbol"], keep="first")
            .set_index("symbol", drop=False)
        )
        symbol_list = sorted(signals_by_symbol.index.tolist())

        if not symbol_list:
            st.warning("Aucun symbole disponible.")
        else:
            # Build "SYMBOL — NAME" labels for search by company name
            _name_map = {}
            _label_list = []
            for sym in symbol_list:
                base_row = signals_by_symbol.loc[sym]
                name = str(base_row.get("name", "")).strip()
                label = f"{sym} — {name}" if name and name.lower() not in ("", "nan", "none") else sym
                _name_map[label] = sym
                _label_list.append(label)

            selected_label = st.selectbox(
                "Sélectionner un symbole (recherche par nom ou ticker) :",
                _label_list,
                key="v2_symbol_select",
            )
            selected_symbol = _name_map.get(selected_label, selected_label)

            if selected_symbol:
                if selected_symbol not in signals_by_symbol.index:
                    st.warning(f"Aucune donnée pour {selected_symbol}")
                else:
                    row = signals_by_symbol.loc[selected_symbol]

                    # ---- Row 1: KPI Cards ----
                    st.subheader(f"📊 {selected_symbol} — {row.get('name', '')}")

                    mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)

                    close_price = safe_float(row.get("last_close", 0))
                    mc1.metric("Close", f"{close_price:.2f} €" if close_price > 0 else "—")

                    h1_act = str(row.get("h1_action", "")).upper()
                    h1_sc = safe_float(row.get("h1_score", 0))
                    mc2.metric("H1", f"{h1_act}", delta=f"Score: {h1_sc:.0f}")

                    d1_act = str(row.get("d1_action", "")).upper()
                    d1_sc = safe_float(row.get("d1_score", 0))
                    mc3.metric("D1", f"{d1_act}", delta=f"Score: {d1_sc:.0f}")

                    ai_dec = str(row.get("ai_decision", "—"))
                    mc4.metric("Décision IA", ai_dec if ai_dec.strip() else "—")

                    ai_qual = safe_float(row.get("ai_quality", 0))
                    mc5.metric("Qualité IA", f"{ai_qual:.0f}/10" if ai_qual > 0 else "—")

                    rr = safe_float(row.get("ai_rr_theoretical", 0))
                    mc6.metric("R/R Théorique", f"{rr:.2f}" if rr > 0 else "—")

                    st.divider()

                    # ---- Row 2: Indicators H1 | D1 with visual bars ----
                    col_h1, col_d1 = st.columns(2)

                    for tf_col, tf_label in [(col_h1, "H1"), (col_d1, "D1")]:
                        prefix = tf_label.lower() + "_"
                        with tf_col:
                            st.markdown(f"#### Indicateurs {tf_label}")

                            # RSI Gauge (keep the gauge — it's the most important)
                            rsi_val = safe_float(row.get(f"{prefix}rsi14", 0))
                            if rsi_val > 0:
                                fig_rsi = _make_rsi_gauge(rsi_val, f"RSI 14 ({tf_label})")
                                st.plotly_chart(fig_rsi, use_container_width=True, key=f"rsi_{tf_label}_{selected_symbol}")

                            # All other indicators as visual bars
                            indicators_to_show = [
                                ("macd_hist", f"{prefix}macd_hist"),
                                ("stoch_k", f"{prefix}stoch_k"),
                                ("stoch_d", f"{prefix}stoch_d"),
                                ("adx", f"{prefix}adx"),
                                ("atr_pct", f"{prefix}atr_pct"),
                                ("bb_width", f"{prefix}bb_width"),
                                ("volatility", f"{prefix}volatility"),
                                ("obv_slope", f"{prefix}obv_slope"),
                            ]

                            bars_html = ""
                            for ind_key, col_key in indicators_to_show:
                                val = safe_float(row.get(col_key, 0))
                                bars_html += _indicator_bar(ind_key, val, tf_label)

                            st.markdown(bars_html, unsafe_allow_html=True)

                    st.divider()

                    # ---- Row 3: SMA Alignment ----
                    st.markdown("#### Alignement SMA")

                    sma_col1, sma_col2 = st.columns(2)

                    with sma_col1:
                        st.markdown("**H1**")
                        h1_close = safe_float(row.get("h1_last_close", row.get("last_close", 0)))
                        h1_sma20 = safe_float(row.get("h1_sma20", 0))
                        h1_sma50 = safe_float(row.get("h1_sma50", 0))
                        h1_sma200 = safe_float(row.get("h1_sma200", 0))
                        st.markdown(_sma_alignment_text(h1_close, h1_sma20, h1_sma50, h1_sma200))

                    with sma_col2:
                        st.markdown("**D1**")
                        d1_close = safe_float(row.get("d1_last_close", row.get("last_close", 0)))
                        d1_sma20 = safe_float(row.get("d1_sma20", 0))
                        d1_sma50 = safe_float(row.get("d1_sma50", 0))
                        d1_sma200 = safe_float(row.get("d1_sma200", 0))
                        st.markdown(_sma_alignment_text(d1_close, d1_sma20, d1_sma50, d1_sma200))

                    st.divider()

                    # ---- Row 4: Graphiques Chandelier (via yfinance-api) ----
                    st.markdown("#### Graphiques de Prix")

                    chart_col1, chart_col2 = st.columns(2)

                    for chart_col, tf_label, interval, lookback in [
                        (chart_col1, "H1", "1h", 60),
                        (chart_col2, "D1", "1d", 120),
                    ]:
                        prefix = tf_label.lower() + "_"
                        support = safe_float(row.get(f"{prefix}support", 0))
                        resistance = safe_float(row.get(f"{prefix}resistance", 0))

                        with chart_col:
                            df_chart = fetch_yfinance_history(selected_symbol, interval=interval, lookback_days=lookback)
                            if not df_chart.empty:
                                fig_candle = _make_candlestick_chart(
                                    df_chart,
                                    title=f"{selected_symbol} — {tf_label} ({interval})",
                                    support=support,
                                    resistance=resistance,
                                )
                                st.plotly_chart(fig_candle, use_container_width=True, key=f"chart_{tf_label}_{selected_symbol}")
                            else:
                                st.caption(f"Données {tf_label} indisponibles (yfinance-api).")

                    st.divider()

                    # ---- Row 5: AI Analysis Card ----
                    ai_decision = str(row.get("ai_decision", "")).strip()

                    if ai_decision and ai_decision.lower() not in ("", "nan", "none"):
                        st.markdown("#### Analyse IA")

                        with st.container(border=True):
                            ai_c1, ai_c2, ai_c3 = st.columns(3)

                            with ai_c1:
                                st.markdown(f"**Décision :** {_ai_badge(ai_decision)}", unsafe_allow_html=True)
                                st.markdown(f"**Qualité :** {safe_float(row.get('ai_quality', 0)):.0f}/10")
                                st.markdown(f"**Biais SMA200 :** {row.get('ai_bias_sma200', '—')}")
                                st.markdown(f"**Régime D1 :** {row.get('ai_regime_d1', '—')}")

                            with ai_c2:
                                st.markdown(f"**Alignement :** {row.get('ai_alignment', '—')}")
                                st.markdown(f"**Pattern :** {row.get('ai_chart_pattern', '—')}")
                                st.markdown(f"**Stop Loss :** {row.get('ai_stop_loss', '—')} ({row.get('ai_stop_basis', '—')})")
                                st.markdown(f"**R/R Théorique :** {safe_float(row.get('ai_rr_theoretical', 0)):.2f}")

                            with ai_c3:
                                ai_missing = str(row.get("ai_missing", "")).strip()
                                ai_anomalies = str(row.get("ai_anomalies", "")).strip()
                                if ai_missing and ai_missing.lower() not in ("nan", "none", ""):
                                    st.markdown(f"**Données manquantes :** {ai_missing}")
                                if ai_anomalies and ai_anomalies.lower() not in ("nan", "none", ""):
                                    st.markdown(f"**Anomalies :** {ai_anomalies}")

                            # Reasoning (full width)
                            ai_reasoning = str(row.get("ai_reasoning", "")).strip()
                            if ai_reasoning and ai_reasoning.lower() not in ("nan", "none", ""):
                                st.markdown("---")
                                st.markdown(f"**Raisonnement IA :**")
                                st.markdown(ai_reasoning)
                    else:
                        st.info("Pas d'analyse IA pour ce symbole (filtre non passé ou IA non appelée).")

    # ================================================================
    # TAB 3: HISTORIQUE RUNS
    # ================================================================
    with tab_runs:
        if df_runs is None or df_runs.empty:
            st.info("Aucun historique de runs disponible.")
        else:
            st.subheader("Historique des exécutions AG2-V2")

            df_runs_display = df_runs.copy()

            # Format status badges
            if "status" in df_runs_display.columns:
                df_runs_display["Statut"] = df_runs_display["status"].fillna("").astype(str).str.upper().replace("", "—")
            else:
                df_runs_display["Statut"] = "—"

            display_cols = []
            col_mapping = {
                "run_id": "Run ID",
                "started_at": "Démarré",
                "finished_at": "Terminé",
                "Statut": "Statut",
                "symbols_ok": "Symboles OK",
                "symbols_error": "Symboles Erreur",
                "ai_calls": "Appels IA",
                "vectors_written": "Vecteurs écrits",
            }

            for src, dst in col_mapping.items():
                if src in df_runs_display.columns:
                    display_cols.append(src)
                elif src == "Statut":
                    display_cols.append(src)

            df_runs_show = df_runs_display[[c for c in display_cols if c in df_runs_display.columns]].copy()

            # Rename for display
            rename_map = {k: v for k, v in col_mapping.items() if k in df_runs_show.columns}
            df_runs_show = df_runs_show.rename(columns=rename_map)

            render_interactive_table(df_runs_show, key_suffix="v2_runs")


# ================================================================
# PAGE 4: ANALYSE FONDAMENTALE V2
# ================================================================
elif page == "Analyse Fondamentale V2":
    st.title("Analyse Fondamentale V2 (AG3)")

    if st.button("Rafraichir", key="refresh_funda_v2"):
        load_data.clear()
        load_duckdb_data.clear()
        st.rerun()

    df_funda_latest = duckdb_data.get("df_funda_latest", pd.DataFrame())
    df_funda_runs = duckdb_data.get("df_funda_runs", pd.DataFrame())
    df_funda_history = duckdb_data.get("df_funda_history", pd.DataFrame())
    df_funda_consensus = duckdb_data.get("df_funda_consensus", pd.DataFrame())
    df_funda_metrics = duckdb_data.get("df_funda_metrics", pd.DataFrame())

    if df_funda_latest is None or df_funda_latest.empty:
        st.info(
            "Aucune donnée fondamentale AG3-V2 disponible dans DuckDB. "
            f"Vérifiez le fichier `{AG3_DUCKDB_PATH}` et l'exécution du workflow AG3."
        )
        st.stop()

    # Enrichissements noms/secteurs depuis Universe si présent
    if df_univ is not None and not df_univ.empty and "symbol" in df_funda_latest.columns:
        df_funda_latest = enrich_df_with_name(df_funda_latest, df_univ)
    if (
        df_funda_consensus is not None
        and not df_funda_consensus.empty
        and df_univ is not None
        and not df_univ.empty
        and "symbol" in df_funda_consensus.columns
    ):
        df_funda_consensus = enrich_df_with_name(df_funda_consensus, df_univ)

    tab_overview, tab_detail, tab_runs = st.tabs(
        ["Vue d'ensemble", "Vue détaillée", "Historique Runs"]
    )

    # ================================================================
    # TAB 1: VUE D'ENSEMBLE
    # ================================================================
    with tab_overview:
        df_ov = df_funda_latest.copy()
        score_num = _safe_series(df_ov, ["score", "funda_conf"])
        risk_num = _safe_series(df_ov, ["risk_score"])
        upside_num = _safe_series(df_ov, ["upside_pct"])
        analysts_num = _safe_series(df_ov, ["analyst_count"])
        coverage_num = _safe_series(df_ov, ["data_coverage_pct"]).clip(lower=0, upper=100)
        quality_num = _safe_series(df_ov, ["quality_score"])

        total_symbols = len(df_ov)
        high_conv = int(((score_num >= 70) & (risk_num <= 50)).sum())
        weak_conv = int((score_num < 55).sum())
        risk_high = int((risk_num > 60).sum())
        avg_score = float(score_num.mean()) if total_symbols > 0 else 0.0
        avg_upside = float(upside_num.mean()) if total_symbols > 0 else 0.0

        kc1, kc2, kc3, kc4, kc5, kc6 = st.columns(6)
        kc1.metric("Symboles scorés", total_symbols)
        kc2.metric("Convictions fortes", high_conv)
        kc3.metric("Scores faibles", weak_conv)
        kc4.metric("Risque élevé", risk_high)
        kc5.metric("Score moyen", f"{avg_score:.1f}/100")
        kc6.metric("Potentiel moyen", f"{avg_upside:.1f}%")

        st.divider()

        c1, c2 = st.columns(2)

        with c1:
            fig_hist = px.histogram(
                pd.DataFrame({"score": score_num}),
                x="score",
                nbins=20,
                title="Distribution du score de triage",
                color_discrete_sequence=["#2ca02c"],
            )
            fig_hist.add_vline(x=75, line_dash="dot", line_color="#28a745")
            fig_hist.add_vline(x=60, line_dash="dot", line_color="#ffc107")
            fig_hist.update_layout(height=340, margin=dict(t=40, b=20, l=20, r=20))
            st.plotly_chart(fig_hist, use_container_width=True)

        with c2:
            sc = df_ov.copy()
            sc["score_num"] = score_num
            sc["risk_num"] = risk_num
            sc["size_num"] = analysts_num.where(analysts_num > 0, 3.0)
            sc["horizon_txt"] = sc.get("horizon", pd.Series(index=sc.index)).fillna("WATCH").astype(str)
            sc["symbol"] = sc.get("symbol", pd.Series(index=sc.index)).fillna("").astype(str)
            sc = sc[sc["symbol"] != ""]

            if not sc.empty:
                fig_sc = px.scatter(
                    sc,
                    x="risk_num",
                    y="score_num",
                    size="size_num",
                    color="horizon_txt",
                    hover_name="symbol",
                    title="Carte Conviction vs Risque",
                    labels={"risk_num": "Score de risque (plus bas = mieux)", "score_num": "Score de triage"},
                )
                fig_sc.add_hline(y=70, line_dash="dot", line_color="#28a745")
                fig_sc.add_hline(y=55, line_dash="dot", line_color="#ffc107")
                fig_sc.add_vline(x=50, line_dash="dot", line_color="#ffc107")
                fig_sc.update_layout(height=340, margin=dict(t=40, b=20, l=20, r=20))
                st.plotly_chart(fig_sc, use_container_width=True)

        st.divider()

        # Performance du moteur fondamental par run
        if df_funda_history is not None and not df_funda_history.empty and "run_id" in df_funda_history.columns:
            hist = df_funda_history.copy()
            ts_col = _first_existing_column(hist, ["updated_at", "created_at", "fetched_at"])
            if ts_col:
                hist["ts"] = pd.to_datetime(hist[ts_col], errors="coerce")
                hist = hist.dropna(subset=["ts"])
                if not hist.empty:
                    hist["score_num"] = _safe_series(hist, ["score", "funda_conf"])
                    hist["risk_num"] = _safe_series(hist, ["risk_score"])
                    run_perf = (
                        hist.groupby("run_id", as_index=False)
                        .agg(
                            ts=("ts", "max"),
                            avg_score=("score_num", "mean"),
                            avg_risk=("risk_num", "mean"),
                            symbols=("symbol", "nunique"),
                        )
                        .sort_values("ts")
                    )
                    if not run_perf.empty:
                        fig_run = go.Figure()
                        fig_run.add_trace(
                            go.Scatter(
                                x=run_perf["ts"],
                                y=run_perf["avg_score"],
                                mode="lines+markers",
                                name="Score moyen",
                                line=dict(color="#28a745", width=2),
                            )
                        )
                        fig_run.add_trace(
                            go.Scatter(
                                x=run_perf["ts"],
                                y=run_perf["avg_risk"],
                                mode="lines+markers",
                                name="Risque moyen",
                                line=dict(color="#dc3545", width=2),
                            )
                        )
                        fig_run.update_layout(
                            title="Performance des runs AG3 (qualité des sorties)",
                            height=320,
                            margin=dict(t=40, b=20, l=20, r=20),
                            yaxis=dict(title="Score /100"),
                        )
                        st.plotly_chart(fig_run, use_container_width=True)

        # Tableau de synthèse
        show = pd.DataFrame()
        show["Symbole"] = df_ov.get("symbol", pd.Series(index=df_ov.index)).fillna("").astype(str)
        show["Nom"] = df_ov.get("name", pd.Series(index=df_ov.index)).fillna("")
        show["Secteur"] = df_ov.get("sector", pd.Series(index=df_ov.index)).fillna("")
        show["Score triage"] = score_num.round(0).astype(int)
        show["Risque"] = risk_num.round(0).astype(int)
        show["Qualite"] = quality_num.round(0).astype(int)
        show["Potentiel %"] = upside_num.round(1)
        show["Analystes"] = analysts_num.round(0).astype(int)
        show["Couverture %"] = coverage_num.round(1)
        show["Horizon"] = df_ov.get("horizon", pd.Series(index=df_ov.index)).fillna("WATCH")
        show["Lecture score"] = score_num.map(lambda v: _funda_eval("score", float(v))[0])
        show["Lecture risque"] = risk_num.map(lambda v: _funda_eval("risk_score", float(v))[0])
        show = show[show["Symbole"] != ""].sort_values("Score triage", ascending=False)

        st.subheader("Tableau synthèse fondamentale")
        render_interactive_table(show, key_suffix="funda_v2_overview")

    # ================================================================
    # TAB 2: VUE DETAILLEE
    # ================================================================
    with tab_detail:
        by_symbol = (
            df_funda_latest.dropna(subset=["symbol"])
            .drop_duplicates(subset=["symbol"], keep="first")
            .set_index("symbol", drop=False)
        )
        symbols = sorted(by_symbol.index.tolist())

        if not symbols:
            st.warning("Aucun symbole fondamental disponible.")
        else:
            labels_map = {}
            labels = []
            for sym in symbols:
                r = by_symbol.loc[sym]
                name = str(r.get("name", "")).strip()
                lbl = f"{sym} — {name}" if name else sym
                labels_map[lbl] = sym
                labels.append(lbl)

            selected_label = st.selectbox(
                "Sélectionner un symbole :",
                labels,
                key="funda_v2_symbol",
            )
            selected_symbol = labels_map[selected_label]
            row = by_symbol.loc[selected_symbol]

            score_v = safe_float(row.get("score", row.get("funda_conf", 0)))
            risk_v = safe_float(row.get("risk_score", 0))
            quality_v = safe_float(row.get("quality_score", 0))
            growth_v = safe_float(row.get("growth_score", 0))
            val_v = safe_float(row.get("valuation_score", 0))
            health_v = safe_float(row.get("health_score", 0))
            cons_v = safe_float(row.get("consensus_score", 0))
            cov_v = _clamp_pct(safe_float(row.get("data_coverage_pct", 0)))
            upside_v = safe_float(row.get("upside_pct", 0))
            analysts_v = safe_float(row.get("analyst_count", 0))

            st.subheader(f"🔬 {selected_symbol} — {row.get('name', '')}")
            mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)
            mc1.metric("Triage", f"{score_v:.0f}/100", delta=_funda_eval("score", score_v)[0])
            mc2.metric("Risque", f"{risk_v:.0f}/100", delta=_funda_eval("risk_score", risk_v)[0])
            mc3.metric("Horizon", str(row.get("horizon", "WATCH")))
            mc4.metric("Potentiel", f"{upside_v:.1f}%")
            mc5.metric("Analystes", f"{analysts_v:.0f}")
            mc6.metric("Couverture", f"{cov_v:.1f}%")

            st.divider()

            gauges = [
                ("score", score_v),
                ("risk_score", risk_v),
                ("quality_score", quality_v),
                ("growth_score", growth_v),
                ("valuation_score", val_v),
                ("health_score", health_v),
                ("consensus_score", cons_v),
            ]
            cols = st.columns(3)
            for idx, (k, v) in enumerate(gauges):
                if idx > 0 and idx % 3 == 0:
                    cols = st.columns(3)
                with cols[idx % 3]:
                    title = FUNDAMENTAL_META.get(k, {}).get("label", k)
                    fig = _make_funda_gauge(v, title=title, inverse=(k == "risk_score"))
                    st.plotly_chart(fig, use_container_width=True, key=f"gauge_{selected_symbol}_{k}")

            st.divider()

            # Table d'interprétation (bon/mauvais + sens de l'indicateur)
            interp_rows = []
            for key, val in [
                ("score", score_v),
                ("risk_score", risk_v),
                ("quality_score", quality_v),
                ("growth_score", growth_v),
                ("valuation_score", val_v),
                ("health_score", health_v),
                ("consensus_score", cons_v),
                ("data_coverage_pct", cov_v),
            ]:
                meta = FUNDAMENTAL_META.get(key, {})
                verdict, _ = _funda_eval(key, val)
                interp_rows.append(
                    {
                        "Indicateur": meta.get("label", key),
                        "Valeur": f"{val:.1f}/100",
                        "Lecture": verdict,
                        "Ce que cela veut dire": meta.get("desc", ""),
                    }
                )

            st.markdown("#### Interprétation des indicateurs")
            render_interactive_table(
                pd.DataFrame(interp_rows),
                key_suffix="funda_v2_interp",
                enable_controls=False,
                height=320,
            )

            # Evolution historique du symbole
            if (
                df_funda_history is not None
                and not df_funda_history.empty
                and "symbol" in df_funda_history.columns
            ):
                h = df_funda_history[df_funda_history["symbol"] == selected_symbol].copy()
                ts_col = _first_existing_column(h, ["updated_at", "created_at", "fetched_at"])
                if ts_col:
                    h["ts"] = pd.to_datetime(h[ts_col], errors="coerce")
                    h = h.dropna(subset=["ts"]).sort_values("ts")
                    if not h.empty:
                        h_score = _safe_series(h, ["score", "funda_conf"])
                        h_risk = _safe_series(h, ["risk_score"])
                        fig_hist = go.Figure()
                        fig_hist.add_trace(
                            go.Scatter(
                                x=h["ts"],
                                y=h_score,
                                mode="lines+markers",
                                name="Triage",
                                line=dict(color="#28a745", width=2),
                            )
                        )
                        fig_hist.add_trace(
                            go.Scatter(
                                x=h["ts"],
                                y=h_risk,
                                mode="lines+markers",
                                name="Risque",
                                line=dict(color="#dc3545", width=2),
                            )
                        )
                        fig_hist.update_layout(
                            title=f"Évolution historique — {selected_symbol}",
                            height=320,
                            margin=dict(t=40, b=20, l=20, r=20),
                            yaxis=dict(title="Score /100"),
                        )
                        st.plotly_chart(fig_hist, use_container_width=True)

            # Consensus + scénarios
            c_left, c_right = st.columns([1, 1])
            with c_left:
                st.markdown("#### Consensus")
                c_row = pd.DataFrame()
                if (
                    df_funda_consensus is not None
                    and not df_funda_consensus.empty
                    and "symbol" in df_funda_consensus.columns
                ):
                    c_row = df_funda_consensus[df_funda_consensus["symbol"] == selected_symbol].head(1)

                if not c_row.empty:
                    cr = c_row.iloc[0]
                    st.markdown(f"**Recommandation**: {cr.get('recommendation', '—')}")
                    st.markdown(f"**Objectif moyen**: {safe_float(cr.get('target_mean_price', 0)):.2f}")
                    st.markdown(f"**Objectif haut**: {safe_float(cr.get('target_high_price', 0)):.2f}")
                    st.markdown(f"**Objectif bas**: {safe_float(cr.get('target_low_price', 0)):.2f}")
                    st.markdown(f"**Potentiel**: {safe_float(cr.get('upside_pct', 0)):.1f}%")
                    st.markdown(f"**Analystes**: {safe_float(cr.get('analyst_count', 0)):.0f}")
                else:
                    st.caption("Pas de ligne consensus disponible.")

            with c_right:
                st.markdown("#### Scénarios de valorisation")
                scenarios = extract_valuation_scenarios(str(row.get("valuation", "")))
                current_px = safe_float(row.get("current_price", 0))
                if scenarios and current_px > 0:
                    hist_px = fetch_yfinance_history(selected_symbol, interval="1d", lookback_days=365)
                    if not hist_px.empty and "close" in hist_px.columns:
                        hist_px = hist_px.sort_values("time")
                        anchor_dt = hist_px["time"].iloc[-1].to_pydatetime()
                        anchor_px = safe_float(hist_px["close"].iloc[-1])
                    else:
                        anchor_dt = datetime.now()
                        anchor_px = current_px

                    fut = anchor_dt + timedelta(days=365)
                    probs = _estimate_scenario_probabilities(score_v, risk_v, upside_v)
                    bear_target = safe_float(scenarios.get("Bear", anchor_px * 0.85))
                    base_target = safe_float(scenarios.get("Base", anchor_px))
                    bull_target = safe_float(scenarios.get("Bull", anchor_px * 1.15))

                    fig_sc = go.Figure()
                    if not hist_px.empty and "close" in hist_px.columns:
                        fig_sc.add_trace(
                            go.Scatter(
                                x=hist_px["time"],
                                y=hist_px["close"],
                                mode="lines",
                                name="Cours réel (1 an)",
                                line=dict(color="#ffffff", width=2),
                            )
                        )
                    fig_sc.add_trace(
                        go.Scatter(
                            x=[anchor_dt, fut],
                            y=[anchor_px, bear_target],
                            mode="lines+markers+text",
                            name=f"Baissier ({probs['baissier']}%)",
                            text=[None, f"{bear_target:.1f}"],
                            textposition="top right",
                            line=dict(color="#dc3545", dash="dash"),
                        )
                    )
                    fig_sc.add_trace(
                        go.Scatter(
                            x=[anchor_dt, fut],
                            y=[anchor_px, base_target],
                            mode="lines+markers+text",
                            name=f"Central ({probs['central']}%)",
                            text=[None, f"{base_target:.1f}"],
                            textposition="top right",
                            line=dict(color="#ffc107", dash="dash"),
                        )
                    )
                    fig_sc.add_trace(
                        go.Scatter(
                            x=[anchor_dt, fut],
                            y=[anchor_px, bull_target],
                            mode="lines+markers+text",
                            name=f"Haussier ({probs['haussier']}%)",
                            text=[None, f"{bull_target:.1f}"],
                            textposition="top right",
                            line=dict(color="#28a745", dash="dash"),
                        )
                    )
                    fig_sc.update_layout(
                        height=320,
                        margin=dict(t=20, b=20, l=20, r=20),
                        title="Cours réel (1 an) + projections (12 mois)",
                    )
                    st.plotly_chart(fig_sc, use_container_width=True)
                    st.caption("Probabilités indicatives calculées par heuristique locale (pas un modèle IA prédictif).")
                else:
                    st.caption("Scénarios baissier/central/haussier non disponibles pour ce symbole.")

            # Métriques fondamentales brutes (latest)
            if df_funda_metrics is not None and not df_funda_metrics.empty and "symbol" in df_funda_metrics.columns:
                m = df_funda_metrics[df_funda_metrics["symbol"] == selected_symbol].copy()
                if not m.empty:
                    m["value_num"] = pd.to_numeric(m.get("value_num", pd.Series(index=m.index)), errors="coerce")
                    m["Valeur"] = m["value_num"]
                    if "value_text" in m.columns:
                        m["Valeur"] = m["Valeur"].fillna(m["value_text"])

                    show_cols = []
                    for col in ["section", "metric", "Valeur", "unit", "notes", "as_of_date", "extracted_at"]:
                        if col in m.columns:
                            show_cols.append(col)
                    if show_cols:
                        st.markdown("#### Métriques fondamentales (latest)")
                        render_interactive_table(
                            m[show_cols].rename(
                                columns={
                                    "section": "Section",
                                    "metric": "Indicateur",
                                    "unit": "Unité",
                                    "notes": "Notes",
                                    "as_of_date": "Date référence",
                                    "extracted_at": "Date extraction",
                                }
                            ),
                            key_suffix=f"funda_metrics_{selected_symbol}",
                            height=300,
                        )

    # ================================================================
    # TAB 3: HISTORIQUE RUNS
    # ================================================================
    with tab_runs:
        if df_funda_runs is None or df_funda_runs.empty:
            st.info("Aucun historique de runs AG3 disponible.")
        else:
            st.subheader("Historique des exécutions AG3-V2")

            run_df = df_funda_runs.copy()
            if "status" in run_df.columns:
                run_df["status"] = run_df["status"].fillna("").astype(str).str.upper()

            # KPIs run-level
            last = run_df.iloc[0]
            rk1, rk2, rk3, rk4, rk5 = st.columns(5)
            rk1.metric("Dernier statut", str(last.get("status", "—")))
            rk2.metric("Symboles", f"{safe_float(last.get('symbols_total', 0)):.0f}")
            rk3.metric("OK", f"{safe_float(last.get('symbols_ok', 0)):.0f}")
            rk4.metric("Erreur", f"{safe_float(last.get('symbols_error', 0)):.0f}")
            rk5.metric("Metrics écrits", f"{safe_float(last.get('metric_rows', 0)):.0f}")

            if "started_at" in run_df.columns:
                run_df["started_at"] = pd.to_datetime(run_df["started_at"], errors="coerce")
                chart_df = run_df.dropna(subset=["started_at"]).copy().sort_values("started_at")
                if not chart_df.empty:
                    fig_runs = go.Figure()
                    fig_runs.add_trace(
                        go.Bar(
                            x=chart_df["started_at"],
                            y=_safe_series(chart_df, ["symbols_ok"]),
                            name="Symbols OK",
                            marker_color="#28a745",
                        )
                    )
                    fig_runs.add_trace(
                        go.Bar(
                            x=chart_df["started_at"],
                            y=_safe_series(chart_df, ["symbols_error"]),
                            name="Symbols Erreur",
                            marker_color="#dc3545",
                        )
                    )
                    fig_runs.update_layout(
                        barmode="stack",
                        height=320,
                        margin=dict(t=30, b=20, l=20, r=20),
                        title="Qualité des runs AG3 dans le temps",
                    )
                    st.plotly_chart(fig_runs, use_container_width=True)

            ren_map = {
                "run_id": "Run ID",
                "started_at": "Démarré",
                "finished_at": "Terminé",
                "status": "Statut",
                "symbols_total": "Total",
                "symbols_ok": "OK",
                "symbols_error": "Erreurs",
                "triage_rows": "Rows Triage",
                "consensus_rows": "Rows Consensus",
                "metric_rows": "Rows Metrics",
                "snapshot_rows": "Rows Snapshot",
                "version": "Version",
            }
            keep = [c for c in ren_map.keys() if c in run_df.columns]
            render_interactive_table(
                run_df[keep].rename(columns={k: v for k, v in ren_map.items() if k in keep}),
                key_suffix="funda_v2_runs",
            )

