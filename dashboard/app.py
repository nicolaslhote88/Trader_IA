import json
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

st.set_page_config(page_title="AI Trading Executor", layout="wide", page_icon="🤖")

SHEET_ID = os.getenv("SHEET_ID")
CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/secrets/service_account.json")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/files/duckdb/ag2_v2.duckdb")
AG3_DUCKDB_PATH = os.getenv("AG3_DUCKDB_PATH", "/files/duckdb/ag3_v2.duckdb")
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
        st.error(f"Configuration manquante: {', '.join(missing)}")
        return False

    if not os.path.exists(CREDENTIALS_FILE):
        st.error(f"Fichier de credentials introuvable: {CREDENTIALS_FILE}")
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
        "News_History": "News_History",
        "AG3_Triage_History": "AG3_Triage_History",
        "Technical_Analysis": "AG2 - étape 1 - sortie",
        "Research_Notes": "Research_Notes",
        "Analyst_Consensus": "research_analyst_consensus",
        "news_raw_Symbol": "news_raw_Symbol",
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
    """Charge les donnees DuckDB AG2 (technique) + AG3 (fondamentale)."""
    result = {
        "df_signals": pd.DataFrame(),
        "df_runs": pd.DataFrame(),
        "df_signals_all": pd.DataFrame(),
        "df_funda_latest": pd.DataFrame(),
        "df_funda_runs": pd.DataFrame(),
        "df_funda_history": pd.DataFrame(),
        "df_funda_consensus": pd.DataFrame(),
        "df_funda_metrics": pd.DataFrame(),
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
                parts.append(f"< {name}({val:.2f}) ❌")
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


# ============================================================
# MAIN APP
# ============================================================

st.sidebar.title("🤖 TradingSim AI")
page = st.sidebar.radio(
    "Navigation",
    [
        "📊 Dashboard Trading",
        "🛠️ System Health (Monitoring)",
        "📈 Analyse Technique V2",
        "📚 Analyse Fondamentale V2",
    ],
)

data_dict = load_data()
if not data_dict:
    st.warning("Donnees Google Sheets indisponibles. Seules les pages d'analyse V2 (Technique/Fondamentale) peuvent fonctionner.")

# Load DuckDB data (non-blocking)
duckdb_data = load_duckdb_data()

# ------------------------------------------------------------
# PRE-CALCULS (ROBUSTES)
# ------------------------------------------------------------

df_univ = data_dict.get("Universe", pd.DataFrame()) if data_dict else pd.DataFrame()
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

if page == "📊 Dashboard Trading":
    if not data_dict:
        st.error("Donnees Google Sheets requises pour cette page.")
        st.stop()

    st.title("🤖 AI Trading Executor Dashboard")

    if st.button("🔄 Rafraîchir"):
        load_data.clear()
        load_duckdb_data.clear()
        st.rerun()

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Capital Départ", f"{init_cap:,.0f} €")
    c2.metric("Valeur Totale", f"{total_val:,.2f} €", delta=f"{total_val - init_cap:,.2f} €")
    c3.metric("Cash", f"{cash:,.2f} €")
    c4.metric("Investi", f"{invest:,.2f} €")
    c5.metric("ROI", f"{roi * 100:.2f} %")
    c6.metric("% Cash", f"{cash_pct:.1f} %")

    t1, t2, t3, t4 = st.tabs(["💼 Portefeuille", "📈 Performance", "🧠 Cerveau IA", "🌍 Marché & Recherche"])

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
                eff = perf_ts.copy()
                eff["total_value"] = eff["total_value"].replace(0, pd.NA)
                eff["cash_pct"] = (eff["cash_value"] / eff["total_value"] * 100).fillna(0.0).clip(lower=0, upper=100)
                eff["invested_pct"] = (eff["invested_value"] / eff["total_value"] * 100).fillna(0.0).clip(lower=0, upper=100)
                eff["roi_pct"] = ((eff["total_value"] / init_cap) - 1.0) * 100 if init_cap else 0.0
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
                fig_eff.update_layout(
                    title="Exposition capital vs performance",
                    height=380,
                    margin=dict(t=50, b=20, l=20, r=20),
                    yaxis=dict(title="Allocation (%)", range=[0, 100]),
                    yaxis2=dict(title="ROI (%)", overlaying="y", side="right", showgrid=False),
                )
                st.plotly_chart(fig_eff, use_container_width=True)

                avg_invested = float(eff["invested_value"].mean()) if "invested_value" in eff.columns else 0.0
                roi_global_pct = (total_gain / init_cap * 100) if init_cap else 0.0
                roic_pct = (total_gain / avg_invested * 100) if avg_invested > 0 else 0.0
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
                    f"Montant moyen investi: {avg_invested:,.2f} EUR | "
                    "ROIC = Gains / montant moyen investi"
                )

        with v_quality:
            quality_df = _build_trade_quality_dataframe(tx_norm)
            hist_df = quality_df.dropna(subset=["trade_return_pct"]) if not quality_df.empty else pd.DataFrame()
            scat_df = quality_df.dropna(subset=["duration_days", "trade_return_pct"]) if not quality_df.empty else pd.DataFrame()

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

            if scat_df.empty:
                st.info("Pas assez d'historique pour le nuage Duree vs ROI.")
            else:
                fig_scatter = px.scatter(
                    scat_df,
                    x="duration_days",
                    y="trade_return_pct",
                    color="agent_label",
                    hover_data=["symbol", "realized_pnl"],
                    title="Duree de detention vs ROI",
                )
                fig_scatter.add_hline(y=0, line_dash="dash", line_color="#999")
                fig_scatter.update_layout(height=360, margin=dict(t=50, b=20, l=20, r=20))
                st.plotly_chart(fig_scatter, use_container_width=True)

        with v_risk:
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
                        name="Drawdown %",
                    )
                )
                fig_dd.update_layout(
                    title="Underwater plot (drawdown)",
                    height=340,
                    margin=dict(t=50, b=20, l=20, r=20),
                    yaxis=dict(title="Drawdown %"),
                )
                st.plotly_chart(fig_dd, use_container_width=True)

            cards = _compute_risk_scorecards(perf_ts, tx_norm)
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Sharpe Ratio", f"{cards['sharpe']:.2f}")
            pf_val = cards["profit_factor"]
            c2.metric("Profit Factor", "inf" if pf_val == float("inf") else f"{pf_val:.2f}")
            c3.metric("Win Rate", f"{cards['win_rate']:.1f}%")
            c4.metric("Max Drawdown", f"{cards['max_drawdown_pct']:.2f}%")
            st.caption("Repere: Profit Factor > 1.5")

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

        st.subheader("🛡️ Alertes")
        if df_alt is not None and not df_alt.empty:
            render_interactive_table(df_alt, key_suffix="alt")
        else:
            st.caption("RAS")

    # TAB 4: MARCHE & RECHERCHE
    with t4:
        df_news = data_dict.get("News_History", pd.DataFrame())
        df_news_sym = data_dict.get("news_raw_Symbol", pd.DataFrame())
        df_res = enrich_df_with_name(data_dict.get("AG3_Triage_History", pd.DataFrame()), df_univ)

        st_macro, st_research = st.tabs(["🌍 Macro & Buzz", "🔬 Recherche"])

        with st_macro:
            st.subheader("🌡️ Météo Secteurs (30j)")
            if df_news is not None and not df_news.empty:
                df_sec = calculate_sector_sentiment(df_news)
                if df_sec is not None and not df_sec.empty:
                    fig = px.bar(df_sec, x="NetScore", y="Sector", orientation="h", title="Momentum Sectoriel", text="NetScore")
                    fig.update_traces(marker_color=df_sec["Color"])
                    st.plotly_chart(fig, use_container_width=True)

            st.divider()
            st.subheader("📢 Palmarès Actions (30j)")
            if df_news_sym is not None and not df_news_sym.empty:
                df_sym = calculate_symbol_momentum(df_news_sym)
                if df_sym is not None and not df_sym.empty:
                    fig = px.bar(df_sym, x="NetScore", y="Label", orientation="h", title="Momentum Actions", text="NetScore")
                    fig.update_traces(marker_color=df_sym["Color"])
                    st.plotly_chart(fig, use_container_width=True)

        with st_research:
            if df_res is None or df_res.empty:
                st.info("📭 Aucune note de recherche disponible.")
            else:
                df_viz = df_res.copy()

                if "score" in df_viz.columns:
                    df_viz["score_num"] = df_viz["score"].apply(safe_float)
                else:
                    df_viz["score_num"] = 0.0

                if "sector" not in df_viz.columns:
                    df_viz["sector"] = "Indéfini"
                if "name" not in df_viz.columns:
                    if "symbol" in df_viz.columns:
                        df_viz["name"] = df_viz["symbol"]
                    else:
                        df_viz["name"] = "N/A"

                top_picks = df_viz[df_viz["score_num"] >= 70]

                k1, k2, k3, k4 = st.columns(4)
                k1.metric("Dossiers Analysés", len(df_viz))
                k2.metric("⭐ Top Convictions", len(top_picks))
                k3.metric("Qualité Moyenne", f"{df_viz['score_num'].mean():.1f}/100" if len(df_viz) else "0/100")

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
                    st.subheader("🗺️ Carte des Opportunités")
                    if not df_viz.empty:
                        if "symbol" in df_viz.columns:
                            path = [px.Constant("Univers"), "sector", "symbol"]
                        else:
                            path = [px.Constant("Univers"), "sector"]

                        fig_tree = px.treemap(
                            df_viz,
                            path=path,
                            values="score_num",
                            color="score_num",
                            color_continuous_scale=["#d73027", "#fee08b", "#1a9850"],
                            range_color=[30, 90],
                            hover_data=["name"] if "name" in df_viz.columns else None,
                            title="Taille = Score",
                        )
                        st.plotly_chart(fig_tree, use_container_width=True)

                with c_top:
                    st.subheader("🏆 Top 3")
                    if "symbol" in df_viz.columns:
                        for _, row in df_viz.sort_values("score_num", ascending=False).head(3).iterrows():
                            with st.container(border=True):
                                st.markdown(f"**{row.get('symbol','')}** — {row.get('score_num',0):.0f}/100")
                                st.caption(f"{row.get('name','')}")
                                if st.button(f"🔍 Voir {row.get('symbol','')}", key=f"btn_{row.get('symbol','NA')}"):
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

elif page == "🛠️ System Health (Monitoring)":
    if not data_dict:
        st.error("Donnees Google Sheets requises pour cette page.")
        st.stop()

    st.title("🛠️ Tour de Contrôle")

    if st.button("🔄 Rafraîchir"):
        load_data.clear()
        load_duckdb_data.clear()
        st.rerun()

    df_univ2 = data_dict.get("Universe", pd.DataFrame())
    df_news_hist = data_dict.get("news_raw_Symbol", pd.DataFrame())

    # Proxy confiance si présent
    df_funda_hist = data_dict.get("AG3_Triage_History", pd.DataFrame())

    # Split fondamental IA
    df_research_notes = data_dict.get("Research_Notes", pd.DataFrame())

    # Split fondamental Consensus
    df_consensus = data_dict.get("Analyst_Consensus", pd.DataFrame())

    # Technique H1/D1
    df_tech_hist = data_dict.get("Technical_Analysis", pd.DataFrame())

    if df_univ2 is None or df_univ2.empty:
        st.warning("Univers vide.")
        st.stop()

    if "symbol" not in df_univ2.columns:
        st.warning("Onglet Universe: colonne 'symbol' manquante.")
        st.stop()

    # Normalisation symbol_key pour jointures
    df_univ2 = norm_symbol(df_univ2, "symbol")

    # ✅ IMPORTANT : base_cols DOIT être défini AVANT usage
    base_cols = [c for c in ["symbol_key", "symbol", "name", "sector", "industry"] if c in df_univ2.columns]
    if "symbol_key" not in base_cols:
        st.warning("Universe: impossible de créer 'symbol_key' (colonne symbol manquante ou vide).")
        st.stop()

    monitor_df = df_univ2[base_cols].drop_duplicates(subset="symbol_key").copy()

    # ============================================================
    # 1) MERGE NEWS
    # ============================================================
    if df_news_hist is not None and not df_news_hist.empty:
        df_news_hist = norm_symbol(df_news_hist, "symbol")

        if "symbol_key" in df_news_hist.columns and "publishedat" in df_news_hist.columns:
            df_news_hist["publishedat"] = pd.to_datetime(df_news_hist["publishedat"], errors="coerce", utc=True)
            df_news_hist = df_news_hist.dropna(subset=["publishedat"])

            latest_news = (
                df_news_hist.sort_values("publishedat", ascending=False)
                .groupby("symbol_key")
                .first()
                .reset_index()
            )

            cols_n = [c for c in ["symbol_key", "publishedat", "impactscore"] if c in latest_news.columns]
            if cols_n:
                monitor_df = monitor_df.merge(latest_news[cols_n], on="symbol_key", how="left")
                monitor_df = monitor_df.rename(columns={"publishedat": "Last_News_Date", "impactscore": "News_Score"})

    # ============================================================
    # 2) MERGE FUNDA CONF (proxy) depuis AG3_Triage_History
    # ============================================================
    if df_funda_hist is not None and not df_funda_hist.empty:
        df_funda_hist = norm_symbol(df_funda_hist, "symbol")

        date_col_f = next((c for c in ["updatedat", "fetchedat"] if c in df_funda_hist.columns), None)

        if "symbol_key" in df_funda_hist.columns and date_col_f:
            df_funda_hist[date_col_f] = pd.to_datetime(df_funda_hist[date_col_f], errors="coerce", utc=True)
            df_funda_hist = df_funda_hist.dropna(subset=[date_col_f])

            latest_funda = (
                df_funda_hist.sort_values(date_col_f, ascending=False)
                .groupby("symbol_key")
                .first()
                .reset_index()
            )

            conf_col = next((c for c in ["score", "confidence", "funda_conf"] if c in latest_funda.columns), None)
            cols_f = [c for c in ["symbol_key", date_col_f, conf_col] if c and c in latest_funda.columns]

            if cols_f:
                monitor_df = monitor_df.merge(latest_funda[cols_f], on="symbol_key", how="left")

                ren_f = {date_col_f: "Last_Funda_Date"}
                if conf_col:
                    ren_f[conf_col] = "Funda_Conf"
                monitor_df = monitor_df.rename(columns=ren_f)

    # ============================================================
    # 3) MERGE IA (Agent 3) depuis Research_Notes
    # ============================================================
    if df_research_notes is not None and not df_research_notes.empty:
        df_research_notes = norm_symbol(df_research_notes, "symbol")

        ia_date_col = next(
            (
                c
                for c in [
                    "updatedat",
                    "updated_at",
                    "fetchedat",
                    "created_at",
                    "timestamp",
                    "date",
                ]
                if c in df_research_notes.columns
            ),
            None,
        )
        ia_status_col = next((c for c in ["status", "state"] if c in df_research_notes.columns), None)

        if "symbol_key" in df_research_notes.columns and ia_date_col:
            df_research_notes[ia_date_col] = pd.to_datetime(df_research_notes[ia_date_col], errors="coerce", utc=True)
            df_research_notes = df_research_notes.dropna(subset=[ia_date_col])

            latest_ia = (
                df_research_notes.sort_values(ia_date_col, ascending=False)
                .groupby("symbol_key")
                .first()
                .reset_index()
            )

            cols_ia = ["symbol_key", ia_date_col]
            if ia_status_col and ia_status_col in latest_ia.columns:
                cols_ia.append(ia_status_col)

            monitor_df = monitor_df.merge(latest_ia[cols_ia], on="symbol_key", how="left")

            ren_ia = {ia_date_col: "Last_IA_Date"}
            if ia_status_col:
                ren_ia[ia_status_col] = "IA_Status"
            monitor_df = monitor_df.rename(columns=ren_ia)

    # ============================================================
    # 4) MERGE CONSENSUS (Boursorama) depuis Analyst_Consensus
    # ============================================================
    if df_consensus is not None and not df_consensus.empty:
        df_consensus = norm_symbol(df_consensus, "symbol")

        cons_date_col = next(
            (
                c
                for c in [
                    "updatedat",
                    "updated_at",
                    "fetchedat",
                    "created_at",
                    "timestamp",
                    "date",
                ]
                if c in df_consensus.columns
            ),
            None,
        )
        cons_view_col = next(
            (
                c
                for c in [
                    "consensusinterpretation",
                    "consensus_view",
                    "consensus",
                    "recommendation",
                ]
                if c in df_consensus.columns
            ),
            None,
        )
        cons_note_col = next(
            (
                c
                for c in ["mediannote", "median_note", "median", "medianrating"]
                if c in df_consensus.columns
            ),
            None,
        )

        if "symbol_key" in df_consensus.columns and cons_date_col:
            df_consensus[cons_date_col] = pd.to_datetime(df_consensus[cons_date_col], errors="coerce", utc=True)
            df_consensus = df_consensus.dropna(subset=[cons_date_col])

            latest_cons = (
                df_consensus.sort_values(cons_date_col, ascending=False)
                .groupby("symbol_key")
                .first()
                .reset_index()
            )

            cols_cons = ["symbol_key", cons_date_col]
            if cons_view_col and cons_view_col in latest_cons.columns:
                cols_cons.append(cons_view_col)
            if cons_note_col and cons_note_col in latest_cons.columns:
                cols_cons.append(cons_note_col)

            monitor_df = monitor_df.merge(latest_cons[cols_cons], on="symbol_key", how="left")

            ren_cons = {cons_date_col: "Last_Consensus_Date"}
            if cons_view_col:
                ren_cons[cons_view_col] = "Consensus_View"
            if cons_note_col:
                ren_cons[cons_note_col] = "Consensus_MedianNote"
            monitor_df = monitor_df.rename(columns=ren_cons)

    # ============================================================
    # 5) MERGE TECH (Split H1/D1) - Google Sheets + DuckDB complement
    # ============================================================
    if df_tech_hist is not None and not df_tech_hist.empty:
        df_tech_hist = norm_symbol(df_tech_hist, "symbol")

        h1_col = "h1_date" if "h1_date" in df_tech_hist.columns else None
        d1_col = "d1_date" if "d1_date" in df_tech_hist.columns else None
        fallback_date_col = next((c for c in ["date", "created_at"] if c in df_tech_hist.columns), None)

        if h1_col:
            df_tech_hist[h1_col] = pd.to_datetime(df_tech_hist[h1_col], errors="coerce", utc=True)
        if d1_col:
            df_tech_hist[d1_col] = pd.to_datetime(df_tech_hist[d1_col], errors="coerce", utc=True)
        if fallback_date_col:
            df_tech_hist[fallback_date_col] = pd.to_datetime(df_tech_hist[fallback_date_col], errors="coerce", utc=True)

        df_tech_hist["__sort_dt"] = pd.NaT
        if d1_col:
            df_tech_hist["__sort_dt"] = df_tech_hist[d1_col]
        if fallback_date_col:
            df_tech_hist["__sort_dt"] = df_tech_hist["__sort_dt"].fillna(df_tech_hist[fallback_date_col])
        if h1_col:
            df_tech_hist["__sort_dt"] = df_tech_hist["__sort_dt"].fillna(df_tech_hist[h1_col])

        df_tech_hist = df_tech_hist.dropna(subset=["__sort_dt"])

        latest_tech = (
            df_tech_hist.sort_values("__sort_dt", ascending=False)
            .groupby("symbol_key")
            .first()
            .reset_index()
        )

        sig_col = None
        for candidate in ["signal", "d1_action", "action"]:
            if candidate in latest_tech.columns:
                sig_col = candidate
                break

        cols_t = ["symbol_key"]
        if h1_col and h1_col in latest_tech.columns:
            cols_t.append(h1_col)
        if d1_col and d1_col in latest_tech.columns:
            cols_t.append(d1_col)
        if fallback_date_col and fallback_date_col in latest_tech.columns:
            cols_t.append(fallback_date_col)
        if sig_col:
            cols_t.append(sig_col)

        monitor_df = monitor_df.merge(latest_tech[cols_t], on="symbol_key", how="left")

        ren_t = {}
        if h1_col:
            ren_t[h1_col] = "Last_H1_Date"
        if d1_col:
            ren_t[d1_col] = "Last_D1_Date"
        if fallback_date_col:
            ren_t[fallback_date_col] = "Last_Tech_Date"
        if sig_col:
            ren_t[sig_col] = "signal"

        if ren_t:
            monitor_df = monitor_df.rename(columns=ren_t)

    # DuckDB complement for tech data
    duck_signals = duckdb_data.get("df_signals", pd.DataFrame())
    if duck_signals is not None and not duck_signals.empty:
        duck_tech = duck_signals.copy()
        if "symbol" in duck_tech.columns:
            duck_tech = norm_symbol(duck_tech, "symbol")

            duck_cols = ["symbol_key"]
            if "workflow_date" in duck_tech.columns:
                duck_tech["workflow_date"] = pd.to_datetime(duck_tech["workflow_date"], errors="coerce", utc=True)
                duck_cols.append("workflow_date")
            if "d1_action" in duck_tech.columns:
                duck_cols.append("d1_action")

            duck_latest = duck_tech[duck_cols].dropna(subset=["symbol_key"]).drop_duplicates(subset=["symbol_key"])
            monitor_df = monitor_df.merge(duck_latest, on="symbol_key", how="left", suffixes=("", "_duck"))

            if "Last_H1_Date" not in monitor_df.columns:
                monitor_df["Last_H1_Date"] = pd.NaT
            if "Last_D1_Date" not in monitor_df.columns:
                monitor_df["Last_D1_Date"] = pd.NaT
            if "signal" not in monitor_df.columns:
                monitor_df["signal"] = ""

            if "workflow_date" in monitor_df.columns:
                monitor_df["Last_H1_Date"] = monitor_df["Last_H1_Date"].combine_first(monitor_df["workflow_date"])
                monitor_df["Last_D1_Date"] = monitor_df["Last_D1_Date"].combine_first(monitor_df["workflow_date"])

            if "d1_action" in monitor_df.columns:
                signal_clean = monitor_df["signal"].fillna("").astype(str).str.strip()
                d1_clean = monitor_df["d1_action"].fillna("").astype(str).str.strip()
                missing_signal = signal_clean.eq("") | signal_clean.str.lower().isin(["nan", "none"])
                monitor_df.loc[missing_signal & d1_clean.ne(""), "signal"] = d1_clean[missing_signal & d1_clean.ne("")]

            monitor_df = monitor_df.drop(columns=[c for c in ["workflow_date", "d1_action"] if c in monitor_df.columns])

    # ============================================================
    # Remplissages par defaut
    # ============================================================
    for c in [
        "Last_News_Date",
        "Last_Funda_Date",
        "Last_IA_Date",
        "Last_Consensus_Date",
        "Last_Tech_Date",
        "Last_H1_Date",
        "Last_D1_Date",
    ]:
        if c not in monitor_df.columns:
            monitor_df[c] = None

    if "News_Score" not in monitor_df.columns:
        monitor_df["News_Score"] = 0
    if "Funda_Conf" not in monitor_df.columns:
        monitor_df["Funda_Conf"] = 50
    if "signal" not in monitor_df.columns:
        monitor_df["signal"] = ""
    if "Consensus_View" not in monitor_df.columns:
        monitor_df["Consensus_View"] = ""
    if "Consensus_MedianNote" not in monitor_df.columns:
        monitor_df["Consensus_MedianNote"] = None

    # ============================================================
    # Fraîcheur : News / IA / Consensus / H1 / D1
    # ============================================================
    news_pairs = monitor_df["Last_News_Date"].map(lambda v: check_freshness(v, 7))
    ia_pairs = monitor_df["Last_IA_Date"].map(lambda v: check_freshness(v, 90))
    cons_pairs = monitor_df["Last_Consensus_Date"].map(lambda v: check_freshness(v, 30))
    h1_pairs = monitor_df["Last_H1_Date"].map(lambda v: check_freshness(v, 2))

    d1_ref = monitor_df["Last_D1_Date"].where(monitor_df["Last_D1_Date"].notna(), monitor_df["Last_Tech_Date"])
    d1_pairs = d1_ref.map(lambda v: check_freshness(v, 3))

    monitor_df["Age_N"] = news_pairs.str[1].astype(int)
    monitor_df["Age_IA"] = ia_pairs.str[1].astype(int)
    monitor_df["Age_Cons"] = cons_pairs.str[1].astype(int)
    monitor_df["Age_H1"] = h1_pairs.str[1].astype(int)
    monitor_df["Age_D1"] = d1_pairs.str[1].astype(int)

    ico_n = news_pairs.str[0]
    ico_ia = ia_pairs.str[0]
    ico_cons = cons_pairs.str[0]
    ico_h1 = h1_pairs.str[0]
    ico_d1 = d1_pairs.str[0]

    monitor_df["News_Stat"] = ico_n + monitor_df["Age_N"].astype(str) + "j"

    cons_view = monitor_df["Consensus_View"].fillna("").astype(str).str.strip()
    cons_stat = "Cons:" + ico_cons + monitor_df["Age_Cons"].astype(str) + "j"
    cons_stat = cons_stat.where(cons_view == "", cons_stat + " (" + cons_view + ")")

    monitor_df["Funda_Stat"] = (
        "IA:" + ico_ia + monitor_df["Age_IA"].astype(str) + "j | " + cons_stat
    )
    monitor_df["Tech_Stat"] = (
        "H1:" + ico_h1 + monitor_df["Age_H1"].astype(str) + "j | "
        + "D1:" + ico_d1 + monitor_df["Age_D1"].astype(str) + "j"
    )

    # ============================================================
    # KPIs
    # ============================================================
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Univers", f"{len(monitor_df)}")

    miss_n = int((monitor_df["Age_N"] > 14).sum()) if "Age_N" in monitor_df.columns else 0
    miss_ia = int((monitor_df["Age_IA"] > 90).sum()) if "Age_IA" in monitor_df.columns else 0
    miss_cons = int((monitor_df["Age_Cons"] > 30).sum()) if "Age_Cons" in monitor_df.columns else 0
    miss_d1 = int((monitor_df["Age_D1"] > 3).sum()) if "Age_D1" in monitor_df.columns else 0

    c2.metric("News > 14j", f"{miss_n}", delta="-Bad" if miss_n > 0 else "OK", delta_color="inverse")
    c3.metric("IA > 90j", f"{miss_ia}", delta="-Bad" if miss_ia > 0 else "OK", delta_color="inverse")
    c4.metric("Cons > 30j", f"{miss_cons}", delta="-Bad" if miss_cons > 0 else "OK", delta_color="inverse")
    c5.metric("D1 > 3j", f"{miss_d1}", delta="-Bad" if miss_d1 > 0 else "OK", delta_color="inverse")

    # ============================================================
    # Matrice Risk/Reward
    # ============================================================
    st.divider()
    st.subheader("🎯 Matrice Risk/Reward")

    mat = monitor_df.copy()
    news_score_num = pd.to_numeric(safe_float_series(mat["News_Score"]), errors="coerce").fillna(0.0)
    funda_conf_num = pd.to_numeric(safe_float_series(mat["Funda_Conf"]), errors="coerce").fillna(0.0)
    signal_upper = mat["signal"].fillna("").astype(str).str.upper()

    mat["Attractivité"] = (
        50
        + news_score_num * 3
        + (funda_conf_num > 80).astype(int) * 10
        - (funda_conf_num < 30).astype(int) * 10
        + signal_upper.str.contains("BUY", regex=False).astype(int) * 15
        - signal_upper.str.contains("SELL", regex=False).astype(int) * 15
    )

    age_funda = mat["Age_IA"] if "Age_IA" in mat.columns else pd.Series(999, index=mat.index)
    age_tech = mat["Age_D1"] if "Age_D1" in mat.columns else pd.Series(999, index=mat.index)

    mat["Robustesse"] = (
        funda_conf_num
        - (age_funda > 90).astype(int) * 20
        - (age_tech > 7).astype(int) * 10
    )

    # La taille de la bulle depend de la confiance fondamentale
    mat["Taille"] = funda_conf_num

    if "sector" not in mat.columns:
        mat["sector"] = "N/A"

    # Hard guardrail: Plotly n'accepte pas de NaN dans size/x/y.
    mat_plot = mat.copy()
    mat_plot["Attractivité"] = pd.to_numeric(mat_plot["Attractivité"], errors="coerce").replace([float("inf"), float("-inf")], pd.NA).fillna(50.0).clip(0, 100)
    mat_plot["Robustesse"] = pd.to_numeric(mat_plot["Robustesse"], errors="coerce").replace([float("inf"), float("-inf")], pd.NA).fillna(50.0).clip(0, 100)
    mat_plot["Taille"] = pd.to_numeric(mat_plot["Taille"], errors="coerce").replace([float("inf"), float("-inf")], pd.NA).fillna(15.0).clip(lower=15.0)

    fig_mat = px.scatter(
        mat_plot, x="Robustesse", y="Attractivité",
        size="Taille", color="sector",
        hover_name="name" if "name" in mat_plot.columns else None, text="symbol",
        title="Carte des Opportunités (Risk/Reward)",
        labels={"Robustesse": "🛡️ Robustesse (Qualité & Fraîcheur)", "Attractivité": "🚀 Attractivité (Momentum & News)"}
    )

    # Quadrants
    fig_mat.add_hline(y=50, line_dash="dot", line_color="grey")
    fig_mat.add_vline(x=50, line_dash="dot", line_color="grey")

    # Annotations Zones
    fig_mat.add_annotation(x=95, y=95, text="🌟 TOP", showarrow=False, font=dict(color="green", size=14))
    fig_mat.add_annotation(x=5, y=95, text="🎰 SPÉCULATIF", showarrow=False, font=dict(color="orange"))
    fig_mat.add_annotation(x=95, y=5, text="🐢 STABLE", showarrow=False, font=dict(color="blue"))
    fig_mat.add_annotation(x=5, y=5, text="🗑️ FLOP", showarrow=False, font=dict(color="red"))

    fig_mat.update_traces(textposition="top center")
    fig_mat.update_layout(height=650)
    st.plotly_chart(fig_mat, use_container_width=True)


    # ============================================================
    # Détail Synchronisation
    # ============================================================
    st.subheader("Détail Synchronisation")
    cols_sync = [c for c in ["symbol", "name", "News_Stat", "Funda_Stat", "Tech_Stat"] if c in monitor_df.columns]
    render_interactive_table(monitor_df[cols_sync], key_suffix="sync_detail", hide_index=True)


# ============================================================
# PAGE 3: ANALYSE TECHNIQUE V2
# ============================================================

elif page == "📈 Analyse Technique V2":
    st.title("📈 Analyse Technique V2 (AG2)")

    if st.button("🔄 Rafraîchir", key="refresh_v2"):
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
elif page == "📚 Analyse Fondamentale V2":
    st.title("📚 Analyse Fondamentale V2 (AG3)")

    if st.button("🔄 Rafraîchir", key="refresh_funda_v2"):
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

