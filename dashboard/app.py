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
    """Charge les donnees depuis la base DuckDB AG2-V2.

    Retourne un dict avec les cles: df_signals, df_runs, df_signals_all.
    En cas d'indisponibilite, retourne un dict vide sans crash.
    """
    result = {}

    if not os.path.exists(DUCKDB_PATH):
        return result

    max_retries = 3
    delay = 0.5
    conn = None

    for attempt in range(max_retries):
        try:
            conn = duckdb.connect(DUCKDB_PATH, read_only=True)
            break
        except duckdb.IOException:
            if attempt < max_retries - 1:
                time.sleep(delay)
            else:
                return result
        except Exception:
            return result

    if conn is None:
        return result

    try:
        # df_signals: latest signal per symbol (most recent workflow_date)
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

        # df_runs: run log
        try:
            result["df_runs"] = conn.execute("""
                SELECT * FROM run_log ORDER BY started_at DESC
            """).fetchdf()
        except Exception:
            result["df_runs"] = pd.DataFrame()

        # df_signals_all: all signals for history
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


# ============================================================
# MAIN APP
# ============================================================

st.sidebar.title("🤖 TradingSim AI")
page = st.sidebar.radio(
    "Navigation",
    ["📊 Dashboard Trading", "🛠️ System Health (Monitoring)", "📈 Analyse Technique V2"],
)

data_dict = load_data()
if not data_dict:
    st.warning("Donnees Google Sheets indisponibles. Seule la page Analyse Technique V2 peut fonctionner.")

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
            st.subheader("📋 Positions")
            cols_show = ["name", "symbol", "sector", "quantity", "avgprice", "lastprice", "marketvalue", "unrealizedpnl"]
            cols_exist = [c for c in cols_show if c in df_clean.columns]
            df_view = df_clean[cols_exist].copy()
            if "marketvalue" in df_view.columns:
                df_view = df_view.sort_values("marketvalue", ascending=False)
            render_interactive_table(df_view, key_suffix="positions", hide_index=True)
        else:
            st.info("Portefeuille vide.")

    # TAB 2: PERFORMANCE
    with t2:
        st.subheader("Historique Valeur")
        if df_perf is not None and not df_perf.empty:
            if "timestamp" in df_perf.columns:
                df_perf = df_perf.sort_values("timestamp", ascending=True)

            for c in ["totalvalue", "equity", "cash"]:
                if c in df_perf.columns:
                    df_perf[c] = df_perf[c].apply(safe_float)

            y_cols = [c for c in ["totalvalue", "equity", "cash"] if c in df_perf.columns]
            if "timestamp" in df_perf.columns and y_cols:
                fig = px.line(df_perf, x="timestamp", y=y_cols, title="Evolution")
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Pas d'historique.")

        st.divider()
        st.subheader("💰 P&L Réalisé (Ventes)")

        if df_trans is not None and not df_trans.empty and "realizedpnl" in df_trans.columns:
            df_pnl = df_trans.copy()

            if "timestamp" in df_pnl.columns:
                df_pnl = df_pnl.sort_values("timestamp", ascending=True)
            else:
                df_pnl["timestamp"] = pd.NaT

            df_pnl["realized_num"] = df_pnl["realizedpnl"].apply(safe_float)
            df_sales = df_pnl[df_pnl["realized_num"] != 0].copy()

            if not df_sales.empty:
                df_sales["CumPnL"] = df_sales["realized_num"].cumsum()
                df_sales["Color"] = df_sales["CumPnL"].apply(lambda x: "green" if x >= 0 else "red")
                fig_pnl = px.bar(
                    df_sales,
                    x="timestamp",
                    y="CumPnL",
                    color="Color",
                    title="P&L Cumulé",
                    color_discrete_map={"green": "#28a745", "red": "#dc3545"},
                )
                st.plotly_chart(fig_pnl, use_container_width=True)
            else:
                st.info("Pas de ventes.")
        else:
            st.caption("Pas de données Transactions.")

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

    news_score_num = safe_float_series(mat["News_Score"])
    funda_conf_num = safe_float_series(mat["Funda_Conf"])
    signal_upper = mat["signal"].fillna("").astype(str).str.upper()

    mat["Attractivité"] = (
        50
        + news_score_num * 3
        + (funda_conf_num > 80).astype(int) * 10
        - (funda_conf_num < 30).astype(int) * 10
        + signal_upper.str.contains("BUY", regex=False).astype(int) * 15
        - signal_upper.str.contains("SELL", regex=False).astype(int) * 15
    ).clip(0, 100)

    age_funda = mat["Age_IA"] if "Age_IA" in mat.columns else pd.Series(999, index=mat.index)
    age_tech = mat["Age_D1"] if "Age_D1" in mat.columns else pd.Series(999, index=mat.index)

    mat["Robustesse"] = (
        funda_conf_num
        - (age_funda > 90).astype(int) * 20
        - (age_tech > 7).astype(int) * 10
    ).clip(0, 100)

    # La taille de la bulle depend de la confiance fondamentale
    mat["Taille"] = funda_conf_num.clip(lower=15)

    if "sector" not in mat.columns:
        mat["sector"] = "N/A"

    fig_mat = px.scatter(
        mat, x="Robustesse", y="Attractivité",
        size="Taille", color="sector",
        hover_name="name" if "name" in mat.columns else None, text="symbol",
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

