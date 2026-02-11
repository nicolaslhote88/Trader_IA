import json
import os
import re
import time
from datetime import datetime, timedelta

import duckdb
import gspread
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from oauth2client.service_account import ServiceAccountCredentials

# ============================================================
# CONFIGURATION
# ============================================================

st.set_page_config(page_title="AI Trading Executor", layout="wide", page_icon="🤖")

SHEET_ID = os.getenv("SHEET_ID")
CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/secrets/service_account.json")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/files/duckdb/ag2_v2.duckdb")

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

# ============================================================
# HELPERS GENERAUX
# ============================================================


def safe_float(val: object) -> float:
    """Conversion float robuste (support virgule, nettoyage, valeurs vides)."""
    try:
        if val is None:
            return 0.0
        if isinstance(val, bool):
            return float(val)
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip()
        if s == "" or s.lower() == "nan":
            return 0.0
        s = s.replace(",", ".")
        s = re.sub(r"[^\d.\-]", "", s)
        if s in ("", "-", "."):
            return 0.0
        return float(s)
    except Exception:
        return 0.0


def safe_json_parse(json_str: object) -> dict:
    if not isinstance(json_str, str) or not json_str.strip():
        return {}
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        return {}


def clean_text(text: object) -> str:
    if not text:
        return ""
    return str(text).replace("\\n", " ").replace("\n", " ").strip()


def clean_research_text(text: object) -> str:
    if not isinstance(text, str):
        return ""
    t = text.replace("\n", "<br>")
    t = re.sub(r"(Note:|BestScenario:|Consensus:|Risque:|Action:|Why:)", r"<strong>\1</strong>", t)
    return t


def format_impact_html(val: object) -> str:
    try:
        v = float(val)
        color = "#888"
        if v >= 4:
            color = "#28a745"
        elif v <= -4:
            color = "#dc3545"
        elif v > 0:
            color = "#90ee90"
        elif v < 0:
            color = "#f08080"
        return f'<span style="color:{color}; font-weight:bold;">{val}</span>'
    except Exception:
        return str(val)


def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Met toutes les colonnes en minuscules pour eviter les erreurs de casse."""
    if df is None or df.empty:
        return df
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def norm_symbol(df: pd.DataFrame, col_name: str = "symbol") -> pd.DataFrame:
    """Cree une cle de jointure normalisee (symbol_key)."""
    if df is None or df.empty:
        return df
    col_lower = str(col_name).lower()
    if col_lower in df.columns:
        df["symbol_key"] = df[col_lower].astype(str).str.strip().str.upper()
    return df


def enrich_df_with_name(df: pd.DataFrame, universe_df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute/complete la colonne name et sector fallback depuis Universe via symbol."""
    if df is None or df.empty or universe_df is None or universe_df.empty:
        return df
    if "symbol" not in df.columns:
        return df

    u_clean = universe_df.copy()
    if "symbol" in u_clean.columns:
        u_clean["symbol"] = u_clean["symbol"].astype(str).str.strip()

    # Name mapping
    if "name" in u_clean.columns:
        mapping = pd.Series(u_clean["name"].values, index=u_clean["symbol"]).to_dict()
        df["symbol"] = df["symbol"].astype(str).str.strip()
        df["name_enrich"] = df["symbol"].map(mapping).fillna("")
        if "name" not in df.columns:
            df["name"] = df["name_enrich"]
        else:
            df["name"] = df["name"].fillna(df["name_enrich"])

    # Sector fallback
    if "sector" not in df.columns and "sector" in u_clean.columns:
        sec_map = pd.Series(u_clean["sector"].values, index=u_clean["symbol"]).to_dict()
        df["sector"] = df["symbol"].map(sec_map).fillna("N/A")

    return df


def check_freshness(date_val: object, days_limit: int) -> tuple[str, int]:
    """Retourne (icone, age_en_jours). Robust: None / NaT / formats ISO."""
    try:
        if date_val is None:
            return "❌", 999

        # Pandas NaT / NaN
        if pd.isna(date_val):
            return "❌", 999

        s = str(date_val).strip()
        if s == "" or s.lower() in ("nan", "nat", "none", "null"):
            return "❌", 999

        # Cas pandas Timestamp / datetime
        if isinstance(date_val, (pd.Timestamp, datetime)):
            d = date_val.to_pydatetime() if isinstance(date_val, pd.Timestamp) else date_val
        else:
            d_str = s.replace("Z", "").split(".")[0]
            try:
                d = datetime.fromisoformat(d_str)
            except ValueError:
                d = pd.to_datetime(d_str, errors="coerce")
                if pd.isna(d):
                    return "❓", 999
                d = d.to_pydatetime()

        # Neutraliser TZ
        if getattr(d, "tzinfo", None) is not None:
            d = d.replace(tzinfo=None)

        delta = (datetime.now() - d).days

        if delta <= days_limit:
            return "✅", int(delta)
        if delta <= days_limit * 3:
            return "⚠️", int(delta)
        return "🛑", int(delta)

    except Exception:
        return "❓", 999


# ============================================================
# HELPER : EXTRACTION SCENARIOS (Bear/Base/Bull)
# ============================================================


def extract_valuation_scenarios(text: object) -> dict:
    """Extrait des prix Bear/Base/Bull depuis un texte (moyenne si range)."""
    if not isinstance(text, str):
        return {}

    scenarios = {}
    patterns = {
        "Bear": r"Bear.*?[:\s]+~?([\d.,]+)(?:[–-]([\d.,]+))?",
        "Base": r"Base.*?[:\s]+~?([\d.,]+)(?:[–-]([\d.,]+))?",
        "Bull": r"Bull.*?[:\s]+~?([\d.,]+)(?:[–-]([\d.,]+))?",
    }

    for key, pat in patterns.items():
        m = re.search(pat, text, re.IGNORECASE)
        if not m:
            continue
        try:
            v1 = safe_float(m.group(1))
            v2 = safe_float(m.group(2)) if m.group(2) else v1
            if v1 > 0:
                scenarios[key] = (v1 + v2) / 2
        except Exception:
            continue

    return scenarios


# ============================================================
# CALCULS METIERS
# ============================================================


def calculate_sector_sentiment(df_news: pd.DataFrame, days_lookback: int = 30) -> pd.DataFrame:
    """Barometre sectoriel base sur News_History (publishedat, impactscore, winners, losers)."""
    if df_news is None or df_news.empty:
        return pd.DataFrame()
    if "publishedat" not in df_news.columns:
        return pd.DataFrame()

    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days_lookback)
    df_wk = df_news.copy()
    df_wk["publishedat"] = pd.to_datetime(df_wk["publishedat"], errors="coerce", utc=True)
    df_wk = df_wk.dropna(subset=["publishedat"])
    df_recent = df_wk[df_wk["publishedat"] >= cutoff].copy()

    sector_scores = {}

    def parse_sectors(txt: object) -> list[str]:
        if not isinstance(txt, str) or not txt.strip():
            return []
        return [s.strip().title() for s in txt.split(",") if s.strip()]

    for _, row in df_recent.iterrows():
        try:
            impact = abs(safe_float(row.get("impactscore", 0)))
            if impact == 0:
                continue

            winners = parse_sectors(row.get("winners"))
            losers = parse_sectors(row.get("losers"))

            for w in winners:
                sector_scores[w] = sector_scores.get(w, 0) + impact
            for l in losers:
                sector_scores[l] = sector_scores.get(l, 0) - impact
        except Exception:
            continue

    if not sector_scores:
        return pd.DataFrame()

    df_res = pd.DataFrame(list(sector_scores.items()), columns=["Sector", "NetScore"])
    df_res = df_res.sort_values("NetScore", ascending=True)
    df_res["Color"] = df_res["NetScore"].apply(lambda x: "#28a745" if x >= 0 else "#dc3545")
    return df_res


def calculate_symbol_momentum(df_news: pd.DataFrame, days_lookback: int = 30, top_n: int = 10) -> pd.DataFrame:
    """Palmares actions base sur news_raw_Symbol (publishedat, symbol, impactscore, companyname/name)."""
    if df_news is None or df_news.empty:
        return pd.DataFrame()
    if "publishedat" not in df_news.columns or "symbol" not in df_news.columns:
        return pd.DataFrame()

    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days_lookback)
    df_wk = df_news.copy()
    df_wk["publishedat"] = pd.to_datetime(df_wk["publishedat"], errors="coerce", utc=True)
    df_wk = df_wk.dropna(subset=["publishedat"])
    df_recent = df_wk[df_wk["publishedat"] >= cutoff].copy()

    if "impactscore" not in df_recent.columns:
        return pd.DataFrame()

    df_recent["ImpactNum"] = df_recent["impactscore"].apply(safe_float)

    group_cols = ["symbol"]
    name_col = None
    if "companyname" in df_recent.columns:
        name_col = "companyname"
    elif "name" in df_recent.columns:
        name_col = "name"

    if name_col:
        group_cols.append(name_col)

    symbol_scores = df_recent.groupby(group_cols)["ImpactNum"].sum().reset_index()
    symbol_scores.rename(columns={"ImpactNum": "NetScore"}, inplace=True)

    def mk_label(r: pd.Series) -> str:
        s = str(r["symbol"])
        if name_col and pd.notna(r.get(name_col)) and str(r.get(name_col)).strip():
            return f"{s} ({r.get(name_col)})"
        return s

    symbol_scores["Label"] = symbol_scores.apply(mk_label, axis=1)

    positive = symbol_scores[symbol_scores["NetScore"] > 0.1].nlargest(top_n, "NetScore")
    negative = symbol_scores[symbol_scores["NetScore"] < -0.1].nsmallest(top_n, "NetScore")

    final_df = pd.concat([positive, negative]).sort_values("NetScore", ascending=True)
    final_df["Color"] = final_df["NetScore"].apply(lambda x: "#28a745" if x >= 0 else "#dc3545")
    return final_df


def display_wrapped_table(
    df: pd.DataFrame,
    key_suffix: str = "",
    hide_header: bool = False,
    enable_controls: bool = True,
) -> None:
    if df is None or df.empty:
        st.info("Aucune donnee.")
        return

    df_show = df.copy()

    if enable_controls:
        _, c2 = st.columns([1, 3])
        with c2:
            search = st.text_input(f"Rechercher ({key_suffix})", "", key=f"search_{key_suffix}")
        if search:
            mask = df_show.astype(str).apply(lambda x: x.str.contains(search, case=False, na=False)).any(axis=1)
            df_show = df_show[mask]

    html = df_show.to_html(classes="dataframe-wrap", escape=False, index=False, header=not hide_header)
    st.markdown(html, unsafe_allow_html=True)

    if enable_controls:
        st.markdown("<br>", unsafe_allow_html=True)


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
            st.dataframe(df_view, use_container_width=True, hide_index=True)
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
            display_wrapped_table(df_sig, "sig")
        else:
            st.caption("Aucun signal.")

        st.subheader("🛡️ Alertes")
        if df_alt is not None and not df_alt.empty:
            display_wrapped_table(df_alt, "alt")
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
                    display_wrapped_table(df_list, "res_list")
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

            if "workflow_date" in duck_tech.columns:
                duck_tech["workflow_date"] = pd.to_datetime(duck_tech["workflow_date"], errors="coerce", utc=True)

            # For symbols missing tech data from Sheets, fill from DuckDB
            for _, drow in duck_tech.iterrows():
                sk = drow.get("symbol_key", "")
                if sk and sk in monitor_df["symbol_key"].values:
                    idx = monitor_df.index[monitor_df["symbol_key"] == sk]
                    if not idx.empty:
                        i = idx[0]
                        # Fill Last_H1_Date / Last_D1_Date if missing
                        wf_date = drow.get("workflow_date")
                        if pd.isna(monitor_df.at[i, "Last_H1_Date"]) if "Last_H1_Date" in monitor_df.columns else True:
                            if "Last_H1_Date" not in monitor_df.columns:
                                monitor_df["Last_H1_Date"] = pd.NaT
                            monitor_df.at[i, "Last_H1_Date"] = wf_date
                        if pd.isna(monitor_df.at[i, "Last_D1_Date"]) if "Last_D1_Date" in monitor_df.columns else True:
                            if "Last_D1_Date" not in monitor_df.columns:
                                monitor_df["Last_D1_Date"] = pd.NaT
                            monitor_df.at[i, "Last_D1_Date"] = wf_date
                        # Fill signal if missing
                        if "signal" not in monitor_df.columns:
                            monitor_df["signal"] = ""
                        cur_sig = str(monitor_df.at[i, "signal"]).strip()
                        if not cur_sig or cur_sig.lower() in ("", "nan", "none"):
                            d1_act = str(drow.get("d1_action", "")).strip()
                            if d1_act:
                                monitor_df.at[i, "signal"] = d1_act

    # ============================================================
    # Remplissages par défaut
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
    def get_status(row: pd.Series) -> pd.Series:
        ico_n, age_n = check_freshness(row.get("Last_News_Date"), 7)
        ico_ia, age_ia = check_freshness(row.get("Last_IA_Date"), 90)
        ico_cons, age_cons = check_freshness(row.get("Last_Consensus_Date"), 30)
        cons_view = str(row.get("Consensus_View") or "").strip()

        ico_h1, age_h1 = check_freshness(row.get("Last_H1_Date"), 2)

        last_d1 = row.get("Last_D1_Date")
        last_tech = row.get("Last_Tech_Date")
        d1_ref = last_d1 if pd.notna(last_d1) else last_tech

        ico_d1, age_d1 = check_freshness(d1_ref, 3)

        tech_stat = f"H1:{ico_h1}{age_h1}j | D1:{ico_d1}{age_d1}j"

        ia_stat = f"IA:{ico_ia}{age_ia}j"
        cons_stat = f"Cons:{ico_cons}{age_cons}j"
        if cons_view:
            cons_stat = f"{cons_stat} ({cons_view})"

        funda_stat = f"{ia_stat} | {cons_stat}"

        return pd.Series(
            [
                age_n,
                age_ia,
                age_cons,
                age_h1,
                age_d1,
                f"{ico_n}{age_n}j",
                funda_stat,
                tech_stat,
            ]
        )

    out = monitor_df.apply(get_status, axis=1)
    monitor_df[["Age_N", "Age_IA", "Age_Cons", "Age_H1", "Age_D1", "News_Stat", "Funda_Stat", "Tech_Stat"]] = out

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

    def calc_attract(row):
        score = 50
        # Potentiel News (Impact x3) -> +/- 30 pts max
        score += safe_float(row.get("News_Score", 0)) * 3

        # Bonus/Malus Funda (Confiance extrême)
        conf = safe_float(row.get("Funda_Conf", 0))
        if conf > 80: score += 10
        elif conf < 30: score -= 10

        # Signal Technique (Le juge de paix à court terme)
        sig = str(row.get("signal", "")).upper()
        if "BUY" in sig: score += 15
        elif "SELL" in sig: score -= 15

        return min(100, max(0, score))

    def calc_robust(row):
        # Base : La qualité fondamentale (0-100)
        # CORRECTION : On utilise le score plein (facteur 1.0) pour atteindre 100
        score = safe_float(row.get("Funda_Conf", 50))

        # Malus d'Obsolescence (L'incertitude tue la robustesse)
        # 1. Funda > 3 mois
        if row.get("Age_F", 999) > 90:
            score -= 20
        # 2. Tech > 1 semaine (Tendance D1 perdue)
        if row.get("Age_T", 999) > 7:
            score -= 10

        return min(100, max(0, score))

    mat["Attractivité"] = mat.apply(calc_attract, axis=1)
    mat["Robustesse"] = mat.apply(calc_robust, axis=1)

    # La taille de la bulle dépend de la confiance fondamentale
    mat["Taille"] = mat["Funda_Conf"].apply(lambda x: max(15, safe_float(x)))

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
    st.dataframe(monitor_df[cols_sync], use_container_width=True, hide_index=True)


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
        ai_calls = int(df_signals.get("call_ai", pd.Series(dtype=object)).apply(lambda x: str(x).strip().upper() in ("TRUE", "1", "OUI", "YES")).sum()) if "call_ai" in df_signals.columns else 0
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
        df_display["Close"] = df_ov["last_close"].apply(lambda x: f"{safe_float(x):.2f}" if safe_float(x) > 0 else "—")
        df_display["H1 Action"] = df_ov["h1_action"].apply(_action_badge)
        df_display["H1 Score"] = df_ov["h1_score"].apply(lambda x: f"{safe_float(x):.0f}" if safe_float(x) != 0 else "—")
        df_display["H1 RSI"] = df_ov["h1_rsi14"].apply(lambda x: f"{safe_float(x):.1f}" if safe_float(x) > 0 else "—")
        df_display["D1 Action"] = df_ov["d1_action"].apply(_action_badge)
        df_display["D1 Score"] = df_ov["d1_score"].apply(lambda x: f"{safe_float(x):.0f}" if safe_float(x) != 0 else "—")
        df_display["D1 RSI"] = df_ov["d1_rsi14"].apply(lambda x: f"{safe_float(x):.1f}" if safe_float(x) > 0 else "—")
        df_display["Filtre"] = df_ov["filter_reason"].fillna("—")
        df_display["IA"] = df_ov["ai_decision"].apply(_ai_badge)
        df_display["Qualité IA"] = df_ov["ai_quality"].apply(lambda x: f"{safe_float(x):.0f}/10" if safe_float(x) > 0 else "—")
        df_display["Date"] = df_ov["workflow_date"].apply(lambda x: str(x)[:10] if pd.notna(x) and str(x).strip() not in ("", "nan", "NaT") else "—")

        # Apply RSI coloring via Styler on a numeric version for conditional formatting
        # Since we use HTML badges, we display via HTML table
        display_wrapped_table(df_display, "v2_overview")

    # ================================================================
    # TAB 2: VUE DETAILLEE
    # ================================================================
    with tab_detail:
        symbol_list = sorted(df_signals["symbol"].dropna().unique().tolist())

        if not symbol_list:
            st.warning("Aucun symbole disponible.")
        else:
            selected_symbol = st.selectbox(
                "Sélectionner un symbole :",
                symbol_list,
                key="v2_symbol_select",
            )

            if selected_symbol:
                row_mask = df_signals["symbol"] == selected_symbol
                if row_mask.sum() == 0:
                    st.warning(f"Aucune donnée pour {selected_symbol}")
                else:
                    row = df_signals[row_mask].iloc[0]

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

                    # ---- Row 2: Indicators H1 | D1 ----
                    col_h1, col_d1 = st.columns(2)

                    for tf_col, tf_label in [(col_h1, "H1"), (col_d1, "D1")]:
                        prefix = tf_label.lower() + "_"
                        with tf_col:
                            st.markdown(f"#### Indicateurs {tf_label}")

                            # RSI Gauge
                            rsi_val = safe_float(row.get(f"{prefix}rsi14", 0))
                            if rsi_val > 0:
                                fig_rsi = _make_rsi_gauge(rsi_val, f"RSI 14 ({tf_label})")
                                st.plotly_chart(fig_rsi, use_container_width=True, key=f"rsi_{tf_label}_{selected_symbol}")

                            # Key metrics
                            m1, m2 = st.columns(2)
                            with m1:
                                macd_hist = safe_float(row.get(f"{prefix}macd_hist", 0))
                                st.metric("MACD Hist", f"{macd_hist:.4f}" if macd_hist != 0 else "—")

                                stoch_k = safe_float(row.get(f"{prefix}stoch_k", 0))
                                st.metric("Stochastic K", f"{stoch_k:.1f}" if stoch_k > 0 else "—")

                                adx_val = safe_float(row.get(f"{prefix}adx", 0))
                                st.metric("ADX", f"{adx_val:.1f}" if adx_val > 0 else "—")

                                bb_width = safe_float(row.get(f"{prefix}bb_width", 0))
                                st.metric("BB Width", f"{bb_width:.4f}" if bb_width > 0 else "—")

                            with m2:
                                vol = safe_float(row.get(f"{prefix}volatility", 0))
                                st.metric("Volatilité", f"{vol:.4f}" if vol > 0 else "—")

                                stoch_d = safe_float(row.get(f"{prefix}stoch_d", 0))
                                st.metric("Stochastic D", f"{stoch_d:.1f}" if stoch_d > 0 else "—")

                                atr_pct = safe_float(row.get(f"{prefix}atr_pct", 0))
                                st.metric("ATR %", f"{atr_pct:.2f}%" if atr_pct > 0 else "—")

                                obv_slope = safe_float(row.get(f"{prefix}obv_slope", 0))
                                st.metric("OBV Slope", f"{obv_slope:.2f}" if obv_slope != 0 else "—")

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

                    # ---- Row 4: Support / Resistance ----
                    st.markdown("#### Support / Résistance")

                    sr_col1, sr_col2 = st.columns(2)

                    for sr_col, tf_label in [(sr_col1, "H1"), (sr_col2, "D1")]:
                        prefix = tf_label.lower() + "_"
                        with sr_col:
                            support = safe_float(row.get(f"{prefix}support", 0))
                            resistance = safe_float(row.get(f"{prefix}resistance", 0))
                            price = safe_float(row.get(f"{prefix}last_close", row.get("last_close", 0)))
                            dist_sup = safe_float(row.get(f"{prefix}dist_sup_pct", 0))
                            dist_res = safe_float(row.get(f"{prefix}dist_res_pct", 0))

                            if support > 0 or resistance > 0:
                                fig_sr = go.Figure()

                                # Determine range
                                all_vals = [v for v in [support, price, resistance] if v > 0]
                                if not all_vals:
                                    st.caption(f"Pas de niveaux S/R pour {tf_label}")
                                    continue

                                y_min = min(all_vals) * 0.98
                                y_max = max(all_vals) * 1.02

                                # Support line
                                if support > 0:
                                    fig_sr.add_trace(go.Scatter(
                                        x=[0, 1], y=[support, support],
                                        mode="lines+text",
                                        name="Support",
                                        line=dict(color="#28a745", width=2, dash="dash"),
                                        text=[f"Support: {support:.2f} ({dist_sup:+.1f}%)", ""],
                                        textposition="top left",
                                        textfont=dict(color="#28a745"),
                                    ))

                                # Resistance line
                                if resistance > 0:
                                    fig_sr.add_trace(go.Scatter(
                                        x=[0, 1], y=[resistance, resistance],
                                        mode="lines+text",
                                        name="Résistance",
                                        line=dict(color="#dc3545", width=2, dash="dash"),
                                        text=[f"Résistance: {resistance:.2f} ({dist_res:+.1f}%)", ""],
                                        textposition="bottom left",
                                        textfont=dict(color="#dc3545"),
                                    ))

                                # Current price marker
                                if price > 0:
                                    fig_sr.add_trace(go.Scatter(
                                        x=[0.5], y=[price],
                                        mode="markers+text",
                                        name="Prix actuel",
                                        marker=dict(color="#ffc107", size=14, symbol="diamond"),
                                        text=[f"Prix: {price:.2f}"],
                                        textposition="top center",
                                        textfont=dict(color="#ffc107"),
                                    ))

                                fig_sr.update_layout(
                                    title=f"S/R {tf_label}",
                                    height=250,
                                    showlegend=False,
                                    xaxis=dict(showticklabels=False, showgrid=False, range=[-0.1, 1.1]),
                                    yaxis=dict(range=[y_min, y_max]),
                                    margin=dict(t=30, b=10, l=50, r=20),
                                    paper_bgcolor="rgba(0,0,0,0)",
                                    plot_bgcolor="rgba(0,0,0,0)",
                                )
                                st.plotly_chart(fig_sr, use_container_width=True, key=f"sr_{tf_label}_{selected_symbol}")
                            else:
                                st.caption(f"Pas de niveaux S/R pour {tf_label}")

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
                df_runs_display["Statut"] = df_runs_display["status"].apply(_status_badge)
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

            display_wrapped_table(df_runs_show, "v2_runs")
