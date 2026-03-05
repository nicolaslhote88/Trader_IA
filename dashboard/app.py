import ast
import json
import hashlib
import html
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
import streamlit.components.v1 as components
from oauth2client.service_account import ServiceAccountCredentials

# ============================================================
# CONFIGURATION
# ============================================================

st.set_page_config(page_title="AI Trading Executor", layout="wide", page_icon="AI")

SHEET_ID = os.getenv("SHEET_ID")
CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/secrets/service_account.json")

def _duckdb_default_path(filename: str) -> str:
    base_dir = str(os.getenv("DUCKDB_DIR", "/files/duckdb") or "/files/duckdb").strip()
    base_dir = base_dir.rstrip("/\\")
    return f"{base_dir}/{filename}"


def _resolve_duckdb_path(
    primary_env: str,
    default_filename: str,
    *legacy_envs: str,
    fallback_filenames: tuple[str, ...] = (),
) -> str:
    for env_name in (primary_env, *legacy_envs):
        raw = str(os.getenv(env_name, "") or "").strip()
        if raw:
            return raw

    candidates = [_duckdb_default_path(default_filename), *[_duckdb_default_path(name) for name in fallback_filenames]]
    for path in candidates:
        if os.path.exists(path):
            return path
    return candidates[0]


def _dedupe_nonempty_paths(paths: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in paths:
        p = str(raw or "").strip()
        if not p or p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


def _latest_existing_path(paths: list[str]) -> str | None:
    candidates: list[tuple[float, int, str]] = []
    for p in _dedupe_nonempty_paths(paths):
        if not os.path.exists(p):
            continue
        try:
            stat = os.stat(p)
            candidates.append((float(stat.st_mtime), int(stat.st_size), p))
        except OSError:
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda row: (row[0], row[1]), reverse=True)
    return candidates[0][2]


def _resolve_ag1_variant_duckdb_path(primary_env: str, default_filename: str) -> str:
    # Strict variant routing: never fallback to shared ag1_v3.duckdb.
    # Each variant must resolve to its own dedicated DB file.
    explicit_variant = str(os.getenv(primary_env, "") or "").strip()
    candidates = [
        explicit_variant,
        _duckdb_default_path(default_filename),
    ]
    freshest = _latest_existing_path(candidates)
    if freshest:
        return freshest
    for p in candidates:
        if str(p or "").strip():
            return str(p).strip()
    return _duckdb_default_path(default_filename)


AG2_DUCKDB_PATH = _resolve_duckdb_path("AG2_DUCKDB_PATH", "ag2_v3.duckdb", "DUCKDB_PATH")
DUCKDB_PATH = AG2_DUCKDB_PATH  # Backward compatibility across existing code paths.
AG1_CHATGPT52_DUCKDB_PATH = _resolve_ag1_variant_duckdb_path("AG1_CHATGPT52_DUCKDB_PATH", "ag1_v3_chatgpt52.duckdb")
AG1_GROK41_REASONING_DUCKDB_PATH = _resolve_ag1_variant_duckdb_path("AG1_GROK41_REASONING_DUCKDB_PATH", "ag1_v3_grok41_reasoning.duckdb")
AG1_GEMINI30_PRO_DUCKDB_PATH = _resolve_ag1_variant_duckdb_path("AG1_GEMINI30_PRO_DUCKDB_PATH", "ag1_v3_gemini30_pro.duckdb")


def _resolve_ag1_legacy_duckdb_path() -> str:
    # Legacy widgets should never default to the shared ag1_v3.duckdb anymore.
    explicit = str(os.getenv("AG1_DUCKDB_PATH", "") or "").strip()
    banned_shared_name = "ag1_v3.duckdb"
    variant_candidates = [
        AG1_CHATGPT52_DUCKDB_PATH,
        AG1_GROK41_REASONING_DUCKDB_PATH,
        AG1_GEMINI30_PRO_DUCKDB_PATH,
    ]

    if explicit and os.path.basename(explicit).lower() != banned_shared_name:
        return explicit

    freshest_variant = _latest_existing_path(variant_candidates)
    if freshest_variant:
        return freshest_variant

    for p in variant_candidates:
        if str(p or "").strip():
            return str(p).strip()

    # Last resort if no variant DB path is available.
    return _duckdb_default_path("ag1_v3_chatgpt52.duckdb")


AG1_DUCKDB_PATH = _resolve_ag1_legacy_duckdb_path()
AG3_DUCKDB_PATH = _resolve_duckdb_path("AG3_DUCKDB_PATH", "ag3_v2.duckdb", fallback_filenames=("ag3_v3.duckdb",))
AG4_DUCKDB_PATH = _resolve_duckdb_path("AG4_DUCKDB_PATH", "ag4_v3.duckdb")
AG4_SPE_DUCKDB_PATH = _resolve_duckdb_path(
    "AG4_SPE_DUCKDB_PATH",
    "ag4_spe_v2.duckdb",
    fallback_filenames=("ag4_spe_v3.duckdb",),
)
YF_ENRICH_DUCKDB_PATH = _resolve_duckdb_path("YF_ENRICH_DUCKDB_PATH", "yf_enrichment_v1.duckdb")
YFINANCE_API_URL = os.getenv("YFINANCE_API_URL", "http://yfinance-api:8080")

AG1_MULTI_PORTFOLIO_CONFIG = {
    "chatgpt52": {
        "label": "ChatGPT 5.2",
        "short_label": "GPT",
        "db_path": AG1_CHATGPT52_DUCKDB_PATH,
        "accent": "#10b981",
    },
    "grok41_reasoning": {
        "label": "Grok 4.1 Reasoning",
        "short_label": "Grok",
        "db_path": AG1_GROK41_REASONING_DUCKDB_PATH,
        "accent": "#f59e0b",
    },
    "gemini30_pro": {
        "label": "Gemini 3.0 Pro",
        "short_label": "Gemini",
        "db_path": AG1_GEMINI30_PRO_DUCKDB_PATH,
        "accent": "#60a5fa",
    },
}

DEFAULT_BENCHMARKS = {
    "CAC 40": {"ticker": "^FCHI"},
    "S&P 500": {"ticker": "^GSPC"},
    "EURO STOXX 50": {"ticker": "^STOXX50E"},
}


def _load_benchmarks_config_from_env() -> dict[str, dict[str, str]]:
    raw = str(os.getenv("BENCHMARK_TICKERS_JSON", "") or "").strip()
    if not raw:
        return {k: {"ticker": str(v.get("ticker", "")).strip().upper()} for k, v in DEFAULT_BENCHMARKS.items()}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {k: {"ticker": str(v.get("ticker", "")).strip().upper()} for k, v in DEFAULT_BENCHMARKS.items()}

    if not isinstance(parsed, dict):
        return {k: {"ticker": str(v.get("ticker", "")).strip().upper()} for k, v in DEFAULT_BENCHMARKS.items()}

    out: dict[str, dict[str, str]] = {}
    for label, cfg in parsed.items():
        lbl = str(label or "").strip()
        if not lbl:
            continue
        ticker = ""
        if isinstance(cfg, dict):
            ticker = str(cfg.get("ticker", "") or "").strip().upper()
        else:
            ticker = str(cfg or "").strip().upper()
        if ticker:
            out[lbl] = {"ticker": ticker}

    if out:
        return out
    return {k: {"ticker": str(v.get("ticker", "")).strip().upper()} for k, v in DEFAULT_BENCHMARKS.items()}


BENCHMARKS_CONFIG = _load_benchmarks_config_from_env()

GRADE_COLOR_MAP = {"A": "#0072B2", "B": "#E69F00", "C": "#CC79A7"}
GRADE_CONTOUR_WIDTH_MAP = {"A": 3.2, "B": 2.4, "C": 1.6}
EV_SIGN_BORDER_MAP = {"EV_POS": "#2ECC71", "EV_NEG": "#FF4D4F"}
DECISION_SYMBOL_MAP = {
    "Entrer / Renforcer|EV_POS": "triangle-up",
    "Entrer / Renforcer|EV_NEG": "triangle-up",
    "Surveiller|EV_POS": "circle",
    "Surveiller|EV_NEG": "circle",
    "Reduire / Sortir|EV_POS": "x",
    "Reduire / Sortir|EV_NEG": "x",
}

METRICS_META: dict[str, dict[str, str]] = {
    "total_values": {
        "label": "Valeurs suivies",
        "definition_short": "Nombre total de symboles analyses dans la matrice.",
        "definition_long": "Compteur de toutes les valeurs presentes apres fusion AG2 (tech), AG3 (fonda), AG4 (news) et enrichissement YF.",
        "formula": "COUNT(symbol)",
        "unit": "nb",
        "source": "Derived (AG2+AG3+AG4+YF)",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Definit la taille de l'univers et le contexte des seuils dynamiques.",
        "display_format": "entier",
    },
    "enter_count": {
        "label": "Entrer / Renforcer",
        "definition_short": "Nombre de valeurs avec signal d'action positive.",
        "definition_long": "Compte les valeurs dont la decision finale matrice est 'Entrer / Renforcer' apres scoring + gates.",
        "formula": "COUNT(matrix_action == 'Entrer / Renforcer')",
        "unit": "nb",
        "source": "Derived",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Indique combien d'opportunites immediates sont actionnables.",
        "display_format": "entier",
    },
    "watch_count": {
        "label": "Surveiller",
        "definition_short": "Nombre de valeurs a conserver en watchlist active.",
        "definition_long": "Valeurs qui ne passent pas tous les filtres d'entree mais ne justifient pas une sortie immediate.",
        "formula": "COUNT(matrix_action == 'Surveiller')",
        "unit": "nb",
        "source": "Derived",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Represente les setups conditionnels en attente de confirmation.",
        "display_format": "entier",
    },
    "exit_count": {
        "label": "Reduire / Sortir",
        "definition_short": "Nombre de valeurs a derisquer en priorite.",
        "definition_long": "Valeurs avec EV defavorable, risque relatif trop eleve, ou combinaison risque/reward deteriorante.",
        "formula": "COUNT(matrix_action == 'Reduire / Sortir')",
        "unit": "nb",
        "source": "Derived",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Signale la pression de reduction du risque global.",
        "display_format": "entier",
    },
    "avg_ev_r": {
        "label": "EV(R) moyen",
        "definition_short": "Esperance moyenne en multiple de risque.",
        "definition_long": "Moyenne de EV(R) = p_win x R_utilise - (1 - p_win), sur tout l'univers.",
        "formula": "AVG(ev_r)",
        "unit": "R",
        "source": "Derived (AG2+AG3+AG4)",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Donne le biais moyen de l'univers (favorable si positif).",
        "display_format": "float_2",
    },
    "grade_a_count": {
        "label": "Setups grade A",
        "definition_short": "Valeurs dans la meilleure classe de probabilite.",
        "definition_long": "Grade calcule par quantiles sur prob_score_for_grade (ajuste par data quality et gates).",
        "formula": "COUNT(setup_grade == 'A')",
        "unit": "nb",
        "source": "Derived (AG2+AG3+AG4)",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Quantifie le stock de setups haute qualite relative.",
        "display_format": "entier",
    },
    "rr_outliers": {
        "label": "RR outliers",
        "definition_short": "Valeurs avec ratio R suspect (souvent stop trop serre).",
        "definition_long": "Outlier si R brut > 6 ou si risque brut < plancher ATR. Ces cas sont gates pour eviter les faux signaux.",
        "formula": "COUNT(rr_outlier == True)",
        "unit": "nb",
        "source": "Derived (AG2+AG3+YF)",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Reduit le risque de surestimer des trades artificiellement attractifs.",
        "display_format": "entier",
    },
    "low_data_quality": {
        "label": "Data quality <60",
        "definition_short": "Valeurs avec fiabilite de donnees insuffisante.",
        "definition_long": "Score de qualite data agregant couverture quote/options/earnings, fraicheur et completude des features.",
        "formula": "COUNT(data_quality_score < 60)",
        "unit": "nb",
        "source": "Derived + YF",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Declenche un gate et limite les entrees non robustes.",
        "display_format": "entier",
    },
    "options_missing": {
        "label": "Options indispo",
        "definition_short": "Valeurs sans chaines options exploitables.",
        "definition_long": "Nombre de symboles avec options_ok=false (cas frequent hors US).",
        "formula": "COUNT(options_ok == False)",
        "unit": "nb",
        "source": "YF /options + derived",
        "update_frequency": "Workflow daily enrich + rafraichissement dashboard",
        "impact_on_decision": "Baisse la confiance sur le risque options/IV, sans bloquer systematiquement.",
        "display_format": "entier",
    },
    "invalid_options_state_count": {
        "label": "Invalid options state",
        "definition_short": "Valeurs avec etat options invalide (erreur systeme).",
        "definition_long": "Detection de patterns d'erreur critiques dans les etats options (fichiers temporaires, etats incoherents).",
        "formula": "COUNT(invalid_options_state == True)",
        "unit": "nb",
        "source": "YF enrich workflow state",
        "update_frequency": "Workflow daily enrich + rafraichissement dashboard",
        "impact_on_decision": "Gate bloquant: pas de nouvel 'Entrer' si etat options invalide.",
        "display_format": "entier",
    },
    "risk_score_u": {
        "label": "Risk score (0-100)",
        "definition_short": "Score de risque relatif: plus haut = plus risque.",
        "definition_long": "Agrege risque fondamental, volatilite, liquidite, event risk, news risk, concentration, options risk.",
        "formula": "Weighted sum des composantes risk puis normalisation 0-100",
        "unit": "score",
        "source": "Derived (AG2+AG3+AG4+YF+Portfolio)",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Position X de la matrice et condition principale de derisk.",
        "display_format": "entier",
    },
    "reward_score_u": {
        "label": "Reward score (0-100)",
        "definition_short": "Score de potentiel de gain relatif: plus haut = plus attractif.",
        "definition_long": "Agrege asymetrie R, upside fondamental, espace technique, catalyseurs, trend bonus.",
        "formula": "Weighted sum des composantes reward puis normalisation 0-100",
        "unit": "score",
        "source": "Derived (AG2+AG3+AG4)",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Position Y de la matrice et composante cle de priorisation.",
        "display_format": "entier",
    },
    "r_multiple": {
        "label": "R utilise",
        "definition_short": "Ratio reward/risk utilise pour scoring (cap).",
        "definition_long": "R utilise = reward_pct / risk_pct_effective, puis cap a 6 pour eviter les outliers extremes.",
        "formula": "min(6, reward_pct / risk_pct_effective)",
        "unit": "R",
        "source": "Derived (AG2+AG3)",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Entre dans EV(R), sizing, et filtre des faux setups.",
        "display_format": "float_2",
    },
    "r_multiple_raw": {
        "label": "R brut",
        "definition_short": "Ratio reward/risk sans cap.",
        "definition_long": "Permet de detecter les ratios artificiellement gonfles par un stop trop proche.",
        "formula": "reward_pct / risk_pct_raw",
        "unit": "R",
        "source": "Derived (AG2+AG3)",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Si trop eleve, classe en RR outlier et bloque l'entree.",
        "display_format": "float_2",
    },
    "ev_r": {
        "label": "EV(R)",
        "definition_short": "Esperance de gain en multiple R.",
        "definition_long": "EV(R) combine proba de gain et asymetrie R. Positif = avantage statistique; negatif = desavantage.",
        "formula": "p_win x R_utilise - (1 - p_win)",
        "unit": "R",
        "source": "Derived",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Moteur principal de priorisation et sizing.",
        "display_format": "signed_float_2",
    },
    "p_win": {
        "label": "Prob. win",
        "definition_short": "Probabilite estimee de scenario gagnant.",
        "definition_long": "Issue du score probabiliste AG2+AG3+AG4 avec ajustements regime/alignment.",
        "formula": "p_win = clamp(prob_score / 100, 0.05, 0.95)",
        "unit": "%",
        "source": "Derived (AG2+AG3+AG4)",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Transforme le ratio R en esperance reelle via EV(R).",
        "display_format": "pct_1",
    },
    "data_quality_score": {
        "label": "Data quality",
        "definition_short": "Indice de confiance des donnees utilisees.",
        "definition_long": "Combine couverture quote/options/earnings, fraicheur YF, completude des features et validite des etats.",
        "formula": "Weighted blend qualite sources, puis clamp 0-100",
        "unit": "score",
        "source": "Derived + YF",
        "update_frequency": "Workflow daily enrich + rafraichissement dashboard",
        "impact_on_decision": "Gate de fiabilite: faible qualite degrade grade et bloque certaines entrees.",
        "display_format": "entier",
    },
    "risk_threshold_dyn": {
        "label": "Seuil risque p60",
        "definition_short": "Frontiere dynamique risque de l'univers du jour.",
        "definition_long": "P60 des risk scores: au-dessus du seuil, le risque est eleve relativement aux autres valeurs.",
        "formula": "quantile_60(risk_score_u)",
        "unit": "score",
        "source": "Derived",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Determine les quadrants gauche/droite de la matrice.",
        "display_format": "entier",
    },
    "reward_threshold_dyn": {
        "label": "Seuil reward p60",
        "definition_short": "Frontiere dynamique reward de l'univers du jour.",
        "definition_long": "P60 des reward scores: au-dessus du seuil, les valeurs appartiennent aux 40% les plus attractives en reward relatif.",
        "formula": "quantile_60(reward_score_u)",
        "unit": "score",
        "source": "Derived",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Determine les quadrants haut/bas de la matrice.",
        "display_format": "entier",
    },
    "grade_a_threshold": {
        "label": "Seuil grade A",
        "definition_short": "Score mini pour entrer en grade A.",
        "definition_long": "Quantile 90% du score de probabilite ajuste par qualite data et gates.",
        "formula": "quantile_90(prob_score_for_grade)",
        "unit": "score",
        "source": "Derived",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Identifie les setups top decile de l'univers du jour.",
        "display_format": "float_1",
    },
    "grade_b_threshold": {
        "label": "Seuil grade B",
        "definition_short": "Score mini pour entrer en grade B.",
        "definition_long": "Quantile median (50%) du score de probabilite ajuste.",
        "formula": "quantile_50(prob_score_for_grade)",
        "unit": "score",
        "source": "Derived",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Separation entre setups moyens (B) et faibles (C).",
        "display_format": "float_1",
    },
    "matrix_action": {
        "label": "Decision finale",
        "definition_short": "Action proposee par la matrice.",
        "definition_long": "Decision issue du pipeline complet: scoring, quadrants, gates et regles EV/risk/reward.",
        "formula": "Rule engine (enter/watch/exit)",
        "unit": "classe",
        "source": "Derived",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Traduction actionnable immediate pour execution ou surveillance.",
        "display_format": "texte",
    },
    "setup_grade": {
        "label": "Grade setup",
        "definition_short": "Classe de probabilite/qualite relative du setup.",
        "definition_long": "A/B/C via quantiles dynamiques, puis ajustements selon gates et data quality.",
        "formula": "A si score>=thr_A, B si score>=thr_B sinon C, avec downgrades gates",
        "unit": "classe",
        "source": "Derived",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Filtre qualitatif principal des entrees.",
        "display_format": "texte",
    },
    "gate_summary": {
        "label": "Gates actifs",
        "definition_short": "Liste des verrous/alertes actifs sur une valeur.",
        "definition_long": "Concatene les controles critiques (earnings, qualite data, liquidite, options state, RR outlier).",
        "formula": "Join des gates actifs par '|'",
        "unit": "texte",
        "source": "Derived",
        "update_frequency": "A chaque rafraichissement dashboard",
        "impact_on_decision": "Un gate critique peut forcer 'Surveiller' ou interdire l'entree.",
        "display_format": "texte",
    },
    "spreadPct": {
        "label": "Spread %",
        "definition_short": "Spread bid/ask relatif au prix mid.",
        "definition_long": "Mesure la friction d'execution immediate. Plus haut = liquidite plus fragile.",
        "formula": "(ask - bid) / mid x 100",
        "unit": "%",
        "source": "YF quote",
        "update_frequency": "Workflow daily enrich",
        "impact_on_decision": "Alimente le risque liquidite et les gates d'execution.",
        "display_format": "pct_2",
    },
    "iv_atm": {
        "label": "IV ATM",
        "definition_short": "Volatilite implicite at-the-money.",
        "definition_long": "Proxy de risque evenementiel issu de la chaine options lorsque disponible.",
        "formula": "Extraction /options (ATM interpolation simple)",
        "unit": "vol",
        "source": "YF options",
        "update_frequency": "Workflow daily enrich",
        "impact_on_decision": "Influence le score de risque options et la confiance data.",
        "display_format": "float_3",
    },
    "days_to_next_earnings": {
        "label": "Jours avant earnings",
        "definition_short": "Delai estime avant prochaine publication de resultats.",
        "definition_long": "Si <=7 jours, gate bloquant pour limiter le gap risk evenementiel.",
        "formula": "next_earnings_date - now (jours)",
        "unit": "jours",
        "source": "YF calendar + derived",
        "update_frequency": "Workflow daily enrich",
        "impact_on_decision": "Active le gate earnings et reduit/annule les entrees.",
        "display_format": "float_1",
    },
    "days_since_last_earnings": {
        "label": "Jours depuis earnings",
        "definition_short": "Temps ecoule depuis la derniere publication connue.",
        "definition_long": "Permet de distinguer post-earnings (souvent moins de risque evenementiel) vs pre-earnings.",
        "formula": "abs(days_to_earnings) quand date passee",
        "unit": "jours",
        "source": "YF calendar + derived",
        "update_frequency": "Workflow daily enrich",
        "impact_on_decision": "Ajuste event risk et interpretation du timing.",
        "display_format": "float_1",
    },
}


TEXTS_FR: dict[str, object] = {
    "value_header_title": "Vue consolidée AG2 + AG3 + AG4 — Détail par valeur",
    "value_header_subtitle": (
        "Cette page explique la décision et le plan de trade, avec preuves "
        "(tech/fonda/news/risques) et qualité des données."
    ),
    "value_search_tip": "Astuce : tape 2–3 lettres puis sélectionne dans la liste.",
    "beginner_panel_title": "📘 Aide — Définitions des indicateurs (débutant)",
    "beginner_panel_md": """
- Decision
  “Entrer/Renforcer : setup favorable et gates ouverts (ou setup conditionnel si warnings).”
  “Surveiller : pas assez favorable ou attente de confirmation (reward/EV faibles, ou gates incertains).”
  “Réduire/Sortir : EV(R) défavorable, risque trop élevé, ou gate bloquant.”

- Risque (0–100)
  “Score relatif : 0 = faible risque vs l’univers du jour, 100 = risque élevé vs l’univers.”
  “Construit à partir de plusieurs composantes (volatilité, évènements, liquidité, news, fondamentaux, concentration…).”

- Reward (0–100)
  “Score relatif d’attractivité : 0 = upside faible vs l’univers, 100 = upside fort vs l’univers.”
  “Combinaison de tendance/structure, asymétrie du trade, upside fondamental, catalyseurs/news.”

- R (ratio)
  “R = (TP − Entry) / (Entry − Stop).”
  “R > 2 : ratio généralement intéressant ; R < 1 : ratio faible.”

- Prob. win (%)
  “Probabilité estimée (modèle) que le scénario gagne (TP atteint avant Stop).”
  “C’est une estimation, sensible à la qualité et fraîcheur des données.”

- EV(R)
  “Valeur espérée du trade en unités de risque.”
  “EV(R) > 0 : favorable ; ≈ 0 : neutre ; < 0 : défavorable.”
  “Plus |EV(R)| est grand, plus la conviction est forte.”

- Grade (A/B/C)
  “Classement relatif sur l’univers du jour.”
  “A = top X% des setups (après gates), B = intermédiaire supérieur, C = le reste.”

- Data quality (0–100)
  “Fiabilité/complétude des données utilisées (tech/fonda/news/options/liquidité…).”
  “Si faible, la décision est dégradée et certains trades peuvent être bloqués.”

- Sizing reco (%)
  “Taille recommandée du trade (en % d’une taille cible/budget position).”
  “Diminue si risque élevé, data quality faible, liquidité incertaine, concentration portefeuille forte.”
""",
    "kpi_tooltips_exact": {
        "Decision": "Décision synthétique issue des scores Risk/Reward/EV(R) + des gates (bloquants/avertissements).",
        "Grade": "Grade relatif (A/B/C) calculé sur l’univers du jour. A = meilleur décile (si paramétré).",
        "Risque": "Risque relatif (0–100). 0 = faible risque vs l’univers, 100 = risque élevé. Voir ‘Décomposition risque’.",
        "Reward": "Reward relatif (0–100). 0 = faible attractivité vs l’univers, 100 = forte attractivité. Voir ‘Décomposition reward’.",
        "R": "R du plan : (TP−Entry)/(Entry−Stop). Mesure l’asymétrie du trade.",
        "EV(R)": "Valeur espérée (en R). Positif = favorable, négatif = défavorable. Plus |EV(R)| est grand, plus la conviction est forte.",
        "Prob. win": "Probabilité estimée de succès du setup (modèle). Dépend de la qualité des données.",
        "Data quality": "Score (0–100) de complétude/fiabilité des inputs. Si < seuil, gates WARN/BLOCK.",
        "Sizing reco": "Taille recommandée (%). Ajustée par EV(R), risque, liquidité, concentration, data quality.",
    },
    "na_tooltip": "Donnée indisponible (source manquante).",
}


def safe_text(x: object, default: str = "N/A") -> str:
    if x is None:
        return default
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none", "nat"):
        return default
    return s


def safe_num(x: object, ndigits: int = 2, default: str = "N/A") -> str:
    try:
        v = float(pd.to_numeric(pd.Series([x]), errors="coerce").iloc[0])
    except Exception:
        return default
    if pd.isna(v):
        return default
    return f"{v:.{ndigits}f}"


def safe_pct(x: object, ndigits: int = 1, default: str = "N/A") -> str:
    try:
        v = float(pd.to_numeric(pd.Series([x]), errors="coerce").iloc[0])
    except Exception:
        return default
    if pd.isna(v):
        return default
    return f"{(v * 100.0):.{ndigits}f}%"


def safe_score(x: object, default: str = "N/A") -> str:
    try:
        v = float(pd.to_numeric(pd.Series([x]), errors="coerce").iloc[0])
    except Exception:
        return default
    if pd.isna(v):
        return default
    v = max(0.0, min(100.0, v))
    return f"{v:.0f}/100"


def safe_dt(x: object, default: str = "N/A") -> str:
    ts = pd.to_datetime(x, errors="coerce", utc=True)
    if pd.isna(ts):
        return default
    try:
        return ts.tz_convert("Europe/Paris").strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(ts)


def safe_get(row: object, keys: list[str], default: object = None) -> object:
    if row is None:
        return default
    for key in keys:
        try:
            if isinstance(row, dict):
                if key in row:
                    val = row.get(key)
                else:
                    continue
            else:
                if key not in row:
                    continue
                val = row.get(key, default)
        except Exception:
            continue

        if val is None:
            continue
        if isinstance(val, str) and val.strip() == "":
            continue
        if isinstance(val, float) and pd.isna(val):
            continue
        if pd.isna(val):
            continue
        return val
    return default


def _is_truthy(v: object) -> bool:
    s = safe_text(v, default="").lower()
    return s in ("1", "true", "yes", "y", "ok")


def _value_is_na(v: object) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        vv = v.strip().lower()
        return vv in ("", "n/a", "na", "nan", "none", "nat")
    try:
        return bool(pd.isna(v))
    except Exception:
        return False


def _render_inline_info(label: str, text: str, uid: str = "") -> None:
    seed = f"{label}|{uid}|{text}"
    hidden_suffix = "\u200b" * (1 + (sum(ord(ch) for ch in seed) % 5))
    info_label = f"\u24d8 {label}{hidden_suffix}"
    if hasattr(st, "popover"):
        with st.popover(info_label):
            st.write(text)
    else:
        with st.expander(info_label):
            st.write(text)


def _render_copy_buttons(plan_text: str, plan_json_text: str, key_suffix: str) -> None:
    ks = re.sub(r"[^A-Za-z0-9_]", "_", str(key_suffix or "default"))
    if not re.match(r"^[A-Za-z_]", ks):
        ks = f"k_{ks}"
    js_text = json.dumps(safe_text(plan_text, default=""))
    js_json = json.dumps(safe_text(plan_json_text, default=""))
    html = f"""
<div style="display:flex;gap:8px;align-items:center;">
  <button onclick="copyPlanText_{ks}()" style="background:#0d6efd;color:#fff;border:none;border-radius:6px;padding:6px 10px;cursor:pointer;">
    Copier le plan (texte)
  </button>
  <button onclick="copyPlanJson_{ks}()" style="background:#198754;color:#fff;border:none;border-radius:6px;padding:6px 10px;cursor:pointer;">
    Copier le plan (JSON)
  </button>
  <span id="copyStatus_{ks}" style="color:#9aa0a6;font-size:0.85rem;"></span>
</div>
<script>
  async function copyPlanText_{ks}(){{
    try {{
      await navigator.clipboard.writeText({js_text});
      document.getElementById("copyStatus_{ks}").innerText = "Texte copié.";
    }} catch(e) {{
      document.getElementById("copyStatus_{ks}").innerText = "Copie bloquée par le navigateur.";
    }}
  }}
  async function copyPlanJson_{ks}(){{
    try {{
      await navigator.clipboard.writeText({js_json});
      document.getElementById("copyStatus_{ks}").innerText = "JSON copié.";
    }} catch(e) {{
      document.getElementById("copyStatus_{ks}").innerText = "Copie bloquée par le navigateur.";
    }}
  }}
</script>
"""
    components.html(html, height=52)


def _fmt_pct_auto(v: object, ndigits: int = 1, default: str = "N/A") -> str:
    n = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
    if pd.isna(n):
        return default
    n = float(n)
    if abs(n) <= 1.0:
        n *= 100.0
    return f"{n:.{ndigits}f}%"


def _kpi_metric_with_info(label: str, value: object, tooltip_text: str) -> None:
    shown = safe_text(value, default="N/A")
    st.metric(label, shown)
    tip = tooltip_text
    if shown == "N/A":
        tip = f"{tooltip_text}\n\n{TEXTS_FR['na_tooltip']}"
    _render_inline_info(label, tip)


def _metric_meta(metric_id: str) -> dict[str, str]:
    return METRICS_META.get(metric_id, {})


def _render_metric_help_popover(metric_id: str, unique_suffix: str, overrides: dict[str, str] | None = None) -> None:
    meta = dict(_metric_meta(metric_id))
    if overrides:
        meta.update({k: str(v) for k, v in overrides.items() if v is not None})
    if not meta:
        return

    label = str(meta.get("label", metric_id))
    hidden_suffix = "\u200b" * (1 + (sum(ord(ch) for ch in str(unique_suffix or metric_id)) % 5))
    pop_label = f"ⓘ {label}{hidden_suffix}"
    content = {
        "Definition simple": meta.get("definition_short", "N/A"),
        "Definition detaillee": meta.get("definition_long", "N/A"),
        "Regle de calcul": meta.get("formula", "N/A"),
        "Source": meta.get("source", "N/A"),
        "Frequence MAJ": meta.get("update_frequency", "N/A"),
        "Impact decision": meta.get("impact_on_decision", "N/A"),
    }
    if hasattr(st, "popover"):
        with st.popover(pop_label):
            for k, v in content.items():
                st.markdown(f"**{k}:** {v}")
    else:
        with st.expander(pop_label):
            for k, v in content.items():
                st.markdown(f"**{k}:** {v}")


def _metrics_dictionary_df(metric_ids: list[str] | None = None) -> pd.DataFrame:
    ids = metric_ids if metric_ids else list(METRICS_META.keys())
    rows = []
    for mid in ids:
        meta = _metric_meta(mid)
        if not meta:
            continue
        rows.append(
            {
                "Champ": mid,
                "Label": meta.get("label", mid),
                "Definition": meta.get("definition_long", meta.get("definition_short", "")),
                "Formule": meta.get("formula", ""),
                "Unite": meta.get("unit", ""),
                "Source": meta.get("source", ""),
                "Frequence MAJ": meta.get("update_frequency", ""),
                "Impact decision": meta.get("impact_on_decision", ""),
                "Format": meta.get("display_format", ""),
            }
        )
    return pd.DataFrame(rows)

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
# LOAD DATA - DuckDB (Lazy / page-level)
# ============================================================


def _env_int(name: str, default: int, minimum: int) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except Exception:
        return max(minimum, default)


def _env_float(name: str, default: float, minimum: float) -> float:
    try:
        return max(minimum, float(os.getenv(name, str(default))))
    except Exception:
        return max(minimum, default)


DUCKDB_CACHE_TTL_SEC = _env_int("DASHBOARD_DUCKDB_CACHE_TTL_SEC", 1800, 60)
RUN_LOG_LIMIT = _env_int("RUN_LOG_LIMIT", 200, 20)
HISTORY_DAYS_DEFAULT = _env_int("HISTORY_DAYS_DEFAULT", 30, 1)
HISTORY_LIMIT_DEFAULT = _env_int("HISTORY_LIMIT_DEFAULT", 20000, 1000)


def duckdb_file_signature(path: str) -> tuple[str, float, int]:
    """Signature stable de fichier pour invalider le cache par mtime/size."""
    norm_path = str(path or "")
    if not norm_path:
        return ("", 0.0, 0)
    try:
        abs_path = str(Path(norm_path).resolve())
    except Exception:
        abs_path = norm_path
    if not os.path.exists(norm_path):
        return (abs_path, 0.0, 0)
    try:
        return (abs_path, float(os.path.getmtime(norm_path)), int(os.path.getsize(norm_path)))
    except Exception:
        return (abs_path, 0.0, 0)


def _connect_readonly(path: str):
    if not path or not os.path.exists(path):
        return None
    max_retries = _env_int("DUCKDB_READ_RETRIES", 8, 3)
    base_delay = _env_float("DUCKDB_READ_BASE_DELAY_SEC", 0.25, 0.1)
    max_delay = _env_float("DUCKDB_READ_MAX_DELAY_SEC", 3.0, base_delay)

    conn = None
    for attempt in range(max_retries):
        try:
            conn = duckdb.connect(path, read_only=True)
            return conn
        except Exception as exc:
            msg = str(exc).lower()
            is_lock_like = isinstance(exc, duckdb.IOException) or ("lock" in msg) or ("busy" in msg)
            if is_lock_like and attempt < max_retries - 1:
                time.sleep(min(base_delay * (2 ** attempt), max_delay))
                continue
            return None
    return conn


def _read_duckdb_df(path: str, query: str, params: tuple[object, ...] | None = None) -> pd.DataFrame:
    conn = _connect_readonly(path)
    if conn is None:
        return pd.DataFrame()
    try:
        if params:
            return conn.execute(query, list(params)).fetchdf()
        return conn.execute(query).fetchdf()
    except Exception:
        return pd.DataFrame()
    finally:
        try:
            conn.close()
        except Exception:
            pass


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_universe_latest(db_path: str, db_sig: tuple[str, float, int]) -> pd.DataFrame:
    _ = db_sig
    return _read_duckdb_df(db_path, "SELECT * FROM universe ORDER BY symbol")


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_ag1_portfolio_latest(db_path: str, db_sig: tuple[str, float, int]) -> pd.DataFrame:
    _ = db_sig
    return _read_duckdb_df(
        db_path,
        """
        SELECT
            symbol AS symbol,
            name AS name,
            asset_class AS assetclass,
            sector AS sector,
            industry AS industry,
            isin AS isin,
            quantity AS quantity,
            avg_price AS avgprice,
            last_price AS lastprice,
            market_value AS marketvalue,
            unrealized_pnl AS unrealizedpnl,
            updated_at AS updatedat
        FROM portfolio_positions_mtm_latest
        ORDER BY market_value DESC NULLS LAST, symbol
        """,
    )


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_ag2_overview(db_path: str, db_sig: tuple[str, float, int], run_log_limit: int) -> dict[str, pd.DataFrame]:
    _ = db_sig
    run_limit = max(1, int(run_log_limit))
    return {
        "df_signals": _read_duckdb_df(
            db_path,
            """
            SELECT ts.*
            FROM technical_signals ts
            INNER JOIN (
                SELECT symbol, MAX(workflow_date) AS max_date
                FROM technical_signals
                GROUP BY symbol
            ) latest ON ts.symbol = latest.symbol AND ts.workflow_date = latest.max_date
            ORDER BY ts.symbol
            """,
        ),
        "df_runs": _read_duckdb_df(
            db_path,
            "SELECT * FROM run_log ORDER BY started_at DESC LIMIT ?",
            (run_limit,),
        ),
    }


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_ag2_history(
    db_path: str,
    db_sig: tuple[str, float, int],
    window_days: int,
    limit: int,
) -> pd.DataFrame:
    _ = db_sig
    days = max(1, int(window_days))
    lim = max(1, int(limit))
    return _read_duckdb_df(
        db_path,
        """
        SELECT *
        FROM technical_signals
        WHERE workflow_date >= (NOW() - (? * INTERVAL '1 day'))
        ORDER BY workflow_date DESC, symbol
        LIMIT ?
        """,
        (days, lim),
    )


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_ag3_overview(db_path: str, db_sig: tuple[str, float, int], run_log_limit: int) -> dict[str, pd.DataFrame]:
    _ = db_sig
    run_limit = max(1, int(run_log_limit))

    df_funda_latest = _read_duckdb_df(db_path, "SELECT * FROM v_latest_triage ORDER BY symbol")
    if df_funda_latest.empty:
        df_funda_latest = _read_duckdb_df(
            db_path,
            """
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
            """,
        )

    df_funda_consensus = _read_duckdb_df(db_path, "SELECT * FROM v_latest_consensus ORDER BY symbol")
    if df_funda_consensus.empty:
        df_funda_consensus = _read_duckdb_df(
            db_path,
            """
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
            """,
        )

    return {
        "df_funda_latest": df_funda_latest,
        "df_funda_runs": _read_duckdb_df(
            db_path,
            "SELECT * FROM run_log ORDER BY started_at DESC LIMIT ?",
            (run_limit,),
        ),
        "df_funda_consensus": df_funda_consensus,
        "df_funda_metrics": _read_duckdb_df(
            db_path,
            """
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
            """,
        ),
    }


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_ag3_symbol_history(
    db_path: str,
    db_sig: tuple[str, float, int],
    symbol: str,
    window_days: int,
    limit: int,
) -> pd.DataFrame:
    _ = db_sig
    sym = str(symbol or "").strip().upper()
    if not sym:
        return pd.DataFrame()
    days = max(1, int(window_days))
    lim = max(1, int(limit))
    return _read_duckdb_df(
        db_path,
        """
        SELECT *
        FROM fundamentals_triage_history
        WHERE UPPER(symbol) = ?
          AND COALESCE(updated_at, created_at, fetched_at) >= (NOW() - (? * INTERVAL '1 day'))
        ORDER BY COALESCE(updated_at, created_at, fetched_at) DESC
        LIMIT ?
        """,
        (sym, days, lim),
    )


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_ag3_run_quality_history(
    db_path: str,
    db_sig: tuple[str, float, int],
    window_days: int,
    limit: int,
) -> pd.DataFrame:
    _ = db_sig
    days = max(1, int(window_days))
    lim = max(1, int(limit))
    return _read_duckdb_df(
        db_path,
        """
        SELECT
            run_id,
            MAX(COALESCE(updated_at, created_at, fetched_at)) AS ts,
            AVG(COALESCE(score, funda_conf)) AS avg_score,
            AVG(risk_score) AS avg_risk,
            COUNT(DISTINCT symbol) AS symbols
        FROM fundamentals_triage_history
        WHERE COALESCE(updated_at, created_at, fetched_at) >= (NOW() - (? * INTERVAL '1 day'))
        GROUP BY run_id
        ORDER BY ts DESC
        LIMIT ?
        """,
        (days, lim),
    )


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_ag4_macro_history(
    db_path: str,
    db_sig: tuple[str, float, int],
    window_days: int,
    limit: int,
) -> pd.DataFrame:
    _ = db_sig
    days = max(1, int(window_days))
    lim = max(1, int(limit))
    return _read_duckdb_df(
        db_path,
        """
        SELECT *
        FROM news_history
        WHERE COALESCE(type, 'macro') = 'macro'
          AND COALESCE(published_at, analyzed_at, last_seen_at, updated_at, created_at) >= (NOW() - (? * INTERVAL '1 day'))
        ORDER BY COALESCE(published_at, analyzed_at, last_seen_at, updated_at, created_at) DESC
        LIMIT ?
        """,
        (days, lim),
    )


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_ag4_macro_runs(db_path: str, db_sig: tuple[str, float, int], run_log_limit: int) -> pd.DataFrame:
    _ = db_sig
    run_limit = max(1, int(run_log_limit))
    return _read_duckdb_df(db_path, "SELECT * FROM run_log ORDER BY started_at DESC LIMIT ?", (run_limit,))


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_ag4_symbol_history(
    db_path: str,
    db_sig: tuple[str, float, int],
    window_days: int,
    limit: int,
    scope_symbols: tuple[str, ...] = (),
) -> pd.DataFrame:
    _ = db_sig
    days = max(1, int(window_days))
    lim = max(1, int(limit))
    clean_symbols = tuple(sorted({str(s).strip().upper() for s in scope_symbols if str(s).strip()}))

    query = """
        SELECT *
        FROM news_history
        WHERE COALESCE(published_at, analyzed_at, fetched_at, updated_at, created_at) >= (NOW() - (? * INTERVAL '1 day'))
    """
    params: list[object] = [days]

    if clean_symbols:
        placeholders = ",".join(["?"] * len(clean_symbols))
        query += f"\n  AND UPPER(symbol) IN ({placeholders})"
        params.extend(clean_symbols)

    query += "\nORDER BY COALESCE(published_at, analyzed_at, fetched_at, updated_at, created_at) DESC, symbol\nLIMIT ?"
    params.append(lim)
    return _read_duckdb_df(db_path, query, tuple(params))


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_ag4_symbol_history_from_macro(
    db_path: str,
    db_sig: tuple[str, float, int],
    window_days: int,
    limit: int,
    scope_symbols: tuple[str, ...] = (),
) -> pd.DataFrame:
    _ = db_sig
    days = max(1, int(window_days))
    lim = max(1, int(limit))
    clean_symbols = tuple(sorted({str(s).strip().upper() for s in scope_symbols if str(s).strip()}))

    query = """
        SELECT *
        FROM news_history
        WHERE COALESCE(type, '') = 'symbol'
          AND COALESCE(published_at, analyzed_at, last_seen_at, updated_at, created_at) >= (NOW() - (? * INTERVAL '1 day'))
    """
    params: list[object] = [days]

    if clean_symbols:
        placeholders = ",".join(["?"] * len(clean_symbols))
        query += f"\n  AND UPPER(symbol) IN ({placeholders})"
        params.extend(clean_symbols)

    query += "\nORDER BY COALESCE(published_at, analyzed_at, last_seen_at, updated_at, created_at) DESC, symbol\nLIMIT ?"
    params.append(lim)
    return _read_duckdb_df(db_path, query, tuple(params))


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_ag4_symbol_runs(db_path: str, db_sig: tuple[str, float, int], run_log_limit: int) -> pd.DataFrame:
    _ = db_sig
    run_limit = max(1, int(run_log_limit))
    return _read_duckdb_df(db_path, "SELECT * FROM run_log ORDER BY started_at DESC LIMIT ?", (run_limit,))


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_yf_enrichment_latest(db_path: str, db_sig: tuple[str, float, int]) -> pd.DataFrame:
    _ = db_sig
    df = _read_duckdb_df(db_path, "SELECT * FROM v_latest_symbol_enrichment ORDER BY symbol")
    if not df.empty:
        return df
    return _read_duckdb_df(
        db_path,
        """
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
        """,
    )


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_dashboard_market_data(
    ag3_db_path: str,
    ag3_db_sig: tuple[str, float, int],
    ag4_db_path: str,
    ag4_db_sig: tuple[str, float, int],
    ag4_spe_db_path: str,
    ag4_spe_db_sig: tuple[str, float, int],
    window_days: int,
    history_limit: int,
    run_log_limit: int,
) -> dict[str, pd.DataFrame]:
    ag3_data = load_ag3_overview(ag3_db_path, ag3_db_sig, run_log_limit)
    symbol_history = load_ag4_symbol_history(ag4_spe_db_path, ag4_spe_db_sig, window_days, history_limit)
    if symbol_history.empty:
        symbol_history = load_ag4_symbol_history_from_macro(
            ag4_db_path,
            ag4_db_sig,
            window_days,
            history_limit,
        )
    return {
        "df_funda_latest": ag3_data.get("df_funda_latest", pd.DataFrame()),
        "df_news_macro_history": load_ag4_macro_history(ag4_db_path, ag4_db_sig, window_days, history_limit),
        "df_news_symbol_history": symbol_history,
    }


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_system_health_page_data(
    ag2_db_path: str,
    ag2_db_sig: tuple[str, float, int],
    ag3_db_path: str,
    ag3_db_sig: tuple[str, float, int],
    ag4_db_path: str,
    ag4_db_sig: tuple[str, float, int],
    ag4_spe_db_path: str,
    ag4_spe_db_sig: tuple[str, float, int],
    history_days: int,
    history_limit: int,
    run_log_limit: int,
) -> dict[str, pd.DataFrame]:
    ag2_data = load_ag2_overview(ag2_db_path, ag2_db_sig, run_log_limit)
    ag3_data = load_ag3_overview(ag3_db_path, ag3_db_sig, run_log_limit)
    macro_history = load_ag4_macro_history(ag4_db_path, ag4_db_sig, history_days, history_limit)
    symbol_history = load_ag4_symbol_history(ag4_spe_db_path, ag4_spe_db_sig, history_days, history_limit)
    if symbol_history.empty:
        symbol_history = load_ag4_symbol_history_from_macro(
            ag4_db_path,
            ag4_db_sig,
            history_days,
            history_limit,
        )
    return {
        "df_signals": ag2_data.get("df_signals", pd.DataFrame()),
        "df_runs": ag2_data.get("df_runs", pd.DataFrame()),
        "df_funda_latest": ag3_data.get("df_funda_latest", pd.DataFrame()),
        "df_funda_runs": ag3_data.get("df_funda_runs", pd.DataFrame()),
        "df_news_macro_history": macro_history,
        "df_news_macro_runs": load_ag4_macro_runs(ag4_db_path, ag4_db_sig, run_log_limit),
        "df_news_symbol_history": symbol_history,
        "df_news_symbol_runs": load_ag4_symbol_runs(ag4_spe_db_path, ag4_spe_db_sig, run_log_limit),
    }


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_multi_agent_page_data(
    ag2_db_path: str,
    ag2_db_sig: tuple[str, float, int],
    ag3_db_path: str,
    ag3_db_sig: tuple[str, float, int],
    ag4_db_path: str,
    ag4_db_sig: tuple[str, float, int],
    ag4_spe_db_path: str,
    ag4_spe_db_sig: tuple[str, float, int],
    yf_db_path: str,
    yf_db_sig: tuple[str, float, int],
    history_days: int,
    history_limit: int,
    run_log_limit: int,
) -> dict[str, pd.DataFrame]:
    ag2_data = load_ag2_overview(ag2_db_path, ag2_db_sig, run_log_limit)
    ag3_data = load_ag3_overview(ag3_db_path, ag3_db_sig, run_log_limit)
    macro_history = load_ag4_macro_history(ag4_db_path, ag4_db_sig, history_days, history_limit)
    symbol_history = load_ag4_symbol_history(ag4_spe_db_path, ag4_spe_db_sig, history_days, history_limit)
    if symbol_history.empty:
        symbol_history = load_ag4_symbol_history_from_macro(
            ag4_db_path,
            ag4_db_sig,
            history_days,
            history_limit,
        )
    return {
        "df_signals": ag2_data.get("df_signals", pd.DataFrame()),
        "df_funda_latest": ag3_data.get("df_funda_latest", pd.DataFrame()),
        "df_news_macro_history": macro_history,
        "df_news_symbol_history": symbol_history,
        "df_yf_enrichment_latest": load_yf_enrichment_latest(yf_db_path, yf_db_sig),
    }


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_ag4_page_data(
    ag4_db_path: str,
    ag4_db_sig: tuple[str, float, int],
    ag4_spe_db_path: str,
    ag4_spe_db_sig: tuple[str, float, int],
    window_days: int,
    history_limit: int,
    run_log_limit: int,
    scope_symbols: tuple[str, ...] = (),
) -> dict[str, pd.DataFrame]:
    symbol_history = load_ag4_symbol_history(ag4_spe_db_path, ag4_spe_db_sig, window_days, history_limit, scope_symbols)
    if symbol_history.empty:
        symbol_history = load_ag4_symbol_history_from_macro(
            ag4_db_path,
            ag4_db_sig,
            window_days,
            history_limit,
            scope_symbols,
        )
    return {
        "df_news_macro_history": load_ag4_macro_history(ag4_db_path, ag4_db_sig, window_days, history_limit),
        "df_news_symbol_history": symbol_history,
        "df_news_macro_runs": load_ag4_macro_runs(ag4_db_path, ag4_db_sig, run_log_limit),
        "df_news_symbol_runs": load_ag4_symbol_runs(ag4_spe_db_path, ag4_spe_db_sig, run_log_limit),
    }


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_ag2_page_data(
    ag2_db_path: str,
    ag2_db_sig: tuple[str, float, int],
    run_log_limit: int,
) -> dict[str, pd.DataFrame]:
    return load_ag2_overview(ag2_db_path, ag2_db_sig, run_log_limit)


@st.cache_data(ttl=DUCKDB_CACHE_TTL_SEC)
def load_ag3_page_data(
    ag3_db_path: str,
    ag3_db_sig: tuple[str, float, int],
    run_log_limit: int,
) -> dict[str, pd.DataFrame]:
    return load_ag3_overview(ag3_db_path, ag3_db_sig, run_log_limit)


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
# HELPERS V2 - AG2 overview (visual/actionable)
# ============================================================


def _ag2_short_run_id(run_id: object) -> str:
    s = str(run_id or "").strip()
    if not s:
        return "—"
    return s if len(s) <= 22 else f"{s[:10]}...{s[-8:]}"


def _ag2_safe_div(num: float, den: float) -> float | None:
    try:
        n = float(num)
        d = float(den)
    except Exception:
        return None
    if d == 0:
        return None
    return n / d


def _ag2_ratio_text(num: float, den: float, digits: int = 1, suffix: str = "") -> str:
    r = _ag2_safe_div(num, den)
    if r is None:
        return "—"
    return f"{r * 100.0:.{digits}f}%{suffix}"


def _ag2_delta_text(current: object, previous: object, *, digits: int = 0, prefix: str = "vs prev ") -> str | None:
    cur = pd.to_numeric(pd.Series([current]), errors="coerce").iloc[0]
    prev = pd.to_numeric(pd.Series([previous]), errors="coerce").iloc[0]
    if pd.isna(cur) or pd.isna(prev):
        return None
    diff = float(cur) - float(prev)
    sign = "+" if diff >= 0 else ""
    return f"{prefix}{sign}{diff:.{digits}f}"


def _ag2_age_hours(ts: object) -> float | None:
    dt = pd.to_datetime(ts, errors="coerce", utc=True)
    if pd.isna(dt):
        return None
    now_utc = pd.Timestamp.now(tz="UTC")
    return max(0.0, float((now_utc - dt).total_seconds() / 3600.0))


def _ag2_fmt_age(age_h: object) -> str:
    n = pd.to_numeric(pd.Series([age_h]), errors="coerce").iloc[0]
    if pd.isna(n):
        return "—"
    n = float(n)
    if n < 24:
        return f"{n:.1f}h"
    return f"{(n / 24.0):.1f}j"


def _ag2_status_pill_html(level: str, text: str | None = None) -> str:
    lvl = str(level or "WARN").upper()
    if lvl == "OK":
        bg = "#16a34a"
    elif lvl == "ERROR":
        bg = "#dc2626"
    else:
        bg = "#d97706"
        lvl = "WARN"
    label = html.escape(text or lvl)
    return (
        f"<span style='display:inline-block;padding:4px 10px;border-radius:999px;"
        f"background:{bg};color:#fff;font-weight:700;font-size:0.8rem;'>{label}</span>"
    )


def _ag2_norm_action_value(v: object) -> str:
    s = str(v or "").strip().upper()
    if s in ("BUY", "SELL", "NEUTRAL"):
        return s
    return "NEUTRAL"


def _ag2_norm_ai_decision_value(v: object) -> str:
    s = str(v or "").strip().upper()
    if not s:
        return "—"
    return s


def _ag2_pick_col(df: pd.DataFrame, candidates: list[str], default_name: str, default_value: object = pd.NA) -> pd.Series:
    if df is None or df.empty:
        return pd.Series([], dtype=object, name=default_name)
    col = _first_existing_column(df, candidates)
    if col and col in df.columns:
        return df[col]
    return pd.Series([default_value] * len(df), index=df.index, name=default_name)


@st.cache_data(ttl=60)
def _ag2_prepare_overview_working_df(df_signals: pd.DataFrame) -> pd.DataFrame:
    if df_signals is None or df_signals.empty:
        return pd.DataFrame()

    wk = normalize_cols(df_signals.copy())
    if "symbol" not in wk.columns:
        wk["symbol"] = ""
    wk["symbol"] = wk["symbol"].fillna("").astype(str).str.strip().str.upper()
    wk = wk[wk["symbol"] != ""].copy()
    if wk.empty:
        return pd.DataFrame()

    if "name" not in wk.columns:
        wk["name"] = ""
    if "sector" not in wk.columns:
        wk["sector"] = ""
    if "filter_reason" not in wk.columns:
        wk["filter_reason"] = _ag2_pick_col(wk, ["filter", "reason"], "filter_reason", "")
    if "ai_decision" not in wk.columns:
        wk["ai_decision"] = _ag2_pick_col(wk, ["ia_decision", "decision_ia"], "ai_decision", "")
    if "ai_quality" not in wk.columns:
        wk["ai_quality"] = _ag2_pick_col(wk, ["ia_quality", "quality_ia"], "ai_quality", pd.NA)
    if "call_ai" not in wk.columns:
        wk["call_ai"] = _ag2_pick_col(wk, ["should_call_ai"], "call_ai", False)
    if "workflow_date" not in wk.columns:
        wk["workflow_date"] = _ag2_pick_col(wk, ["updated_at", "created_at", "date"], "workflow_date", pd.NaT)
    if "last_close" not in wk.columns:
        wk["last_close"] = _ag2_pick_col(wk, ["close", "d1_last_close", "h1_last_close"], "last_close", pd.NA)
    if "d1_action" not in wk.columns:
        wk["d1_action"] = _ag2_pick_col(wk, ["action_d1"], "d1_action", "")
    if "h1_action" not in wk.columns:
        wk["h1_action"] = _ag2_pick_col(wk, ["action_h1"], "h1_action", "")
    if "d1_score" not in wk.columns:
        wk["d1_score"] = _ag2_pick_col(wk, ["score_d1"], "d1_score", pd.NA)
    if "h1_score" not in wk.columns:
        wk["h1_score"] = _ag2_pick_col(wk, ["score_h1"], "h1_score", pd.NA)
    if "d1_rsi14" not in wk.columns:
        wk["d1_rsi14"] = _ag2_pick_col(wk, ["d1_rsi", "rsi_d1"], "d1_rsi14", pd.NA)
    if "h1_rsi14" not in wk.columns:
        wk["h1_rsi14"] = _ag2_pick_col(wk, ["h1_rsi", "rsi_h1"], "h1_rsi14", pd.NA)

    wk["name"] = wk["name"].fillna("").astype(str).str.strip()
    wk["sector"] = wk["sector"].fillna("").astype(str).str.strip().replace("", "Sans secteur")
    wk["filter_reason"] = wk["filter_reason"].fillna("").astype(str).str.strip()
    wk["ai_decision_norm"] = wk["ai_decision"].map(_ag2_norm_ai_decision_value)
    wk["h1_action_norm"] = wk["h1_action"].map(_ag2_norm_action_value)
    wk["d1_action_norm"] = wk["d1_action"].map(_ag2_norm_action_value)

    wk["last_close_num"] = safe_float_series(wk["last_close"])
    wk["h1_score_num"] = safe_float_series(wk["h1_score"])
    wk["d1_score_num"] = safe_float_series(wk["d1_score"])
    wk["h1_rsi_num"] = safe_float_series(wk["h1_rsi14"])
    wk["d1_rsi_num"] = safe_float_series(wk["d1_rsi14"])
    wk["ai_quality_num"] = safe_float_series(wk["ai_quality"])
    wk["call_ai_flag"] = truthy_series(wk["call_ai"]) if "call_ai" in wk.columns else pd.Series(False, index=wk.index)

    wk["workflow_ts"] = pd.to_datetime(wk["workflow_date"], errors="coerce", utc=True)
    wk["h1_ts"] = pd.to_datetime(_ag2_pick_col(wk, ["h1_date", "h1_ts", "h1_updated_at"], "h1_ts", pd.NaT), errors="coerce", utc=True)
    wk["d1_ts"] = pd.to_datetime(_ag2_pick_col(wk, ["d1_date", "d1_ts", "d1_updated_at"], "d1_ts", pd.NaT), errors="coerce", utc=True)

    wk["is_actionable_d1"] = wk["d1_action_norm"].isin(["BUY", "SELL"])
    wk["is_divergence_h1d1"] = (
        (wk["h1_action_norm"] != wk["d1_action_norm"])
        & ~((wk["h1_action_norm"] == "NEUTRAL") & (wk["d1_action_norm"] == "NEUTRAL"))
    )
    wk["is_confluence_buy"] = (wk["h1_action_norm"] == "BUY") & (wk["d1_action_norm"] == "BUY")
    wk["is_confluence_sell"] = (wk["h1_action_norm"] == "SELL") & (wk["d1_action_norm"] == "SELL")

    return wk.reset_index(drop=True)


def _ag2_kpi_counts(df: pd.DataFrame) -> dict[str, int]:
    if df is None or df.empty:
        return {
            "total_symbols": 0,
            "buy_count": 0,
            "sell_count": 0,
            "neutral_count": 0,
            "actionable_count": 0,
            "ai_calls": 0,
            "ai_approvals": 0,
        }
    d1 = df.get("d1_action_norm", pd.Series("NEUTRAL", index=df.index)).astype(str)
    ai = df.get("ai_decision_norm", pd.Series("—", index=df.index)).astype(str)
    calls = df.get("call_ai_flag", pd.Series(False, index=df.index))
    total = int(len(df))
    buy = int((d1 == "BUY").sum())
    sell = int((d1 == "SELL").sum())
    neutral = max(0, total - buy - sell)
    actionable = buy + sell
    ai_calls = int(pd.Series(calls).fillna(False).astype(bool).sum())
    ai_approvals = int(ai.eq("APPROVE").sum())
    return {
        "total_symbols": total,
        "buy_count": buy,
        "sell_count": sell,
        "neutral_count": neutral,
        "actionable_count": actionable,
        "ai_calls": ai_calls,
        "ai_approvals": ai_approvals,
    }


def _ag2_latest_run_meta(df_runs: pd.DataFrame, df_working: pd.DataFrame) -> dict[str, object]:
    out: dict[str, object] = {
        "run_id": "",
        "run_ts": pd.NaT,
        "run_status": "",
        "prev_total": None,
        "prev_ai_calls": None,
        "prev_ai_approvals": None,
    }
    if df_runs is not None and not df_runs.empty:
        rk = normalize_cols(df_runs.copy())
        ts_col = _first_existing_column(rk, ["finished_at", "started_at", "updated_at", "created_at"])
        if ts_col:
            rk["_run_ts"] = pd.to_datetime(rk[ts_col], errors="coerce", utc=True)
        else:
            rk["_run_ts"] = pd.NaT
        rk = rk.sort_values("_run_ts", ascending=False, na_position="last").reset_index(drop=True)
        if not rk.empty:
            row0 = rk.iloc[0]
            out["run_id"] = str(row0.get("run_id", "") or "")
            out["run_ts"] = row0.get("_run_ts", pd.NaT)
            out["run_status"] = str(row0.get("status", "") or "").upper()
            if "symbols_ok" in rk.columns or "symbols_error" in rk.columns:
                ok0 = pd.to_numeric(pd.Series([row0.get("symbols_ok", pd.NA)]), errors="coerce").iloc[0]
                er0 = pd.to_numeric(pd.Series([row0.get("symbols_error", pd.NA)]), errors="coerce").iloc[0]
                if len(rk) > 1:
                    row1 = rk.iloc[1]
                    ok1 = pd.to_numeric(pd.Series([row1.get("symbols_ok", pd.NA)]), errors="coerce").iloc[0]
                    er1 = pd.to_numeric(pd.Series([row1.get("symbols_error", pd.NA)]), errors="coerce").iloc[0]
                    if not (pd.isna(ok1) and pd.isna(er1)):
                        out["prev_total"] = float((0 if pd.isna(ok1) else ok1) + (0 if pd.isna(er1) else er1))
                if not (pd.isna(ok0) and pd.isna(er0)):
                    out["run_total"] = float((0 if pd.isna(ok0) else ok0) + (0 if pd.isna(er0) else er0))
            if "ai_calls" in rk.columns:
                cur = pd.to_numeric(pd.Series([rk.iloc[0].get("ai_calls", pd.NA)]), errors="coerce").iloc[0]
                if not pd.isna(cur):
                    out["run_ai_calls"] = float(cur)
                if len(rk) > 1:
                    prev = pd.to_numeric(pd.Series([rk.iloc[1].get("ai_calls", pd.NA)]), errors="coerce").iloc[0]
                    if not pd.isna(prev):
                        out["prev_ai_calls"] = float(prev)
            appr_col = _first_existing_column(rk, ["ai_approved", "ai_approvals", "approved_ai", "approved_count"])
            if appr_col:
                cur = pd.to_numeric(pd.Series([rk.iloc[0].get(appr_col, pd.NA)]), errors="coerce").iloc[0]
                if not pd.isna(cur):
                    out["run_ai_approvals"] = float(cur)
                if len(rk) > 1:
                    prev = pd.to_numeric(pd.Series([rk.iloc[1].get(appr_col, pd.NA)]), errors="coerce").iloc[0]
                    if not pd.isna(prev):
                        out["prev_ai_approvals"] = float(prev)

    if pd.isna(out.get("run_ts", pd.NaT)) and df_working is not None and not df_working.empty:
        out["run_ts"] = _latest_timestamp(df_working, ["workflow_ts", "workflow_date", "updated_at", "created_at"])

    return out


def _ag2_signal_mix_figure(df: pd.DataFrame) -> go.Figure | None:
    if df is None or df.empty:
        return None
    actions = ["BUY", "SELL", "NEUTRAL"]
    colors = {"BUY": "#22c55e", "SELL": "#ef4444", "NEUTRAL": "#6b7280"}
    rows = []
    for tf_label, col in [("H1", "h1_action_norm"), ("D1", "d1_action_norm")]:
        ser = df.get(col, pd.Series("NEUTRAL", index=df.index)).astype(str)
        counts = ser.value_counts()
        row = {"timeframe": tf_label}
        for a in actions:
            row[a] = int(counts.get(a, 0))
        rows.append(row)
    chart_df = pd.DataFrame(rows)
    fig = go.Figure()
    for a in actions:
        fig.add_bar(
            x=chart_df["timeframe"],
            y=chart_df[a],
            name=a,
            marker_color=colors[a],
            text=chart_df[a],
            textposition="inside",
        )
    fig.update_layout(
        barmode="stack",
        height=250,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=1.08, x=0),
        yaxis=dict(title="Count", gridcolor="rgba(128,128,128,0.15)"),
        xaxis=dict(title=None),
    )
    return fig


def _ag2_sector_action_heatmap_figure(df: pd.DataFrame) -> go.Figure | None:
    if df is None or df.empty or "sector" not in df.columns:
        return None
    wk = df.copy()
    wk["sector"] = wk["sector"].fillna("").astype(str).str.strip().replace("", "Sans secteur")
    wk["d1_action_norm"] = wk.get("d1_action_norm", pd.Series("NEUTRAL", index=wk.index)).astype(str)
    heat = wk.groupby(["sector", "d1_action_norm"]).size().unstack(fill_value=0)
    if heat.empty:
        return None
    cols = ["BUY", "SELL", "NEUTRAL"]
    heat = heat.reindex(columns=cols, fill_value=0)
    heat["__total"] = heat.sum(axis=1)
    heat = heat.sort_values("__total", ascending=False).head(12)
    row_totals = heat["__total"].replace(0, pd.NA)
    z = heat[cols].to_numpy()
    text = []
    for sector in heat.index:
        row = []
        tot = row_totals.get(sector, pd.NA)
        for a in cols:
            cnt = int(heat.loc[sector, a])
            pct = (cnt / float(tot) * 100.0) if pd.notna(tot) and float(tot) > 0 else 0.0
            row.append(f"{cnt}<br>{pct:.0f}%")
        text.append(row)
    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=cols,
            y=heat.index.tolist(),
            text=text,
            texttemplate="%{text}",
            colorscale="Blues",
            reversescale=False,
            hovertemplate="Sector=%{y}<br>Action=%{x}<br>Count=%{z}<extra></extra>",
            colorbar=dict(title="Count"),
        )
    )
    fig.update_layout(
        height=280,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(title=None),
        yaxis=dict(title=None, automargin=True),
    )
    return fig


def _ag2_h1_d1_matrix_figure(df: pd.DataFrame) -> go.Figure | None:
    if df is None or df.empty:
        return None
    cols = ["BUY", "SELL", "NEUTRAL"]
    h1 = df.get("h1_action_norm", pd.Series("NEUTRAL", index=df.index)).astype(str)
    d1 = df.get("d1_action_norm", pd.Series("NEUTRAL", index=df.index)).astype(str)
    mat = pd.crosstab(h1, d1).reindex(index=cols, columns=cols, fill_value=0)
    z = mat.to_numpy()
    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=cols,
            y=cols,
            colorscale="RdYlGn",
            reversescale=True,
            text=[[str(int(v)) for v in row] for row in z],
            texttemplate="%{text}",
            hovertemplate="H1=%{y}<br>D1=%{x}<br>Count=%{z}<extra></extra>",
            colorbar=dict(title="Count"),
        )
    )
    fig.update_layout(
        height=280,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(title="D1 action"),
        yaxis=dict(title="H1 action"),
    )
    return fig


def _ag2_funnel_figure(total: int, actionable: int, ai_calls: int, ai_approvals: int) -> go.Figure:
    labels = ["Analyses", "Actionables", "Appels IA", "IA approuves"]
    values = [max(0, int(total)), max(0, int(actionable)), max(0, int(ai_calls)), max(0, int(ai_approvals))]
    fig = go.Figure(
        go.Funnel(
            y=labels,
            x=values,
            textinfo="value+percent previous",
            marker=dict(color=["#64748b", "#60a5fa", "#a78bfa", "#22c55e"]),
        )
    )
    fig.update_layout(
        height=260,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def _ag2_score_rsi_scatter_figure(df: pd.DataFrame) -> go.Figure | None:
    if df is None or df.empty:
        return None
    wk = df.copy()
    if "d1_score_num" not in wk.columns or "d1_rsi_num" not in wk.columns:
        return None
    wk = wk[pd.notna(wk["d1_score_num"]) & pd.notna(wk["d1_rsi_num"])].copy()
    if wk.empty:
        return None
    wk["d1_action_norm"] = wk.get("d1_action_norm", pd.Series("NEUTRAL", index=wk.index)).astype(str)
    color_map = {"BUY": "#22c55e", "SELL": "#ef4444", "NEUTRAL": "#9ca3af"}
    fig = px.scatter(
        wk,
        x="d1_rsi_num",
        y="d1_score_num",
        color="d1_action_norm",
        hover_data={"symbol": True, "name": True, "sector": True, "d1_rsi_num": ":.1f", "d1_score_num": ":.1f"},
        color_discrete_map=color_map,
        labels={"d1_rsi_num": "D1 RSI", "d1_score_num": "D1 Score", "d1_action_norm": "D1 Action"},
    )
    fig.update_traces(marker=dict(size=9, opacity=0.85))
    fig.add_vline(x=30, line_dash="dot", line_color="#64748b")
    fig.add_vline(x=70, line_dash="dot", line_color="#64748b")
    fig.update_layout(
        height=280,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=1.08, x=0),
        xaxis=dict(gridcolor="rgba(128,128,128,0.15)"),
        yaxis=dict(gridcolor="rgba(128,128,128,0.15)"),
    )
    return fig


def _ag2_make_display_table(df: pd.DataFrame, *, advanced: bool = False) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    wk = df.copy()
    out = pd.DataFrame(index=wk.index)
    out["Symbol"] = wk.get("symbol", pd.Series("", index=wk.index)).astype(str)
    out["Name"] = wk.get("name", pd.Series("", index=wk.index)).fillna("").astype(str)
    out["Sector"] = wk.get("sector", pd.Series("", index=wk.index)).fillna("").astype(str)
    close_num = wk.get("last_close_num", safe_float_series(wk.get("last_close", pd.Series(pd.NA, index=wk.index))))
    out["Close"] = close_num.apply(lambda v: f"{v:.2f}" if pd.notna(v) and float(v) > 0 else "—")
    out["D1 Action"] = wk.get("d1_action_norm", pd.Series("NEUTRAL", index=wk.index)).astype(str)
    d1_score = wk.get("d1_score_num", pd.Series(pd.NA, index=wk.index))
    out["D1 Score"] = d1_score.apply(lambda v: f"{float(v):.0f}" if pd.notna(v) else "—")
    d1_rsi = wk.get("d1_rsi_num", pd.Series(pd.NA, index=wk.index))
    out["D1 RSI"] = d1_rsi.apply(lambda v: f"{float(v):.1f}" if pd.notna(v) else "—")
    out["Filtre"] = wk.get("filter_reason", pd.Series("", index=wk.index)).fillna("").astype(str).replace("", "—")
    out["IA"] = wk.get("ai_decision_norm", pd.Series("—", index=wk.index)).astype(str)
    ai_q = wk.get("ai_quality_num", pd.Series(pd.NA, index=wk.index))
    out["Qualite IA"] = ai_q.apply(lambda v: f"{float(v):.1f}/10" if pd.notna(v) and float(v) > 0 else "—")
    wf_ts = pd.to_datetime(wk.get("workflow_ts", pd.Series(pd.NaT, index=wk.index)), errors="coerce", utc=True)
    out["Date"] = wf_ts.apply(lambda x: x.tz_convert("Europe/Paris").strftime("%Y-%m-%d") if pd.notna(x) else "—")

    if advanced:
        out["H1 Action"] = wk.get("h1_action_norm", pd.Series("NEUTRAL", index=wk.index)).astype(str)
        h1_score = wk.get("h1_score_num", pd.Series(pd.NA, index=wk.index))
        h1_rsi = wk.get("h1_rsi_num", pd.Series(pd.NA, index=wk.index))
        out["H1 Score"] = h1_score.apply(lambda v: f"{float(v):.0f}" if pd.notna(v) else "—")
        out["H1 RSI"] = h1_rsi.apply(lambda v: f"{float(v):.1f}" if pd.notna(v) else "—")

    return out.reset_index(drop=True)


def _ag2_style_display_table(df_display: pd.DataFrame, df_source: pd.DataFrame, *, quality_warn_threshold: float = 4.0):
    if df_display is None or df_display.empty:
        return df_display
    src = df_source.reset_index(drop=True).copy()
    disp = df_display.reset_index(drop=True).copy()

    def _cell_style(val: object) -> str:
        s = str(val or "").upper()
        if s == "BUY":
            return "background-color:#14532d;color:#dcfce7;font-weight:700;"
        if s == "SELL":
            return "background-color:#7f1d1d;color:#fee2e2;font-weight:700;"
        if s == "NEUTRAL":
            return "background-color:#374151;color:#e5e7eb;font-weight:700;"
        if s == "APPROVE":
            return "background-color:#14532d;color:#dcfce7;font-weight:700;"
        if s == "REJECT":
            return "background-color:#7c2d12;color:#ffedd5;font-weight:700;"
        if s == "WATCH":
            return "background-color:#78350f;color:#fef3c7;font-weight:700;"
        if s == "SKIP":
            return "background-color:#1f2937;color:#d1d5db;font-weight:700;"
        return ""

    def _row_styles(row: pd.Series) -> list[str]:
        i = row.name
        styles = [""] * len(row)
        if i >= len(src):
            return styles
        confluence_buy = bool(src.iloc[i].get("is_confluence_buy", False))
        confluence_sell = bool(src.iloc[i].get("is_confluence_sell", False))
        divergence = bool(src.iloc[i].get("is_divergence_h1d1", False))
        d1_score = pd.to_numeric(pd.Series([src.iloc[i].get("d1_score_num", pd.NA)]), errors="coerce").iloc[0]
        ai_dec = str(src.iloc[i].get("ai_decision_norm", "") or "").upper()
        ai_q = pd.to_numeric(pd.Series([src.iloc[i].get("ai_quality_num", pd.NA)]), errors="coerce").iloc[0]

        row_bg = ""
        if confluence_buy:
            row_bg = "background-color: rgba(34,197,94,0.08);"
        elif confluence_sell:
            row_bg = "background-color: rgba(239,68,68,0.08);"
        elif divergence:
            row_bg = "background-color: rgba(245,158,11,0.08);"
        if row_bg:
            styles = [row_bg] * len(row)

        if ai_dec == "REJECT" and pd.notna(d1_score) and abs(float(d1_score)) >= 70:
            styles = ["background-color: rgba(251,191,36,0.10);"] * len(row)

        if pd.notna(ai_q) and float(ai_q) > 0 and float(ai_q) < float(quality_warn_threshold):
            try:
                q_col = row.index.get_loc("Qualite IA")
                styles[q_col] = (styles[q_col] or "") + "background-color: rgba(239,68,68,0.18);"
            except Exception:
                pass
        return styles

    styler = disp.style
    for c in ["D1 Action", "H1 Action", "IA"]:
        if c in disp.columns:
            if hasattr(styler, "map"):
                styler = styler.map(_cell_style, subset=[c])
            else:
                styler = styler.applymap(_cell_style, subset=[c])
    styler = styler.apply(_row_styles, axis=1)
    return styler

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


def _benchmark_lookback_days(period_key: str, min_start_ts: object = None) -> int:
    mapping = {"7j": 15, "30j": 60, "90j": 150}
    if str(period_key) in mapping:
        return int(mapping[str(period_key)])

    ts = pd.to_datetime(min_start_ts, errors="coerce", utc=True)
    if pd.isna(ts):
        return 730
    days = int(max(30, (pd.Timestamp.now(tz="UTC") - ts).days + 30))
    return int(min(days, 3650))


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_benchmarks_history(
    tickers: tuple[str, ...],
    yfinance_api_url: str,
    lookback_days: int,
    interval: str = "1d",
) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    cleaned = tuple(sorted({str(t).strip().upper() for t in tickers if str(t).strip()}))
    if not cleaned:
        return out

    for ticker in cleaned:
        try:
            resp = requests.get(
                f"{str(yfinance_api_url).rstrip('/')}/history",
                params={
                    "symbol": ticker,
                    "interval": str(interval or "1d"),
                    "lookback_days": int(max(1, lookback_days)),
                    "allow_stale": "true",
                },
                timeout=10,
            )
            if resp.status_code != 200:
                out[ticker] = pd.DataFrame()
                continue
            payload = resp.json()
            bars = payload.get("bars", []) if isinstance(payload, dict) else []
            if not payload.get("ok") or not isinstance(bars, list) or not bars:
                out[ticker] = pd.DataFrame()
                continue

            df = pd.DataFrame(bars)
            if df.empty:
                out[ticker] = pd.DataFrame()
                continue
            df.rename(columns={"t": "timestamp", "c": "close"}, inplace=True)
            if "timestamp" not in df.columns or "close" not in df.columns:
                out[ticker] = pd.DataFrame()
                continue
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df = (
                df.dropna(subset=["timestamp", "close"])
                .sort_values("timestamp")
                .drop_duplicates(subset=["timestamp"], keep="last")
            )
            out[ticker] = df[["timestamp", "close"]].copy()
        except Exception:
            out[ticker] = pd.DataFrame()
    return out


def normalize_to_base100(df: pd.DataFrame, ts_col: str = "timestamp", value_col: str = "value") -> pd.DataFrame:
    if df is None or df.empty or ts_col not in df.columns or value_col not in df.columns:
        return pd.DataFrame(columns=["timestamp", "value", "value_norm"])

    wk = df[[ts_col, value_col]].copy()
    wk[ts_col] = pd.to_datetime(wk[ts_col], errors="coerce", utc=True)
    wk[value_col] = pd.to_numeric(wk[value_col], errors="coerce")
    wk = wk.dropna(subset=[ts_col, value_col]).sort_values(ts_col)
    wk = wk[wk[value_col] > 0]
    if wk.empty:
        return pd.DataFrame(columns=["timestamp", "value", "value_norm"])

    base = float(wk[value_col].iloc[0])
    if base <= 0:
        return pd.DataFrame(columns=["timestamp", "value", "value_norm"])

    wk["value_norm"] = (wk[value_col] / base) * 100.0
    wk = wk.rename(columns={ts_col: "timestamp", value_col: "value"})
    return wk[["timestamp", "value", "value_norm"]].copy()


def _align_daily_normalized_series(series_map: dict[str, pd.DataFrame]) -> pd.DataFrame:
    merged: pd.DataFrame | None = None
    for label, df in series_map.items():
        if df is None or df.empty or "timestamp" not in df.columns or "value_norm" not in df.columns:
            continue
        wk = df.copy()
        wk["date"] = pd.to_datetime(wk["timestamp"], errors="coerce", utc=True).dt.floor("D")
        wk["value_norm"] = pd.to_numeric(wk["value_norm"], errors="coerce")
        wk = wk.dropna(subset=["date", "value_norm"]).sort_values("date")
        if wk.empty:
            continue
        wk = wk.groupby("date", as_index=False)["value_norm"].last().rename(columns={"value_norm": str(label)})
        merged = wk if merged is None else merged.merge(wk, on="date", how="outer")

    if merged is None:
        return pd.DataFrame(columns=["date"])

    merged = merged.sort_values("date")
    value_cols = [c for c in merged.columns if c != "date"]
    if value_cols:
        merged[value_cols] = merged[value_cols].ffill()
        merged = merged.dropna(subset=value_cols, how="all")
        merged = merged.dropna(subset=value_cols, how="any")
    return merged


def _series_period_return_pct(series: pd.Series) -> float | None:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < 2:
        return None
    first = float(s.iloc[0])
    last = float(s.iloc[-1])
    if first == 0:
        return None
    return ((last / first) - 1.0) * 100.0


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
            "options_fetched_at",
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
        risk_pct_raw = ((entry - stop) / entry * 100.0) if entry > 0 else 0.0
        atr_stop_floor_pct = max(0.8 * d1_atr_pct, 0.75 if d1_atr_pct > 0 else 1.2)
        risk_pct_effective = max(0.1, risk_pct_raw, atr_stop_floor_pct)
        r_multiple_raw = reward_pct / max(0.1, risk_pct_raw) if reward_pct > 0 else 0.0
        r_multiple = reward_pct / risk_pct_effective if reward_pct > 0 else 0.0
        r_multiple_capped = min(6.0, max(0.0, r_multiple))
        rr_outlier = bool(r_multiple_raw > 6.0 or (risk_pct_raw > 0 and risk_pct_raw < atr_stop_floor_pct * 0.85))
        rr_note = ""
        if rr_outlier:
            rr_note = "RR_OUTLIER_STOP_TROP_PROCHE_OU_TARGET_TROP_LOIN"

        funda_risk = safe_float(r.get("funda_risk", 50.0))
        spread_pct = safe_float(r.get("spreadPct", 0))
        slip_pct = safe_float(r.get("slippageProxyPct", 0))
        symbol_news_impact = safe_float(r.get("symbol_news_impact_7d", 0))
        macro_impact = safe_float(r.get("macro_impact_30d", 0))
        sector = str(r.get("sector", "")).strip()
        sector_weight = safe_float(sec_w_map.get(sector, 0.0))
        symbol_weight = safe_float(sym_w_map.get(symbol, 0.0))

        raw_days_to_earnings = pd.to_numeric(pd.Series([r.get("days_to_earnings", pd.NA)]), errors="coerce").iloc[0]
        next_earnings_ts = pd.to_datetime(r.get("next_earnings_date", pd.NA), errors="coerce", utc=True)
        now_utc = pd.Timestamp.now(tz="UTC")

        days_to_next_earnings = pd.NA
        days_since_last_earnings = pd.NA
        if pd.notna(next_earnings_ts):
            delta_days = (next_earnings_ts - now_utc).total_seconds() / 86400.0
            if delta_days >= -0.4:
                days_to_next_earnings = max(0.0, round(delta_days, 1))
            else:
                days_since_last_earnings = round(abs(delta_days), 1)
        elif pd.notna(raw_days_to_earnings):
            d = float(raw_days_to_earnings)
            if d >= 0:
                days_to_next_earnings = round(d, 1)
            else:
                days_since_last_earnings = round(abs(d), 1)

        if pd.notna(days_to_next_earnings):
            d = max(0.0, min(30.0, float(days_to_next_earnings)))
            # Linear scale: 0 day -> 95 risk, 30+ days -> 20 risk
            event_risk = 20.0 + ((30.0 - d) / 30.0) * 75.0
        elif pd.notna(days_since_last_earnings):
            # Post-earnings: event risk is usually lower than pre-earnings.
            d = max(0.0, min(30.0, float(days_since_last_earnings)))
            event_risk = min(45.0, 25.0 + d * 0.5)
        else:
            event_risk = 42.0

        vol_risk = min(100.0, max(0.0, d1_atr_pct * 20.0))
        liq_risk = min(100.0, max(0.0, spread_pct * 35.0 + slip_pct * 20.0))
        news_risk = min(100.0, max(0.0, max(0.0, -symbol_news_impact) * 8.0 + max(0.0, -macro_impact) * 3.0))
        concentration_risk = min(100.0, max(0.0, sector_weight * 1.3 + symbol_weight * 1.1))

        iv_atm = pd.to_numeric(pd.Series([r.get("iv_atm", pd.NA)]), errors="coerce").iloc[0]
        options_raw = str(r.get("options_ok", "")).strip().lower()
        options_ok = options_raw in ("1", "true", "yes", "y")
        options_has_iv = options_ok and pd.notna(iv_atm) and float(iv_atm) > 0
        options_error_text = str(r.get("options_error", "") or "").strip()
        options_warning_text = str(r.get("options_warning", "") or "").strip()
        options_state_text = f"{options_error_text} {options_warning_text}".lower()
        invalid_options_state = any(
            token in options_state_text
            for token in ("_global.json.tmp", "global.json.tmp", "/data/state/", "invalid_options_state")
        )
        options_missing_known = any(
            token in options_state_text for token in ("no_expirations_available", "skipped_recent_no_expirations")
        )
        options_fetched_ts = pd.to_datetime(r.get("options_fetched_at", pd.NA), errors="coerce", utc=True)
        options_age_h = pd.NA
        if pd.notna(options_fetched_ts):
            options_age_h = (now_utc - options_fetched_ts).total_seconds() / 3600.0

        if options_has_iv:
            iv_val = float(iv_atm)
            iv_as_pct = iv_val * 100.0 if iv_val <= 3 else iv_val
            options_risk = min(100.0, max(0.0, iv_as_pct * 1.8))
        else:
            # Keep a neutral baseline when options are missing (common for FR symbols).
            options_risk = 35.0

        if invalid_options_state:
            options_coverage_quality = 0.0
        elif options_has_iv:
            options_coverage_quality = 100.0
        elif options_ok:
            options_coverage_quality = 70.0
        elif options_missing_known:
            options_coverage_quality = 35.0
        else:
            options_coverage_quality = 20.0

        if pd.notna(options_age_h):
            if float(options_age_h) <= 48:
                options_freshness_quality = 100.0
            elif float(options_age_h) <= 120:
                options_freshness_quality = 70.0
            else:
                options_freshness_quality = 40.0
        else:
            options_freshness_quality = 45.0

        yf_age_h = pd.to_numeric(pd.Series([r.get("yf_age_h", pd.NA)]), errors="coerce").iloc[0]
        stale_penalty = 0.0
        if pd.notna(yf_age_h):
            if float(yf_age_h) > 72:
                stale_penalty = 12.0
            elif float(yf_age_h) > 36:
                stale_penalty = 6.0
            elif float(yf_age_h) > 24:
                stale_penalty = 3.0

        calendar_raw = str(r.get("calendar_ok", "")).strip().lower()
        calendar_ok = calendar_raw in ("1", "true", "yes", "y")
        has_quote = quote_px > 0
        has_days_to_next_earnings = pd.notna(days_to_next_earnings)

        if pd.notna(yf_age_h):
            if float(yf_age_h) <= 24:
                freshness_quality = 100.0
            elif float(yf_age_h) <= 48:
                freshness_quality = 85.0
            elif float(yf_age_h) <= 72:
                freshness_quality = 70.0
            elif float(yf_age_h) <= 120:
                freshness_quality = 50.0
            else:
                freshness_quality = 30.0
        else:
            freshness_quality = 50.0

        core_fields = [
            r.get("tech_action", None),
            r.get("tech_confidence", None),
            r.get("funda_score", None),
            r.get("funda_risk", None),
            r.get("funda_upside", None),
            r.get("symbol_news_impact_7d", None),
            r.get("macro_impact_30d", None),
            r.get("d1_atr_pct", None),
            r.get("d1_dist_res_pct", None),
            r.get("d1_dist_sup_pct", None),
        ]
        core_present = 0
        for fv in core_fields:
            if fv is None:
                continue
            if isinstance(fv, str) and not fv.strip():
                continue
            if pd.isna(fv):
                continue
            core_present += 1
        feature_quality = (core_present / len(core_fields)) * 100.0 if core_fields else 50.0

        earnings_quality = 100.0 if has_days_to_next_earnings else (65.0 if calendar_ok else 35.0)
        data_quality_score = (
            0.20 * (100.0 if has_quote else 30.0)
            + 0.25 * options_coverage_quality
            + 0.10 * options_freshness_quality
            + 0.15 * earnings_quality
            + 0.15 * freshness_quality
            + 0.15 * feature_quality
        )
        if invalid_options_state:
            data_quality_score = 0.0
        data_quality_score = max(0.0, min(100.0, data_quality_score))

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

        reward_r = min(100.0, max(0.0, r_multiple_capped * 35.0))
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
        ev_r = (p_win * max(0.0, r_multiple_capped)) - (1.0 - p_win)

        risk_1_5 = _score_to_1_5(risk_score_100)
        reward_1_5 = _score_to_1_5(reward_score_100)
        risk_u = _score_unit(risk_score_100)
        reward_u = _score_unit(reward_score_100)
        # Final grade/action are set after full-universe quantiles and dynamic thresholds.
        grade = _grade_from_prob(prob_score)
        quadrant = "Q?"
        matrix_action = "Surveiller"

        data_quality_gate_ok = data_quality_score >= 60.0
        earnings_gate_block = bool(pd.notna(days_to_next_earnings) and float(days_to_next_earnings) <= 7.0)
        liquidity_gate_block = liq_risk >= 85.0
        invalid_options_state_gate = invalid_options_state
        gates_note = []
        if not data_quality_gate_ok:
            gates_note.append("DATA_QUALITY_LOW")
        if earnings_gate_block:
            gates_note.append("EARNINGS_IMMINENT")
        if liquidity_gate_block:
            gates_note.append("LIQUIDITY_STRESS")
        if invalid_options_state_gate:
            gates_note.append("INVALID_OPTIONS_STATE")
        if rr_outlier:
            gates_note.append(rr_note)
        gate_summary = "|".join(gates_note)

        options_note = ""
        if invalid_options_state:
            options_note = "INVALID_OPTIONS_STATE"
        elif not options_ok:
            options_note = options_error_text or options_warning_text or "Aucune option disponible"

        rows.append(
            {
                **r.to_dict(),
                "regularMarketPrice": quote_px if quote_px > 0 else pd.NA,
                "spreadPct": spread_pct if spread_pct > 0 else pd.NA,
                "slippageProxyPct": slip_pct if slip_pct > 0 else pd.NA,
                "days_to_earnings": float(days_to_next_earnings) if pd.notna(days_to_next_earnings) else pd.NA,
                "days_to_next_earnings": float(days_to_next_earnings) if pd.notna(days_to_next_earnings) else pd.NA,
                "days_since_last_earnings": float(days_since_last_earnings) if pd.notna(days_since_last_earnings) else pd.NA,
                "iv_atm": float(iv_atm) if pd.notna(iv_atm) else pd.NA,
                "options_ok": options_ok,
                "options_error": options_error_text,
                "options_warning": options_warning_text,
                "options_age_h": float(options_age_h) if pd.notna(options_age_h) else pd.NA,
                "options_coverage_quality": options_coverage_quality,
                "options_freshness_quality": options_freshness_quality,
                "invalid_options_state": invalid_options_state,
                "has_enrichment": bool(pd.notna(r.get("yf_fetched_at"))) if "yf_fetched_at" in r.index else False,
                "entry_price": entry,
                "stop_price": stop,
                "tp_price": tp,
                "reward_pct": reward_pct,
                "risk_pct": risk_pct_effective,
                "risk_pct_raw": risk_pct_raw,
                "atr_stop_floor_pct": atr_stop_floor_pct,
                "r_multiple_raw": r_multiple_raw,
                "r_multiple": r_multiple_capped,
                "rr_outlier": rr_outlier,
                "rr_note": rr_note,
                "risk_score_100": risk_score_100,
                "reward_score_100": reward_score_100,
                "risk_score_u": risk_u,
                "reward_score_u": reward_u,
                "risk_score_plot": max(0.0, min(100.0, risk_u + _stable_jitter(symbol, "risk"))),
                "reward_score_plot": max(0.0, min(100.0, reward_u + _stable_jitter(symbol, "reward"))),
                "reward_component_r": reward_r,
                "reward_component_upside": reward_upside,
                "reward_component_space": reward_space,
                "reward_component_catalyst": reward_catalyst,
                "reward_component_trend": trend_bonus,
                "prob_score": prob_score,
                "prob_score_base": prob_score,
                "prob_score_for_grade": (0.85 * prob_score + 0.15 * data_quality_score),
                "prob_component_tech": tech_prob,
                "prob_component_funda": funda_prob,
                "prob_component_sentiment": sentiment_prob,
                "prob_component_regime": (50.0 + regime_adj + align_adj),
                "p_win": p_win,
                "ev_r": ev_r,
                "risk_score_1_5": risk_1_5,
                "reward_score_1_5": reward_1_5,
                "setup_grade": grade,
                "matrix_action": matrix_action,
                "quadrant": quadrant,
                "data_quality_score": data_quality_score,
                "data_quality_gate_ok": data_quality_gate_ok,
                "earnings_gate_block": earnings_gate_block,
                "liquidity_gate_block": liquidity_gate_block,
                "invalid_options_state_gate": invalid_options_state_gate,
                "gate_summary": gate_summary,
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

    risk_scores = safe_float_series(out_df.get("risk_score_u", pd.Series(50.0, index=out_df.index))).fillna(50.0)
    reward_scores = safe_float_series(out_df.get("reward_score_u", pd.Series(50.0, index=out_df.index))).fillna(50.0)
    risk_threshold = int(round(float(risk_scores.quantile(0.60)))) if len(risk_scores) else 50
    reward_threshold = int(round(float(reward_scores.quantile(0.60)))) if len(reward_scores) else 50
    risk_threshold = max(20, min(85, risk_threshold))
    reward_threshold = max(20, min(85, reward_threshold))

    grade_scores = safe_float_series(out_df.get("prob_score_for_grade", out_df.get("prob_score", pd.Series(50.0, index=out_df.index)))).fillna(50.0)
    grade_a_thr = float(grade_scores.quantile(0.90)) if len(grade_scores) else 75.0
    grade_b_thr = float(grade_scores.quantile(0.50)) if len(grade_scores) else 55.0

    def _downgrade_grade(g: str) -> str:
        if g == "A":
            return "B"
        if g == "B":
            return "C"
        return "C"

    final_grades = []
    final_quadrants = []
    final_actions = []
    final_action_reason = []
    final_size_pct = []

    for _, row in out_df.iterrows():
        score_g = safe_float(row.get("prob_score_for_grade", row.get("prob_score", 50.0)))
        grade = "A" if score_g >= grade_a_thr else ("B" if score_g >= grade_b_thr else "C")
        if safe_float(row.get("data_quality_score", 50.0)) < 45.0:
            grade = _downgrade_grade(grade)
        if bool(row.get("rr_outlier", False)) and grade == "A":
            grade = "B"
        if bool(row.get("earnings_gate_block", False)) and grade == "A":
            grade = "B"
        if bool(row.get("invalid_options_state_gate", False)):
            grade = _downgrade_grade(grade)

        risk_u = safe_float(row.get("risk_score_u", 50.0))
        reward_u = safe_float(row.get("reward_score_u", 50.0))
        ev_r = safe_float(row.get("ev_r", 0.0))
        data_quality = safe_float(row.get("data_quality_score", 50.0))
        rr_outlier = bool(row.get("rr_outlier", False))
        earnings_block = bool(row.get("earnings_gate_block", False))
        liquidity_block = bool(row.get("liquidity_gate_block", False))
        invalid_options_state = bool(row.get("invalid_options_state_gate", False))
        quality_block = data_quality < 60.0

        if risk_u <= risk_threshold and reward_u >= reward_threshold:
            quadrant = "Q1 - Priorite"
        elif risk_u > risk_threshold and reward_u >= reward_threshold:
            quadrant = "Q2 - Speculatif"
        elif risk_u <= risk_threshold and reward_u < reward_threshold:
            quadrant = "Q3 - Defensif"
        else:
            quadrant = "Q4 - Sortie"

        enter_core = (
            ev_r >= 0.20
            and reward_u >= reward_threshold
            and risk_u <= risk_threshold
            and grade in ("A", "B")
        )
        reduce_core = (
            ev_r < 0.0
            or (risk_u >= min(95.0, risk_threshold + 18.0) and reward_u <= max(5.0, reward_threshold - 12.0))
            or (liquidity_block and ev_r < 0.15)
        )

        reasons = []
        if enter_core and not (quality_block or earnings_block or rr_outlier or invalid_options_state):
            action = "Entrer / Renforcer"
            reasons.append("SETUP_OK")
        elif reduce_core:
            action = "Reduire / Sortir"
            reasons.append("RISK_REWARD_UNFAVORABLE")
        else:
            action = "Surveiller"
            reasons.append("WAIT_CONFIRMATION")
            if quality_block:
                reasons.append("DATA_QUALITY_GATE")
            if earnings_block:
                reasons.append("EARNINGS_GATE")
            if rr_outlier:
                reasons.append("RR_OUTLIER_GATE")
            if invalid_options_state:
                reasons.append("INVALID_OPTIONS_STATE_GATE")

        ev_component = max(0.0, min(100.0, (ev_r / 1.5) * 100.0))
        risk_component = max(0.0, min(100.0, 100.0 - risk_u))
        size_score = 0.55 * ev_component + 0.25 * risk_component + 0.20 * data_quality
        if action == "Entrer / Renforcer":
            size_pct = max(10.0, min(100.0, size_score))
        elif action == "Surveiller":
            size_pct = max(0.0, min(50.0, size_score * 0.50))
        else:
            size_pct = 0.0
        if earnings_block:
            size_pct = min(size_pct, 30.0)
        if invalid_options_state:
            size_pct = min(size_pct, 20.0)

        final_grades.append(grade)
        final_quadrants.append(quadrant)
        final_actions.append(action)
        final_action_reason.append("|".join(reasons))
        final_size_pct.append(round(size_pct, 1))

    out_df["setup_grade"] = final_grades
    out_df["quadrant"] = final_quadrants
    out_df["matrix_action"] = final_actions
    out_df["action_reason"] = final_action_reason
    out_df["size_reco_pct"] = final_size_pct
    out_df["risk_threshold_dyn"] = risk_threshold
    out_df["reward_threshold_dyn"] = reward_threshold
    out_df["grade_a_threshold"] = grade_a_thr
    out_df["grade_b_threshold"] = grade_b_thr
    out_df["rr_used_for_scoring"] = safe_float_series(out_df.get("r_multiple", pd.Series(0.0, index=out_df.index))).fillna(0.0).clip(upper=6.0)

    action_rank = {"Entrer / Renforcer": 0, "Surveiller": 1, "Reduire / Sortir": 2}
    grade_rank = {"A": 0, "B": 1, "C": 2}
    out_df["action_rank"] = out_df["matrix_action"].map(action_rank).fillna(9)
    out_df["grade_rank"] = out_df["setup_grade"].map(grade_rank).fillna(9)
    out_df = out_df.sort_values(
        ["action_rank", "grade_rank", "ev_r", "reward_score_u", "risk_score_u"],
        ascending=[True, True, False, False, True],
        na_position="last",
    ).reset_index(drop=True)
    out_df = out_df.drop(columns=["action_rank", "grade_rank"], errors="ignore")
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
    cols = [
        "publishedat",
        "impactscore",
        "sectors_bullish",
        "sectors_bearish",
        "winners",
        "losers",
        "theme",
        "regime",
        "title",
        "snippet",
        "notes",
        "source",
        "action",
        "reason",
    ]
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

    bullish_col = _first_existing_column(wk, ["sectors_bullish", "winners"])
    bearish_col = _first_existing_column(wk, ["sectors_bearish", "losers"])
    wk["sectors_bullish"] = wk[bullish_col].fillna("").astype(str) if bullish_col else ""
    wk["sectors_bearish"] = wk[bearish_col].fillna("").astype(str) if bearish_col else ""
    # Legacy aliases kept for the rest of the dashboard code.
    wk["winners"] = wk["sectors_bullish"]
    wk["losers"] = wk["sectors_bearish"]

    for c in ["theme", "regime", "title", "snippet", "notes", "source", "action", "reason"]:
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


def _news_parse_listish(v: object) -> list[str]:
    if v is None:
        return []
    if isinstance(v, (list, tuple, set)):
        vals = [str(x).strip() for x in v]
        return [x for x in vals if x and x.lower() not in ("nan", "none", "nat")]
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "nat", "[]", "{}"):
        return []

    parsed = None
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(s)
                break
            except Exception:
                parsed = None
        if isinstance(parsed, dict):
            parsed = list(parsed.values())
        if isinstance(parsed, (list, tuple, set)):
            vals = [str(x).strip() for x in parsed]
            return [x for x in vals if x and x.lower() not in ("nan", "none", "nat")]

    # Fallback split on common separators
    parts = re.split(r"[|;,/]", s)
    if len(parts) <= 1 and " - " in s:
        parts = s.split(" - ")
    vals = [p.strip() for p in parts]
    return [x for x in vals if x and x.lower() not in ("nan", "none", "nat")]


def _news_to_numeric_0_100(v: object) -> float | None:
    n = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
    if pd.isna(n):
        return None
    x = float(n)
    # Common scales: [-1..1], [0..1], [0..10], [0..100]
    if -1.0 <= x <= 1.0:
        x *= 100.0
    elif 1.0 < abs(x) <= 10.0:
        x *= 10.0
    x = max(-100.0, min(100.0, x))
    return x


def _news_urgency_to_score(v: object) -> float | None:
    n = _news_to_numeric_0_100(v)
    if n is not None:
        return max(0.0, min(100.0, abs(n)))
    s = str(v or "").strip().lower()
    if not s:
        return None
    if any(k in s for k in ["critical", "urgent", "very_high", "very high", "haute", "high"]):
        return 90.0
    if any(k in s for k in ["medium", "moderate", "moyenne", "normal"]):
        return 55.0
    if any(k in s for k in ["low", "faible"]):
        return 25.0
    return None


def _news_confidence_to_score(v: object) -> float | None:
    n = _news_to_numeric_0_100(v)
    if n is not None:
        return max(0.0, min(100.0, abs(n)))
    s = str(v or "").strip().lower()
    if not s:
        return None
    if any(k in s for k in ["high", "forte", "strong"]):
        return 80.0
    if any(k in s for k in ["medium", "moderate", "moyenne"]):
        return 55.0
    if any(k in s for k in ["low", "faible", "weak"]):
        return 30.0
    return None


def _news_bool_or_none(v: object) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "ok", "relevant", "oui"):
        return True
    if s in ("0", "false", "no", "n", "non", "irrelevant"):
        return False
    return None


def _news_extract_symbols(raw_symbol: object, raw_symbols: object) -> list[str]:
    vals: list[str] = []
    if raw_symbol is not None:
        vals.extend(_news_parse_listish(raw_symbol))
        if not vals:
            s = str(raw_symbol).strip()
            if s:
                vals.append(s)
    vals.extend(_news_parse_listish(raw_symbols))
    clean = []
    for v in vals:
        vv = str(v).strip().upper()
        if not vv or vv in ("NAN", "NONE", "N/A"):
            continue
        if vv not in clean:
            clean.append(vv)
    return clean


def _news_infer_direction(
    *,
    scope: str,
    market_regime: object = None,
    sentiment: object = None,
    action: object = None,
    reason: object = None,
    impact_score: object = None,
) -> str:
    txt = " ".join(
        [
            str(market_regime or ""),
            str(sentiment or ""),
            str(action or ""),
            str(reason or ""),
        ]
    ).lower()
    if any(k in txt for k in ["risk-off", "risk off", "bear", "baiss", "negative", "négat", "negatif", "loser", "underweight", "reduce", "sortir"]):
        return "BEARISH"
    if any(k in txt for k in ["risk-on", "risk on", "bull", "hauss", "positive", "winner", "overweight", "renforcer", "acheter", "buy"]):
        return "BULLISH"
    imp = _news_to_numeric_0_100(impact_score)
    if imp is not None and imp < 0:
        return "BEARISH"
    if imp is not None and imp > 0 and str(scope).upper() == "MACRO":
        return "BULLISH"
    return "NEUTRAL"


def normalize_news_schema(df_news: pd.DataFrame, scope: str) -> pd.DataFrame:
    """
    Map AG4 macro / AG4-SPE raw tables to a common internal schema.
    Robust to schema drift: missing fields are filled with NA/empty values.
    """
    cols = [
        "run_id",
        "scope",
        "published_at",
        "ingested_at",
        "source",
        "url",
        "headline",
        "summary",
        "themes",
        "market_regime",
        "sector_tags",
        "symbols",
        "symbol_primary",
        "is_relevant",
        "urgency",
        "impact_score",
        "confidence_score",
        "direction",
        "type",
        "name",
        "raw_title",
        "raw_summary",
    ]
    if df_news is None or df_news.empty:
        return pd.DataFrame(columns=cols + ["priority_score", "priority_score_signed", "urgency_norm", "confidence_norm", "impact_abs"])

    wk = normalize_cols(df_news.copy())
    out = pd.DataFrame(index=wk.index)
    scope_norm = str(scope or "").strip().upper() or "MACRO"

    run_col = _first_existing_column(wk, ["run_id", "workflow_run_id"])
    pub_col = _first_existing_column(
        wk,
        ["published_at", "publishedat", "analyzed_at", "analyzedat", "last_seen_at", "lastseenat", "updated_at", "updatedat", "created_at", "fetched_at", "fetchedat"],
    )
    ing_col = _first_existing_column(wk, ["ingested_at", "ingestedat", "fetched_at", "fetchedat", "analyzed_at", "analyzedat", "updated_at", "updatedat", "created_at"])
    source_col = _first_existing_column(wk, ["source", "publisher", "domain", "feed"])
    url_col = _first_existing_column(wk, ["url", "link", "source_url", "sourceurl"])
    headline_col = _first_existing_column(wk, ["headline", "title"])
    summary_col = _first_existing_column(wk, ["summary", "snippet", "notes", "body_summary"])
    theme_col = _first_existing_column(wk, ["themes", "theme", "macro_themes"])
    regime_col = _first_existing_column(wk, ["market_regime", "regime"])
    sector_tags_col = _first_existing_column(wk, ["sector_tags", "affected_sectors", "sectors"])
    winners_col = _first_existing_column(wk, ["sectors_bullish", "winners"])
    losers_col = _first_existing_column(wk, ["sectors_bearish", "losers"])
    symbol_col = _first_existing_column(wk, ["symbol", "ticker"])
    symbols_col = _first_existing_column(wk, ["symbols", "tickers"])
    relevant_col = _first_existing_column(wk, ["is_relevant", "relevant", "isrelevant"])
    urgency_col = _first_existing_column(wk, ["urgency", "urgency_score", "urgencyscore", "priority"])
    impact_col = _first_existing_column(wk, ["impact_score", "impactscore", "impact"])
    conf_col = _first_existing_column(wk, ["confidence_score", "confidence", "confidencescore"])
    type_col = _first_existing_column(wk, ["type", "news_type", "event_type", "category"])
    name_col = _first_existing_column(wk, ["company_name", "companyname", "name"])
    sentiment_col = _first_existing_column(wk, ["sentiment", "bias"])
    action_col = _first_existing_column(wk, ["action", "recommended_action"])
    reason_col = _first_existing_column(wk, ["reason", "rationale"])

    out["run_id"] = wk[run_col].fillna("").astype(str) if run_col else ""
    out["scope"] = scope_norm
    out["published_at"] = pd.to_datetime(wk[pub_col], errors="coerce", utc=True) if pub_col else pd.NaT
    out["ingested_at"] = pd.to_datetime(wk[ing_col], errors="coerce", utc=True) if ing_col else pd.NaT
    out["source"] = wk[source_col].fillna("").astype(str) if source_col else ""
    out["url"] = wk[url_col].fillna("").astype(str) if url_col else ""
    out["raw_title"] = wk[headline_col].fillna("").astype(str) if headline_col else ""
    out["raw_summary"] = wk[summary_col].fillna("").astype(str) if summary_col else ""
    out["headline"] = out["raw_title"].where(out["raw_title"].astype(str).str.strip() != "", "—")
    out["summary"] = out["raw_summary"].where(out["raw_summary"].astype(str).str.strip() != "", "—")
    out["market_regime"] = wk[regime_col].fillna("").astype(str) if regime_col else ""
    out["type"] = wk[type_col].fillna("").astype(str) if type_col else ""
    out["name"] = wk[name_col].fillna("").astype(str) if name_col else ""

    theme_series = wk[theme_col] if theme_col else pd.Series("", index=wk.index)
    sec_tag_series = wk[sector_tags_col] if sector_tags_col else pd.Series("", index=wk.index)
    win_series = wk[winners_col] if winners_col else pd.Series("", index=wk.index)
    lose_series = wk[losers_col] if losers_col else pd.Series("", index=wk.index)
    symbol_series = wk[symbol_col] if symbol_col else pd.Series("", index=wk.index)
    symbols_series = wk[symbols_col] if symbols_col else pd.Series("", index=wk.index)

    themes_list = []
    sector_list = []
    bullish_sector_list = []
    bearish_sector_list = []
    symbols_list = []
    symbol_primary_list = []
    for i in wk.index:
        theme_vals = _news_parse_listish(theme_series.get(i, ""))
        if not theme_vals and str(theme_series.get(i, "")).strip():
            theme_vals = [str(theme_series.get(i)).strip()]
        win_vals = _news_parse_listish(win_series.get(i, ""))
        lose_vals = _news_parse_listish(lose_series.get(i, ""))
        base_sector_vals = _news_parse_listish(sec_tag_series.get(i, ""))
        all_sector_vals = []
        for s in base_sector_vals + win_vals + lose_vals:
            ss = str(s).strip()
            if ss and ss not in all_sector_vals:
                all_sector_vals.append(ss)
        syms = _news_extract_symbols(symbol_series.get(i, None), symbols_series.get(i, None))
        themes_list.append(theme_vals)
        bullish_sector_list.append(win_vals)
        bearish_sector_list.append(lose_vals)
        sector_list.append(all_sector_vals)
        symbols_list.append(syms)
        symbol_primary_list.append(syms[0] if syms else "")
    out["themes"] = themes_list
    out["sector_tags"] = sector_list
    out["sector_tags_bullish"] = bullish_sector_list
    out["sector_tags_bearish"] = bearish_sector_list
    out["symbols"] = symbols_list
    out["symbol_primary"] = symbol_primary_list

    if relevant_col:
        out["is_relevant"] = wk[relevant_col].map(_news_bool_or_none)
    else:
        out["is_relevant"] = pd.Series([None] * len(out), index=out.index, dtype=object)
    if scope_norm == "SPE":
        out["is_relevant"] = out["is_relevant"].where(out["is_relevant"].notna(), True)

    impact_series = wk[impact_col] if impact_col else pd.Series(pd.NA, index=wk.index)
    urgency_series = wk[urgency_col] if urgency_col else pd.Series(pd.NA, index=wk.index)
    conf_series = wk[conf_col] if conf_col else pd.Series(pd.NA, index=wk.index)
    sentiment_series = wk[sentiment_col] if sentiment_col else pd.Series("", index=wk.index)
    action_series = wk[action_col] if action_col else pd.Series("", index=wk.index)
    reason_series = wk[reason_col] if reason_col else pd.Series("", index=wk.index)

    out["impact_score"] = [(_news_to_numeric_0_100(impact_series.get(i, None))) for i in wk.index]
    out["urgency"] = [(_news_urgency_to_score(urgency_series.get(i, None))) for i in wk.index]
    out["confidence_score"] = [(_news_confidence_to_score(conf_series.get(i, None))) for i in wk.index]
    out["impact_score"] = pd.to_numeric(out["impact_score"], errors="coerce")
    out["urgency"] = pd.to_numeric(out["urgency"], errors="coerce")
    out["confidence_score"] = pd.to_numeric(out["confidence_score"], errors="coerce")

    out["direction"] = [
        _news_infer_direction(
            scope=scope_norm,
            market_regime=out.at[i, "market_regime"],
            sentiment=sentiment_series.get(i, None),
            action=action_series.get(i, None),
            reason=reason_series.get(i, None),
            impact_score=out.at[i, "impact_score"],
        )
        for i in out.index
    ]

    out["urgency_norm"] = (out["urgency"].fillna(50.0).clip(lower=0.0, upper=100.0) / 100.0)
    out["confidence_norm"] = (out["confidence_score"].fillna(60.0).clip(lower=0.0, upper=100.0) / 100.0)
    out["impact_abs"] = out["impact_score"].abs().fillna(0.0)
    out["priority_score"] = out["impact_abs"] * (0.5 + out["urgency_norm"]) * out["confidence_norm"]
    sign_map = out["direction"].map({"BULLISH": 1.0, "BEARISH": -1.0, "NEUTRAL": 0.0}).fillna(0.0)
    out["priority_score_signed"] = out["priority_score"] * sign_map

    if scope_norm == "SPE":
        out = out[out["symbol_primary"].astype(str).str.strip() != ""].copy()

    # Normalize text placeholders and ordering
    for c in ["source", "url", "headline", "summary", "market_regime", "type", "name"]:
        if c in out.columns:
            out[c] = out[c].fillna("").astype(str)
    out = out.sort_values("published_at", ascending=False, na_position="last")
    return out.reset_index(drop=True)


def _news_short_run_id(run_id: object) -> str:
    s = str(run_id or "").strip()
    if not s:
        return "—"
    return s if len(s) <= 24 else f"{s[:10]}...{s[-8:]}"


def _news_latest_run_snapshot(df_runs: pd.DataFrame, workflow: str) -> dict[str, object]:
    now_utc = pd.Timestamp.now(tz="UTC")
    out = {
        "workflow": workflow,
        "run_id": "",
        "status_raw": "NO_DATA",
        "status": "Aucune donnee",
        "started_at": pd.NaT,
        "finished_at": pd.NaT,
        "ref_ts": pd.NaT,
        "age_h": pd.NA,
        "raw": {},
    }
    if df_runs is None or df_runs.empty:
        return out
    wk = normalize_cols(df_runs.copy())
    ts_start_col = _first_existing_column(wk, ["started_at", "startedat", "created_at", "createdat"])
    ts_end_col = _first_existing_column(wk, ["finished_at", "finishedat", "updated_at", "updatedat"])
    if not ts_start_col:
        return out
    wk["started_at"] = pd.to_datetime(wk[ts_start_col], errors="coerce", utc=True)
    wk["finished_at"] = pd.to_datetime(wk[ts_end_col], errors="coerce", utc=True) if ts_end_col else pd.NaT
    wk["status_u"] = wk.get("status", pd.Series("", index=wk.index)).fillna("").astype(str).str.upper().str.strip()
    wk = wk.dropna(subset=["started_at"]).sort_values("started_at", ascending=False)
    if wk.empty:
        return out
    row = wk.iloc[0]
    ref_ts = row["finished_at"] if pd.notna(row["finished_at"]) else row["started_at"]
    out["run_id"] = str(row.get("run_id", "") or "")
    out["status_raw"] = str(row.get("status_u", "") or "UNKNOWN")
    out["status"] = out["status_raw"] if out["status_raw"] else "UNKNOWN"
    out["started_at"] = row["started_at"]
    out["finished_at"] = row["finished_at"]
    out["ref_ts"] = ref_ts
    out["age_h"] = round(float((now_utc - ref_ts).total_seconds() / 3600.0), 1) if pd.notna(ref_ts) else pd.NA
    try:
        out["raw"] = row.to_dict()
    except Exception:
        out["raw"] = {}
    return out


def _news_window_cutoff(window_key: str) -> pd.Timestamp | None:
    now_utc = pd.Timestamp.now(tz="UTC")
    mapping = {"24h": pd.Timedelta(hours=24), "7j": pd.Timedelta(days=7), "30j": pd.Timedelta(days=30)}
    delta = mapping.get(str(window_key), None)
    return (now_utc - delta) if delta is not None else None


def _news_filter_window(df: pd.DataFrame, window_key: str, ts_col: str = "published_at") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=df.columns if isinstance(df, pd.DataFrame) else [])
    cutoff = _news_window_cutoff(window_key)
    if cutoff is None or ts_col not in df.columns:
        return df.copy()
    ts = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    return df[ts >= cutoff].copy()


def _news_pill_html(label: str, tone: str = "neutral") -> str:
    tone_norm = str(tone or "neutral").lower()
    color_map = {
        "buy": "#16a34a",
        "bullish": "#16a34a",
        "ok": "#16a34a",
        "sell": "#dc2626",
        "bearish": "#dc2626",
        "error": "#dc2626",
        "warn": "#d97706",
        "warning": "#d97706",
        "info": "#2563eb",
        "neutral": "#6b7280",
        "risk-on": "#16a34a",
        "risk-off": "#dc2626",
    }
    bg = color_map.get(tone_norm, "#6b7280")
    return (
        f"<span style='display:inline-block;padding:3px 8px;border-radius:999px;"
        f"background:{bg};color:#fff;font-weight:700;font-size:0.75rem;'>{html.escape(str(label))}</span>"
    )


def _news_dedupe_clusters(df_news: pd.DataFrame) -> pd.DataFrame:
    if df_news is None or df_news.empty:
        return pd.DataFrame(columns=list(df_news.columns) + ["cluster_size", "cluster_urls", "dedupe_key"])
    wk = df_news.copy()
    wk["headline_norm"] = (
        wk.get("headline", pd.Series("", index=wk.index))
        .fillna("")
        .astype(str)
        .str.lower()
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    wk["source_norm"] = wk.get("source", pd.Series("", index=wk.index)).fillna("").astype(str).str.lower().str.strip()
    wk["url_norm"] = wk.get("url", pd.Series("", index=wk.index)).fillna("").astype(str).str.strip()
    wk["pub_day"] = pd.to_datetime(wk.get("published_at", pd.Series(pd.NaT, index=wk.index)), errors="coerce", utc=True).dt.strftime("%Y-%m-%d")
    wk["dedupe_key"] = wk["url_norm"]
    no_url = wk["dedupe_key"].astype(str).str.strip() == ""
    wk.loc[no_url, "dedupe_key"] = (
        wk.loc[no_url, "headline_norm"]
        + "|"
        + wk.loc[no_url, "source_norm"]
        + "|"
        + wk.loc[no_url, "pub_day"].fillna("")
    )
    wk = wk.sort_values("published_at", ascending=False, na_position="last")
    grp = wk.groupby("dedupe_key", dropna=False)
    first = grp.head(1).copy()
    counts = grp.size().rename("cluster_size")
    urls = grp["url_norm"].apply(lambda s: [u for u in s.dropna().astype(str).tolist() if u]).rename("cluster_urls")
    first = first.merge(counts, left_on="dedupe_key", right_index=True, how="left")
    first = first.merge(urls, left_on="dedupe_key", right_index=True, how="left")
    first["cluster_size"] = pd.to_numeric(first["cluster_size"], errors="coerce").fillna(1).astype(int)
    return first.sort_values("published_at", ascending=False, na_position="last").reset_index(drop=True)


def _news_priority_agg(series: pd.Series) -> float:
    x = pd.to_numeric(series, errors="coerce").fillna(0.0)
    return float(x.sum())


def _news_fmt_ts_paris(v: object) -> str:
    ts = pd.to_datetime(v, errors="coerce", utc=True)
    if pd.isna(ts):
        return "—"
    return ts.tz_convert("Europe/Paris").strftime("%Y-%m-%d %H:%M")


def _news_fmt_age_h(v: object) -> str:
    n = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
    if pd.isna(n):
        return "—"
    n = float(n)
    return f"{n:.1f}h" if n < 24 else f"{n/24.0:.1f}j"


def _news_fmt_pct(v: object, ndigits: int = 1) -> str:
    n = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
    if pd.isna(n):
        return "—"
    x = float(n)
    if abs(x) <= 1.0:
        x *= 100.0
    return f"{x:.{ndigits}f}%"


def _news_fmt_score(v: object, ndigits: int = 1) -> str:
    n = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
    if pd.isna(n):
        return "—"
    return f"{float(n):.{ndigits}f}"


def _news_pill_html(label: str, tone: str = "neutral") -> str:
    tone_norm = str(tone or "neutral").lower()
    color_map = {
        "ok": "#16a34a",
        "bullish": "#16a34a",
        "risk-on": "#16a34a",
        "warn": "#d97706",
        "warning": "#d97706",
        "error": "#dc2626",
        "bearish": "#dc2626",
        "risk-off": "#dc2626",
        "info": "#2563eb",
        "neutral": "#6b7280",
    }
    bg = color_map.get(tone_norm, "#6b7280")
    return (
        f"<span style='display:inline-block;padding:3px 8px;border-radius:999px;"
        f"background:{bg};color:#fff;font-weight:700;font-size:0.75rem;'>{html.escape(str(label))}</span>"
    )


def _news_scope_catalog_from_ag1() -> tuple[dict[str, list[str]], pd.DataFrame, str]:
    catalog: dict[str, list[str]] = {
        "Allocation active": [],
        "Tous portefeuilles": [],
        "Universe complet": [],
    }
    df_positions_active = pd.DataFrame()
    active_key = str(st.session_state.get("dashboard_active_portfolio") or "").strip()
    try:
        ag1_multi = load_ag1_multi_portfolios()
    except Exception:
        ag1_multi = {}

    available_keys = [
        k for k, p in ag1_multi.items()
        if isinstance(p, dict) and str(p.get("status", "")).lower() == "ok"
    ]
    if active_key not in available_keys:
        active_key = available_keys[0] if available_keys else ""

    all_syms: list[str] = []
    for key in available_keys:
        payload = ag1_multi.get(key, {})
        dfp = payload.get("df_portfolio", pd.DataFrame()) if isinstance(payload, dict) else pd.DataFrame()
        if not isinstance(dfp, pd.DataFrame) or dfp.empty:
            continue
        wk = normalize_cols(dfp.copy())
        if "symbol" not in wk.columns:
            continue
        wk["symbol"] = wk["symbol"].astype(str).str.strip().str.upper()
        wk = wk[~wk["symbol"].isin(["", "CASH_EUR", "__META__"])]
        syms = [s for s in wk["symbol"].dropna().tolist() if s and s not in all_syms]
        all_syms.extend(syms)
        if key == active_key:
            df_positions_active = wk.copy()
            catalog["Allocation active"] = syms
    catalog["Tous portefeuilles"] = all_syms
    return catalog, df_positions_active, active_key


def _render_macro_alert_card(row: pd.Series) -> None:
    headline = str(row.get("headline", "—") or "—").strip()
    source = str(row.get("source", "") or "Source inconnue").strip()
    ts_txt = _news_fmt_ts_paris(row.get("published_at"))
    summary = str(row.get("summary", "—") or "—").strip()
    url = str(row.get("url", "") or "").strip()
    direction = str(row.get("direction", "NEUTRAL") or "NEUTRAL").upper()
    direction_tone = "bullish" if direction == "BULLISH" else ("bearish" if direction == "BEARISH" else "neutral")
    urgency = pd.to_numeric(pd.Series([row.get("urgency", pd.NA)]), errors="coerce").iloc[0]
    impact = pd.to_numeric(pd.Series([row.get("impact_score", pd.NA)]), errors="coerce").iloc[0]
    conf = pd.to_numeric(pd.Series([row.get("confidence_score", pd.NA)]), errors="coerce").iloc[0]
    regime = str(row.get("market_regime", "") or "").strip()
    themes = row.get("themes", []) if isinstance(row.get("themes"), list) else []
    sectors = row.get("sector_tags", []) if isinstance(row.get("sector_tags"), list) else []
    bull_secs = row.get("sector_tags_bullish", []) if isinstance(row.get("sector_tags_bullish"), list) else []
    bear_secs = row.get("sector_tags_bearish", []) if isinstance(row.get("sector_tags_bearish"), list) else []

    with st.container(border=True):
        st.markdown(f"**{html.escape(headline)}**  \n{html.escape(source)} | {ts_txt}")
        if summary and summary != "—":
            st.caption(summary[:280] + ("..." if len(summary) > 280 else ""))
        pills = [
            _news_pill_html(f"Urg {_news_fmt_score(urgency, 0)}", "warn" if pd.notna(urgency) and float(urgency) >= 80 else "neutral"),
            _news_pill_html(f"Impact {_news_fmt_score(impact, 0)}", "info"),
            _news_pill_html(f"Conf {_news_fmt_score(conf, 0)}", "ok"),
            _news_pill_html(direction, direction_tone),
        ]
        if regime:
            regime_tone = "risk-on" if "on" in regime.lower() else ("risk-off" if "off" in regime.lower() else "neutral")
            pills.append(_news_pill_html(regime, regime_tone))
        st.markdown(" ".join(pills), unsafe_allow_html=True)
        tags = []
        if themes:
            tags.append("Themes: " + ", ".join([str(t) for t in themes[:4]]))
        if sectors:
            tags.append("Secteurs: " + ", ".join([str(s) for s in sectors[:4]]))
        if tags:
            st.caption(" | ".join(tags))
        with st.expander("Explain impact", expanded=False):
            posture = "Prudent" if direction == "BEARISH" or (pd.notna(urgency) and float(urgency) >= 80) else ("Offensif" if direction == "BULLISH" else "Neutre")
            st.write(
                f"Impact attendu: posture `{posture}`. "
                f"Secteurs favorises: {', '.join(bull_secs[:3]) if bull_secs else '—'}. "
                f"Secteurs sous pression: {', '.join(bear_secs[:3]) if bear_secs else '—'}. "
                f"Themes a surveiller: {', '.join([str(t) for t in themes[:3]]) if themes else '—'}."
            )
        if url:
            st.markdown(f"[Ouvrir la source]({url})")


def render_macro_alerts(df_macro: pd.DataFrame, *, key_prefix: str = "ag4_macro_alerts") -> None:
    with st.container(border=True):
        st.markdown("#### Macro Risk feed")
        st.caption("Tri actionnable = impact_score * (0.5 + urgency_norm) * (confidence/100)")
        if df_macro is None or df_macro.empty:
            st.info("Aucune news macro sur la periode.")
            return
        top_n = int(st.selectbox("Nombre d'alertes", [10, 15, 20], index=0, key=f"{key_prefix}_topn"))
        wk = df_macro.sort_values(["urgency", "priority_score", "published_at"], ascending=[False, False, False], na_position="last")
        for _, row in wk.head(top_n).iterrows():
            _render_macro_alert_card(row)


def render_macro_overview(df_macro: pd.DataFrame, df_macro_runs: pd.DataFrame, df_positions_optional: pd.DataFrame | None = None) -> None:
    if df_macro is None or df_macro.empty:
        st.info("Aucune donnee macro AG4 disponible.")
        return

    macro_window = st.radio("Fenetre macro", ["7j", "30j"], horizontal=True, key="ag4_macro_overview_window")
    df_macro_win = _news_filter_window(df_macro, macro_window)
    if df_macro_win.empty:
        st.info(f"Aucune news macro sur {macro_window}.")
        return

    run_meta = _news_latest_run_snapshot(df_macro_runs, "AG4 Macro")
    pub_all = pd.to_datetime(df_macro.get("published_at", pd.Series(pd.NaT, index=df_macro.index)), errors="coerce", utc=True)
    latest_pub = pub_all.dropna().max() if not pub_all.dropna().empty else pd.NaT
    age_h = (pd.Timestamp.now(tz="UTC") - latest_pub).total_seconds() / 3600.0 if pd.notna(latest_pub) else pd.NA
    cov_24h = len(_news_filter_window(df_macro, "24h"))
    cov_7d = len(_news_filter_window(df_macro, "7j"))
    status = "OK"
    reasons: list[str] = []
    if pd.isna(age_h):
        status = "WARN"
        reasons.append("published_at manquant")
    elif float(age_h) > 24.0:
        status = "WARN"
        reasons.append(f"freshness={_news_fmt_age_h(age_h)}")
    if cov_24h < 3 and cov_7d > 0:
        status = "WARN"
        reasons.append("coverage 24h faible")

    latest_row = df_macro.sort_values("published_at", ascending=False, na_position="last").iloc[0]
    regime = str(latest_row.get("market_regime", "") or "").strip() or "Neutral"
    regime_tone = "risk-on" if "on" in regime.lower() else ("risk-off" if "off" in regime.lower() else "neutral")
    conf = pd.to_numeric(pd.Series([latest_row.get("confidence_score", pd.NA)]), errors="coerce").iloc[0]
    alerts_24h = _news_filter_window(df_macro, "24h")
    high_urg_24h = int((pd.to_numeric(alerts_24h.get("urgency", pd.Series(pd.NA, index=alerts_24h.index)), errors="coerce").fillna(0.0) >= 80.0).sum()) if not alerts_24h.empty else 0

    theme_tokens = []
    for t in _news_filter_window(df_macro, "7j").get("themes", pd.Series(dtype=object)):
        if isinstance(t, list):
            theme_tokens.extend([str(x) for x in t if str(x).strip()])
    active_themes_7d = len(set(theme_tokens))
    macro_sent_net = _news_priority_agg(df_macro_win.get("priority_score_signed", pd.Series(dtype=float)))

    sec_rows = []
    for _, r in df_macro_win.iterrows():
        p = float(pd.to_numeric(pd.Series([r.get("priority_score", 0.0)]), errors="coerce").fillna(0.0).iloc[0])
        for s in (r.get("sector_tags_bullish", []) if isinstance(r.get("sector_tags_bullish"), list) else []):
            sec_rows.append({"sector": str(s), "direction": "Bullish", "score": p})
        for s in (r.get("sector_tags_bearish", []) if isinstance(r.get("sector_tags_bearish"), list) else []):
            sec_rows.append({"sector": str(s), "direction": "Bearish", "score": p})
    sec_df = pd.DataFrame(sec_rows)
    top_winner_txt = "—"
    top_loser_txt = "—"
    if not sec_df.empty:
        sec_agg = sec_df.groupby(["sector", "direction"], as_index=False)["score"].sum()
        bull = sec_agg[sec_agg["direction"] == "Bullish"].sort_values("score", ascending=False)
        bear = sec_agg[sec_agg["direction"] == "Bearish"].sort_values("score", ascending=False)
        if not bull.empty:
            top_winner_txt = f"{bull.iloc[0]['sector']} ({bull.iloc[0]['score']:.0f})"
        if not bear.empty:
            top_loser_txt = f"{bear.iloc[0]['sector']} ({bear.iloc[0]['score']:.0f})"

    with st.container(border=True):
        b1, b2, b3, b4 = st.columns([2.2, 1.0, 1.1, 1.7])
        b1.markdown(f"**Dernier run AG4**: `{_news_short_run_id(run_meta.get('run_id'))}` | {_news_fmt_ts_paris(run_meta.get('ref_ts'))}")
        b2.markdown(_news_pill_html(status, "ok" if status == "OK" else "warn"), unsafe_allow_html=True)
        b3.metric("Freshness", _news_fmt_age_h(age_h), delta_color="off")
        b4.markdown(f"**Coverage**: 24h={cov_24h} | 7j={cov_7d}  \n**Notes**: {', '.join(reasons[:2]) if reasons else 'RAS'}")

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.markdown("**Market regime**")
    k1.markdown(_news_pill_html(regime, regime_tone), unsafe_allow_html=True)
    k1.caption(f"Conf {_news_fmt_score(conf, 0)}")
    k2.metric("Macro sentiment net", _news_fmt_score(macro_sent_net, 0), delta_color="off")
    k3.metric("# alertes high urgency (24h)", high_urg_24h)
    k4.metric("# themes actifs (7j)", active_themes_7d)
    k5.metric("Winner sector", top_winner_txt, delta_color="off")
    k6.metric("Loser sector", top_loser_txt, delta_color="off")

    c1, c2, c3 = st.columns(3, gap="large")
    with c1:
        with st.container(border=True):
            st.markdown("#### Timeline regime")
            tl_window = st.radio("Fenetre", ["7j", "30j"], horizontal=True, key="ag4_macro_timeline_window")
            df_tl = _news_filter_window(df_macro, tl_window).copy()
            if df_tl.empty:
                st.info("Aucune donnee.")
            else:
                df_tl["day"] = pd.to_datetime(df_tl["published_at"], errors="coerce", utc=True).dt.floor("D")
                df_tl["regime_score"] = df_tl.get("market_regime", pd.Series("", index=df_tl.index)).astype(str).str.lower().map(
                    lambda s: 1 if "risk-on" in s or "risk on" in s else (-1 if "risk-off" in s or "risk off" in s else 0)
                )
                daily = df_tl.groupby("day", as_index=False).agg(
                    regime_score=("regime_score", "mean"),
                    articles=("day", "size"),
                )
                if daily.empty:
                    st.info("Aucune donnee.")
                else:
                    fig_tl = go.Figure()
                    fig_tl.add_trace(go.Bar(x=daily["day"], y=daily["articles"], name="#articles", marker_color="rgba(96,165,250,0.35)", yaxis="y2"))
                    fig_tl.add_trace(go.Scatter(x=daily["day"], y=daily["regime_score"], name="Regime", mode="lines+markers", line=dict(color="#eab308", width=2)))
                    fig_tl.update_layout(
                        height=260,
                        margin=dict(l=10, r=10, t=10, b=10),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        xaxis=dict(gridcolor="rgba(128,128,128,0.15)"),
                        yaxis=dict(title="RiskOff -1 / 0 / +1 RiskOn", range=[-1.2, 1.2], gridcolor="rgba(128,128,128,0.15)"),
                        yaxis2=dict(title="#", overlaying="y", side="right", showgrid=False),
                        legend=dict(orientation="h", y=1.08, x=0),
                    )
                    st.plotly_chart(fig_tl, use_container_width=True, config={"displayModeBar": False})
    with c2:
        with st.container(border=True):
            st.markdown("#### Top Themes")
            rows = []
            for _, r in df_macro_win.iterrows():
                themes = r.get("themes", []) if isinstance(r.get("themes"), list) else []
                for t in themes:
                    rows.append({"theme": str(t), "score": float(pd.to_numeric(pd.Series([r.get('priority_score', 0.0)]), errors='coerce').fillna(0.0).iloc[0]), "direction": str(r.get("direction", "NEUTRAL"))})
            th_df = pd.DataFrame(rows)
            if th_df.empty:
                st.info("Themes indisponibles.")
            else:
                agg = th_df.groupby("theme", as_index=False).agg(
                    theme_score=("score", "sum"),
                    bull=("direction", lambda s: int((pd.Series(s).astype(str).str.upper() == "BULLISH").sum())),
                    bear=("direction", lambda s: int((pd.Series(s).astype(str).str.upper() == "BEARISH").sum())),
                )
                agg["dir"] = agg.apply(lambda r: "Bullish" if r["bull"] > r["bear"] else ("Bearish" if r["bear"] > r["bull"] else "Neutral"), axis=1)
                agg = agg.sort_values("theme_score", ascending=False).head(8).sort_values("theme_score", ascending=True)
                show_legend = agg["dir"].nunique() > 1
                fig_th = px.bar(agg, x="theme_score", y="theme", orientation="h", color="dir", color_discrete_map={"Bullish": "#22c55e", "Bearish": "#ef4444", "Neutral": "#9ca3af"}, text="theme_score")
                fig_th.update_traces(texttemplate="%{text:.0f}", textposition="outside", cliponaxis=False)
                fig_th.update_layout(
                    height=260,
                    margin=dict(l=10, r=10, t=64, b=10),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    xaxis=dict(gridcolor="rgba(128,128,128,0.15)"),
                    yaxis=dict(title=None, automargin=True),
                    showlegend=show_legend,
                    legend=dict(
                        title=None,
                        orientation="h",
                        yanchor="bottom",
                        y=1.12,
                        xanchor="left",
                        x=0.0,
                        bgcolor="rgba(0,0,0,0)",
                    ),
                )
                fig_th.update_layout(legend_title_text=None)
                st.plotly_chart(fig_th, use_container_width=True, config={"displayModeBar": False})
    with c3:
        with st.container(border=True):
            st.markdown("#### Heatmap Secteur x Impact")
            if sec_df.empty:
                st.info("Secteurs winners/losers indisponibles.")
            else:
                sec_heat = sec_df.groupby(["sector", "direction"], as_index=False)["score"].sum()
                pv = sec_heat.pivot_table(index="sector", columns="direction", values="score", aggfunc="sum", fill_value=0.0)
                for col in ["Bullish", "Bearish"]:
                    if col not in pv.columns:
                        pv[col] = 0.0
                pv["total"] = pv["Bullish"] + pv["Bearish"]
                pv = pv.sort_values("total", ascending=False).head(10)
                z = pv[["Bullish", "Bearish"]].to_numpy()
                fig_sec = go.Figure(data=go.Heatmap(z=z, x=["Bullish", "Bearish"], y=pv.index.tolist(), text=[[f"{v:.0f}" for v in row] for row in z], texttemplate="%{text}", colorscale="Viridis"))
                fig_sec.update_layout(height=260, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", xaxis=dict(title=None), yaxis=dict(title=None, automargin=True))
                st.plotly_chart(fig_sec, use_container_width=True, config={"displayModeBar": False})

    render_macro_alerts(df_macro_win, key_prefix="ag4_macro_alerts_feed")

    with st.container(border=True):
        st.markdown("#### So what for portfolio?")
        posture = "Neutre"
        if ("risk-off" in regime.lower() or "risk off" in regime.lower()) or high_urg_24h >= 3:
            posture = "Prudent"
        elif ("risk-on" in regime.lower() or "risk on" in regime.lower()) and high_urg_24h <= 1:
            posture = "Offensif"
        posture_tone = "warn" if posture == "Prudent" else ("ok" if posture == "Offensif" else "neutral")
        s1, s2, s3 = st.columns(3)
        s1.markdown(_news_pill_html(f"Posture: {posture}", posture_tone), unsafe_allow_html=True)
        over_txt = "—"
        under_txt = "—"
        if not sec_df.empty:
            sec_agg = sec_df.groupby(["sector", "direction"], as_index=False)["score"].sum()
            bull = sec_agg[sec_agg["direction"] == "Bullish"].sort_values("score", ascending=False)
            bear = sec_agg[sec_agg["direction"] == "Bearish"].sort_values("score", ascending=False)
            over_txt = ", ".join(bull["sector"].head(3).tolist()) if not bull.empty else "—"
            under_txt = ", ".join(bear["sector"].head(3).tolist()) if not bear.empty else "—"
        theme_risks = []
        for _, r in df_macro_win.iterrows():
            if str(r.get("direction", "NEUTRAL")).upper() not in ("BEARISH", "NEUTRAL"):
                continue
            for t in (r.get("themes", []) if isinstance(r.get("themes"), list) else []):
                theme_risks.append({"theme": str(t), "score": float(pd.to_numeric(pd.Series([r.get("priority_score", 0.0)]), errors="coerce").fillna(0.0).iloc[0])})
        risk_txt = "—"
        if theme_risks:
            th = pd.DataFrame(theme_risks).groupby("theme", as_index=False)["score"].sum().sort_values("score", ascending=False)
            risk_txt = ", ".join(th["theme"].head(4).tolist()) if not th.empty else "—"
        s2.markdown(f"**Surponderer**: {over_txt}  \n**Sous-ponderer**: {under_txt}")
        s3.markdown(f"**Risques a surveiller**: {risk_txt}")

        if isinstance(df_positions_optional, pd.DataFrame) and not df_positions_optional.empty:
            pos = normalize_cols(df_positions_optional.copy())
            if "sector" in pos.columns and "marketvalue" in pos.columns:
                pos["sector"] = pos["sector"].fillna("").astype(str).str.strip()
                pos["marketvalue"] = pd.to_numeric(pos["marketvalue"], errors="coerce").fillna(0.0)
                pos = pos[(pos["sector"] != "") & (pos["marketvalue"] > 0)]
                if not pos.empty:
                    total_mv = float(pos["marketvalue"].sum()) or 1.0
                    sec_w = (pos.groupby("sector")["marketvalue"].sum() / total_mv * 100.0).sort_values(ascending=False)
                    st.caption("Impact portefeuille (secteurs exposés): " + ", ".join([f"{sec} {w:.1f}%" for sec, w in sec_w.head(5).items()]))


def render_symbol_news(
    df_spe: pd.DataFrame,
    *,
    scope_catalog: dict[str, list[str]] | None = None,
    df_universe_optional: pd.DataFrame | None = None,
    df_positions_active: pd.DataFrame | None = None,
) -> None:
    if df_spe is None or df_spe.empty:
        st.info("Aucune news par valeur AG4-SPE disponible.")
        return

    scope_catalog = dict(scope_catalog or {})
    if "Universe complet" not in scope_catalog:
        scope_catalog["Universe complet"] = []

    spe_all = df_spe.copy()
    spe_all["symbol_primary"] = spe_all.get("symbol_primary", pd.Series("", index=spe_all.index)).fillna("").astype(str).str.upper()

    base_symbols = set([s for s in spe_all["symbol_primary"].dropna().tolist() if s])
    if isinstance(df_universe_optional, pd.DataFrame) and not df_universe_optional.empty:
        un = normalize_cols(df_universe_optional.copy())
        if "symbol" in un.columns:
            base_symbols.update(un["symbol"].dropna().astype(str).str.strip().str.upper().tolist())
    scope_catalog["Universe complet"] = sorted([s for s in base_symbols if s])

    c1, c2, c3, c4 = st.columns([1.1, 1.6, 2.4, 2.1])
    window_key = c1.radio("Fenetre", ["24h", "7j", "30j"], key="ag4_spe_window")
    scope_name = c2.selectbox("Scope", list(scope_catalog.keys()), key="ag4_spe_scope")
    symbol_options = sorted([s for s in spe_all["symbol_primary"].dropna().unique().tolist() if s])
    selected_symbols = c3.multiselect("Filtre symbol", symbol_options, key="ag4_spe_symbol_filter")
    c4.write("")
    holdings_only = c4.toggle("Holdings only", value=False, key="ag4_spe_holdings_only")
    high_urg_only = c4.toggle("High urgency only", value=False, key="ag4_spe_high_urg_only")
    relevant_only = c4.toggle("Relevant only", value=False, key="ag4_spe_relevant_only")

    scope_symbols = set([str(s).upper() for s in scope_catalog.get(scope_name, []) if str(s).strip()])

    spe_window = _news_filter_window(spe_all, window_key)
    if (scope_name != "Universe complet" or holdings_only) and scope_symbols:
        spe_window = spe_window[spe_window["symbol_primary"].isin(scope_symbols)]
    if selected_symbols:
        spe_window = spe_window[spe_window["symbol_primary"].isin([str(s).upper() for s in selected_symbols])]
    if high_urg_only:
        spe_window = spe_window[pd.to_numeric(spe_window.get("urgency", pd.Series(pd.NA, index=spe_window.index)), errors="coerce").fillna(0.0) >= 80.0]
    if relevant_only:
        rel = spe_window.get("is_relevant", pd.Series(True, index=spe_window.index))
        spe_window = spe_window[rel.fillna(False) == True]  # noqa: E712

    events_count = len(spe_window)
    high_urg_count = int((pd.to_numeric(spe_window.get("urgency", pd.Series(pd.NA, index=spe_window.index)), errors="coerce").fillna(0.0) >= 80.0).sum()) if not spe_window.empty else 0
    relevant_count = int((spe_window.get("is_relevant", pd.Series(False, index=spe_window.index)).fillna(False) == True).sum()) if not spe_window.empty else 0  # noqa: E712
    risk_7d_df = _news_filter_window(spe_window, "7j")
    risk_30d_df = _news_filter_window(spe_window, "30j")
    headline_risk_7d = float(pd.to_numeric(risk_7d_df.get("priority_score", pd.Series(0.0, index=risk_7d_df.index)), errors="coerce").fillna(0.0).sum()) if not risk_7d_df.empty else 0.0
    headline_risk_30d = float(pd.to_numeric(risk_30d_df.get("priority_score", pd.Series(0.0, index=risk_30d_df.index)), errors="coerce").fillna(0.0).sum()) if not risk_30d_df.empty else 0.0
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("# evenements (scope)", events_count)
    k2.metric("# high urgency", high_urg_count)
    k3.metric("# relevant", relevant_count)
    k4.metric("Headline risk score", f"{headline_risk_7d:.0f}", delta=f"30j {headline_risk_30d:.0f}", delta_color="off")
    st.caption("Headline risk = somme(priority_score) ; priority_score = impact_score * (0.5 + urgency_norm) * (confidence/100)")

    base_for_scoreboard = spe_all.copy()
    if (scope_name != "Universe complet" or holdings_only) and scope_symbols:
        base_for_scoreboard = base_for_scoreboard[base_for_scoreboard["symbol_primary"].isin(scope_symbols)]
    if selected_symbols:
        base_for_scoreboard = base_for_scoreboard[base_for_scoreboard["symbol_primary"].isin([str(s).upper() for s in selected_symbols])]
    if high_urg_only:
        base_for_scoreboard = base_for_scoreboard[pd.to_numeric(base_for_scoreboard.get("urgency", pd.Series(pd.NA, index=base_for_scoreboard.index)), errors="coerce").fillna(0.0) >= 80.0]
    if relevant_only:
        base_for_scoreboard = base_for_scoreboard[base_for_scoreboard.get("is_relevant", pd.Series(True, index=base_for_scoreboard.index)).fillna(False) == True]  # noqa: E712

    if base_for_scoreboard.empty:
        st.info("Aucune news symbole dans le scope courant.")
        return

    def _sym_score_agg(df_src: pd.DataFrame, suffix: str) -> pd.DataFrame:
        if df_src is None or df_src.empty:
            return pd.DataFrame(columns=["symbol_primary"])
        wk = df_src.copy()
        wk["published_at"] = pd.to_datetime(wk.get("published_at", pd.Series(pd.NaT, index=wk.index)), errors="coerce", utc=True)
        grp = wk.groupby("symbol_primary", as_index=False).agg(
            **{
                f"headline_risk_{suffix}": ("priority_score", "sum"),
                f"urgency_max_{suffix}": ("urgency", "max"),
                f"impact_max_{suffix}": ("impact_score", lambda s: pd.to_numeric(s, errors="coerce").abs().max()),
                f"confidence_mean_{suffix}": ("confidence_score", "mean"),
                f"last_event_at_{suffix}": ("published_at", "max"),
                f"events_{suffix}": ("symbol_primary", "size"),
            }
        )
        return grp

    sb7 = _sym_score_agg(_news_filter_window(base_for_scoreboard, "7j"), "7d")
    sb30 = _sym_score_agg(_news_filter_window(base_for_scoreboard, "30j"), "30d")
    sb = sb30.merge(sb7, on="symbol_primary", how="outer")
    latest_evt = (
        base_for_scoreboard.sort_values("published_at", ascending=False, na_position="last")
        .drop_duplicates(subset=["symbol_primary"], keep="first")
        [[c for c in ["symbol_primary", "headline", "published_at", "urgency", "impact_score", "confidence_score", "name"] if c in base_for_scoreboard.columns]]
        .rename(columns={"headline": "last_headline", "published_at": "last_event_at", "name": "name_latest"})
    )
    sb = sb.merge(latest_evt, on="symbol_primary", how="left")

    if isinstance(df_universe_optional, pd.DataFrame) and not df_universe_optional.empty:
        un = normalize_cols(df_universe_optional.copy())
        if "symbol" in un.columns:
            un["symbol"] = un["symbol"].astype(str).str.strip().str.upper()
            if "name" not in un.columns:
                un["name"] = ""
            if "sector" not in un.columns:
                un["sector"] = ""
            sb = sb.merge(un[["symbol", "name", "sector"]].drop_duplicates("symbol"), left_on="symbol_primary", right_on="symbol", how="left")
            sb.drop(columns=["symbol"], inplace=True, errors="ignore")
    if "name" not in sb.columns:
        sb["name"] = sb.get("name_latest", pd.Series("", index=sb.index))
    if "sector" not in sb.columns:
        sb["sector"] = ""

    for c in ["headline_risk_7d", "headline_risk_30d", "urgency_max_7d", "impact_max_7d", "confidence_mean_7d"]:
        if c not in sb.columns:
            sb[c] = pd.NA
    sb["breaking_flag"] = pd.to_numeric(sb.get("urgency_max_7d", pd.Series(pd.NA, index=sb.index)), errors="coerce").fillna(0.0) >= 90.0
    sb = sb.sort_values(["headline_risk_7d", "headline_risk_30d"], ascending=[False, False], na_position="last")

    st.markdown("#### Scoreboard par valeur")
    sb_show = pd.DataFrame(
        {
            "Symbol": sb["symbol_primary"],
            "Name": sb.get("name", pd.Series("", index=sb.index)).fillna("").astype(str),
            "Sector": sb.get("sector", pd.Series("", index=sb.index)).fillna("").astype(str),
            "HeadlineRisk 7d": pd.to_numeric(sb.get("headline_risk_7d", pd.Series(pd.NA, index=sb.index)), errors="coerce").round(1),
            "HeadlineRisk 30d": pd.to_numeric(sb.get("headline_risk_30d", pd.Series(pd.NA, index=sb.index)), errors="coerce").round(1),
            "Last event": pd.to_datetime(sb.get("last_event_at", pd.Series(pd.NaT, index=sb.index)), errors="coerce", utc=True).apply(lambda x: x.tz_convert("Europe/Paris").strftime("%m-%d %H:%M") if pd.notna(x) else "—"),
            "Urgency max 7d": pd.to_numeric(sb.get("urgency_max_7d", pd.Series(pd.NA, index=sb.index)), errors="coerce").round(0),
            "Impact max 7d": pd.to_numeric(sb.get("impact_max_7d", pd.Series(pd.NA, index=sb.index)), errors="coerce").round(0),
            "Confidence moyen 7d": pd.to_numeric(sb.get("confidence_mean_7d", pd.Series(pd.NA, index=sb.index)), errors="coerce").round(0),
            "Flag Breaking": sb.get("breaking_flag", pd.Series(False, index=sb.index)).map(lambda x: "YES" if bool(x) else "—"),
        }
    )
    st.dataframe(sb_show.head(120), use_container_width=True, hide_index=True, height=360)

    symbol_choices = [s for s in sb["symbol_primary"].dropna().astype(str).tolist() if s]
    if not symbol_choices:
        st.info("Aucun symbole pour le detail.")
        return
    selected_symbol = st.selectbox("Detail valeur", symbol_choices, index=0, key="ag4_spe_selected_symbol")

    detail_df = base_for_scoreboard[base_for_scoreboard["symbol_primary"] == str(selected_symbol).upper()].copy()
    if detail_df.empty:
        st.info("Aucune news pour ce symbole.")
        return
    detail_df = detail_df.sort_values("published_at", ascending=False, na_position="last")
    detail_clusters = _news_dedupe_clusters(detail_df)

    left, right = st.columns([1.1, 1.9], gap="large")
    with left:
        with st.container(border=True):
            st.markdown(f"#### Timeline — {selected_symbol}")
            tl = detail_df.copy()
            tl["published_at"] = pd.to_datetime(tl["published_at"], errors="coerce", utc=True)
            tl["priority_score"] = pd.to_numeric(tl.get("priority_score", pd.Series(0.0, index=tl.index)), errors="coerce").fillna(0.0)
            tl["urgency"] = pd.to_numeric(tl.get("urgency", pd.Series(pd.NA, index=tl.index)), errors="coerce")
            if tl["published_at"].dropna().empty:
                st.info("Pas de timeline disponible.")
            else:
                tl["direction_plot"] = tl.get("direction", pd.Series("NEUTRAL", index=tl.index)).astype(str)
                fig = px.scatter(
                    tl,
                    x="published_at",
                    y="priority_score",
                    color="direction_plot",
                    size="urgency" if tl["urgency"].notna().any() else None,
                    hover_data={"headline": True, "priority_score": ":.1f", "urgency": ":.0f", "confidence_score": ":.0f"},
                    color_discrete_map={"BULLISH": "#22c55e", "BEARISH": "#ef4444", "NEUTRAL": "#9ca3af"},
                    labels={"direction_plot": "Direction"},
                )
                fig.update_layout(height=340, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", xaxis=dict(gridcolor="rgba(128,128,128,0.15)"), yaxis=dict(gridcolor="rgba(128,128,128,0.15)"), legend=dict(orientation="h", y=1.08, x=0))
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    with right:
        with st.container(border=True):
            st.markdown("#### Liste news (dédoublonnée)")
            st.caption("Déduplication par URL sinon (headline normalisé + source + jour), regroupée en clusters.")
            max_items = int(st.selectbox("Nb items", [10, 20, 30], index=1, key="ag4_spe_detail_nb_items"))
            if detail_clusters.empty:
                st.info("Aucune news.")
            else:
                for _, r in detail_clusters.head(max_items).iterrows():
                    cluster_n = int(pd.to_numeric(pd.Series([r.get("cluster_size", 1)]), errors="coerce").fillna(1).iloc[0])
                    title = str(r.get("headline", "—") or "—")
                    st.markdown(f"**{html.escape(title)}**" + (f" _( +{cluster_n-1} similaires )_" if cluster_n > 1 else ""))
                    st.caption(f"{_news_fmt_ts_paris(r.get('published_at'))} | {str(r.get('source', '') or 'Source inconnue')}")
                    summary = str(r.get("summary", "—") or "—")
                    if summary and summary != "—":
                        st.write(summary[:260] + ("..." if len(summary) > 260 else ""))
                    urg = pd.to_numeric(pd.Series([r.get("urgency", pd.NA)]), errors="coerce").iloc[0]
                    imp = pd.to_numeric(pd.Series([r.get("impact_score", pd.NA)]), errors="coerce").iloc[0]
                    conf = pd.to_numeric(pd.Series([r.get("confidence_score", pd.NA)]), errors="coerce").iloc[0]
                    rel = r.get("is_relevant", None)
                    typ = str(r.get("type", "—") or "—")
                    st.markdown(
                        " ".join(
                            [
                                _news_pill_html(f"Urg {_news_fmt_score(urg, 0)}", "warn" if pd.notna(urg) and float(urg) >= 80 else "neutral"),
                                _news_pill_html(f"Impact {_news_fmt_score(imp, 0)}", "info"),
                                _news_pill_html(f"Conf {_news_fmt_score(conf, 0)}", "ok"),
                                _news_pill_html("Relevant" if rel is True else ("Not relevant" if rel is False else "Relevance —"), "ok" if rel is True else "neutral"),
                                _news_pill_html(typ, "neutral"),
                            ]
                        ),
                        unsafe_allow_html=True,
                    )
                    url = str(r.get("url", "") or "").strip()
                    if url:
                        st.markdown(f"[Lien source]({url})")
                    st.divider()


def render_news_runs_history(df_macro: pd.DataFrame, df_spe: pd.DataFrame, df_macro_runs: pd.DataFrame, df_spe_runs: pd.DataFrame) -> None:
    st.markdown("#### Historique runs (AG4 & AG4-SPE)")
    m_snap = _news_latest_run_snapshot(df_macro_runs, "AG4 Macro")
    s_snap = _news_latest_run_snapshot(df_spe_runs, "AG4-SPE")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("AG4 Macro status", str(m_snap.get("status_raw", "NO_DATA")), delta=_news_fmt_age_h(m_snap.get("age_h")), delta_color="off")
    c2.metric("AG4-SPE status", str(s_snap.get("status_raw", "NO_DATA")), delta=_news_fmt_age_h(s_snap.get("age_h")), delta_color="off")
    c3.metric("#articles macro (7j)", len(_news_filter_window(df_macro, "7j")) if isinstance(df_macro, pd.DataFrame) else 0)
    c4.metric("#events spe (7j)", len(_news_filter_window(df_spe, "7j")) if isinstance(df_spe, pd.DataFrame) else 0)

    chart_window = st.radio("Fenetre chart", ["7j", "30j"], horizontal=True, key="ag4_runs_chart_window")
    rows = []
    if isinstance(df_macro, pd.DataFrame) and not df_macro.empty:
        dm = _news_filter_window(df_macro, chart_window).copy()
        if not dm.empty:
            dm["day"] = pd.to_datetime(dm.get("published_at", pd.Series(pd.NaT, index=dm.index)), errors="coerce", utc=True).dt.floor("D")
            rows.append(dm.groupby("day").size().reset_index(name="count").assign(flow="Macro"))
    if isinstance(df_spe, pd.DataFrame) and not df_spe.empty:
        ds = _news_filter_window(df_spe, chart_window).copy()
        if not ds.empty:
            ds["day"] = pd.to_datetime(ds.get("published_at", pd.Series(pd.NaT, index=ds.index)), errors="coerce", utc=True).dt.floor("D")
            rows.append(ds.groupby("day").size().reset_index(name="count").assign(flow="SPE"))
    if rows:
        hist_df = pd.concat(rows, ignore_index=True)
        fig = px.line(hist_df, x="day", y="count", color="flow", markers=True, color_discrete_map={"Macro": "#60a5fa", "SPE": "#f59e0b"})
        fig.update_layout(height=280, margin=dict(l=10, r=10, t=10, b=10), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", xaxis=dict(gridcolor="rgba(128,128,128,0.15)"), yaxis=dict(gridcolor="rgba(128,128,128,0.15)", title="#articles/jour"), legend=dict(orientation="h", y=1.08, x=0))
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    else:
        st.info("Pas de donnees d'articles sur la fenetre.")

    t1, t2 = st.tabs(["Runs AG4 Macro", "Runs AG4-SPE"])
    with t1:
        if df_macro_runs is None or df_macro_runs.empty:
            st.info("Aucun run AG4 Macro.")
        else:
            wk = normalize_cols(df_macro_runs.copy())
            cols = [c for c in ["run_id", "status", "started_at", "finished_at", "articles_count", "articles_ingested", "high_alerts", "top_theme", "regime"] if c in wk.columns]
            if not cols:
                cols = wk.columns.tolist()[:12]
            st.dataframe(wk[cols], use_container_width=True, hide_index=True, height=320)
    with t2:
        if df_spe_runs is None or df_spe_runs.empty:
            st.info("Aucun run AG4-SPE.")
        else:
            wk = normalize_cols(df_spe_runs.copy())
            cols = [c for c in ["run_id", "status", "started_at", "finished_at", "events_count", "relevant_count", "high_urgency_count", "top_theme", "regime"] if c in wk.columns]
            if not cols:
                cols = wk.columns.tolist()[:12]
            st.dataframe(wk[cols], use_container_width=True, hide_index=True, height=320)


def _news_health_metrics(df_news: pd.DataFrame, label: str, fresh_warn_h: float) -> dict[str, object]:
    out: dict[str, object] = {
        "Flux": label,
        "Rows": 0,
        "Freshness": "—",
        "Age_h": pd.NA,
        "% sans URL": pd.NA,
        "% sans published_at": pd.NA,
        "% sans score": pd.NA,
        "Duplicates %": pd.NA,
        "Lag ingest mean (h)": pd.NA,
        "Status": "WARN",
        "Raisons": "",
    }
    if df_news is None or df_news.empty:
        out["Status"] = "ERROR"
        out["Raisons"] = "table vide"
        return out

    wk = df_news.copy()
    n = len(wk)
    out["Rows"] = n
    pub = pd.to_datetime(wk.get("published_at", pd.Series(pd.NaT, index=wk.index)), errors="coerce", utc=True)
    ing = pd.to_datetime(wk.get("ingested_at", pd.Series(pd.NaT, index=wk.index)), errors="coerce", utc=True)
    latest_pub = pub.dropna().max() if not pub.dropna().empty else pd.NaT
    if pd.notna(latest_pub):
        age_h = max(0.0, (pd.Timestamp.now(tz="UTC") - latest_pub).total_seconds() / 3600.0)
        out["Age_h"] = round(age_h, 1)
        out["Freshness"] = _news_fmt_age_h(age_h)

    out["% sans URL"] = round(float(wk.get("url", pd.Series("", index=wk.index)).fillna("").astype(str).str.strip().eq("").mean() * 100.0), 1) if n else pd.NA
    out["% sans published_at"] = round(float(pub.isna().mean() * 100.0), 1) if n else pd.NA
    score_masks = []
    for c in ["impact_score", "urgency", "confidence_score"]:
        if c in wk.columns:
            score_masks.append(pd.to_numeric(wk[c], errors="coerce").isna())
    out["% sans score"] = round(float(pd.concat(score_masks, axis=1).all(axis=1).mean() * 100.0), 1) if score_masks else 100.0
    clustered = _news_dedupe_clusters(wk)
    out["Duplicates %"] = round(float((1.0 - (len(clustered) / max(1, n))) * 100.0), 1)
    if ing.notna().any() and pub.notna().any():
        lag = (ing - pub).dt.total_seconds() / 3600.0
        lag = lag[(lag.notna()) & (lag >= 0)]
        if not lag.empty:
            out["Lag ingest mean (h)"] = round(float(lag.mean()), 1)

    status = "OK"
    reasons: list[str] = []
    if pd.isna(out["Age_h"]) or (pd.notna(out["Age_h"]) and float(out["Age_h"]) > fresh_warn_h):
        status = "WARN"
        reasons.append("freshness")
    if pd.notna(out["% sans published_at"]) and float(out["% sans published_at"]) > 20.0:
        status = "WARN"
        reasons.append("published_at")
    if pd.notna(out["% sans score"]) and float(out["% sans score"]) > 50.0:
        status = "WARN"
        reasons.append("scores")
    out["Status"] = status
    out["Raisons"] = ", ".join(reasons) if reasons else "RAS"
    return out


def render_news_health(df_macro: pd.DataFrame, df_spe: pd.DataFrame) -> None:
    st.markdown("#### Qualite pipeline (observabilite)")
    macro_metrics = _news_health_metrics(df_macro, "AG4 Macro", fresh_warn_h=24.0)
    spe_metrics = _news_health_metrics(df_spe, "AG4-SPE", fresh_warn_h=24.0)
    health_df = pd.DataFrame([macro_metrics, spe_metrics])
    status_rank = {"OK": 0, "WARN": 1, "ERROR": 2}
    worst_status = max(health_df["Status"].tolist(), key=lambda s: status_rank.get(str(s), 1)) if not health_df.empty else "WARN"
    reasons = "; ".join([f"{r['Flux']}: {r['Raisons']}" for _, r in health_df.iterrows() if str(r.get("Raisons", "")) not in ("", "RAS")]) or "RAS"

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(_news_pill_html(f"Global {worst_status}", "ok" if worst_status == "OK" else ("error" if worst_status == "ERROR" else "warn")), unsafe_allow_html=True)
    c2.metric("Freshness macro", _news_fmt_age_h(macro_metrics.get("Age_h")))
    c3.metric("Freshness spe", _news_fmt_age_h(spe_metrics.get("Age_h")))
    c4.caption(f"Raisons: {reasons}")

    st.dataframe(health_df, use_container_width=True, hide_index=True, height=220)
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


def _to_dt_utc(v: object) -> pd.Timestamp:
    return pd.to_datetime(v, errors="coerce", utc=True)


def _fmt_dt_short(v: object) -> str:
    ts = _to_dt_utc(v)
    if pd.isna(ts):
        return "N/A"
    return ts.tz_convert("Europe/Paris").strftime("%Y-%m-%d %H:%M")


def _gate_badge_html(
    status: str,
    label: str,
    detail: str = "",
    gate_type: str = "",
    rule: str = "",
    consequence: str = "",
) -> str:
    st_norm = str(status or "").upper().strip()
    if st_norm == "OK":
        bg = "#28a745"
    elif st_norm in ("WARN", "WARNING"):
        bg = "#fd7e14"
    elif st_norm in ("N/A", "NA", "MISSING"):
        bg = "#6c757d"
        st_norm = "N/A"
    else:
        bg = "#dc3545"
    label_txt = safe_text(label, default="Gate")
    detail_txt = safe_text(detail, default="")
    gate_type_norm = safe_text(gate_type, default="-")
    txt = f"{label_txt}: {st_norm} [{gate_type_norm}]"
    if detail_txt:
        txt += f" ({detail_txt})"
    tooltip = (
        f"Règle : {safe_text(rule, default='N/A')}\n"
        f"Type : {gate_type_norm}\n"
        f"Conséquence : {safe_text(consequence, default='N/A')}"
    )
    tooltip_esc = html.escape(tooltip, quote=True)
    txt_esc = html.escape(txt, quote=False)
    return (
        f"<span title=\"{tooltip_esc}\" style=\"display:inline-block;margin:2px 6px 2px 0;padding:4px 8px;border-radius:6px;"
        f"background:{bg};color:white;font-size:0.85rem;font-weight:600;\">{txt_esc}</span>"
    )


def _freshness_status(age_h: float, warn_h: float, block_h: float) -> str:
    a = safe_float(age_h)
    if a <= warn_h:
        return "OK"
    if a <= block_h:
        return "WARN"
    return "BLOCK"


def _latest_timestamp(df: pd.DataFrame, candidates: list[str]) -> pd.Timestamp:
    if df is None or df.empty:
        return pd.NaT
    wk = normalize_cols(df.copy())
    col = _first_existing_column(wk, candidates)
    if not col:
        return pd.NaT
    ts = pd.to_datetime(wk[col], errors="coerce", utc=True)
    if ts.dropna().empty:
        return pd.NaT
    return ts.max()


def _freshness_label_from_age(age_h: float, warn_h: float, late_h: float) -> str:
    if pd.isna(age_h):
        return "Manquant"
    a = safe_float(age_h)
    if a <= warn_h:
        return "A jour"
    if a <= late_h:
        return "A surveiller"
    return "En retard"


def _build_multi_agent_data_freshness(
    duckdb_data: dict[str, pd.DataFrame],
    view_df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, float]]:
    now_utc = pd.Timestamp.now(tz="UTC")
    total_values = len(view_df) if view_df is not None else 0
    opt_missing = int((~view_df.get("options_ok", pd.Series(False, index=view_df.index)).fillna(False).astype(bool)).sum()) if total_values else 0
    invalid_opt_state = int(view_df.get("invalid_options_state", pd.Series(False, index=view_df.index)).fillna(False).astype(bool).sum()) if total_values else 0
    enrich_cov = (
        float(view_df.get("has_enrichment", pd.Series(False, index=view_df.index)).fillna(False).astype(bool).mean()) * 100.0
        if total_values
        else 0.0
    )

    snapshots = [
        {
            "source_id": "ag2_h1",
            "source": "AG2 Technique H1",
            "ts": _latest_timestamp(duckdb_data.get("df_signals", pd.DataFrame()), ["h1_date", "workflow_date", "updated_at", "created_at"]),
            "warn_h": 24.0,
            "late_h": 72.0,
        },
        {
            "source_id": "ag2_d1",
            "source": "AG2 Technique D1",
            "ts": _latest_timestamp(duckdb_data.get("df_signals", pd.DataFrame()), ["d1_date", "workflow_date", "updated_at", "created_at"]),
            "warn_h": 36.0,
            "late_h": 96.0,
        },
        {
            "source_id": "ag3",
            "source": "AG3 Fondamentale",
            "ts": _latest_timestamp(
                _load_fundamentals_for_dashboard(duckdb_data),
                ["updated_at", "updatedat", "fetched_at", "fetchedat", "created_at"],
            ),
            "warn_h": 24.0 * 7.0,
            "late_h": 24.0 * 30.0,
        },
        {
            "source_id": "ag4_macro",
            "source": "AG4 Macro",
            "ts": _latest_timestamp(_normalize_macro_news_df(duckdb_data.get("df_news_macro_history", pd.DataFrame())), ["publishedat"]),
            "warn_h": 24.0,
            "late_h": 72.0,
        },
        {
            "source_id": "ag4_symbol",
            "source": "AG4 News symbole",
            "ts": _latest_timestamp(_normalize_symbol_news_df(duckdb_data.get("df_news_symbol_history", pd.DataFrame())), ["publishedat"]),
            "warn_h": 24.0,
            "late_h": 72.0,
        },
        {
            "source_id": "yf_quote",
            "source": "YF Enrich quote",
            "ts": _latest_timestamp(
                duckdb_data.get("df_yf_enrichment_latest", pd.DataFrame()),
                ["yf_fetched_at", "fetched_at", "fetchedat"],
            ),
            "warn_h": 30.0,
            "late_h": 96.0,
        },
        {
            "source_id": "yf_options",
            "source": "YF Enrich options",
            "ts": _latest_timestamp(
                duckdb_data.get("df_yf_enrichment_latest", pd.DataFrame()),
                ["options_fetched_at", "fetched_at", "fetchedat"],
            ),
            "warn_h": 48.0,
            "late_h": 120.0,
        },
    ]

    rows = []
    for rec in snapshots:
        ts = pd.to_datetime(rec.get("ts"), errors="coerce", utc=True)
        age_h = (now_utc - ts).total_seconds() / 3600.0 if pd.notna(ts) else pd.NA
        rows.append(
            {
                "Source": rec.get("source"),
                "Derniere mise a jour": _fmt_dt_short(ts),
                "Age (h)": round(float(age_h), 1) if pd.notna(age_h) else pd.NA,
                "Statut": _freshness_label_from_age(age_h, warn_h=safe_float(rec.get("warn_h", 24.0)), late_h=safe_float(rec.get("late_h", 72.0))),
                "Couverture / erreurs": (
                    f"Couverture YF: {enrich_cov:.1f}%"
                    if rec.get("source_id") == "yf_quote"
                    else (
                        f"Options indispo: {opt_missing}, invalid state: {invalid_opt_state}"
                        if rec.get("source_id") == "yf_options"
                        else "N/A"
                    )
                ),
            }
        )

    out_df = pd.DataFrame(rows)

    summary = {
        "enrichment_coverage_pct": enrich_cov,
        "options_missing_count": float(opt_missing),
        "invalid_options_state_count": float(invalid_opt_state),
    }
    return out_df, summary


def _macro_relevance_score(row: pd.Series, sector_token: str, industry_token: str, symbol_token: str = "") -> float:
    txt = " ".join(
        [
            str(row.get("theme", "") or ""),
            str(row.get("title", "") or ""),
            str(row.get("snippet", "") or ""),
            str(row.get("notes", "") or ""),
            str(row.get("winners", "") or ""),
            str(row.get("losers", "") or ""),
            str(row.get("regime", "") or ""),
        ]
    ).lower()
    s = 0.0
    if sector_token and sector_token in txt:
        s += 55.0
    if industry_token and industry_token in txt:
        s += 35.0
    if symbol_token and symbol_token in txt:
        s += 25.0
    impact = safe_float(row.get("impactscore", 0.0))
    recency_bonus = 0.0
    p = _to_dt_utc(row.get("publishedat"))
    if pd.notna(p):
        days = max(0.0, (pd.Timestamp.now(tz="UTC") - p).total_seconds() / 86400.0)
        recency_bonus = max(0.0, 12.0 - days * 0.8)
    return s + min(20.0, max(0.0, impact)) + recency_bonus


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
# AG1 V3 - Multi-Portfolio Loader (ChatGPT / Grok / Gemini)
# ============================================================


def _duckdb_connect_readonly_retry(path: str):
    max_retries = 8
    base_delay = 0.25
    max_delay = 3.0

    try:
        max_retries = max(3, int(os.getenv("DUCKDB_READ_RETRIES", "8")))
        base_delay = max(0.1, float(os.getenv("DUCKDB_READ_BASE_DELAY_SEC", "0.25")))
        max_delay = max(base_delay, float(os.getenv("DUCKDB_READ_MAX_DELAY_SEC", "3.0")))
    except Exception:
        pass

    for attempt in range(max_retries):
        try:
            return duckdb.connect(path, read_only=True)
        except Exception as exc:
            msg = str(exc).lower()
            is_lock_like = isinstance(exc, duckdb.IOException) or ("lock" in msg) or ("busy" in msg)
            if is_lock_like and attempt < max_retries - 1:
                time.sleep(min(base_delay * (2 ** attempt), max_delay))
                continue
            return None
    return None


def _ag1_fetchdf(conn, sql: str, params: list | None = None) -> pd.DataFrame:
    try:
        return conn.execute(sql, params or []).fetchdf()
    except Exception:
        return pd.DataFrame()


def _ag1_norm_run_id(value: object) -> str:
    txt = str(value or "").strip()
    if not txt:
        return ""
    if txt.lower() in {"nan", "none", "nat", "null"}:
        return ""
    return txt.upper()


def _ag1_norm_run_id_series(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    invalid = s.str.lower().isin({"", "nan", "none", "nat", "null"})
    s = s.mask(invalid, pd.NA)
    return s.str.upper()


def _ag1_has_ag1_run_shape(series: pd.Series) -> bool:
    try:
        s = series.dropna().astype(str).str.strip().str.upper()
        if s.empty:
            return False
        return bool(s.str.startswith("RUN_").any())
    except Exception:
        return False


def _ag1_expected_model_tokens(key: str, cfg: dict[str, str]) -> list[str]:
    token_map = {
        "chatgpt52": ["chatgpt", "gpt", "openai", "o3", "o4"],
        "grok41_reasoning": ["grok", "xai"],
        "gemini30_pro": ["gemini", "google"],
    }
    tokens = list(token_map.get(str(key), []))
    label_blob = f"{cfg.get('label', '')} {cfg.get('short_label', '')}".lower()
    for tok in re.findall(r"[a-z0-9]+", label_blob):
        if len(tok) >= 4 and tok not in tokens:
            tokens.append(tok)
    return tokens


def _ag1_model_matches_expected(model_name: object, key: str, cfg: dict[str, str]) -> bool:
    raw = str(model_name or "").strip().lower()
    if not raw:
        return False
    return any(tok in raw for tok in _ag1_expected_model_tokens(key, cfg))


def _ag1_resolve_display_model(
    key: str,
    cfg: dict[str, str],
    latest: dict[str, object],
    latest_run_meta: dict[str, object],
    df_runs: pd.DataFrame,
) -> dict[str, object]:
    expected_label = str(cfg.get("label") or key).strip() or str(key)
    candidates: list[str] = []
    for v in [latest.get("model"), latest_run_meta.get("model")]:
        s = str(v or "").strip()
        if s and s not in candidates:
            candidates.append(s)

    if isinstance(df_runs, pd.DataFrame) and not df_runs.empty and "model" in df_runs.columns:
        ser = df_runs["model"].astype(str).str.strip()
        for s in ser.tolist():
            if s and s not in candidates:
                candidates.append(s)
            if len(candidates) >= 12:
                break

    raw_model = candidates[0] if candidates else ""
    matched = next((m for m in candidates if _ag1_model_matches_expected(m, key, cfg)), "")
    if matched:
        return {
            "display_model": matched,
            "raw_model": raw_model,
            "source": "runs",
            "mismatch": False,
        }
    if raw_model:
        return {
            "display_model": expected_label,
            "raw_model": raw_model,
            "source": "config_fallback",
            "mismatch": True,
        }
    return {
        "display_model": expected_label,
        "raw_model": "",
        "source": "config",
        "mismatch": False,
    }


def _ag1_default_payload(key: str, cfg: dict[str, str]) -> dict[str, object]:
    return {
        "key": key,
        "label": cfg.get("label", key),
        "short_label": cfg.get("short_label", key),
        "db_path": cfg.get("db_path", ""),
        "accent": cfg.get("accent", "#666"),
        "status": "missing",
        "error": "",
        "summary": {
            "init_cap": 50000.0,
            "cash": 0.0,
            "invest": 0.0,
            "total_val": 0.0,
            "roi": 0.0,
            "cash_pct": 0.0,
            "drawdown_pct": 0.0,
            "cum_fees_eur": 0.0,
            "cum_ai_cost_eur": 0.0,
            "positions_count": 0,
            "trades_this_run": 0,
            "runs_count": 0,
            "signals_24h": 0,
            "alerts_24h": 0,
            "last_run_id": "",
            "last_model": "",
            "last_model_raw": "",
            "last_model_source": "",
            "last_model_mismatch": False,
            "last_strategy_version": "",
            "last_config_version": "",
            "last_prompt_version": "",
            "last_data_ok_for_trading": None,
            "last_price_coverage_pct": None,
            "last_decision_summary": "",
            "last_update": pd.NaT,
        },
        "df_portfolio": pd.DataFrame(),
        "df_performance": pd.DataFrame(),
        "df_transactions": pd.DataFrame(),
        "df_ai_signals": pd.DataFrame(),
        "df_alerts": pd.DataFrame(),
        "df_runs": pd.DataFrame(),
        "diagnostics": {
            "positions_source_table": "core.positions_snapshot",
            "ledger_run_id": "",
            "ledger_positions_count": 0,
            "mtm_run_id": "",
            "mtm_positions_count": None,
            "mtm_last_updated_at": None,
            "mtm_age_hours": None,
            "mtm_is_stale": None,
            "positions_only_in_ledger": [],
            "positions_only_in_mtm": [],
        },
    }


def _ag1_load_single_portfolio_ledger(key: str, cfg: dict[str, str]) -> dict[str, object]:
    payload = _ag1_default_payload(key, cfg)
    db_path = str(cfg.get("db_path") or "").strip()
    payload["db_path"] = db_path
    if not db_path or not os.path.exists(db_path):
        payload["status"] = "missing"
        return payload

    conn = _duckdb_connect_readonly_retry(db_path)
    if conn is None:
        payload["status"] = "error"
        payload["error"] = "Impossible d'ouvrir la base DuckDB (lock/busy)."
        return payload

    try:
        has_ledger = False
        try:
            chk = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM information_schema.tables
                WHERE table_schema = 'core'
                  AND table_name = 'portfolio_snapshot'
                """
            ).fetchone()
            has_ledger = bool(chk and chk[0] and int(chk[0]) > 0)
        except Exception:
            has_ledger = False

        if not has_ledger:
            payload["status"] = "error"
            payload["error"] = "Schema AG1-V3 ledger non detecte (core.portfolio_snapshot absent)."
            return payload

        df_runs = _ag1_fetchdf(
            conn,
            """
            SELECT
              run_id,
              ts_start,
              ts_end,
              model,
              strategy_version,
              config_version,
              prompt_version,
              decision_summary,
              data_ok_for_trading,
              price_coverage_pct,
              news_count,
              ai_cost_eur,
              expected_fees_eur
            FROM core.runs
            ORDER BY COALESCE(ts_end, ts_start) DESC
            """,
        )

        df_perf = _ag1_fetchdf(
            conn,
            """
            WITH snapshots AS (
              SELECT
                ps.*,
                COALESCE(json_extract_string(ps.meta_json, '$.writer'), '') AS snapshot_writer,
                CASE
                  WHEN UPPER(COALESCE(json_extract_string(ps.meta_json, '$.writer'), '')) = 'INLINE_MINIMAL' THEN 0
                  ELSE 1
                END AS is_preferred
              FROM core.portfolio_snapshot ps
            ),
            pref AS (
              SELECT MAX(is_preferred) AS has_preferred
              FROM snapshots
            ),
            selected AS (
              SELECT s.*
              FROM snapshots s
              CROSS JOIN pref p
              WHERE p.has_preferred = 0 OR s.is_preferred = 1
            )
            SELECT
              ps.ts AS timestamp,
              ps.run_id AS run_id,
              r.model AS model,
              CAST(ps.total_value_eur AS DOUBLE) AS totalvalueeur,
              CAST(ps.cash_eur AS DOUBLE) AS casheur,
              CAST(ps.equity_eur AS DOUBLE) AS equityeur,
              CAST(ps.cum_fees_eur AS DOUBLE) AS cum_fees_eur,
              CAST(ps.cum_ai_cost_eur AS DOUBLE) AS cum_ai_cost_eur,
              ps.trades_this_run AS trades_this_run,
              CAST(ps.roi AS DOUBLE) AS roi,
              CAST(ps.drawdown_pct AS DOUBLE) AS drawdown_pct
            FROM selected ps
            LEFT JOIN core.runs r ON r.run_id = ps.run_id
            ORDER BY ps.ts
            """,
        )

        df_latest = _ag1_fetchdf(
            conn,
            """
            WITH snapshots AS (
              SELECT
                ps.*,
                COALESCE(json_extract_string(ps.meta_json, '$.writer'), '') AS snapshot_writer,
                CASE
                  WHEN UPPER(COALESCE(json_extract_string(ps.meta_json, '$.writer'), '')) = 'INLINE_MINIMAL' THEN 0
                  ELSE 1
                END AS is_preferred
              FROM core.portfolio_snapshot ps
            )
            SELECT
              ps.ts AS snapshot_ts,
              ps.run_id AS run_id,
              r.model AS model,
              ps.snapshot_writer AS snapshot_writer,
              ps.is_preferred AS snapshot_is_preferred,
              CAST(ps.cash_eur AS DOUBLE) AS cash_eur,
              CAST(ps.equity_eur AS DOUBLE) AS equity_eur,
              CAST(ps.total_value_eur AS DOUBLE) AS total_value_eur,
              CAST(ps.cum_fees_eur AS DOUBLE) AS cum_fees_eur,
              CAST(ps.cum_ai_cost_eur AS DOUBLE) AS cum_ai_cost_eur,
              ps.trades_this_run AS trades_this_run,
              CAST(ps.roi AS DOUBLE) AS roi,
              CAST(ps.drawdown_pct AS DOUBLE) AS drawdown_pct
            FROM snapshots ps
            LEFT JOIN core.runs r ON r.run_id = ps.run_id
            ORDER BY
              ps.is_preferred DESC,
              ps.ts DESC,
              COALESCE(r.ts_end, r.ts_start) DESC NULLS LAST
            LIMIT 1
            """,
        )

        df_pos = pd.DataFrame()

        mtm_schema = _ag1_fetchdf(conn, "PRAGMA table_info('portfolio_positions_mtm_latest')")
        mtm_available_cols = set()
        if mtm_schema is not None and not mtm_schema.empty and "name" in mtm_schema.columns:
            mtm_available_cols = {
                str(c).strip().lower()
                for c in mtm_schema["name"].dropna().astype(str).tolist()
                if str(c).strip()
            }
        mtm_select_cols = ["symbol", "run_id", "updated_at"]
        if "ag1_source_run_id" in mtm_available_cols:
            mtm_select_cols.append("ag1_source_run_id")
        if "source_run_id" in mtm_available_cols:
            mtm_select_cols.append("source_run_id")
        if "ag1_run_id" in mtm_available_cols:
            mtm_select_cols.append("ag1_run_id")
        if "ag1_source_snapshot_ts" in mtm_available_cols:
            mtm_select_cols.append("ag1_source_snapshot_ts")
        mtm_sql = (
            "SELECT "
            + ", ".join(mtm_select_cols)
            + "\nFROM portfolio_positions_mtm_latest\nWHERE symbol IS NOT NULL"
        )
        df_mtm_latest = _ag1_fetchdf(conn, mtm_sql)

        df_transactions = _ag1_fetchdf(
            conn,
            """
            WITH lot_realized AS (
              SELECT
                close_fill_id AS fill_id,
                CAST(SUM(COALESCE(realized_pnl_eur, 0)) AS DOUBLE) AS realizedpnl
              FROM core.position_lots
              WHERE close_fill_id IS NOT NULL
              GROUP BY close_fill_id
            )
            SELECT
              f.ts_fill AS timestamp,
              f.run_id AS run_id,
              r.model AS agent,
              o.symbol AS symbol,
              o.side AS side,
              CAST(f.qty AS DOUBLE) AS quantity,
              CAST(f.price AS DOUBLE) AS price,
              CAST(CAST(f.qty AS DOUBLE) * CAST(f.price AS DOUBLE) AS DOUBLE) AS notional,
              CAST(COALESCE(lr.realizedpnl, 0) AS DOUBLE) AS realizedpnl,
              CAST(COALESCE(f.fees_eur, 0) AS DOUBLE) AS fees_eur,
              o.order_type AS order_type,
              o.status AS status,
              o.reason AS reason
            FROM core.fills f
            LEFT JOIN core.orders o ON o.order_id = f.order_id
            LEFT JOIN lot_realized lr ON lr.fill_id = f.fill_id
            LEFT JOIN core.runs r ON r.run_id = f.run_id
            ORDER BY f.ts_fill
            """,
        )

        df_ai_signals = _ag1_fetchdf(
            conn,
            """
            SELECT
              s.ts AS timestamp,
              s.run_id AS run_id,
              r.model AS model,
              s.symbol AS symbol,
              s.signal AS signal,
              s.confidence AS confidence,
              s.horizon AS horizon,
              s.entry_zone AS entry_zone,
              CAST(s.stop_loss AS DOUBLE) AS stop_loss,
              CAST(s.take_profit AS DOUBLE) AS take_profit,
              s.risk_score AS risk_score,
              s.catalyst AS catalyst,
              s.rationale AS rationale
            FROM core.ai_signals s
            LEFT JOIN core.runs r ON r.run_id = s.run_id
            ORDER BY s.ts DESC
            LIMIT 1000
            """,
        )

        df_alerts = _ag1_fetchdf(
            conn,
            """
            SELECT
              a.ts AS timestamp,
              a.run_id AS run_id,
              r.model AS model,
              a.severity AS severity,
              a.category AS category,
              a.symbol AS symbol,
              a.code AS code,
              a.message AS message
            FROM core.alerts a
            LEFT JOIN core.runs r ON r.run_id = a.run_id
            ORDER BY a.ts DESC
            LIMIT 1000
            """,
        )

        init_cap = 50000.0
        cfg_cap = _ag1_fetchdf(
            conn,
            """
            SELECT CAST(initial_capital_eur AS DOUBLE) AS initial_cap
            FROM cfg.portfolio_config
            WHERE initial_capital_eur IS NOT NULL
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 1
            """,
        )
        if not cfg_cap.empty and "initial_cap" in cfg_cap.columns:
            init_cap_val = safe_float(cfg_cap.iloc[0].get("initial_cap"))
            if init_cap_val > 0:
                init_cap = init_cap_val
        else:
            dep_cap = _ag1_fetchdf(
                conn,
                """
                SELECT CAST(SUM(COALESCE(amount, 0)) AS DOUBLE) AS initial_cap
                FROM core.cash_ledger
                WHERE UPPER(COALESCE(type, '')) = 'DEPOSIT'
                """,
            )
            if not dep_cap.empty and "initial_cap" in dep_cap.columns:
                init_cap_val = safe_float(dep_cap.iloc[0].get("initial_cap"))
                if init_cap_val > 0:
                    init_cap = init_cap_val

        latest = df_latest.iloc[0].to_dict() if df_latest is not None and not df_latest.empty else {}
        latest_run_id = str(latest.get("run_id") or "").strip()

        if latest_run_id:
            df_pos = _ag1_fetchdf(
                conn,
                """
                SELECT
                  p.run_id AS run_id,
                  p.ts AS updatedat,
                  p.symbol AS symbol,
                  COALESCE(i.name, p.symbol) AS name,
                  COALESCE(i.asset_class, 'Equity') AS assetclass,
                  COALESCE(i.sector, '') AS sector,
                  COALESCE(i.industry, '') AS industry,
                  COALESCE(i.isin, '') AS isin,
                  CAST(p.qty AS DOUBLE) AS quantity,
                  CAST(p.avg_cost AS DOUBLE) AS avgprice,
                  CAST(p.last_price AS DOUBLE) AS lastprice,
                  CAST(p.market_value_eur AS DOUBLE) AS marketvalue,
                  CAST(p.unrealized_pnl_eur AS DOUBLE) AS unrealizedpnl
                FROM core.positions_snapshot p
                LEFT JOIN core.instruments i ON i.symbol = p.symbol
                WHERE p.run_id = ?
                ORDER BY p.market_value_eur DESC NULLS LAST, p.symbol
                """,
                [latest_run_id],
            )
        latest_run_meta: dict[str, object] = {}
        if df_runs is not None and not df_runs.empty and "run_id" in df_runs.columns:
            last_run_id_guess = str(latest.get("run_id") or "").strip()
            runs_src = df_runs.copy()
            runs_src["run_id"] = runs_src["run_id"].astype(str)
            if last_run_id_guess:
                match = runs_src[runs_src["run_id"] == last_run_id_guess]
                if not match.empty:
                    latest_run_meta = match.iloc[0].to_dict()
            if not latest_run_meta and not runs_src.empty:
                latest_run_meta = runs_src.iloc[0].to_dict()

        model_info = _ag1_resolve_display_model(key, cfg, latest, latest_run_meta, df_runs)

        cash = safe_float(latest.get("cash_eur"))
        invest = safe_float(latest.get("equity_eur"))
        total_val = safe_float(latest.get("total_value_eur"))
        if total_val <= 0 and (cash > 0 or invest > 0):
            total_val = cash + invest
        roi = float(latest.get("roi")) if pd.notna(latest.get("roi")) else ((total_val - init_cap) / init_cap if init_cap else 0.0)
        cash_pct = (cash / total_val * 100.0) if total_val > 0 else 0.0
        positions_count = int(len(df_pos)) if df_pos is not None and not df_pos.empty else 0

        # Synthetise CASH_EUR / __META__ rows for compatibility with existing dashboard widgets.
        if df_pos is None or df_pos.empty:
            df_portfolio = pd.DataFrame(
                columns=[
                    "symbol",
                    "name",
                    "assetclass",
                    "sector",
                    "industry",
                    "isin",
                    "quantity",
                    "avgprice",
                    "lastprice",
                    "marketvalue",
                    "unrealizedpnl",
                    "updatedat",
                    "notes",
                ]
            )
        else:
            df_portfolio = df_pos.copy()
            if "notes" not in df_portfolio.columns:
                df_portfolio["notes"] = ""

        snap_ts = pd.to_datetime(latest.get("snapshot_ts"), errors="coerce")
        snap_ts_val = snap_ts if pd.notna(snap_ts) else pd.Timestamp.utcnow()
        cash_row = pd.DataFrame(
            [
                {
                    "symbol": "CASH_EUR",
                    "name": "Cash EUR",
                    "assetclass": "Cash",
                    "sector": "Cash",
                    "industry": "Cash",
                    "isin": "",
                    "quantity": 1.0,
                    "avgprice": cash,
                    "lastprice": cash,
                    "marketvalue": cash,
                    "unrealizedpnl": 0.0,
                    "updatedat": snap_ts_val,
                    "notes": "",
                }
            ]
        )
        meta_row = pd.DataFrame(
            [
                {
                    "symbol": "__META__",
                    "name": "__META__",
                    "assetclass": "Meta",
                    "sector": "Meta",
                    "industry": "Meta",
                    "isin": "",
                    "quantity": 0.0,
                    "avgprice": 0.0,
                    "lastprice": 0.0,
                    "marketvalue": init_cap,
                    "unrealizedpnl": 0.0,
                    "updatedat": snap_ts_val,
                    "notes": json.dumps({"initialCapitalEUR": init_cap}, ensure_ascii=False),
                }
            ]
        )
        df_portfolio = pd.concat([df_portfolio, cash_row, meta_row], ignore_index=True)

        # Signals/alerts activity in the last 24h for header.
        now_utc = pd.Timestamp.now(tz="UTC")
        signals_24h = 0
        alerts_24h = 0
        if df_ai_signals is not None and not df_ai_signals.empty and "timestamp" in df_ai_signals.columns:
            ts_sig = pd.to_datetime(df_ai_signals["timestamp"], errors="coerce", utc=True)
            signals_24h = int((ts_sig >= (now_utc - pd.Timedelta(hours=24))).sum())
        if df_alerts is not None and not df_alerts.empty and "timestamp" in df_alerts.columns:
            ts_alt = pd.to_datetime(df_alerts["timestamp"], errors="coerce", utc=True)
            alerts_24h = int((ts_alt >= (now_utc - pd.Timedelta(hours=24))).sum())

        diagnostics = {
            "positions_source_table": "core.positions_snapshot",
            "ledger_run_id": str(latest.get("run_id") or ""),
            "ledger_positions_count": 0,
            "mtm_run_id": "",
            "mtm_source_run_id": "",
            "mtm_match_col": "",
            "mtm_positions_count": None,
            "mtm_last_updated_at": None,
            "mtm_age_hours": None,
            "mtm_is_stale": None,
            "mtm_reason": "",
            "positions_only_in_ledger": [],
            "positions_only_in_mtm": [],
        }

        ledger_syms = set()
        if df_pos is not None and not df_pos.empty and "symbol" in df_pos.columns:
            ser = df_pos["symbol"].astype(str).str.strip().str.upper()
            ledger_syms = {s for s in ser.tolist() if s and s not in {"CASH_EUR", "__META__"}}
        diagnostics["ledger_positions_count"] = int(len(ledger_syms))

        mtm_syms = set()
        compare_mtm_with_ledger = True
        if df_mtm_latest is not None and not df_mtm_latest.empty:
            mtm_scan = df_mtm_latest.copy()
            mtm_cols = [str(c).strip().lower() for c in mtm_scan.columns]
            mtm_scan.columns = mtm_cols
            mtm_ts_latest = pd.NaT
            mtm_run_id_latest = ""
            mtm_source_run_id_latest = ""
            if "run_id" in mtm_scan.columns:
                mtm_scan["__run_id_norm"] = _ag1_norm_run_id_series(mtm_scan["run_id"])
            if "ag1_source_run_id" in mtm_scan.columns:
                mtm_scan["__ag1_source_run_id_norm"] = _ag1_norm_run_id_series(mtm_scan["ag1_source_run_id"])
            if "source_run_id" in mtm_scan.columns:
                mtm_scan["__source_run_id_norm"] = _ag1_norm_run_id_series(mtm_scan["source_run_id"])
            if "ag1_run_id" in mtm_scan.columns:
                mtm_scan["__ag1_run_id_norm"] = _ag1_norm_run_id_series(mtm_scan["ag1_run_id"])
            source_series_non_empty = "__ag1_source_run_id_norm" in mtm_scan.columns and bool(
                mtm_scan["__ag1_source_run_id_norm"].notna().any()
            )
            mtm_match_col = "ag1_source_run_id" if source_series_non_empty else "run_id"
            mtm_match_norm_col = "__ag1_source_run_id_norm" if source_series_non_empty else "__run_id_norm"
            diagnostics["mtm_match_col"] = mtm_match_col
            if "updated_at" in mtm_scan.columns:
                mtm_ts = pd.to_datetime(mtm_scan["updated_at"], errors="coerce", utc=True)
                if mtm_ts is not None and len(mtm_ts) > 0:
                    mtm_ts_latest = mtm_ts.max()
                    if pd.notna(mtm_ts_latest):
                        diagnostics["mtm_last_updated_at"] = mtm_ts_latest.isoformat()
                    if "run_id" in mtm_scan.columns:
                        tmp = mtm_scan.copy()
                        tmp["__updated_at_utc"] = mtm_ts
                        tmp = tmp.sort_values("__updated_at_utc", ascending=False, na_position="last")
                        mtm_run_series = tmp.get("__run_id_norm", pd.Series([], dtype="object")).dropna()
                        if not mtm_run_series.empty:
                            mtm_run_id_latest = str(mtm_run_series.iloc[0])
                        for source_norm_col in [
                            "__ag1_source_run_id_norm",
                            "__source_run_id_norm",
                            "__ag1_run_id_norm",
                            "__run_id_norm",
                        ]:
                            if source_norm_col not in tmp.columns:
                                continue
                            mtm_source_run_series = tmp[source_norm_col].dropna()
                            if not mtm_source_run_series.empty:
                                mtm_source_run_id_latest = str(mtm_source_run_series.iloc[0])
                                break
            if "__run_id_norm" in mtm_scan.columns:
                mtm_run_ids = mtm_scan["__run_id_norm"].dropna()
                if not mtm_run_ids.empty:
                    diagnostics["mtm_run_id"] = mtm_run_id_latest or str(mtm_run_ids.iloc[0])
            for source_norm_col in [
                "__ag1_source_run_id_norm",
                "__source_run_id_norm",
                "__ag1_run_id_norm",
                "__run_id_norm",
            ]:
                if source_norm_col not in mtm_scan.columns:
                    continue
                mtm_source_ids = mtm_scan[source_norm_col].dropna()
                if not mtm_source_ids.empty:
                    diagnostics["mtm_source_run_id"] = mtm_source_run_id_latest or str(mtm_source_ids.iloc[0])
                    break
            ledger_run_id = _ag1_norm_run_id(diagnostics.get("ledger_run_id"))
            if ledger_run_id:
                match_candidates: list[tuple[str, str]] = []
                if "__ag1_source_run_id_norm" in mtm_scan.columns:
                    match_candidates.append(("ag1_source_run_id", "__ag1_source_run_id_norm"))
                if "__source_run_id_norm" in mtm_scan.columns:
                    match_candidates.append(("source_run_id", "__source_run_id_norm"))
                if "__ag1_run_id_norm" in mtm_scan.columns:
                    match_candidates.append(("ag1_run_id", "__ag1_run_id_norm"))
                if "__run_id_norm" in mtm_scan.columns:
                    match_candidates.append(("run_id", "__run_id_norm"))

                selected_match: tuple[str, str] | None = None
                for raw_col, norm_col in match_candidates:
                    has_match = bool(mtm_scan[norm_col].eq(ledger_run_id).fillna(False).any())
                    if has_match:
                        selected_match = (raw_col, norm_col)
                        break

                if selected_match is not None:
                    mtm_match_col, mtm_match_norm_col = selected_match
                    diagnostics["mtm_match_col"] = mtm_match_col
                    mtm_same_run = mtm_scan[mtm_scan[mtm_match_norm_col].eq(ledger_run_id)]
                    if mtm_same_run.empty:
                        compare_mtm_with_ledger = False
                        diagnostics["mtm_is_stale"] = True
                        diagnostics["mtm_reason"] = "run_id_mismatch"
                    else:
                        mtm_scan = mtm_same_run
                elif match_candidates:
                    has_ag1_shape = False
                    for _, norm_col in match_candidates:
                        if _ag1_has_ag1_run_shape(mtm_scan[norm_col]):
                            has_ag1_shape = True
                            break
                    if has_ag1_shape:
                        compare_mtm_with_ledger = False
                        diagnostics["mtm_is_stale"] = True
                        diagnostics["mtm_reason"] = "run_id_mismatch"
                    else:
                        # Some MTM writers keep pipeline run ids (PFMTM_*) only.
                        # In this case run-id equivalence cannot be asserted; keep comparison enabled.
                        diagnostics["mtm_reason"] = "run_id_unavailable"

            if "symbol" in mtm_scan.columns:
                ser = mtm_scan["symbol"].astype(str).str.strip().str.upper()
                mtm_syms = {s for s in ser.tolist() if s and s not in {"CASH_EUR", "__META__"}}
                diagnostics["mtm_positions_count"] = int(len(mtm_syms))

            mtm_ref_ts = pd.to_datetime(latest.get("snapshot_ts"), errors="coerce", utc=True)
            if pd.notna(mtm_ref_ts) and pd.notna(mtm_ts_latest):
                mtm_age_hours = max(0.0, (mtm_ref_ts - mtm_ts_latest).total_seconds() / 3600.0)
                diagnostics["mtm_age_hours"] = float(mtm_age_hours)
                if str(diagnostics.get("mtm_reason") or "") != "run_id_mismatch":
                    diagnostics["mtm_is_stale"] = bool(mtm_age_hours > 24.0)
            elif pd.notna(mtm_ts_latest):
                mtm_age_hours = max(0.0, (pd.Timestamp.now(tz="UTC") - mtm_ts_latest).total_seconds() / 3600.0)
                diagnostics["mtm_age_hours"] = float(mtm_age_hours)
                if str(diagnostics.get("mtm_reason") or "") != "run_id_mismatch":
                    diagnostics["mtm_is_stale"] = bool(mtm_age_hours > 24.0)

        if compare_mtm_with_ledger and (ledger_syms or mtm_syms):
            diagnostics["positions_only_in_ledger"] = sorted(list(ledger_syms - mtm_syms))[:20]
            diagnostics["positions_only_in_mtm"] = sorted(list(mtm_syms - ledger_syms))[:20]

        summary = {
            "init_cap": float(init_cap),
            "cash": float(cash),
            "invest": float(invest),
            "total_val": float(total_val),
            "roi": float(roi if pd.notna(roi) else 0.0),
            "cash_pct": float(cash_pct),
            "drawdown_pct": float(safe_float(latest.get("drawdown_pct"))),
            "cum_fees_eur": float(safe_float(latest.get("cum_fees_eur"))),
            "cum_ai_cost_eur": float(safe_float(latest.get("cum_ai_cost_eur"))),
            "positions_count": int(positions_count),
            "trades_this_run": int(safe_float(latest.get("trades_this_run"))),
            "runs_count": int(len(df_runs)) if df_runs is not None else 0,
            "signals_24h": int(signals_24h),
            "alerts_24h": int(alerts_24h),
            "last_run_id": str(latest.get("run_id") or ""),
            "last_model": str(model_info.get("display_model") or ""),
            "last_model_raw": str(model_info.get("raw_model") or ""),
            "last_model_source": str(model_info.get("source") or ""),
            "last_model_mismatch": bool(model_info.get("mismatch")),
            "last_strategy_version": str(latest_run_meta.get("strategy_version") or ""),
            "last_config_version": str(latest_run_meta.get("config_version") or ""),
            "last_prompt_version": str(latest_run_meta.get("prompt_version") or ""),
            "last_data_ok_for_trading": latest_run_meta.get("data_ok_for_trading"),
            "last_price_coverage_pct": (
                float(safe_float(latest_run_meta.get("price_coverage_pct")))
                if latest_run_meta.get("price_coverage_pct") is not None
                else None
            ),
            "last_decision_summary": str(latest_run_meta.get("decision_summary") or ""),
            "last_update": pd.to_datetime(latest.get("snapshot_ts"), errors="coerce", utc=True),
        }

        payload.update(
            {
                "status": "ok",
                "summary": summary,
                "df_portfolio": normalize_cols(df_portfolio),
                "df_performance": normalize_cols(df_perf) if df_perf is not None else pd.DataFrame(),
                "df_transactions": normalize_cols(df_transactions) if df_transactions is not None else pd.DataFrame(),
                "df_ai_signals": normalize_cols(df_ai_signals) if df_ai_signals is not None else pd.DataFrame(),
                "df_alerts": normalize_cols(df_alerts) if df_alerts is not None else pd.DataFrame(),
                "df_runs": normalize_cols(df_runs) if df_runs is not None else pd.DataFrame(),
                "diagnostics": diagnostics,
            }
        )
        return payload

    except Exception as exc:
        payload["status"] = "error"
        payload["error"] = str(exc)
        return payload
    finally:
        try:
            conn.close()
        except Exception:
            pass


@st.cache_data(ttl=30)
def load_ag1_multi_portfolios() -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    for key, cfg in AG1_MULTI_PORTFOLIO_CONFIG.items():
        out[key] = _ag1_load_single_portfolio_ledger(key, cfg)
    return out


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
        "Macro & News (AG4)",
    ],
)

data_dict = load_data()
if not data_dict:
    st.warning("Donnees Google Sheets indisponibles. Les vues basees DuckDB (System Health, Vue consolidee, Analyse V2) restent disponibles.")

# Signatures fichiers DuckDB (invalidation cache basee sur mtime/size)
ag1_db_sig = duckdb_file_signature(AG1_DUCKDB_PATH)
ag2_db_sig = duckdb_file_signature(DUCKDB_PATH)
ag3_db_sig = duckdb_file_signature(AG3_DUCKDB_PATH)
ag4_db_sig = duckdb_file_signature(AG4_DUCKDB_PATH)
ag4_spe_db_sig = duckdb_file_signature(AG4_SPE_DUCKDB_PATH)
yf_db_sig = duckdb_file_signature(YF_ENRICH_DUCKDB_PATH)

# ------------------------------------------------------------
# PRE-CALCULS (ROBUSTES)
# ------------------------------------------------------------

df_univ = data_dict.get("Universe", pd.DataFrame()) if data_dict else pd.DataFrame()
if df_univ is None or df_univ.empty:
    df_univ = load_universe_latest(DUCKDB_PATH, ag2_db_sig)
# Portfolio source of truth is now DuckDB AG1.
df_port = load_ag1_portfolio_latest(AG1_DUCKDB_PATH, ag1_db_sig)
if df_port is None:
    df_port = pd.DataFrame()

df_port = enrich_df_with_name(df_port, df_univ) if df_port is not None else pd.DataFrame()
df_perf = pd.DataFrame()
df_trans = pd.DataFrame()
df_prices = pd.DataFrame()

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


COMPARE_PERIOD_DAYS = {"7j": 7, "30j": 30, "90j": 90, "All": None}
COMPARE_WINNER_META = {
    "ROI": {"key": "roi_pct", "higher_is_better": True},
    "TotalValue": {"key": "total_val", "higher_is_better": True},
    "MaxDD": {"key": "max_drawdown_pct", "higher_is_better": True},  # less negative is better
    "Sharpe": {"key": "sharpe", "higher_is_better": True},
}


def _fmt_currency(v: object, digits: int = 2, unit: str = "EUR") -> str:
    n = safe_float(v)
    if pd.isna(n):
        return "—"
    return f"{n:,.{digits}f} {unit}".replace(",", " ")


def _fmt_number(v: object, digits: int = 0) -> str:
    n = safe_float(v)
    if pd.isna(n):
        return "—"
    return f"{n:,.{digits}f}".replace(",", " ")


def _fmt_pct(v: object, digits: int = 2, suffix: str = "%") -> str:
    n = safe_float(v)
    if pd.isna(n):
        return "—"
    return f"{n:.{digits}f} {suffix}"


def _fmt_delta_eur(v: object, digits: int = 2) -> str:
    if v is None or pd.isna(v):
        return "—"
    n = safe_float(v)
    return f"{n:+,.{digits}f} EUR".replace(",", " ")


def _fmt_delta_pp(v: object, digits: int = 2) -> str:
    if v is None or pd.isna(v):
        return "—"
    n = safe_float(v)
    return f"{n:+.{digits}f} pp"


def _signed_color(v: object) -> str:
    n = safe_float(v)
    if pd.isna(n):
        return "#94a3b8"
    return "#16a34a" if float(n) >= 0.0 else "#dc2626"


def _position_pnl_row_html(row: dict[str, object]) -> str:
    symbol = html.escape(str(row.get("symbol") or "N/A"))
    pnl_eur = safe_float(row.get("pnl_eur"))
    pnl_pct = safe_float(row.get("pnl_pct"))
    pnl_eur_txt = f"{pnl_eur:+,.0f} EUR".replace(",", " ") if not pd.isna(pnl_eur) else "N/A"
    pnl_pct_txt = f"{pnl_pct:+.1f}%" if not pd.isna(pnl_pct) else "N/A"
    return (
        f"&bull; <code>{symbol}</code> "
        f"<span style='color:{_signed_color(pnl_eur)};font-weight:700;'>{html.escape(pnl_eur_txt)}</span> "
        f"(<span style='color:{_signed_color(pnl_pct)};font-weight:700;'>{html.escape(pnl_pct_txt)}</span>)"
    )


def _fmt_paris_datetime(ts: object, fmt: str = "%Y-%m-%d %H:%M") -> str:
    dt = pd.to_datetime(ts, errors="coerce", utc=True)
    if pd.isna(dt):
        return "N/A"
    try:
        return dt.tz_convert("Europe/Paris").strftime(fmt)
    except Exception:
        return dt.strftime(fmt)


def _short_run_id(run_id: object, keep: int = 20) -> str:
    s = str(run_id or "").strip()
    if not s:
        return "N/A"
    if len(s) <= keep:
        return s
    return f"{s[:keep]}…"


def _coerce_bool_or_none(v: object) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)) and not pd.isna(v):
        return bool(v)
    s = str(v).strip().lower()
    if s in {"true", "1", "yes", "ok"}:
        return True
    if s in {"false", "0", "no"}:
        return False
    return None


def _slice_timeseries_by_period(df: pd.DataFrame, period_key: str) -> pd.DataFrame:
    if df is None or df.empty or "timestamp" not in df.columns:
        return pd.DataFrame(columns=df.columns if isinstance(df, pd.DataFrame) else [])

    out = df.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce", utc=True)
    out = out.dropna(subset=["timestamp"]).sort_values("timestamp")
    if out.empty:
        return out

    days = COMPARE_PERIOD_DAYS.get(str(period_key), None)
    if days is None:
        return out

    end_ts = out["timestamp"].max()
    start_ts = end_ts - pd.Timedelta(days=int(days))
    pre = out[out["timestamp"] < start_ts].tail(1)
    cur = out[out["timestamp"] >= start_ts]
    return pd.concat([pre, cur], ignore_index=True).drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp")


def _slice_events_by_period(df: pd.DataFrame, period_key: str, ts_col_candidates: list[str] | None = None, ref_end: object = None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=df.columns if isinstance(df, pd.DataFrame) else [])
    cands = ts_col_candidates or ["timestamp", "updatedat", "created_at", "date"]
    ts_col = _first_existing_column(df, cands)
    if not ts_col:
        return df.copy()

    out = df.copy()
    out[ts_col] = pd.to_datetime(out[ts_col], errors="coerce", utc=True)
    out = out.dropna(subset=[ts_col])
    if out.empty:
        return out

    days = COMPARE_PERIOD_DAYS.get(str(period_key), None)
    if days is None:
        return out

    end_ts = pd.to_datetime(ref_end, errors="coerce", utc=True)
    if pd.isna(end_ts):
        end_ts = out[ts_col].max()
    start_ts = end_ts - pd.Timedelta(days=int(days))
    return out[(out[ts_col] >= start_ts) & (out[ts_col] <= end_ts)]


def _compute_position_pnl_lists(df_port: pd.DataFrame) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    if df_port is None or df_port.empty or "symbol" not in df_port.columns:
        return [], []

    df = df_port.copy()
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df = df[~df["symbol"].isin(["", "CASH_EUR", "__META__"])].copy()
    if df.empty:
        return [], []

    for c in ["marketvalue", "unrealizedpnl", "quantity", "avgprice"]:
        if c not in df.columns:
            df[c] = 0.0
        df[c] = safe_float_series(df[c])

    basis = (df.get("avgprice", 0.0) * df.get("quantity", 0.0)).where(df.get("quantity", 0.0) > 0, df["marketvalue"] - df["unrealizedpnl"])
    df["unrealizedpnl_pct"] = 0.0
    valid = basis != 0
    df.loc[valid, "unrealizedpnl_pct"] = (df.loc[valid, "unrealizedpnl"] / basis[valid]) * 100

    if "name" not in df.columns:
        df["name"] = df["symbol"]
    df["name"] = df["name"].fillna("").astype(str).str.strip().replace("", pd.NA).fillna(df["symbol"])

    top = df.sort_values("unrealizedpnl", ascending=False).head(3)
    worst = df.sort_values("unrealizedpnl", ascending=True).head(3)

    def _rows(src: pd.DataFrame) -> list[dict[str, object]]:
        rows = []
        for _, r in src.iterrows():
            rows.append(
                {
                    "symbol": str(r.get("symbol") or ""),
                    "name": str(r.get("name") or r.get("symbol") or ""),
                    "pnl_eur": float(safe_float(r.get("unrealizedpnl"))),
                    "pnl_pct": float(safe_float(r.get("unrealizedpnl_pct"))),
                }
            )
        return rows

    return _rows(top), _rows(worst)


def _compute_concentration_and_sectors(df_port: pd.DataFrame) -> dict[str, object]:
    out = {
        "equity_pct": 0.0,
        "cash_pct": 0.0,
        "top1_weight_pct": None,
        "top3_weight_pct": None,
        "hhi": None,
        "sector_rows": [],
    }
    if df_port is None or df_port.empty or "symbol" not in df_port.columns:
        return out

    df = df_port.copy()
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    if "marketvalue" not in df.columns:
        df["marketvalue"] = 0.0
    df["marketvalue"] = safe_float_series(df["marketvalue"])

    total_val_local = float(df["marketvalue"].sum()) if not df.empty else 0.0
    cash_val = float(df[df["symbol"] == "CASH_EUR"]["marketvalue"].sum()) if not df.empty else 0.0
    equity_df = df[~df["symbol"].isin(["CASH_EUR", "__META__"])].copy()
    equity_df = equity_df[equity_df["marketvalue"] > 0]
    equity_val = float(equity_df["marketvalue"].sum()) if not equity_df.empty else 0.0

    out["cash_pct"] = (cash_val / total_val_local * 100.0) if total_val_local > 0 else 0.0
    out["equity_pct"] = (equity_val / total_val_local * 100.0) if total_val_local > 0 else 0.0

    if not equity_df.empty and equity_val > 0:
        weights = (equity_df["marketvalue"] / equity_val * 100.0).sort_values(ascending=False)
        out["top1_weight_pct"] = float(weights.head(1).sum()) if not weights.empty else None
        out["top3_weight_pct"] = float(weights.head(3).sum()) if not weights.empty else None
        out["hhi"] = float(((weights / 100.0) ** 2).sum() * 10000.0)

        if "sector" not in equity_df.columns:
            equity_df["sector"] = ""
        equity_df["sector"] = equity_df["sector"].fillna("").astype(str).str.strip().replace("", "Unknown")
        sectors = (
            equity_df.groupby("sector", dropna=False)["marketvalue"].sum().sort_values(ascending=False)
            / equity_val
            * 100.0
        )
        sector_rows = [{"label": str(k), "weight_pct": float(v)} for k, v in sectors.head(5).items()]
        others = float(sectors.iloc[5:].sum()) if len(sectors) > 5 else 0.0
        if others > 0:
            sector_rows.append({"label": "Others", "weight_pct": others})
        out["sector_rows"] = sector_rows

    return out


def _compute_order_completeness(df_tx_raw: pd.DataFrame, run_id: str, trades_this_run: int) -> float | None:
    if df_tx_raw is None or df_tx_raw.empty:
        return 100.0 if int(trades_this_run or 0) == 0 else None

    df = df_tx_raw.copy()
    if "run_id" in df.columns and str(run_id or "").strip():
        df = df[df["run_id"].astype(str).str.strip() == str(run_id).strip()]
    if df.empty:
        return 100.0 if int(trades_this_run or 0) == 0 else None

    for c in ["symbol", "side", "order_type"]:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].fillna("").astype(str).str.strip()
    for c in ["quantity", "price"]:
        if c not in df.columns:
            df[c] = 0.0
        df[c] = safe_float_series(df[c])

    required_ok = (
        (df["symbol"] != "")
        & (df["side"] != "")
        & (df["order_type"] != "")
        & (df["quantity"] > 0)
        & (df["price"] > 0)
    )
    if len(required_ok) == 0:
        return 100.0 if int(trades_this_run or 0) == 0 else None
    return float(required_ok.mean() * 100.0)


def _compute_freshness_score(last_data_update: object, price_coverage_pct: object, ag1_output_ok: bool | None, critical_anoms: int, diag: dict[str, object]) -> tuple[int | None, float | None]:
    dt = pd.to_datetime(last_data_update, errors="coerce", utc=True)
    if pd.isna(dt):
        return None, None

    age_hours = max(0.0, (pd.Timestamp.now(tz="UTC") - dt).total_seconds() / 3600.0)
    score = 100.0
    if age_hours > 3:
        score -= 8
    if age_hours > 12:
        score -= 12
    if age_hours > 24:
        score -= 20
    if age_hours > 72:
        score -= 25

    cov = None if price_coverage_pct is None else safe_float(price_coverage_pct)
    if cov is not None and not pd.isna(cov):
        cov = max(0.0, min(100.0, float(cov)))
        score -= min(25.0, (100.0 - cov) * 0.35)

    if ag1_output_ok is False:
        score -= 20

    score -= min(25.0, max(0, int(critical_anoms)) * 10.0)

    only_ledger = diag.get("positions_only_in_ledger") or []
    only_mtm = diag.get("positions_only_in_mtm") or []
    mtm_is_stale = bool(diag.get("mtm_is_stale"))
    if (only_ledger or only_mtm) and not mtm_is_stale:
        score -= 10

    return int(max(0, min(100, round(score)))), float(age_hours)


def _make_scoreboard_status(payload_status: str, ag1_output_ok: bool | None, freshness_score: int | None, critical_anoms: int, diag: dict[str, object], has_perf: bool) -> tuple[str, list[str]]:
    reasons: list[str] = []
    status = "OK"

    if str(payload_status or "").lower() != "ok":
        return "ERROR", ["Chargement DB KO"]
    if not has_perf:
        status = "WARN"
        reasons.append("Historique perf indisponible")
    if ag1_output_ok is False:
        status = "WARN"
        reasons.append("data_ok_for_trading=false")
    if critical_anoms > 0:
        status = "WARN"
        reasons.append(f"{critical_anoms} alerte(s) critiques 24h")
    if freshness_score is not None and freshness_score < 70:
        status = "WARN"
        reasons.append(f"freshness={freshness_score}/100")
    mtm_is_stale = bool(diag.get("mtm_is_stale"))
    if ((diag.get("positions_only_in_ledger") or []) or (diag.get("positions_only_in_mtm") or [])) and not mtm_is_stale:
        status = "WARN"
        reasons.append("Divergence ledger/MTM")

    return status, reasons


def _build_mini_equity_curve(card: dict[str, object], mode: str = "EUR", y_range: tuple[float, float] | None = None, show_drawdown_overlay: bool = False) -> go.Figure:
    curve = card.get("curve_df")
    accent = str(card.get("accent") or "#60a5fa")
    label = "TotalValue" if str(mode).upper() in {"EUR", "€"} else "Base 100"

    if not isinstance(curve, pd.DataFrame) or curve.empty or "timestamp" not in curve.columns or "display_value" not in curve.columns:
        fig = go.Figure()
        fig.update_layout(
            height=145,
            margin=dict(t=8, b=8, l=8, r=8),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            annotations=[dict(text="No curve", x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False, font=dict(size=10, color="#888"))],
        )
        return fig

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=curve["timestamp"],
            y=curve["display_value"],
            mode="lines",
            line=dict(color=accent, width=2.2),
            fill="tozeroy",
            fillcolor="rgba(99,102,241,0.04)",
            name=label,
            hovertemplate="%{x|%d/%m %H:%M}<br>%{y:.2f}<extra></extra>",
        )
    )
    if show_drawdown_overlay and "drawdown_pct" in curve.columns:
        fig.add_trace(
            go.Scatter(
                x=curve["timestamp"],
                y=curve["drawdown_pct"],
                mode="lines",
                line=dict(color="rgba(239,68,68,0.65)", width=1.2, dash="dot"),
                name="DD%",
                yaxis="y2",
                hovertemplate="%{x|%d/%m %H:%M}<br>DD %{y:.2f}%<extra></extra>",
            )
        )

    fig.update_layout(
        height=150,
        margin=dict(t=8, b=8, l=8, r=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=True, gridcolor="rgba(128,128,128,0.15)", zeroline=False, showticklabels=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0.0, font=dict(size=9)),
    )
    if y_range is not None and all(pd.notna(x) for x in y_range):
        fig.update_yaxes(range=[float(y_range[0]), float(y_range[1])])
    if show_drawdown_overlay:
        fig.update_layout(
            yaxis2=dict(
                overlaying="y",
                side="right",
                showgrid=False,
                zeroline=False,
                showticklabels=False,
                range=[-25, 5],
            )
        )
    return fig


def _build_compare_overlay_chart(cards: list[dict[str, object]], mode: str = "EUR", show_drawdown: bool = False) -> tuple[go.Figure | None, go.Figure | None]:
    fig_eq = go.Figure()
    fig_dd = go.Figure()
    has_eq = False
    has_dd = False

    for c in cards:
        curve = c.get("curve_df")
        if not isinstance(curve, pd.DataFrame) or curve.empty:
            continue
        if "timestamp" not in curve.columns or "display_value" not in curve.columns:
            continue
        name = str(c.get("label") or c.get("key") or "Portfolio")
        color = str(c.get("accent") or "#888")
        fig_eq.add_trace(
            go.Scatter(
                x=curve["timestamp"],
                y=curve["display_value"],
                mode="lines",
                name=name,
                line=dict(width=2.2, color=color),
            )
        )
        has_eq = True
        if show_drawdown and "drawdown_pct" in curve.columns:
            fig_dd.add_trace(
                go.Scatter(
                    x=curve["timestamp"],
                    y=curve["drawdown_pct"],
                    mode="lines",
                    name=name,
                    line=dict(width=2.0, color=color),
                )
            )
            has_dd = True

    if has_eq:
        fig_eq.update_layout(
            height=260,
            margin=dict(t=36, b=20, l=20, r=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(gridcolor="rgba(128,128,128,0.15)"),
            yaxis=dict(gridcolor="rgba(128,128,128,0.15)", title="EUR" if str(mode).upper() in {"EUR", "€"} else "Base 100"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            title=f"Equity curves comparees ({'EUR' if str(mode).upper() in {'EUR', '€'} else 'normalise base 100'})",
        )
    if has_dd:
        fig_dd.update_layout(
            height=210,
            margin=dict(t=36, b=20, l=20, r=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(gridcolor="rgba(128,128,128,0.15)"),
            yaxis=dict(gridcolor="rgba(128,128,128,0.15)", title="Drawdown %"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            title="Drawdown compare",
        )
    return (fig_eq if has_eq else None), (fig_dd if has_dd else None)


def _prepare_compare_card(portfolio_key: str, payload: dict[str, object], period_key: str, curve_mode: str) -> dict[str, object]:
    summary = payload.get("summary", {}) if isinstance(payload, dict) else {}
    diag = payload.get("diagnostics", {}) if isinstance(payload, dict) else {}
    label = str(payload.get("label") or portfolio_key)
    accent = str(payload.get("accent") or "#888")

    df_port = payload.get("df_portfolio", pd.DataFrame()) if isinstance(payload, dict) else pd.DataFrame()
    df_perf_raw = payload.get("df_performance", pd.DataFrame()) if isinstance(payload, dict) else pd.DataFrame()
    df_tx_raw = payload.get("df_transactions", pd.DataFrame()) if isinstance(payload, dict) else pd.DataFrame()
    df_runs = payload.get("df_runs", pd.DataFrame()) if isinstance(payload, dict) else pd.DataFrame()
    df_sig = payload.get("df_ai_signals", pd.DataFrame()) if isinstance(payload, dict) else pd.DataFrame()
    df_alt = payload.get("df_alerts", pd.DataFrame()) if isinstance(payload, dict) else pd.DataFrame()

    perf_ts = _prepare_performance_timeseries(df_perf_raw)
    tx_norm = _prepare_transactions(df_tx_raw)
    perf_period = _slice_timeseries_by_period(perf_ts, period_key)

    ref_end = None
    if perf_ts is not None and not perf_ts.empty and "timestamp" in perf_ts.columns:
        ref_end = pd.to_datetime(perf_ts["timestamp"], errors="coerce", utc=True).max()
    if pd.isna(pd.to_datetime(ref_end, errors="coerce", utc=True)):
        ref_end = summary.get("last_update")

    tx_period = _slice_events_by_period(tx_norm, period_key, ["timestamp"], ref_end)
    sig_period = _slice_events_by_period(df_sig, period_key, ["timestamp"], ref_end)
    alt_period = _slice_events_by_period(df_alt, period_key, ["timestamp"], ref_end)

    init_cap_local = float(safe_float(summary.get("init_cap", 50000.0)) or 50000.0)
    total_val_local = float(safe_float(summary.get("total_val", 0.0)))
    cash_local = float(safe_float(summary.get("cash", 0.0)))
    invest_local = float(safe_float(summary.get("invest", 0.0)))
    roi_pct_local = float(safe_float(summary.get("roi", 0.0)) * 100.0)
    cash_pct_local = float(safe_float(summary.get("cash_pct", 0.0)))
    pnl_total_local = total_val_local - init_cap_local

    period_delta_eur = None
    roi_delta_pp = None
    cash_delta_pp = None
    if perf_period is not None and not perf_period.empty and "total_value" in perf_period.columns:
        perf_period = perf_period.copy()
        perf_period["timestamp"] = pd.to_datetime(perf_period["timestamp"], errors="coerce", utc=True)
        perf_period = perf_period.dropna(subset=["timestamp"]).sort_values("timestamp")
        if not perf_period.empty:
            start_row = perf_period.iloc[0]
            end_row = perf_period.iloc[-1]
            start_total = float(safe_float(start_row.get("total_value")))
            end_total = float(safe_float(end_row.get("total_value")))
            period_delta_eur = end_total - start_total
            if init_cap_local > 0:
                start_roi_pct = ((start_total / init_cap_local) - 1.0) * 100.0
                roi_delta_pp = roi_pct_local - start_roi_pct
            start_cash = float(safe_float(start_row.get("cash_value")))
            start_cash_pct = (start_cash / start_total * 100.0) if start_total > 0 else None
            if start_cash_pct is not None:
                cash_delta_pp = cash_pct_local - start_cash_pct

    underwater = _build_underwater_dataframe(perf_period)
    current_drawdown_pct = float(underwater["drawdown_pct"].iloc[-1]) if not underwater.empty else float(safe_float(summary.get("drawdown_pct", 0.0)))
    risk_cards = _compute_risk_scorecards(perf_period, tx_period)
    max_drawdown_pct = float(risk_cards.get("max_drawdown_pct", 0.0))
    sharpe = risk_cards.get("sharpe")

    vol_30d = None
    if perf_ts is not None and not perf_ts.empty and "timestamp" in perf_ts.columns:
        perf_30 = _slice_timeseries_by_period(perf_ts, "30j")
        if perf_30 is not None and not perf_30.empty:
            rets = perf_30["total_value"].pct_change().replace([float("inf"), float("-inf")], pd.NA).dropna()
            if len(rets) >= 2:
                std = float(rets.std(ddof=0))
                if std > 0:
                    vol_30d = std * (252 ** 0.5) * 100.0

    alloc = _compute_concentration_and_sectors(df_port)
    top_positions, worst_positions = _compute_position_pnl_lists(df_port)

    latest_run_id = str(summary.get("last_run_id") or "").strip()
    latest_run_row = {}
    if df_runs is not None and not df_runs.empty:
        runs = df_runs.copy()
        if "run_id" in runs.columns:
            runs["run_id"] = runs["run_id"].astype(str).str.strip()
            match = runs[runs["run_id"] == latest_run_id] if latest_run_id else pd.DataFrame()
            if not match.empty:
                latest_run_row = match.iloc[0].to_dict()
            elif not runs.empty:
                latest_run_row = runs.iloc[0].to_dict()

    ag1_output_ok = _coerce_bool_or_none(summary.get("last_data_ok_for_trading"))
    if ag1_output_ok is None:
        ag1_output_ok = _coerce_bool_or_none(latest_run_row.get("data_ok_for_trading"))
    price_coverage_pct = summary.get("last_price_coverage_pct")
    if price_coverage_pct is None:
        price_coverage_pct = latest_run_row.get("price_coverage_pct")

    # Critical anomalies in 24h (operational view)
    critical_anoms_24h = 0
    if df_alt is not None and not df_alt.empty:
        alt = df_alt.copy()
        if "timestamp" in alt.columns:
            alt["timestamp"] = pd.to_datetime(alt["timestamp"], errors="coerce", utc=True)
            alt = alt.dropna(subset=["timestamp"])
            alt = alt[alt["timestamp"] >= (pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=24))]
        sev_col = _first_existing_column(alt, ["severity"])
        if sev_col:
            critical_anoms_24h = int((alt[sev_col].astype(str).str.upper() == "CRITICAL").sum())

    trades_this_run = int(safe_float(summary.get("trades_this_run", 0)))
    order_completeness_pct = _compute_order_completeness(df_tx_raw, latest_run_id, trades_this_run)

    # Last data update = max across portfolio/perf/signals/alerts/tx
    last_updates = [pd.to_datetime(summary.get("last_update"), errors="coerce", utc=True)]
    for df_obj, cols in [
        (df_port, ["updatedat"]),
        (perf_ts, ["timestamp"]),
        (tx_norm, ["timestamp"]),
        (df_sig, ["timestamp"]),
        (df_alt, ["timestamp"]),
    ]:
        if isinstance(df_obj, pd.DataFrame) and not df_obj.empty:
            c = _first_existing_column(df_obj, cols)
            if c:
                ts_ser = pd.to_datetime(df_obj[c], errors="coerce", utc=True)
                if ts_ser.notna().any():
                    last_updates.append(ts_ser.max())
    last_data_update = max([t for t in last_updates if pd.notna(t)], default=pd.NaT)

    freshness_score, freshness_age_h = _compute_freshness_score(
        last_data_update,
        price_coverage_pct,
        ag1_output_ok,
        critical_anoms_24h,
        diag if isinstance(diag, dict) else {},
    )

    has_perf = isinstance(perf_ts, pd.DataFrame) and not perf_ts.empty
    status_level, status_reasons = _make_scoreboard_status(
        str(payload.get("status") or ""),
        ag1_output_ok,
        freshness_score,
        critical_anoms_24h,
        diag if isinstance(diag, dict) else {},
        has_perf,
    )

    # Curve payload for mini charts / overlay
    curve_df = pd.DataFrame(columns=["timestamp", "display_value", "drawdown_pct"])
    if isinstance(perf_period, pd.DataFrame) and not perf_period.empty:
        tmp = perf_period.copy()
        tmp["timestamp"] = pd.to_datetime(tmp["timestamp"], errors="coerce", utc=True)
        tmp = tmp.dropna(subset=["timestamp"]).sort_values("timestamp")
        if not tmp.empty:
            tmp_ud = _build_underwater_dataframe(tmp)
            tmp = tmp.merge(tmp_ud, on="timestamp", how="left")
            if curve_mode == "Normalise":
                base = float(safe_float(tmp.iloc[0].get("total_value")))
                tmp["display_value"] = (tmp["total_value"] / base * 100.0) if base > 0 else pd.NA
            else:
                tmp["display_value"] = tmp["total_value"]
            curve_df = tmp[["timestamp", "display_value", "drawdown_pct"]].replace([float("inf"), float("-inf")], pd.NA).dropna(subset=["display_value"])

    # Agent robustness score (operational synthetic score)
    agent_score_components: list[float] = []
    if freshness_score is not None:
        agent_score_components.append(float(freshness_score))
    if order_completeness_pct is not None:
        agent_score_components.append(float(order_completeness_pct))
    if ag1_output_ok is not None:
        agent_score_components.append(100.0 if ag1_output_ok else 40.0)
    agent_score_components.append(max(0.0, 100.0 - (critical_anoms_24h * 20.0)))
    operational_agent_score = float(sum(agent_score_components) / len(agent_score_components)) if agent_score_components else None

    # Optional PM score if present in runs
    pm_score = None
    for candidate in ["agent_score", "pm_score", "score", "score_agent"]:
        if candidate in latest_run_row and latest_run_row.get(candidate) not in [None, ""]:
            pm_score = float(safe_float(latest_run_row.get(candidate)))
            break

    last_run_ts = latest_run_row.get("ts_end") or latest_run_row.get("ts_start") or summary.get("last_update")

    return {
        "key": portfolio_key,
        "label": label,
        "short_label": str(payload.get("short_label") or portfolio_key),
        "accent": accent,
        "payload_status": str(payload.get("status") or ""),
        "error": str(payload.get("error") or ""),
        "summary": summary,
        "diagnostics": diag if isinstance(diag, dict) else {},
        "status_level": status_level,
        "status_reasons": status_reasons,
        "init_cap": init_cap_local,
        "total_val": total_val_local,
        "cash": cash_local,
        "invest": invest_local,
        "pnl_total": pnl_total_local,
        "roi_pct": roi_pct_local,
        "cash_pct": cash_pct_local,
        "positions_count": int(safe_float(summary.get("positions_count", 0))),
        "period_delta_eur": period_delta_eur,
        "roi_delta_pp": roi_delta_pp,
        "cash_delta_pp": cash_delta_pp,
        "current_drawdown_pct": current_drawdown_pct,
        "max_drawdown_pct": max_drawdown_pct,
        "exposure_pct": 100.0 - cash_pct_local if total_val_local > 0 else alloc.get("equity_pct", 0.0),
        "top1_weight_pct": alloc.get("top1_weight_pct"),
        "top3_weight_pct": alloc.get("top3_weight_pct"),
        "hhi": alloc.get("hhi"),
        "volatility_30d_pct": vol_30d,
        "sharpe": sharpe,
        "signals_24h": int(safe_float(summary.get("signals_24h", 0))),
        "alerts_24h": int(safe_float(summary.get("alerts_24h", 0))),
        "trades_period": int(len(tx_period)) if isinstance(tx_period, pd.DataFrame) else 0,
        "trades_this_run": trades_this_run,
        "cum_fees_eur": float(safe_float(summary.get("cum_fees_eur", 0.0))),
        "cum_ai_cost_eur": float(safe_float(summary.get("cum_ai_cost_eur", 0.0))),
        "last_data_update": last_data_update,
        "ag1_output_ok": ag1_output_ok,
        "order_completeness_pct": order_completeness_pct,
        "critical_anoms_24h": critical_anoms_24h,
        "freshness_score": freshness_score,
        "freshness_age_h": freshness_age_h,
        "operational_agent_score": operational_agent_score,
        "pm_agent_score": pm_score,
        "price_coverage_pct": (float(safe_float(price_coverage_pct)) if price_coverage_pct is not None else None),
        "last_run_id": latest_run_id,
        "last_run_ts": last_run_ts,
        "last_model": str(summary.get("last_model") or payload.get("label") or latest_run_row.get("model") or ""),
        "last_model_raw": str(summary.get("last_model_raw") or latest_run_row.get("model") or ""),
        "last_model_source": str(
            summary.get("last_model_source")
            or ("runs" if str(latest_run_row.get("model") or "").strip() else "config")
        ),
        "last_model_mismatch": bool(summary.get("last_model_mismatch", False)),
        "last_strategy_version": str(summary.get("last_strategy_version") or latest_run_row.get("strategy_version") or ""),
        "last_config_version": str(summary.get("last_config_version") or latest_run_row.get("config_version") or ""),
        "last_prompt_version": str(summary.get("last_prompt_version") or latest_run_row.get("prompt_version") or ""),
        "last_decision_summary": str(summary.get("last_decision_summary") or latest_run_row.get("decision_summary") or ""),
        "sector_rows": alloc.get("sector_rows", []),
        "cash_alloc_pct": alloc.get("cash_pct", 0.0),
        "equity_alloc_pct": alloc.get("equity_pct", 0.0),
        "top_positions": top_positions,
        "worst_positions": worst_positions,
        "curve_df": curve_df,
        "df_portfolio": df_port,
        "df_performance": perf_ts,
        "df_transactions_norm": tx_norm,
    }


# ============================================================
# PAGE 1: DASHBOARD
# ============================================================

if page == "Dashboard Trading":
    if not data_dict:
        st.warning(
            "Donnees Google Sheets indisponibles: les vues Portfolio (DuckDB) restent accessibles, "
            "mais certaines sections historiques peuvent etre vides."
        )

    st.title("AI Trading Executor Dashboard")

    ag1_multi = load_ag1_multi_portfolios()
    compare_keys = [k for k in AG1_MULTI_PORTFOLIO_CONFIG.keys() if k in ag1_multi]
    available_keys = [
        k for k, p in ag1_multi.items()
        if isinstance(p, dict) and str(p.get("status", "")).lower() == "ok"
    ]
    compare_cards: list[dict[str, object]] = []

    ctrl_period, ctrl_mode, ctrl_kpi, ctrl_bonus, ctrl_refresh = st.columns([2.0, 1.8, 1.6, 1.4, 1.0], gap="large")
    with ctrl_period:
        compare_period = st.radio(
            "Periode",
            options=["7j", "30j", "90j", "All"],
            horizontal=True,
            key="dashboard_compare_period",
            index=["7j", "30j", "90j", "All"].index(st.session_state.get("dashboard_compare_period", "30j"))
            if st.session_state.get("dashboard_compare_period", "30j") in ["7j", "30j", "90j", "All"] else 1,
        )
    with ctrl_mode:
        compare_curve_mode = st.radio(
            "Affichage",
            options=["EUR", "Normalise"],
            horizontal=True,
            key="dashboard_compare_curve_mode",
            index=["EUR", "Normalise"].index(st.session_state.get("dashboard_compare_curve_mode", "EUR"))
            if st.session_state.get("dashboard_compare_curve_mode", "EUR") in ["EUR", "Normalise"] else 0,
        )
    with ctrl_kpi:
        compare_winner_kpi = st.selectbox(
            "KPI winner / tri",
            options=["ROI", "TotalValue", "MaxDD", "Sharpe"],
            index=["ROI", "TotalValue", "MaxDD", "Sharpe"].index(st.session_state.get("dashboard_compare_winner_kpi", "ROI"))
            if st.session_state.get("dashboard_compare_winner_kpi", "ROI") in ["ROI", "TotalValue", "MaxDD", "Sharpe"] else 0,
            key="dashboard_compare_winner_kpi",
        )
    with ctrl_bonus:
        compare_show_dd = st.checkbox(
            "Drawdown compare",
            value=bool(st.session_state.get("dashboard_compare_show_dd", False)),
            key="dashboard_compare_show_dd",
            help="Affiche la courbe de drawdown comparee sous les 3 colonnes (bonus).",
        )
    with ctrl_refresh:
        st.write("")
        st.write("")
        if st.button("Rafraichir", use_container_width=True):
            load_data.clear()
            load_dashboard_market_data.clear()
            load_ag1_multi_portfolios.clear()
            fetch_benchmarks_history.clear()
            st.rerun()

    # Comparative scoreboard (3 AG1 variants) + Focus on one portfolio for detailed tabs below.
    selected_portfolio_key = None
    active_portfolio = None
    active_positions_source_note = ""

    if compare_keys:
        default_focus = st.session_state.get("dashboard_active_portfolio")
        if default_focus not in compare_keys:
            default_focus = available_keys[0] if available_keys else compare_keys[0]
        selected_portfolio_key = default_focus

        st.caption("Vue comparative AG1-V3 (3 colonnes fixes : portefeuille + qualite agent)")

        cards_by_key: dict[str, dict[str, object]] = {}
        for key in compare_keys:
            card = _prepare_compare_card(key, ag1_multi.get(key, {}), compare_period, compare_curve_mode)
            compare_cards.append(card)
            cards_by_key[key] = card

        # Winner / loser badges based on selected KPI.
        best_key = None
        worst_key = None
        winner_cfg = COMPARE_WINNER_META.get(compare_winner_kpi, COMPARE_WINNER_META["ROI"])
        winner_value_key = winner_cfg.get("key")
        higher_is_better = bool(winner_cfg.get("higher_is_better", True))
        ranked_values = []
        for c in compare_cards:
            val = c.get(winner_value_key) if winner_value_key else None
            if val is None or pd.isna(val):
                continue
            ranked_values.append((c["key"], float(safe_float(val))))
        if ranked_values:
            ranked_values = sorted(ranked_values, key=lambda x: x[1], reverse=higher_is_better)
            best_key = ranked_values[0][0]
            if len(ranked_values) > 1:
                worst_key = ranked_values[-1][0]
                if worst_key == best_key:
                    worst_key = None

        # Shared mini-curve Y range for scanability.
        curve_vals = []
        for c in compare_cards:
            curve_df = c.get("curve_df")
            if isinstance(curve_df, pd.DataFrame) and not curve_df.empty and "display_value" in curve_df.columns:
                curve_vals.extend([float(v) for v in pd.to_numeric(curve_df["display_value"], errors="coerce").dropna().tolist()])
        curve_y_range = None
        if curve_vals:
            ymin, ymax = min(curve_vals), max(curve_vals)
            if ymin == ymax:
                pad = max(1.0, abs(ymin) * 0.02)
            else:
                pad = (ymax - ymin) * 0.08
            curve_y_range = (ymin - pad, ymax + pad)

        status_colors = {"OK": "#16a34a", "WARN": "#d97706", "ERROR": "#dc2626"}

        sb_cols = st.columns(3, gap="large")
        for idx in range(3):
            if idx >= len(compare_keys):
                continue
            key = compare_keys[idx]
            c = cards_by_key[key]
            is_focus = key == selected_portfolio_key
            status_level = str(c.get("status_level") or "WARN").upper()
            status_color = status_colors.get(status_level, "#6b7280")
            accent = str(c.get("accent") or "#6b7280")

            winner_badges = []
            if key == best_key:
                winner_badges.append(f"<span style='padding:2px 8px;border-radius:999px;background:rgba(22,163,74,.12);color:#16a34a;border:1px solid rgba(22,163,74,.35);font-size:0.75rem;'>Best {html.escape(compare_winner_kpi)}</span>")
            if key == worst_key:
                winner_badges.append(f"<span style='padding:2px 8px;border-radius:999px;background:rgba(220,38,38,.10);color:#dc2626;border:1px solid rgba(220,38,38,.30);font-size:0.75rem;'>Worst {html.escape(compare_winner_kpi)}</span>")

            with sb_cols[idx]:
                with st.container(border=True):
                    st.markdown(
                        (
                            "<div style='display:flex;justify-content:space-between;align-items:flex-start;gap:8px;'>"
                            f"<div style='border-left:4px solid {accent};padding-left:8px;'>"
                            f"<div style='font-weight:700;font-size:1rem;'>{html.escape(str(c.get('label') or key))}{' <span style=\"color:#94a3b8;\">[Focus]</span>' if is_focus else ''}</div>"
                            f"<div style='margin-top:4px;display:flex;gap:6px;flex-wrap:wrap;'>"
                            f"<span style='padding:2px 8px;border-radius:999px;background:{status_color}22;color:{status_color};border:1px solid {status_color}55;font-size:0.75rem;font-weight:600;'>{status_level}</span>"
                            + "".join(winner_badges) +
                            "</div></div></div>"
                        ),
                        unsafe_allow_html=True,
                    )

                    last_run_txt = _short_run_id(c.get("last_run_id"))
                    last_run_ts_txt = _fmt_paris_datetime(c.get("last_run_ts"), "%d/%m %H:%M")
                    st.caption(
                        f"Dernier run: {last_run_txt} | {last_run_ts_txt}"
                    )
                    st.caption(
                        f"Model: {c.get('last_model') or 'N/A'} | "
                        f"strategy={c.get('last_strategy_version') or '—'} | "
                        f"config={c.get('last_config_version') or '—'}"
                    )
                    if bool(c.get("last_model_mismatch")) and str(c.get("last_model_raw") or "").strip():
                        st.caption(f"Model source mismatch (core.runs.model={c.get('last_model_raw')}) -> fallback portefeuille")
                    if c.get("status_reasons"):
                        st.caption(" | ".join([str(x) for x in c.get("status_reasons", [])[:2]]))

                    # KPI principaux
                    k1, k2 = st.columns(2)
                    k1.metric("Valeur totale", _fmt_currency(c.get("total_val"), 2), _fmt_delta_eur(c.get("period_delta_eur")))
                    k2.metric("PnL total", _fmt_currency(c.get("pnl_total"), 2), _fmt_delta_eur(c.get("period_delta_eur")))
                    k3, k4 = st.columns(2)
                    k3.metric("ROI", _fmt_pct(c.get("roi_pct"), 2), _fmt_delta_pp(c.get("roi_delta_pp")))
                    k4.metric("% Cash", _fmt_pct(c.get("cash_pct"), 1), _fmt_delta_pp(c.get("cash_delta_pp"), 1))
                    k5, k6 = st.columns(2)
                    k5.metric("Positions", _fmt_number(c.get("positions_count"), 0))
                    k6.metric("Score agent", _fmt_pct(c.get("operational_agent_score"), 0, "/100"))

                    # Mini equity curve
                    st.caption(f"Equity curve ({compare_period} | {'EUR' if compare_curve_mode == 'EUR' else 'Base 100'})")
                    st.plotly_chart(
                        _build_mini_equity_curve(c, mode=compare_curve_mode, y_range=curve_y_range, show_drawdown_overlay=False),
                        use_container_width=True,
                        config={"displayModeBar": False},
                    )

                    # Risque & exposition
                    st.caption("Risque & exposition")
                    r1, r2 = st.columns(2)
                    r1.metric("DD courant", _fmt_pct(c.get("current_drawdown_pct"), 2))
                    r2.metric("MaxDD periode", _fmt_pct(c.get("max_drawdown_pct"), 2))
                    r3, r4 = st.columns(2)
                    r3.metric("Expo actions", _fmt_pct(c.get("exposure_pct"), 1))
                    r4.metric("Top3 poids", _fmt_pct(c.get("top3_weight_pct"), 1) if c.get("top3_weight_pct") is not None else "—")
                    sharpe_val = c.get("sharpe")
                    sharpe_txt = (
                        f"{safe_float(sharpe_val):.2f}"
                        if sharpe_val is not None and not pd.isna(sharpe_val)
                        else "—"
                    )
                    st.caption(
                        f"Top1: {_fmt_pct(c.get('top1_weight_pct'), 1) if c.get('top1_weight_pct') is not None else '—'} | "
                        f"HHI: {_fmt_number(c.get('hhi'), 0) if c.get('hhi') is not None else '—'} | "
                        f"Vol 30j: {_fmt_pct(c.get('volatility_30d_pct'), 1) if c.get('volatility_30d_pct') is not None else '—'} | "
                        f"Sharpe: {sharpe_txt}"
                    )

                    # Activite / couts / ops
                    st.caption("Activite / couts / ops")
                    a1, a2 = st.columns(2)
                    a1.metric("Signaux 24h", _fmt_number(c.get("signals_24h"), 0))
                    a2.metric("Alertes 24h", _fmt_number(c.get("alerts_24h"), 0))
                    a3, a4 = st.columns(2)
                    a3.metric(f"Trades {compare_period}", _fmt_number(c.get("trades_period"), 0))
                    a4.metric("Trades run", _fmt_number(c.get("trades_this_run"), 0))
                    st.caption(
                        f"CumFees: {_fmt_currency(c.get('cum_fees_eur'), 2)} | "
                        f"CumAiCost: {_fmt_currency(c.get('cum_ai_cost_eur'), 2)} | "
                        f"Derniere MAJ data: {_fmt_paris_datetime(c.get('last_data_update'), '%d/%m %H:%M')}"
                    )

                    # Qualite agent AG1
                    st.caption("Qualite agent AG1")
                    ag1_ok = c.get("ag1_output_ok")
                    ag1_ok_txt = "OK" if ag1_ok is True else ("KO" if ag1_ok is False else "—")
                    q1, q2 = st.columns(2)
                    q1.metric("AG1 Output", ag1_ok_txt)
                    q2.metric("Completeness", _fmt_pct(c.get("order_completeness_pct"), 0) if c.get("order_completeness_pct") is not None else "—")
                    q3, q4 = st.columns(2)
                    q3.metric("Anom. critiques", _fmt_number(c.get("critical_anoms_24h"), 0))
                    q4.metric("Freshness", _fmt_pct(c.get("freshness_score"), 0, "/100") if c.get("freshness_score") is not None else "—")
                    cov_txt = _fmt_pct(c.get("price_coverage_pct"), 1) if c.get("price_coverage_pct") is not None else "—"
                    age_txt = f"{float(c.get('freshness_age_h')):.1f}h" if c.get("freshness_age_h") is not None else "—"
                    st.caption(f"Price coverage: {cov_txt} | Age data: {age_txt}")
                    if c.get("pm_agent_score") is not None:
                        st.caption(f"Score agent (PM): {safe_float(c.get('pm_agent_score')):.1f}/100")

                    # Mini allocation
                    st.caption("Allocation (mini)")
                    eq_pct = max(0.0, min(100.0, float(safe_float(c.get("equity_alloc_pct", 0.0)))))
                    cash_pct_bar = max(0.0, min(100.0, float(safe_float(c.get("cash_alloc_pct", 0.0)))))
                    st.caption(f"Equity {eq_pct:.1f}% | Cash {cash_pct_bar:.1f}%")
                    st.progress(int(round(eq_pct)))
                    for srow in c.get("sector_rows", [])[:5]:
                        lbl = str(srow.get("label") or "Unknown")
                        w = float(safe_float(srow.get("weight_pct", 0.0)))
                        st.caption(f"{lbl}: {w:.1f}%")

                    # Top/Worst positions
                    st.caption("Top / Worst positions")
                    tw1, tw2 = st.columns(2)
                    with tw1:
                        st.caption("Top 3 PnL")
                        top_rows = c.get("top_positions") or []
                        if top_rows:
                            for row in top_rows:
                                st.markdown(_position_pnl_row_html(row), unsafe_allow_html=True)
                        else:
                            st.caption("—")
                    with tw2:
                        st.caption("Worst 3 PnL")
                        worst_rows = c.get("worst_positions") or []
                        if worst_rows:
                            for row in worst_rows:
                                st.markdown(_position_pnl_row_html(row), unsafe_allow_html=True)
                        else:
                            st.caption("—")

                    if st.button(
                        "Focus",
                        key=f"focus_{key}",
                        use_container_width=True,
                        type="primary" if is_focus else "secondary",
                    ):
                        st.session_state["dashboard_active_portfolio"] = key
                        st.session_state["active_portfolio_id"] = key
                        st.rerun()

        st.session_state["dashboard_active_portfolio"] = selected_portfolio_key
        st.session_state["active_portfolio_id"] = selected_portfolio_key

        # Bonus: overlay chart compare
        fig_cmp_eq, fig_cmp_dd = _build_compare_overlay_chart(compare_cards, mode=compare_curve_mode, show_drawdown=compare_show_dd)
        if fig_cmp_eq is not None:
            st.plotly_chart(fig_cmp_eq, use_container_width=True)
        if compare_show_dd and fig_cmp_dd is not None:
            st.plotly_chart(fig_cmp_dd, use_container_width=True)

        # Detailed numeric table moved to expander.
        comp_rows = []
        for c in compare_cards:
            comp_rows.append(
                {
                    "Modele": c.get("label"),
                    "Statut": c.get("status_level"),
                    "Valeur Totale (EUR)": c.get("total_val"),
                    "P&L (EUR)": c.get("pnl_total"),
                    "ROI (%)": c.get("roi_pct"),
                    "Cash (%)": c.get("cash_pct"),
                    "MaxDD periode (%)": c.get("max_drawdown_pct"),
                    "Sharpe": c.get("sharpe"),
                    "Score agent (/100)": c.get("operational_agent_score"),
                    "Anom. critiques 24h": c.get("critical_anoms_24h"),
                }
            )
        with st.expander("Details chiffres comparatifs", expanded=False):
            if comp_rows:
                comp_df = pd.DataFrame(comp_rows)
                sort_col_map = {
                    "ROI": "ROI (%)",
                    "TotalValue": "Valeur Totale (EUR)",
                    "MaxDD": "MaxDD periode (%)",
                    "Sharpe": "Sharpe",
                }
                sort_col = sort_col_map.get(compare_winner_kpi, "ROI (%)")
                # MaxDD is typically negative; best is the least negative value.
                ascending = False
                if sort_col in comp_df.columns:
                    comp_df = comp_df.sort_values(sort_col, ascending=ascending, na_position="last")
                st.dataframe(comp_df, use_container_width=True)

        # Focused details zone (same tabs below) uses selected portfolio if healthy, otherwise fallback to first healthy.
        details_key = selected_portfolio_key if selected_portfolio_key in available_keys else (available_keys[0] if available_keys else None)
        if details_key and details_key != selected_portfolio_key:
            st.warning(
                f"Le portefeuille en Focus ({cards_by_key[selected_portfolio_key]['label']}) n'est pas exploitable pour les details. "
                f"Bascule details sur {cards_by_key[details_key]['label']}."
            )

        if details_key:
            active_portfolio = ag1_multi[details_key]
            selected_portfolio_key = details_key

            # Override dashboard datasets/metrics with the selected AG1 portfolio for the detailed tabs.
            selected_summary = active_portfolio.get("summary", {}) if isinstance(active_portfolio, dict) else {}
            df_port = active_portfolio.get("df_portfolio", pd.DataFrame()) if isinstance(active_portfolio, dict) else pd.DataFrame()
            df_perf = active_portfolio.get("df_performance", pd.DataFrame()) if isinstance(active_portfolio, dict) else pd.DataFrame()
            df_trans = active_portfolio.get("df_transactions", pd.DataFrame()) if isinstance(active_portfolio, dict) else pd.DataFrame()
            df_sig_dashboard = active_portfolio.get("df_ai_signals", pd.DataFrame()) if isinstance(active_portfolio, dict) else pd.DataFrame()
            df_alt_dashboard = active_portfolio.get("df_alerts", pd.DataFrame()) if isinstance(active_portfolio, dict) else pd.DataFrame()

            df_port = enrich_df_with_name(df_port, df_univ) if df_port is not None else pd.DataFrame()
            df_trans = enrich_df_with_name(df_trans, df_univ) if df_trans is not None else pd.DataFrame()

            init_cap = safe_float(selected_summary.get("init_cap", 50000.0)) or 50000.0
            total_val = safe_float(selected_summary.get("total_val", 0.0))
            cash = safe_float(selected_summary.get("cash", 0.0))
            invest = safe_float(selected_summary.get("invest", 0.0))
            roi = float(selected_summary.get("roi", 0.0) or 0.0)
            cash_pct = float(selected_summary.get("cash_pct", 0.0) or 0.0)

            last_model_txt = str(selected_summary.get("last_model", "") or active_portfolio.get("label", "") or "")
            last_model_raw_txt = str(selected_summary.get("last_model_raw", "") or "")
            last_model_mismatch = bool(selected_summary.get("last_model_mismatch", False))
            last_update_ts = pd.to_datetime(selected_summary.get("last_update"), errors="coerce", utc=True)
            last_update_txt = (
                last_update_ts.tz_convert("Europe/Paris").strftime("%Y-%m-%d %H:%M")
                if pd.notna(last_update_ts)
                else "N/A"
            )
            st.info(
                f"Allocation active (Focus): {active_portfolio.get('label', selected_portfolio_key)} | "
                f"Dernier run: {selected_summary.get('last_run_id', 'N/A')} | "
                f"Modele: {last_model_txt or 'N/A'} | MAJ: {last_update_txt}"
            )
            if last_model_mismatch and last_model_raw_txt:
                st.caption(f"Model source mismatch (core.runs.model={last_model_raw_txt}) -> fallback portefeuille")

            diag = active_portfolio.get("diagnostics", {}) if isinstance(active_portfolio, dict) else {}
            if isinstance(diag, dict):
                ledger_run = str(diag.get("ledger_run_id") or selected_summary.get("last_run_id") or "").strip() or "N/A"
                mtm_run = str(diag.get("mtm_run_id") or "").strip() or "N/A"
                mtm_source_run = str(diag.get("mtm_source_run_id") or "").strip() or "N/A"
                mtm_match_col = str(diag.get("mtm_match_col") or "run_id").strip() or "run_id"
                mtm_age_hours = diag.get("mtm_age_hours")
                mtm_is_stale = bool(diag.get("mtm_is_stale"))
                mtm_age_txt = ""
                if mtm_age_hours is not None:
                    try:
                        mtm_age_txt = f", age~{float(mtm_age_hours):.1f}h"
                    except Exception:
                        mtm_age_txt = ""
                active_positions_source_note = (
                    "Source Positions: AG1-V3 ledger `core.positions_snapshot` "
                    f"(run_id={ledger_run}) | Miroir MTM `portfolio_positions_mtm_latest` "
                    f"(run_id={mtm_run}, source_run_id={mtm_source_run}, match_col={mtm_match_col}{mtm_age_txt})"
                )

                only_ledger = [str(s) for s in (diag.get("positions_only_in_ledger") or []) if str(s).strip()]
                only_mtm = [str(s) for s in (diag.get("positions_only_in_mtm") or []) if str(s).strip()]
                mtm_reason = str(diag.get("mtm_reason") or "").strip()
                if mtm_reason == "run_id_mismatch":
                    st.info(
                        "Miroir MTM sur un run different du ledger actif: comparaison des positions desactivee pour eviter un faux ecart."
                    )
                if only_ledger or only_mtm:
                    diff_parts = []
                    if only_ledger:
                        diff_parts.append("uniquement dans ledger: " + ", ".join(only_ledger))
                    if only_mtm:
                        diff_parts.append("uniquement dans MTM: " + ", ".join(only_mtm))
                    if mtm_is_stale:
                        st.info(
                            "Miroir MTM obsolete: divergence ledger/MTM ignoree pour ce run "
                            f"(run_id={mtm_run}{mtm_age_txt})."
                        )
                    else:
                        st.warning("Ecart detecte entre `core.positions_snapshot` et `portfolio_positions_mtm_latest` (" + " | ".join(diff_parts) + ")")
        else:
            st.warning(
                "Aucune base AG1-V3 exploitable pour la zone details. "
                "Affichage du mode legacy (single portfolio) si les donnees historiques sont presentes."
            )
            df_sig_dashboard = pd.DataFrame()
            df_alt_dashboard = pd.DataFrame()
            active_positions_source_note = "Source Positions: mode legacy `portfolio_positions_mtm_latest` via `AG1_DUCKDB_PATH`"
    else:
        st.warning(
            "Aucun portefeuille AG1-V3 configure pour la vue comparative. "
            "Affichage du mode legacy (single portfolio) si les donnees historiques sont presentes."
        )
        df_sig_dashboard = pd.DataFrame()
        df_alt_dashboard = pd.DataFrame()
        active_positions_source_note = "Source Positions: mode legacy `portfolio_positions_mtm_latest` via `AG1_DUCKDB_PATH`"

    if active_positions_source_note:
        st.caption(active_positions_source_note)

    # Detailed KPI band (active portfolio / legacy fallback)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Capital Depart", f"{init_cap:,.0f} EUR")
    c2.metric("Valeur Totale", f"{total_val:,.2f} EUR", delta=f"{total_val - init_cap:,.2f} EUR")
    c3.metric("Cash", f"{cash:,.2f} EUR")
    c4.metric("Investi", f"{invest:,.2f} EUR")
    c5.metric("ROI", f"{roi * 100:.2f} %")
    c6.metric("% Cash", f"{cash_pct:.1f} %")

    t1, t2, t3, t4, t5 = st.tabs(
        [
            "Allocation (actif)",
            "Rendement (actif)",
            "Cerveau IA (actif)",
            "Marche & Recherche (global)",
            "Benchmarks & Indices",
        ]
    )

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
            port_subtab_sparks, port_subtab_positions = st.tabs(["Sparklines (90j)", "Positions"])

            with port_subtab_sparks:
                st.subheader("Portfolio Sparklines (90j)")
                render_portfolio_sparklines(
                    df_clean,
                    df_trans,
                    yfinance_api_url=YFINANCE_API_URL,
                    lookback_days=90,
                    columns_per_row=3,
                )

            with port_subtab_positions:
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
                pos_key = f"positions_{selected_portfolio_key}" if selected_portfolio_key else "positions"
                render_interactive_table(df_view, key_suffix=pos_key, hide_index=True)
        else:
            st.info("Allocation vide.")

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
                "1) Rendement Financier",
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
        if "df_sig_dashboard" in locals() and df_sig_dashboard is not None and not df_sig_dashboard.empty:
            df_sig = enrich_df_with_name(df_sig_dashboard, df_univ)
        else:
            df_sig = pd.DataFrame()

        if "df_alt_dashboard" in locals() and df_alt_dashboard is not None and not df_alt_dashboard.empty:
            df_alt = enrich_df_with_name(df_alt_dashboard, df_univ)
        else:
            df_alt = pd.DataFrame()

        if selected_portfolio_key and active_portfolio:
            st.caption(
                f"Source AG1-V3: {active_portfolio.get('label', selected_portfolio_key)} "
                f"({active_portfolio.get('db_path', '')})"
            )
        else:
            st.caption("Source legacy (DuckDB)")

        st.subheader("🚦 Signaux")
        if df_sig is not None and not df_sig.empty:
            if "rationale" in df_sig.columns:
                df_sig["rationale"] = df_sig["rationale"].apply(clean_text)
            sig_key = f"sig_{selected_portfolio_key}" if selected_portfolio_key else "sig"
            render_interactive_table(df_sig, key_suffix=sig_key)
        else:
            st.caption("Aucun signal.")

        st.subheader("Alertes")
        if df_alt is not None and not df_alt.empty:
            alt_key = f"alt_{selected_portfolio_key}" if selected_portfolio_key else "alt"
            render_interactive_table(df_alt, key_suffix=alt_key)
        else:
            st.caption("RAS")

    # TAB 4: MARCHE & RECHERCHE
    with t4:
        dashboard_market_data = load_dashboard_market_data(
            AG3_DUCKDB_PATH,
            ag3_db_sig,
            AG4_DUCKDB_PATH,
            ag4_db_sig,
            AG4_SPE_DUCKDB_PATH,
            ag4_spe_db_sig,
            HISTORY_DAYS_DEFAULT,
            HISTORY_LIMIT_DEFAULT,
            RUN_LOG_LIMIT,
        )
        df_news = _normalize_macro_news_df(dashboard_market_data.get("df_news_macro_history", pd.DataFrame()))
        df_news_sym = _normalize_symbol_news_df(dashboard_market_data.get("df_news_symbol_history", pd.DataFrame()))
        df_res = _load_fundamentals_for_dashboard(dashboard_market_data)
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

    # TAB 5: BENCHMARKS & INDICES
    with t5:
        st.subheader("Benchmarks / Indices")
        st.caption("Comparaison AG1 (GPT/Grok/Gemini) vs CAC 40 / S&P 500 / EURO STOXX 50 sur la meme fenetre.")

        if not compare_cards:
            st.info("Comparatif AG1 indisponible: aucun portefeuille multi-modele charge.")
        else:
            benchmark_labels_all = list(BENCHMARKS_CONFIG.keys())
            if not benchmark_labels_all:
                st.warning("Configuration benchmarks vide. Verifiez `BENCHMARK_TICKERS_JSON`.")
                st.stop()

            b1, b2, b3 = st.columns([2.0, 1.6, 1.4], gap="large")
            selected_benchmarks = b1.multiselect(
                "Benchmarks",
                options=benchmark_labels_all,
                default=benchmark_labels_all,
                key="dashboard_bench_selected",
            )

            ref_default = "CAC 40" if "CAC 40" in selected_benchmarks else (selected_benchmarks[0] if selected_benchmarks else "")
            alpha_ref = b2.selectbox(
                "Benchmark reference (alpha)",
                options=selected_benchmarks if selected_benchmarks else benchmark_labels_all,
                index=(
                    (selected_benchmarks if selected_benchmarks else benchmark_labels_all).index(ref_default)
                    if ref_default in (selected_benchmarks if selected_benchmarks else benchmark_labels_all)
                    else 0
                ),
                key="dashboard_bench_alpha_ref",
            )
            bench_mode = b3.radio(
                "Mode",
                options=["Normalise (base 100)", "Perf (%)"],
                horizontal=False,
                key="dashboard_bench_mode",
            )

            portfolio_series_norm: dict[str, pd.DataFrame] = {}
            portfolio_accent: dict[str, str] = {}
            portfolio_missing: list[str] = []
            min_portfolio_starts: list[pd.Timestamp] = []

            for card in compare_cards:
                p_label = str(card.get("label") or card.get("key") or "").strip()
                if not p_label:
                    continue
                portfolio_accent[p_label] = str(card.get("accent") or "#7c7c7c")

                raw_series = pd.DataFrame()
                perf_df = card.get("df_performance", pd.DataFrame())
                if isinstance(perf_df, pd.DataFrame) and not perf_df.empty and {"timestamp", "total_value"}.issubset(set(perf_df.columns)):
                    perf_period = _slice_timeseries_by_period(perf_df.copy(), compare_period)
                    if perf_period is not None and not perf_period.empty:
                        raw_series = perf_period[["timestamp", "total_value"]].rename(columns={"total_value": "value"})

                if raw_series.empty:
                    curve_df = card.get("curve_df", pd.DataFrame())
                    if isinstance(curve_df, pd.DataFrame) and not curve_df.empty and {"timestamp", "display_value"}.issubset(set(curve_df.columns)):
                        raw_series = curve_df[["timestamp", "display_value"]].rename(columns={"display_value": "value"})

                norm_series = normalize_to_base100(raw_series, ts_col="timestamp", value_col="value")
                if norm_series.empty:
                    portfolio_missing.append(p_label)
                    continue

                portfolio_series_norm[p_label] = norm_series
                first_ts = pd.to_datetime(norm_series["timestamp"], errors="coerce", utc=True).min()
                if pd.notna(first_ts):
                    min_portfolio_starts.append(first_ts)

            if portfolio_missing:
                st.warning("Series portefeuille indisponibles: " + ", ".join(portfolio_missing))

            min_start_ts = min(min_portfolio_starts) if min_portfolio_starts else pd.NaT
            lookback_days = _benchmark_lookback_days(compare_period, min_start_ts)
            selected_tickers = tuple(
                BENCHMARKS_CONFIG.get(lbl, {}).get("ticker", "")
                for lbl in selected_benchmarks
                if BENCHMARKS_CONFIG.get(lbl, {}).get("ticker", "")
            )
            benchmarks_raw = fetch_benchmarks_history(
                selected_tickers,
                YFINANCE_API_URL,
                lookback_days=lookback_days,
                interval="1d",
            )

            benchmark_series_norm: dict[str, pd.DataFrame] = {}
            benchmark_kpis: list[dict[str, object]] = []
            benchmark_missing: list[str] = []

            for bench_label in selected_benchmarks:
                ticker = str(BENCHMARKS_CONFIG.get(bench_label, {}).get("ticker", "")).strip().upper()
                if not ticker:
                    benchmark_missing.append(bench_label)
                    continue

                raw = benchmarks_raw.get(ticker, pd.DataFrame())
                if raw is None or raw.empty or not {"timestamp", "close"}.issubset(set(raw.columns)):
                    benchmark_missing.append(f"{bench_label} ({ticker})")
                    continue

                wk = raw[["timestamp", "close"]].rename(columns={"close": "total_value"})
                wk = _slice_timeseries_by_period(wk, compare_period)
                if wk is None or wk.empty:
                    benchmark_missing.append(f"{bench_label} ({ticker})")
                    continue

                norm = normalize_to_base100(
                    wk.rename(columns={"total_value": "value"}),
                    ts_col="timestamp",
                    value_col="value",
                )
                if norm.empty:
                    benchmark_missing.append(f"{bench_label} ({ticker})")
                    continue

                benchmark_series_norm[bench_label] = norm
                k_last = pd.to_numeric(wk.get("total_value", pd.Series(dtype=float)), errors="coerce").dropna()
                k_last_close = float(k_last.iloc[-1]) if not k_last.empty else pd.NA
                k_ret = _series_period_return_pct(pd.to_numeric(wk.get("total_value", pd.Series(dtype=float)), errors="coerce"))
                k_ts = pd.to_datetime(wk.get("timestamp", pd.Series(dtype=object)), errors="coerce", utc=True).dropna()
                k_last_ts = k_ts.iloc[-1] if not k_ts.empty else pd.NaT

                benchmark_kpis.append(
                    {
                        "label": bench_label,
                        "ticker": ticker,
                        "last_close": k_last_close,
                        "return_pct": k_ret,
                        "last_ts": k_last_ts,
                    }
                )

            if benchmark_missing:
                st.warning("Benchmarks ignores (ticker invalide/vide): " + ", ".join(benchmark_missing))

            if benchmark_kpis:
                kpi_cols = st.columns(len(benchmark_kpis))
                for idx, row in enumerate(benchmark_kpis):
                    close_txt = f"{safe_float(row.get('last_close')):,.2f}".replace(",", " ") if pd.notna(row.get("last_close")) else "N/A"
                    ret_v = row.get("return_pct")
                    delta_txt = f"{float(ret_v):+.2f}%" if ret_v is not None and pd.notna(ret_v) else "N/A"
                    kpi_cols[idx].metric(f"{row.get('label')} ({row.get('ticker')})", close_txt, delta=delta_txt)
                    ts = pd.to_datetime(row.get("last_ts"), errors="coerce", utc=True)
                    kpi_cols[idx].caption(f"Derniere barre: {_fmt_dt_short(ts)}")

            combined_series: dict[str, pd.DataFrame] = {}
            for p_label, p_df in portfolio_series_norm.items():
                combined_series[p_label] = p_df
            for b_label, b_df in benchmark_series_norm.items():
                combined_series[b_label] = b_df

            aligned_norm = _align_daily_normalized_series(combined_series)
            if aligned_norm is None or aligned_norm.empty:
                st.info("Donnees insuffisantes pour construire la comparaison portefeuille/indices.")
            else:
                value_cols = [c for c in aligned_norm.columns if c != "date"]
                # Rebase all series on the first common displayed date so every curve starts at 100 (or 0% in Perf mode).
                aligned_base = aligned_norm.copy()
                for col in value_cols:
                    base_val = pd.to_numeric(pd.Series([aligned_base[col].iloc[0]]), errors="coerce").iloc[0]
                    if pd.notna(base_val) and float(base_val) != 0.0:
                        aligned_base[col] = (pd.to_numeric(aligned_base[col], errors="coerce") / float(base_val)) * 100.0

                chart_df = aligned_base.copy()
                if bench_mode == "Perf (%)":
                    for col in value_cols:
                        chart_df[col] = pd.to_numeric(chart_df[col], errors="coerce") - 100.0
                    y_title = "Performance cumulee (%)"
                else:
                    y_title = "Base 100"

                fig_cmp = go.Figure()
                for p_label in portfolio_series_norm.keys():
                    if p_label not in chart_df.columns:
                        continue
                    fig_cmp.add_trace(
                        go.Scatter(
                            x=chart_df["date"],
                            y=chart_df[p_label],
                            mode="lines",
                            name=p_label,
                            line=dict(color=portfolio_accent.get(p_label, "#7c7c7c"), width=2.6),
                        )
                    )
                for b_label in benchmark_series_norm.keys():
                    if b_label not in chart_df.columns:
                        continue
                    fig_cmp.add_trace(
                        go.Scatter(
                            x=chart_df["date"],
                            y=chart_df[b_label],
                            mode="lines",
                            name=b_label,
                            line=dict(width=2.2, dash="dash"),
                        )
                    )

                fig_cmp.update_layout(
                    title=f"Portefeuilles AG1 vs Benchmarks ({compare_period})",
                    height=460,
                    margin=dict(t=50, b=20, l=20, r=20),
                    yaxis=dict(title=y_title),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                st.plotly_chart(fig_cmp, use_container_width=True)

                perf_rows: list[dict[str, object]] = []
                for col in value_cols:
                    s = pd.to_numeric(aligned_base[col], errors="coerce").dropna()
                    if s.empty:
                        continue
                    ret = _series_period_return_pct(s)
                    daily_ret = s.pct_change().replace([float("inf"), float("-inf")], pd.NA).dropna()
                    vol = float(daily_ret.std(ddof=0) * (252 ** 0.5) * 100.0) if len(daily_ret) >= 2 else pd.NA
                    dd = ((s / s.cummax()) - 1.0) * 100.0
                    max_dd = float(dd.min()) if not dd.empty else pd.NA
                    perf_rows.append(
                        {
                            "Serie": col,
                            "Type": "Portfolio" if col in portfolio_series_norm else "Benchmark",
                            "Return periode (%)": round(float(ret), 2) if ret is not None and pd.notna(ret) else pd.NA,
                            "Vol annualisee (%)": round(float(vol), 2) if pd.notna(vol) else pd.NA,
                            "MaxDD (%)": round(float(max_dd), 2) if pd.notna(max_dd) else pd.NA,
                        }
                    )

                if perf_rows:
                    st.markdown("#### Synthese performance")
                    render_interactive_table(pd.DataFrame(perf_rows), key_suffix="benchmarks_perf_table", height=280)

                if alpha_ref in aligned_base.columns:
                    alpha_rows: list[dict[str, object]] = []
                    fig_alpha = go.Figure()
                    for p_label in portfolio_series_norm.keys():
                        if p_label not in aligned_base.columns:
                            continue
                        pair = aligned_base[["date", p_label, alpha_ref]].dropna()
                        if pair.empty:
                            continue
                        alpha_curve = pd.to_numeric(pair[p_label], errors="coerce") - pd.to_numeric(pair[alpha_ref], errors="coerce")
                        if alpha_curve.dropna().empty:
                            continue
                        port_ret = _series_period_return_pct(pair[p_label])
                        bench_ret = _series_period_return_pct(pair[alpha_ref])
                        alpha_pp = (
                            float(port_ret) - float(bench_ret)
                            if port_ret is not None and bench_ret is not None and pd.notna(port_ret) and pd.notna(bench_ret)
                            else pd.NA
                        )
                        alpha_rows.append(
                            {
                                "Portfolio": p_label,
                                "Return portfolio (%)": round(float(port_ret), 2) if port_ret is not None and pd.notna(port_ret) else pd.NA,
                                f"Return {alpha_ref} (%)": round(float(bench_ret), 2) if bench_ret is not None and pd.notna(bench_ret) else pd.NA,
                                "Alpha (pp)": round(float(alpha_pp), 2) if pd.notna(alpha_pp) else pd.NA,
                            }
                        )

                        fig_alpha.add_trace(
                            go.Scatter(
                                x=pair["date"],
                                y=alpha_curve,
                                mode="lines",
                                name=p_label,
                                line=dict(color=portfolio_accent.get(p_label, "#7c7c7c"), width=2.4),
                            )
                        )

                    if alpha_rows:
                        st.markdown("#### Alpha vs benchmark de reference")
                        render_interactive_table(pd.DataFrame(alpha_rows), key_suffix="benchmarks_alpha_table", height=220)
                        fig_alpha.add_hline(y=0.0, line_dash="dot", line_color="#888")
                        fig_alpha.update_layout(
                            title=f"Courbe alpha vs {alpha_ref}",
                            height=360,
                            margin=dict(t=45, b=20, l=20, r=20),
                            yaxis=dict(title="Alpha (points base 100)"),
                            legend=dict(orientation="h", yanchor="bottom", y=1.02),
                        )
                        st.plotly_chart(fig_alpha, use_container_width=True)

                st.caption(f"Fenetre={compare_period} | Lookback indices={lookback_days} jours | Source indices={YFINANCE_API_URL}")


# ============================================================
# PAGE 2: SYSTEM HEALTH (MONITORING)
# ============================================================

elif page == "System Health (Monitoring)":
    st.title("System Health - Fraicheur des donnees")
    st.caption("Controle par symbole (AG2/AG3/AG4-SPE) + controle macro global (AG4) + verification des derniers runs.")

    if st.button("Rafraichir", key="refresh_system_health"):
        load_data.clear()
        load_system_health_page_data.clear()
        st.rerun()

    system_health_data = load_system_health_page_data(
        DUCKDB_PATH,
        ag2_db_sig,
        AG3_DUCKDB_PATH,
        ag3_db_sig,
        AG4_DUCKDB_PATH,
        ag4_db_sig,
        AG4_SPE_DUCKDB_PATH,
        ag4_spe_db_sig,
        HISTORY_DAYS_DEFAULT,
        HISTORY_LIMIT_DEFAULT,
        RUN_LOG_LIMIT,
    )

    # Sources
    tech_latest = normalize_cols(system_health_data.get("df_signals", pd.DataFrame()).copy())
    funda_latest = _load_fundamentals_for_dashboard(system_health_data)
    symbol_news = _normalize_symbol_news_df(system_health_data.get("df_news_symbol_history", pd.DataFrame()))
    macro_news = _normalize_macro_news_df(system_health_data.get("df_news_macro_history", pd.DataFrame()))

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
        _latest_run_snapshot(system_health_data.get("df_runs", pd.DataFrame()), "AG2 Technique"),
        _latest_run_snapshot(system_health_data.get("df_funda_runs", pd.DataFrame()), "AG3 Fondamentale"),
        _latest_run_snapshot(system_health_data.get("df_news_macro_runs", pd.DataFrame()), "AG4 Macro"),
        _latest_run_snapshot(system_health_data.get("df_news_symbol_runs", pd.DataFrame()), "AG4 SPE News Symbole"),
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
        load_multi_agent_page_data.clear()
        st.rerun()

    multi_agent_data = load_multi_agent_page_data(
        DUCKDB_PATH,
        ag2_db_sig,
        AG3_DUCKDB_PATH,
        ag3_db_sig,
        AG4_DUCKDB_PATH,
        ag4_db_sig,
        AG4_SPE_DUCKDB_PATH,
        ag4_spe_db_sig,
        YF_ENRICH_DUCKDB_PATH,
        yf_db_sig,
        HISTORY_DAYS_DEFAULT,
        HISTORY_LIMIT_DEFAULT,
        RUN_LOG_LIMIT,
    )

    df_funda_for_view = _load_fundamentals_for_dashboard(multi_agent_data)
    consolidated, macro_news_norm, symbol_news_norm = _prepare_multi_agent_view(
        df_universe=df_univ,
        df_tech_latest=multi_agent_data.get("df_signals", pd.DataFrame()),
        df_funda_latest=df_funda_for_view,
        df_macro_news=multi_agent_data.get("df_news_macro_history", pd.DataFrame()),
        df_symbol_news=multi_agent_data.get("df_news_symbol_history", pd.DataFrame()),
    )

    if consolidated is None or consolidated.empty:
        st.warning("Aucune vue consolidee disponible. Verifiez les bases DuckDB AG2/AG3/AG4.")
        st.stop()

    matrix_df = _build_multi_agent_matrix(
        consolidated,
        df_port,
        multi_agent_data.get("df_yf_enrichment_latest", pd.DataFrame()),
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
            rr_outliers = int(view_df.get("rr_outlier", pd.Series(False, index=view_df.index)).fillna(False).astype(bool).sum())
            low_quality = int((safe_float_series(view_df.get("data_quality_score", pd.Series(0.0, index=view_df.index))).fillna(0.0) < 60.0).sum())
            invalid_opt_state_count = int(view_df.get("invalid_options_state", pd.Series(False, index=view_df.index)).fillna(False).astype(bool).sum())
            risk_thr = safe_float(view_df.get("risk_threshold_dyn", pd.Series([50.0])).iloc[0] if total_values else 50.0)
            reward_thr = safe_float(view_df.get("reward_threshold_dyn", pd.Series([50.0])).iloc[0] if total_values else 50.0)
            grade_a_thr = safe_float(view_df.get("grade_a_threshold", pd.Series([70.0])).iloc[0] if total_values else 70.0)
            grade_b_thr = safe_float(view_df.get("grade_b_threshold", pd.Series([55.0])).iloc[0] if total_values else 55.0)
            enrich_cov = (
                float(view_df.get("has_enrichment", pd.Series(False, index=view_df.index)).fillna(False).astype(bool).mean()) * 100.0
                if total_values
                else 0.0
            )
            freshness_df, freshness_summary = _build_multi_agent_data_freshness(multi_agent_data, view_df)

            st.markdown("#### Comment lire cette page en 30 secondes")
            st.markdown(
                """
1. Lisez le bandeau KPI pour voir rapidement le nombre de valeurs a **entrer**, **surveiller** ou **reduire/sortir**.
2. Dans la matrice: **X = Risque (0-100)**, **Y = Reward (0-100)**. Le cadrant **haut-gauche** est la zone prioritaire.
3. **Couleur = grade**, **forme = decision**, **taille = |EV(R)|**, **bordure verte/rouge = EV positif/negatif**.
4. Survolez un point pour les details complets, puis cliquez pour afficher la fiche rapide de la valeur.
5. Verifiez la section **Methodologie + Fraicheur des donnees** avant toute execution.
"""
            )

            def _render_kpi_block(col, metric_id: str, value: object, delta: object | None = None, overrides: dict[str, str] | None = None) -> None:
                with col:
                    st.metric(_metric_meta(metric_id).get("label", metric_id), value, delta=delta)
                    _render_metric_help_popover(metric_id, unique_suffix=f"kpi_{metric_id}", overrides=overrides)

            k1, k2, k3, k4, k5 = st.columns(5)
            _render_kpi_block(k1, "total_values", total_values)
            _render_kpi_block(k2, "enter_count", enter_count)
            _render_kpi_block(k3, "watch_count", watch_count)
            _render_kpi_block(k4, "exit_count", exit_count)
            _render_kpi_block(k5, "avg_ev_r", f"{avg_ev:.2f}")

            k6, k7, k8, k9, k10 = st.columns(5)
            _render_kpi_block(
                k6,
                "grade_a_count",
                grade_a,
                overrides={
                    "definition_long": (
                        f"Setups classes A. Seuils du jour: A >= {grade_a_thr:.1f}, "
                        f"B >= {grade_b_thr:.1f}, sinon C."
                    ),
                    "formula": f"COUNT(setup_grade == 'A') avec A >= {grade_a_thr:.1f}",
                },
            )
            _render_kpi_block(k7, "rr_outliers", rr_outliers)
            _render_kpi_block(k8, "low_data_quality", low_quality)
            _render_kpi_block(k9, "options_missing", opt_missing)
            _render_kpi_block(k10, "invalid_options_state_count", invalid_opt_state_count)

            if enrich_cov < 85:
                st.warning(f"Couverture enrichissement YF insuffisante: {enrich_cov:.1f}% des symboles ont une ligne daily.")
            else:
                st.caption(f"Couverture enrichissement YF: {enrich_cov:.1f}%")

            st.markdown("#### Legende visuelle")
            lg1, lg2, lg3 = st.columns([1.2, 1.1, 1.7])
            with lg1:
                st.markdown(
                    f"""
- **Couleur = Grade**: A ({GRADE_COLOR_MAP['A']}), B ({GRADE_COLOR_MAP['B']}), C ({GRADE_COLOR_MAP['C']})
- **Alternative daltonisme**: contour aussi informatif  
  A = epais ({GRADE_CONTOUR_WIDTH_MAP['A']:.1f}px), B = moyen ({GRADE_CONTOUR_WIDTH_MAP['B']:.1f}px), C = fin ({GRADE_CONTOUR_WIDTH_MAP['C']:.1f}px)
"""
                )
            with lg2:
                st.markdown(
                    """
- **Forme = Decision**
  triangle-up = Entrer / Renforcer
  circle = Surveiller
  x = Reduire / Sortir
- **Bordure = signe EV(R)**
  verte = EV(R) > 0, rouge = EV(R) < 0
"""
                )
            with lg3:
                st.markdown(
                    f"""
- **Axes**
  X = Risque (0 faible -> 100 eleve), seuil p60 = **{risk_thr:.0f}**  
  Y = Reward (0 faible -> 100 fort), seuil p60 = **{reward_thr:.0f}**
- **Grades dynamiques**
  A >= **{grade_a_thr:.1f}**, B >= **{grade_b_thr:.1f}**, sinon C
- **Taille = |EV(R)|**
  petite = signal faible proche de 0  
  grande = conviction forte; grande+verte favorable, grande+rouge defavorable
"""
                )
                _render_metric_help_popover(
                    "setup_grade",
                    unique_suffix="grade_legend",
                    overrides={
                        "definition_long": (
                            f"Classes du jour: A >= {grade_a_thr:.1f}, B >= {grade_b_thr:.1f}, sinon C. "
                            "A = top relatif de l'univers; B = intermediaire; C = faible qualite relative."
                        )
                    },
                )

            st.markdown("#### Comment la decision est prise")
            st.markdown(
                f"""
1. **Scores de base**: calcul de Risk, Reward, R, p_win, EV(R).
2. **Quadrants dynamiques**: application des seuils p60 du jour (`Risk={risk_thr:.0f}`, `Reward={reward_thr:.0f}`).
3. **Gates**: data quality, earnings proches, liquidite, RR outlier, invalid options state.
4. **Decision finale**: regles metier -> Entrer / Renforcer, Surveiller, ou Reduire / Sortir.
5. **Sizing**: taille recommandee derivee de EV(R), du risque relatif et de la qualite data.
"""
            )

            plot_df = view_df.copy()
            plot_df["bubble_size"] = (plot_df.get("ev_r", pd.Series(0.0, index=plot_df.index)).abs() * 24.0 + 10.0).clip(lower=10, upper=60)
            plot_df["p_win_pct"] = plot_df.get("p_win", pd.Series(0.0, index=plot_df.index)) * 100.0
            plot_df["risk_score_u"] = safe_float_series(plot_df.get("risk_score_u", pd.Series(50.0, index=plot_df.index))).fillna(50.0)
            plot_df["reward_score_u"] = safe_float_series(plot_df.get("reward_score_u", pd.Series(50.0, index=plot_df.index))).fillna(50.0)
            plot_df["risk_score_plot"] = safe_float_series(plot_df.get("risk_score_plot", plot_df["risk_score_u"])).fillna(50.0)
            plot_df["reward_score_plot"] = safe_float_series(plot_df.get("reward_score_plot", plot_df["reward_score_u"])).fillna(50.0)
            plot_df["name"] = plot_df.get("name", pd.Series("", index=plot_df.index)).fillna("").astype(str)
            plot_df["sector"] = plot_df.get("sector", pd.Series("", index=plot_df.index)).fillna("").astype(str)
            plot_df["matrix_action"] = plot_df.get("matrix_action", pd.Series("Surveiller", index=plot_df.index)).fillna("Surveiller").astype(str)
            plot_df["setup_grade"] = plot_df.get("setup_grade", pd.Series("C", index=plot_df.index)).fillna("C").astype(str)
            plot_df["ev_sign"] = plot_df.get("ev_r", pd.Series(0.0, index=plot_df.index)).apply(lambda v: "EV_POS" if safe_float(v) >= 0 else "EV_NEG")
            plot_df["decision_shape_key"] = plot_df["matrix_action"] + "|" + plot_df["ev_sign"]
            plot_df["gate_summary_hover"] = (
                plot_df.get("gate_summary", pd.Series("", index=plot_df.index))
                .fillna("")
                .astype(str)
                .str.replace("|", " | ", regex=False)
                .replace("", "Aucun gate actif")
            )
            plot_df["action_reason_hover"] = plot_df.get("action_reason", pd.Series("", index=plot_df.index)).fillna("").astype(str).str.replace("|", " | ", regex=False)

            custom_cols = [
                "symbol",
                "name",
                "sector",
                "matrix_action",
                "setup_grade",
                "risk_score_u",
                "reward_score_u",
                "r_multiple",
                "r_multiple_raw",
                "ev_r",
                "p_win_pct",
                "data_quality_score",
                "gate_summary_hover",
                "action_reason_hover",
                "reward_pct",
                "risk_pct",
                "spreadPct",
                "iv_atm",
                "days_to_next_earnings",
                "days_since_last_earnings",
            ]
            for col in custom_cols:
                if col not in plot_df.columns:
                    plot_df[col] = pd.NA

            fig_rr = px.scatter(
                plot_df,
                x="risk_score_plot",
                y="reward_score_plot",
                color="setup_grade",
                symbol="decision_shape_key",
                size="bubble_size",
                custom_data=custom_cols,
                color_discrete_map=GRADE_COLOR_MAP,
                symbol_map=DECISION_SYMBOL_MAP,
                category_orders={"setup_grade": ["A", "B", "C"]},
                labels={
                    "risk_score_plot": "Risque (0-100)",
                    "reward_score_plot": "Reward (0-100)",
                },
                title="Matrice Risk / Reward / Probabilite (echelle 0-100)",
            )

            hover_template = (
                "<b>%{customdata[0]}</b> - %{customdata[1]}<br>"
                "Secteur: %{customdata[2]}<br>"
                "Decision: %{customdata[3]} | Grade: %{customdata[4]}<br>"
                "Risque / Reward: %{customdata[5]:.0f} / %{customdata[6]:.0f}<br>"
                "R utilise / R brut: %{customdata[7]:.2f} / %{customdata[8]:.2f}<br>"
                "EV(R): %{customdata[9]:+.2f} | Prob. win: %{customdata[10]:.1f}%<br>"
                "Data quality: %{customdata[11]:.1f}/100<br>"
                "Gates actifs: %{customdata[12]}<br>"
                "Raison decision: %{customdata[13]}<br>"
                "Reward%: %{customdata[14]:.2f} | Risk%: %{customdata[15]:.2f}<br>"
                "Spread%: %{customdata[16]:.2f} | IV ATM: %{customdata[17]:.3f}<br>"
                "Jours earnings (avant/depuis): %{customdata[18]:.1f} / %{customdata[19]:.1f}<br>"
                "<i>Cliquer pour ouvrir vue detail.</i><extra></extra>"
            )
            for tr in fig_rr.data:
                trace_name = str(getattr(tr, "name", "") or "")
                grade_token = trace_name.split(",")[0].strip() if "," in trace_name else trace_name.strip()
                ev_sign = "EV_POS" if "EV_POS" in trace_name else "EV_NEG"
                tr.marker.opacity = 0.78
                tr.marker.line.color = EV_SIGN_BORDER_MAP.get(ev_sign, "#ffffff")
                tr.marker.line.width = GRADE_CONTOUR_WIDTH_MAP.get(grade_token, 2.0)
                tr.hovertemplate = hover_template

            fig_rr.add_vline(x=risk_thr, line_dash="dot", line_color="#FF4D4F")
            fig_rr.add_hline(y=reward_thr, line_dash="dot", line_color="#2ECC71")
            fig_rr.update_xaxes(range=[0, 100], dtick=10)
            fig_rr.update_yaxes(range=[0, 100], dtick=10)
            fig_rr.update_layout(height=620)

            plot_selection = None
            try:
                plot_selection = st.plotly_chart(
                    fig_rr,
                    use_container_width=True,
                    key="multi_agents_rr_scatter",
                    on_select="rerun",
                    selection_mode=("points",),
                )
            except TypeError:
                st.plotly_chart(fig_rr, use_container_width=True, key="multi_agents_rr_scatter_fallback")

            clicked_symbol = ""
            try:
                points = ((plot_selection or {}).get("selection") or {}).get("points", [])
                if points:
                    custom_data = points[0].get("customdata", [])
                    if custom_data:
                        clicked_symbol = str(custom_data[0] or "").strip().upper()
            except Exception:
                clicked_symbol = ""

            if clicked_symbol:
                st.session_state["multi_agents_symbol_jump"] = clicked_symbol
                quick = view_df[view_df.get("symbol", pd.Series("", index=view_df.index)).astype(str).str.upper() == clicked_symbol]
                if not quick.empty:
                    q = quick.iloc[0]
                    st.info(
                        f"Fiche rapide: {clicked_symbol}. "
                        "Ouvrez l'onglet 'Vue par valeur' pour le detail complet."
                    )
                    q1, q2, q3, q4, q5 = st.columns(5)
                    q1.metric("Decision", str(q.get("matrix_action", "N/A")))
                    q2.metric("Grade", str(q.get("setup_grade", "N/A")))
                    q3.metric("EV(R)", f"{safe_float(q.get('ev_r', 0.0)):+.2f}")
                    q4.metric("R", f"{safe_float(q.get('r_multiple', 0.0)):.2f}")
                    q5.metric("Data quality", f"{safe_float(q.get('data_quality_score', 0.0)):.0f}/100")

            st.markdown(
                """
**Lecture des 4 cadrants (investissement)**
1. `Haut gauche` (Risque <= seuil dynamique, Reward >= seuil dynamique): Zone de priorite. Entree/renforcement possible si les gates sont ouverts.
2. `Haut droite` (Risque > seuil dynamique, Reward >= seuil dynamique): Opportunites speculatives. Taille reduite, stop strict, execution selective.
3. `Bas gauche` (Risque <= seuil dynamique, Reward < seuil dynamique): Profil defensif. Surveillance/conservation, upside limite a court terme.
4. `Bas droite` (Risque > seuil dynamique, Reward < seuil dynamique): Zone de derisque. Reduction/sortie prioritaire sauf argument tactique fort.
"""
            )

            with st.expander("Exemple concret de calcul R et EV(R)", expanded=False):
                entry_ex = 100.0
                stop_ex = 95.0
                tp_ex = 112.0
                pwin_ex = 0.55
                risk_ex = (entry_ex - stop_ex) / entry_ex * 100.0
                reward_ex = (tp_ex - entry_ex) / entry_ex * 100.0
                r_ex = (tp_ex - entry_ex) / max(0.0001, entry_ex - stop_ex)
                evr_ex = (pwin_ex * r_ex) - (1.0 - pwin_ex)
                st.markdown(
                    f"""
- Entree = `{entry_ex:.2f}`, Stop = `{stop_ex:.2f}`, TP = `{tp_ex:.2f}`
- Risk % = `(Entree-Stop)/Entree` = `{risk_ex:.2f}%`
- Reward % = `(TP-Entree)/Entree` = `{reward_ex:.2f}%`
- **R** = `(TP-Entree)/(Entree-Stop)` = `{r_ex:.2f}R`
- Avec `p_win={pwin_ex*100:.1f}%`, **EV(R)** = `p_win x R - (1-p_win)` = `{evr_ex:+.2f}R`
- Interpretation: EV(R) positif = avantage statistique, EV(R) negatif = setup defavorable.
"""
                )

            st.markdown("#### Etat des donnees & fraicheur")
            f1, f2, f3 = st.columns(3)
            f1.metric("Couverture YF enrich", f"{safe_float(freshness_summary.get('enrichment_coverage_pct', 0.0)):.1f}%")
            f2.metric("Options indispo", int(safe_float(freshness_summary.get("options_missing_count", 0.0))))
            f3.metric("Invalid options state", int(safe_float(freshness_summary.get("invalid_options_state_count", 0.0))))
            render_interactive_table(freshness_df, key_suffix="multi_agents_data_freshness", height=260)

            st.markdown("#### Sanity checks")
            qc1, qc2 = st.columns(2)
            with qc1:
                qtab = (
                    pd.crosstab(
                        view_df.get("quadrant", pd.Series(dtype=str)),
                        view_df.get("matrix_action", pd.Series(dtype=str)),
                    )
                    .reset_index()
                    .rename(columns={"quadrant": "Quadrant"})
                )
                render_interactive_table(qtab, key_suffix="multi_agents_quadrant_action", height=220)
            with qc2:
                rr_out = view_df[view_df.get("rr_outlier", pd.Series(False, index=view_df.index)).fillna(False).astype(bool)].copy()
                cols_rr = [c for c in ["symbol", "name", "r_multiple_raw", "r_multiple", "risk_pct_raw", "atr_stop_floor_pct", "matrix_action"] if c in rr_out.columns]
                if cols_rr:
                    render_interactive_table(
                        rr_out[cols_rr].sort_values("r_multiple_raw", ascending=False).head(15).rename(
                            columns={
                                "symbol": "Symbole",
                                "name": "Nom",
                                "r_multiple_raw": "R brut",
                                "r_multiple": "R utilise",
                                "risk_pct_raw": "Risk % brut",
                                "atr_stop_floor_pct": "Plancher ATR %",
                                "matrix_action": "Decision",
                            }
                        ),
                        key_suffix="multi_agents_rr_outliers",
                        height=220,
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
                "data_quality_score",
                "r_multiple",
                "r_multiple_raw",
                "risk_pct_raw",
                "atr_stop_floor_pct",
                "rr_outlier",
                "ev_r",
                "p_win",
                "size_reco_pct",
                "action_reason",
                "reward_pct",
                "risk_pct",
                "spreadPct",
                "iv_atm",
                "days_to_next_earnings",
                "days_since_last_earnings",
                "invalid_options_state",
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
                        "data_quality_score": "Data quality",
                        "r_multiple": "R",
                        "r_multiple_raw": "R brut",
                        "risk_pct_raw": "Risk % brut",
                        "atr_stop_floor_pct": "Plancher ATR %",
                        "rr_outlier": "RR outlier",
                        "ev_r": "EV(R)",
                        "p_win": "Prob. win %",
                        "size_reco_pct": "Sizing reco %",
                        "action_reason": "Raison action",
                        "reward_pct": "Reward %",
                        "risk_pct": "Risk %",
                        "spreadPct": "Spread %",
                        "iv_atm": "IV ATM",
                        "days_to_next_earnings": "Jours avant earnings",
                        "days_since_last_earnings": "Jours depuis earnings",
                        "invalid_options_state": "Etat options invalide",
                        "options_note": "Note options",
                    }
                ),
                key_suffix="multi_agents_matrix_global",
                height=460,
            )
            with st.expander("Dictionnaire des champs (metriques affichees)", expanded=False):
                dict_metric_ids = [
                    "total_values",
                    "enter_count",
                    "watch_count",
                    "exit_count",
                    "avg_ev_r",
                    "grade_a_count",
                    "rr_outliers",
                    "low_data_quality",
                    "options_missing",
                    "invalid_options_state_count",
                    "risk_score_u",
                    "reward_score_u",
                    "r_multiple",
                    "r_multiple_raw",
                    "ev_r",
                    "p_win",
                    "setup_grade",
                    "matrix_action",
                    "data_quality_score",
                    "risk_threshold_dyn",
                    "reward_threshold_dyn",
                    "grade_a_threshold",
                    "grade_b_threshold",
                    "gate_summary",
                    "spreadPct",
                    "iv_atm",
                    "days_to_next_earnings",
                    "days_since_last_earnings",
                ]
                dict_df = _metrics_dictionary_df(dict_metric_ids)
                render_interactive_table(dict_df, key_suffix="multi_agents_metrics_dictionary", height=380)
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
        st.markdown(f"### {TEXTS_FR['value_header_title']}")
        st.caption(str(TEXTS_FR["value_header_subtitle"]))
        beginner_default = bool(st.session_state.get("multi_agents_beginner_mode", True))
        if hasattr(st, "toggle"):
            st.toggle("Mode débutant", value=beginner_default, key="multi_agents_beginner_mode")
        else:
            st.checkbox("Mode débutant", value=beginner_default, key="multi_agents_beginner_mode")

        labels = []
        label_to_symbol = {}
        for _, row in view_df.iterrows():
            sym = str(row.get("symbol", "")).strip().upper()
            if not sym:
                continue
            name = str(row.get("name", "")).strip()
            lbl = f"{sym} - {name}" if name else sym
            if lbl not in label_to_symbol:
                labels.append(lbl)
                label_to_symbol[lbl] = sym

        labels = sorted(labels, key=lambda x: x.lower())

        if not labels:
            st.info("Aucune valeur disponible.")
        else:
            st.markdown("#### Recherche valeur")
            search_txt = st.text_input(
                "Recherche texte (ticker ou nom)",
                value="",
                key="multi_agents_symbol_search",
                placeholder="Ex: OR.PA, LOREAL, APPLE...",
            )
            st.caption(str(TEXTS_FR["value_search_tip"]))
            query = str(search_txt or "").strip().lower()
            labels_filtered = [lbl for lbl in labels if query in lbl.lower()] if query else labels
            if not labels_filtered:
                st.warning("Aucun resultat pour cette recherche. La liste complete est rechargee.")
                labels_filtered = labels

            jump_symbol = str(st.session_state.get("multi_agents_symbol_jump", "") or "").strip().upper()
            if jump_symbol:
                jump_label = next((lbl for lbl in labels_filtered if label_to_symbol.get(lbl, "") == jump_symbol), "")
                if jump_label:
                    st.session_state["multi_agents_symbol_last"] = jump_label
                st.session_state["multi_agents_symbol_jump"] = ""

            previous_label = st.session_state.get("multi_agents_symbol_last", labels_filtered[0])
            if previous_label not in labels_filtered:
                previous_label = labels_filtered[0]
            default_idx = labels_filtered.index(previous_label) if previous_label in labels_filtered else 0

            selected_label = st.selectbox(
                "Selectionner via combobox",
                labels_filtered,
                index=default_idx,
                key="multi_agents_symbol",
            )
            st.session_state["multi_agents_symbol_last"] = selected_label
            selected_symbol = label_to_symbol[selected_label]
            row = view_df[view_df["symbol"] == selected_symbol].iloc[0]

            if use_matrix:
                selected_name = str(row.get("name", "") or "").strip()
                st.markdown(f"### {selected_symbol} - {selected_name or 'N/A'}")
                field_keys = {
                    "risk": ["Risk", "Risque", "risk_score", "risk_score_u", "risk_score_100", "risk_0_100"],
                    "reward": ["Reward", "reward_score", "reward_score_u", "reward_score_100", "reward_0_100"],
                    "evr": ["EV(R)", "EVR", "ev_r", "evR"],
                    "prob_win": ["Prob. win %", "Prob_win_%", "prob_win", "p_win", "probWin"],
                    "data_quality": ["Data quality", "data_quality", "data_quality_score", "dq_score"],
                    "sizing": ["Sizing reco %", "size_reco_pct", "sizing_reco_pct", "sizingPct"],
                    "entry": ["entry_price", "entry", "Entry"],
                    "stop": ["stop_price", "stop", "Stop"],
                    "tp1": ["tp_price", "tp1", "TP1"],
                    "tp2": ["tp2", "tp_runner", "runner_tp", "TP2"],
                    "risk_pct": ["risk_pct", "riskPct"],
                    "reward_pct": ["reward_pct", "rewardPct"],
                    "invalidation": ["invalidation", "invalidation_txt"],
                    "order_type": ["order_type", "orderType"],
                    "reason_action": ["action_reason", "reason_action"],
                }
                # Checklist conformite Vue par valeur (neophyte + robustesse):
                # [F1] KPI + aide debutant visibles avec definitions explicites.
                # [F2] Gates avec statut OK/WARN/BLOCK + type HARD/SOFT + gate summary.
                # [F3] Trade card executable + copier texte/JSON.
                # [F4] Mini-matrice avec seuils p60 et point valeur.
                # [F5] Jamais de None/NaN brut: fallback N/A + explication.
                # [F6] Palette coherente: OK vert, WARN orange, BLOCK rouge, N/A gris.

                def _row_num(keys: list[str]) -> float:
                    return pd.to_numeric(
                        pd.Series([safe_get(row, keys, default=pd.NA)]),
                        errors="coerce",
                    ).iloc[0]

                s1, s2, s3, s4, s5, s6, s7, s8, s9 = st.columns(9)
                with s1:
                    _kpi_metric_with_info(
                        "Decision",
                        safe_text(safe_get(row, ["matrix_action", "decision"], default="N/A")),
                        str(TEXTS_FR["kpi_tooltips_exact"]["Decision"]),
                    )
                with s2:
                    _kpi_metric_with_info(
                        "Grade",
                        safe_text(safe_get(row, ["setup_grade", "grade"], default="N/A")),
                        str(TEXTS_FR["kpi_tooltips_exact"]["Grade"]),
                    )
                with s3:
                    risk_v = safe_get(row, field_keys["risk"], default=pd.NA)
                    _kpi_metric_with_info("Risque", safe_score(risk_v), str(TEXTS_FR["kpi_tooltips_exact"]["Risque"]))
                with s4:
                    reward_v = safe_get(row, field_keys["reward"], default=pd.NA)
                    _kpi_metric_with_info("Reward", safe_score(reward_v), str(TEXTS_FR["kpi_tooltips_exact"]["Reward"]))
                with s5:
                    _kpi_metric_with_info(
                        "R",
                        safe_num(safe_get(row, ["r_multiple", "r", "R"], default=pd.NA), ndigits=2),
                        str(TEXTS_FR["kpi_tooltips_exact"]["R"]),
                    )
                with s6:
                    ev_val = pd.to_numeric(pd.Series([safe_get(row, field_keys["evr"], default=pd.NA)]), errors="coerce").iloc[0]
                    ev_txt = "N/A" if pd.isna(ev_val) else f"{float(ev_val):+.2f}"
                    _kpi_metric_with_info("EV(R)", ev_txt, str(TEXTS_FR["kpi_tooltips_exact"]["EV(R)"]))
                with s7:
                    p_win_v = safe_get(row, field_keys["prob_win"], default=pd.NA)
                    _kpi_metric_with_info("Prob. win", _fmt_pct_auto(p_win_v, ndigits=1), str(TEXTS_FR["kpi_tooltips_exact"]["Prob. win"]))
                with s8:
                    _kpi_metric_with_info(
                        "Data quality",
                        safe_score(safe_get(row, field_keys["data_quality"], default=pd.NA)),
                        str(TEXTS_FR["kpi_tooltips_exact"]["Data quality"]),
                    )
                with s9:
                    size_v = safe_get(row, field_keys["sizing"], default=pd.NA)
                    size_txt = "N/A" if pd.isna(pd.to_numeric(pd.Series([size_v]), errors="coerce").iloc[0]) else f"{float(pd.to_numeric(pd.Series([size_v]), errors='coerce').iloc[0]):.1f}%"
                    _kpi_metric_with_info("Sizing reco", size_txt, str(TEXTS_FR["kpi_tooltips_exact"]["Sizing reco"]))

                if bool(st.session_state.get("multi_agents_beginner_mode", True)):
                    with st.expander(str(TEXTS_FR["beginner_panel_title"]), expanded=False):
                        st.markdown(str(TEXTS_FR["beginner_panel_md"]))
                dnext = safe_get(
                    row,
                    ["days_to_next_earnings", "days_to_earnings", "earnings_days_to_next"],
                    default=pd.NA,
                )
                dnext_num = pd.to_numeric(pd.Series([dnext]), errors="coerce").iloc[0]
                has_dnext = pd.notna(dnext_num)
                if has_dnext and float(dnext_num) >= 0 and float(dnext_num) <= 7:
                    earnings_status = "BLOCK"
                elif has_dnext and float(dnext_num) >= 0 and float(dnext_num) <= 14:
                    earnings_status = "WARN"
                else:
                    earnings_status = "OK" if has_dnext else "WARN"
                earnings_type = "HARD" if earnings_status == "BLOCK" else "SOFT"

                rr_outlier = _is_truthy(row.get("rr_outlier", False)) or bool(row.get("rr_outlier", False))
                rr_status = "WARN" if rr_outlier else "OK"
                rr_type = "SOFT"

                liq_num = _row_num(["liquidity_risk_score", "liquidity_score", "liq_risk_score"])
                liq_score = float(liq_num) if pd.notna(liq_num) else float("nan")
                liq_gate_block = _is_truthy(row.get("liquidity_gate_block", False)) or bool(row.get("liquidity_gate_block", False))
                if liq_gate_block:
                    liq_status = "BLOCK"
                elif pd.isna(liq_score):
                    liq_status = "WARN"
                elif liq_score >= 85:
                    liq_status = "BLOCK"
                elif liq_score >= 60:
                    liq_status = "WARN"
                else:
                    liq_status = "OK"
                liq_type = "HARD" if liq_status == "BLOCK" else "SOFT"

                invalid_options_state = (
                    _is_truthy(row.get("invalid_options_state", False))
                    or _is_truthy(row.get("invalid_options_state_gate", False))
                    or bool(row.get("invalid_options_state", False))
                    or bool(row.get("invalid_options_state_gate", False))
                )
                options_ok_raw = str(row.get("options_ok", "")).strip().lower()
                options_ok = options_ok_raw in ("1", "true", "yes", "y")
                if invalid_options_state:
                    options_status = "BLOCK"
                elif options_ok:
                    options_status = "OK"
                else:
                    options_status = "WARN"
                options_type = "HARD" if options_status == "BLOCK" else "SOFT"

                concentration_num = _row_num(["concentration_risk_score", "concentration_score"])
                concentration_score = float(concentration_num) if pd.notna(concentration_num) else float("nan")
                if pd.isna(concentration_score):
                    concentration_status = "WARN"
                elif concentration_score >= 85:
                    concentration_status = "BLOCK"
                elif concentration_score >= 60:
                    concentration_status = "WARN"
                else:
                    concentration_status = "OK"
                concentration_type = "HARD" if concentration_status == "BLOCK" else "SOFT"

                data_quality_num = _row_num(field_keys["data_quality"])
                data_quality = float(data_quality_num) if pd.notna(data_quality_num) else float("nan")
                if pd.isna(data_quality):
                    quality_status = "BLOCK"
                elif data_quality < 55:
                    quality_status = "BLOCK"
                elif data_quality < 75:
                    quality_status = "WARN"
                else:
                    quality_status = "OK"
                quality_type = "HARD" if quality_status == "BLOCK" else "SOFT"

                age_candidates = []
                for age_col in ["data_age_h1_hours", "data_age_d1_hours", "yf_age_h"]:
                    age_val = pd.to_numeric(pd.Series([row.get(age_col, pd.NA)]), errors="coerce").iloc[0]
                    if pd.notna(age_val):
                        age_candidates.append(float(age_val))
                max_age_h = max(age_candidates) if age_candidates else pd.NA
                freshness_status = _freshness_status(max_age_h, warn_h=36.0, block_h=96.0) if pd.notna(max_age_h) else "WARN"
                freshness_type = "HARD" if freshness_status == "BLOCK" else "SOFT"

                st.markdown("#### Gates")
                badges = [
                    _gate_badge_html(
                        earnings_status,
                        "Earnings",
                        (f"J-{float(dnext_num):.1f}" if float(dnext_num) >= 0 else f"J+{abs(float(dnext_num)):.1f}")
                        if has_dnext
                        else "N/A",
                        gate_type=earnings_type,
                        rule="Si earnings trop proches (J <= seuil), prudence.",
                        consequence="Réduit le sizing / bloque l'exécution auto.",
                    ),
                    _gate_badge_html(
                        rr_status,
                        "RR outlier",
                        "stop/target à vérifier",
                        gate_type=rr_type,
                        rule="Si R brut est extrême, le setup peut être artificiellement gonflé.",
                        consequence="Setup conditionnel : vérification manuelle avant exécution.",
                    ),
                    _gate_badge_html(
                        liq_status,
                        "Liquidité",
                        (f"score {safe_num(liq_score, 0)}/100" if pd.notna(liq_score) else "N/A"),
                        gate_type=liq_type,
                        rule="Si spread/slippage élevés, risque d'exécution défavorable.",
                        consequence="Réduire le sizing / éviter exécution automatique.",
                    ),
                    _gate_badge_html(
                        options_status,
                        "Options/IV",
                        safe_text(row.get("options_note", ""), default="couverture partielle"),
                        gate_type=options_type,
                        rule="IV/Options indisponibles -> risque évènementiel moins mesuré.",
                        consequence="Setup conditionnel : vérifier manuellement avant exécution.",
                    ),
                    _gate_badge_html(
                        freshness_status,
                        "Fraîcheur data",
                        f"{float(max_age_h):.1f}h" if pd.notna(max_age_h) else "N/A",
                        gate_type=freshness_type,
                        rule="Si les données sont trop anciennes, le signal peut être obsolète.",
                        consequence="Décision dégradée, exécution à confirmer manuellement.",
                    ),
                    _gate_badge_html(
                        concentration_status,
                        "Concentration",
                        (f"score {safe_num(concentration_score, 0)}/100" if pd.notna(concentration_score) else "N/A"),
                        gate_type=concentration_type,
                        rule="Si poids symbole/secteur trop élevé -> concentration risk.",
                        consequence="Réduire sizing / éviter renforcement.",
                    ),
                    _gate_badge_html(
                        quality_status,
                        "Data quality",
                        (f"{safe_num(data_quality, 0)}/100" if pd.notna(data_quality) else "N/A"),
                        gate_type=quality_type,
                        rule="Data quality < seuil -> données insuffisantes.",
                        consequence="Décision dégradée ou trade bloqué.",
                    ),
                ]
                st.markdown("".join(badges), unsafe_allow_html=True)

                hard_blocks = []
                if earnings_status == "BLOCK":
                    hard_blocks.append("Earnings")
                if liq_status == "BLOCK":
                    hard_blocks.append("Liquidite")
                if options_status == "BLOCK":
                    hard_blocks.append("Options/IV")
                if freshness_status == "BLOCK":
                    hard_blocks.append("Fraicheur")
                if concentration_status == "BLOCK":
                    hard_blocks.append("Concentration")
                if quality_status == "BLOCK":
                    hard_blocks.append("Data quality")

                warns = []
                for label, stt in [
                    ("Earnings", earnings_status),
                    ("RR outlier", rr_status),
                    ("Liquidite", liq_status),
                    ("Options/IV", options_status),
                    ("Fraicheur", freshness_status),
                    ("Concentration", concentration_status),
                    ("Data quality", quality_status),
                ]:
                    if stt == "WARN":
                        warns.append(label)

                if hard_blocks:
                    st.error(
                        f"\u26d4 Setup bloqué : {', '.join(hard_blocks)}. "
                        "Action recommandée = Surveiller/Réduire."
                    )
                elif warns:
                    st.warning(
                        f"\u26a0\ufe0f Setup conditionnel : exécution possible mais vérification manuelle requise : {', '.join(warns)}."
                    )
                elif "WARN" in [earnings_status, rr_status, liq_status, options_status, freshness_status, concentration_status, quality_status]:
                    st.warning("\u26a0\ufe0f Setup conditionnel : exécution possible mais vérification manuelle requise.")
                else:
                    st.success("\u2705 Setup exécutable : données OK et contraintes respectées.")
                if "BLOCK" in [earnings_status, liq_status, options_status, freshness_status, concentration_status, quality_status]:
                    st.caption("Gate summary: blocage actif (HARD).")
                elif "WARN" in [earnings_status, rr_status, liq_status, options_status, freshness_status, concentration_status, quality_status]:
                    st.caption("Gate summary: setup conditionnel (SOFT).")
                else:
                    st.caption("Gate summary: setup exécutable.")

                st.markdown("#### Trade card")
                entry_num = pd.to_numeric(pd.Series([safe_get(row, field_keys["entry"], default=pd.NA)]), errors="coerce").iloc[0]
                stop_num = pd.to_numeric(pd.Series([safe_get(row, field_keys["stop"], default=pd.NA)]), errors="coerce").iloc[0]
                tp1_num = pd.to_numeric(pd.Series([safe_get(row, field_keys["tp1"], default=pd.NA)]), errors="coerce").iloc[0]
                reward_num = pd.to_numeric(pd.Series([safe_get(row, field_keys["reward_pct"], default=pd.NA)]), errors="coerce").iloc[0]
                risk_num = pd.to_numeric(pd.Series([safe_get(row, field_keys["risk_pct"], default=pd.NA)]), errors="coerce").iloc[0]
                atr_num = pd.to_numeric(pd.Series([row.get("d1_atr_pct", pd.NA)]), errors="coerce").iloc[0]
                entry = float(entry_num) if pd.notna(entry_num) else float("nan")
                stop = float(stop_num) if pd.notna(stop_num) else float("nan")
                tp1 = float(tp1_num) if pd.notna(tp1_num) else float("nan")
                reward_pct = float(reward_num) if pd.notna(reward_num) else float("nan")
                risk_pct = float(risk_num) if pd.notna(risk_num) else float("nan")
                atr_pct = float(atr_num) if pd.notna(atr_num) else float("nan")
                stop_atr = (risk_pct / atr_pct) if pd.notna(risk_pct) and pd.notna(atr_pct) and atr_pct > 0 else float("nan")
                runner_bonus_pct = max(1.0, min(12.0, reward_pct * 0.60)) if pd.notna(reward_pct) else 1.0
                tp2 = entry * (1.0 + (max(0.0, reward_pct) + runner_bonus_pct) / 100.0) if pd.notna(entry) and entry > 0 and pd.notna(reward_pct) else float("nan")
                if pd.notna(tp1) and pd.notna(tp2):
                    tp2 = max(tp1, tp2)
                matrix_action = safe_text(safe_get(row, ["matrix_action", "decision"], default="Surveiller"), default="Surveiller")
                reward_score_num = _row_num(["reward_score_u", "reward_score_100", "reward_score"])
                risk_score_num = _row_num(["risk_score_u", "risk_score_100", "risk_score"])
                spread_num = pd.to_numeric(pd.Series([safe_get(row, ["spreadPct", "spread_pct"], default=pd.NA)]), errors="coerce").iloc[0]
                if matrix_action == "Entrer / Renforcer":
                    order_type = "Limit"
                    order_why = "Entrée sur zone de prix contrôlée pour limiter le slippage."
                    if pd.notna(reward_score_num) and pd.notna(risk_score_num) and float(reward_score_num) > float(risk_score_num) + 15.0:
                        order_type = "Stop"
                        order_why = "Validation momentum attendue avant exécution."
                    if (
                        pd.notna(spread_num)
                        and float(spread_num) <= 0.20
                        and pd.notna(reward_pct)
                        and pd.notna(risk_pct)
                        and float(reward_pct) >= 2.0 * max(0.1, float(risk_pct))
                    ):
                        order_type = "StopLimit"
                        order_why = "Breakout + spread contenu: contrôle du prix d'exécution."
                elif matrix_action == "Reduire / Sortir":
                    order_type = "Market"
                    order_why = "Réduction prioritaire du risque portefeuille."
                else:
                    order_type = "Limit"
                    order_why = "Setup en surveillance: exécution uniquement après confirmation."

                pf_total = float("nan")
                if df_port is not None and not df_port.empty:
                    mv_series = safe_float_series(df_port.get("marketvalue", pd.Series(0.0, index=df_port.index)))
                    if not mv_series.empty:
                        pf_total = float(mv_series.sum())
                size_pct_num = pd.to_numeric(pd.Series([safe_get(row, field_keys["sizing"], default=pd.NA)]), errors="coerce").iloc[0]
                size_pct = float(size_pct_num) if pd.notna(size_pct_num) else float("nan")
                size_eur = pf_total * (size_pct / 100.0) if pd.notna(pf_total) and pf_total > 0 else float("nan")
                qty_reco = (size_eur / entry) if entry > 0 and pd.notna(size_eur) and size_eur > 0 else float("nan")
                invalidation_candidate = safe_text(safe_get(row, field_keys["invalidation"], default=""), default="")
                invalidation_txt = invalidation_candidate if invalidation_candidate else (f"Setup invalide sous {stop:.2f}" if stop > 0 else "Niveau d'invalidation indisponible")

                trade_col, mini_col = st.columns([1.55, 1.0])
                with trade_col:
                    entry_txt = safe_num(entry, 2)
                    stop_txt = safe_num(stop, 2)
                    tp1_txt = safe_num(tp1, 2)
                    tp2_txt = safe_num(tp2, 2)
                    risk_pct_txt = safe_num(risk_pct, 2)
                    reward_pct_txt = safe_num(reward_pct, 2)
                    stop_atr_txt = safe_num(stop_atr, 2)
                    r_used_txt_trade = safe_num(row.get("r_multiple", pd.NA), 2)
                    r_raw_txt_trade = safe_num(row.get("r_multiple_raw", pd.NA), 2)
                    ev_txt_trade = safe_num(row.get("ev_r", pd.NA), 2)
                    size_pct_txt = safe_num(size_pct, 1)
                    size_eur_txt = safe_num(size_eur, 0)
                    qty_txt = safe_num(qty_reco, 0)
                    risk_pct_show = f"{risk_pct_txt}%" if risk_pct_txt != "N/A" else "N/A"
                    reward_pct_show = f"{reward_pct_txt}%" if reward_pct_txt != "N/A" else "N/A"
                    size_pct_show = f"{size_pct_txt}%" if size_pct_txt != "N/A" else "N/A"
                    st.markdown(
                        f"""
- Type d'ordre: `{order_type}`  
  Justification: {order_why}
- Entree: `{entry_txt}` | Stop: `{stop_txt}` ({risk_pct_show} ; {stop_atr_txt} ATR si ATR>0)
- TP1: `{tp1_txt}` ({reward_pct_show}) | TP2/Runner: `{tp2_txt}`
- R brut: `{r_raw_txt_trade}` | R utilise: `{r_used_txt_trade}` (cap)
- EV(R): `{ev_txt_trade}`
- Sizing recommande: `{size_pct_show}` de la taille cible ({size_eur_txt} EUR, env. {qty_txt} titres)
- Validite: `{invalidation_txt}`
"""
                    )
                    plan_txt = (
                        f"{selected_symbol} | {matrix_action} | Grade {safe_text(safe_get(row, ['setup_grade', 'grade'], default='N/A'))}\n"
                        f"OrderType={order_type} | Why={order_why}\n"
                        f"Entree={entry_txt} | Stop={stop_txt} | TP1={tp1_txt} | TP2={tp2_txt}\n"
                        f"Risk={risk_pct_show} | Reward={reward_pct_show} | Rraw={r_raw_txt_trade} | R={r_used_txt_trade} | EV(R)={ev_txt_trade}\n"
                        f"ProbWin={_fmt_pct_auto(safe_get(row, field_keys['prob_win'], default=pd.NA), 1)} | DataQuality={safe_score(safe_get(row, field_keys['data_quality'], default=pd.NA))}\n"
                        f"Sizing={size_pct_show} ({size_eur_txt} EUR, ~{qty_txt} titres)\n"
                        f"Gates={safe_text(row.get('gate_summary', ''), default='OK')}\n"
                        f"Invalidation={invalidation_txt}\n"
                    )
                    plan_json_obj = {
                        "symbol": selected_symbol,
                        "decision": matrix_action,
                        "entry": float(entry) if entry > 0 else None,
                        "stop": float(stop) if stop > 0 else None,
                        "tp1": float(tp1) if tp1 > 0 else None,
                        "tp2": float(tp2) if tp2 > 0 else None,
                        "riskPct": float(risk_pct) if pd.notna(risk_pct) else None,
                        "rewardPct": float(reward_pct) if pd.notna(reward_pct) else None,
                        "r": (
                            float(pd.to_numeric(pd.Series([safe_get(row, ["r_multiple", "r", "R"], default=pd.NA)]), errors="coerce").iloc[0])
                            if pd.notna(pd.to_numeric(pd.Series([safe_get(row, ["r_multiple", "r", "R"], default=pd.NA)]), errors="coerce").iloc[0])
                            else None
                        ),
                        "evR": (
                            float(pd.to_numeric(pd.Series([safe_get(row, field_keys["evr"], default=pd.NA)]), errors="coerce").iloc[0])
                            if pd.notna(pd.to_numeric(pd.Series([safe_get(row, field_keys["evr"], default=pd.NA)]), errors="coerce").iloc[0])
                            else None
                        ),
                        "probWin": (
                            float(pd.to_numeric(pd.Series([safe_get(row, field_keys["prob_win"], default=pd.NA)]), errors="coerce").iloc[0])
                            if pd.notna(pd.to_numeric(pd.Series([safe_get(row, field_keys["prob_win"], default=pd.NA)]), errors="coerce").iloc[0])
                            else None
                        ),
                        "sizingPct": float(size_pct) if pd.notna(size_pct) else None,
                        "gatesSummary": safe_text(row.get("gate_summary", ""), default="N/A"),
                        "invalidation": invalidation_txt,
                        "timestamps": {"generatedAt": datetime.utcnow().isoformat() + "Z"},
                    }
                    plan_json_txt = json.dumps(plan_json_obj, ensure_ascii=False, indent=2)
                    st.caption("Plan execution pret a copier")
                    _render_copy_buttons(plan_txt, plan_json_txt, key_suffix=re.sub(r"[^A-Za-z0-9_]", "_", selected_symbol))
                    st.code(plan_txt, language="text")
                    with st.expander("Voir le plan JSON"):
                        st.code(plan_json_txt, language="json")

                with mini_col:
                    st.markdown("#### Mini matrice")
                    risk_thr = safe_float(view_df.get("risk_threshold_dyn", pd.Series([50.0])).iloc[0])
                    reward_thr = safe_float(view_df.get("reward_threshold_dyn", pd.Series([50.0])).iloc[0])
                    mini_df = view_df.copy()
                    mini_df["risk_score_plot"] = safe_float_series(mini_df.get("risk_score_plot", mini_df.get("risk_score_u", pd.Series(50.0, index=mini_df.index)))).fillna(50.0)
                    mini_df["reward_score_plot"] = safe_float_series(mini_df.get("reward_score_plot", mini_df.get("reward_score_u", pd.Series(50.0, index=mini_df.index)))).fillna(50.0)
                    mini_all = mini_df[mini_df["symbol"] != selected_symbol]
                    mini_sel = mini_df[mini_df["symbol"] == selected_symbol].head(1)
                    fig_mini = go.Figure()
                    fig_mini.add_trace(go.Scatter(x=mini_all["risk_score_plot"], y=mini_all["reward_score_plot"], mode="markers", marker=dict(size=6, color="rgba(130,130,130,0.35)"), name="Univers", hoverinfo="skip"))
                    fig_mini.add_trace(
                        go.Scatter(
                            x=mini_sel["risk_score_plot"],
                            y=mini_sel["reward_score_plot"],
                            mode="markers+text",
                            text=[selected_symbol],
                            textposition="top center",
                            marker=dict(size=16, color="#00d4ff", line=dict(width=2, color="#ffffff")),
                            name="Valeur",
                            hovertemplate=(
                                f"Ticker : {selected_symbol}<br>"
                                f"Risk : {safe_num(row.get('risk_score_u', row.get('risk_score_100', pd.NA)), 0)}/100 | "
                                f"Reward : {safe_num(row.get('reward_score_u', row.get('reward_score_100', pd.NA)), 0)}/100<br>"
                                f"Grade : {safe_text(row.get('setup_grade', 'N/A'))} | EV(R) : {safe_num(row.get('ev_r', pd.NA), 2)}<br>"
                                f"Decision : {safe_text(row.get('matrix_action', 'N/A'))}<extra></extra>"
                            ),
                        )
                    )
                    fig_mini.add_trace(
                        go.Scatter(
                            x=[risk_thr, risk_thr],
                            y=[0, 100],
                            mode="lines",
                            line=dict(color="#dc3545", dash="dot", width=1.4),
                            showlegend=False,
                            hovertemplate="Seuil dynamique (percentile) calculé sur l'univers du jour.<extra></extra>",
                        )
                    )
                    fig_mini.add_trace(
                        go.Scatter(
                            x=[0, 100],
                            y=[reward_thr, reward_thr],
                            mode="lines",
                            line=dict(color="#28a745", dash="dot", width=1.4),
                            showlegend=False,
                            hovertemplate="Seuil dynamique (percentile) calculé sur l'univers du jour.<extra></extra>",
                        )
                    )
                    fig_mini.update_xaxes(range=[0, 100], title="Risque")
                    fig_mini.update_yaxes(range=[0, 100], title="Reward")
                    fig_mini.update_layout(height=330, margin=dict(t=20, b=20, l=20, r=20))
                    st.plotly_chart(fig_mini, use_container_width=True)
                    st.caption("\u2022 Univers = toutes les valeurs analysées aujourd'hui")
                    st.caption("\u2022 Valeur = ticker sélectionné")

                def _score_or_na(keys: list[str], default_if_na: float = float("nan")) -> float:
                    n = _row_num(keys)
                    if pd.notna(n):
                        return float(n)
                    return default_if_na

                rd_left, rd_right, rd_extra = st.columns([1.2, 1.2, 1.0])
                with rd_left:
                    st.markdown("#### Decomposition risque (0-100)")
                    risk_parts = pd.DataFrame(
                        [
                            {"Composant": "Funda risk", "ScoreRaw": _score_or_na(["funda_risk"]), "Mesure": "Risque business/valorisation", "Source": "AG3", "ImpactTxt": "Plus haut = risque structurel plus eleve"},
                            {"Composant": "Volatilite (ATR%)", "ScoreRaw": _score_or_na(["vol_risk_score"]), "Mesure": "Amplitude moyenne des variations", "Source": "AG2", "ImpactTxt": "Plus haut = stop-out plus probable"},
                            {"Composant": "Liquidite (spread/slippage)", "ScoreRaw": _score_or_na(["liquidity_risk_score"]), "Mesure": "Cout d'execution implicite", "Source": "YF+Portfolio", "ImpactTxt": "Plus haut = execution degradee"},
                            {"Composant": "Event risk (earnings)", "ScoreRaw": _score_or_na(["event_risk_score"]), "Mesure": "Risque evenementiel proche", "Source": "YF", "ImpactTxt": "Plus haut = risque de gap"},
                            {"Composant": "News pressure", "ScoreRaw": _score_or_na(["news_risk_score"]), "Mesure": "Pression news negative", "Source": "AG4", "ImpactTxt": "Plus haut = headline risk"},
                            {"Composant": "Concentration portefeuille", "ScoreRaw": _score_or_na(["concentration_risk_score"]), "Mesure": "Poids symbole/secteur", "Source": "Portfolio", "ImpactTxt": "Plus haut = baisse de diversification"},
                            {"Composant": "Options IV risk", "ScoreRaw": _score_or_na(["options_risk_score"]), "Mesure": "Risque implicite options", "Source": "YF Options", "ImpactTxt": "Plus haut = incertitude evenementielle"},
                        ]
                    )
                    risk_parts["Score"] = risk_parts["ScoreRaw"].fillna(0.0)
                    risk_parts.loc[risk_parts["ScoreRaw"].isna(), "Mesure"] = "Donnée indisponible (source manquante)."
                    risk_parts.loc[risk_parts["ScoreRaw"].isna(), "ImpactTxt"] = "N/A: composant non calculable."
                    fig_risk = px.bar(
                        risk_parts.sort_values("Score", ascending=True),
                        x="Score",
                        y="Composant",
                        orientation="h",
                        color="Score",
                        color_continuous_scale=["#28a745", "#ffc107", "#dc3545"],
                        custom_data=["Mesure", "Source", "ImpactTxt"],
                    )
                    fig_risk.update_traces(
                        hovertemplate=(
                            "Mesure : %{customdata[0]}<br>"
                            "Source : %{customdata[1]}<br>"
                            "Impact : %{customdata[2]}<extra></extra>"
                        )
                    )
                    fig_risk.update_layout(height=320, margin=dict(t=20, b=20, l=20, r=20), coloraxis_showscale=False)
                    st.plotly_chart(fig_risk, use_container_width=True)

                with rd_right:
                    st.markdown("#### Decomposition reward (0-100)")
                    reward_parts = pd.DataFrame(
                        [
                            {"Composant": "Asymetrie R", "ScoreRaw": _score_or_na(["reward_component_r"]), "Mesure": "Asymetrie TP/Stop", "Source": "AG2+AG3", "ImpactTxt": "Plus haut = ratio plus attractif"},
                            {"Composant": "Upside fondamental", "ScoreRaw": _score_or_na(["reward_component_upside"]), "Mesure": "Potentiel target", "Source": "AG3", "ImpactTxt": "Plus haut = upside theorique plus fort"},
                            {"Composant": "Espace technique", "ScoreRaw": _score_or_na(["reward_component_space"]), "Mesure": "Distance a la resistance", "Source": "AG2", "ImpactTxt": "Plus haut = trajectoire prix plus libre"},
                            {"Composant": "Catalyseurs", "ScoreRaw": _score_or_na(["reward_component_catalyst"]), "Mesure": "News/catalyseurs favorables", "Source": "AG4", "ImpactTxt": "Plus haut = potentiel d'acceleration"},
                            {"Composant": "Trend bonus", "ScoreRaw": _score_or_na(["reward_component_trend"]), "Mesure": "Alignement tendance/regime", "Source": "AG2", "ImpactTxt": "Plus haut = momentum plus exploitable"},
                        ]
                    )
                    reward_parts["Score"] = reward_parts["ScoreRaw"].fillna(0.0)
                    reward_parts.loc[reward_parts["ScoreRaw"].isna(), "Mesure"] = "Donnée indisponible (source manquante)."
                    reward_parts.loc[reward_parts["ScoreRaw"].isna(), "ImpactTxt"] = "N/A: composant non calculable."
                    fig_reward = px.bar(
                        reward_parts.sort_values("Score", ascending=True),
                        x="Score",
                        y="Composant",
                        orientation="h",
                        color="Score",
                        color_continuous_scale=["#dc3545", "#ffc107", "#28a745"],
                        custom_data=["Mesure", "Source", "ImpactTxt"],
                    )
                    fig_reward.update_traces(
                        hovertemplate=(
                            "Mesure : %{customdata[0]}<br>"
                            "Source : %{customdata[1]}<br>"
                            "Impact : %{customdata[2]}<extra></extra>"
                        )
                    )
                    fig_reward.update_layout(height=320, margin=dict(t=20, b=20, l=20, r=20), coloraxis_showscale=False)
                    st.plotly_chart(fig_reward, use_container_width=True)

                with rd_extra:
                    st.markdown("#### Pourquoi cette decision")
                    reward_component_r = _score_or_na(["reward_component_r"], default_if_na=50.0)
                    reward_component_upside = _score_or_na(["reward_component_upside"], default_if_na=40.0)
                    reward_component_catalyst = _score_or_na(["reward_component_catalyst"], default_if_na=30.0)
                    prob_score_v = _score_or_na(["prob_score", "prob_score_for_grade"], default_if_na=50.0)
                    funda_risk_v = _score_or_na(["funda_risk"], default_if_na=50.0)
                    liquidity_risk_v = _score_or_na(["liquidity_risk_score"], default_if_na=35.0)
                    event_risk_v = _score_or_na(["event_risk_score"], default_if_na=35.0)
                    concentration_risk_v = _score_or_na(["concentration_risk_score"], default_if_na=35.0)
                    data_quality_v = _score_or_na(field_keys["data_quality"], default_if_na=60.0)
                    contributions = [
                        ("Asymetrie R", reward_component_r - 50.0, "TP/Stop effectif", "AG2+AG3"),
                        ("Upside fondamental", reward_component_upside - 40.0, "target/funda", "AG3"),
                        ("Catalyseurs news", reward_component_catalyst - 30.0, "news symbole + macro", "AG4"),
                        ("Confluence probabilite", prob_score_v - 50.0, "convergence des signaux", "AG2+AG3+AG4"),
                        ("Risque fondamental", -(funda_risk_v - 50.0), "solidite business", "AG3"),
                        ("Liquidite", -(liquidity_risk_v - 35.0), "spread/slippage", "YF+Portfolio"),
                        ("Event risk", -(event_risk_v - 35.0), "earnings", "YF"),
                        ("Concentration", -(concentration_risk_v - 35.0), "exposition portefeuille", "Portfolio"),
                        ("Data quality", data_quality_v - 60.0, "fraicheur/completude", "Derived"),
                    ]
                    if rr_outlier:
                        contributions.append(("Gate RR outlier", -22.0, "stop trop proche ou target trop loin", "Derived"))
                    if has_dnext and float(dnext_num) >= 0 and float(dnext_num) <= 7:
                        contributions.append(("Gate earnings", -25.0, "publication imminente", "YF"))
                    if invalid_options_state:
                        contributions.append(("Etat options invalide", -30.0, "qualite options non fiable", "YF"))
                    cdf = pd.DataFrame(contributions, columns=["Facteur", "Impact", "Detail", "Source"])
                    cdf["ImpactAbs"] = cdf["Impact"].abs()
                    pos = cdf[cdf["Impact"] > 0].sort_values("Impact", ascending=False).head(3).reset_index(drop=True)
                    neg = cdf[cdf["Impact"] < 0].sort_values("Impact", ascending=True).head(3).reset_index(drop=True)
                    cp, cn = st.columns(2)
                    with cp:
                        st.markdown("**Top 3 contributeurs positifs**")
                        if pos.empty:
                            st.caption("N/A")
                        for idx, c_row in pos.iterrows():
                            impact = safe_float(c_row.get("Impact", 0))
                            st.write(f"+ {c_row.get('Facteur')}: {impact:+.1f} (source: {c_row.get('Source')})")
                            _render_inline_info(
                                f"Positif {idx+1}",
                                f"Mesure : {safe_text(c_row.get('Detail'))}\nSource : {safe_text(c_row.get('Source'))}\nImpact : augmente la confiance du setup.",
                            )
                    with cn:
                        st.markdown("**Top 3 contributeurs négatifs**")
                        if neg.empty:
                            st.caption("N/A")
                        for idx, c_row in neg.iterrows():
                            impact = safe_float(c_row.get("Impact", 0))
                            st.write(f"- {c_row.get('Facteur')}: {impact:+.1f} (source: {c_row.get('Source')})")
                            _render_inline_info(
                                f"Negatif {idx+1}",
                                f"Mesure : {safe_text(c_row.get('Detail'))}\nSource : {safe_text(c_row.get('Source'))}\nImpact : réduit la qualité/exécutabilité du setup.",
                            )
                    if hard_blocks:
                        st.caption("Conclusion : la décision est défensive car au moins un gate HARD bloque l'exécution.")
                    elif warns:
                        st.caption("Conclusion : la décision est conditionnelle et nécessite une validation manuelle.")
                    else:
                        st.caption("Conclusion : la décision est exécutable dans les contraintes actuelles.")

                    st.markdown("#### Donnees marche")
                    dnext = safe_get(
                        row,
                        ["days_to_next_earnings", "days_to_earnings", "earnings_days_to_next"],
                        default=pd.NA,
                    )
                    dsince = safe_get(
                        row,
                        ["days_since_last_earnings", "earnings_days_since_last"],
                        default=pd.NA,
                    )
                    m1, m2, m3, m4, m5 = st.columns(5)
                    with m1:
                        spread_txt = safe_num(row.get("spreadPct", pd.NA), 2)
                        spread_show = f"{spread_txt}%" if spread_txt != "N/A" else "N/A"
                        st.metric("Spread %", spread_show)
                        _render_inline_info("Spread %", "Mesure du spread bid/ask relatif au mid. Plus haut = execution plus couteuse.")
                    with m2:
                        slip_txt = safe_num(row.get("slippageProxyPct", pd.NA), 2)
                        slip_show = f"{slip_txt}%" if slip_txt != "N/A" else "N/A"
                        st.metric("Slippage proxy %", slip_show)
                        _render_inline_info("Slippage proxy %", "Proxy de cout d'execution. Plus haut = plus de friction.")
                    with m3:
                        st.metric("IV ATM", safe_num(row.get("iv_atm", pd.NA), 3))
                        _render_inline_info("IV ATM", "Volatilite implicite at-the-money. Plus haut = risque evenementiel accru.")
                    with m4:
                        dnext_num_market = pd.to_numeric(pd.Series([dnext]), errors="coerce").iloc[0]
                        if pd.notna(dnext_num_market) and float(dnext_num_market) >= 0:
                            dnext_show = f"{safe_num(dnext_num_market, 1)} j"
                        else:
                            dnext_show = "N/A"
                        st.metric("Jours avant earnings", dnext_show)
                        _render_inline_info("Jours avant earnings", "Distance a la prochaine publication. Proche = risque de gap.")
                    with m5:
                        st.metric("Regime marche", safe_text(row.get("ai_regime_d1", "N/A")))
                        _render_inline_info("Regime marche", "Contexte directionnel global du marche pour le timing.")
                    st.caption(
                        f"Jours depuis earnings: {safe_num(dsince, 1)} | "
                        f"Raison action: {safe_text(row.get('action_reason', ''), default='N/A')}"
                    )
                    if not options_ok:
                        note = safe_text(
                            row.get("options_note", ""),
                            default="Aucune option Yahoo disponible (cas courant sur titres FR).",
                        )
                        st.info(note)

                    with st.expander("\U0001F9FE Pourquoi cette décision (trace)", expanded=False):
                        reason_raw = safe_text(row.get("action_reason", ""), default="N/A")
                        gates_raw = safe_text(row.get("gate_summary", ""), default="N/A")
                        if reason_raw != "N/A":
                            for token in reason_raw.split("|"):
                                tok = safe_text(token, default="")
                                if tok:
                                    st.write(f"\u2022 {tok} -> pénalise ou favorise la décision.")
                        if gates_raw != "N/A":
                            for token in gates_raw.split("|"):
                                tok = safe_text(token, default="")
                                if tok:
                                    st.write(f"\u2022 {tok} -> contrainte active de contrôle risque.")
                        if reason_raw == "N/A" and gates_raw == "N/A":
                            st.caption("N/A")
            else:
                s1, s2, s3, s4, s5, s6 = st.columns(6)
                s1.metric("Conviction", f"{safe_num(row.get('conviction_score', pd.NA), 1)}/100", delta=safe_text(row.get("conclusion", "")))
                s2.metric("Tech", safe_text(row.get("tech_action", "N/A")))
                s3.metric("Funda", f"{safe_num(row.get('funda_score', pd.NA), 0)}/100")
                s4.metric("Risque", f"{safe_num(row.get('funda_risk', pd.NA), 0)}/100")
                s5.metric("Upside", f"{safe_num(row.get('funda_upside', pd.NA), 1)}%")
                s6.metric("News 7j", safe_num(row.get('symbol_news_impact_7d', pd.NA), 1))

                st.markdown("#### Conclusion de synthese")
                st.write(
                    f"{selected_symbol}: {safe_text(row.get('conclusion', ''), default='N/A')}. "
                    f"Macro 30j={safe_num(row.get('macro_impact_30d', pd.NA), 1)}, "
                    f"News symbole 7j={safe_num(row.get('symbol_news_impact_7d', pd.NA), 1)}, "
                    f"themes macro dominants={safe_text(row.get('macro_themes', ''), default='N/A')}."
                )

            st.divider()
            st.markdown("#### Resume news")
            rn1, rn2, rn3, rn4 = st.columns(4)
            rn1.metric("Impact symbole 7j", safe_num(row.get("symbol_news_impact_7d", pd.NA), 2))
            rn2.metric("Impact macro 30j", safe_num(row.get("macro_impact_30d", pd.NA), 2))
            rn3.metric("Theme dominant", safe_text(row.get("macro_themes", "N/A")))
            rn4.metric("Regime", safe_text(row.get("ai_regime_d1", "N/A")))
            c_left, c_right = st.columns(2)

            with c_left:
                st.markdown("#### News macro reliees (30j)")
                sec = _clean_context_token(row.get("sector", ""))
                ind = _clean_context_token(row.get("industry", ""))
                macro_show = macro_news_norm.copy()
                if not macro_show.empty:
                    cut30 = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=30)
                    if "publishedat" in macro_show.columns:
                        macro_show["publishedat"] = pd.to_datetime(macro_show["publishedat"], errors="coerce", utc=True)
                        macro_show = macro_show[macro_show["publishedat"] >= cut30].copy()
                    else:
                        macro_show["publishedat"] = pd.NaT
                    macro_show["relevance_score"] = macro_show.apply(
                        lambda x: _macro_relevance_score(x, sec, ind, selected_symbol.lower()),
                        axis=1,
                    )
                    macro_show["relevance_class"] = "Background"
                    macro_show.loc[macro_show["relevance_score"] >= 55, "relevance_class"] = "Relevant"
                    macro_show.loc[macro_show["relevance_score"] >= 85, "relevance_class"] = "Highly relevant"
                    macro_show["impactscore"] = safe_float_series(macro_show.get("impactscore", pd.Series(pd.NA, index=macro_show.index)))
                    macro_show = macro_show.sort_values(["impactscore", "publishedat"], ascending=[False, False], na_position="last")
                if macro_show.empty:
                    st.caption("Aucune news macro reliee sur 30 jours.")
                else:
                    cards = st.columns(3)
                    for idx, (_, m_row) in enumerate(macro_show.head(3).iterrows()):
                        if idx >= 3:
                            break
                        title = str(m_row.get("title", "") or "").strip()
                        title = title[:120] + ("..." if len(title) > 120 else "")
                        cards[idx].markdown(
                            f"**{_fmt_dt_short(m_row.get('publishedat'))}**  \n"
                            f"{title}  \n"
                            f"Theme `{str(m_row.get('theme', '') or 'N/A')}` | Impact `{safe_num(m_row.get('impactscore', pd.NA), 1)}` | `{str(m_row.get('relevance_class', 'Background'))}`"
                        )
                    cols = [c for c in ["publishedat", "theme", "title", "impactscore", "regime", "relevance_score", "relevance_class", "winners", "losers"] if c in macro_show.columns]
                    render_interactive_table(
                        macro_show[cols].head(10).rename(
                            columns={
                                "publishedat": "Date",
                                "theme": "Theme",
                                "title": "Titre",
                                "impactscore": "Impact",
                                "regime": "Regime",
                                "relevance_score": "Score pertinence",
                                "relevance_class": "Classe",
                                "winners": "Winners",
                                "losers": "Losers",
                            }
                        ),
                        key_suffix=f"macro_symbol_{selected_symbol}",
                        height=320,
                    )
                    with st.expander("Voir toutes les news macro (30j)"):
                        render_interactive_table(
                            macro_show[cols].rename(
                                columns={
                                    "publishedat": "Date",
                                    "theme": "Theme",
                                    "title": "Titre",
                                    "impactscore": "Impact",
                                    "regime": "Regime",
                                    "relevance_score": "Score pertinence",
                                    "relevance_class": "Classe",
                                    "winners": "Winners",
                                    "losers": "Losers",
                                }
                            ),
                            key_suffix=f"macro_symbol_all_{selected_symbol}",
                            height=340,
                        )

            with c_right:
                st.markdown("#### News specifiques symbole (30j)")
                sym_show = symbol_news_norm.copy()
                if not sym_show.empty:
                    cut30 = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=30)
                    if "publishedat" in sym_show.columns:
                        sym_show["publishedat"] = pd.to_datetime(sym_show["publishedat"], errors="coerce", utc=True)
                    else:
                        sym_show["publishedat"] = pd.NaT
                    sym_show["symbol"] = sym_show.get("symbol", pd.Series("", index=sym_show.index)).astype(str).str.upper()
                    sym_show = sym_show[(sym_show["symbol"] == selected_symbol) & (sym_show["publishedat"] >= cut30)].copy()
                    sym_show["impactscore"] = safe_float_series(sym_show.get("impactscore", pd.Series(pd.NA, index=sym_show.index)))
                    urg_text = sym_show.get("urgency", pd.Series("", index=sym_show.index)).fillna("").astype(str).str.lower().str.strip()
                    urg_rank = pd.Series(1, index=sym_show.index)
                    urg_rank.loc[urg_text.str.contains("high|urgent|crit")] = 3
                    urg_rank.loc[urg_text.str.contains("med")] = 2
                    urg_rank.loc[urg_text.str.contains("low")] = 1
                    sym_show["urgency_rank"] = urg_rank
                    sym_show = sym_show.sort_values(["urgency_rank", "impactscore", "publishedat"], ascending=[False, False, False], na_position="last")
                if sym_show.empty:
                    last_ag4 = pd.NaT
                    if not symbol_news_norm.empty and "publishedat" in symbol_news_norm.columns:
                        last_ag4 = symbol_news_norm["publishedat"].max()
                    if pd.isna(last_ag4) and not macro_news_norm.empty and "publishedat" in macro_news_norm.columns:
                        last_ag4 = macro_news_norm["publishedat"].max()
                    st.caption(
                        "Aucune news specifique sur 30 jours. "
                        f"Derniere collecte AG4: {_fmt_dt_short(last_ag4)}."
                    )
                else:
                    cards = st.columns(3)
                    for idx, (_, s_row) in enumerate(sym_show.head(3).iterrows()):
                        if idx >= 3:
                            break
                        title = str(s_row.get("title", "") or "").strip()
                        title = title[:120] + ("..." if len(title) > 120 else "")
                        summary = str(s_row.get("summary", "") or "").strip()
                        summary = summary[:170] + ("..." if len(summary) > 170 else "")
                        cards[idx].markdown(
                            f"**{_fmt_dt_short(s_row.get('publishedat'))}**  \n"
                            f"{title}  \n"
                            f"Impact `{safe_num(s_row.get('impactscore', pd.NA), 1)}` | Sentiment `{str(s_row.get('sentiment', '') or 'N/A')}`  \n"
                            f"{summary}"
                        )
                    cols = [c for c in ["publishedat", "title", "impactscore", "sentiment", "urgency", "confidence", "summary"] if c in sym_show.columns]
                    render_interactive_table(
                        sym_show[cols].head(10).rename(
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
                    with st.expander("Voir toutes les news symbole (30j)"):
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
                            key_suffix=f"symbol_news_all_{selected_symbol}",
                            height=340,
                        )

            if use_matrix:
                st.divider()
                t_tech, t_funda, t_news, t_port, t_audit = st.tabs(
                    ["Tech (AG2)", "Funda (AG3)", "News (AG4)", "Portfolio overlay", "Audit data"]
                )

                with t_tech:
                    st.markdown("#### Contexte technique")
                    st.caption("Comment lire: ce bloc explique le timing (tendance, momentum, support/resistance).")
                    st.caption("Plus les signaux AG2 sont alignes, plus la probabilite du setup augmente.")
                    t1, t2, t3, t4, t5, t6 = st.columns(6)
                    t1.metric("Action AG2", safe_text(row.get("tech_action", "N/A")))
                    t2.metric("Confiance", safe_num(row.get("tech_confidence", pd.NA), 1))
                    t3.metric("RSI D1", safe_num(row.get("d1_rsi14", pd.NA), 1))
                    t4.metric("MACD hist D1", safe_num(row.get("d1_macd_hist", pd.NA), 3))
                    atr_txt = safe_num(row.get("d1_atr_pct", pd.NA), 2)
                    t5.metric("ATR D1 %", f"{atr_txt}%" if atr_txt != "N/A" else "N/A")
                    t6.metric("Regime", safe_text(row.get("ai_regime_d1", "N/A")))
                    st.caption(
                        f"Support={safe_num(row.get('d1_support', pd.NA), 2)} | "
                        f"Resistance={safe_num(row.get('d1_resistance', pd.NA), 2)} | "
                        f"Dist support={safe_num(row.get('d1_dist_sup_pct', pd.NA), 2)}% | "
                        f"Dist resistance={safe_num(row.get('d1_dist_res_pct', pd.NA), 2)}% | "
                        f"Derniere maj tech={_fmt_dt_short(row.get('last_tech_date', pd.NA))}"
                    )

                with t_funda:
                    st.markdown("#### Contexte fondamental")
                    st.caption("Comment lire: ce bloc mesure qualite business, valorisation et risque fondamental.")
                    st.caption("Un score funda eleve avec risque modere soutient les decisions de renforcement.")
                    f1, f2, f3, f4, f5, f6 = st.columns(6)
                    f1.metric("Score Funda", f"{safe_num(row.get('funda_score', pd.NA), 1)}/100")
                    f2.metric("Risque Funda", f"{safe_num(row.get('funda_risk', pd.NA), 1)}/100")
                    f3.metric("Upside", f"{safe_num(row.get('funda_upside', pd.NA), 2)}%")
                    f4.metric("Target", safe_num(row.get("target_price", pd.NA), 2))
                    f5.metric("Reco", safe_text(row.get("recommendation", "N/A")))
                    f6.metric("Horizon", safe_text(row.get("funda_horizon", "N/A")))
                    st.caption(
                        f"Quality={safe_num(row.get('quality_score', pd.NA), 1)} | "
                        f"Growth={safe_num(row.get('growth_score', pd.NA), 1)} | "
                        f"Valuation={safe_num(row.get('valuation_score', pd.NA), 1)} | "
                        f"Health={safe_num(row.get('health_score', pd.NA), 1)} | "
                        f"Consensus={safe_num(row.get('consensus_score', pd.NA), 1)} | "
                        f"Derniere maj funda={_fmt_dt_short(row.get('last_funda_date', pd.NA))}"
                    )

                with t_news:
                    st.markdown("#### Resume news et contexte")
                    st.caption("Comment lire: ce bloc resume les catalyseurs macro et specifiques au symbole.")
                    st.caption("Impact positif soutient le reward, impact negatif augmente le risque headline.")
                    st.caption(
                        f"Impact news symbole 7j={safe_num(row.get('symbol_news_impact_7d', pd.NA), 2)} | "
                        f"Impact macro 30j={safe_num(row.get('macro_impact_30d', pd.NA), 2)} | "
                        f"Theme macro dominant={safe_text(row.get('macro_themes', 'N/A'))}"
                    )
                    st.caption("Le detail Top 3 / Top 10 macro et symbole est affiche juste au-dessus.")

                with t_port:
                    st.markdown("#### Overlay portefeuille et regime")
                    st.caption("Comment lire: ce bloc identifie le risque de concentration portefeuille.")
                    st.caption("Poids symbole/secteur eleves => sizing reduit ou pas de renforcement.")
                    p1, p2, p3, p4 = st.columns(4)
                    p1.metric("Poids symbole", f"{safe_num(row.get('symbol_weight_pct', pd.NA), 2)}%")
                    p2.metric("Poids secteur", f"{safe_num(row.get('sector_weight_pct', pd.NA), 2)}%")
                    p3.metric("Concentration risk", f"{safe_num(row.get('concentration_risk_score', pd.NA), 1)}/100")
                    p4.metric("Regime marche", safe_text(row.get("ai_regime_d1", "N/A")))
                    st.caption(
                        f"Cluster proxy portefeuille={safe_num(row.get('concentration_risk_score', pd.NA), 1)}/100 | "
                        f"Action recommandee={safe_text(row.get('matrix_action', 'N/A'))}"
                    )
                    if df_port is not None and not df_port.empty and "symbol" in df_port.columns:
                        pos = df_port.copy()
                        pos["symbol"] = pos["symbol"].astype(str).str.strip().str.upper()
                        pos_show = pos[pos["symbol"] == selected_symbol].copy()
                        if pos_show.empty:
                            st.caption("Aucune position portefeuille active sur ce symbole.")
                        else:
                            cols_pos = [c for c in ["symbol", "name", "sector", "industry", "quantity", "avgprice", "lastprice", "marketvalue", "unrealizedpnl", "unrealizedpnl_pct"] if c in pos_show.columns]
                            render_interactive_table(
                                pos_show[cols_pos].rename(
                                    columns={
                                        "symbol": "Symbole",
                                        "name": "Nom",
                                        "sector": "Secteur",
                                        "industry": "Industrie",
                                        "quantity": "Quantite",
                                        "avgprice": "Prix moyen",
                                        "lastprice": "Dernier prix",
                                        "marketvalue": "Valeur marche",
                                        "unrealizedpnl": "PnL latent",
                                        "unrealizedpnl_pct": "PnL latent %",
                                    }
                                ),
                                key_suffix=f"portfolio_symbol_{selected_symbol}",
                                height=220,
                            )

                with t_audit:
                    st.markdown("#### Fraicheur et qualite des sources")
                    st.caption("Comment lire: ce bloc mesure la fraicheur des donnees et leur impact sur la confiance.")
                    st.caption("Si statut critique, eviter execution automatique et revalider les donnees.")
                    now_utc = pd.Timestamp.now(tz="UTC")
                    age_h1 = pd.to_numeric(pd.Series([row.get("data_age_h1_hours", pd.NA)]), errors="coerce").iloc[0]
                    age_d1 = pd.to_numeric(pd.Series([row.get("data_age_d1_hours", pd.NA)]), errors="coerce").iloc[0]
                    age_funda_d = pd.to_numeric(pd.Series([row.get("funda_age_days", pd.NA)]), errors="coerce").iloc[0]
                    last_news_ts = _to_dt_utc(row.get("last_news_date", pd.NA))
                    age_news_h = pd.NA
                    if pd.notna(last_news_ts):
                        age_news_h = (now_utc - last_news_ts).total_seconds() / 3600.0
                    yf_age_h = pd.to_numeric(pd.Series([row.get("yf_age_h", pd.NA)]), errors="coerce").iloc[0]

                    audit_rows = [
                        {
                            "Source": "AG2 H1",
                            "Derniere MAJ": _fmt_dt_short(row.get("last_tech_date", pd.NA)),
                            "Age": f"{float(age_h1):.1f}h" if pd.notna(age_h1) else "N/A",
                            "Statut": _freshness_status(float(age_h1), 24.0, 72.0) if pd.notna(age_h1) else "WARN",
                            "Impact": "Timing court terme",
                        },
                        {
                            "Source": "AG2 D1",
                            "Derniere MAJ": _fmt_dt_short(row.get("last_tech_date", pd.NA)),
                            "Age": f"{float(age_d1):.1f}h" if pd.notna(age_d1) else "N/A",
                            "Statut": _freshness_status(float(age_d1), 36.0, 96.0) if pd.notna(age_d1) else "WARN",
                            "Impact": "Structure tendance",
                        },
                        {
                            "Source": "AG3 Funda",
                            "Derniere MAJ": _fmt_dt_short(row.get("last_funda_date", pd.NA)),
                            "Age": f"{float(age_funda_d):.1f}j" if pd.notna(age_funda_d) else "N/A",
                            "Statut": _freshness_status(float(age_funda_d) * 24.0, 24.0 * 30.0, 24.0 * 90.0) if pd.notna(age_funda_d) else "WARN",
                            "Impact": "Business et valorisation",
                        },
                        {
                            "Source": "AG4 News",
                            "Derniere MAJ": _fmt_dt_short(last_news_ts),
                            "Age": f"{float(age_news_h):.1f}h" if pd.notna(age_news_h) else "N/A",
                            "Statut": _freshness_status(float(age_news_h), 48.0, 120.0) if pd.notna(age_news_h) else "WARN",
                            "Impact": "Catalyseurs",
                        },
                        {
                            "Source": "YF Enrichment",
                            "Derniere MAJ": _fmt_dt_short(row.get("yf_fetched_at", pd.NA)),
                            "Age": f"{float(yf_age_h):.1f}h" if pd.notna(yf_age_h) else "N/A",
                            "Statut": _freshness_status(float(yf_age_h), 36.0, 96.0) if pd.notna(yf_age_h) else "WARN",
                            "Impact": "Spread/IV/Earnings",
                        },
                    ]
                    audit_df = pd.DataFrame(audit_rows)
                    audit_df["Statut"] = audit_df["Statut"].map({"OK": "OK", "WARN": "A surveiller", "BLOCK": "Critique"}).fillna("A surveiller")
                    render_interactive_table(audit_df, key_suffix=f"audit_symbol_{selected_symbol}", height=260)
                    st.caption(
                        f"Data quality globale: {safe_num(row.get('data_quality_score', pd.NA), 1)}/100 | "
                        f"Gate summary: {safe_text(row.get('gate_summary', 'N/A'))}"
                    )


# ============================================================
# PAGE X: MACRO & NEWS (AG4)
# ============================================================

elif page == "Macro & News (AG4)":
    st.title("Macro & News (AG4)")
    st.caption("Vue macro AG4 + news par valeur AG4-SPE (normalisation robuste, scoring actionnable, observabilite pipeline).")

    ctrl_days, ctrl_limit = st.columns([1.2, 1.2], gap="medium")
    with ctrl_days:
        ag4_window_days = int(
            st.selectbox(
                "Fenetre historique (jours)",
                options=[7, 30, 90],
                index=[7, 30, 90].index(HISTORY_DAYS_DEFAULT) if HISTORY_DAYS_DEFAULT in [7, 30, 90] else 1,
                key="ag4_window_days",
            )
        )
    with ctrl_limit:
        ag4_history_limit = int(
            st.number_input(
                "Limite lignes",
                min_value=1000,
                max_value=100000,
                step=1000,
                value=int(min(max(HISTORY_LIMIT_DEFAULT, 1000), 100000)),
                key="ag4_history_limit",
            )
        )
    scope_catalog, df_positions_active, active_ag1_key = _news_scope_catalog_from_ag1()
    active_label = AG1_MULTI_PORTFOLIO_CONFIG.get(active_ag1_key, {}).get("label", active_ag1_key or "—")
    use_active_sql_scope = st.toggle(
        "Limiter SQL news symbole au portefeuille actif",
        value=False,
        key="ag4_sql_scope_active_only",
    )
    ag4_scope_symbols: tuple[str, ...] = ()
    if use_active_sql_scope and df_positions_active is not None and not df_positions_active.empty and "symbol" in df_positions_active.columns:
        ag4_scope_symbols = tuple(
            sorted(
                {
                    str(s).strip().upper()
                    for s in df_positions_active["symbol"].dropna().astype(str).tolist()
                    if str(s).strip()
                }
            )
        )

    if st.button("Rafraichir", key="refresh_ag4_news"):
        load_data.clear()
        load_ag4_page_data.clear()
        load_ag1_multi_portfolios.clear()
        st.rerun()

    ag4_page_data = load_ag4_page_data(
        AG4_DUCKDB_PATH,
        ag4_db_sig,
        AG4_SPE_DUCKDB_PATH,
        ag4_spe_db_sig,
        ag4_window_days,
        ag4_history_limit,
        RUN_LOG_LIMIT,
        ag4_scope_symbols,
    )

    df_macro_raw = ag4_page_data.get("df_news_macro_history", pd.DataFrame())
    df_spe_raw = ag4_page_data.get("df_news_symbol_history", pd.DataFrame())
    df_macro_runs = ag4_page_data.get("df_news_macro_runs", pd.DataFrame())
    df_spe_runs = ag4_page_data.get("df_news_symbol_runs", pd.DataFrame())

    df_macro_news = normalize_news_schema(df_macro_raw, "MACRO")
    df_spe_news = normalize_news_schema(df_spe_raw, "SPE")

    if (df_macro_news is None or df_macro_news.empty) and (df_spe_news is None or df_spe_news.empty):
        st.warning("Aucune donnee AG4 / AG4-SPE exploitable apres normalisation. Verifiez les tables `news_history` et le mapping.")
        st.stop()

    top_tabs = st.tabs(["Vue Macro (Overview)", "News par valeur", "Historique runs", "Qualite pipeline"])

    with top_tabs[0]:
        st.caption(f"Allocation active (bridge AG1): {active_label}")
        render_macro_overview(df_macro_news, df_macro_runs, df_positions_optional=df_positions_active)

    with top_tabs[1]:
        st.caption(
            f"Scopes disponibles: portefeuille actif ({active_label}), tous portefeuilles AG1, universe complet. "
            "La table est triée par headline risk 7j."
        )
        render_symbol_news(
            df_spe_news,
            scope_catalog=scope_catalog,
            df_universe_optional=df_univ,
            df_positions_active=df_positions_active,
        )

    with top_tabs[2]:
        render_news_runs_history(df_macro_news, df_spe_news, df_macro_runs, df_spe_runs)

    with top_tabs[3]:
        render_news_health(df_macro_news, df_spe_news)


# ============================================================
# PAGE 4: ANALYSE TECHNIQUE V2
# ============================================================

elif page == "Analyse Technique V2":
    st.title("Analyse Technique V2 (AG2)")

    if st.button("Rafraichir", key="refresh_v2"):
        load_data.clear()
        load_ag2_page_data.clear()
        load_ag2_history.clear()
        st.rerun()

    ag2_page_data = load_ag2_page_data(
        DUCKDB_PATH,
        ag2_db_sig,
        RUN_LOG_LIMIT,
    )

    df_signals = ag2_page_data.get("df_signals", pd.DataFrame())
    df_runs = ag2_page_data.get("df_runs", pd.DataFrame())

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
        df_ov = _ag2_prepare_overview_working_df(df_signals)
        if df_ov is None or df_ov.empty:
            st.warning("Aucune donnee AG2 exploitable pour la vue d'ensemble.")
        else:
            run_meta = _ag2_latest_run_meta(df_runs, df_ov)
            counts_all = _ag2_kpi_counts(df_ov)

            latest_d1_ts = _latest_timestamp(df_ov, ["d1_ts", "workflow_ts", "workflow_date"])
            latest_h1_ts = _latest_timestamp(df_ov, ["h1_ts", "workflow_ts", "workflow_date"])
            latest_workflow_ts = _latest_timestamp(df_ov, ["workflow_ts", "workflow_date"])
            age_d1_h = _ag2_age_hours(latest_d1_ts)
            age_h1_h = _ag2_age_hours(latest_h1_ts)
            age_workflow_h = _ag2_age_hours(latest_workflow_ts)

            missing_core_mask = pd.Series(False, index=df_ov.index)
            for col in ["symbol", "d1_action_norm", "d1_score_num"]:
                if col not in df_ov.columns:
                    missing_core_mask = pd.Series(True, index=df_ov.index)
                    break
                if col.endswith("_num"):
                    missing_core_mask = missing_core_mask | pd.isna(df_ov[col])
                else:
                    missing_core_mask = missing_core_mask | df_ov[col].astype(str).str.strip().eq("")
            missing_core_pct = float(missing_core_mask.mean() * 100.0) if len(df_ov) else 100.0

            status_level = "OK"
            status_reasons = []
            if len(df_ov) == 0:
                status_level = "ERROR"
                status_reasons.append("table vide")
            if age_d1_h is None and age_h1_h is None:
                status_level = "ERROR"
                status_reasons.append("dates H1/D1 indisponibles")
            elif (age_d1_h is not None and age_d1_h > 36.0) or (age_h1_h is not None and age_h1_h > 24.0):
                if status_level != "ERROR":
                    status_level = "WARN"
                status_reasons.append(f"fraicheur D1/H1 ({_ag2_fmt_age(age_d1_h)} / {_ag2_fmt_age(age_h1_h)})")
            if missing_core_pct > 25.0:
                if status_level != "ERROR":
                    status_level = "WARN"
                status_reasons.append(f"champs manquants eleves ({missing_core_pct:.0f}%)")
            if run_meta.get("run_status") and str(run_meta.get("run_status")).upper() not in ("SUCCESS", "OK"):
                if status_level != "ERROR":
                    status_level = "WARN"
                status_reasons.append(f"run_status={run_meta.get('run_status')}")

            with st.container(border=True):
                c_run1, c_run2, c_run3, c_run4 = st.columns([2.3, 1.0, 1.1, 1.8])
                run_ts = pd.to_datetime(run_meta.get("run_ts", pd.NaT), errors="coerce", utc=True)
                if pd.isna(run_ts):
                    run_ts = pd.to_datetime(latest_workflow_ts, errors="coerce", utc=True)
                run_ts_txt = run_ts.tz_convert("Europe/Paris").strftime("%Y-%m-%d %H:%M") if pd.notna(run_ts) else "—"
                c_run1.markdown(
                    (
                        "<div style='font-size:0.95rem;font-weight:700;'>Run & sante AG2</div>"
                        f"<div style='margin-top:4px;'>Dernier run: <code>{html.escape(_ag2_short_run_id(run_meta.get('run_id')))}</code> | "
                        f"{html.escape(run_ts_txt)}</div>"
                    ),
                    unsafe_allow_html=True,
                )
                c_run2.markdown(
                    f"<div style='margin-top:0.45rem'>{_ag2_status_pill_html(status_level)}</div>",
                    unsafe_allow_html=True,
                )
                c_run3.metric(
                    "Freshness D1",
                    _ag2_fmt_age(age_d1_h),
                    _freshness_label_from_age(age_d1_h if age_d1_h is not None else pd.NA, 36.0, 96.0) if age_d1_h is not None else "Manquant",
                    delta_color="off",
                )
                c_run4.markdown(
                    (
                        f"**Barres H1**: {_ag2_fmt_age(age_h1_h)}  \n"
                        f"**Scan AG2**: {_ag2_fmt_age(age_workflow_h)}  \n"
                        f"**Nulls coeur**: {missing_core_pct:.0f}%"
                    )
                    + (f"  \n**Notes**: {', '.join(status_reasons[:2])}" if status_reasons else "")
                )

            buy_count = counts_all["buy_count"]
            sell_count = counts_all["sell_count"]
            neutral_count = counts_all["neutral_count"]
            total_symbols = counts_all["total_symbols"]
            actionable_count = counts_all["actionable_count"]
            ai_calls = counts_all["ai_calls"]
            ai_approvals = counts_all["ai_approvals"]

            approval_rate_txt = _ag2_ratio_text(ai_approvals, ai_calls)
            buy_ratio_txt = _ag2_ratio_text(buy_count, actionable_count, suffix=" actionable")
            sell_ratio_txt = _ag2_ratio_text(sell_count, actionable_count, suffix=" actionable")
            neutral_ratio_txt = _ag2_ratio_text(neutral_count, total_symbols, suffix=" total")
            ai_call_cov_txt = _ag2_ratio_text(ai_calls, total_symbols, suffix=" total")

            kc1, kc2, kc3, kc4, kc5, kc6 = st.columns(6)
            kc1.metric("Symboles analyses", total_symbols, _ag2_delta_text(total_symbols, run_meta.get("prev_total"), digits=0), delta_color="off")
            kc2.metric("BUY", buy_count, buy_ratio_txt, delta_color="off")
            kc3.metric("SELL", sell_count, sell_ratio_txt, delta_color="off")
            kc4.metric("NEUTRAL", neutral_count, neutral_ratio_txt, delta_color="off")
            kc5.metric("Appels IA", ai_calls, _ag2_delta_text(ai_calls, run_meta.get("prev_ai_calls"), digits=0) or ai_call_cov_txt, delta_color="off")
            kc6.metric("IA approuves", ai_approvals, _ag2_delta_text(ai_approvals, run_meta.get("prev_ai_approvals"), digits=0) or f"Tx {approval_rate_txt}", delta_color="off")

            with st.container(border=True):
                st.markdown("#### Filtres rapides & scope")

                d1_actions_available = sorted([x for x in df_ov.get("d1_action_norm", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x])
                h1_actions_available = sorted([x for x in df_ov.get("h1_action_norm", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x])
                sector_options = sorted([x for x in df_ov.get("sector", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x])
                ai_status_options = sorted([x for x in df_ov.get("ai_decision_norm", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x and x != "—"])

                f1, f2, f3, f4, f5 = st.columns([1.4, 1.4, 1.6, 1.4, 1.2])
                selected_d1_actions = f1.multiselect("Action D1", d1_actions_available, key="ag2_v3_f_d1")
                selected_h1_actions = f2.multiselect("Action H1", h1_actions_available, key="ag2_v3_f_h1")
                selected_sectors = f3.multiselect("Secteurs", sector_options, key="ag2_v3_f_sector")
                selected_ai_status = f4.multiselect("IA status", ai_status_options, key="ag2_v3_f_ia")
                graphs_scope = f5.radio("Scope graphes", ["All", "Filtered"], key="ag2_v3_scope_graphs")

                f6, f7, f8, f9, f10 = st.columns([1.2, 1.3, 1.6, 1.3, 1.0])
                only_actionable = f6.toggle("Only actionable", value=False, key="ag2_v3_f_only_actionable")
                only_divergences = f7.toggle("Only divergences", value=False, key="ag2_v3_f_only_div")
                include_neutral_div = f8.toggle("Inclure div. NEUTRAL", value=False, key="ag2_v3_f_div_neutral")
                show_advanced_cols = f9.toggle("Colonnes avancees", value=False, key="ag2_v3_f_cols_adv")
                top_n = int(f10.selectbox("Top N", [10, 15, 20], index=1, key="ag2_v3_f_top_n"))

                ai_quality_valid = pd.to_numeric(df_ov.get("ai_quality_num", pd.Series(pd.NA, index=df_ov.index)), errors="coerce").dropna()
                max_quality = float(max(10.0, ai_quality_valid.max())) if not ai_quality_valid.empty else 10.0
                quality_min = st.slider("Qualite IA min", 0.0, float(max_quality), 0.0, 0.5, key="ag2_v3_f_quality_min")

                d1_score_valid = pd.to_numeric(df_ov.get("d1_score_num", pd.Series(pd.NA, index=df_ov.index)), errors="coerce").dropna()
                if not d1_score_valid.empty:
                    score_lo = float(d1_score_valid.min())
                    score_hi = float(d1_score_valid.max())
                    if score_lo == score_hi:
                        score_lo -= 1.0
                        score_hi += 1.0
                    score_range = st.slider("Range D1 score", score_lo, score_hi, (score_lo, score_hi), 1.0, key="ag2_v3_f_score_range")
                else:
                    score_range = None
                    st.caption("Range D1 score indisponible (colonne absente ou vide).")

            df_filtered = df_ov.copy()
            if selected_d1_actions:
                df_filtered = df_filtered[df_filtered["d1_action_norm"].isin(selected_d1_actions)]
            if selected_h1_actions:
                df_filtered = df_filtered[df_filtered["h1_action_norm"].isin(selected_h1_actions)]
            if selected_sectors:
                df_filtered = df_filtered[df_filtered["sector"].isin(selected_sectors)]
            if selected_ai_status:
                df_filtered = df_filtered[df_filtered["ai_decision_norm"].isin(selected_ai_status)]
            if only_actionable:
                df_filtered = df_filtered[df_filtered["is_actionable_d1"] == True]  # noqa: E712
            if only_divergences:
                div_mask = df_filtered["is_divergence_h1d1"] == True  # noqa: E712
                if not include_neutral_div:
                    div_mask = div_mask & ~(
                        (df_filtered["h1_action_norm"] == "NEUTRAL") | (df_filtered["d1_action_norm"] == "NEUTRAL")
                    )
                df_filtered = df_filtered[div_mask]
            if quality_min > 0:
                q = pd.to_numeric(df_filtered.get("ai_quality_num", pd.Series(pd.NA, index=df_filtered.index)), errors="coerce")
                df_filtered = df_filtered[q >= float(quality_min)]
            if score_range is not None:
                sc = pd.to_numeric(df_filtered.get("d1_score_num", pd.Series(pd.NA, index=df_filtered.index)), errors="coerce")
                df_filtered = df_filtered[sc.isna() | sc.between(float(score_range[0]), float(score_range[1]), inclusive="both")]

            df_scope = df_filtered if graphs_scope == "Filtered" else df_ov
            counts_scope = _ag2_kpi_counts(df_scope)
            counts_filtered = _ag2_kpi_counts(df_filtered)
            st.caption(
                f"Graphes={graphs_scope} | Filtre: {len(df_filtered)}/{len(df_ov)} lignes | "
                f"Actionables(scope)={counts_scope['actionable_count']} | Tx approb IA(scope)={_ag2_ratio_text(counts_scope['ai_approvals'], counts_scope['ai_calls'])}"
            )

            g1, g2, g3 = st.columns(3, gap="large")
            with g1:
                with st.container(border=True):
                    st.markdown("#### Signal mix H1 vs D1")
                    fig_mix = _ag2_signal_mix_figure(df_scope)
                    if fig_mix is None:
                        st.info("Donnees insuffisantes.")
                    else:
                        st.plotly_chart(fig_mix, use_container_width=True, config={"displayModeBar": False})
            with g2:
                with st.container(border=True):
                    st.markdown("#### Heatmap secteur x action (D1)")
                    fig_heat = _ag2_sector_action_heatmap_figure(df_scope)
                    if fig_heat is None:
                        st.info("Secteur/action indisponible.")
                    else:
                        st.plotly_chart(fig_heat, use_container_width=True, config={"displayModeBar": False})
            with g3:
                with st.container(border=True):
                    st.markdown("#### Matrice accord H1 vs D1")
                    fig_mat = _ag2_h1_d1_matrix_figure(df_scope)
                    if fig_mat is None:
                        st.info("Donnees insuffisantes.")
                    else:
                        st.plotly_chart(fig_mat, use_container_width=True, config={"displayModeBar": False})
                        mat_counts = pd.crosstab(
                            df_scope.get("h1_action_norm", pd.Series("NEUTRAL", index=df_scope.index)),
                            df_scope.get("d1_action_norm", pd.Series("NEUTRAL", index=df_scope.index)),
                        ).reindex(index=["BUY", "SELL", "NEUTRAL"], columns=["BUY", "SELL", "NEUTRAL"], fill_value=0)
                        st.caption(
                            f"BUY/BUY={int(mat_counts.loc['BUY','BUY'])} | SELL/SELL={int(mat_counts.loc['SELL','SELL'])} | "
                            f"BUY/SELL + SELL/BUY={int(mat_counts.loc['BUY','SELL']) + int(mat_counts.loc['SELL','BUY'])}"
                        )

            qleft, qright = st.columns(2, gap="large")
            with qleft:
                with st.container(border=True):
                    st.markdown("#### Funnel IA & qualite")
                    st.plotly_chart(
                        _ag2_funnel_figure(
                            counts_scope["total_symbols"],
                            counts_scope["actionable_count"],
                            counts_scope["ai_calls"],
                            counts_scope["ai_approvals"],
                        ),
                        use_container_width=True,
                        config={"displayModeBar": False},
                    )
                    ai_quality_scope = pd.to_numeric(df_scope.get("ai_quality_num", pd.Series(pd.NA, index=df_scope.index)), errors="coerce")
                    ai_quality_scope = ai_quality_scope[ai_quality_scope.notna() & (ai_quality_scope > 0)]
                    ai_dec_scope = df_scope.get("ai_decision_norm", pd.Series("—", index=df_scope.index)).astype(str)
                    q1, q2, q3, q4 = st.columns(4)
                    q1.metric("% qualite IA", _ag2_ratio_text(int(ai_quality_scope.shape[0]), len(df_scope)), delta_color="off")
                    q2.metric("Qualite IA moy", f"{float(ai_quality_scope.mean()):.1f}/10" if not ai_quality_scope.empty else "—", delta_color="off")
                    q3.metric("% REJECT", _ag2_ratio_text(int(ai_dec_scope.eq("REJECT").sum()), len(df_scope)), delta_color="off")
                    q4.metric("% SKIP", _ag2_ratio_text(int(ai_dec_scope.eq("SKIP").sum()), len(df_scope)), delta_color="off")
                    st.caption(
                        f"Freshness H1={_ag2_fmt_age(age_h1_h)} | D1={_ag2_fmt_age(age_d1_h)} | "
                        f"Status={status_level}"
                    )
            with qright:
                with st.container(border=True):
                    st.markdown("#### Scatter D1 score vs RSI")
                    fig_scatter = _ag2_score_rsi_scatter_figure(df_scope)
                    if fig_scatter is None:
                        st.info("Scatter indisponible (D1 score / RSI manquants).")
                    else:
                        st.plotly_chart(fig_scatter, use_container_width=True, config={"displayModeBar": False})
                        st.caption("Repere: BUY score haut + RSI > 70 = attention surachat ; SELL score bas + RSI < 30 = possible capitulation.")

            with st.container(border=True):
                st.markdown("#### Actionable maintenant")
                st.caption(
                    f"Source = {'filtree' if len(df_filtered) != len(df_ov) else 'globale'} | Top N={top_n} | "
                    f"Actionables filtres={counts_filtered['actionable_count']}"
                )
                top_buy_tab, top_sell_tab, top_div_tab = st.tabs(["Top BUY (D1)", "Top SELL (D1)", "Divergences H1 vs D1"])

                def _render_top(df_src: pd.DataFrame, empty_msg: str):
                    if df_src is None or df_src.empty:
                        st.info(empty_msg)
                        return
                    display_df = _ag2_make_display_table(df_src, advanced=show_advanced_cols)
                    styled_df = _ag2_style_display_table(
                        display_df,
                        df_src,
                        quality_warn_threshold=max(1.0, float(quality_min if quality_min > 0 else 4.0)),
                    )
                    st.dataframe(styled_df, use_container_width=True, hide_index=True, height=420)

                with top_buy_tab:
                    buy_df = (
                        df_filtered[df_filtered["d1_action_norm"] == "BUY"]
                        .sort_values(["d1_score_num", "ai_quality_num"], ascending=[False, False], na_position="last")
                        .head(top_n)
                    )
                    _render_top(buy_df, "Aucun signal BUY dans le scope courant.")

                with top_sell_tab:
                    sell_df = df_filtered[df_filtered["d1_action_norm"] == "SELL"].copy()
                    sell_scores = pd.to_numeric(sell_df.get("d1_score_num", pd.Series(pd.NA, index=sell_df.index)), errors="coerce")
                    sort_sell_ascending = bool((sell_scores.dropna() < 0).any()) if not sell_df.empty else True
                    sell_df = sell_df.sort_values(
                        ["d1_score_num", "ai_quality_num"],
                        ascending=[sort_sell_ascending, False],
                        na_position="last",
                    ).head(top_n)
                    _render_top(sell_df, "Aucun signal SELL dans le scope courant.")

                with top_div_tab:
                    div_include_neutral = st.toggle(
                        "Inclure divergences avec NEUTRAL",
                        value=include_neutral_div,
                        key="ag2_v3_div_tab_neutral",
                    )
                    div_df = df_filtered[df_filtered["h1_action_norm"] != df_filtered["d1_action_norm"]].copy()
                    if not div_include_neutral:
                        div_df = div_df[
                            ~((div_df["h1_action_norm"] == "NEUTRAL") | (div_df["d1_action_norm"] == "NEUTRAL"))
                        ]
                    div_df["abs_div_strength"] = (
                        pd.to_numeric(div_df.get("d1_score_num", pd.Series(0.0, index=div_df.index)), errors="coerce").abs().fillna(0.0)
                        + pd.to_numeric(div_df.get("h1_score_num", pd.Series(0.0, index=div_df.index)), errors="coerce").abs().fillna(0.0)
                    )
                    div_df = div_df.sort_values(["abs_div_strength", "ai_quality_num"], ascending=[False, False], na_position="last").head(top_n)
                    _render_top(div_df, "Aucune divergence H1/D1 dans le scope courant.")

            with st.container(border=True):
                st.markdown("#### Table complete (secondaire)")
                st.caption(
                    f"{len(df_filtered)} ligne(s) affichee(s) / {len(df_ov)} | "
                    f"Divergences={int(df_filtered.get('is_divergence_h1d1', pd.Series(False, index=df_filtered.index)).sum()) if not df_filtered.empty else 0} | "
                    "Highlights: confluence, divergences, low quality, reject sur signal fort"
                )
                if df_filtered.empty:
                    st.info("Aucune ligne apres filtres.")
                else:
                    df_display = _ag2_make_display_table(df_filtered, advanced=show_advanced_cols)
                    styled_display = _ag2_style_display_table(
                        df_display,
                        df_filtered,
                        quality_warn_threshold=max(1.0, float(quality_min if quality_min > 0 else 4.0)),
                    )
                    st.dataframe(styled_display, use_container_width=True, hide_index=True, height=560)

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

            search_txt = st.text_input(
                "Recherche texte (ticker ou nom)",
                value="",
                key="ag2_v3_symbol_search",
                placeholder="Ex: 74SW.PA, 74software, AIRBUS...",
            )
            query = str(search_txt or "").strip().lower()
            labels_filtered = [lbl for lbl in _label_list if query in lbl.lower()] if query else _label_list
            if not labels_filtered:
                st.warning("Aucun resultat pour cette recherche. La liste complete est rechargee.")
                labels_filtered = _label_list

            previous_label = st.session_state.get("ag2_v3_symbol_last", labels_filtered[0])
            if previous_label not in labels_filtered:
                previous_label = labels_filtered[0]
            default_idx = labels_filtered.index(previous_label) if previous_label in labels_filtered else 0

            selected_label = st.selectbox(
                "Sélectionner un symbole (recherche par nom ou ticker) :",
                labels_filtered,
                index=default_idx,
                key="v2_symbol_select",
            )
            st.session_state["ag2_v3_symbol_last"] = selected_label
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
            st.subheader("Historique des exécutions AG2-V3")

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

            with st.expander("Historique signaux techniques (filtré)", expanded=False):
                h1, h2 = st.columns([1.1, 1.1])
                hist_days = int(h1.selectbox("Fenêtre (jours)", [7, 30, 90], index=1, key="ag2_hist_days"))
                hist_limit = int(
                    h2.number_input(
                        "Limite lignes",
                        min_value=1000,
                        max_value=100000,
                        value=int(min(max(HISTORY_LIMIT_DEFAULT, 1000), 100000)),
                        step=1000,
                        key="ag2_hist_limit",
                    )
                )
                if st.toggle("Charger l'historique des signaux", value=False, key="ag2_hist_toggle"):
                    df_hist_signals = load_ag2_history(
                        DUCKDB_PATH,
                        ag2_db_sig,
                        hist_days,
                        hist_limit,
                    )
                    if df_hist_signals is None or df_hist_signals.empty:
                        st.info("Aucun historique technique disponible sur la fenêtre sélectionnée.")
                    else:
                        render_interactive_table(df_hist_signals, key_suffix="ag2_signals_history", height=320)


# ================================================================
# PAGE 4: ANALYSE FONDAMENTALE V2
# ================================================================
elif page == "Analyse Fondamentale V2":
    st.title("Analyse Fondamentale V2 (AG3)")

    if st.button("Rafraichir", key="refresh_funda_v2"):
        load_data.clear()
        load_ag3_page_data.clear()
        load_ag3_run_quality_history.clear()
        load_ag3_symbol_history.clear()
        st.rerun()

    ag3_page_data = load_ag3_page_data(
        AG3_DUCKDB_PATH,
        ag3_db_sig,
        RUN_LOG_LIMIT,
    )

    df_funda_latest = ag3_page_data.get("df_funda_latest", pd.DataFrame())
    df_funda_runs = ag3_page_data.get("df_funda_runs", pd.DataFrame())
    df_funda_consensus = ag3_page_data.get("df_funda_consensus", pd.DataFrame())
    df_funda_metrics = ag3_page_data.get("df_funda_metrics", pd.DataFrame())

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

        # Qualite du moteur fondamental par run (requete agregee, fenetre glissante)
        q1, q2 = st.columns([1.1, 1.1])
        run_hist_days = int(q1.selectbox("Fenetre runs (jours)", [7, 30, 90], index=1, key="ag3_run_hist_days"))
        run_hist_limit = int(
            q2.number_input(
                "Limite runs",
                min_value=200,
                max_value=100000,
                value=int(min(max(HISTORY_LIMIT_DEFAULT, 200), 100000)),
                step=200,
                key="ag3_run_hist_limit",
            )
        )
        run_perf = load_ag3_run_quality_history(
            AG3_DUCKDB_PATH,
            ag3_db_sig,
            run_hist_days,
            run_hist_limit,
        )
        if run_perf is not None and not run_perf.empty:
            run_perf["ts"] = pd.to_datetime(run_perf.get("ts"), errors="coerce")
            run_perf = run_perf.dropna(subset=["ts"]).sort_values("ts")
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
                    title="Qualite des runs AG3 (sorties)",
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

            search_txt = st.text_input(
                "Recherche texte (ticker ou nom)",
                value="",
                key="funda_v2_symbol_search",
                placeholder="Ex: BEN.PA, beneteau, LVMH...",
            )
            query = str(search_txt or "").strip().lower()
            labels_filtered = [lbl for lbl in labels if query in lbl.lower()] if query else labels
            if not labels_filtered:
                st.warning("Aucun resultat pour cette recherche. La liste complete est rechargee.")
                labels_filtered = labels

            previous_label = st.session_state.get("funda_v2_symbol_last", labels_filtered[0])
            if previous_label not in labels_filtered:
                previous_label = labels_filtered[0]
            default_idx = labels_filtered.index(previous_label) if previous_label in labels_filtered else 0

            selected_label = st.selectbox(
                "Sélectionner un symbole :",
                labels_filtered,
                index=default_idx,
                key="funda_v2_symbol",
            )
            st.session_state["funda_v2_symbol_last"] = selected_label
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

            # Evolution historique du symbole (chargement lazy par symbole)
            hs1, hs2 = st.columns([1.1, 1.1])
            symbol_hist_days = int(hs1.selectbox("Fenetre historique symbole (jours)", [7, 30, 90], index=1, key="ag3_symbol_hist_days"))
            symbol_hist_limit = int(
                hs2.number_input(
                    "Limite lignes symbole",
                    min_value=200,
                    max_value=100000,
                    value=int(min(max(HISTORY_LIMIT_DEFAULT, 200), 100000)),
                    step=200,
                    key="ag3_symbol_hist_limit",
                )
            )
            h = load_ag3_symbol_history(
                AG3_DUCKDB_PATH,
                ag3_db_sig,
                selected_symbol,
                symbol_hist_days,
                symbol_hist_limit,
            )
            if h is not None and not h.empty:
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
