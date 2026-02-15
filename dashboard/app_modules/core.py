import json
import re
from datetime import datetime

import pandas as pd


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


def safe_float_series(series: pd.Series) -> pd.Series:
    """Conversion vectorisee avec la logique safe_float pour une Serie pandas."""
    if series is None:
        return pd.Series(dtype=float)
    return series.map(safe_float).astype(float)


TRUTHY_VALUES = {"TRUE", "1", "OUI", "YES"}


def truthy_series(series: pd.Series) -> pd.Series:
    """Normalise une Serie texte en booleens (TRUE/1/OUI/YES)."""
    if series is None:
        return pd.Series(dtype=bool)
    return series.fillna("").astype(str).str.strip().str.upper().isin(TRUTHY_VALUES)


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

    if "name" in u_clean.columns:
        mapping = pd.Series(u_clean["name"].values, index=u_clean["symbol"]).to_dict()
        df["symbol"] = df["symbol"].astype(str).str.strip()
        df["name_enrich"] = df["symbol"].map(mapping).fillna("")
        if "name" not in df.columns:
            df["name"] = df["name_enrich"]
        else:
            df["name"] = df["name"].fillna(df["name_enrich"])

    if "sector" not in df.columns and "sector" in u_clean.columns:
        sec_map = pd.Series(u_clean["sector"].values, index=u_clean["symbol"]).to_dict()
        df["sector"] = df["symbol"].map(sec_map).fillna("N/A")

    return df


def check_freshness(date_val: object, days_limit: int) -> tuple[str, int]:
    """Retourne (icone, age_en_jours). Robust: None / NaT / formats ISO."""
    try:
        if date_val is None:
            return "❌", 999

        if pd.isna(date_val):
            return "❌", 999

        s = str(date_val).strip()
        if s == "" or s.lower() in ("nan", "nat", "none", "null"):
            return "❌", 999

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


def calculate_sector_sentiment(df_news: pd.DataFrame, days_lookback: int = 30) -> pd.DataFrame:
    """Barometre sectoriel base sur News_History (publishedat, impactscore, winners, losers)."""
    if df_news is None or df_news.empty:
        return pd.DataFrame()
    if "publishedat" not in df_news.columns:
        return pd.DataFrame()

    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days_lookback)
    df_recent = df_news.copy()
    df_recent["publishedat"] = pd.to_datetime(df_recent["publishedat"], errors="coerce", utc=True)
    df_recent = df_recent.dropna(subset=["publishedat"])
    df_recent = df_recent[df_recent["publishedat"] >= cutoff]
    if df_recent.empty:
        return pd.DataFrame()

    impacts = safe_float_series(df_recent.get("impactscore", pd.Series(index=df_recent.index))).abs()
    df_recent = df_recent.assign(_impact=impacts)
    df_recent = df_recent[df_recent["_impact"] > 0]
    if df_recent.empty:
        return pd.DataFrame()

    winners_col = df_recent["winners"] if "winners" in df_recent.columns else pd.Series("", index=df_recent.index)
    winners = (
        df_recent.assign(Sector=winners_col.fillna("").astype(str).str.split(","))
        .explode("Sector")
    )
    winners["Sector"] = winners["Sector"].astype(str).str.strip().str.title()
    winners = winners[winners["Sector"] != ""]
    winners["NetScore"] = winners["_impact"]
    winners = winners[["Sector", "NetScore"]]

    losers_col = df_recent["losers"] if "losers" in df_recent.columns else pd.Series("", index=df_recent.index)
    losers = (
        df_recent.assign(Sector=losers_col.fillna("").astype(str).str.split(","))
        .explode("Sector")
    )
    losers["Sector"] = losers["Sector"].astype(str).str.strip().str.title()
    losers = losers[losers["Sector"] != ""]
    losers["NetScore"] = -losers["_impact"]
    losers = losers[["Sector", "NetScore"]]

    stacked = pd.concat([winners, losers], ignore_index=True)
    if stacked.empty:
        return pd.DataFrame()

    df_res = stacked.groupby("Sector", as_index=False)["NetScore"].sum()
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

    df_recent["ImpactNum"] = safe_float_series(df_recent["impactscore"])

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

    symbol_scores["Label"] = symbol_scores["symbol"].astype(str)
    if name_col and name_col in symbol_scores.columns:
        name_s = symbol_scores[name_col].fillna("").astype(str).str.strip()
        symbol_scores["Label"] = symbol_scores["Label"].where(
            name_s == "",
            symbol_scores["Label"] + " (" + name_s + ")",
        )

    positive = symbol_scores[symbol_scores["NetScore"] > 0.1].nlargest(top_n, "NetScore")
    negative = symbol_scores[symbol_scores["NetScore"] < -0.1].nsmallest(top_n, "NetScore")

    final_df = pd.concat([positive, negative]).sort_values("NetScore", ascending=True)
    final_df["Color"] = final_df["NetScore"].apply(lambda x: "#28a745" if x >= 0 else "#dc3545")
    return final_df
