"""Page Streamlit commune AG5-AG9.

Le module ne recalcule aucune règle métier. Il ouvre les trois bases en lecture
seule et affiche exclusivement les tables/vues canoniques persistées.
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any

import duckdb
import pandas as pd


def _redact(value: Any) -> str:
    text = str(value or "")
    return re.sub(r"wm_(?:live|oat|ort)_[A-Za-z0-9_-]+", "[REDACTED]", text)


def _query(path: str, sql: str, params: list | None = None, retries: int = 4) -> tuple[pd.DataFrame, str | None]:
    if not path or not os.path.isfile(path):
        return pd.DataFrame(), "BASE_ABSENTE"
    for attempt in range(retries):
        con = None
        try:
            con = duckdb.connect(path, read_only=True)
            return con.execute(sql, params or []).fetchdf(), None
        except Exception as exc:
            detail = _redact(exc)
            if attempt + 1 >= retries or not any(token in detail.lower() for token in ("lock", "busy", "conflict")):
                return pd.DataFrame(), detail
            time.sleep(0.15 * (1.7 ** attempt))
        finally:
            if con is not None:
                try:
                    con.close()
                except Exception:
                    pass
    return pd.DataFrame(), "LECTURE_ECHOUEE"


def _json(value: Any, default: Any = None) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


def load_global_context_data(global_path: str, world_path: str, macro_path: str) -> dict:
    queries = {
        "snapshot": (global_path, "SELECT * FROM main.v_latest_global_context"),
        "components": (global_path, "SELECT * FROM main.v_component_health ORDER BY component"),
        "global_regime": (global_path, "SELECT * FROM core.global_regime WHERE snapshot_id=(SELECT snapshot_id FROM main.v_latest_global_context)"),
        "countries": (global_path, "SELECT * FROM main.v_latest_country_context ORDER BY risk_score DESC NULLS LAST"),
        "currencies": (global_path, "SELECT * FROM main.v_latest_currency_context ORDER BY currency"),
        "sectors": (global_path, "SELECT * FROM main.v_latest_sector_context ORDER BY risk_score DESC NULLS LAST"),
        "assets": (global_path, "SELECT * FROM main.v_latest_asset_context ORDER BY risk_score DESC NULLS LAST"),
        "critical_events": (global_path, "SELECT * FROM main.v_latest_critical_events"),
        "global_runs": (global_path, "SELECT * FROM core.run_log ORDER BY started_at DESC LIMIT 200"),
        "ag5": (macro_path, "SELECT * FROM main.v_latest_ag5_macro ORDER BY entity_id"),
        "ag6": (macro_path, "SELECT * FROM main.v_latest_ag6_fx_valuation ORDER BY currency"),
        "ag7": (macro_path, "SELECT * FROM main.v_latest_ag7_positioning ORDER BY entity_id"),
        "ag8": (macro_path, "SELECT * FROM main.v_latest_ag8_rates_liquidity ORDER BY currency"),
        "macro_runs": (macro_path, "SELECT * FROM components.run_log ORDER BY started_at DESC LIMIT 200"),
        "ag9_snapshot": (world_path, "SELECT * FROM main.v_latest_ag9_global_risk"),
        "ag9_events": (world_path, "SELECT * FROM main.v_latest_events"),
        "ag9_country": (world_path, "SELECT * FROM main.v_latest_country_risk ORDER BY risk_score DESC NULLS LAST"),
        "ag9_chokepoints": (world_path, "SELECT * FROM core.chokepoint_status WHERE snapshot_id=(SELECT snapshot_id FROM main.v_latest_ag9_global_risk) ORDER BY risk_score DESC NULLS LAST"),
        "ag9_energy": (world_path, "SELECT * FROM core.energy_risk WHERE snapshot_id=(SELECT snapshot_id FROM main.v_latest_ag9_global_risk) ORDER BY risk_score DESC NULLS LAST"),
        "ag9_supply": (world_path, "SELECT * FROM core.supply_chain_risk WHERE snapshot_id=(SELECT snapshot_id FROM main.v_latest_ag9_global_risk) ORDER BY risk_score DESC NULLS LAST"),
        "ag9_cyber": (world_path, "SELECT * FROM core.cyber_risk WHERE snapshot_id=(SELECT snapshot_id FROM main.v_latest_ag9_global_risk) ORDER BY risk_score DESC NULLS LAST"),
        "ag9_sources": (world_path, "SELECT * FROM main.v_source_health ORDER BY capability"),
        "ag9_runs": (world_path, "SELECT * FROM core.run_log ORDER BY started_at DESC LIMIT 200"),
        "tool_registry": (world_path, "SELECT capability, domain, tool_name, tool_contract_hash, discovery_status, compatible, discovered_at, config_version, detail FROM cfg.tool_registry ORDER BY capability"),
        "event_decay": (world_path, "SELECT * FROM cfg.event_decay ORDER BY event_type"),
        "neutral_rates": (macro_path, "SELECT * FROM cfg.neutral_rates ORDER BY currency"),
    }
    data, errors = {}, {}
    for key, (path, query) in queries.items():
        frame, error = _query(path, query)
        data[key] = frame
        if error:
            errors[key] = error
    data["errors"] = errors
    data["paths"] = {"global_context": global_path, "worldmonitor": world_path, "macro": macro_path}
    return data


def _show_frame(st, frame: pd.DataFrame, *, height: int = 360) -> None:
    if frame is None or frame.empty:
        st.info("Aucune donnée persistée pour cette vue.")
        return
    st.dataframe(frame, width="stretch", hide_index=True, height=height)


def _metric_value(row: pd.Series | dict, key: str, default: str = "—") -> Any:
    value = row.get(key) if hasattr(row, "get") else None
    return default if value is None or (isinstance(value, float) and pd.isna(value)) else value


def render_global_context_tab(st, *, global_path: str, world_path: str, macro_path: str) -> None:
    data = load_global_context_data(global_path, world_path, macro_path)
    st.title("Contexte global")
    st.caption("AG5–AG9 commun Actions/ETF/Forex — lecture seule des vues canoniques; aucun recalcul Streamlit.")

    snapshot = data["snapshot"]
    if snapshot.empty:
        st.warning("Aucun snapshot global publié. Les pages restent consultables pour diagnostiquer les composants.")
    else:
        row = snapshot.iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Snapshot", str(_metric_value(row, "snapshot_id"))[:28])
        c2.metric("Statut", _metric_value(row, "status"))
        c3.metric("Fraîcheur", _metric_value(row, "freshness_status"))
        c4.metric("Couverture", f"{100 * float(_metric_value(row, 'coverage_ratio', 0) or 0):.1f}%")
        c5.metric("Confiance", f"{100 * float(_metric_value(row, 'confidence', 0) or 0):.1f}%")

    tabs = st.tabs([
        "Vue synthétique", "AG5 — Macro & Flows", "AG6 — Valorisation FX",
        "AG7 — Positionnement", "AG8 — Taux & Liquidité",
        "AG9 — Global Risk Intelligence", "Qualité des données", "Historique", "Méthodologie",
    ])

    with tabs[0]:
        st.subheader("Santé des composants")
        _show_frame(st, data["components"], height=260)
        st.subheader("Contexte devises consolidé")
        _show_frame(st, data["currencies"])
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Overlays pays")
            _show_frame(st, data["countries"], height=300)
        with c2:
            st.subheader("Overlays secteurs")
            _show_frame(st, data["sectors"], height=300)
        if not data["assets"].empty:
            st.subheader("Actifs affectés — expositions connues seulement")
            _show_frame(st, data["assets"], height=300)

    with tabs[1]:
        st.caption("Balance courante comparable en % du PIB; les montants absolus exclus sont visibles dans la lineage.")
        _show_frame(st, data["ag5"], height=520)
        st.subheader("Estimations de taux neutres configurables")
        _show_frame(st, data["neutral_rates"], height=300)

    with tabs[2]:
        st.info("Périmètre : valorisation relative des devises et risque de change. Ce composant ne valorise pas les actions.")
        _show_frame(st, data["ag6"], height=520)

    with tabs[3]:
        st.caption("Le millésime CFTC hebdomadaire, le seuil de crowding et le statut proxy USD restent visibles.")
        _show_frame(st, data["ag7"], height=520)

    with tabs[4]:
        st.caption("Régimes et overlays explicables; aucune prescription long/short n'est produite.")
        _show_frame(st, data["ag8"], height=520)

    with tabs[5]:
        _render_ag9(st, data)

    with tabs[6]:
        st.subheader("Erreurs et bases")
        if data["errors"]:
            error_rows = [{"Vue": key, "Erreur": _redact(value)} for key, value in data["errors"].items()]
            _show_frame(st, pd.DataFrame(error_rows), height=300)
        else:
            st.success("Toutes les vues demandées sont lisibles.")
        st.subheader("Source health AG9")
        _show_frame(st, data["ag9_sources"], height=420)
        st.subheader("Registry de capacités")
        _show_frame(st, data["tool_registry"], height=420)

    with tabs[7]:
        h1, h2, h3 = st.tabs(["Synthèse", "AG9", "AG5–AG8"])
        with h1:
            _show_frame(st, data["global_runs"], height=520)
        with h2:
            _show_frame(st, data["ag9_runs"], height=520)
        with h3:
            _show_frame(st, data["macro_runs"], height=520)

    with tabs[8]:
        st.markdown("""
Les valeurs manquantes restent manquantes. Les composites disponibles sont bornés et renormalisés,
avec couverture et confiance publiées. AG5 décrit la macro structurelle; AG6 la valorisation FX;
AG7 le positionnement; AG8 les taux/liquidité; AG9 les risques événementiels structurés.

AG4 demeure la source `AG4_NEWS_SENTIMENT`; AG9 demeure `AG9_GLOBAL_RISK`. Le contexte transmis à
AG1 est consultatif et ne modifie ni gates, ni quantités, ni consensus, ni Risk Manager.
""")
        st.subheader("Demi-vies événementielles versionnées")
        _show_frame(st, data["event_decay"], height=420)


def _render_ag9(st, data: dict) -> None:
    snapshot = data["ag9_snapshot"]
    if snapshot.empty:
        st.warning("AG9 n'a pas encore publié de snapshot World Monitor.")
    else:
        row = snapshot.iloc[0]
        a, b, c, d, e = st.columns(5)
        a.metric("Pipeline", _metric_value(row, "freshness_status"))
        b.metric("Régime", _metric_value(row, "global_risk_regime"))
        c.metric("Score", f"{float(_metric_value(row, 'global_risk_score', 0) or 0):.3f}")
        d.metric("Confiance", f"{100 * float(_metric_value(row, 'confidence', 0) or 0):.1f}%")
        e.metric("Événements critiques", int(_metric_value(row, "critical_event_count", 0) or 0))

    ag9_tabs = st.tabs([
        "Synthèse", "Événements critiques", "Risque pays", "Chokepoints & supply chain",
        "Énergie & matières premières", "Cyber & infrastructures", "Source health",
        "Historique des runs", "Méthodologie",
    ])
    with ag9_tabs[0]:
        _show_frame(st, snapshot, height=220)
        st.subheader("Secteurs et actifs affectés")
        _show_frame(st, data["sectors"], height=280)
        _show_frame(st, data["assets"], height=280)
    with ag9_tabs[1]:
        _show_frame(st, data["ag9_events"], height=560)
    with ag9_tabs[2]:
        _show_frame(st, data["ag9_country"], height=520)
    with ag9_tabs[3]:
        _show_frame(st, data["ag9_chokepoints"], height=300)
        _show_frame(st, data["ag9_supply"], height=300)
    with ag9_tabs[4]:
        _show_frame(st, data["ag9_energy"], height=520)
    with ag9_tabs[5]:
        _show_frame(st, data["ag9_cyber"], height=520)
        st.caption("Les incidents infrastructure sans mapping fiable restent dans les événements avec exposition inconnue.")
    with ag9_tabs[6]:
        _show_frame(st, data["ag9_sources"], height=520)
        _show_frame(st, data["tool_registry"], height=420)
    with ag9_tabs[7]:
        _show_frame(st, data["ag9_runs"], height=560)
    with ag9_tabs[8]:
        st.code("effective_score = severity × confidence × source_diversity × freshness_decay × relevance", language="text")
        st.code("aggregate = 1 - product(1 - individual_score)", language="text")
        _show_frame(st, data["event_decay"], height=420)
