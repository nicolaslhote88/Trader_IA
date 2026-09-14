"""Vue de synthèse Streamlit du contexte de marché AG5–AG8.

La page ne recalcule aucune règle métier. Elle transforme uniquement les
scores, statuts et métadonnées canoniques persistés en indicateurs visuels.
"""

from __future__ import annotations

import html
import json
import math
import os
import re
import time
from collections import Counter
from typing import Any

import duckdb
import pandas as pd


DISPLAY_CURRENCY_ORDER = (
    "USD", "EUR", "JPY", "GBP", "CHF", "CAD", "AUD", "NZD",
    "NOK", "SEK", "MXN", "KRW",
)

FRESHNESS_LABELS = {
    "fresh": "À jour",
    "aging": "À surveiller",
    "stale": "Périmé",
    "missing": "Indisponible",
}

POLICY_LABELS = {
    "restrictive": "Restrictive",
    "tightening": "Resserrement",
    "easing": "Assouplissement",
    "accommodative": "Accommodante",
    "neutral": "Neutre",
}

AG1_POLICY_LABELS = {
    "NORMAL": ("Utilisation normale", "positive"),
    "CAUTION": ("Utilisé avec prudence", "warning"),
    "CAVEAT_ONLY": ("Limites uniquement", "warning"),
    "IGNORE": ("Contexte ignoré", "negative"),
}

AGENT_GUIDES = {
    "AG5": {
        "measure": "Croissance, inflation, politique et taux réels, balance courante, situation budgétaire et emploi.",
        "colors": "Vert : environnement macro plutôt porteur pour la devise. Rouge : environnement plutôt défavorable. Gris : signal faible ou proche de zéro.",
        "ag1_effect": "Peut renforcer ou tempérer la conviction sur une action/ETF libellé dans cette devise. Ne remplace jamais les fondamentaux de l'entreprise ni les gates d'entrée.",
    },
    "AG6": {
        "measure": "Valorisation relative de la devise : carry, carry réel, parité de pouvoir d'achat, REER et termes de l'échange.",
        "colors": "Vert : devise relativement attractive ou décotée. Rouge : devise relativement chère. Ce n'est pas la valorisation de l'action.",
        "ag1_effect": "Donne le contexte de change d'une position ou opportunité. Le LLM peut nuancer sa conviction, mais ne doit jamais vendre une action uniquement à cause du change et AG6 ne remplace pas AG3.",
    },
    "AG7": {
        "measure": "Positionnement spéculatif CFTC comparé à son historique; le score est volontairement contrariant.",
        "colors": "Vert : marché très short, donc potentiel de rebond contrariant. Rouge : marché très long, donc risque de correction. Vert ne signifie pas “fondamentaux solides”.",
        "ag1_effect": "Aide le LLM à éviter de poursuivre un mouvement déjà surchargé ou à reconnaître un soutien contrariant. Ce signal ne déclenche jamais un ordre à lui seul.",
    },
    "AG8": {
        "measure": "Régime des banques centrales, courbe des taux, taux réels, liquidité et pression exercée sur les actifs de longue duration.",
        "colors": "Ici l'échelle est un risque : vert = pression faible; orange = vigilance; rouge = pression forte, notamment pour les actifs sensibles aux taux.",
        "ag1_effect": "Peut pousser le LLM à être plus prudent sur les valeurs très sensibles aux taux ou à privilégier la résilience. Les limites de poids et le Risk Manager restent autoritaires.",
    },
}

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


def load_global_context_data(
    global_path: str,
    world_path: str,
    macro_path: str,
    ag1_path: str | None = None,
    *,
    include_diagnostics: bool = True,
) -> dict:
    """Charge les vues canoniques en lecture seule.

    La page utilise uniquement le petit jeu de requêtes de synthèse. Le mode
    diagnostic reste disponible pour les tests et les outils d'exploitation,
    sans imposer le coût des anciennes tables à chaque affichage.
    """

    queries = {
        "snapshot": (global_path, "SELECT * FROM main.v_latest_global_context"),
        "components": (global_path, "SELECT * FROM main.v_component_health ORDER BY component"),
        "ag5": (macro_path, "SELECT * FROM main.v_latest_ag5_macro ORDER BY entity_id"),
        "ag6": (macro_path, "SELECT * FROM main.v_latest_ag6_fx_valuation ORDER BY currency"),
        "ag7": (macro_path, "SELECT * FROM main.v_latest_ag7_positioning ORDER BY entity_id"),
        "ag8": (macro_path, "SELECT * FROM main.v_latest_ag8_rates_liquidity ORDER BY currency"),
    }
    if ag1_path:
        queries["ag1_brief"] = (
            ag1_path,
            """
            SELECT run_id, ts_start, model, global_context_status,
                   global_context_age, global_context_pack_json
            FROM core.runs
            WHERE global_context_pack_json IS NOT NULL
            ORDER BY ts_start DESC
            LIMIT 1
            """,
        )
    if include_diagnostics:
        queries.update({
            "global_regime": (global_path, "SELECT * FROM core.global_regime WHERE snapshot_id=(SELECT snapshot_id FROM main.v_latest_global_context)"),
            "countries": (global_path, "SELECT * FROM main.v_latest_country_context ORDER BY risk_score DESC NULLS LAST"),
            "currencies": (global_path, "SELECT * FROM main.v_latest_currency_context ORDER BY currency"),
            "sectors": (global_path, "SELECT * FROM main.v_latest_sector_context ORDER BY risk_score DESC NULLS LAST"),
            "assets": (global_path, "SELECT * FROM main.v_latest_asset_context ORDER BY risk_score DESC NULLS LAST"),
            "critical_events": (global_path, "SELECT * FROM main.v_latest_critical_events"),
            "global_runs": (global_path, "SELECT * FROM core.run_log ORDER BY started_at DESC LIMIT 200"),
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
        })

    data, errors = {}, {}
    for key, (path, query) in queries.items():
        frame, error = _query(path, query)
        data[key] = frame
        if error:
            errors[key] = error
    data["errors"] = errors
    data["paths"] = {
        "global_context": global_path,
        "worldmonitor": world_path,
        "macro": macro_path,
        "ag1": ag1_path,
    }
    return data


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return default if not math.isfinite(number) else number


def _records(frame: Any) -> list[dict]:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return []
    return frame.to_dict(orient="records")


def _first_row(frame: Any) -> dict:
    rows = _records(frame)
    return rows[0] if rows else {}


def _entry_map(rows: list[dict], key: str) -> dict[str, dict]:
    result = {}
    for row in rows:
        entity = str(row.get(key) or "").upper()
        if entity:
            result[entity] = row
    return result


def _pack_entries(pack: dict, section: str) -> dict[str, dict]:
    raw = pack.get(section, {}) if isinstance(pack, dict) else {}
    by_currency = raw.get("by_currency", {}) if isinstance(raw, dict) else {}
    if isinstance(by_currency, dict):
        return {str(key).upper(): value for key, value in by_currency.items() if isinstance(value, dict)}
    return {}


def _component_health(data: dict) -> dict[str, dict]:
    return {
        str(row.get("component") or "").upper(): row
        for row in _records(data.get("components"))
        if row.get("component")
    }


def _merged_entries(primary: dict[str, dict], fallback: dict[str, dict]) -> dict[str, dict]:
    merged = {key: dict(value) for key, value in fallback.items()}
    for key, value in primary.items():
        merged.setdefault(key, {}).update(value)
    return merged


def _sort_entries(entries: dict[str, dict], field: str, *, reverse: bool = True) -> list[tuple[str, dict]]:
    available = [
        (currency, row)
        for currency, row in entries.items()
        if _safe_float(row.get(field)) is not None
    ]
    return sorted(available, key=lambda item: _safe_float(item[1].get(field), 0.0), reverse=reverse)


def _health_value(health: dict, field: str, entries: dict[str, dict]) -> float | None:
    direct = _safe_float(health.get(field))
    if direct is not None:
        return direct
    values = [_safe_float(row.get(field)) for row in entries.values()]
    values = [value for value in values if value is not None]
    return sum(values) / len(values) if values else None


def _freshness(health: dict, entries: dict[str, dict]) -> str:
    direct = str(health.get("freshness_status") or "").lower()
    if direct:
        return direct
    values = [str(row.get("freshness_status") or "").lower() for row in entries.values()]
    if "stale" in values:
        return "stale"
    if "aging" in values:
        return "aging"
    if "fresh" in values:
        return "fresh"
    return "missing"


def _agent_quality(health: dict, entries: dict[str, dict]) -> dict:
    return {
        "status": str(health.get("status") or ("OK" if entries else "MISSING")).upper(),
        "coverage": _health_value(health, "coverage_ratio", entries),
        "confidence": _health_value(health, "confidence", entries),
        "freshness": _freshness(health, entries),
        "age_hours": _safe_float(health.get("age_hours")),
    }


def _signal(currency: str, value: Any, *, detail: str = "", risk: bool = False) -> dict:
    return {
        "currency": currency,
        "value": _safe_float(value),
        "detail": detail,
        "risk": risk,
    }


def _top_bottom_signals(entries: dict[str, dict], field: str) -> list[dict]:
    ranked = _sort_entries(entries, field)
    if not ranked:
        return []
    strongest = ranked[:2]
    weakest = list(reversed(ranked[-2:]))
    seen, signals = set(), []
    for currency, row in strongest + weakest:
        if currency not in seen:
            signals.append(_signal(currency, row.get(field)))
            seen.add(currency)
    return signals


def _build_agent_summaries(data: dict, pack: dict) -> dict[str, dict]:
    health = _component_health(data)

    ag5 = _merged_entries(
        _pack_entries(pack, "macro_regime"),
        _entry_map(_records(data.get("ag5")), "entity_id"),
    )
    ag6 = _merged_entries(
        _pack_entries(pack, "fx_relative_valuation"),
        _entry_map(_records(data.get("ag6")), "currency"),
    )
    ag7 = _merged_entries(
        _pack_entries(pack, "positioning_regime"),
        _entry_map(_records(data.get("ag7")), "entity_id"),
    )
    ag8 = _merged_entries(
        _pack_entries(pack, "rates_liquidity_regime"),
        _entry_map(_records(data.get("ag8")), "currency"),
    )

    macro_ranked = _sort_entries(ag5, "macro_score")
    valuation_ranked = _sort_entries(ag6, "valuation_score")
    positioning_ranked = _sort_entries(ag7, "positioning_score")
    duration_ranked = _sort_entries(ag8, "duration_pressure")

    macro_lead = macro_ranked[0] if macro_ranked else None
    macro_lag = macro_ranked[-1] if macro_ranked else None
    value_lead = valuation_ranked[0] if valuation_ranked else None
    value_lag = valuation_ranked[-1] if valuation_ranked else None
    crowded = [
        (currency, row)
        for currency, row in ag7.items()
        if bool(row.get("crowded_flag"))
    ]
    crowded.sort(key=lambda item: abs(_safe_float(item[1].get("z_score"), 0.0)), reverse=True)
    duration_lead = duration_ranked[0] if duration_ranked else None

    macro_summary = (
        f"{macro_lead[0]} porte la dynamique macro ({_format_score(macro_lead[1].get('macro_score'))}), "
        f"{macro_lag[0]} ferme la marche ({_format_score(macro_lag[1].get('macro_score'))})."
        if macro_lead and macro_lag else "Aucun score macro disponible."
    )
    valuation_summary = (
        f"Écart relatif positif sur {value_lead[0]} ({_format_score(value_lead[1].get('valuation_score'))}); "
        f"le plus négatif est {value_lag[0]} ({_format_score(value_lag[1].get('valuation_score'))})."
        if value_lead and value_lag else "Aucun score de valorisation disponible."
    )
    if crowded:
        crowded_text = ", ".join(
            f"{currency} {str(row.get('crowded_direction') or '').lower()}"
            for currency, row in crowded[:3]
        )
        positioning_summary = f"Positionnements extrêmes détectés : {crowded_text}."
    elif positioning_ranked:
        positioning_summary = "Aucun positionnement extrême détecté sur le dernier millésime."
    else:
        positioning_summary = "Aucune donnée de positionnement disponible."

    policy_counts = Counter(str(row.get("policy_regime") or "unknown").lower() for row in ag8.values())
    dominant_policy = policy_counts.most_common(1)[0][0] if policy_counts else "unknown"
    rates_summary = (
        f"Pression duration maximale sur {duration_lead[0]} ({_format_pct(duration_lead[1].get('duration_pressure'))}); "
        f"régime monétaire dominant : {POLICY_LABELS.get(dominant_policy, dominant_policy)}."
        if duration_lead else "Aucune donnée de taux disponible."
    )

    positioning_signals = []
    for currency, row in crowded[:3]:
        direction = str(row.get("crowded_direction") or "extrême").lower()
        positioning_signals.append(
            _signal(currency, row.get("positioning_score"), detail=f"Crowding {direction}")
        )
    if not positioning_signals:
        positioning_signals = _top_bottom_signals(ag7, "positioning_score")

    rates_signals = []
    for currency, row in duration_ranked[:4]:
        policy = POLICY_LABELS.get(str(row.get("policy_regime") or "").lower(), str(row.get("policy_regime") or "—"))
        rates_signals.append(_signal(currency, row.get("duration_pressure"), detail=policy, risk=True))

    return {
        "AG5": {
            "title": "Macro & flux",
            "eyebrow": "Dynamique structurelle",
            "description": macro_summary,
            "guide": AGENT_GUIDES["AG5"],
            "entries": ag5,
            "score_field": "macro_score",
            "quality": _agent_quality(health.get("AG5", {}), ag5),
            "signals": _top_bottom_signals(ag5, "macro_score"),
        },
        "AG6": {
            "title": "Valorisation FX",
            "eyebrow": "Écarts relatifs",
            "description": valuation_summary,
            "guide": AGENT_GUIDES["AG6"],
            "entries": ag6,
            "score_field": "valuation_score",
            "quality": _agent_quality(health.get("AG6", {}), ag6),
            "signals": _top_bottom_signals(ag6, "valuation_score"),
        },
        "AG7": {
            "title": "Positionnement",
            "eyebrow": "Crowding & lecture contrariante",
            "description": positioning_summary,
            "guide": AGENT_GUIDES["AG7"],
            "entries": ag7,
            "score_field": "positioning_score",
            "quality": _agent_quality(health.get("AG7", {}), ag7),
            "signals": positioning_signals,
        },
        "AG8": {
            "title": "Taux & liquidité",
            "eyebrow": "Régimes monétaires",
            "description": rates_summary,
            "guide": AGENT_GUIDES["AG8"],
            "entries": ag8,
            "score_field": "duration_pressure",
            "quality": _agent_quality(health.get("AG8", {}), ag8),
            "signals": rates_signals,
        },
    }


def _build_ag1_brief(data: dict) -> dict:
    row = _first_row(data.get("ag1_brief"))
    pack = _json(row.get("global_context_pack_json"), {}) or {}
    policy = str(pack.get("use_policy") or "UNKNOWN").upper()
    label, tone = AG1_POLICY_LABELS.get(policy, ("Brief non disponible", "missing"))
    explanations = {
        "NORMAL": "Les détails frais et suffisamment fiables sont remis aux trois LLM pour leur raisonnement consultatif.",
        "CAUTION": "Les détails sont remis aux trois LLM, avec consigne explicite de les utiliser avec prudence.",
        "CAVEAT_ONLY": "Les scores détaillés sont retirés; les LLM ne reçoivent que les limites et réserves de qualité.",
        "IGNORE": "Le contexte global est indisponible, périmé ou désactivé; les LLM doivent raisonner comme avant sans l'utiliser.",
    }
    component_keys = {
        "AG5": "macro",
        "AG6": "fx_valuation",
        "AG7": "positioning",
        "AG8": "rates_liquidity",
    }
    raw_components = pack.get("component_summary") or {}
    components = {
        agent_id: raw_components.get(pack_key, {})
        for agent_id, pack_key in component_keys.items()
    }
    relevant = [str(value).upper() for value in pack.get("relevant_currencies") or []]
    included = [
        agent_id
        for agent_id, component in components.items()
        if str(component.get("details") or "").upper() == "INCLUDED"
    ]
    return {
        "available": bool(row and pack),
        "run_id": str(row.get("run_id") or ""),
        "as_of": row.get("ts_start"),
        "status": str(pack.get("status") or row.get("global_context_status") or "UNKNOWN").upper(),
        "policy": policy,
        "policy_label": label,
        "policy_tone": tone,
        "policy_explanation": explanations.get(policy, "Aucun brief AG1 exploitable n'a été trouvé."),
        "relevant_currencies": relevant,
        "components": components,
        "included_components": included,
        "currency_signals": pack.get("currency_signals") or {},
        "quality": pack.get("quality") or {},
        "warnings": [str(value) for value in pack.get("source_warnings") or []],
    }


def build_market_context_summary(data: dict) -> dict:
    """Construit un modèle de présentation à partir des valeurs persistées."""

    snapshot = _first_row(data.get("snapshot"))
    pack = _json(snapshot.get("ag1_pack_json"), {}) or {}
    agents = _build_agent_summaries(data, pack)
    ag1_brief = _build_ag1_brief(data)
    status = str(snapshot.get("status") or "MISSING").upper()
    freshness = str(snapshot.get("freshness_status") or "missing").lower()

    if status == "OK" and freshness == "fresh":
        headline = "Contexte disponible et à jour"
        tone = "positive"
    elif status == "OK" and freshness == "aging":
        headline = "Contexte disponible, fraîcheur à surveiller"
        tone = "warning"
    elif freshness == "stale":
        headline = "Contexte périmé — prudence renforcée"
        tone = "negative"
    else:
        headline = "Contexte partiel ou indisponible"
        tone = "negative"

    available_currencies = {
        currency
        for agent in agents.values()
        for currency in agent["entries"]
    }
    ordered_currencies = [currency for currency in DISPLAY_CURRENCY_ORDER if currency in available_currencies]
    ordered_currencies.extend(sorted(available_currencies - set(ordered_currencies)))
    relevant_currencies = set(ag1_brief["relevant_currencies"])

    matrix = []
    for currency in ordered_currencies:
        ag5 = agents["AG5"]["entries"].get(currency, {})
        ag6 = agents["AG6"]["entries"].get(currency, {})
        ag7 = agents["AG7"]["entries"].get(currency, {})
        ag8 = agents["AG8"]["entries"].get(currency, {})
        if not any((ag5, ag6, ag7, ag8)):
            continue
        matrix.append({
            "currency": currency,
            "ag5": _safe_float(ag5.get("macro_score")),
            "ag6": _safe_float(ag6.get("valuation_score")),
            "ag7": _safe_float(ag7.get("positioning_score")),
            "ag7_crowded": bool(ag7.get("crowded_flag")),
            "ag8": _safe_float(ag8.get("duration_pressure")),
            "ag8_policy": str(ag8.get("policy_regime") or ""),
            "ag1_relevant": currency in relevant_currencies,
        })

    return {
        "snapshot": {
            "id": str(snapshot.get("snapshot_id") or ""),
            "as_of": snapshot.get("as_of"),
            "status": status,
            "freshness": freshness,
            "coverage": _safe_float(snapshot.get("coverage_ratio")),
            "confidence": _safe_float(snapshot.get("confidence")),
            "headline": headline,
            "tone": tone,
        },
        "agents": agents,
        "ag1_brief": ag1_brief,
        "universe": {
            "currencies": ordered_currencies,
            "count": len(ordered_currencies),
            "coverage_by_agent": {
                agent_id: len(agent["entries"])
                for agent_id, agent in agents.items()
            },
        },
        "matrix": matrix,
        "errors": data.get("errors", {}),
    }


def _format_score(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return "—"
    return f"{number:+.2f}".replace(".", ",")


def _format_pct(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return "—"
    return f"{100 * number:.0f} %"


def _format_age(value: Any) -> str:
    hours = _safe_float(value)
    if hours is None:
        return "âge inconnu"
    return f"{hours:.1f} h".replace(".", ",")


def _format_datetime(value: Any) -> str:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return "heure inconnue"
    return timestamp.tz_convert("Europe/Paris").strftime("%d/%m/%Y · %H:%M")


def _esc(value: Any) -> str:
    return html.escape(str(value or ""), quote=True)


def _tone_for_score(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return "missing"
    if number >= 0.1:
        return "positive"
    if number <= -0.1:
        return "negative"
    return "neutral"


def _tone_for_risk(value: Any) -> str:
    number = _safe_float(value)
    if number is None:
        return "missing"
    if number >= 0.65:
        return "negative"
    if number >= 0.4:
        return "warning"
    return "positive"


def _freshness_tone(value: str) -> str:
    return {"fresh": "positive", "aging": "warning", "stale": "negative"}.get(value, "missing")


def _progress(value: Any, css_class: str = "positive") -> str:
    number = _safe_float(value, 0.0) or 0.0
    width = max(0.0, min(100.0, number * 100.0))
    return (
        '<div class="gctx-progress" aria-hidden="true">'
        f'<span class="gctx-progress-fill {css_class}" style="width:{width:.1f}%"></span>'
        "</div>"
    )


def _signed_bar(value: Any, *, risk: bool = False) -> str:
    number = _safe_float(value)
    if number is None:
        return '<div class="gctx-mini-track"><span class="gctx-mini-missing"></span></div>'
    if risk:
        width = max(0.0, min(100.0, number * 100.0))
        tone = _tone_for_risk(number)
        return f'<div class="gctx-mini-track risk"><span class="{tone}" style="left:0;width:{width:.1f}%"></span></div>'
    clipped = max(-1.0, min(1.0, number))
    left = 50.0 if clipped >= 0 else 50.0 + clipped * 50.0
    width = abs(clipped) * 50.0
    tone = _tone_for_score(clipped)
    return (
        '<div class="gctx-mini-track signed"><i></i>'
        f'<span class="{tone}" style="left:{left:.1f}%;width:{width:.1f}%"></span>'
        "</div>"
    )


def _render_hero(summary: dict) -> str:
    snap = summary["snapshot"]
    freshness = FRESHNESS_LABELS.get(snap["freshness"], snap["freshness"] or "Indisponible")
    snapshot_id = snap["id"]
    short_id = snapshot_id[-8:] if snapshot_id else "—"
    return f"""
<section class="gctx-hero {snap['tone']}">
  <div class="gctx-hero-main">
    <div class="gctx-kicker">VERDICT GLOBAL</div>
    <div class="gctx-headline"><span class="gctx-status-dot"></span>{_esc(snap['headline'])}</div>
    <div class="gctx-meta">Mise à jour {_esc(_format_datetime(snap['as_of']))} · snapshot #{_esc(short_id)}</div>
  </div>
  <div class="gctx-kpis">
    <div class="gctx-kpi">
      <div><span>Couverture</span><strong>{_format_pct(snap['coverage'])}</strong></div>
      {_progress(snap['coverage'], 'positive')}
    </div>
    <div class="gctx-kpi">
      <div><span>Confiance</span><strong>{_format_pct(snap['confidence'])}</strong></div>
      {_progress(snap['confidence'], 'accent')}
    </div>
    <div class="gctx-kpi freshness">
      <span>Fraîcheur</span>
      <strong class="gctx-badge {_freshness_tone(snap['freshness'])}">{_esc(freshness)}</strong>
    </div>
  </div>
</section>
"""


def _render_reading_guide(summary: dict) -> str:
    universe = summary["universe"]
    ag7_currencies = set(summary["agents"]["AG7"]["entries"])
    missing_ag7 = [currency for currency in universe["currencies"] if currency not in ag7_currencies]
    missing_note = ", ".join(missing_ag7) if missing_ag7 else "aucune"
    return f"""
<section class="gctx-guide-grid">
  <article class="gctx-guide-card">
    <div class="gctx-guide-number">1</div>
    <div><strong>La couleur décrit le sens du signal</strong>
    <p>Pour AG5–AG7, vert = soutien selon l'angle de l'agent et rouge = frein. Pour AG8, l'échelle mesure un risque : rouge = forte pression de taux. Les badges de fraîcheur décrivent la qualité, pas la direction du marché.</p></div>
  </article>
  <article class="gctx-guide-card">
    <div class="gctx-guide-number">2</div>
    <div><strong>{universe['count']} devises suivies, quelques signaux mis en avant</strong>
    <p>Chaque carte montre seulement les extrêmes les plus informatifs. La matrice affiche tout l'univers. AG7 ne couvre pas directement { _esc(missing_note) } faute de série CFTC fiable; une case absente ne signifie jamais “neutre”.</p></div>
  </article>
  <article class="gctx-guide-card">
    <div class="gctx-guide-number">3</div>
    <div><strong>Couverture et confiance ne sont pas des signaux d'achat</strong>
    <p>La couverture mesure la part des données attendues disponible; la confiance en pondère la qualité; l'âge indique le temps depuis la publication du composant. Ces trois valeurs disent combien croire le signal.</p></div>
  </article>
</section>
"""


def _render_ag1_component(agent_id: str, component: dict) -> str:
    details = str(component.get("details") or "OMITTED").upper()
    included = details == "INCLUDED"
    relevant = int(_safe_float(component.get("relevant_rows"), 0.0) or 0)
    usable = int(_safe_float(component.get("usable_rows"), 0.0) or 0)
    confidence = _format_pct(component.get("confidence"))
    freshness_key = str(component.get("freshness") or "missing").lower()
    freshness = FRESHNESS_LABELS.get(freshness_key, freshness_key)
    tone = _freshness_tone(freshness_key) if included else "negative"
    state = "Inclus" if included else "Omis"
    return f"""
<div class="gctx-ag1-component {tone}">
  <div><strong>{_esc(agent_id)}</strong><span class="gctx-badge {tone}">{state}</span></div>
  <p>{usable}/{relevant} devises utilisables · confiance {confidence} · {_esc(freshness)}</p>
</div>
"""


def _render_ag1_brief(summary: dict) -> str:
    brief = summary["ag1_brief"]
    if not brief["available"]:
        return """
<section class="gctx-ag1-panel missing">
  <div class="gctx-kicker">DERNIER BRIEF AG1 V4</div>
  <h3>Aucun brief journalisé disponible</h3>
  <p>La vue explique l'univers AG5–AG8, mais ne peut pas confirmer les devises effectivement envoyées au dernier run AG1.</p>
</section>
"""

    currencies = "".join(f'<span class="gctx-currency-chip">{_esc(currency)}</span>' for currency in brief["relevant_currencies"])
    if not currencies:
        currencies = '<span class="gctx-muted">Aucune devise pertinente</span>'
    components = "".join(
        _render_ag1_component(agent_id, brief["components"].get(agent_id, {}))
        for agent_id in ("AG5", "AG6", "AG7", "AG8")
    )
    short_run = brief["run_id"][-12:] if brief["run_id"] else "—"
    return f"""
<section class="gctx-ag1-panel">
  <div class="gctx-ag1-head">
    <div>
      <div class="gctx-kicker">CE QU'AG1 A RÉELLEMENT REÇU AU DERNIER RUN</div>
      <h3>Brief complémentaire AG5–AG8</h3>
      <p class="gctx-meta">Run …{_esc(short_run)} · {_esc(_format_datetime(brief['as_of']))}</p>
    </div>
    <span class="gctx-badge {brief['policy_tone']}">{_esc(brief['policy_label'])}</span>
  </div>
  <p class="gctx-policy-explanation">{_esc(brief['policy_explanation'])}</p>
  <div class="gctx-ag1-scope">
    <div><span class="gctx-small-label">DEVISES PERTINENTES POUR CE RUN</span><div class="gctx-chips">{currencies}</div></div>
    <p>Ce filtre est recalculé à chaque exécution à partir des devises du portefeuille et des opportunités éligibles. Les autres devises restent suivies par AG5–AG8 mais ne consomment pas de place dans le prompt AG1.</p>
  </div>
  <div class="gctx-ag1-components">{components}</div>
  <div class="gctx-flow" aria-label="Chaîne d'utilisation du contexte global par AG1">
    <div><strong>1 · AG5–AG8</strong><span>produisent les signaux</span></div><b>→</b>
    <div><strong>2 · Filtrage</strong><span>garde les devises utiles au run</span></div><b>→</b>
    <div><strong>3 · Trois LLM</strong><span>reçoivent exactement le même brief</span></div><b>→</b>
    <div><strong>4 · Consensus 2/3</strong><span>puis gates et Risk Manager</span></div>
  </div>
  <div class="gctx-boundary"><strong>Influence réelle :</strong> ce brief peut modifier la conviction, la sélection entre candidats éligibles et le poids proposé dans la fourchette autorisée. Il ne peut ni rendre une opportunité inéligible éligible, ni contourner une gate, ni produire un ordre seul. L'attribution exacte d'une décision à un score précis nécessiterait un replay A/B; elle n'est pas déterministe.</div>
</section>
"""


def _render_signal(signal: dict) -> str:
    value = signal.get("value")
    risk = bool(signal.get("risk"))
    tone = _tone_for_risk(value) if risk else _tone_for_score(value)
    rendered_value = _format_pct(value) if risk else _format_score(value)
    detail = f'<span class="gctx-signal-detail">{_esc(signal.get("detail"))}</span>' if signal.get("detail") else ""
    return f"""
<div class="gctx-signal-row">
  <div class="gctx-signal-name"><strong>{_esc(signal.get('currency'))}</strong>{detail}</div>
  {_signed_bar(value, risk=risk)}
  <span class="gctx-score {tone}">{_esc(rendered_value)}</span>
</div>
"""


def _render_agent_card(agent_id: str, agent: dict) -> str:
    quality = agent["quality"]
    freshness = FRESHNESS_LABELS.get(quality["freshness"], quality["freshness"])
    badge_text = freshness if quality["status"] == "OK" else quality["status"]
    badge_tone = _freshness_tone(quality["freshness"]) if quality["status"] == "OK" else "negative"
    signals = "".join(_render_signal(signal) for signal in agent["signals"])
    if not signals:
        signals = '<div class="gctx-empty">Données indisponibles</div>'
    guide = agent["guide"]
    return f"""
<article class="gctx-agent-card">
  <div class="gctx-agent-head">
    <div>
      <div class="gctx-agent-id">{_esc(agent_id)}</div>
      <h3>{_esc(agent['title'])}</h3>
      <div class="gctx-agent-eyebrow">{_esc(agent['eyebrow'])}</div>
    </div>
    <span class="gctx-badge {badge_tone}">{_esc(badge_text)}</span>
  </div>
  <p class="gctx-agent-summary">{_esc(agent['description'])}</p>
  <div class="gctx-explainer">
    <div><strong>Ce que mesure cette tuile</strong><span>{_esc(guide['measure'])}</span></div>
    <div><strong>Comment lire vert / rouge</strong><span>{_esc(guide['colors'])}</span></div>
    <div class="impact"><strong>Influence possible sur AG1</strong><span>{_esc(guide['ag1_effect'])}</span></div>
  </div>
  <div class="gctx-signals-title">Signaux clés · {len(agent['signals'])} affichés sur {len(agent['entries'])} devises suivies</div>
  <div class="gctx-signals">{signals}</div>
  <div class="gctx-quality">
    <span>Couverture <strong>{_format_pct(quality['coverage'])}</strong></span>
    <span>Confiance <strong>{_format_pct(quality['confidence'])}</strong></span>
    <span>Âge <strong>{_esc(_format_age(quality['age_hours']))}</strong></span>
  </div>
</article>
"""


def _matrix_score_cell(value: Any, *, crowded: bool = False) -> str:
    if _safe_float(value) is None:
        return '<div class="gctx-matrix-cell missing" title="Pas de donnée fiable publiée par cet agent"><strong>Non couvert</strong></div>'
    tone = _tone_for_score(value)
    flag = '<span class="gctx-flag">extrême</span>' if crowded else ""
    return f'<div class="gctx-matrix-cell {tone}"><strong>{_format_score(value)}</strong>{flag}</div>'


def _matrix_risk_cell(value: Any, policy: str) -> str:
    if _safe_float(value) is None:
        return '<div class="gctx-matrix-cell missing" title="Pas de donnée fiable publiée par cet agent"><strong>Non couvert</strong></div>'
    tone = _tone_for_risk(value)
    policy_label = POLICY_LABELS.get(policy.lower(), policy or "—")
    return (
        f'<div class="gctx-matrix-cell {tone}"><strong>{_format_pct(value)}</strong>'
        f'<span>{_esc(policy_label)}</span></div>'
    )


def _render_matrix(rows: list[dict]) -> str:
    rendered = []
    for row in rows:
        ag1_chip = '<span class="gctx-ag1-chip">Brief AG1</span>' if row.get("ag1_relevant") else ""
        row_class = " ag1-relevant" if row.get("ag1_relevant") else ""
        rendered.append(
            f'<div class="gctx-matrix-row{row_class}" role="row">'
            f'<div class="gctx-currency" role="rowheader"><strong>{_esc(row["currency"])}</strong>{ag1_chip}</div>'
            f'{_matrix_score_cell(row["ag5"])}'
            f'{_matrix_score_cell(row["ag6"])}'
            f'{_matrix_score_cell(row["ag7"], crowded=row["ag7_crowded"])}'
            f'{_matrix_risk_cell(row["ag8"], row["ag8_policy"])}'
            "</div>"
        )
    if not rendered:
        return '<div class="gctx-empty">Aucune devise commune disponible.</div>'
    return """
<div class="gctx-matrix" role="table" aria-label="Lecture croisée AG5 à AG8">
  <div class="gctx-matrix-row header" role="row">
    <div role="columnheader">Devise<span>dernier brief</span></div>
    <div role="columnheader"><strong>AG5</strong><span>Macro</span></div>
    <div role="columnheader"><strong>AG6</strong><span>Valorisation</span></div>
    <div role="columnheader"><strong>AG7</strong><span>Positionnement</span></div>
    <div role="columnheader"><strong>AG8</strong><span>Pression taux</span></div>
  </div>
  %s
</div>
""" % "".join(rendered)


PAGE_CSS = """
<style>
.gctx-hero, .gctx-agent-card, .gctx-matrix { font-family: inherit; }
.gctx-hero {
  display:grid; grid-template-columns:minmax(0,1.25fr) minmax(330px,.75fr); gap:28px;
  padding:24px 26px; margin:8px 0 26px; border:1px solid rgba(148,163,184,.2);
  border-radius:18px; background:linear-gradient(135deg,rgba(24,30,42,.98),rgba(13,18,27,.98));
  box-shadow:0 14px 34px rgba(0,0,0,.18); position:relative; overflow:hidden;
}
.gctx-hero:before { content:""; position:absolute; inset:0 auto 0 0; width:4px; background:#64748b; }
.gctx-hero.positive:before { background:#22c55e; }
.gctx-hero.warning:before { background:#f59e0b; }
.gctx-hero.negative:before { background:#ef4444; }
.gctx-kicker { color:#94a3b8; font-size:.71rem; letter-spacing:.13em; font-weight:800; margin-bottom:8px; }
.gctx-headline { color:#f8fafc; font-size:clamp(1.25rem,2vw,1.72rem); font-weight:760; line-height:1.2; display:flex; align-items:center; gap:10px; }
.gctx-status-dot { width:11px; height:11px; border-radius:50%; flex:none; background:#94a3b8; box-shadow:0 0 0 5px rgba(148,163,184,.12); }
.gctx-hero.positive .gctx-status-dot { background:#22c55e; box-shadow:0 0 0 5px rgba(34,197,94,.12); }
.gctx-hero.warning .gctx-status-dot { background:#f59e0b; box-shadow:0 0 0 5px rgba(245,158,11,.13); }
.gctx-hero.negative .gctx-status-dot { background:#ef4444; box-shadow:0 0 0 5px rgba(239,68,68,.13); }
.gctx-meta { color:#94a3b8; font-size:.8rem; margin-top:10px; }
.gctx-kpis { display:grid; grid-template-columns:1fr 1fr; gap:12px; align-content:center; }
.gctx-kpi { background:rgba(255,255,255,.035); border:1px solid rgba(148,163,184,.13); padding:12px 14px; border-radius:12px; }
.gctx-kpi > div:first-child { display:flex; align-items:center; justify-content:space-between; gap:8px; }
.gctx-kpi span { color:#94a3b8; font-size:.76rem; }
.gctx-kpi strong { color:#f8fafc; font-size:.95rem; }
.gctx-kpi.freshness { display:flex; align-items:center; justify-content:space-between; grid-column:1/-1; }
.gctx-progress { height:5px; background:rgba(148,163,184,.14); border-radius:99px; margin-top:9px; overflow:hidden; }
.gctx-progress-fill { display:block; height:100%; border-radius:inherit; background:#22c55e; }
.gctx-progress-fill.accent { background:#60a5fa; }
.gctx-guide-grid { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:12px; margin:8px 0 28px; }
.gctx-guide-card { display:flex; gap:12px; padding:15px; border:1px solid rgba(148,163,184,.16); border-radius:14px; background:rgba(20,25,34,.72); }
.gctx-guide-number { width:25px; height:25px; flex:none; display:flex; align-items:center; justify-content:center; border-radius:8px; background:rgba(96,165,250,.13); color:#93c5fd; font-size:.75rem; font-weight:850; }
.gctx-guide-card strong { color:#e2e8f0; font-size:.8rem; }
.gctx-guide-card p { color:#94a3b8; font-size:.72rem; line-height:1.45; margin:5px 0 0!important; }
.gctx-ag1-panel { padding:22px; margin:8px 0 28px; border:1px solid rgba(96,165,250,.28); border-radius:17px; background:linear-gradient(145deg,rgba(22,34,52,.94),rgba(13,18,27,.96)); }
.gctx-ag1-panel.missing { border-color:rgba(148,163,184,.18); }
.gctx-ag1-panel h3 { color:#f8fafc; font-size:1.22rem!important; margin:2px 0!important; }
.gctx-ag1-head { display:flex; align-items:flex-start; justify-content:space-between; gap:18px; }
.gctx-policy-explanation { color:#dbeafe; font-size:.86rem; line-height:1.45; margin:14px 0!important; }
.gctx-ag1-scope { display:grid; grid-template-columns:minmax(230px,.7fr) minmax(300px,1.3fr); gap:18px; padding:14px; border:1px solid rgba(148,163,184,.13); background:rgba(0,0,0,.12); border-radius:12px; }
.gctx-ag1-scope p { color:#94a3b8; font-size:.73rem; line-height:1.45; margin:0!important; }
.gctx-small-label { display:block; color:#7f8da1; font-size:.62rem; letter-spacing:.08em; font-weight:800; margin-bottom:7px; }
.gctx-chips { display:flex; flex-wrap:wrap; gap:6px; }
.gctx-currency-chip { display:inline-flex; padding:4px 8px; border-radius:7px; color:#dbeafe; background:rgba(59,130,246,.14); border:1px solid rgba(96,165,250,.24); font-size:.72rem; font-weight:800; }
.gctx-muted { color:#94a3b8; font-size:.75rem; }
.gctx-ag1-components { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:9px; margin-top:12px; }
.gctx-ag1-component { padding:10px; border:1px solid rgba(148,163,184,.13); border-radius:11px; background:rgba(255,255,255,.025); }
.gctx-ag1-component > div { display:flex; align-items:center; justify-content:space-between; gap:6px; }
.gctx-ag1-component > div > strong { color:#f8fafc; font-size:.78rem; }
.gctx-ag1-component p { color:#94a3b8; font-size:.64rem; line-height:1.35; margin:7px 0 0!important; }
.gctx-flow { display:grid; grid-template-columns:1fr auto 1fr auto 1fr auto 1fr; gap:8px; align-items:center; margin-top:15px; }
.gctx-flow > div { min-height:62px; padding:9px; display:flex; flex-direction:column; justify-content:center; border-radius:10px; background:rgba(148,163,184,.06); text-align:center; }
.gctx-flow strong { color:#e2e8f0; font-size:.72rem; } .gctx-flow span { color:#94a3b8; font-size:.62rem; line-height:1.3; margin-top:3px; }
.gctx-flow > b { color:#64748b; font-size:.8rem; }
.gctx-boundary { margin-top:14px; padding:11px 13px; border-left:3px solid #60a5fa; background:rgba(59,130,246,.07); color:#aebdd0; font-size:.7rem; line-height:1.45; }
.gctx-boundary strong { color:#dbeafe; }
.gctx-agent-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:16px; margin:8px 0 28px; }
.gctx-agent-card { background:linear-gradient(150deg,rgba(24,29,39,.92),rgba(13,17,24,.94)); border:1px solid rgba(148,163,184,.18); border-radius:16px; padding:19px; }
.gctx-agent-head { display:flex; justify-content:space-between; align-items:flex-start; gap:12px; }
.gctx-agent-id { color:#fb7185; font-size:.72rem; font-weight:850; letter-spacing:.11em; }
.gctx-agent-card h3 { margin:2px 0 0!important; padding:0!important; color:#f8fafc; font-size:1.12rem!important; }
.gctx-agent-eyebrow { color:#94a3b8; font-size:.74rem; margin-top:3px; }
.gctx-agent-summary { color:#dbe4ee; font-size:.86rem; line-height:1.45; min-height:2.5em; margin:16px 0 14px!important; }
.gctx-explainer { display:flex; flex-direction:column; gap:8px; margin-bottom:15px; }
.gctx-explainer > div { padding:9px 10px; border-radius:9px; background:rgba(148,163,184,.05); }
.gctx-explainer > div.impact { background:rgba(59,130,246,.075); border-left:2px solid rgba(96,165,250,.65); }
.gctx-explainer strong { display:block; color:#cbd5e1; font-size:.67rem; margin-bottom:3px; }
.gctx-explainer span { display:block; color:#94a3b8; font-size:.68rem; line-height:1.42; }
.gctx-signals-title { color:#7f8da1; font-size:.63rem; letter-spacing:.03em; margin:0 0 9px; }
.gctx-badge { display:inline-flex; align-items:center; white-space:nowrap; border-radius:999px; padding:4px 8px; font-size:.68rem!important; font-weight:760; border:1px solid rgba(148,163,184,.22); color:#cbd5e1!important; background:rgba(148,163,184,.08); }
.gctx-badge.positive { color:#86efac!important; border-color:rgba(34,197,94,.3); background:rgba(34,197,94,.1); }
.gctx-badge.warning { color:#fcd34d!important; border-color:rgba(245,158,11,.32); background:rgba(245,158,11,.1); }
.gctx-badge.negative { color:#fca5a5!important; border-color:rgba(239,68,68,.3); background:rgba(239,68,68,.1); }
.gctx-signals { display:flex; flex-direction:column; gap:10px; }
.gctx-signal-row { display:grid; grid-template-columns:82px minmax(70px,1fr) 48px; gap:10px; align-items:center; }
.gctx-signal-name { min-width:0; display:flex; flex-direction:column; }
.gctx-signal-name strong { color:#e2e8f0; font-size:.8rem; }
.gctx-signal-detail { color:#94a3b8; font-size:.62rem; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.gctx-score { text-align:right; font-size:.75rem; font-variant-numeric:tabular-nums; font-weight:750; color:#cbd5e1; }
.gctx-score.positive { color:#4ade80; } .gctx-score.negative { color:#fb7185; } .gctx-score.warning { color:#fbbf24; }
.gctx-mini-track { height:7px; background:rgba(148,163,184,.13); border-radius:99px; position:relative; overflow:hidden; }
.gctx-mini-track.signed i { position:absolute; left:50%; top:0; bottom:0; width:1px; background:rgba(226,232,240,.35); z-index:2; }
.gctx-mini-track span { position:absolute; top:0; bottom:0; min-width:2px; border-radius:99px; background:#64748b; }
.gctx-mini-track span.positive { background:#22c55e; } .gctx-mini-track span.negative { background:#f43f5e; } .gctx-mini-track span.warning { background:#f59e0b; } .gctx-mini-track span.neutral { background:#64748b; }
.gctx-quality { display:flex; flex-wrap:wrap; gap:7px 14px; border-top:1px solid rgba(148,163,184,.12); margin-top:16px; padding-top:12px; }
.gctx-quality span { color:#7f8da1; font-size:.67rem; } .gctx-quality strong { color:#cbd5e1; font-weight:700; }
.gctx-matrix { border:1px solid rgba(148,163,184,.18); border-radius:16px; overflow:hidden; margin:8px 0 14px; }
.gctx-matrix-row { display:grid; grid-template-columns:78px repeat(4,minmax(110px,1fr)); border-top:1px solid rgba(148,163,184,.1); }
.gctx-matrix-row:first-child { border-top:0; }
.gctx-matrix-row > div { min-height:50px; padding:9px 12px; border-left:1px solid rgba(148,163,184,.08); display:flex; align-items:center; justify-content:center; }
.gctx-matrix-row > div:first-child { border-left:0; }
.gctx-matrix-row.header { background:rgba(148,163,184,.07); }
.gctx-matrix-row.header > div { color:#94a3b8; font-size:.68rem; flex-direction:column; line-height:1.25; }
.gctx-matrix-row.header strong { color:#dbe4ee; font-size:.75rem; }
.gctx-currency { color:#f8fafc; font-weight:800; font-size:.83rem; background:rgba(148,163,184,.035); flex-direction:column; gap:3px; }
.gctx-currency > strong { color:#f8fafc; font-size:.83rem; }
.gctx-ag1-chip { color:#93c5fd; background:rgba(59,130,246,.13); border-radius:99px; padding:2px 5px; font-size:.5rem; line-height:1.1; white-space:nowrap; }
.gctx-matrix-row.ag1-relevant .gctx-currency { background:rgba(59,130,246,.09); }
.gctx-matrix-cell { gap:7px; background:rgba(100,116,139,.035); color:#cbd5e1; }
.gctx-matrix-cell strong { font-size:.8rem; font-variant-numeric:tabular-nums; }
.gctx-matrix-cell > span:not(.gctx-flag) { font-size:.61rem; color:#94a3b8; }
.gctx-matrix-cell.positive { background:rgba(34,197,94,.075); } .gctx-matrix-cell.positive strong { color:#4ade80; }
.gctx-matrix-cell.negative { background:rgba(244,63,94,.075); } .gctx-matrix-cell.negative strong { color:#fb7185; }
.gctx-matrix-cell.warning { background:rgba(245,158,11,.075); } .gctx-matrix-cell.warning strong { color:#fbbf24; }
.gctx-matrix-cell.missing { background:repeating-linear-gradient(135deg,rgba(100,116,139,.02),rgba(100,116,139,.02) 7px,rgba(100,116,139,.05) 7px,rgba(100,116,139,.05) 14px); }
.gctx-matrix-cell.missing strong { color:#64748b; font-size:.61rem; font-weight:650; }
.gctx-flag { color:#fbbf24; background:rgba(245,158,11,.12); border-radius:99px; padding:2px 5px; font-size:.55rem; text-transform:uppercase; letter-spacing:.04em; }
.gctx-legend { display:flex; flex-wrap:wrap; gap:9px 18px; margin:0 0 24px; color:#94a3b8; font-size:.7rem; }
.gctx-legend span { display:inline-flex; align-items:center; gap:6px; }
.gctx-legend i { width:7px; height:7px; border-radius:50%; background:#64748b; }
.gctx-legend .positive { background:#22c55e; } .gctx-legend .negative { background:#f43f5e; } .gctx-legend .warning { background:#f59e0b; }
.gctx-empty { color:#94a3b8; border:1px dashed rgba(148,163,184,.2); border-radius:10px; padding:16px; text-align:center; font-size:.8rem; }
@media (max-width:900px) {
  .gctx-hero { grid-template-columns:1fr; }
  .gctx-guide-grid { grid-template-columns:1fr; }
  .gctx-ag1-components { grid-template-columns:repeat(2,minmax(0,1fr)); }
  .gctx-ag1-scope { grid-template-columns:1fr; }
  .gctx-flow { grid-template-columns:1fr; }
  .gctx-flow > b { transform:rotate(90deg); text-align:center; }
  .gctx-agent-grid { grid-template-columns:1fr; }
  .gctx-matrix { overflow-x:auto; }
  .gctx-matrix-row { min-width:650px; }
}
@media (max-width:520px) {
  .gctx-hero { padding:20px; }
  .gctx-kpis { grid-template-columns:1fr; }
  .gctx-kpi.freshness { grid-column:auto; }
}
</style>
"""


def render_global_context_tab(
    st,
    *,
    global_path: str,
    world_path: str,
    macro_path: str,
    ag1_path: str | None = None,
) -> None:
    data = load_global_context_data(
        global_path,
        world_path,
        macro_path,
        ag1_path,
        include_diagnostics=False,
    )
    summary = build_market_context_summary(data)

    st.title("Contexte de marché")
    st.caption("Une lecture unique des quatre angles AG5–AG8 · scores canoniques inchangés · usage consultatif.")
    st.markdown(PAGE_CSS, unsafe_allow_html=True)

    if not summary["snapshot"]["id"]:
        st.warning("Aucun snapshot global publié. Les indicateurs disponibles ci-dessous proviennent des derniers composants persistés.")

    st.markdown(_render_hero(summary), unsafe_allow_html=True)

    st.subheader("Comment lire les indicateurs")
    st.markdown(_render_reading_guide(summary), unsafe_allow_html=True)

    st.subheader("Influence sur AG1 V4 Consensus")
    st.markdown(_render_ag1_brief(summary), unsafe_allow_html=True)

    st.subheader("Les quatre lectures du marché")
    cards = "".join(_render_agent_card(agent_id, agent) for agent_id, agent in summary["agents"].items())
    st.markdown(f'<section class="gctx-agent-grid">{cards}</section>', unsafe_allow_html=True)

    st.subheader("Lecture croisée des principales devises")
    st.caption("Toutes les devises publiées sont visibles. Le badge “Brief AG1” identifie celles réellement injectées lors du dernier run; “Non couvert” signifie qu'un agent n'a pas publié de signal fiable pour cette devise.")
    st.markdown(_render_matrix(summary["matrix"]), unsafe_allow_html=True)
    st.markdown(
        """
<div class="gctx-legend">
  <span><i class="positive"></i>Soutien / pression faible</span>
  <span><i></i>Neutre</span>
  <span><i class="negative"></i>Frein / pression forte</span>
  <span><i class="warning"></i>Vigilance</span>
</div>
""",
        unsafe_allow_html=True,
    )

    if summary["errors"]:
        failed = ", ".join(sorted(summary["errors"]))
        st.warning(f"Certaines sources de synthèse sont indisponibles : {failed}.")

    st.caption("Cette vue explique le contexte global; elle ne modifie ni les gates, ni les quantités, ni le consensus, ni le Risk Manager d'AG1.")
