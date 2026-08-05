"""Construction de la synthèse canonique AG5-AG9, sans logique d'ordre."""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import duckdb


FRESHNESS_RANK = {"fresh": 0, "aging": 1, "stale": 2, "missing": 3}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def payload_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def parse_time(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def json_value(value: Any) -> Any:
    if value is None or isinstance(value, (dict, list, int, float, bool)):
        return value
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return value


def query_rows(path: str, query: str, params: list | None = None, retries: int = 8) -> list[dict]:
    if not os.path.isfile(path):
        return []
    for attempt in range(retries):
        con = None
        try:
            con = duckdb.connect(path, read_only=True)
            cur = con.execute(query, params or [])
            columns = [description[0] for description in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception as exc:
            if attempt + 1 >= retries or not any(token in str(exc).lower() for token in ("lock", "busy", "conflict")):
                raise
            time.sleep(0.2 * (1.7 ** attempt))
        finally:
            if con is not None:
                con.close()
    return []


def load_config() -> dict:
    path = Path(__file__).resolve().parent / "config" / "context.json"
    config = json.loads(path.read_text(encoding="utf-8"))
    config["ag1_pack"]["max_chars"] = int(os.environ.get("AG1_GLOBAL_CONTEXT_MAX_CHARS", config["ag1_pack"]["max_chars"]))
    config["ag1_pack"]["top_events_max"] = int(os.environ.get("AG1_GLOBAL_CONTEXT_TOP_EVENTS_MAX", config["ag1_pack"]["top_events_max"]))
    config["ag1_pack"]["top_sectors_max"] = int(os.environ.get("AG1_GLOBAL_CONTEXT_TOP_SECTORS_MAX", config["ag1_pack"]["top_sectors_max"]))
    config["ag1_pack"]["top_assets_max"] = int(os.environ.get("AG1_GLOBAL_CONTEXT_TOP_ASSETS_MAX", config["ag1_pack"]["top_assets_max"]))
    global_max = os.environ.get("GLOBAL_CONTEXT_MAX_AGE_HOURS")
    if global_max:
        config["max_age_hours"] = {key: float(global_max) for key in config["max_age_hours"]}
        config["snapshot_max_age_hours"] = float(global_max)
    return config


def _latest_component_rows(macro_path: str, table: str) -> list[dict]:
    latest = query_rows(macro_path, f"SELECT component_snapshot_id FROM {table} ORDER BY calculation_time DESC, component_snapshot_id DESC LIMIT 1")
    if not latest:
        return []
    return query_rows(macro_path, f"SELECT * FROM {table} WHERE component_snapshot_id=? ORDER BY 2", [latest[0]["component_snapshot_id"]])


def _worst(values: list[str]) -> str:
    return max(values, key=lambda value: FRESHNESS_RANK.get(value, 3)) if values else "missing"


def _component_status(name: str, rows: list[dict], config: dict, now: datetime) -> dict:
    if not rows:
        return {"component": name, "component_snapshot_id": None, "component_as_of": None, "age_hours": None, "status": "MISSING", "coverage_ratio": 0.0, "confidence": 0.0, "freshness_status": "missing", "schema_version": None, "method_version": None, "row_count": 0, "warnings": [f"{name}_UNAVAILABLE"]}
    snapshot_ids = sorted({str(row.get("component_snapshot_id") or row.get("snapshot_id") or "") for row in rows})
    as_of_values = [parse_time(row.get("calculation_time") or row.get("created_at") or row.get("as_of")) for row in rows]
    as_of_values = [value for value in as_of_values if value]
    component_as_of = max(as_of_values) if as_of_values else None
    age = ((now - component_as_of).total_seconds() / 3600.0) if component_as_of else None
    freshness = _worst([str(row.get("freshness_status") or "missing") for row in rows])
    max_age = float(config["max_age_hours"][name])
    stale_by_age = age is None or age > max_age
    if stale_by_age:
        freshness = "stale" if age is not None else "missing"
    coverage_values = [float(row.get("coverage_ratio") or 0.0) for row in rows]
    confidence_values = [float(row.get("confidence") or 0.0) for row in rows]
    warnings = []
    if len(snapshot_ids) != 1:
        warnings.append(f"{name}_MIXED_SNAPSHOTS")
    if stale_by_age:
        warnings.append(f"{name}_STALE")
    status = "OK" if not warnings and freshness in {"fresh", "aging"} else "DEGRADED"
    return {
        "component": name, "component_snapshot_id": snapshot_ids[0] if len(snapshot_ids) == 1 else snapshot_ids,
        "component_as_of": component_as_of.isoformat() if component_as_of else None, "age_hours": age,
        "status": status, "coverage_ratio": sum(coverage_values) / len(coverage_values),
        "confidence": sum(confidence_values) / len(confidence_values), "freshness_status": freshness,
        "schema_version": rows[0].get("schema_version"), "method_version": rows[0].get("method_version"),
        "row_count": len(rows), "warnings": warnings,
    }


def _compact_component(row: dict, fields: tuple[str, ...]) -> dict:
    output = {field: json_value(row.get(field)) for field in fields if field in row}
    return {key: value for key, value in output.items() if value is not None}


def _pack_regime(status: str, by_currency: dict, fields: tuple[str, ...], **extra) -> dict:
    """Réduit un régime pour le prompt; le détail complet reste en DuckDB."""

    compact = {
        currency: {key: value for key, value in row.items() if key in fields and value is not None}
        for currency, row in sorted(by_currency.items())
    }
    return {"status": status, "by_currency": compact, **extra}


def _limit_pack(pack: dict, max_chars: int) -> dict:
    if len(canonical_json(pack)) <= max_chars:
        return pack
    compact = json.loads(canonical_json(pack))
    for event in compact.get("critical_events", []):
        if isinstance(event, dict):
            event.pop("summary", None)
            event.pop("lineage", None)
    for key in ("country_overlays", "sector_overlays", "portfolio_exposure_review", "opportunity_exposure_review"):
        values = compact.get(key)
        if isinstance(values, list):
            compact[key] = values[: max(1, len(values) // 2)]
    compact.setdefault("source_warnings", []).append("AG1_GLOBAL_CONTEXT_PACK_TRUNCATED")
    if len(canonical_json(compact)) > max_chars:
        compact["critical_events"] = compact.get("critical_events", [])[:2]
        compact["macro_regime"] = {"status": compact.get("macro_regime", {}).get("status"), "warning": "details_truncated"}
        compact["fx_relative_valuation"] = {"status": compact.get("fx_relative_valuation", {}).get("status"), "warning": "details_truncated"}
    if len(canonical_json(compact)) > max_chars:
        raise ValueError("AG1_GLOBAL_CONTEXT_MAX_CHARS_TOO_SMALL")
    return compact


def synthesize(macro_path: str, world_path: str, *, now: Optional[datetime] = None) -> dict:
    now = now or utcnow()
    config = load_config()
    ag5 = _latest_component_rows(macro_path, "components.ag5_macro")
    ag6 = _latest_component_rows(macro_path, "components.ag6_fx_valuation")
    ag7 = _latest_component_rows(macro_path, "components.ag7_positioning")
    ag8 = _latest_component_rows(macro_path, "components.ag8_rates_liquidity")
    ag9_snapshot_rows = query_rows(world_path, "SELECT * FROM main.v_latest_ag9_global_risk")
    ag9 = ag9_snapshot_rows
    components = {"AG5": ag5, "AG6": ag6, "AG7": ag7, "AG8": ag8, "AG9": ag9}
    statuses = [_component_status(name, rows, config, now) for name, rows in components.items()]
    if not any(row["row_count"] > 0 for row in statuses):
        raise RuntimeError("GLOBAL_CONTEXT_ZERO_AVAILABLE_COMPONENTS")

    snapshot_id = f"GC_{now.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    ag9_payload = json_value(ag9[0].get("payload_json")) if ag9 else {}
    ag9_events = query_rows(world_path, "SELECT * FROM main.v_latest_events") if ag9 else []
    ag9_countries = query_rows(world_path, "SELECT * FROM main.v_latest_country_risk") if ag9 else []
    ag9_sectors = query_rows(world_path, "SELECT * FROM main.v_latest_sector_impacts") if ag9 else []
    ag9_assets = query_rows(world_path, "SELECT * FROM core.asset_impacts WHERE snapshot_id=?", [ag9_payload.get("snapshot_id")]) if ag9 else []

    macro_by = {str(row.get("entity_id")): _compact_component(row, ("macro_score", "subscores_json", "coverage_ratio", "confidence", "freshness_status", "missing_inputs_json", "stale_inputs_json")) for row in ag5}
    valuation_by = {str(row.get("currency")): _compact_component(row, ("valuation_score", "carry_score", "real_carry_score", "ppp_gap", "reer_gap", "terms_of_trade_score", "spot_reference", "coverage_ratio", "confidence", "freshness_status", "missing_inputs_json", "stale_inputs_json", "proxy_inputs_json", "input_status_json")) for row in ag6}
    positioning_by = {str(row.get("entity_id")): _compact_component(row, ("report_date", "z_score", "positioning_score", "crowded_flag", "crowded_direction", "is_proxy", "confidence", "freshness_status", "source")) for row in ag7}
    rates_by = {str(row.get("currency")): _compact_component(row, ("policy_regime", "curve_regime", "yield_2y", "yield_10y", "slope_10y2y", "slope_change", "real_rate", "duration_pressure", "liquidity_score", "overlays_json", "confidence", "freshness_status")) for row in ag8}
    event_currency = ag9_payload.get("currency_overlays") or {}
    currencies = sorted(set(macro_by) | set(valuation_by) | set(positioning_by) | set(rates_by) | set(event_currency))
    currency_context = []
    for currency in currencies:
        pieces = [part for part in (macro_by.get(currency), valuation_by.get(currency), positioning_by.get(currency), rates_by.get(currency), event_currency.get(currency)) if part]
        confidences = [float(part.get("confidence") or 0.0) for part in pieces]
        fresh = _worst([str(part.get("freshness_status") or "missing") for part in pieces])
        currency_context.append({"currency": currency, "macro": macro_by.get(currency), "valuation": valuation_by.get(currency), "positioning": positioning_by.get(currency), "rates": rates_by.get(currency), "event_risk": event_currency.get(currency), "confidence": sum(confidences) / len(confidences) if confidences else 0.0, "freshness_status": fresh})

    source_warnings = [warning for status in statuses for warning in status["warnings"]]
    global_regime = {
        "macro_regime": {"status": next(row["status"] for row in statuses if row["component"] == "AG5"), "by_currency": macro_by},
        "rates_liquidity_regime": {"status": next(row["status"] for row in statuses if row["component"] == "AG8"), "by_currency": rates_by},
        "positioning_regime": {"status": next(row["status"] for row in statuses if row["component"] == "AG7"), "by_currency": positioning_by},
        "fx_relative_valuation": {"status": next(row["status"] for row in statuses if row["component"] == "AG6"), "scope": "FX_RELATIVE_VALUATION_ONLY", "by_currency": valuation_by},
        "geopolitical_risk_regime": {"status": next(row["status"] for row in statuses if row["component"] == "AG9"), "global_risk_regime": ag9_payload.get("global_risk_regime", "unknown"), "global_risk_score": ag9_payload.get("global_risk_score"), "confidence": ag9_payload.get("confidence")},
        "source_warnings": source_warnings,
    }
    limits = config["ag1_pack"]
    critical = [{"event_id": row.get("event_id"), "event_type": row.get("event_type"), "title": row.get("title"), "summary": row.get("summary"), "event_time": str(row.get("event_time") or ""), "effective_score": row.get("effective_score"), "confidence": row.get("confidence"), "countries": json_value(row.get("countries_json")), "sectors": json_value(row.get("sectors_json")), "commodities": json_value(row.get("commodities_json")), "currencies": json_value(row.get("currencies_json")), "lineage": json_value(row.get("lineage_json"))} for row in ag9_events if row.get("effective_score") is not None][: limits["top_events_max"]]
    country_context = [{"country": row.get("entity_id"), "risk_score": row.get("risk_score"), "confidence": row.get("confidence"), "freshness_status": row.get("freshness_status"), "contributors": json_value(row.get("contributors_json")), "payload": json_value(row.get("payload_json"))} for row in ag9_countries]
    sector_context = [{"sector": row.get("sector"), "risk_score": row.get("impact_score"), "confidence": row.get("confidence"), "freshness_status": "fresh", "contributors": json_value(row.get("contributors_json")), "payload": json_value(row.get("payload_json"))} for row in ag9_sectors]
    asset_context = [{"asset_id": row.get("asset_id"), "risk_score": row.get("impact_score"), "confidence": row.get("confidence"), "freshness_status": "fresh", "exposure_known": bool(row.get("exposure_known")), "contributors": json_value(row.get("contributors_json")), "limitations": json_value(row.get("limitations_json")), "payload": json_value(row.get("payload_json"))} for row in ag9_assets]

    available_weight = sum(config["component_weights"][row["component"]] for row in statuses if row["row_count"] > 0)
    coverage = sum(config["component_weights"][row["component"]] * float(row["coverage_ratio"] or 0.0) for row in statuses)
    confidence = sum(config["component_weights"][row["component"]] * float(row["confidence"] or 0.0) for row in statuses) / available_weight if available_weight else 0.0
    overall_freshness = _worst([row["freshness_status"] for row in statuses if row["row_count"] > 0])
    overall_status = "OK" if all(row["status"] == "OK" for row in statuses) else "DEGRADED"
    pack = {
        "schema_version": "AG1_GLOBAL_CONTEXT_PACK_V1", "snapshot_id": snapshot_id,
        "method_version": config["method_version"],
        "as_of": now.isoformat(), "freshness_status": overall_freshness,
        "coverage_ratio": coverage, "confidence": confidence, "status": overall_status,
        "advisory_only": True,
        "source_domains": {"news_sentiment": "AG4_NEWS_SENTIMENT", "structured_global_risk": "AG9_GLOBAL_RISK"},
        "macro_regime": _pack_regime(global_regime["macro_regime"]["status"], macro_by, ("macro_score", "coverage_ratio", "confidence", "freshness_status")),
        "rates_liquidity_regime": _pack_regime(global_regime["rates_liquidity_regime"]["status"], rates_by, ("policy_regime", "curve_regime", "slope_10y2y", "real_rate", "duration_pressure", "liquidity_score", "confidence", "freshness_status")),
        "positioning_regime": _pack_regime(global_regime["positioning_regime"]["status"], positioning_by, ("report_date", "z_score", "positioning_score", "crowded_flag", "crowded_direction", "is_proxy", "confidence", "freshness_status")),
        "fx_relative_valuation": _pack_regime(global_regime["fx_relative_valuation"]["status"], valuation_by, ("valuation_score", "carry_score", "real_carry_score", "ppp_gap", "reer_gap", "terms_of_trade_score", "confidence", "freshness_status"), scope="FX_RELATIVE_VALUATION_ONLY"),
        "geopolitical_risk_regime": global_regime["geopolitical_risk_regime"],
        "portfolio_exposure_review": [], "opportunity_exposure_review": [],
        "sector_overlays": sorted(sector_context, key=lambda row: float(row.get("risk_score") or 0), reverse=True)[: limits["top_sectors_max"]],
        "country_overlays": sorted(country_context, key=lambda row: float(row.get("risk_score") or 0), reverse=True)[: limits["top_countries_max"]],
        "critical_events": critical, "source_warnings": source_warnings,
    }
    pack = _limit_pack(pack, max(1024, limits["max_chars"] - 100))
    pack["payload_hash"] = payload_hash(pack)
    component_ids = {row["component"]: row["component_snapshot_id"] for row in statuses}
    component_as_of = {row["component"]: row["component_as_of"] for row in statuses}
    component_ages = {row["component"]: row["age_hours"] for row in statuses}
    snapshot = {
        "snapshot_id": snapshot_id, "schema_version": config["schema_version"], "as_of": now.isoformat(), "created_at": now.isoformat(),
        "status": overall_status, "component_snapshot_ids": component_ids, "component_as_of": component_as_of,
        "component_ages": component_ages, "coverage_ratio": coverage, "confidence": confidence,
        "freshness_status": overall_freshness, "method_version": config["method_version"], "ag1_pack": pack,
        "global_regime": global_regime,
    }
    snapshot["payload_hash"] = payload_hash(snapshot)
    source_lineage = [{"source_id": row["component"], "component": row["component"], "source_snapshot_id": row["component_snapshot_id"], "source_as_of": row["component_as_of"], "schema_version": row["schema_version"], "method_version": row["method_version"], "payload_hash": None, "detail": {"age_hours": row["age_hours"], "coverage_ratio": row["coverage_ratio"], "freshness_status": row["freshness_status"]}} for row in statuses]
    return {"snapshot": snapshot, "component_status": statuses, "global_regime": global_regime, "country_context": country_context, "currency_context": currency_context, "sector_context": sector_context, "asset_context": asset_context, "critical_events": critical, "source_lineage": source_lineage}


def advisory_pack_for_run(
    base_pack: Optional[dict],
    exposure_context: dict,
    portfolio: list[dict],
    opportunities: list[dict],
    *,
    now: Optional[datetime] = None,
) -> dict:
    """Filtre les overlays canoniques pour le portefeuille/candidats sans rescoring."""

    now = now or utcnow()
    config = load_config()
    if not base_pack:
        pack = {
            "schema_version": "AG1_GLOBAL_CONTEXT_PACK_V1", "snapshot_id": None,
            "method_version": "GLOBAL_CONTEXT_SYNTHESIS_V1",
            "as_of": None, "freshness_status": "missing", "coverage_ratio": None,
            "confidence": None, "status": "GLOBAL_CONTEXT_UNAVAILABLE", "advisory_only": True,
            "macro_regime": {}, "rates_liquidity_regime": {}, "positioning_regime": {},
            "fx_relative_valuation": {"scope": "FX_RELATIVE_VALUATION_ONLY"},
            "geopolitical_risk_regime": {}, "portfolio_exposure_review": [],
            "opportunity_exposure_review": [], "sector_overlays": [], "country_overlays": [],
            "critical_events": [], "source_warnings": ["GLOBAL_CONTEXT_UNAVAILABLE"],
        }
        pack["payload_hash"] = payload_hash(pack)
        return pack

    pack = json.loads(canonical_json(base_pack))
    pack["advisory_only"] = True
    as_of = parse_time(pack.get("as_of"))
    age_hours = (now - as_of).total_seconds() / 3600.0 if as_of else None
    max_age = float(config["snapshot_max_age_hours"])
    pack["context_age_hours"] = age_hours
    if age_hours is None or age_hours > max_age:
        pack["status"] = "GLOBAL_CONTEXT_STALE"
        pack["freshness_status"] = "stale" if age_hours is not None else "missing"
        warnings = list(pack.get("source_warnings") or [])
        if "GLOBAL_CONTEXT_STALE" not in warnings:
            warnings.append("GLOBAL_CONTEXT_STALE")
        pack["source_warnings"] = warnings

    def decode(value: Any) -> Any:
        return json_value(value)

    sectors = {str(row.get("sector") or "").strip().upper(): row for row in exposure_context.get("sectors", [])}
    countries = {str(row.get("country") or "").strip().upper(): row for row in exposure_context.get("countries", [])}
    assets = {str(row.get("asset_id") or "").strip().upper(): row for row in exposure_context.get("assets", [])}

    def review(row: dict) -> dict:
        symbol = str(row.get("symbol") or row.get("Symbol") or row.get("symbol_internal") or "").strip().upper()
        sector = str(row.get("sector") or row.get("Sector") or "").strip()
        country = str(row.get("country") or row.get("Country") or row.get("exchange_country") or "").strip()
        currency = str(row.get("currency") or row.get("Currency") or "").strip().upper()
        matches = []
        if symbol in assets:
            matches.append({"type": "asset", "entity": symbol, "risk_score": assets[symbol].get("risk_score"), "confidence": assets[symbol].get("confidence"), "contributors": decode(assets[symbol].get("contributors_json"))})
        if sector.upper() in sectors:
            matches.append({"type": "sector", "entity": sector, "risk_score": sectors[sector.upper()].get("risk_score"), "confidence": sectors[sector.upper()].get("confidence"), "contributors": decode(sectors[sector.upper()].get("contributors_json"))})
        if country.upper() in countries:
            matches.append({"type": "country", "entity": country, "risk_score": countries[country.upper()].get("risk_score"), "confidence": countries[country.upper()].get("confidence"), "contributors": decode(countries[country.upper()].get("contributors_json"))})
        return {"symbol": symbol, "sector": sector or None, "country": country or None, "currency": currency or None, "overlays": matches, "exposure_known": bool(matches), "limitation": None if matches else "NO_RELIABLE_EXPOSURE_MAPPING"}

    limits = config["ag1_pack"]
    pack["portfolio_exposure_review"] = [review(row) for row in portfolio if isinstance(row, dict)][: limits["top_assets_max"]]
    pack["opportunity_exposure_review"] = [review(row) for row in opportunities if isinstance(row, dict)][: limits["top_assets_max"]]
    pack.pop("payload_hash", None)
    pack = _limit_pack(pack, max(1024, limits["max_chars"] - 100))
    pack["payload_hash"] = payload_hash(pack)
    return pack
