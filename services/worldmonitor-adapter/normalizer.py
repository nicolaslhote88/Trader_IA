"""Normalisation, scoring, decay et déduplication déterministes d'AG9."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from client import canonical_hash


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_time(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        try:
            seconds = float(value) / 1000.0 if float(value) > 10_000_000_000 else float(value)
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if len(text) == 10 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        text += "T00:00:00+00:00"
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _config(name: str) -> dict:
    path = Path(__file__).resolve().parent / "config" / name
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char))
    return " ".join(re.sub(r"[^a-zA-Z0-9]+", " ", text).lower().split())


def normalize_url(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parts = urlsplit(text)
        query = [(key, val) for key, val in parse_qsl(parts.query, keep_blank_values=False) if not key.lower().startswith("utm_") and key.lower() not in {"ref", "source"}]
        return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path.rstrip("/"), urlencode(sorted(query)), ""))
    except Exception:
        return text


def event_fingerprint(title: Any, event_time: Any, countries: Iterable[str], url: Any = None) -> str:
    observed = parse_time(event_time)
    day = observed.date().isoformat() if observed else "unknown-date"
    normalized_url = normalize_url(url)
    identity = normalized_url or normalize_text(title)
    # L'identité article (URL ou titre exact normalisé + jour) est volontairement
    # indépendante des enrichissements d'entités afin de rapprocher AG4 et AG9.
    raw = "|".join([identity, day])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def unwrap_payload(payload: Any) -> Any:
    if isinstance(payload, dict) and "content" in payload and isinstance(payload["content"], list):
        decoded = []
        for block in payload["content"]:
            if not isinstance(block, dict):
                continue
            text = block.get("text")
            if text is None:
                continue
            try:
                decoded.append(json.loads(text))
            except (TypeError, json.JSONDecodeError):
                decoded.append({"text": str(text)})
        if len(decoded) == 1:
            return decoded[0]
        if decoded:
            return decoded
    return payload


COLLECTION_KEYS = (
    "events", "items", "results", "data", "conflicts", "sanctions", "threats",
    "disasters", "anomalies", "signals", "countries", "chokepoints", "incidents",
    "articles", "records", "alerts",
)


def extract_records(payload: Any) -> list[dict]:
    payload = unwrap_payload(payload)
    if isinstance(payload, list):
        records = []
        for item in payload:
            if isinstance(item, dict):
                records.append(item)
            elif isinstance(item, list):
                records.extend(row for row in item if isinstance(row, dict))
        return records
    if not isinstance(payload, dict):
        return []
    for key in COLLECTION_KEYS:
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
        if isinstance(value, dict):
            nested = extract_records(value)
            if nested:
                return nested
    # Un objet métier unique est accepté, une enveloppe technique vide non.
    business_keys = {"title", "name", "country", "countryCode", "severity", "risk", "score", "eventType"}
    return [payload] if business_keys.intersection(payload) else []


def _list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw = value
    elif isinstance(value, dict):
        raw = list(value.keys())
    else:
        raw = re.split(r"[,;|]", str(value))
    result = []
    for item in raw:
        if isinstance(item, dict):
            item = item.get("name") or item.get("code") or item.get("id")
        text = str(item or "").strip()
        if text and text not in result:
            result.append(text)
    return result


def _first(row: dict, keys: Iterable[str]) -> Any:
    for key in keys:
        if row.get(key) not in (None, ""):
            return row[key]
    return None


def normalize_score(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    labels = {
        "minimal": 0.10, "low": 0.25, "guarded": 0.35, "moderate": 0.45,
        "medium": 0.50, "elevated": 0.60, "high": 0.75, "severe": 0.90,
        "critical": 1.0, "extreme": 1.0,
    }
    text = str(value).strip().lower()
    if text in labels:
        return labels[text]
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    if number > 1.0:
        number = number / 100.0 if number <= 100 else 1.0
    return max(0.0, min(1.0, number))


def freshness_decay(event_time: Any, half_life_hours: float, *, now: Optional[datetime] = None) -> Optional[float]:
    observed = parse_time(event_time)
    if observed is None or half_life_hours <= 0:
        return None
    current = now or utcnow()
    age_hours = max(0.0, (current - observed).total_seconds() / 3600.0)
    return math.exp(-math.log(2.0) * age_hours / half_life_hours)


def bounded_aggregate(values: Iterable[Any]) -> Optional[float]:
    valid = []
    for value in values:
        score = normalize_score(value)
        if score is not None:
            valid.append(score)
    if not valid:
        return None
    product = 1.0
    for score in valid:
        product *= 1.0 - score
    return max(0.0, min(1.0, 1.0 - product))


def _half_life(event_type: str, decay_config: dict) -> float:
    env_default = os.environ.get("AG9_DEFAULT_EVENT_HALF_LIFE_HOURS")
    default = float(env_default) if env_default else float(decay_config["default_half_life_hours"])
    return float(decay_config["event_half_life_hours"].get(event_type, default))


def normalize_record(
    record: dict,
    *,
    domain: str,
    tool_name: str,
    request_id: str,
    now: datetime,
    ag4_fingerprints: Optional[set[str]] = None,
) -> dict:
    decay_config = _config("event_decay.json")
    mappings = _config("entity_mappings.json")
    title = str(_first(record, ("title", "headline", "name", "event", "description")) or "").strip()
    countries = _list(_first(record, ("countries", "country", "countryCode", "country_code", "iso3")))
    if not title:
        entity = countries[0] if countries else str(_first(record, ("id", "code", "location")) or "unknown")
        title = f"{domain.replace('_', ' ').title()}: {entity}"
    summary = str(_first(record, ("summary", "description", "details", "text")) or "").strip()
    event_time_raw = _first(record, ("event_time", "eventTime", "timestamp", "published_at", "publishedAt", "date", "updated_at", "updatedAt", "lastSeen"))
    event_time = parse_time(event_time_raw)
    first_seen = parse_time(_first(record, ("first_seen", "firstSeen"))) or event_time
    last_seen = parse_time(_first(record, ("last_seen", "lastSeen", "updated_at", "updatedAt"))) or event_time
    severity_raw = _first(record, ("severity_normalized", "severity", "risk_score", "riskScore", "score", "level"))
    severity = normalize_score(severity_raw)
    confidence = normalize_score(_first(record, ("confidence", "confidence_score", "confidenceScore", "reliability")))
    if confidence is None:
        confidence = 0.50
    sources = _list(_first(record, ("sources", "source_names", "sourceNames", "providers", "provider", "source")))
    source_count_raw = _first(record, ("source_count", "sourceCount"))
    try:
        source_count = max(len(sources), int(source_count_raw or 0), 1)
    except (TypeError, ValueError):
        source_count = max(len(sources), 1)
    diversity_count = max(len(set(normalize_text(source) for source in sources if source)), 1)
    minimum_diversity = int(os.environ.get("AG9_SOURCE_DIVERSITY_MIN", decay_config["source_diversity_min"]))
    diversity_factor = min(1.0, diversity_count / max(1, minimum_diversity))
    half_life = _half_life(domain, decay_config)
    decay = freshness_decay(event_time, half_life, now=now)
    relevance = normalize_score(_first(record, ("relevance", "relevance_factor", "relevanceScore")))
    if relevance is None:
        relevance = 1.0
    effective = None
    if severity is not None and decay is not None:
        effective = max(0.0, min(1.0, severity * confidence * diversity_factor * decay * relevance))
    url = _first(record, ("url", "link", "source_url", "sourceUrl"))
    fingerprint = event_fingerprint(title, event_time, countries, url)
    country_mapping = mappings["country_to_currency"]
    currencies = _list(_first(record, ("currencies", "currency")))
    for country in countries:
        for currency in country_mapping.get(str(country).upper(), []):
            if currency not in currencies:
                currencies.append(currency)
    commodities = _list(_first(record, ("commodities", "commodity")))
    sectors = _list(_first(record, ("sectors", "sector", "industries")))
    for commodity in commodities:
        for sector in mappings["commodity_to_sectors"].get(str(commodity).upper(), []):
            if sector not in sectors:
                sectors.append(sector)
    chokepoints = _list(_first(record, ("chokepoints", "chokepoint")))
    raw_record_hash = canonical_hash(record)
    derived_from = [{"source": "WORLD_MONITOR", "tool_name": tool_name, "request_id": request_id, "source_url": normalize_url(url), "raw_record_hash": raw_record_hash}]
    ag4_duplicate = fingerprint in (ag4_fingerprints or set())
    if ag4_duplicate:
        derived_from.append({"source": "AG4_NEWS_SENTIMENT", "relationship": "same_normalized_article_or_event"})
    return {
        "event_id": f"EVT_{fingerprint[:20]}", "event_fingerprint": fingerprint, "event_type": domain,
        "title": title, "summary": summary, "countries": countries,
        "regions": _list(_first(record, ("regions", "region"))),
        "coordinates": _first(record, ("coordinates", "location")) if isinstance(_first(record, ("coordinates", "location")), (dict, list)) else None,
        "sectors": sectors, "commodities": commodities, "currencies": currencies, "chokepoints": chokepoints,
        "severity_raw": severity_raw, "severity_normalized": severity, "confidence": confidence,
        "source_count": source_count, "source_diversity": diversity_factor,
        "event_time": event_time.isoformat() if event_time else None,
        "first_seen": first_seen.isoformat() if first_seen else None, "last_seen": last_seen.isoformat() if last_seen else None,
        "ingestion_time": now.isoformat(), "half_life_hours": half_life, "freshness_decay": decay,
        "relevance_factor": relevance, "effective_score": effective,
        "is_correlated_signal": domain == "convergence", "is_llm_generated": bool(record.get("is_llm_generated") or record.get("llmGenerated")),
        "ag4_duplicate": ag4_duplicate, "derived_from": derived_from,
        "lineage": {"tool_name": tool_name, "request_id": request_id, "raw_record_hash": raw_record_hash, "decay_config_version": decay_config["config_version"], "mapping_config_version": mappings["config_version"]},
    }


def deduplicate_events(events: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    for event in events:
        key = event["event_fingerprint"]
        if key not in merged:
            merged[key] = dict(event)
            continue
        current = merged[key]
        current["severity_normalized"] = max(value for value in (current.get("severity_normalized"), event.get("severity_normalized")) if value is not None) if any(value is not None for value in (current.get("severity_normalized"), event.get("severity_normalized"))) else None
        current["confidence"] = bounded_aggregate([current.get("confidence"), event.get("confidence")])
        current["source_count"] = max(current.get("source_count") or 0, event.get("source_count") or 0)
        current["source_diversity"] = max(current.get("source_diversity") or 0, event.get("source_diversity") or 0)
        current["derived_from"] = current.get("derived_from", []) + [row for row in event.get("derived_from", []) if row not in current.get("derived_from", [])]
        current["ag4_duplicate"] = bool(current.get("ag4_duplicate") or event.get("ag4_duplicate"))
        current["is_correlated_signal"] = bool(current.get("is_correlated_signal") or event.get("is_correlated_signal"))
        if current.get("severity_normalized") is not None and current.get("freshness_decay") is not None:
            current["effective_score"] = min(1.0, current["severity_normalized"] * current["confidence"] * current["source_diversity"] * current["freshness_decay"] * current.get("relevance_factor", 1.0))
        for field in ("countries", "regions", "sectors", "commodities", "currencies", "chokepoints"):
            current[field] = sorted(set(current.get(field, [])) | set(event.get(field, [])))
    return sorted(merged.values(), key=lambda event: (event.get("effective_score") is not None, event.get("effective_score") or 0.0, event.get("event_time") or ""), reverse=True)


def overlay_rows(snapshot_id: str, events: list[dict], field: str, key_name: str) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    for event in events:
        for entity in event.get(field) or []:
            grouped.setdefault(str(entity), []).append(event)
    rows = []
    for entity, contributors in sorted(grouped.items()):
        score = bounded_aggregate(event.get("effective_score") for event in contributors)
        confidence_values = [event.get("confidence") for event in contributors if event.get("confidence") is not None]
        confidence = (sum(confidence_values) / len(confidence_values)) if confidence_values else 0.0
        rows.append({
            "snapshot_id": snapshot_id, key_name: entity, "entity_id": entity,
            "risk_score": score, "impact_score": score, "confidence": confidence,
            "freshness_status": "fresh" if any((event.get("freshness_decay") or 0) >= 0.5 for event in contributors) else "stale",
            "contributors": [event["event_id"] for event in contributors],
            "payload": {"event_count": len(contributors), "aggregation": "1-product(1-score)"},
        })
    return rows


def regime_for_score(score: Optional[float], thresholds: dict) -> str:
    if score is None:
        return "unknown"
    for label in ("low", "normal", "elevated", "high", "critical"):
        if score <= float(thresholds[label]):
            return label
    return "critical"


def build_ag9_snapshot(
    *,
    snapshot_id: str,
    events: list[dict],
    source_health: list[dict],
    now: datetime,
) -> dict:
    config = _config("event_decay.json")
    minimum_confidence = float(os.environ.get("AG9_MIN_CONFIDENCE", config["minimum_confidence"]))
    # Un article déjà consommé par AG4 reste auditable dans core.events mais ne
    # contribue pas une seconde fois au régime AG9 transmis au PM.
    eligible = [
        event for event in events
        if event.get("effective_score") is not None
        and (event.get("confidence") or 0) >= minimum_confidence
        and not event.get("ag4_duplicate")
    ]
    global_score = bounded_aggregate(event["effective_score"] for event in eligible)
    ok_sources = [row for row in source_health if row.get("status") == "OK"]
    coverage = len(ok_sources) / len(source_health) if source_health else 0.0
    confidences = [event["confidence"] for event in eligible if event.get("confidence") is not None]
    confidence = (sum(confidences) / len(confidences)) * coverage if confidences else 0.0
    critical_threshold = float(os.environ.get("AG9_CRITICAL_SCORE_THRESHOLD", config["critical_score_threshold"]))
    critical = [event for event in eligible if event["effective_score"] >= critical_threshold]
    missing_sources = [row["capability"] for row in source_health if row.get("status") in {"MISSING", "ERROR", "INCOMPATIBLE", "EMPTY"}]
    stale_sources = [row["capability"] for row in source_health if row.get("status") == "STALE"]
    freshness = "missing" if not eligible else ("stale" if all((event.get("freshness_decay") or 0) < 0.5 for event in eligible) else "fresh")
    country = overlay_rows(snapshot_id, eligible, "countries", "entity_id")
    sector = overlay_rows(snapshot_id, eligible, "sectors", "sector")
    currency = overlay_rows(snapshot_id, eligible, "currencies", "entity_id")
    commodity = overlay_rows(snapshot_id, eligible, "commodities", "entity_id")
    chokepoint = overlay_rows(snapshot_id, eligible, "chokepoints", "entity_id")
    output = {
        "snapshot_id": snapshot_id, "schema_version": "AG9_GLOBAL_RISK_V1", "as_of": now.isoformat(), "created_at": now.isoformat(),
        "global_risk_regime": regime_for_score(global_score, config["regime_thresholds"]),
        "global_risk_score": global_score, "critical_events": critical,
        "country_overlays": {row["entity_id"]: row for row in country},
        "sector_overlays": {row["sector"]: row for row in sector},
        "currency_overlays": {row["entity_id"]: row for row in currency},
        "commodity_overlays": {row["entity_id"]: row for row in commodity},
        "asset_overlays": {}, "missing_sources": missing_sources, "stale_sources": stale_sources,
        "confidence": max(0.0, min(1.0, confidence)), "coverage_ratio": coverage,
        "freshness_status": freshness, "method_version": "AG9_EVENT_RISK_V1",
        "source_health": source_health, "pipeline_health": "DEGRADED" if missing_sources else "OK",
    }
    output["payload_hash"] = canonical_hash(output)
    return {"snapshot": output, "country_risk": country, "sector_impacts": sector, "currency_overlays": currency, "commodity_overlays": commodity, "chokepoint_status": chokepoint}
