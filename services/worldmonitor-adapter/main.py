"""World Monitor adapter et agent analytique AG9."""

from __future__ import annotations

import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query

from ag4_dedupe import load_ag4_fingerprints
from client import WorldMonitorClient, WorldMonitorError, canonical_hash, redact
from db import WorldMonitorDB
from normalizer import (
    build_ag9_snapshot,
    deduplicate_events,
    extract_records,
    normalize_record,
    overlay_rows,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("worldmonitor-adapter")

client = WorldMonitorClient()
db = WorldMonitorDB()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def load_capability_config() -> dict:
    path = Path(__file__).resolve().parent / "config" / "capabilities.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_tool(entry: dict, tools_by_name: dict[str, dict]) -> dict | None:
    for candidate in entry["candidates"]:
        if candidate in tools_by_name:
            return tools_by_name[candidate]
    # Repli contrôlé : tous les tokens significatifs doivent être présents.
    capability_tokens = [token for token in entry["capability"].lower().split("_") if len(token) > 2]
    matches = [tool for name, tool in tools_by_name.items() if capability_tokens and all(token in name.lower() for token in capability_tokens)]
    return sorted(matches, key=lambda tool: str(tool.get("name")))[0] if len(matches) == 1 else None


async def discover_capabilities(*, catalog_only: bool = False) -> tuple[list[dict], dict[str, dict]]:
    config = load_capability_config()
    tools = await client.list_tools()
    tools_by_name = {str(tool.get("name")): tool for tool in tools if tool.get("name")}
    registry = []
    resolved = {}
    now = utcnow().isoformat()
    for entry in config["capabilities"]:
        tool = _resolve_tool(entry, tools_by_name)
        if not tool:
            registry.append({
                "capability": entry["capability"], "domain": entry["domain"], "tool_name": None,
                "tool_contract_hash": None, "tool_contract": None, "discovery_status": "MISSING",
                "compatible": False, "discovered_at": now, "config_version": config["config_version"],
                "detail": f"No unique match among {entry['candidates']}",
            })
            continue
        try:
            description = None if catalog_only else await client.describe_tool(tool)
            contract = {"catalog": tool}
            if description is not None:
                contract["description"] = description
            contract_hash = canonical_hash(contract)
            compatible = True
            detail = None
        except Exception as exc:
            contract = {"catalog": tool}
            contract_hash = canonical_hash(contract)
            compatible = False
            detail = redact(str(exc), client.api_key)
        row = {
            "capability": entry["capability"], "domain": entry["domain"], "tool_name": tool["name"],
            "tool_contract_hash": contract_hash, "tool_contract": contract,
            "discovery_status": ("CATALOG_ONLY" if catalog_only and compatible else ("AVAILABLE" if compatible else "INCOMPATIBLE")), "compatible": compatible,
            "discovered_at": now, "config_version": config["config_version"], "detail": detail,
        }
        registry.append(row)
        if compatible:
            resolved[entry["capability"]] = {**entry, "tool": tool, "contract_hash": contract_hash}
    db.persist_registry(registry)
    return registry, resolved


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("worldmonitor-adapter started mode=%s enabled=%s", client.mode, client.enabled)
    yield


app = FastAPI(title="World Monitor Adapter / AG9", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    latest = db.latest_snapshot()
    return {
        "status": "OK" if client.enabled else "DISABLED",
        "service": "worldmonitor-adapter", "version": "1.0.0", "mode": client.mode,
        "enabled": client.enabled, "credential_configured": bool(client.api_key),
        "latest_snapshot_id": (latest or {}).get("snapshot_id"),
        "latest_snapshot_as_of": (latest or {}).get("as_of"),
    }


@app.post("/admin/discover")
async def discover(catalog_only: bool = Query(False)):
    if not client.enabled and not catalog_only:
        raise HTTPException(status_code=503, detail={"error_code": "WORLD_MONITOR_DISABLED"})
    try:
        registry, resolved = await discover_capabilities(catalog_only=catalog_only)
        return {"status": "CATALOG_ONLY" if catalog_only else "OK", "catalog_count": len(registry), "resolved_count": len(resolved), "registry": registry}
    except WorldMonitorError as exc:
        raise HTTPException(status_code=502, detail={"error_code": exc.code, "detail": redact(exc.detail, client.api_key)})


@app.post("/ag9/refresh")
async def refresh_ag9():
    if not client.enabled:
        raise HTTPException(status_code=503, detail={"error_code": "WORLD_MONITOR_DISABLED"})
    run_id = f"AG9_{uuid.uuid4().hex[:16]}"
    started = utcnow()
    raw_responses = []
    source_health = []
    events = []
    try:
        registry, resolved = await discover_capabilities()
    except Exception as exc:
        detail = redact(str(exc), client.api_key)
        db.persist_error_run({
            "run_id": run_id, "started_at": started, "finished_at": utcnow(), "status": "ERROR",
            "error_code": "DISCOVERY_FAILED", "error_detail": detail,
        }, [], [])
        raise HTTPException(status_code=502, detail={"error_code": "DISCOVERY_FAILED", "detail": detail})

    registry_by_capability = {row["capability"]: row for row in registry}
    ag4_fingerprints = load_ag4_fingerprints()
    for capability, registry_row in registry_by_capability.items():
        checked_at = utcnow()
        if capability not in resolved:
            source_health.append({
                "run_id": run_id, "capability": capability, "tool_name": registry_row.get("tool_name"),
                "status": registry_row["discovery_status"], "latency_ms": 0.0, "rows_received": 0,
                "error_code": registry_row["discovery_status"], "error_detail": registry_row.get("detail"),
                "checked_at": checked_at.isoformat(),
            })
            continue
        selected = resolved[capability]
        tool = selected["tool"]
        request_id = f"WM_{uuid.uuid4().hex[:20]}"
        request_at = utcnow()
        timer = time.perf_counter()
        try:
            payload = await client.call_tool(tool, {})
            received_at = utcnow()
            records = extract_records(payload)
            status = "OK" if records else "EMPTY"
            raw_responses.append({
                "request_id": request_id, "run_id": run_id, "capability": capability,
                "tool_name": tool["name"], "tool_contract_hash": selected["contract_hash"],
                "requested_at": request_at.isoformat(), "received_at": received_at.isoformat(),
                "status": status, "cache_status": None, "payload_hash": canonical_hash(payload), "raw_payload": payload,
            })
            for record in records:
                events.append(normalize_record(record, domain=selected["domain"], tool_name=tool["name"], request_id=request_id, now=received_at, ag4_fingerprints=ag4_fingerprints))
            source_health.append({
                "run_id": run_id, "capability": capability, "tool_name": tool["name"], "status": status,
                "latency_ms": (time.perf_counter() - timer) * 1000.0, "rows_received": len(records),
                "checked_at": received_at.isoformat(),
            })
        except Exception as exc:
            received_at = utcnow()
            code = exc.code if isinstance(exc, WorldMonitorError) else type(exc).__name__
            detail = redact(exc.detail if isinstance(exc, WorldMonitorError) else str(exc), client.api_key)
            raw_responses.append({
                "request_id": request_id, "run_id": run_id, "capability": capability, "tool_name": tool["name"],
                "tool_contract_hash": selected["contract_hash"], "requested_at": request_at.isoformat(),
                "received_at": received_at.isoformat(), "status": "ERROR", "raw_payload": None,
                "error_code": code, "error_detail": detail,
            })
            source_health.append({
                "run_id": run_id, "capability": capability, "tool_name": tool["name"], "status": "ERROR",
                "latency_ms": (time.perf_counter() - timer) * 1000.0, "rows_received": 0,
                "error_code": code, "error_detail": detail, "checked_at": received_at.isoformat(),
            })

    deduplicated = deduplicate_events(events)
    ok_responses = sum(row["status"] == "OK" for row in source_health)
    if ok_responses == 0 or not deduplicated:
        run = {
            "run_id": run_id, "started_at": started, "finished_at": utcnow(), "status": "ERROR",
            "tools_discovered": len(registry), "tools_called": len(resolved), "responses_ok": ok_responses,
            "responses_error": sum(row["status"] in {"ERROR", "MISSING", "INCOMPATIBLE", "EMPTY"} for row in source_health),
            "events_normalized": len(events), "events_deduplicated": len(deduplicated),
            "error_code": "AG9_ZERO_VALID_EVENTS", "error_detail": "No eligible normalized event; previous snapshot remains current",
        }
        db.persist_error_run(run, raw_responses, source_health)
        raise HTTPException(status_code=502, detail={"error_code": run["error_code"], "source_health": source_health})

    now = utcnow()
    snapshot_id = f"AG9_{now.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    for event in deduplicated:
        event["snapshot_id"] = snapshot_id
    built = build_ag9_snapshot(snapshot_id=snapshot_id, events=deduplicated, source_health=source_health, now=now)
    filtered = lambda event_type: [event for event in deduplicated if event["event_type"] == event_type]
    bundle = {
        "raw_responses": raw_responses, "events": deduplicated, "source_health": source_health,
        "snapshot": built["snapshot"], "country_risk": built["country_risk"],
        "sector_impacts": built["sector_impacts"], "chokepoint_status": built["chokepoint_status"],
        "energy_risk": overlay_rows(snapshot_id, filtered("energy"), "commodities", "entity_id"),
        "supply_chain_risk": overlay_rows(snapshot_id, filtered("supply_chain") + filtered("chokepoints"), "countries", "entity_id"),
        "cyber_risk": overlay_rows(snapshot_id, filtered("cyber"), "sectors", "entity_id"),
        "sanctions": overlay_rows(snapshot_id, filtered("sanctions"), "countries", "entity_id"),
        "signal_convergence": overlay_rows(snapshot_id, filtered("convergence"), "countries", "entity_id"),
        "temporal_anomalies": overlay_rows(snapshot_id, filtered("temporal_anomalies"), "countries", "entity_id"),
        "asset_impacts": [],
        "run_log": {
            "run_id": run_id, "started_at": started, "finished_at": now,
            "status": "DEGRADED" if built["snapshot"]["missing_sources"] else "OK",
            "tools_discovered": len(registry), "tools_called": len(resolved), "responses_ok": ok_responses,
            "responses_error": sum(row["status"] != "OK" for row in source_health),
            "events_normalized": len(events), "events_deduplicated": len(deduplicated), "snapshot_id": snapshot_id,
            "payload": {"ag4_fingerprint_count": len(ag4_fingerprints)},
        },
    }
    db.persist_refresh(bundle)
    return {"status": bundle["run_log"]["status"], "run_id": run_id, "snapshot": built["snapshot"]}


@app.get("/ag9/latest")
async def latest_ag9():
    snapshot = db.latest_snapshot()
    if not snapshot:
        raise HTTPException(status_code=404, detail={"error_code": "AG9_SNAPSHOT_NOT_FOUND"})
    return snapshot


@app.get("/ag9/source-health")
async def ag9_source_health():
    return {"sources": db.source_health()}


@app.get("/ag9/runs")
async def ag9_runs(limit: int = Query(100, ge=1, le=1000)):
    return {"runs": db.run_history(limit)}
