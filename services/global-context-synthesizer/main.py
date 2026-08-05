"""API du Global-Context-Synthesizer."""

from __future__ import annotations

import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Body, FastAPI, HTTPException, Query

from db import GlobalContextDB
from synthesizer import advisory_pack_for_run, synthesize


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("global-context-synthesizer")

db = GlobalContextDB()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("global-context-synthesizer started")
    yield


app = FastAPI(title="Global Context Synthesizer", version="1.1.0", lifespan=lifespan)


@app.get("/health")
async def health():
    latest = db.latest()
    return {"status": "OK", "service": "global-context-synthesizer", "version": "1.1.0", "latest_snapshot_id": (latest or {}).get("snapshot_id"), "latest_status": (latest or {}).get("status")}


@app.post("/synthesize")
async def run_synthesis():
    run_id = f"GCRUN_{uuid.uuid4().hex[:16]}"
    started = datetime.now(timezone.utc)
    try:
        bundle = synthesize(
            os.environ.get("MACRO_DUCKDB_PATH", "/files/duckdb/macro_data.duckdb"),
            os.environ.get("WORLD_MONITOR_DUCKDB_PATH", "/files/duckdb/worldmonitor_v1.duckdb"),
            now=datetime.now(timezone.utc),
        )
        rows_written = sum(len(bundle.get(key) or []) for key in ("component_status", "country_context", "currency_context", "sector_context", "asset_context", "critical_events", "source_lineage")) + 2
        bundle["run_log"] = {
            "run_id": run_id, "started_at": started, "finished_at": datetime.now(timezone.utc),
            "status": bundle["snapshot"]["status"],
            "components_available": sum(row["row_count"] > 0 for row in bundle["component_status"]),
            "components_missing": sum(row["status"] == "MISSING" for row in bundle["component_status"]),
            "rows_written": rows_written,
        }
        db.publish(bundle)
        return {"status": bundle["snapshot"]["status"], "run_id": run_id, "snapshot": bundle["snapshot"]}
    except Exception as exc:
        logger.exception("Global context synthesis failed")
        db.persist_error({"run_id": run_id, "started_at": started, "finished_at": datetime.now(timezone.utc), "error_code": type(exc).__name__, "error_detail": str(exc)[:1000]})
        raise HTTPException(status_code=500, detail={"error_code": type(exc).__name__, "detail": str(exc)[:1000]})


@app.get("/latest")
async def latest():
    snapshot = db.latest()
    if not snapshot:
        raise HTTPException(status_code=404, detail={"error_code": "GLOBAL_CONTEXT_NOT_FOUND"})
    return snapshot


@app.get("/ag1-pack")
async def ag1_pack_get():
    enabled = os.environ.get("GLOBAL_CONTEXT_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
    base = db.latest_pack() if enabled else None
    pack = advisory_pack_for_run(base, {"countries": [], "sectors": [], "assets": []}, [], [])
    if not enabled:
        pack["status"] = "GLOBAL_CONTEXT_DISABLED"
        pack["source_warnings"] = ["GLOBAL_CONTEXT_DISABLED"]
        pack.pop("payload_hash", None)
        from synthesizer import payload_hash
        pack["payload_hash"] = payload_hash(pack)
    return pack


@app.post("/ag1-pack")
async def ag1_pack_post(payload: dict = Body(default_factory=dict)):
    enabled = os.environ.get("GLOBAL_CONTEXT_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
    base = db.latest_pack() if enabled else None
    context = db.latest_exposure_context() if base else {"countries": [], "sectors": [], "assets": []}
    pack = advisory_pack_for_run(base, context, payload.get("portfolio") or [], payload.get("opportunities") or [])
    if not enabled:
        pack["status"] = "GLOBAL_CONTEXT_DISABLED"
        pack["source_warnings"] = ["GLOBAL_CONTEXT_DISABLED"]
        pack.pop("payload_hash", None)
        from synthesizer import payload_hash
        pack["payload_hash"] = payload_hash(pack)
    return pack


@app.get("/runs")
async def runs(limit: int = Query(100, ge=1, le=1000)):
    return {"runs": db.history(limit)}
