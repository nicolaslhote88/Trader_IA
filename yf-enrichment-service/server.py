import os
import shlex
import subprocess
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI
from pydantic import BaseModel

SERVICE_VERSION = "yf-enrichment-service_v1"

app = FastAPI()


class RunReq(BaseModel):
    yf_enrich_db_path: str = "/files/duckdb/yf_enrichment_v1.duckdb"
    ag2_db_path: str = "/files/duckdb/ag2_v2.duckdb"
    yf_api_url: str = "http://yfinance-api:8080"
    target_days: int = 30
    quote_chunk_size: int = 80
    timeout_sec: int = 14


def _pick_script() -> str:
    primary = os.getenv("YF_ENRICH_SCRIPT", "/workspace/yf-enrichment-v1/daily_enrichment.py")
    candidates = [primary, "/files/yf-enrichment-v1/daily_enrichment.py", "/opt/trader-ia/yf-enrichment-v1/daily_enrichment.py"]
    for path in candidates:
        if os.path.exists(path):
            return path
    return primary


@app.get("/health")
def health():
    return {"status": "OK", "service_version": SERVICE_VERSION}


@app.post("/run")
def run(req: RunReq | None = None):
    payload = req or RunReq()
    run_id = f"RUN_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:12]}"
    script = _pick_script()

    cmd = [
        "python",
        script,
        "--yf-enrich-db-path",
        payload.yf_enrich_db_path,
        "--ag2-db-path",
        payload.ag2_db_path,
        "--yf-api-url",
        payload.yf_api_url,
        "--target-days",
        str(payload.target_days),
        "--quote-chunk-size",
        str(payload.quote_chunk_size),
        "--timeout-sec",
        str(payload.timeout_sec),
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True)
    stdout = (proc.stdout or "")[-12000:]
    stderr = (proc.stderr or "")[-12000:]

    out = {
        "run_id": run_id,
        "service_version": SERVICE_VERSION,
        "at": datetime.now(timezone.utc).isoformat(),
        "status": "OK" if proc.returncode == 0 else "ERROR",
        "exitCode": int(proc.returncode),
        "command": " ".join(shlex.quote(c) for c in cmd),
        "stdout": stdout,
        "stderr": stderr,
    }
    return out
