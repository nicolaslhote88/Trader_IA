import json
from datetime import datetime, timezone

import duckdb


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


items = _items or []
if not items:
    return []

out = []
for it in items:
    j = dict(it.get("json", {}) or {})
    run_id = str(j.get("run_id") or "").strip()
    db_path = str(j.get("db_path") or "").strip()
    if not run_id:
        raise ValueError("Post-Run Health: missing run_id")
    if not db_path:
        raise ValueError("Post-Run Health: missing db_path")

    con = duckdb.connect(db_path)
    try:
        portfolio_row = con.execute(
            """
            SELECT
              COUNT(*) AS cnt,
              COALESCE(MAX(CAST(equity_eur AS DOUBLE)), 0) AS equity_eur
            FROM core.portfolio_snapshot
            WHERE run_id = ?
            """,
            [run_id],
        ).fetchone()
        positions_row = con.execute(
            "SELECT COUNT(*) FROM core.positions_snapshot WHERE run_id = ?",
            [run_id],
        ).fetchone()

        portfolio_cnt = int(portfolio_row[0] or 0)
        equity_eur = float(portfolio_row[1] or 0.0)
        positions_cnt = int(positions_row[0] or 0)

        health_ok = portfolio_cnt > 0 and (positions_cnt > 0 or equity_eur <= 0.0)
        checks = {
            "portfolio_snapshot_count": portfolio_cnt,
            "positions_snapshot_count": positions_cnt,
            "equity_eur": equity_eur,
            "rule": "portfolio must exist; positions may be empty only if equity is zero",
        }

        if not health_ok:
            alert_id = f"ALT|{run_id}|POST_HEALTH"
            payload = json.dumps(checks, ensure_ascii=False)
            ts = utc_now_iso()
            con.execute(
                """
                INSERT INTO core.alerts (
                  alert_id, run_id, ts, severity, category, symbol, message, code, payload_json
                )
                VALUES (?, ?, ?, 'CRITICAL', 'SYSTEM', 'GLOBAL', ?, 'POST_RUN_HEALTH_FAIL', ?)
                ON CONFLICT (alert_id) DO UPDATE SET
                  ts = excluded.ts,
                  message = excluded.message,
                  payload_json = excluded.payload_json
                """,
                [
                    alert_id,
                    run_id,
                    ts,
                    "Post-run health check failed: missing portfolio/positions snapshot for run",
                    payload,
                ],
            )

        out.append(
            {
                "json": {
                    "run_id": run_id,
                    "db_path": db_path,
                    "health_ok": health_ok,
                    "checks": checks,
                    "writer": j.get("writer_path"),
                    "upsert": j.get("upsert"),
                    "snapshots": j.get("snapshots"),
                },
                "pairedItem": it.get("pairedItem"),
            }
        )
    finally:
        try:
            con.close()
        except Exception:
            pass

return out
