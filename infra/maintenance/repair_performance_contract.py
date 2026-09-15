#!/usr/bin/env python3
"""Repair measurement metadata only; never modify NAV, fills, orders or cash.

Run against isolated copies first. Live application requires quiescent writers and
an external file backup. Transactional row backups are also retained per database.
DuckDB 1.4.3 is required for the news/AG1 production readers.
"""
import argparse
import ast
import datetime
import hashlib
import json
import math
from pathlib import Path
import duckdb

MIGRATION = "performance_contract_20260914_v1"


def pf_functions(path):
    tree = ast.parse(Path(path).read_text(encoding="utf-8-sig"))
    selected = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in {"parse_float", "measured_snapshot_risk"}]
    ns = {"math": math}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), "exec"), ns)
    return ns["measured_snapshot_risk"]


def nav_digest(con):
    rows = con.execute("SELECT run_id, ts, cash_eur, equity_eur, total_value_eur FROM core.portfolio_snapshot ORDER BY ts, run_id").fetchall()
    return hashlib.sha256(json.dumps(rows, default=str).encode()).hexdigest()


def repair_ag1(con, measure):
    before = nav_digest(con)
    con.execute("CREATE TABLE IF NOT EXISTS core.portfolio_snapshot_backup_pc20260914 AS SELECT * FROM core.portfolio_snapshot")
    con.execute("CREATE TABLE IF NOT EXISTS core.risk_metrics_backup_pc20260914 AS SELECT * FROM core.risk_metrics")
    rows = con.execute("SELECT run_id, ts, total_value_eur, cash_eur, meta_json FROM core.portfolio_snapshot ORDER BY ts, run_id").fetchall()
    for run, ts, nav, cash, raw in rows:
        positions = con.execute("SELECT run_id,ts,symbol,qty,avg_cost,last_price,market_value_eur FROM core.positions_snapshot WHERE run_id=?", [run]).fetchall()
        m = measure(con, ts, float(nav), float(cash), positions)
        meta = json.loads(raw or "{}")
        meta.update(m["meta"])
        meta["measurement_repair"] = MIGRATION
        con.execute("UPDATE core.portfolio_snapshot SET drawdown_pct=?, cum_ai_cost_eur=?, meta_json=? WHERE run_id=?", [m["drawdown"],m["booked_ai"],json.dumps(meta),run])
        existing = con.execute("SELECT limits_json FROM core.risk_metrics WHERE run_id=?",[run]).fetchone()
        if existing:
            limits = json.loads(existing[0] or "{}")
            limits.update(m["meta"])
            # Historical sector reconstruction uses current classification, explicitly.
            limits["sector_taxonomy"] = "current_instruments_reconstruction"
            if "breaches" in limits:
                threshold = limits.get("max_daily_drawdown_pct")
                daily = m["meta"].get("daily_return_pct")
                limits["breaches"]["daily_drawdown_pct"] = daily <= -float(threshold) if threshold and daily is not None else None
            con.execute("UPDATE core.risk_metrics SET top1_sector_pct=?, var95_est_eur=NULL, risk_status=?, limits_json=? WHERE run_id=?",[m["top_sector"], "RISK_OFF" if limits.get("kill_switch_active") else m["status"],json.dumps(limits),run])
    assert nav_digest(con) == before, "NAV_CHANGED_ABORT"
    return {"snapshots_repaired":len(rows),"nav_unchanged":True,"nav_sha256":before}


def repair_news(con):
    condition = "source='ibkr' AND published_at > LEAST(first_seen_at, analyzed_at)"
    con.execute("CREATE TABLE IF NOT EXISTS news_timestamp_backup_pc20260914 AS SELECT * FROM news_history WHERE " + condition)
    count = con.execute("SELECT COUNT(*) FROM news_history WHERE " + condition).fetchone()[0]
    # Original row, including raw field, is preserved verbatim in the backup.
    con.execute("UPDATE news_history SET published_at=LEAST(first_seen_at, analyzed_at), reason=COALESCE(reason,'') || ' TIMESTAMP_REPAIRED_TO_FIRST_OBSERVATION:provider_timezone_unverified', updated_at=CURRENT_TIMESTAMP WHERE " + condition)
    remaining = con.execute("SELECT COUNT(*) FROM news_history WHERE " + condition).fetchone()[0]
    assert remaining == 0
    return {"future_publications_repaired":count,"remaining":remaining}


def repair_runs(con):
    condition = "status='RUNNING' AND started_at < CURRENT_TIMESTAMP - INTERVAL '6 hours'"
    con.execute("CREATE TABLE IF NOT EXISTS run_log_backup_pc20260914 AS SELECT * FROM run_log WHERE " + condition)
    count = con.execute("SELECT COUNT(*) FROM run_log WHERE " + condition).fetchone()[0]
    con.execute("UPDATE run_log SET status='ERROR', finished_at=CURRENT_TIMESTAMP, error_detail=COALESCE(error_detail,'') || ' AUTO_RECONCILED_STALE_RUN: completion time unknown' WHERE " + condition)
    return {"stale_runs_reconciled":count}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-dir",type=Path,required=True)
    parser.add_argument("--pf-node",type=Path,required=True)
    parser.add_argument("--apply",action="store_true")
    args=parser.parse_args()
    measure=pf_functions(args.pf_node)
    receipt={}
    for name in ["ag1_v4_consensus","ag4_spe_v2","ag3_v2"]:
        with duckdb.connect(str(args.db_dir/(name+".duckdb")),read_only=not args.apply) as con:
            if not args.apply:
                receipt[name]={"read_only":True,"tables":con.execute("SELECT COUNT(*) FROM information_schema.tables").fetchone()[0]}
                continue
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute("CREATE TABLE IF NOT EXISTS maintenance_receipts (migration_id VARCHAR PRIMARY KEY, applied_at TIMESTAMPTZ, receipt_json JSON)")
                row=con.execute("SELECT receipt_json FROM maintenance_receipts WHERE migration_id=?",[MIGRATION]).fetchone()
                if row:
                    receipt[name]={"already_applied":True,"receipt":json.loads(row[0])}
                else:
                    result=repair_ag1(con,measure) if name=="ag1_v4_consensus" else repair_news(con) if name=="ag4_spe_v2" else {}
                    if name in {"ag3_v2","ag4_spe_v2"}:result.update(repair_runs(con))
                    con.execute("INSERT INTO maintenance_receipts VALUES (?,CURRENT_TIMESTAMP,?)",[MIGRATION,json.dumps(result)])
                    receipt[name]=result
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
    print(json.dumps(receipt,indent=2))


if __name__=="__main__":
    main()
