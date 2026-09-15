#!/usr/bin/env python3
"""Read-only decision-input and investable-universe coverage report.
Run after the evening producers; outputs stay private on the VPS.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import duckdb


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-dir",type=Path,default=Path("/files/duckdb"))
    p.add_argument("--output",type=Path,required=True)
    a=p.parse_args()
    now=datetime.now(timezone.utc)
    report={"as_of":now.isoformat(),"schema_version":"performance_contract_v1",
            "score_calibrated":False,"evaluation_state":"collecting_point_in_time_inputs",
            "evaluation_horizons_sessions":[1,5,20],"strategy_changed_from_future_outcomes":False}
    with duckdb.connect(str(a.db_dir/"ag2_v3.duckdb"),read_only=True) as c:
        rows=c.execute("""WITH latest AS (SELECT * FROM technical_signals QUALIFY ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY workflow_date DESC)=1),
        segments AS (SELECT symbol,string_agg(segment, ',') AS segments FROM universe_segments WHERE active GROUP BY symbol)
        SELECT u.symbol,u.exchange,s.segments,COALESCE(q.active,FALSE),t.h1_date,t.d1_date,t.d1_confidence
        FROM universe u LEFT JOIN segments s ON s.symbol=u.symbol LEFT JOIN universe_quarantine q ON q.symbol=u.symbol
        LEFT JOIN latest t ON t.symbol=u.symbol WHERE COALESCE(u.enabled,TRUE) AND u.asset_class IN ('EQUITY','ETF') ORDER BY u.symbol""").fetchall()
        report["coverage"]={"universe":len(rows),"segmented":sum(bool(r[2]) for r in rows),"quarantined":sum(r[3] for r in rows),
                            "technical_present":sum(r[4] is not None for r in rows),"confidence_present":sum(r[6] is not None for r in rows)}
        report["missing_segments"]=[r[0] for r in rows if not r[2] and not r[3]]
        report["universe_rows"]=[dict(zip(["symbol","exchange","segments","quarantined","h1_as_of","d1_as_of","confidence"],r)) for r in rows]
    gates=Counter();captured=[]
    with duckdb.connect(str(a.db_dir/"ag1_v4_consensus.duckdb"),read_only=True) as c:
        runs=c.execute("SELECT run_id,ts_start,strategy_version,prompt_version,agent_output_json FROM core.runs WHERE EXISTS (SELECT 1 FROM core.model_proposals p WHERE p.run_id=core.runs.run_id) AND ts_start>=CURRENT_TIMESTAMP-INTERVAL '90 days' ORDER BY ts_start").fetchall()
        for run,ts,strategy,prompt,raw in runs:
            payload=json.loads(raw or '{}');inp=payload.get("decisionInput") or {}
            if not inp:continue
            candidates=inp.get("executionAudit") or []
            for row in candidates:gates.update(g for g in str(row.get("gates") or "OK").split('|') if g)
            captured.append({"run_id":run,"as_of":ts,"strategy_version":strategy,"prompt_version":prompt,
                             "candidate_count":len(candidates),"selected":sum(bool(r.get("selected")) for r in candidates),
                             "input_snapshot":inp.get("inputSnapshot")})
        report["decision_journal"]={"runs_in_window":len(runs),"captured_runs":len(captured),"capture_start":captured[0]["as_of"] if captured else None,
                                     "gates":dict(gates),"runs":captured}
    # No fabricated backtest or win rate before eligible forward observations exist.
    a.output.parent.mkdir(parents=True,exist_ok=True)
    temp=a.output.with_suffix(a.output.suffix+".tmp");temp.write_text(json.dumps(report,ensure_ascii=False,default=str,indent=2),encoding="utf-8");temp.replace(a.output)
    print(json.dumps({"coverage":report["coverage"],"captured_runs":len(captured),"output":str(a.output)}))

if __name__=="__main__":main()
