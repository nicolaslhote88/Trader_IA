"""Read-only data feasibility assessment. Never invent a trained probability.

The first target is total-return sector outperformance at 90 calendar days.
Training is gated on point-in-time facts, adjusted prices and enough disjoint
time windows. Repeated daily snapshots are not independent samples.
"""
import argparse
import json
from pathlib import Path
import duckdb


def assess(db):
    with duckdb.connect(str(db), read_only=True) as con:
        first,last,n,symbols=con.execute('SELECT min(updated_at),max(updated_at),count(*),count(distinct symbol) FROM fundamentals_triage_history').fetchone()
        span=(last-first).days if first and last else 0
        # This query audits the available contract, not a backtest label.
        metrics,periods=con.execute('SELECT count(*),count(period) FROM fundamental_metrics_history').fetchone()
    return {
        'status':'NOT_READY_FOR_VALIDATED_PROBABILITIES', 'mode':'READ_ONLY_NO_MODEL_NO_ORDER',
        'target':{'event':'90-day total-return outperformance versus point-in-time sector benchmark',
                  'horizon_days':90,'currency_policy':'same currency for equity and benchmark'},
        'available':{'first_observation':str(first),'last_observation':str(last),'calendar_days':span,
                     'snapshots':n,'symbols':symbols,'metric_rows':metrics,'metric_rows_with_period':periods,
                     'nonoverlapping_90d_windows_upper_bound':span//90,
                     'nonoverlapping_365d_windows_upper_bound':span//365},
        'blocking_requirements':['Point-in-time accounting periods/publication dates and revision provenance',
                                  'Split/dividend-adjusted prices and matching sector benchmark history',
                                  'Several disjoint market periods for train/calibration/test, including delisted names'],
        'evaluation_contract':{'baseline':'training-period event frequency and regularized logistic regression',
                               'challenger':'tabular gradient boosting',
                               'split':'all symbols grouped by date; purge label windows crossing boundaries; separate calibration period',
                               'metrics':['Brier score','log loss','reliability curves with sample counts','sector and period robustness'],
                               'promotion':'only after out-of-time improvement and shadow monitoring; no effect on AG1 gates before promotion'},
        'training_performed':False,'probabilities_published':False,
    }


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--db',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();report=assess(args.db)
    args.output.write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps(report,ensure_ascii=False))
