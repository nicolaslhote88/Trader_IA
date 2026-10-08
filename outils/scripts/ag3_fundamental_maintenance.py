"""Dedicated latest-row repair. Preserve source dates; require a physical backup.

Run with DuckDB 1.4.3. Export is read-only; --apply uses an explicit transaction.
No trading database is opened and no orders are invoked.
"""
import argparse
import json
import shutil
from pathlib import Path
import duckdb

TRIAGE_FIELDS = {
    'score': 'Score', 'funda_conf': 'funda_conf', 'risk_score': 'risk_score',
    'quality_score': 'quality_score', 'growth_score': 'growth_score',
    'valuation_score': 'valuation_score', 'health_score': 'health_score',
    'consensus_score': 'consensus_score', 'horizon': 'horizon',
    'current_price': 'current_price', 'target_price': 'target_price',
    'upside_pct': 'upside_pct', 'analyst_count': 'analyst_count',
    'valuation': 'valuation', 'why': 'why', 'risks': 'risks',
    'next_steps': 'nextSteps', 'strategy_version': 'strategy_version',
}
CONS_FIELDS = {'recommendation_mean':'recommendationMean','analyst_count':'analystCount',
               'current_price':'currentPrice','target_mean_price':'targetMeanPrice',
               'target_high_price':'targetHighPrice','target_low_price':'targetLowPrice',
               'upside_pct':'upsidePct','dispersion_pct':'dispersionPct',
               'confidence_proxy':'confidenceProxy','risk_proxy':'riskProxy','horizon':'horizon'}


def dict_rows(con, sql):
    cursor = con.execute(sql)
    cols = [x[0] for x in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def export_input(db, output):
    with duckdb.connect(str(db), read_only=True) as con:
        triage = dict_rows(con, 'SELECT * FROM v_latest_triage')
        snapshots = dict_rows(con, '''SELECT s.* FROM fundamentals_snapshot s
            JOIN v_latest_triage t ON s.symbol=t.symbol AND s.run_id=t.run_id''')
        by_key = {(s['symbol'], s['run_id']): s for s in snapshots}
        inputs = []
        for t in triage:
            s = by_key.get((t['symbol'], t['run_id']))
            if s is None:
                raise RuntimeError('MISSING_SNAPSHOT:' + t['symbol'])
            j = {'Symbol': t['symbol'], 'Name': t['name'], 'Sector': t['sector'],
                 'BoursoramaRef': t['boursorama_ref'], 'run_id': t['run_id'],
                 'asOfDate': str(t['as_of_date']), 'fetchedAt': str(t['fetched_at']),
                 'ok': s['status'] == 'OK', 'error': s['error'],
                 'meta': {'dataCoveragePctApprox': s['data_coverage_pct']}}
            for dest, src in {'profile':'profile_json','price':'price_json','valuation':'valuation_json',
                              'profitability':'profitability_json','growth':'growth_json',
                              'financialHealth':'financial_health_json','consensus':'consensus_json'}.items():
                j[dest] = json.loads(s[src] or '{}')
            inputs.append({'input': j, 'original': t})
        output.write_text(json.dumps(inputs, default=str, ensure_ascii=False), encoding='utf-8')
        print(json.dumps({'exported': len(inputs)}))


def apply_plan(db, plan, backup):
    assert duckdb.__version__ == '1.4.3', 'WRITER_MUST_BE_1.4.3'
    assert backup.resolve() != db.resolve()
    assert not Path(str(db) + '.wal').exists(), 'ACTIVE_WAL'
    assert not backup.exists(), 'BACKUP_ALREADY_EXISTS'
    with duckdb.connect(str(db), read_only=True):
        shutil.copy2(db, backup)
    changes = json.loads(plan.read_text(encoding='utf-8'))
    con = duckdb.connect(str(db))
    try:
        before_dates = con.execute('SELECT count(*), bit_xor(hash(record_id, updated_at, fetched_at, as_of_date)) FROM fundamentals_triage_history').fetchone()
        con.execute('BEGIN TRANSACTION')
        count = 0
        for item in changes:
            old = item['original']; scored = item['scored']
            actual = con.execute('SELECT run_id,score,risk_score,cast(updated_at as varchar) FROM v_latest_triage WHERE symbol=?', [old['symbol']]).fetchone()
            assert actual and actual[0] == old['run_id'] and actual[1:3] == (old['score'],old['risk_score']), 'CONCURRENT_CHANGE:' + old['symbol']
            t = scored['triageRow']; c = scored['consensusRow']
            assert t['RecordId'] == old['record_id'], 'RECORD_ID_MISMATCH'
            con.execute('UPDATE fundamentals_triage_history SET ' + ','.join(k+'=?' for k in TRIAGE_FIELDS) + ' WHERE record_id=?',
                        [t[v] for v in TRIAGE_FIELDS.values()] + [old['record_id']])
            con.execute('UPDATE analyst_consensus_history SET ' + ','.join(k+'=?' for k in CONS_FIELDS) + ' WHERE symbol=? AND run_id=?',
                        [c[v] for v in CONS_FIELDS.values()] + [old['symbol'],old['run_id']])
            metrics = {m['Metric']:m for m in scored['metricRows']}
            existing = con.execute('SELECT record_id,metric FROM fundamental_metrics_history WHERE symbol=? AND run_id=?',[old['symbol'],old['run_id']]).fetchall()
            for rid, key in existing:
                m = metrics.get(key)
                # Preserve extraction dates and rows; never resurrect older values.
                if m:
                    con.execute('UPDATE fundamental_metrics_history SET value_num=?,value_text=NULL,unit=?,sig_hash=? WHERE record_id=?',[m['Value'],m['Unit'],m['SigHash'],rid])
                else:
                    con.execute('UPDATE fundamental_metrics_history SET value_num=NULL,value_text=NULL,sig_hash=NULL WHERE record_id=?',[rid])
            count += 1
        after_dates = con.execute('SELECT count(*), bit_xor(hash(record_id, updated_at, fetched_at, as_of_date)) FROM fundamentals_triage_history').fetchone()
        assert before_dates == after_dates, 'HISTORY_DATES_OR_COUNTS_CHANGED'
        con.execute('COMMIT')
        print(json.dumps({'repaired_latest': count, 'dates_and_counts_preserved': True, 'backup': str(backup)}))
    except Exception:
        con.execute('ROLLBACK')
        raise
    finally:
        con.close()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--db', type=Path, required=True)
    parser.add_argument('--export', type=Path)
    parser.add_argument('--apply', type=Path)
    parser.add_argument('--backup', type=Path)
    args=parser.parse_args()
    assert args.db.name == 'ag3_v2.duckdb', 'AG3_ONLY'
    if args.export:
        export_input(args.db, args.export)
    else:
        assert args.apply and args.backup
        apply_plan(args.db, args.apply, args.backup)


if __name__ == '__main__':
    main()
