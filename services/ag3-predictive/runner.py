"""Idempotent, single-writer collection/evaluation command, invoked by cron."""
import argparse
import fcntl
import json
import logging
import os
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from collectors import import_universe, sec_index, esef_index, collect_sec, collect_esef, collect_prices, collect_ecb, collect_fred
from regional import collect_dart, collect_edinet
from http_client import Client
from store import ROOT, initialize, now, db, coverage, atomic_json, status

EUROPE={'France','Netherlands','Germany','Switzerland','United Kingdom','Luxembourg','Belgium','Spain','Denmark','Italy'}


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--symbols',default='')
    p.add_argument('--limit',type=int,default=0)
    p.add_argument('--scheduled',action='store_true')
    p.add_argument('--train-only',action='store_true')
    p.add_argument('--force-prices',action='store_true')
    args=p.parse_args()
    if args.force_prices:
        os.environ['AG3_PRICE_FORCE_REFRESH']='1'
    if args.scheduled and datetime.now(ZoneInfo('Europe/Paris')).hour!=4:
        return
    ROOT.mkdir(parents=True,exist_ok=True)
    lock=(ROOT/'collector.lock').open('a')
    try:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        print('COLLECTOR_ALREADY_RUNNING',flush=True)
        return
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(message)s',
                        handlers=[logging.FileHandler(ROOT/'collector.log'),logging.StreamHandler()])
    initialize()
    run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    with db() as c:
        c.execute("UPDATE runs SET status='INTERRUPTED',ended_at=? WHERE status='RUNNING'",(now(),))
        c.execute('INSERT INTO runs VALUES(?,?,NULL,?,NULL)',(run_id,now(),'RUNNING'))
    errors=[]
    def attempt(symbol,pipe,func,*values):
        try:
            func(*values)
        except Exception as exc:
            # HTTP client sanitizes errors; don't print URLs carrying API keys.
            detail=str(exc) if type(exc).__name__=='FetchError' else type(exc).__name__
            coverage(symbol,pipe,'ERROR',detail)
            errors.append({'symbol':symbol,'pipe':pipe,'error':detail})
            logging.warning('%s %s %s',symbol,pipe,detail)
    try:
        if not args.train_only:
            for trial in range(5):
                try:
                    import_universe()
                    break
                except Exception:
                    if trial==4:
                        raise
                    time.sleep(10)
            client=Client()
            seeds=sec_index(client)
            attempt('*','esef_index',esef_index,client)
            with db(True) as c:
                instruments=[dict(r) for r in c.execute('SELECT * FROM instruments ORDER BY held DESC,symbol')]
            selected=set(args.symbols.split(',')) if args.symbols else None
            if selected:
                instruments=[r for r in instruments if r['symbol'] in selected]
            # First acquire the US training panel, then European coverage.
            instruments.sort(key=lambda r:(0 if r['symbol'] in seeds else 1,0 if r['held'] else 1,r['symbol']))
            if args.limit:
                instruments=instruments[:args.limit]
            for symbol in ['SPY']:
                attempt(symbol,'prices',collect_prices,client,{'symbol':symbol,'yahoo':symbol})
            attempt('*','ecb',collect_ecb,client)
            attempt('*','alfred',collect_fred,client)
            for i,instrument in enumerate(instruments):
                symbol=instrument['symbol']
                if instrument['quarantined'] and not instrument['held']:
                    coverage(symbol,'scope','QUARANTINED')
                    continue
                if instrument['asset_class']!='EQUITY':
                    coverage(symbol,'scope','ETF_SEPARATE_MODEL_REQUIRED')
                    continue
                attempt(symbol,'prices',collect_prices,client,instrument)
                attempt(symbol,'sec',collect_sec,client,instrument,seeds)
                if instrument['country'] in EUROPE:
                    attempt(symbol,'esef',collect_esef,client,instrument)
                logging.info('PROGRESS %d/%d %s',i+1,len(instruments),symbol)
                atomic_json(ROOT/'status.json',status())
            attempt('*','opendart',collect_dart,client,instruments)
            attempt('*','edinet',collect_edinet,client)
        from model import train
        report=train()
        outcome='PARTIAL' if errors else 'COMPLETED_WITH_COVERAGE_GAPS'
        with db() as c:
            c.execute('UPDATE runs SET ended_at=?,status=?,report_json=? WHERE run_id=?',
                      (now(),outcome,json.dumps({'errors':errors,'model_status':report['status']}),run_id))
        atomic_json(ROOT/'status.json',status())
        logging.info('FINISHED %s %s',run_id,report['status'])
    except Exception as exc:
        with db() as c:
            c.execute('UPDATE runs SET ended_at=?,status=?,report_json=? WHERE run_id=?',
                      (now(),'FAILED',json.dumps({'error_type':type(exc).__name__}),run_id))
        logging.exception('COLLECTION_FAILED')
        atomic_json(ROOT/'status.json',status())
        raise


if __name__=='__main__':
    main()
