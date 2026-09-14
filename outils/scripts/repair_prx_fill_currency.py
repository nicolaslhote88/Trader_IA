#!/usr/bin/env python3
"""Audited PRX cost repair and imported-fill native metadata repair. Dry-run by default.
Run with DuckDB 1.4.3 after a full offline backup; --apply is transactional and idempotent.
Broker snapshots and historical decisions retain their observed values.
"""
import argparse
import json
import math
import duckdb

PREFIX = 'maintenance_fill_currency_20260914'

def plan(con):
    rows = con.execute("""SELECT f.fill_id,o.symbol,f.price,f.price_native,f.fx_rate_eur,
        f.currency,f.fees_eur,f.raw_fill_json,f.ts_fill
        FROM core.fills f JOIN core.orders o USING(order_id)""").fetchall()
    updates = []
    for fid,sym,price,native,rate,ccy,fees,raw,ts in rows:
        payload = json.loads(raw or '{}')
        b = payload.get('ibkrFill') or {}
        if 'reconcile' not in str(payload.get('source')) or not b.get('price'):
            continue
        actual = float(b['price'])
        if actual <= 0 or price <= 0:
            raise ValueError('Invalid execution price: '+fid)
        final, fee = price, float(fees)
        if sym == 'PRX.AS':
            assert int(b['conid']) == 382625193 and b['listing_exchange'] == 'AEB'
            assert ccy == 'EUR' and float(b['size']) == 26 and actual == 39.355
            assert float(b['commission']) == 3
            assert con.execute("SELECT COUNT(*) FROM core.orders WHERE symbol='PRX.AS' AND side='SELL' AND status='FILLED'").fetchone()[0] == 0
            final, fee = actual, 3.0
        elif ccy != 'USD':
            # This repair only has proven scope for PRX EUR and imported USD metadata.
            continue
        new_rate = final / actual
        if native is None or rate is None or not math.isclose(native, actual, abs_tol=1e-9) or not math.isclose(rate,new_rate,abs_tol=1e-12) or final != price or fee != float(fees):
            updates.append(dict(fill_id=fid,symbol=sym,price=final,native=actual,rate=new_rate,fees=fee,
                                old_price=price,old_fees=float(fees),ts=str(ts)))
    return updates

def repair(con, updates):
    if not updates:
        return
    con.execute('BEGIN TRANSACTION')
    try:
        for table in ['fills','position_lots','fill_costs','portfolio_snapshot']:
            con.execute(f'CREATE TABLE main.{PREFIX}_{table} AS SELECT * FROM core.{table}')
        for u in updates:
            con.execute('UPDATE core.fills SET price=?,price_native=?,fx_rate_eur=?,fees_eur=? WHERE fill_id=?',
                        [u['price'],u['native'],u['rate'],u['fees'],u['fill_id']])
            if u['symbol']=='PRX.AS':
                lot = con.execute('SELECT open_qty,remaining_qty,status FROM core.position_lots WHERE open_fill_id=?',[u['fill_id']]).fetchall()
                assert len(lot)==1 and float(lot[0][0])==26 and float(lot[0][1])==26 and lot[0][2]=='OPEN'
                con.execute('UPDATE core.position_lots SET open_price=?,open_fees_eur=? WHERE open_fill_id=?',[u['price'],u['fees'],u['fill_id']])
                con.execute("UPDATE core.fill_costs SET commission_amount=3,commission_ccy='EUR',commission_eur=3 WHERE fill_id=?",[u['fill_id']])
                con.execute('UPDATE core.portfolio_snapshot SET cum_fees_eur=cum_fees_eur+? WHERE ts>=CAST(? AS TIMESTAMPTZ)',[u['fees']-u['old_fees'],u['ts']])
        assert not plan(con), 'repair must be idempotent'
        con.execute('COMMIT')
    except Exception:
        con.execute('ROLLBACK')
        raise

def main():
    p=argparse.ArgumentParser();p.add_argument('--db',required=True);p.add_argument('--apply',action='store_true');a=p.parse_args()
    con=duckdb.connect(a.db,read_only=not a.apply)
    updates=plan(con)
    print(json.dumps({'apply':a.apply,'updates':updates},indent=2))
    if a.apply: repair(con,updates)
    con.close()

if __name__=='__main__': main()
