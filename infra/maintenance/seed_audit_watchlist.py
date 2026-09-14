#!/usr/bin/env python3
"""Admit explicitly requested equities to the normal WATCHLIST after identity checks.
No ranking from subsequent performance; no orders; existing segments are preserved.
"""
import argparse
import json
from pathlib import Path
from urllib.request import urlopen
from urllib.parse import urlencode
import duckdb


def get(base, route, **params):
    with urlopen(base.rstrip("/")+route+"?"+urlencode(params),timeout=40) as response:
        return json.load(response)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db",type=Path,required=True)
    parser.add_argument("--symbols",nargs="+",required=True)
    parser.add_argument("--broker",default="http://ibkr-broker:8080")
    parser.add_argument("--yfinance",default="http://yfinance-api:8080")
    parser.add_argument("--apply",action="store_true")
    args=parser.parse_args()
    validated=[]
    for symbol in args.symbols:
        symbol=symbol.upper().strip()
        info=get(args.yfinance,"/info",symbol=symbol)
        contracts=get(args.broker,"/contracts/equity/resolve",symbols=symbol)
        matches=[r for r in contracts.get("results",[]) if r.get("symbol")==symbol]
        if not info.get("ok") or info.get("quoteType")!="EQUITY" or len(matches)!=1:
            raise RuntimeError("UNVERIFIED_EQUITY_IDENTITY:"+symbol)
        contract=matches[0];quote=info.get("quote") or {}
        if contract.get("currency")!="USD" or quote.get("currency")!="USD" or not contract.get("conid"):
            raise RuntimeError("UNVERIFIED_USD_CONTRACT:"+symbol)
        validated.append((symbol,info,contract))
    with duckdb.connect(str(args.db),read_only=not args.apply) as con:
        if args.apply:
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute("CREATE TABLE IF NOT EXISTS universe_backup_pc20260914 AS SELECT * FROM universe")
                con.execute("CREATE TABLE IF NOT EXISTS universe_segments_backup_pc20260914 AS SELECT * FROM universe_segments")
                for symbol,info,contract in validated:
                    con.execute("INSERT INTO universe (symbol,symbol_yahoo,name,asset_class,exchange,currency,country,sector,industry,isin,enabled,updated_at) VALUES (?,?,?,'EQUITY',?,'USD',?,?,?,?,TRUE,CURRENT_TIMESTAMP) ON CONFLICT(symbol) DO NOTHING",[symbol,symbol,info.get("shortName"),contract.get("exchange"),info.get("country"),info.get("sector"),info.get("industry"),info.get("isin") or None])
                    if not con.execute("SELECT 1 FROM universe_segments WHERE symbol=? AND active",[symbol]).fetchone():
                        con.execute("INSERT INTO universe_segments VALUES (?,'WATCHLIST',TRUE,0,'performance_audit','Explicit coverage request; normal rotation, no performance-based promotion',?,CURRENT_TIMESTAMP) ON CONFLICT(symbol,segment) DO NOTHING",[symbol,json.dumps({"contract_conid":contract["conid"],"verified_source":"IBKR contract and Yahoo metadata"})])
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK");raise
        print(json.dumps({"applied":args.apply,"symbols":[{"symbol":s,"contract_resolved":True,"segments":[r[0] for r in con.execute("SELECT segment FROM universe_segments WHERE symbol=? AND active",[s]).fetchall()]} for s,_,_ in validated]},indent=2))

if __name__=="__main__":main()
