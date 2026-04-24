#!/usr/bin/env python3
"""
Audit comparatif des 3 IA (Grok, ChatGPT, Gemini).
Calcule les mêmes indicateurs pour chacune sur le snapshot 21/04.
"""
from __future__ import annotations
import duckdb
from pathlib import Path
from decimal import Decimal

def f(x): return 0.0 if x is None else float(x)

DBs = {
    "Grok":    "/sessions/funny-elegant-dijkstra/mnt/Trader_IA/snapshots/duckdb_20260422/ag1_v3_grok41_reasoning.duckdb",
    "ChatGPT": "/sessions/funny-elegant-dijkstra/mnt/Trader_IA/snapshots/duckdb_20260422/ag1_v3_chatgpt52.duckdb",
    "Gemini":  "/sessions/funny-elegant-dijkstra/mnt/Trader_IA/snapshots/duckdb_20260422/ag1_v3_gemini30_pro.duckdb",
}

def audit(name: str, dbpath: str) -> dict:
    con = duckdb.connect(dbpath, read_only=True)
    out = {"name": name}
    try:
        # Dernier snapshot
        lr = con.execute("""
            SELECT run_id, ts, cash_eur, equity_eur, total_value_eur,
                   total_pnl_eur, roi, drawdown_pct
            FROM core.portfolio_snapshot ORDER BY ts DESC LIMIT 1
        """).fetchone()
        out["run_id"], out["ts"], out["cash"], out["equity"], out["tv"], \
            out["pnl"], out["roi"], out["dd"] = lr

        # Cohérence positions
        r = con.execute("""
            SELECT COUNT(*), SUM(market_value_eur), SUM(unrealized_pnl_eur),
                   SUM(qty*avg_cost)
            FROM core.positions_snapshot WHERE run_id=?
        """, [lr[0]]).fetchone()
        out["n_pos"], out["sum_mv"], out["sum_upnl"], out["cost_basis"] = r

        # Amplitude 30 j
        amp = con.execute("""
            SELECT MIN(equity_eur), MAX(equity_eur),
                   MIN(total_value_eur), MAX(total_value_eur),
                   MIN(total_pnl_eur), MAX(total_pnl_eur),
                   COUNT(*)
            FROM core.portfolio_snapshot
            WHERE ts >= (SELECT MAX(ts)-INTERVAL '30 days' FROM core.portfolio_snapshot)
        """).fetchone()
        out["eq_min"], out["eq_max"], out["tv_min"], out["tv_max"], \
            out["pnl_min"], out["pnl_max"], out["n_snap_30j"] = amp

        # Fills & notional V1/V2
        fills_all = con.execute("""
            SELECT o.side,
                   CASE WHEN f.ts_fill < '2026-02-22' THEN 'V1' ELSE 'V2' END era,
                   COUNT(*), SUM(f.qty*f.price)
            FROM core.fills f LEFT JOIN core.orders o ON f.order_id=o.order_id
            GROUP BY o.side, era
        """).fetchall()
        out["fills_detail"] = {(s, e): (n, f(v)) for s, e, n, v in fills_all}

        # position_lots réalisé
        lots = con.execute("""
            SELECT status, COUNT(*), SUM(realized_pnl_eur)
            FROM core.position_lots GROUP BY status
        """).fetchall()
        out["lots"] = {s: (n, f(v)) for s, n, v in lots}

        # Realized par période
        lots_period = con.execute("""
            SELECT
                CASE WHEN close_ts < '2026-02-22' THEN 'V1'
                     WHEN close_ts >= '2026-02-22' THEN 'V2'
                     ELSE 'OPEN' END AS p,
                COUNT(*), SUM(realized_pnl_eur)
            FROM core.position_lots GROUP BY p
        """).fetchall()
        out["lots_period"] = {p: (n, f(v)) for p, n, v in lots_period}

        # Bascule V1→V2 (premier snapshot après 22/02)
        piv = con.execute("""
            SELECT ts, cash_eur, equity_eur, total_value_eur, total_pnl_eur, roi
            FROM core.portfolio_snapshot
            WHERE ts >= '2026-02-22' ORDER BY ts ASC LIMIT 1
        """).fetchone()
        out["pivot"] = piv

        # Valeurs distinctes de ROI
        ndr = con.execute("SELECT COUNT(DISTINCT roi) FROM core.portfolio_snapshot").fetchone()[0]
        nt = con.execute("SELECT COUNT(*) FROM core.portfolio_snapshot").fetchone()[0]
        out["n_distinct_roi"] = ndr
        out["n_total_snap"] = nt

        # Cash ledger cover
        cl = con.execute("""
            SELECT COUNT(*), MIN(ts), MAX(ts), SUM(amount)
            FROM core.cash_ledger
        """).fetchone()
        out["cash_ledger"] = cl

        # Fees fills
        fees = con.execute("SELECT SUM(fees_eur) FROM core.fills").fetchone()[0]
        out["fees"] = f(fees)

    finally:
        con.close()
    return out


def print_audit(a: dict) -> None:
    n = a["name"]
    print(f"\n{'─'*72}")
    print(f" {n} — {a['ts']}")
    print(f"{'─'*72}")
    print(f"  Cash           : {f(a['cash']):>+12,.2f} €")
    print(f"  Equity (Σ MV)  : {f(a['equity']):>+12,.2f} €")
    print(f"  Total Value    : {f(a['tv']):>+12,.2f} €")
    print(f"  Total PnL      : {f(a['pnl']):>+12,.2f} €   (ROI : {f(a['roi'])*100:.2f} %)")
    print(f"  Drawdown       : {f(a['dd']):>+12.2f} %")
    print(f"  Positions      : n={a['n_pos']}  cost={f(a['cost_basis']):>,.0f} €  upnl={f(a['sum_upnl']):>+,.0f} €")
    print(f"  Amplitude 30j  : TV [{f(a['tv_min']):,.0f} — {f(a['tv_max']):,.0f}]  Δ={f(a['tv_max'])-f(a['tv_min']):,.0f} €")
    print(f"                   EQ [{f(a['eq_min']):,.0f} — {f(a['eq_max']):,.0f}]  Δ={f(a['eq_max'])-f(a['eq_min']):,.0f} €")
    print(f"                   PNL [{f(a['pnl_min']):+,.0f} — {f(a['pnl_max']):+,.0f}]  Δ={f(a['pnl_max'])-f(a['pnl_min']):,.0f} €")
    print(f"  Nb snapshots total   : {a['n_total_snap']} (ROI distincts : {a['n_distinct_roi']})")

    if a.get("pivot"):
        pts, pc, pe, ptv, pp, pr = a["pivot"]
        print(f"  Bascule V1→V2   : {pts}  TV={f(ptv):,.0f}  PnL={f(pp):+,.0f}  ROI={f(pr)*100:.2f}%")
        net_v2 = f(a['pnl']) - f(pp)
        print(f"  Perf NETTE V2   : {net_v2:>+12,.2f} €  ({net_v2/50000*100:+.2f}% sur 2 mois)")

    print(f"  Fills (BUY)     : {a['fills_detail'].get(('BUY','V1'),(0,0))[0]:>3} V1 + {a['fills_detail'].get(('BUY','V2'),(0,0))[0]:>3} V2")
    print(f"  Fills (SELL)    : {a['fills_detail'].get(('SELL','V1'),(0,0))[0]:>3} V1 + {a['fills_detail'].get(('SELL','V2'),(0,0))[0]:>3} V2")
    print(f"  Σ fees          : {f(a['fees']):,.2f} €")

    print(f"  Lots CLOSED     : n={a['lots'].get('CLOSED',(0,0))[0]}  Σ realized={a['lots'].get('CLOSED',(0,0))[1]:>+10,.2f} €")
    print(f"  Lots OPEN       : n={a['lots'].get('OPEN',(0,0))[0]}")
    lp = a.get("lots_period", {})
    print(f"     fermés V1    : n={lp.get('V1',(0,0))[0]}  {lp.get('V1',(0,0))[1]:>+10,.2f} €")
    print(f"     fermés V2    : n={lp.get('V2',(0,0))[0]}  {lp.get('V2',(0,0))[1]:>+10,.2f} €")

    # Math balance check
    balance = 50000 - f(a["cost_basis"]) - f(a["cash"])
    real_per_lots = a["lots"].get("CLOSED", (0, 0))[1]
    print(f"  MATH — realized implicite (50000 - cost - cash) : {balance:>+10,.2f} €")
    print(f"  Ecart vs position_lots.realized                : {balance - real_per_lots:>+10,.2f} €")

    # Cash ledger
    cl = a["cash_ledger"]
    print(f"  Cash ledger     : n={cl[0]}  [{cl[1]} → {cl[2]}]  Σ={f(cl[3]):+,.0f} €")


if __name__ == "__main__":
    print("═"*72)
    print(" AUDIT COMPARATIF 3 IA — Snapshot 21/04/2026 ")
    print("═"*72)
    results = {}
    for name, path in DBs.items():
        try:
            results[name] = audit(name, path)
            print_audit(results[name])
        except Exception as e:
            print(f"\n ERREUR sur {name}: {e}")

    # Tableau de synthèse final
    print("\n" + "═"*72)
    print(" SYNTHÈSE ")
    print("═"*72)
    print(f"  {'IA':<10} {'TV':>12} {'PnL':>10} {'ROI':>8} {'V2 net':>10} {'unrealized':>12}")
    for n in ["ChatGPT", "Gemini", "Grok"]:
        if n not in results: continue
        a = results[n]
        net_v2 = f(a['pnl']) - f(a['pivot'][4]) if a.get('pivot') else 0
        print(f"  {n:<10} {f(a['tv']):>12,.0f} {f(a['pnl']):>+10,.0f} {f(a['roi'])*100:>7.2f}% {net_v2:>+10,.0f} {f(a['sum_upnl']):>+12,.0f}")
