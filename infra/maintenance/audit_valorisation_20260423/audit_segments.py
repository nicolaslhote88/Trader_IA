#!/usr/bin/env python3
"""
Audit P&L / activité par segment de marché (Equity_EU, Equity_US, FX) × IA.

Segmentation par symbole :
  - FX:*                      → Forex
  - *=X                       → Forex (Yahoo)
  - *.PA / .DE / .MI / .AS    → Equity_EU
  - ticker sans suffixe court (AAPL, NVDA…) → Equity_US
"""
from __future__ import annotations
import duckdb
from pathlib import Path

def f(x): return 0.0 if x is None else float(x)

DBs = {
    "ChatGPT": "/sessions/funny-elegant-dijkstra/mnt/Trader_IA/snapshots/duckdb_20260422/ag1_v3_chatgpt52.duckdb",
    "Gemini":  "/sessions/funny-elegant-dijkstra/mnt/Trader_IA/snapshots/duckdb_20260422/ag1_v3_gemini30_pro.duckdb",
    "Grok":    "/sessions/funny-elegant-dijkstra/mnt/Trader_IA/snapshots/duckdb_20260422/ag1_v3_grok41_reasoning.duckdb",
}

SQL_CLASSIFY = """
    CASE
        WHEN symbol LIKE 'FX:%'   THEN 'Forex'
        WHEN symbol LIKE '%=X'    THEN 'Forex'
        WHEN symbol LIKE '%.PA'   THEN 'Equity_EU'
        WHEN symbol LIKE '%.DE'   THEN 'Equity_EU'
        WHEN symbol LIKE '%.MI'   THEN 'Equity_EU'
        WHEN symbol LIKE '%.AS'   THEN 'Equity_EU'
        WHEN symbol LIKE '%.L'    THEN 'Equity_EU'
        WHEN symbol LIKE '%.SW'   THEN 'Equity_EU'
        WHEN symbol IN ('__META__', 'CASH_EUR') THEN 'Technical'
        WHEN symbol ~ '^[A-Z]{1,5}$' THEN 'Equity_US'
        ELSE 'Other'
    END
"""

def audit(name: str, path: str) -> dict:
    con = duckdb.connect(path, read_only=True)
    out = {"name": name}

    # 1. Positions ouvertes actuelles (dernier run) par segment
    last_run = con.execute("SELECT run_id FROM core.portfolio_snapshot ORDER BY ts DESC LIMIT 1").fetchone()[0]
    open_pos = con.execute(f"""
        SELECT {SQL_CLASSIFY} AS seg,
               COUNT(*) n,
               SUM(qty*avg_cost) cost_basis,
               SUM(market_value_eur) mv,
               SUM(unrealized_pnl_eur) upnl
        FROM core.positions_snapshot
        WHERE run_id = ?
        GROUP BY seg ORDER BY seg
    """, [last_run]).fetchall()
    out["open_pos"] = {seg: {"n": n, "cost": f(c), "mv": f(mv), "upnl": f(u)}
                       for seg, n, c, mv, u in open_pos}

    # 2. Fills par segment (activité historique)
    fills = con.execute(f"""
        SELECT {SQL_CLASSIFY.replace('symbol', 'o.symbol')} AS seg,
               o.side,
               COUNT(*) n,
               SUM(f.qty * f.price) notional,
               COUNT(DISTINCT o.symbol) n_sym
        FROM core.fills f
        JOIN core.orders o ON f.order_id = o.order_id
        GROUP BY seg, o.side ORDER BY seg, o.side
    """).fetchall()
    fills_data = {}
    for seg, side, n, notional, nsym in fills:
        fills_data.setdefault(seg, {"BUY": (0, 0, 0), "SELL": (0, 0, 0)})
        fills_data[seg][side] = (n, f(notional), nsym)
    out["fills"] = fills_data

    # 3. P&L réalisé par segment (lots fermés)
    lots_closed = con.execute(f"""
        SELECT {SQL_CLASSIFY} AS seg,
               COUNT(*) n_closed,
               SUM(realized_pnl_eur) realized,
               SUM(CASE WHEN realized_pnl_eur > 0 THEN 1 ELSE 0 END) wins,
               SUM(CASE WHEN realized_pnl_eur < 0 THEN 1 ELSE 0 END) losses,
               AVG(realized_pnl_eur) avg_pnl,
               MAX(realized_pnl_eur) best,
               MIN(realized_pnl_eur) worst
        FROM core.position_lots
        WHERE status='CLOSED'
        GROUP BY seg ORDER BY seg
    """).fetchall()
    out["lots"] = {seg: {"n": n, "realized": f(r), "wins": w, "losses": l,
                          "avg": f(a), "best": f(b), "worst": f(wo)}
                    for seg, n, r, w, l, a, b, wo in lots_closed}

    # 4. Lots OPEN actuels par segment
    lots_open = con.execute(f"""
        SELECT {SQL_CLASSIFY} AS seg,
               COUNT(*) n_open,
               SUM(remaining_qty * open_price) cost_open
        FROM core.position_lots WHERE status='OPEN'
        GROUP BY seg ORDER BY seg
    """).fetchall()
    out["lots_open"] = {seg: {"n": n, "cost_open": f(c)} for seg, n, c in lots_open}

    # 5. Durée moyenne de détention (lots fermés V2)
    durations = con.execute(f"""
        SELECT {SQL_CLASSIFY} AS seg,
               AVG(DATE_DIFF('day', open_ts, close_ts)) avg_days,
               MIN(DATE_DIFF('day', open_ts, close_ts)) min_days,
               MAX(DATE_DIFF('day', open_ts, close_ts)) max_days
        FROM core.position_lots
        WHERE status='CLOSED' AND close_ts >= '2026-02-22'
        GROUP BY seg
    """).fetchall()
    out["durations"] = {seg: {"avg": f(a), "min": f(mn), "max": f(mx)}
                         for seg, a, mn, mx in durations}

    con.close()
    return out


def print_report(a: dict) -> None:
    n = a["name"]
    print(f"\n{'═'*72}")
    print(f" {n}")
    print(f"{'═'*72}")

    # Positions ouvertes
    print(f"\n  ── Positions ouvertes par segment (dernier run)")
    print(f"     {'segment':<14} {'n':>3} {'cost':>10} {'MV':>10} {'upnl':>10}  {'upnl%':>7}")
    for seg, d in a["open_pos"].items():
        pct = (d["upnl"] / d["cost"] * 100) if d["cost"] else 0
        print(f"     {seg:<14} {d['n']:>3} {d['cost']:>10,.0f} {d['mv']:>10,.0f} {d['upnl']:>+10,.0f}  {pct:>+6.2f}%")

    # Lots fermés (realized)
    print(f"\n  ── P&L réalisé par segment (lots CLOSED)")
    print(f"     {'segment':<14} {'n':>3} {'realized':>10} {'wins':>5} {'losses':>6} {'winrate':>7} {'avg':>8} {'best':>8} {'worst':>8}")
    for seg, d in a["lots"].items():
        total = d["wins"] + d["losses"]
        wr = (d["wins"] / total * 100) if total else 0
        print(f"     {seg:<14} {d['n']:>3} {d['realized']:>+10,.0f} {d['wins']:>5} {d['losses']:>6} {wr:>6.1f}% {d['avg']:>+8,.1f} {d['best']:>+8,.0f} {d['worst']:>+8,.0f}")

    # Activité fills
    print(f"\n  ── Activité (fills)")
    print(f"     {'segment':<14} {'BUY_n':>6} {'BUY_not':>10} {'SELL_n':>7} {'SELL_not':>10} {'n_sym':>6}")
    for seg, d in a["fills"].items():
        bn, bnot, bns = d["BUY"]
        sn, snot, sns = d["SELL"]
        total_sym = max(bns, sns)
        print(f"     {seg:<14} {bn:>6} {bnot:>10,.0f} {sn:>7} {snot:>10,.0f} {total_sym:>6}")

    # Durées moyennes
    if a["durations"]:
        print(f"\n  ── Durée de détention (lots fermés V2, jours)")
        for seg, d in a["durations"].items():
            print(f"     {seg:<14} avg={d['avg']:>5.1f}  min={d['min']:.0f}  max={d['max']:.0f}")


if __name__ == "__main__":
    print("═"*72)
    print(" AUDIT PAR SEGMENT DE MARCHÉ — 3 IA · Snapshot 21/04/2026 ")
    print("═"*72)
    results = {}
    for name, path in DBs.items():
        results[name] = audit(name, path)
        print_report(results[name])

    # Synthèse croisée
    print(f"\n\n{'═'*72}")
    print(" SYNTHÈSE CROISÉE · qui est meilleur sur quoi ? ")
    print(f"{'═'*72}")

    segments = ["Equity_EU", "Equity_US", "Forex"]
    for seg in segments:
        print(f"\n  ▎ Segment : {seg}")
        print(f"     {'IA':<10} {'upnl_now':>10} {'cost_open':>10} {'realized':>10} {'win':>4} {'loss':>5} {'wr':>5} {'n_fills_BUY':>12} {'n_fills_SELL':>13}")
        for name in ["ChatGPT", "Gemini", "Grok"]:
            a = results[name]
            op = a["open_pos"].get(seg, {"n": 0, "cost": 0, "mv": 0, "upnl": 0})
            lo = a["lots"].get(seg, {"n": 0, "realized": 0, "wins": 0, "losses": 0})
            fl = a["fills"].get(seg, {"BUY": (0, 0, 0), "SELL": (0, 0, 0)})
            tot = lo["wins"] + lo["losses"]
            wr = (lo["wins"] / tot * 100) if tot else 0
            print(f"     {name:<10} {op['upnl']:>+10,.0f} {op['cost']:>10,.0f} {lo['realized']:>+10,.0f} {lo['wins']:>4} {lo['losses']:>5} {wr:>4.0f}% {fl['BUY'][0]:>12} {fl['SELL'][0]:>13}")
