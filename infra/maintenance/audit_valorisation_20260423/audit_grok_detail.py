#!/usr/bin/env python3
"""
Audit perf Grok — reprise phase 3 corrigée (Decimal/float safe).
"""
from __future__ import annotations
import duckdb
from pathlib import Path
from decimal import Decimal

DB = Path("/sessions/funny-elegant-dijkstra/mnt/Trader_IA/snapshots/duckdb_20260422/ag1_v3_grok41_reasoning.duckdb")

def f(x):
    if x is None:
        return 0.0
    if isinstance(x, Decimal):
        return float(x)
    return float(x)

con = duckdb.connect(str(DB), read_only=True)

print("═" * 72)
print(" AUDIT GROK — Snapshot du 21/04/2026 ")
print("═" * 72)

# === 3.0 Derniers points clés ===
print("\n── 3.0 · État dernier portfolio_snapshot")
eq_ps = con.execute("""
    SELECT run_id, ts, cash_eur, equity_eur, total_value_eur, total_pnl_eur, roi, drawdown_pct
    FROM core.portfolio_snapshot
    ORDER BY ts DESC LIMIT 1
""").fetchone()
run_id, ts, cash_eur, equity_eur, tv_eur, pnl_eur, roi, dd = eq_ps
print(f"   run_id          : {run_id}")
print(f"   ts              : {ts}")
print(f"   cash_eur        : {f(cash_eur):>12,.2f} €")
print(f"   equity_eur      : {f(equity_eur):>12,.2f} €")
print(f"   total_value_eur : {f(tv_eur):>12,.2f} €")
print(f"   total_pnl_eur   : {f(pnl_eur):>+12,.2f} €")
print(f"   roi             : {f(roi)*100:>12,.2f} %")
print(f"   drawdown_pct    : {f(dd):>12,.2f} %")

# === 3.1 Cohérence interne dernier run ===
print("\n── 3.1 · Cohérence dernier run (positions vs portfolio)")
r = con.execute("""
    SELECT
        SUM(market_value_eur)        AS sum_mv,
        SUM(unrealized_pnl_eur)      AS sum_upnl,
        COUNT(*)                     AS n_pos
    FROM core.positions_snapshot
    WHERE run_id = ?
""", [run_id]).fetchone()
sum_mv_ps, sum_upnl_ps, n_pos = f(r[0]), f(r[1]), r[2]
print(f"   nb positions (hors cash) : {n_pos}")
print(f"   Σ market_value_eur       : {sum_mv_ps:>12,.2f} €")
print(f"   Σ unrealized_pnl_eur     : {sum_upnl_ps:>+12,.2f} €")
print(f"   equity_eur (portfolio)   : {f(equity_eur):>12,.2f} €")
print(f"   Δ equity vs Σ MV         : {f(equity_eur) - sum_mv_ps:>+12,.4f} €")

# === 3.2 Comparaison positions_snapshot vs mtm_latest ===
print("\n── 3.2 · Écart prix dernier_run vs mtm_latest")
diff = con.execute("""
    WITH ps AS (
        SELECT symbol, qty, last_price AS last_ps, market_value_eur AS mv_ps
        FROM core.positions_snapshot
        WHERE run_id = ?
    ),
    ml AS (
        SELECT symbol, last_price AS last_ml, market_value AS mv_ml, mtm_status
        FROM main.portfolio_positions_mtm_latest
        WHERE mtm_status != 'TECHNICAL_ROW' OR mtm_status IS NULL
    )
    SELECT
        ps.symbol,
        ps.qty,
        ps.last_ps,
        ml.last_ml,
        ps.mv_ps,
        ml.mv_ml,
        (ml.mv_ml - ps.mv_ps) AS delta_mv
    FROM ps FULL OUTER JOIN ml USING (symbol)
    ORDER BY ABS(COALESCE(ml.mv_ml - ps.mv_ps, 0)) DESC
""", [run_id]).fetchall()

delta_total = 0.0
for row in diff[:5]:
    sym, qty, lp_ps, lp_ml, mv_ps, mv_ml, dmv = row
    delta_total_row = f(dmv)
    delta_total += delta_total_row
    print(f"   {str(sym):<10} qty={f(qty):>10,.4f} mv_ps={f(mv_ps):>10,.2f} mv_ml={f(mv_ml):>10,.2f} Δ={delta_total_row:>+8,.2f}€")
# total on ALL rows
delta_all = sum(f(row[6]) for row in diff)
print(f"   ... Σ Δ MV toutes positions : {delta_all:>+10,.2f} €")

# === 3.3 Ligne __META__ ===
print("\n── 3.3 · Lignes techniques dans mtm_latest")
meta = con.execute("""
    SELECT symbol, quantity, last_price, market_value, mtm_status, asset_class
    FROM main.portfolio_positions_mtm_latest
    WHERE mtm_status = 'TECHNICAL_ROW' OR symbol IN ('__META__', 'CASH_EUR')
    ORDER BY symbol
""").fetchall()
for m in meta:
    print(f"   {str(m[0]):<12} qty={f(m[1]):>10,.2f} last={f(m[2]):>10,.2f} mv={f(m[3]):>10,.2f} status={m[4]} class={m[5]}")

# === 3.4 Réconciliation alternative ===
print("\n── 3.4 · Réconciliation alternative (recalc TV)")
# MV des positions réelles depuis mtm_latest (hors technical rows)
r = con.execute("""
    SELECT SUM(market_value)
    FROM main.portfolio_positions_mtm_latest
    WHERE (mtm_status != 'TECHNICAL_ROW' OR mtm_status IS NULL)
      AND symbol NOT IN ('__META__', 'CASH_EUR')
""").fetchone()
mv_positions_reelles = f(r[0])

# upnl réel
r = con.execute("""
    SELECT SUM(unrealized_pnl)
    FROM main.portfolio_positions_mtm_latest
    WHERE (mtm_status != 'TECHNICAL_ROW' OR mtm_status IS NULL)
      AND symbol NOT IN ('__META__', 'CASH_EUR')
""").fetchone()
upnl_reel = f(r[0])

cash_ps = f(cash_eur)
tv_alt = mv_positions_reelles + cash_ps
pnl_alt = tv_alt - 50000
roi_alt = pnl_alt / 50000 * 100

print(f"   MV positions réelles (hors META) : {mv_positions_reelles:>12,.2f} €")
print(f"   cash_eur (dernier snapshot)      : {cash_ps:>12,.2f} €")
print(f"   TV recalculée (MV + cash)        : {tv_alt:>12,.2f} €")
print(f"   TV affichée (portfolio_snapshot) : {f(tv_eur):>12,.2f} €")
print(f"   Δ TV (recalc - affiché)          : {tv_alt - f(tv_eur):>+12,.4f} €")
print()
print(f"   PNL unrealized sur positions     : {upnl_reel:>+12,.2f} €")
print(f"   PNL total affiché                : {f(pnl_eur):>+12,.2f} €")
print(f"   Δ (unrealized - total)           : {upnl_reel - f(pnl_eur):>+12,.2f} €")
print(f"       → interpretation: {upnl_reel - f(pnl_eur):+.2f}€ de pertes/gains réalisés passés")
print()
print(f"   ROI affiché                      : {f(roi)*100:>12,.2f} %")
print(f"   ROI recalculé                    : {roi_alt:>12,.2f} %")

# === 3.5 Historique ROI et equity ===
print("\n── 3.5 · Évolution portfolio_snapshot (échantillon)")
hist = con.execute("""
    SELECT ts, cash_eur, equity_eur, total_value_eur, total_pnl_eur, roi
    FROM core.portfolio_snapshot
    ORDER BY ts DESC
    LIMIT 20
""").fetchall()
print(f"   {'ts':<22} {'cash':>10} {'equity':>10} {'TV':>10} {'PNL':>10} {'ROI%':>8}")
for h in hist:
    ts_, c_, eq_, tv_, pnl_, roi_ = h
    print(f"   {str(ts_):<22} {f(c_):>10,.0f} {f(eq_):>10,.0f} {f(tv_):>10,.0f} {f(pnl_):>+10,.0f} {f(roi_)*100:>7,.2f}%")

# === 3.6 Cash ledger ===
print("\n── 3.6 · Cash ledger — types et totaux")
types = con.execute("""
    SELECT type, COUNT(*) AS n, SUM(amount) AS total
    FROM core.cash_ledger
    GROUP BY type
    ORDER BY ABS(SUM(amount)) DESC
""").fetchall()
for t, n, tot in types:
    print(f"   {str(t):<30} n={n:>5} total={f(tot):>+14,.2f} €")

# Somme totale
r = con.execute("SELECT SUM(amount), MIN(ts), MAX(ts) FROM core.cash_ledger").fetchone()
print(f"   Σ amount total : {f(r[0]):>+14,.2f} €  (from {r[1]} to {r[2]})")

# === 3.7 Fills et realized P&L ===
print("\n── 3.7 · Fills et P&L réalisé (si dispo)")
tables = con.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema='core' AND table_name IN ('fills', 'position_lots', 'orders')
""").fetchall()
print(f"   Tables dispo : {[t[0] for t in tables]}")

try:
    cols = con.execute("DESCRIBE core.fills").fetchall()
    print(f"   fills columns: {[c[0] for c in cols]}")
    fills = con.execute("""
        SELECT COUNT(*), MIN(ts_fill), MAX(ts_fill),
               SUM(fees_eur) AS fees
        FROM core.fills
    """).fetchone()
    print(f"   fills n={fills[0]}, {fills[1]} → {fills[2]}")
    print(f"     Σ fees_eur : {f(fills[3]):>12,.2f} €")
    # Buy/sell notional
    bs = con.execute("""
        SELECT side, COUNT(*), SUM(qty * price) AS notional
        FROM core.fills
        GROUP BY side
    """).fetchall()
    for side, n, notional in bs:
        print(f"     {side}: n={n}, notional={f(notional):>12,.2f} €")
except Exception as e:
    print(f"   fills error: {e}")

# position_lots pour realized
try:
    lots = con.execute("""
        SELECT COUNT(*) AS n_lots,
               SUM(realized_pnl_eur) AS real_pnl
        FROM core.position_lots
    """).fetchone()
    print(f"   position_lots n={lots[0]}, Σ realized P&L = {f(lots[1]):>+12,.2f} €")
except Exception as e:
    print(f"   position_lots error: {e}")

# === 3.8 Recherche date V1→V2 (21/02/2026) ===
print("\n── 3.8 · Point de bascule V1→V2 (21-22/02/2026)")
pivot = con.execute("""
    SELECT ts, cash_eur, equity_eur, total_value_eur, total_pnl_eur, roi
    FROM core.portfolio_snapshot
    WHERE ts BETWEEN '2026-02-15' AND '2026-02-28'
    ORDER BY ts
    LIMIT 15
""").fetchall()
print(f"   {'ts':<22} {'cash':>10} {'equity':>10} {'TV':>10} {'PNL':>10} {'ROI%':>8}")
for p in pivot:
    ts_, c_, eq_, tv_, pnl_, roi_ = p
    print(f"   {str(ts_):<22} {f(c_):>10,.0f} {f(eq_):>10,.0f} {f(tv_):>10,.0f} {f(pnl_):>+10,.0f} {f(roi_)*100:>7,.2f}%")

# Premier snapshot ever
first = con.execute("""
    SELECT ts, cash_eur, equity_eur, total_value_eur, total_pnl_eur, roi
    FROM core.portfolio_snapshot
    ORDER BY ts ASC LIMIT 1
""").fetchone()
print(f"\n   Premier snapshot jamais enregistré:")
print(f"     ts={first[0]} cash={f(first[1]):,.0f} equity={f(first[2]):,.0f} TV={f(first[3]):,.0f} pnl={f(first[4]):+,.0f} roi={f(first[5])*100:.2f}%")

con.close()
print("\n" + "═" * 72)
print(" AUDIT GROK — terminé ")
print("═" * 72)
