#!/usr/bin/env python3
"""
Audit perf trading par LLM — reconstitution P&L / drawdown / win rate depuis AG1 snapshots.

Méthodo :
- Equity curve = SUM(market_value) par run_id (inclut Meta 50k, Cash, positions)
- Unrealized P&L = directement depuis colonne
- Realized P&L = reconstruit depuis les exits (symboles présents dans history, absents de latest)
    -> realized = (last_last_price - avg_price) * last_quantity
- Win rate = (closed_winners) / (closed_total)
- Max drawdown = (equity[t] - peak[..t]) / peak[..t] min
- Turnover = closed_positions / total_unique_symbols
"""
import duckdb
import pandas as pd
import numpy as np
import json
from pathlib import Path

OUT_DIR = Path('/sessions/funny-elegant-dijkstra/mnt/Trader_IA/docs/audits/20260422_perf_trading_par_llm')
OUT_DIR.mkdir(parents=True, exist_ok=True)

DBS = {
    'ChatGPT 5.2':       '/sessions/funny-elegant-dijkstra/mnt/Trader_IA/snapshots/duckdb_20260422/ag1_v3_chatgpt52.duckdb',
    'Gemini 3.0 Pro':    '/sessions/funny-elegant-dijkstra/mnt/Trader_IA/snapshots/duckdb_20260422/ag1_v3_gemini30_pro.duckdb',
    'Grok 4.1 Reasoning':'/sessions/funny-elegant-dijkstra/mnt/Trader_IA/snapshots/duckdb_20260422/ag1_v3_grok41_reasoning.duckdb',
}

def analyse_llm(name, path):
    con = duckdb.connect(path, read_only=True)

    # Sandbox réelle = runs avec SUM(market_value) > 80k ET après 2026-03-04 21:00
    # (phase amorce 23/02→02/03 et run reset 04/03 à 0 filtrés)
    SANDBOX_START = '2026-03-04 21:00:00'

    # 1. Equity curve par run_id (aggregation sum des market_value)
    equity = con.execute(f"""
        SELECT run_id,
               MAX(updated_at) AS ts,
               SUM(market_value) AS equity,
               SUM(unrealized_pnl) AS unrealized_pnl,
               COUNT(*) AS positions_count
        FROM portfolio_positions_mtm_history
        GROUP BY run_id
        HAVING SUM(market_value) > 80000
           AND MAX(updated_at) >= TIMESTAMP '{SANDBOX_START}'
        ORDER BY ts
    """).fetchdf()
    equity['ts'] = pd.to_datetime(equity['ts'])

    # Normaliser : starting equity = 1ère valeur (post-filtre)
    starting_equity = equity['equity'].iloc[0]
    equity['equity_pct'] = (equity['equity'] / starting_equity - 1) * 100

    # Running peak + drawdown
    equity['peak'] = equity['equity'].cummax()
    equity['drawdown_pct'] = (equity['equity'] / equity['peak'] - 1) * 100
    max_dd = equity['drawdown_pct'].min()
    max_dd_date = equity.loc[equity['drawdown_pct'].idxmin(), 'ts']

    # Période effective (>2 positions, exclut l'amorce)
    period_start = equity['ts'].min()
    period_end = equity['ts'].max()

    # 2. Positions actuelles
    latest = con.execute("SELECT * FROM portfolio_positions_mtm_latest").fetchdf()
    current_positions = len(latest)
    current_unrealized = latest['unrealized_pnl'].sum()
    current_equity = latest['market_value'].sum()

    # 3. Realized P&L — reconstruit via exits (trades sur sandbox uniquement)
    history = con.execute(f"""
        SELECT symbol, run_id, updated_at, quantity, avg_price, last_price, market_value, unrealized_pnl, asset_class
        FROM portfolio_positions_mtm_history
        WHERE symbol != '' AND asset_class NOT IN ('Meta', 'Cash')
          AND updated_at >= TIMESTAMP '{SANDBOX_START}'
        ORDER BY symbol, updated_at
    """).fetchdf()
    history['updated_at'] = pd.to_datetime(history['updated_at'])

    current_symbols = set(latest['symbol'].unique())
    all_symbols = set(history['symbol'].unique())
    closed_symbols = all_symbols - current_symbols

    # Pour chaque symbole clôturé, prendre la dernière ligne
    closed_trades = []
    for sym in closed_symbols:
        sub = history[history['symbol'] == sym].sort_values('updated_at')
        if len(sub) < 1:
            continue
        first = sub.iloc[0]
        last = sub.iloc[-1]
        # P&L réalisé estimé sur la dernière ligne avant exit
        realized = (last['last_price'] - last['avg_price']) * last['quantity']
        holding_days = (last['updated_at'] - first['updated_at']).total_seconds() / 86400
        closed_trades.append({
            'symbol': sym,
            'opened_at': first['updated_at'],
            'closed_at': last['updated_at'],
            'holding_days': holding_days,
            'quantity': last['quantity'],
            'avg_price': last['avg_price'],
            'last_price': last['last_price'],
            'realized_pnl': realized,
            'return_pct': (last['last_price'] / last['avg_price'] - 1) * 100 if last['avg_price'] > 0 else 0,
            'asset_class': last['asset_class'],
        })
    closed_df = pd.DataFrame(closed_trades)

    if len(closed_df) > 0:
        total_realized = closed_df['realized_pnl'].sum()
        winners = closed_df[closed_df['realized_pnl'] > 0]
        losers = closed_df[closed_df['realized_pnl'] < 0]
        win_rate = len(winners) / len(closed_df) * 100
        avg_winner = winners['realized_pnl'].mean() if len(winners) > 0 else 0
        avg_loser = losers['realized_pnl'].mean() if len(losers) > 0 else 0
        profit_factor = (winners['realized_pnl'].sum() / abs(losers['realized_pnl'].sum())) if len(losers) > 0 and losers['realized_pnl'].sum() != 0 else float('inf')
        avg_holding_days = closed_df['holding_days'].mean()
        best_trade = closed_df.nlargest(1, 'realized_pnl').iloc[0]
        worst_trade = closed_df.nsmallest(1, 'realized_pnl').iloc[0]
    else:
        total_realized, win_rate, avg_winner, avg_loser, profit_factor = 0, 0, 0, 0, 0
        avg_holding_days = 0
        best_trade, worst_trade = None, None

    # 4. Sector distribution (current)
    sectors = latest.groupby('sector').agg(
        count=('symbol', 'count'),
        market_value=('market_value', 'sum')
    ).reset_index().sort_values('market_value', ascending=False)

    # 5. P&L total = realized + unrealized
    total_pnl = total_realized + current_unrealized

    con.close()

    return {
        'name': name,
        'period_start': period_start,
        'period_end': period_end,
        'starting_equity': float(starting_equity),
        'current_equity': float(current_equity),
        'unrealized_pnl': float(current_unrealized),
        'realized_pnl': float(total_realized),
        'total_pnl': float(total_pnl),
        'total_return_pct': float((current_equity / starting_equity - 1) * 100),
        'max_drawdown_pct': float(max_dd),
        'max_dd_date': max_dd_date,
        'current_positions': int(current_positions),
        'unique_symbols': int(len(all_symbols)),
        'closed_trades': int(len(closed_df)),
        'win_rate': float(win_rate),
        'avg_winner': float(avg_winner) if avg_winner else 0,
        'avg_loser': float(avg_loser) if avg_loser else 0,
        'profit_factor': float(profit_factor) if profit_factor != float('inf') else None,
        'avg_holding_days': float(avg_holding_days),
        'best_trade': best_trade.to_dict() if best_trade is not None else None,
        'worst_trade': worst_trade.to_dict() if worst_trade is not None else None,
        'equity_curve': equity[['ts', 'equity', 'equity_pct', 'drawdown_pct']].to_dict('records'),
        'sectors': sectors.to_dict('records'),
        'closed_trades_df': closed_df.to_dict('records'),
        'latest_positions': latest[['symbol', 'name', 'sector', 'asset_class', 'quantity', 'avg_price', 'last_price', 'market_value', 'unrealized_pnl']].to_dict('records'),
    }

results = {name: analyse_llm(name, path) for name, path in DBS.items()}

# Save results pour génération report/dashboard
def json_default(o):
    if isinstance(o, (pd.Timestamp, np.datetime64)):
        return pd.Timestamp(o).isoformat()
    if isinstance(o, (np.integer, np.floating)):
        return float(o)
    if pd.isna(o):
        return None
    return str(o)

with open(OUT_DIR / 'metrics.json', 'w') as f:
    json.dump(results, f, default=json_default, indent=2)

# Impression résumé console
print("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
print(f"{'LLM':<22} {'Equity':>10} {'P&L tot':>10} {'Return':>8} {'MaxDD':>8} {'WinRate':>8} {'Trades':>7}")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
for name, r in results.items():
    print(f"{name:<22} {r['current_equity']:>10,.0f} {r['total_pnl']:>+10,.0f} {r['total_return_pct']:>+7.2f}% {r['max_drawdown_pct']:>+7.2f}% {r['win_rate']:>7.1f}% {r['closed_trades']:>7}")
print()
print(f"Output JSON: {OUT_DIR / 'metrics.json'}")
