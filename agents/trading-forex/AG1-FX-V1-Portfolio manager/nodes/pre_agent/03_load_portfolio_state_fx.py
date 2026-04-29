import os
import duckdb
from pathlib import Path

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("db_path") or "/files/duckdb/ag1_fx_v1_chatgpt52.duckdb"
schema_path = ctx.get("schema_path") or "/files/AG1-FX-V1-EXPORT/sql/ag1_fx_v1_schema.sql"

def split_sql(text):
    buff, out, sq, dq = [], [], False, False
    for ch in text:
        if ch == "'" and not dq:
            sq = not sq
        elif ch == '"' and not sq:
            dq = not dq
        if ch == ";" and not sq and not dq:
            s = "".join(buff).strip()
            if s:
                out.append(s)
            buff = []
        else:
            buff.append(ch)
    s = "".join(buff).strip()
    if s:
        out.append(s)
    return out

with duckdb.connect(db_path) as con:
    if os.path.exists(schema_path):
        for stmt in split_sql(Path(schema_path).read_text(encoding="utf-8")):
            con.execute(stmt)
    cfg = con.execute("SELECT * FROM cfg.portfolio_config WHERE config_key='default'").fetchdf().iloc[0].to_dict()
    cash = con.execute("SELECT COALESCE(SUM(amount_eur), 0) FROM core.cash_ledger").fetchone()[0] or 0.0
    lots = con.execute(
        """
        SELECT lot_id, pair, side, size_lots, open_price, open_at, stop_loss_price, take_profit_price, leverage_used
        FROM core.position_lots
        WHERE status = 'open'
        ORDER BY open_at
        """
    ).fetchdf().to_dict("records")
    snap = con.execute(
        """
        SELECT *
        FROM core.portfolio_snapshot
        ORDER BY as_of DESC
        LIMIT 1
        """
    ).fetchdf()

initial = float(cfg.get("initial_capital_eur") or 10000)
equity = float(snap.iloc[0]["equity_eur"]) if not snap.empty else float(cash or initial)
state = {
    "cash_eur": float(cash or initial),
    "equity_eur": equity,
    "open_lots": lots,
    "leverage_effective": float(snap.iloc[0]["leverage_effective"]) if not snap.empty and snap.iloc[0]["leverage_effective"] is not None else 0.0,
    "drawdown_day_pct": float(snap.iloc[0]["drawdown_day_pct"]) if not snap.empty and snap.iloc[0]["drawdown_day_pct"] is not None else 0.0,
    "drawdown_total_pct": float(snap.iloc[0]["drawdown_total_pct"]) if not snap.empty and snap.iloc[0]["drawdown_total_pct"] is not None else 0.0,
}

cfg["llm_model"] = ctx.get("llm_model") or cfg.get("llm_model") or "unset"
with duckdb.connect(db_path) as con:
    con.execute(
        "UPDATE cfg.portfolio_config SET llm_model = ?, updated_at = CURRENT_TIMESTAMP WHERE config_key = 'default'",
        [cfg["llm_model"]],
    )

return [{"json": {**ctx, "config": cfg, "portfolio_state": state}}]
