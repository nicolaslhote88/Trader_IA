import duckdb
import json
import re
from datetime import datetime, timezone

ctx = (_items or [{"json": {}}])[0].get("json", {})
db_path = ctx.get("db_path") or "/files/duckdb/ag1_fx_v1_chatgpt52.duckdb"
fills = ctx.get("fills") or []
orders_by_id = {o.get("order_id"): o for o in (ctx.get("executable_orders") or [])}


def normalize_timestamp(value):
    if value is None or value == "":
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    text = str(value).strip()
    if re.match(r"^\d{8}-\d{2}:\d{2}:\d{2}$", text):
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]} {text[9:17]}"
    if "T" in text:
        text = text.replace("T", " ")
    if text.endswith("Z"):
        text = text[:-1].strip()
    if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", text):
        return text
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def ensure_fill_costs_table(con):
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS core.fill_costs (
            fill_id             VARCHAR PRIMARY KEY,
            order_id            VARCHAR NOT NULL,
            pair                VARCHAR NOT NULL,
            broker              VARCHAR,
            broker_execution_id VARCHAR,
            commission_amount   DOUBLE NOT NULL DEFAULT 0,
            commission_ccy      VARCHAR,
            commission_eur      DOUBLE NOT NULL DEFAULT 0,
            commission_source   VARCHAR,
            raw_json            VARCHAR,
            recorded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_fill_costs_order ON core.fill_costs(order_id)")


def write_fill_cost(con, fill):
    fee_amount = fill.get("fee_amount")
    if fee_amount is None:
        fee_amount = fill.get("fees_eur") or 0
    raw_json = fill.get("raw_fill_json") or ""
    if isinstance(raw_json, (dict, list)):
        raw_json = json.dumps(raw_json, ensure_ascii=False)
    con.execute(
        """
        INSERT OR REPLACE INTO core.fill_costs (
          fill_id, order_id, pair, broker, broker_execution_id,
          commission_amount, commission_ccy, commission_eur,
          commission_source, raw_json, recorded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        [
            fill.get("fill_id"),
            fill.get("order_id"),
            fill.get("pair"),
            fill.get("broker") or ("IBKR" if str(fill.get("fill_source") or "").startswith("ibkr_") else "SIM"),
            fill.get("broker_execution_id") or "",
            fee_amount,
            fill.get("fee_ccy") or ("EUR" if fee_amount else ""),
            fill.get("fees_eur") or 0,
            fill.get("fee_source") or fill.get("fill_source") or "",
            raw_json,
        ],
    )


opened = 0
with duckdb.connect(db_path) as con:
    ensure_fill_costs_table(con)
    for f in fills:
        filled_at = normalize_timestamp(f.get("filled_at"))
        f["filled_at"] = filled_at
        con.execute(
            "INSERT OR REPLACE INTO core.fills VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                f.get("fill_id"), f.get("order_id"), f.get("pair"), f.get("side"),
                f.get("fill_price"), f.get("fill_size_lots"), f.get("fees_eur"),
                f.get("swap_eur"), filled_at, f.get("fill_source"),
            ],
        )
        write_fill_cost(con, f)
        con.execute("UPDATE core.orders SET status='filled' WHERE order_id=?", [f.get("order_id")])
        order = orders_by_id.get(f.get("order_id")) or {}
        if f.get("is_currency_conversion") or order.get("is_currency_conversion") or str(order.get("order_type") or "").lower() == "cash_conversion":
            continue
        if f.get("side") in {"buy_base", "sell_base"}:
            side = "long" if f.get("side") == "buy_base" else "short"
            con.execute(
                """
                INSERT OR REPLACE INTO core.position_lots (
                  lot_id, run_id_open, run_id_close, pair, side, size_lots, open_price, open_at,
                  close_price, close_at, pnl_quote, pnl_eur, fees_eur, swap_eur_total,
                  leverage_used, stop_loss_price, take_profit_price, status, notes
                ) VALUES (?, ?, NULL, ?, ?, ?, ?, CAST(? AS TIMESTAMP), NULL, NULL, NULL, NULL, ?, 0, ?, ?, ?, 'open', ?)
                """,
                [
                    f"LOT_{f.get('fill_id')}", ctx.get("run_id"), f.get("pair"), side,
                    f.get("fill_size_lots"), f.get("fill_price"), filled_at, f.get("fees_eur"),
                    order.get("leverage_used") or 1, order.get("stop_loss_price"), order.get("take_profit_price"),
                    order.get("risk_check_notes") or "",
                ],
            )
            opened += 1

return [{"json": {**ctx, "fills_written": len(fills), "lots_opened": opened}}]
