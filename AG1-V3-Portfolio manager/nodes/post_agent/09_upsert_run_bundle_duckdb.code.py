import json
import duckdb

INITIAL_CAPITAL_DEFAULT = 50000.0
EPS = 1e-9


def _safe_float(v, default=0.0):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(v, default=None):
    try:
        if v is None:
            return default
        return int(round(float(v)))
    except Exception:
        return default


def _safe_str(v, default=""):
    if v is None:
        return default
    return str(v)


def _safe_json(v):
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return "{}"


def _norm_symbol(v):
    return _safe_str(v, "").strip().upper()


def _is_int_like(x):
    try:
        xf = float(x)
        return abs(xf - int(xf)) < 1e-9
    except Exception:
        return False


def _map_signal(v):
    s = _safe_str(v, "").strip().upper()
    if s in ("OPEN", "INCREASE", "BUY"):
        return "BUY"
    if s in ("DECREASE", "CLOSE", "SELL"):
        return "SELL"
    if s in ("WATCH", "HOLD", "PROPOSE_OPEN", "PROPOSE_CLOSE"):
        return s
    return "NEUTRAL"


def _infer_asset_class(symbol):
    s = _norm_symbol(symbol)
    if s.startswith("FX:") or s.endswith("=X"):
        return "FX"
    return "EQUITY"


def _load_previous_state(con, initial_capital):
    prev = con.execute(
        """
        SELECT run_id, cash_eur, cum_fees_eur, cum_ai_cost_eur
        FROM core.portfolio_snapshot
        ORDER BY ts DESC
        LIMIT 1
    """
    ).fetchone()

    if prev:
        prev_run_id = _safe_str(prev[0], "")
        cash = _safe_float(prev[1], initial_capital)
        cum_fees = _safe_float(prev[2], 0.0)
        cum_ai = _safe_float(prev[3], 0.0)
    else:
        prev_run_id = ""
        cash = initial_capital
        cum_fees = 0.0
        cum_ai = 0.0

    positions = {}
    if prev_run_id:
        pos_rows = con.execute(
            """
            SELECT symbol, qty, avg_cost
            FROM core.positions_snapshot
            WHERE run_id = ?
        """,
            [prev_run_id],
        ).fetchall()

        for r in pos_rows:
            sym = _norm_symbol(r[0])
            if not sym:
                continue
            positions[sym] = {
                "qty": _safe_float(r[1], 0.0),
                "avg": _safe_float(r[2], 0.0),
            }

    return cash, cum_fees, cum_ai, positions


def _ensure_instruments(con, symbols, ts):
    rows = []
    for sym in symbols:
        s = _norm_symbol(sym)
        if not s:
            continue
        rows.append((s, _infer_asset_class(s), ts))
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO core.instruments (symbol, asset_class, currency, is_active, updated_at)
        VALUES (?, ?, 'EUR', TRUE, ?)
        ON CONFLICT (symbol) DO UPDATE SET
          updated_at = excluded.updated_at
        """,
        rows,
    )
    return len(rows)


def _upsert_ai_signals(con, rows_in, run_id, ts_default):
    rows = []
    for idx, r in enumerate(rows_in or [], start=1):
        symbol = _norm_symbol(r.get("symbol"))
        signal = _map_signal(r.get("signal") or r.get("action"))
        if not symbol:
            continue
        signal_id = _safe_str(r.get("signal_id"), "").strip() or f"SIG_{run_id}_{idx}"
        confidence = _safe_int(r.get("confidence"), None)
        if confidence is not None:
            confidence = max(0, min(100, confidence))
        risk_score = _safe_int(r.get("risk_score"), _safe_int(r.get("riskScore"), None))
        if risk_score is None and confidence is not None:
            risk_score = 100 - confidence
        if risk_score is not None:
            risk_score = max(0, min(100, risk_score))

        horizon = _safe_str(r.get("horizon"), "").strip()
        if not horizon:
            horizon_days = _safe_int(r.get("horizonDays"), None)
            horizon = f"D{horizon_days}" if horizon_days is not None and horizon_days > 0 else None
        else:
            horizon = horizon[:32]

        entry_zone = _safe_str(r.get("entry_zone"), "").strip() or None
        stop_loss = _safe_float(r.get("stop_loss"), _safe_float(r.get("stopLoss"), _safe_float(r.get("stopLossPct"), None)))
        take_profit = _safe_float(
            r.get("take_profit"),
            _safe_float(r.get("takeProfit"), _safe_float(r.get("takeProfitPct"), None)),
        )
        catalyst = _safe_str(r.get("catalyst"), "").strip() or None
        rationale = _safe_str(r.get("rationale"), "").strip() or None
        payload = r.get("payload_json", r)

        rows.append(
            (
                signal_id,
                run_id,
                _safe_str(r.get("ts"), "").strip() or ts_default,
                symbol,
                signal,
                confidence,
                horizon,
                entry_zone,
                stop_loss,
                take_profit,
                risk_score,
                catalyst,
                rationale,
                _safe_json(payload),
            )
        )

    if not rows:
        return 0

    con.executemany(
        """
        INSERT INTO core.ai_signals (
          signal_id, run_id, ts, symbol, signal, confidence, horizon, entry_zone,
          stop_loss, take_profit, risk_score, catalyst, rationale, payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (signal_id) DO UPDATE SET
          run_id = excluded.run_id,
          ts = excluded.ts,
          symbol = excluded.symbol,
          signal = excluded.signal,
          confidence = excluded.confidence,
          horizon = excluded.horizon,
          entry_zone = excluded.entry_zone,
          stop_loss = excluded.stop_loss,
          take_profit = excluded.take_profit,
          risk_score = excluded.risk_score,
          catalyst = excluded.catalyst,
          rationale = excluded.rationale,
          payload_json = excluded.payload_json
        """,
        rows,
    )
    return len(rows)


def _upsert_alerts(con, rows_in, run_id, ts_default):
    rows = []
    for idx, r in enumerate(rows_in or [], start=1):
        message = _safe_str(r.get("message"), "").strip()
        if not message:
            continue
        alert_id = _safe_str(r.get("alert_id"), "").strip() or f"ALT_{run_id}_{idx}"
        severity = (_safe_str(r.get("severity"), "INFO").strip() or "INFO").upper()
        category = (_safe_str(r.get("category"), "SYSTEM").strip() or "SYSTEM").upper()
        symbol = _norm_symbol(r.get("symbol")) or "GLOBAL"
        code = _safe_str(r.get("code"), "").strip() or None
        payload = r.get("payload_json", r)

        rows.append(
            (
                alert_id,
                run_id,
                _safe_str(r.get("ts"), "").strip() or ts_default,
                severity,
                category,
                symbol,
                message,
                code,
                _safe_json(payload),
            )
        )

    if not rows:
        return 0

    con.executemany(
        """
        INSERT INTO core.alerts (
          alert_id, run_id, ts, severity, category, symbol, message, code, payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (alert_id) DO UPDATE SET
          run_id = excluded.run_id,
          ts = excluded.ts,
          severity = excluded.severity,
          category = excluded.category,
          symbol = excluded.symbol,
          message = excluded.message,
          code = excluded.code,
          payload_json = excluded.payload_json
        """,
        rows,
    )
    return len(rows)


def _execute_ledger_engine(con, bundle, run_id):
    run_meta = bundle.get("run", {}) or {}
    ts_start = _safe_str(run_meta.get("ts_start"), "")
    ts_end = _safe_str(run_meta.get("ts_end"), "") or ts_start
    ts = ts_end or ts_start
    model = _safe_str(run_meta.get("model"), "UNKNOWN")
    decision_summary = _safe_str(run_meta.get("decision_summary"), "NO_TRADE")

    initial_capital = INITIAL_CAPITAL_DEFAULT

    # 1) Etat precedent
    cash, cum_fees, cum_ai, positions = _load_previous_state(con, initial_capital)
    starting_cash = cash

    # 2) Prix de marche
    prices = {}
    for p in (bundle.get("market_prices", []) or []):
        sym = _norm_symbol(p.get("symbol"))
        px = _safe_float(p.get("close"), 0.0)
        if sym and px > 0:
            prices[sym] = px

    # 3) Nettoyage eventuel si rerun meme run_id
    con.execute("DELETE FROM core.positions_snapshot WHERE run_id = ?", [run_id])
    con.execute("DELETE FROM core.portfolio_snapshot WHERE run_id = ?", [run_id])
    con.execute("DELETE FROM core.ai_signals WHERE run_id = ?", [run_id])
    con.execute("DELETE FROM core.alerts WHERE run_id = ?", [run_id])
    con.execute("DELETE FROM core.runs WHERE run_id = ?", [run_id])

    # 4) Execution des fills avec garde-fou cash
    execution_notes = []
    actual_fills = []
    trades_count = 0

    for f in (bundle.get("fills", []) or []):
        sym = _norm_symbol(f.get("symbol"))
        side = _safe_str(f.get("side"), "").strip().upper()
        req_qty = _safe_float(f.get("qty"), 0.0)
        px = _safe_float(f.get("price"), 0.0)

        if not sym:
            execution_notes.append("SKIP_FILL:NO_SYMBOL")
            continue
        if req_qty <= 0:
            execution_notes.append("SKIP_FILL:NONPOSITIVE_QTY:" + sym)
            continue
        if px <= 0:
            execution_notes.append("SKIP_FILL:NONPOSITIVE_PRICE:" + sym)
            continue

        if side == "BUY":
            # Cap cash : on n'achete jamais plus que ce que le cash permet
            if _is_int_like(req_qty):
                affordable_qty = int((cash + EPS) / px)
                requested_qty = int(req_qty)
                actual_qty = min(requested_qty, affordable_qty)
            else:
                affordable_qty = cash / px
                requested_qty = req_qty
                actual_qty = min(requested_qty, affordable_qty)

            actual_qty = _safe_float(actual_qty, 0.0)

            if actual_qty <= EPS:
                execution_notes.append(
                    "BUY_SKIPPED:INSUFFICIENT_CASH:"
                    + sym
                    + ":need="
                    + str(round(req_qty * px, 2))
                    + ":cash="
                    + str(round(cash, 2))
                )
                continue

            if actual_qty + EPS < req_qty:
                execution_notes.append(
                    "BUY_RESIZED:CASH_CAP:"
                    + sym
                    + ":from="
                    + str(req_qty)
                    + ":to="
                    + str(actual_qty)
                )

            cash -= actual_qty * px

            pos = positions.setdefault(sym, {"qty": 0.0, "avg": 0.0})
            new_qty = pos["qty"] + actual_qty
            if new_qty > EPS:
                pos["avg"] = ((pos["qty"] * pos["avg"]) + (actual_qty * px)) / new_qty
                pos["qty"] = new_qty

            trades_count += 1
            actual_fills.append({"symbol": sym, "side": "BUY", "req_qty": req_qty, "exec_qty": actual_qty, "price": px})

        elif side == "SELL":
            held_qty = _safe_float(positions.get(sym, {}).get("qty"), 0.0)

            if _is_int_like(req_qty):
                actual_qty = min(int(req_qty), int(held_qty))
            else:
                actual_qty = min(req_qty, held_qty)

            actual_qty = _safe_float(actual_qty, 0.0)

            if actual_qty <= EPS:
                execution_notes.append("SELL_SKIPPED:NO_POSITION_OR_ZERO_QTY:" + sym + ":held=" + str(held_qty))
                continue

            if actual_qty + EPS < req_qty:
                execution_notes.append(
                    "SELL_RESIZED:POSITION_CAP:" + sym + ":from=" + str(req_qty) + ":to=" + str(actual_qty)
                )

            cash += actual_qty * px

            positions[sym]["qty"] -= actual_qty
            if positions[sym]["qty"] <= EPS:
                del positions[sym]

            trades_count += 1
            actual_fills.append({"symbol": sym, "side": "SELL", "req_qty": req_qty, "exec_qty": actual_qty, "price": px})

        else:
            execution_notes.append("SKIP_FILL:UNKNOWN_SIDE:" + sym + ":" + side)

    # securite absolue
    if cash < 0 and abs(cash) < 0.01:
        cash = 0.0

    # 5) Valorisation
    equity_eur = 0.0
    for sym, d in positions.items():
        last_px = prices.get(sym, d["avg"])
        equity_eur += d["qty"] * last_px

    total_value = cash + equity_eur
    total_pnl_eur = total_value - initial_capital

    # ROI stocke en ratio, pas en % (ex: 0.049 = 4.9%)
    if abs(initial_capital) > EPS:
        roi = total_pnl_eur / initial_capital
    else:
        roi = 0.0

    ai_signals_in = bundle.get("ai_signals") or []
    alerts_in = bundle.get("alerts") or []

    symbols_needed = set(positions.keys())
    for s in ai_signals_in:
        sym = _norm_symbol(s.get("symbol"))
        if sym:
            symbols_needed.add(sym)
    for a in alerts_in:
        sym = _norm_symbol(a.get("symbol"))
        if sym and sym != "GLOBAL":
            symbols_needed.add(sym)

    # 6) Ecriture core.runs
    con.execute(
        """
        INSERT INTO core.runs (run_id, ts_start, ts_end, model, decision_summary)
        VALUES (?, ?, ?, ?, ?)
    """,
        [run_id, ts_start or ts, ts_end or ts, model, decision_summary],
    )

    # 7) Garantir les instruments pour positions/signaux/alertes
    _ensure_instruments(con, symbols_needed, ts)

    # 8) Ecriture core.positions_snapshot
    for sym, d in positions.items():
        px = prices.get(sym, d["avg"])
        market_value = d["qty"] * px
        unrealized_pnl = (px - d["avg"]) * d["qty"]

        con.execute(
            """
            INSERT INTO core.positions_snapshot
            (run_id, ts, symbol, qty, avg_cost, last_price, market_value_eur, unrealized_pnl_eur)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            [run_id, ts, sym, d["qty"], d["avg"], px, market_value, unrealized_pnl],
        )

    # 9) Ecriture core.portfolio_snapshot
    con.execute(
        """
        INSERT INTO core.portfolio_snapshot
        (run_id, ts, cash_eur, equity_eur, total_value_eur, cum_fees_eur, cum_ai_cost_eur,
         trades_this_run, total_pnl_eur, roi, drawdown_pct)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        [
            run_id,
            ts,
            cash,
            equity_eur,
            total_value,
            cum_fees,
            cum_ai,
            trades_count,
            total_pnl_eur,
            roi,
            0.0,
        ],
    )

    # 10) Ecriture core.ai_signals / core.alerts
    ai_signals_count = _upsert_ai_signals(con, ai_signals_in, run_id, ts)
    alerts_count = _upsert_alerts(con, alerts_in, run_id, ts)

    return {
        "starting_cash": starting_cash,
        "ending_cash": cash,
        "equity_eur": equity_eur,
        "total_value_eur": total_value,
        "total_pnl_eur": total_pnl_eur,
        "roi_ratio": roi,
        "trades_count": trades_count,
        "actual_fills": actual_fills,
        "execution_notes": execution_notes,
        "ai_signals_count": ai_signals_count,
        "alerts_count": alerts_count,
    }


items = _items or []
out = []

for it in items:
    j = it.get("json", {})
    db_path = j.get("db_path")
    run_id = j.get("run_id")
    bundle = j.get("bundle", {}) or {}

    con = duckdb.connect(db_path)
    con.execute("BEGIN")
    try:
        result = _execute_ledger_engine(con, bundle, run_id)
        con.execute("COMMIT")

        out.append({"json": {"ok": True, "db_path": db_path, "run_id": run_id, "ledger_result": result}})
    except Exception as e:
        con.execute("ROLLBACK")
        out.append({"json": {"status": "FATAL_ERROR", "error_message": str(e), "db_path": db_path, "run_id": run_id}})
    finally:
        con.close()

return out
