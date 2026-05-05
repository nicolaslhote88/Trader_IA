from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import re

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

from app_modules.core import safe_float, safe_float_series


FX_SUFFIX = "=X"
FX_CURRENCY_CODES = {
    "USD",
    "EUR",
    "JPY",
    "GBP",
    "CHF",
    "CAD",
    "AUD",
    "NZD",
    "SEK",
    "NOK",
    "DKK",
    "CNH",
    "CNY",
    "HKD",
    "SGD",
    "MXN",
    "ZAR",
    "TRY",
    "PLN",
    "CZK",
    "HUF",
    "ILS",
    "RUB",
    "BRL",
    "INR",
    "KRW",
}

FX_MAJOR_PAIRS = ("EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD")


def _first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _normalize_history_symbol(symbol: str) -> str:
    raw = str(symbol or "").strip().upper()
    if not raw:
        return ""
    if raw.endswith(FX_SUFFIX):
        return raw

    fx_candidate = raw[3:] if raw.startswith("FX:") else raw
    fx_candidate = re.sub(r"[^A-Z]", "", fx_candidate)
    if len(fx_candidate) >= 6:
        pair = fx_candidate[:6]
        base, quote = pair[:3], pair[3:6]
        if base in FX_CURRENCY_CODES and quote in FX_CURRENCY_CODES:
            return f"{pair}{FX_SUFFIX}"
    return raw


def _normalize_portfolio_positions(df_portfolio: pd.DataFrame) -> pd.DataFrame:
    cols = ["symbol", "name", "sector", "industry", "quantity", "avgprice", "lastprice", "marketvalue", "unrealizedpnl"]
    if df_portfolio is None or df_portfolio.empty:
        return pd.DataFrame(columns=cols)

    df = df_portfolio.copy()
    if "symbol" not in df.columns:
        return pd.DataFrame(columns=cols)

    for txt_col in ["name", "sector", "industry"]:
        if txt_col not in df.columns:
            df[txt_col] = ""
        df[txt_col] = df[txt_col].fillna("").astype(str).str.strip()

    for col in ["quantity", "avgprice", "lastprice", "marketvalue", "unrealizedpnl"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = safe_float_series(df[col])

    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df = df[~df["symbol"].isin(["", "CASH_EUR", "__META__"])].copy()
    if df.empty:
        return pd.DataFrame(columns=cols)

    has_qty = df["quantity"] > 0
    has_value = df["marketvalue"] > 0
    df = df[has_qty | has_value].copy()
    if df.empty:
        return pd.DataFrame(columns=cols)

    return df[cols].sort_values("marketvalue", ascending=False)


def _normalize_transactions(df_transactions: pd.DataFrame) -> pd.DataFrame:
    cols = ["timestamp", "symbol", "side", "price", "quantity", "notional"]
    if df_transactions is None or df_transactions.empty:
        return pd.DataFrame(columns=cols)

    df = df_transactions.copy()

    ts_col = _first_existing_column(df, ["timestamp", "date", "created_at", "updatedat"])
    side_col = _first_existing_column(df, ["side", "action", "signal"])
    symbol_col = _first_existing_column(df, ["symbol", "ticker"])
    price_col = _first_existing_column(df, ["price", "executionprice", "fillprice", "avgprice", "lastprice"])
    qty_col = _first_existing_column(df, ["quantity", "qty"])
    notional_col = _first_existing_column(df, ["notional", "tradevalue", "value", "amount"])

    if not symbol_col or not side_col:
        return pd.DataFrame(columns=cols)

    df["timestamp"] = pd.to_datetime(df[ts_col], errors="coerce", utc=True) if ts_col else pd.NaT
    df["symbol"] = df[symbol_col].astype(str).str.strip().str.upper()
    df["side"] = df[side_col].astype(str).str.strip().str.upper()
    side_map = {
        "OPEN": "BUY",
        "INCREASE": "BUY",
        "BUY": "BUY",
        "CLOSE": "SELL",
        "DECREASE": "SELL",
        "SELL": "SELL",
    }
    df["side"] = df["side"].map(lambda s: side_map.get(str(s).upper(), str(s).upper()))
    df["price"] = safe_float_series(df[price_col]) if price_col else 0.0
    df["quantity"] = safe_float_series(df[qty_col]) if qty_col else 0.0
    df["notional"] = safe_float_series(df[notional_col]) if notional_col else 0.0

    return df[cols].sort_values("timestamp", na_position="last")


def _fetch_one_symbol_history(
    symbol: str,
    yfinance_api_url: str,
    interval: str,
    lookback_days: int,
) -> pd.DataFrame:
    request_symbol = _normalize_history_symbol(symbol)
    if not request_symbol:
        return pd.DataFrame()
    try:
        resp = requests.get(
            f"{yfinance_api_url}/history",
            params={
                "symbol": request_symbol,
                "interval": interval,
                "lookback_days": int(lookback_days),
                "allow_stale": "true",
            },
            timeout=8,
        )
        if resp.status_code != 200:
            return pd.DataFrame()
        data = resp.json()
        if not data.get("ok") or not data.get("bars"):
            return pd.DataFrame()

        df = pd.DataFrame(data["bars"])
        if df.empty:
            return pd.DataFrame()
        df = df.rename(
            columns={"t": "timestamp", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}
        )
        if "timestamp" not in df.columns or "close" not in df.columns:
            return pd.DataFrame()

        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["timestamp", "close"]).sort_values("timestamp")
        return df[["timestamp", "close"]]
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def _prefetch_histories(
    symbols: tuple[str, ...],
    yfinance_api_url: str,
    interval: str = "1d",
    lookback_days: int = 90,
) -> dict[str, pd.DataFrame]:
    if not symbols:
        return {}

    symbol_list = [s for s in symbols if isinstance(s, str) and s.strip()]
    if not symbol_list:
        return {}

    max_workers = min(8, len(symbol_list))
    out: dict[str, pd.DataFrame] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _fetch_one_symbol_history,
                symbol=sym,
                yfinance_api_url=yfinance_api_url,
                interval=interval,
                lookback_days=lookback_days,
            ): sym
            for sym in symbol_list
        }

        for future in as_completed(futures):
            sym = futures[future]
            try:
                out[sym] = future.result()
            except Exception:
                out[sym] = pd.DataFrame()

    return out


def _compute_buy_levels(symbol: str, tx_buy: pd.DataFrame) -> tuple[list[float], float | None]:
    if tx_buy is None or tx_buy.empty:
        return [], None

    tx = tx_buy[tx_buy["symbol"] == symbol].copy()
    if tx.empty:
        return [], None

    levels_set: set[float] = set()
    for price in tx["price"].tolist():
        p = safe_float(price)
        if p > 0:
            levels_set.add(round(p, 6))
    levels = sorted(levels_set)
    qty_sum = float(tx["quantity"].sum())

    if qty_sum > 0 and (tx["price"] > 0).any():
        weighted_sum = float((tx["price"] * tx["quantity"]).sum())
        pru = weighted_sum / qty_sum if qty_sum > 0 else None
    else:
        pru = None

    return levels, pru


def _extract_trade_events(
    symbol: str,
    tx: pd.DataFrame,
    *,
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
) -> pd.DataFrame:
    cols = ["timestamp", "side"]
    if tx is None or tx.empty:
        return pd.DataFrame(columns=cols)

    ev = tx[(tx["symbol"] == symbol) & (tx["side"].isin(["BUY", "SELL"]))].copy()
    if ev.empty:
        return pd.DataFrame(columns=cols)

    ev = ev.dropna(subset=["timestamp"])
    if ev.empty:
        return pd.DataFrame(columns=cols)

    if start_ts is not None:
        ev = ev[ev["timestamp"] >= start_ts]
    if end_ts is not None:
        ev = ev[ev["timestamp"] <= end_ts]
    if ev.empty:
        return pd.DataFrame(columns=cols)

    ev["trade_day"] = ev["timestamp"].dt.floor("D")
    ev = (
        ev.sort_values("timestamp")
        .drop_duplicates(subset=["trade_day", "side"], keep="first")
        .sort_values("timestamp")
    )
    return ev[cols]


def _build_position_sparkline(
    title_text: str,
    hist: pd.DataFrame,
    buy_levels: list[float],
    trade_events: pd.DataFrame,
    pnl_pct: float | None,
    profitable: bool | None,
) -> go.Figure:
    if hist is None or hist.empty:
        fig = go.Figure()
        fig.update_layout(
            title=f"{title_text} | no history",
            height=190,
            margin=dict(t=34, b=8, l=8, r=8),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            annotations=[
                dict(
                    text="Data unavailable",
                    x=0.5,
                    y=0.5,
                    xref="paper",
                    yref="paper",
                    showarrow=False,
                    font=dict(size=11, color="#999"),
                )
            ],
        )
        return fig

    title_pnl = "n/a" if pnl_pct is None else f"{pnl_pct:+.2f}%"
    line_color = "#4ea1ff"
    fill_color = "rgba(40,167,69,0.18)" if profitable else "rgba(220,53,69,0.16)"

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=hist["timestamp"],
            y=hist["close"],
            mode="lines",
            line=dict(color=line_color, width=1.6),
            fill="tozeroy",
            fillcolor=fill_color,
            name="Close",
            hovertemplate="%{x|%Y-%m-%d}<br>Close: %{y:.2f}<extra></extra>",
        )
    )

    y_min = float(hist["close"].min())
    y_max = float(hist["close"].max())
    if y_max <= y_min:
        y_max = y_min + 1.0

    if trade_events is not None and not trade_events.empty:
        for ev in trade_events.itertuples(index=False):
            color = "rgba(40,167,69,0.85)" if str(ev.side).upper() == "BUY" else "rgba(220,53,69,0.85)"
            fig.add_shape(
                type="line",
                x0=ev.timestamp,
                x1=ev.timestamp,
                y0=y_min,
                y1=y_max,
                line=dict(color=color, width=1.5, dash="dot"),
            )

    for level in buy_levels:
        fig.add_hline(
            y=level,
            line_dash="dot",
            line_width=1,
            line_color="rgba(200,200,200,0.65)",
        )

    fig.update_layout(
        title=f"{title_text} | {title_pnl}",
        height=190,
        margin=dict(t=34, b=8, l=8, r=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, ticks="", zeroline=False),
        hovermode="x unified",
    )
    return fig


def _build_fx_pair_sparkline(
    pair: str,
    hist: pd.DataFrame,
    row: pd.Series | None,
    entry_levels: list[dict[str, object]] | None = None,
    focus: bool = False,
) -> go.Figure:
    def clean_text(value: object) -> str:
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass
        return str(value).strip()

    def price_decimals(value: float) -> int:
        if str(pair).endswith("JPY"):
            return 2
        if value and abs(value) < 0.1:
            return 5
        if value and abs(value) < 2:
            return 4
        return 3

    def y_axis_window(values: pd.Series) -> tuple[float, float]:
        vals = pd.to_numeric(values, errors="coerce").dropna()
        if vals.empty:
            return 0.0, 1.0
        y_min = float(vals.min())
        y_max = float(vals.max())
        mid = (y_min + y_max) / 2.0
        span = y_max - y_min
        min_span = max(abs(mid) * 0.003, 0.0005)
        if span < min_span:
            span = min_span
            y_min = mid - span / 2.0
            y_max = mid + span / 2.0
        pad = max(span * 0.18, abs(mid) * 0.00035)
        return y_min - pad, y_max + pad

    title_suffix = "n/a"
    profitable: bool | None = None
    last_close = 0.0

    if hist is not None and not hist.empty:
        first_close = safe_float(hist["close"].iloc[0])
        last_close = safe_float(hist["close"].iloc[-1])
        if first_close > 0 and last_close > 0:
            ret_pct = (last_close / first_close - 1.0) * 100.0
            title_suffix = f"{ret_pct:+.2f}%"
            profitable = ret_pct >= 0
    else:
        last_close = safe_float(row.get("Prix")) if row is not None and "Prix" in row.index else 0.0

    signal = clean_text(row.get("Signal")) if row is not None and "Signal" in row.index else ""
    technique = clean_text(row.get("Technique")) if row is not None and "Technique" in row.index else ""
    news_24h = safe_float(row.get("News 24h")) if row is not None and "News 24h" in row.index else 0.0
    open_lots = safe_float(row.get("Lots ouverts")) if row is not None and "Lots ouverts" in row.index else 0.0

    meta_bits = []
    if signal and signal != "-":
        meta_bits.append(signal)
    elif technique and technique != "-":
        meta_bits.append(technique)
    if news_24h > 0:
        meta_bits.append(f"news 24h: {int(news_24h)}")
    if open_lots > 0:
        meta_bits.append(f"lots: {int(open_lots)}")

    title_text = f"{pair} | {title_suffix}"
    if meta_bits:
        title_text = f"{pair} - {' / '.join(meta_bits)} | {title_suffix}"

    entry_levels = entry_levels or []

    if hist is None or hist.empty:
        fig = go.Figure()
        fig.update_layout(
            title=f"{title_text} | no history",
            height=300 if focus else 204,
            margin=dict(t=34, b=26, l=42, r=8),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            annotations=[
                dict(
                    text="Data unavailable",
                    x=0.5,
                    y=0.5,
                    xref="paper",
                    yref="paper",
                    showarrow=False,
                    font=dict(size=11, color="#999"),
                )
            ],
        )
        return fig

    decimals = price_decimals(last_close)
    y_inputs = [hist["close"]]
    entry_prices = []
    for entry in entry_levels:
        price = safe_float(entry.get("price"))
        if price > 0:
            entry_prices.append(price)
    if entry_prices:
        y_inputs.append(pd.Series(entry_prices))
    y_floor, y_ceiling = y_axis_window(pd.concat(y_inputs, ignore_index=True))
    line_color = "#4ea1ff"
    fill_color = "rgba(40,167,69,0.18)" if profitable else "rgba(220,53,69,0.16)"

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=hist["timestamp"],
            y=[y_floor] * len(hist),
            mode="lines",
            line=dict(width=0),
            hoverinfo="skip",
            showlegend=False,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=hist["timestamp"],
            y=hist["close"],
            mode="lines",
            line=dict(color=line_color, width=1.6),
            fill="tonexty",
            fillcolor=fill_color,
            name=pair,
            hovertemplate=f"%{{x|%Y-%m-%d}}<br>{pair}: %{{y:.{decimals}f}}<extra></extra>",
        )
    )

    first_close = safe_float(hist["close"].iloc[0])
    last_close = safe_float(hist["close"].iloc[-1])
    if first_close > 0:
        fig.add_hline(
            y=first_close,
            line_dash="dot",
            line_width=1,
            line_color="rgba(200,200,200,0.55)",
        )
    if last_close > 0:
        fig.add_hline(
            y=last_close,
            line_dash="dash",
            line_width=1,
            line_color="rgba(100,180,255,0.35)",
        )

    if entry_levels and len(hist) > 0:
        x0 = hist["timestamp"].iloc[0]
        x1 = hist["timestamp"].iloc[-1]
        for entry in entry_levels:
            price = safe_float(entry.get("price"))
            if price <= 0:
                continue
            llm = clean_text(entry.get("llm")) or "LLM"
            side = clean_text(entry.get("side")).lower()
            lots = safe_float(entry.get("lots"))
            color = clean_text(entry.get("color")) or "#f59e0b"
            label_bits = [llm, "entry"]
            if side:
                label_bits.append(side)
            if lots > 0:
                label_bits.append(f"{lots:g} lot")
            fig.add_trace(
                go.Scatter(
                    x=[x0, x1],
                    y=[price, price],
                    mode="lines",
                    line=dict(color=color, width=2.2 if focus else 1.4, dash="dash"),
                    name=" ".join(label_bits),
                    hovertemplate=f"{llm}<br>Entry: %{{y:.{decimals}f}}<br>{side}<extra></extra>",
                    showlegend=focus,
                )
            )

    fig.update_layout(
        title=title_text,
        height=320 if focus else 204,
        margin=dict(t=42 if focus else 34, b=32 if focus else 28, l=52 if focus else 44, r=12 if focus else 8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=bool(focus and entry_levels),
        xaxis=dict(
            showgrid=False,
            showticklabels=True,
            zeroline=False,
            nticks=4,
            tickformat="%d/%m",
            tickfont=dict(size=10, color="rgba(230,230,230,0.75)"),
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(255,255,255,0.10)",
            ticks="outside",
            ticklen=3,
            zeroline=False,
            nticks=4,
            tickformat=f".{decimals}f",
            range=[y_floor, y_ceiling],
            tickfont=dict(size=10, color="rgba(230,230,230,0.78)"),
        ),
        hovermode="x unified",
        title_font=dict(size=15 if focus else 12),
    )
    if focus and entry_levels:
        fig.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, title_text="Entrées ouvertes"))
    return fig


def render_fx_pair_sparklines(
    pair_overview: pd.DataFrame,
    *,
    yfinance_api_url: str,
    lookback_days: int = 90,
    columns_per_row: int = 3,
    major_pairs: tuple[str, ...] = FX_MAJOR_PAIRS,
    selected_pairs: list[str] | tuple[str, ...] | None = None,
    entry_levels_by_pair: dict[str, list[dict[str, object]]] | None = None,
    focus: bool = False,
) -> None:
    if pair_overview is None or pair_overview.empty:
        st.caption("Aucune paire FX disponible pour les sparklines.")
        return

    pair_col = "Paire" if "Paire" in pair_overview.columns else "pair" if "pair" in pair_overview.columns else None
    if pair_col is None:
        st.caption("Colonne paire introuvable pour les sparklines.")
        return

    df = pair_overview.copy()
    df["_pair"] = df[pair_col].fillna("").astype(str).str.upper().str.replace(r"[^A-Z]", "", regex=True).str[:6]
    df = df[df["_pair"].map(lambda p: len(p) == 6 and p[:3] in FX_CURRENCY_CODES and p[3:] in FX_CURRENCY_CODES)].copy()
    if df.empty:
        st.caption("Aucune paire FX valide pour les sparklines.")
        return

    meta_by_pair = {str(row["_pair"]): row for _, row in df.drop_duplicates("_pair").iterrows()}
    all_pairs = sorted(meta_by_pair.keys())
    if selected_pairs:
        selected_set = {
            re.sub(r"[^A-Z]", "", str(pair or "").upper())[:6]
            for pair in selected_pairs
            if str(pair or "").strip()
        }
        all_pairs = [pair for pair in all_pairs if pair in selected_set]
    if not all_pairs:
        st.caption("Aucune paire FX valide dans la selection sparkline.")
        return

    histories = _prefetch_histories(
        symbols=tuple(all_pairs),
        yfinance_api_url=yfinance_api_url,
        interval="1d",
        lookback_days=lookback_days,
    )
    entry_levels_by_pair = entry_levels_by_pair or {}

    if selected_pairs:
        grid_cols = st.columns(max(1, int(columns_per_row)))
        for idx, pair in enumerate(all_pairs):
            fig = _build_fx_pair_sparkline(
                pair,
                histories.get(pair, pd.DataFrame()),
                meta_by_pair.get(pair),
                entry_levels=entry_levels_by_pair.get(pair, []),
                focus=focus,
            )
            with grid_cols[idx % len(grid_cols)]:
                st.plotly_chart(fig, use_container_width=True, key=f"fx_pair_spark_selected_{pair}_{idx}")
        return

    major_set = set(major_pairs)
    sections = [
        ("Paires principales", [p for p in major_pairs if p in meta_by_pair]),
        ("Autres paires", [p for p in all_pairs if p not in major_set]),
    ]

    for section_title, section_pairs in sections:
        if not section_pairs:
            continue
        st.markdown(f"**{section_title}**")
        grid_cols = st.columns(max(1, int(columns_per_row)))
        for idx, pair in enumerate(section_pairs):
            fig = _build_fx_pair_sparkline(
                pair,
                histories.get(pair, pd.DataFrame()),
                meta_by_pair.get(pair),
                entry_levels=entry_levels_by_pair.get(pair, []),
                focus=False,
            )
            with grid_cols[idx % len(grid_cols)]:
                st.plotly_chart(fig, use_container_width=True, key=f"fx_pair_spark_{section_title}_{pair}_{idx}")


def render_portfolio_sparklines(
    df_portfolio: pd.DataFrame,
    df_transactions: pd.DataFrame,
    *,
    yfinance_api_url: str,
    lookback_days: int = 90,
    columns_per_row: int = 3,
) -> None:
    df_pos = _normalize_portfolio_positions(df_portfolio)
    if df_pos.empty:
        st.caption("No open positions for sparklines.")
        return

    tx = _normalize_transactions(df_transactions)
    tx_buy = tx[tx["side"] == "BUY"].copy() if not tx.empty else pd.DataFrame(columns=tx.columns)

    symbols = tuple(sorted(df_pos["symbol"].dropna().astype(str).str.strip().unique().tolist()))
    if not symbols:
        st.caption("No symbols found.")
        return

    histories = _prefetch_histories(
        symbols=symbols,
        yfinance_api_url=yfinance_api_url,
        interval="1d",
        lookback_days=lookback_days,
    )

    grid_cols = st.columns(max(1, int(columns_per_row)))
    for idx, pos in enumerate(df_pos.itertuples(index=False)):
        symbol = str(pos.symbol)
        name = str(getattr(pos, "name", "") or "").strip()
        sector = str(getattr(pos, "sector", "") or "").strip()
        industry = str(getattr(pos, "industry", "") or "").strip()
        qty = safe_float(getattr(pos, "quantity", 0))
        avgprice = safe_float(getattr(pos, "avgprice", 0))
        lastprice = safe_float(getattr(pos, "lastprice", 0))
        marketvalue = safe_float(getattr(pos, "marketvalue", 0))
        unrealized = safe_float(getattr(pos, "unrealizedpnl", 0))

        pru_from_portfolio = avgprice if avgprice > 0 else None
        buy_levels, pru_from_buys = _compute_buy_levels(symbol, tx_buy)
        pru = pru_from_portfolio if pru_from_portfolio else pru_from_buys

        if lastprice <= 0:
            hist = histories.get(symbol, pd.DataFrame())
            if hist is not None and not hist.empty:
                lastprice = safe_float(hist["close"].iloc[-1])

        if pru and pru > 0:
            pnl_pct = ((lastprice / pru) - 1.0) * 100 if lastprice > 0 else None
        else:
            cost_basis = (marketvalue - unrealized) if marketvalue != 0 else (avgprice * qty)
            pnl_pct = (unrealized / cost_basis * 100) if cost_basis else None

        profitable = bool(lastprice > pru) if (lastprice > 0 and pru and pru > 0) else (bool(pnl_pct and pnl_pct > 0))

        hist = histories.get(symbol, pd.DataFrame())
        start_ts = hist["timestamp"].min() if (hist is not None and not hist.empty) else None
        end_ts = hist["timestamp"].max() if (hist is not None and not hist.empty) else None
        trade_events = _extract_trade_events(symbol, tx, start_ts=start_ts, end_ts=end_ts)

        title_bits = [symbol]
        if name:
            title_bits.append(name)
        label_meta = []
        if sector:
            label_meta.append(sector)
        if industry:
            label_meta.append(industry)
        if label_meta:
            title_bits.append(f"({', '.join(label_meta)})")
        title_text = " - ".join(title_bits)

        fig = _build_position_sparkline(
            title_text=title_text,
            hist=hist,
            buy_levels=buy_levels,
            trade_events=trade_events,
            pnl_pct=pnl_pct,
            profitable=profitable,
        )

        with grid_cols[idx % len(grid_cols)]:
            st.plotly_chart(fig, use_container_width=True, key=f"spark_{symbol}_{idx}")
