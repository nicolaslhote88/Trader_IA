"""Daily research contract; served by the existing Yahoo gateway and cache.

The trading /history response is deliberately unchanged. Never substitute
unadjusted Close when Adj Close is missing, or a cached failure for success.
"""
from datetime import date, datetime, timezone
import re
import threading
from fastapi import Query
from fastapi.responses import JSONResponse

_lock = threading.Lock()


def serialize(frame):
    import math
    def number(value):
        try:
            value = float(value)
            return value if math.isfinite(value) else None
        except (TypeError, ValueError):
            return None
    bars = []
    for timestamp, row in frame.iterrows():
        bars.append(dict(date=timestamp.date().isoformat(), **{
            field: number(row.get(column)) for field, column in {
                'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close',
                'adj_close': 'Adj Close', 'volume': 'Volume',
                'dividends': 'Dividends', 'splits': 'Stock Splits',
            }.items()}))
    return bars


def register(app, yf, cache_path, read_cache, write_cache, rate_gate):
    @app.get('/research/history')
    def history(symbol: str = Query(..., max_length=32), start: str = '2010-01-01', end: str = ''):
        symbol = symbol.strip().upper()
        if not re.fullmatch(r'[A-Z0-9.^=\-]{1,32}', symbol):
            return JSONResponse(status_code=422, content={'ok': False, 'error': 'INVALID_SYMBOL'})
        try:
            first = date.fromisoformat(start)
            # Exclude today's unfinished sessions, independent of exchange.
            last = min(date.fromisoformat(end) if end else date.today(), datetime.now(timezone.utc).date())
            if first < date(1980, 1, 1) or first >= last:
                raise ValueError()
        except ValueError:
            return JSONResponse(status_code=422, content={'ok': False, 'error': 'INVALID_DATE_RANGE'})
        path = cache_path('research_v1', f'{symbol}_{first}')
        cached = read_cache(path, 86400)
        if cached and cached.get('end_exclusive') == str(last):
            return dict(cached, cache_hit=True)
        if not _lock.acquire(blocking=False):
            return JSONResponse(status_code=429, content={'ok': False, 'error': 'RESEARCH_BUSY'})
        try:
            rate_gate()
            ticker = yf.Ticker(symbol)
            frame = ticker.history(start=str(first), end=str(last), interval='1d',
                                   auto_adjust=False, actions=True, repair=False, timeout=30)
            if frame is None or frame.empty or 'Adj Close' not in frame:
                return JSONResponse(status_code=502, content={'ok': False, 'error': 'NO_ADJUSTED_HISTORY'})
            meta = ticker.history_metadata or {}
            result = {'ok': True, 'contract': 'YF_RESEARCH_HISTORY_V1', 'symbol': symbol,
                      'source': 'yahoo', 'yfinance_version': yf.__version__,
                      'fetched_at': datetime.now(timezone.utc).isoformat(),
                      'currency': meta.get('currency'), 'exchange_timezone': meta.get('exchangeTimezoneName'),
                      'adjustment': 'vendor_close_split_basis_adj_close_total_return_proxy',
                      'start': str(first), 'end_exclusive': str(last),
                      'bars': serialize(frame), 'cache_hit': False}
            write_cache(path, result)
            return result
        except Exception as exc:
            return JSONResponse(status_code=502, content={'ok': False, 'error': type(exc).__name__})
        finally:
            _lock.release()
