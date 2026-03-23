from __future__ import annotations

import io
import math
from datetime import datetime, timedelta, timezone
from typing import Iterable

import numpy as np
import pandas as pd
import requests

from database import get_conn

BYBIT_BASE_URL = 'https://api.bybit.com'
SUPPORTED_INTERVALS = {'1', '3', '5', '15', '30', '60', '120', '240', '360', '720', 'D', 'W', 'M'}


class DataValidationError(ValueError):
    pass

from config import INSTANCE_DIR

SYMBOL_CATALOG_CACHE_PATH = INSTANCE_DIR / 'symbol_catalog_linear_usdt.json'
SYMBOL_CATALOG_TTL_SECONDS = 6 * 60 * 60
DEFAULT_SYMBOL_CATALOG = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'BNBUSDT']


def _catalog_cache_payload(symbols: list[dict], source: str, stale: bool = False, error: str | None = None) -> dict:
    return {
        'source': source,
        'fetched_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'stale': stale,
        'error': error,
        'symbols': symbols,
    }


def _read_symbol_catalog_cache() -> dict | None:
    if not SYMBOL_CATALOG_CACHE_PATH.exists():
        return None
    try:
        payload = pd.read_json(SYMBOL_CATALOG_CACHE_PATH, typ='series').to_dict()
    except Exception:
        import json
        try:
            payload = json.loads(SYMBOL_CATALOG_CACHE_PATH.read_text())
        except Exception:
            return None
    fetched_at = payload.get('fetched_at')
    if fetched_at:
        try:
            ts = pd.Timestamp(fetched_at)
            if ts.tzinfo is None:
                ts = ts.tz_localize('UTC')
            else:
                ts = ts.tz_convert('UTC')
            payload['age_seconds'] = max(0, int((pd.Timestamp.now(tz='UTC') - ts).total_seconds()))
        except Exception:
            payload['age_seconds'] = None
    return payload


def _write_symbol_catalog_cache(payload: dict) -> None:
    import json
    SYMBOL_CATALOG_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    SYMBOL_CATALOG_CACHE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2))


def _fallback_symbol_catalog(reason: str | None = None) -> dict:
    db_symbols = list_symbols('5')
    merged = []
    seen = set()
    for idx, symbol in enumerate([*db_symbols, *DEFAULT_SYMBOL_CATALOG]):
        symbol = symbol.upper()
        if symbol in seen:
            continue
        seen.add(symbol)
        merged.append({
            'symbol': symbol,
            'base_coin': symbol.replace('USDT', '') if symbol.endswith('USDT') else symbol,
            'quote_coin': 'USDT' if symbol.endswith('USDT') else '',
            'turnover24h': None,
            'volume24h': None,
            'last_price': None,
            'sort_rank': idx + 1,
        })
    return _catalog_cache_payload(merged, source='fallback', stale=True, error=reason)


def fetch_bybit_linear_usdt_symbol_catalog(limit: int = 120, force_refresh: bool = False) -> dict:
    cache = _read_symbol_catalog_cache()
    if cache and not force_refresh:
        age_seconds = cache.get('age_seconds')
        if age_seconds is not None and age_seconds < SYMBOL_CATALOG_TTL_SECONDS:
            return cache

    import json
    session = requests.Session()
    session.headers.update({'User-Agent': 'BybitPerpsLab/4.1'})

    def _get(path: str, params: dict) -> dict:
        response = session.get(f'{BYBIT_BASE_URL}{path}', params=params, timeout=20)
        response.raise_for_status()
        payload = response.json()
        if str(payload.get('retCode', 0)) not in {'0', 'None'}:
            raise RuntimeError(payload.get('retMsg') or 'Bybit API error')
        return payload.get('result') or {}

    try:
        instruments = []
        cursor = None
        while True:
            params = {'category': 'linear', 'status': 'Trading', 'limit': 1000}
            if cursor:
                params['cursor'] = cursor
            result = _get('/v5/market/instruments-info', params)
            items = result.get('list') or []
            instruments.extend(items)
            cursor = result.get('nextPageCursor')
            if not cursor:
                break
            if len(instruments) > 5000:
                break

        ticker_result = _get('/v5/market/tickers', {'category': 'linear'})
        ticker_map = {item.get('symbol'): item for item in (ticker_result.get('list') or [])}

        symbols = []
        seen = set()
        for item in instruments:
            symbol = str(item.get('symbol', '')).upper()
            if not symbol or symbol in seen:
                continue
            if item.get('quoteCoin') != 'USDT':
                continue
            if item.get('status') != 'Trading':
                continue
            if item.get('contractType') not in {'LinearPerpetual', 'LinearFutures'}:
                continue
            if symbol.endswith('USDC'):
                continue
            seen.add(symbol)
            ticker = ticker_map.get(symbol, {})
            try:
                turnover24h = float(ticker.get('turnover24h')) if ticker.get('turnover24h') not in (None, '') else None
            except Exception:
                turnover24h = None
            try:
                volume24h = float(ticker.get('volume24h')) if ticker.get('volume24h') not in (None, '') else None
            except Exception:
                volume24h = None
            try:
                last_price = float(ticker.get('lastPrice')) if ticker.get('lastPrice') not in (None, '') else None
            except Exception:
                last_price = None
            symbols.append({
                'symbol': symbol,
                'base_coin': item.get('baseCoin'),
                'quote_coin': item.get('quoteCoin'),
                'contract_type': item.get('contractType'),
                'status': item.get('status'),
                'launch_time': item.get('launchTime'),
                'turnover24h': turnover24h,
                'volume24h': volume24h,
                'last_price': last_price,
            })

        symbols.sort(key=lambda row: (
            -(row.get('turnover24h') or -1),
            -(row.get('volume24h') or -1),
            row['symbol'],
        ))
        trimmed = []
        for idx, row in enumerate(symbols[:limit], start=1):
            out = dict(row)
            out['sort_rank'] = idx
            trimmed.append(out)

        payload = _catalog_cache_payload(trimmed, source='bybit')
        _write_symbol_catalog_cache(payload)
        return payload
    except Exception as exc:
        if cache:
            cache['stale'] = True
            cache['source'] = 'cache-fallback'
            cache['error'] = str(exc)
            return cache
        return _fallback_symbol_catalog(str(exc))


def _parse_numeric_epoch(numeric: pd.Series) -> pd.Series:
    result = pd.Series(pd.NaT, index=numeric.index, dtype='datetime64[ns, UTC]')
    valid = numeric.notna()
    if not valid.any():
        return result

    abs_values = numeric[valid].abs()
    masks = {
        'ns': abs_values >= 1e18,
        'us': (abs_values >= 1e15) & (abs_values < 1e18),
        'ms': (abs_values >= 1e12) & (abs_values < 1e15),
        's': abs_values < 1e12,
    }
    for unit, mask in masks.items():
        if mask.any():
            idx = mask.index[mask]
            result.loc[idx] = pd.to_datetime(numeric.loc[idx], unit=unit, utc=True, errors='coerce')
    return result


def _coerce_ts_to_utc_series(value: pd.Series) -> pd.Series:
    series = value.copy()

    if pd.api.types.is_numeric_dtype(series):
        numeric = pd.to_numeric(series, errors='coerce')
        return _parse_numeric_epoch(numeric)

    stripped = series.astype(str).str.strip()
    numeric_mask = stripped.str.fullmatch(r'[+-]?\d+(\.\d+)?').fillna(False)
    result = pd.Series(pd.NaT, index=series.index, dtype='datetime64[ns, UTC]')

    if numeric_mask.any():
        numeric = pd.to_numeric(stripped[numeric_mask], errors='coerce')
        result.loc[numeric_mask] = _parse_numeric_epoch(numeric)

    if (~numeric_mask).any():
        result.loc[~numeric_mask] = pd.to_datetime(stripped[~numeric_mask], utc=True, errors='coerce')

    return result


def _normalize_ts(value: pd.Series) -> pd.Series:
    ts = _coerce_ts_to_utc_series(value)
    return ts.dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def _to_utc_iso(value) -> str:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize('UTC')
    else:
        ts = ts.tz_convert('UTC')
    return ts.strftime('%Y-%m-%dT%H:%M:%SZ')


def normalize_candles(df: pd.DataFrame, symbol: str, interval: str, source: str) -> pd.DataFrame:
    required = {'timestamp', 'open', 'high', 'low', 'close', 'volume'}
    missing = required - set(df.columns)
    if missing:
        raise DataValidationError(f'CSV is missing required columns: {sorted(missing)}')

    clean = df.copy()
    clean['symbol'] = symbol.upper()
    clean['interval'] = interval
    clean['ts'] = _normalize_ts(clean['timestamp'])
    for col in ['open', 'high', 'low', 'close', 'volume']:
        clean[col] = pd.to_numeric(clean[col], errors='coerce')
    clean = clean.dropna(subset=['ts', 'open', 'high', 'low', 'close', 'volume'])
    clean = clean[['symbol', 'interval', 'ts', 'open', 'high', 'low', 'close', 'volume']].copy()
    clean['source'] = source
    clean = clean.sort_values('ts').drop_duplicates(subset=['symbol', 'interval', 'ts'], keep='last')
    if clean.empty:
        raise DataValidationError('No valid candle rows found after normalization.')
    return clean


def normalize_funding(df: pd.DataFrame, symbol: str, source: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=['symbol', 'ts', 'funding_rate', 'source'])
    required = {'timestamp', 'funding_rate'}
    missing = required - set(df.columns)
    if missing:
        raise DataValidationError(f'Funding data is missing required columns: {sorted(missing)}')
    clean = df.copy()
    clean['symbol'] = symbol.upper()
    clean['ts'] = _normalize_ts(clean['timestamp'])
    clean['funding_rate'] = pd.to_numeric(clean['funding_rate'], errors='coerce')
    clean = clean.dropna(subset=['ts', 'funding_rate'])
    clean = clean[['symbol', 'ts', 'funding_rate']].copy()
    clean['source'] = source
    clean = clean.sort_values('ts').drop_duplicates(subset=['symbol', 'ts'], keep='last')
    return clean


def upsert_candles(df: pd.DataFrame) -> int:
    rows = list(df.itertuples(index=False, name=None))
    with get_conn() as conn:
        conn.executemany(
            '''
            INSERT INTO candles(symbol, interval, ts, open, high, low, close, volume, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, interval, ts)
            DO UPDATE SET open=excluded.open, high=excluded.high, low=excluded.low,
                          close=excluded.close, volume=excluded.volume, source=excluded.source
            ''',
            rows,
        )
        conn.commit()
    return len(rows)


def upsert_funding(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = list(df.itertuples(index=False, name=None))
    with get_conn() as conn:
        conn.executemany(
            '''
            INSERT INTO funding_rates(symbol, ts, funding_rate, source)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(symbol, ts)
            DO UPDATE SET funding_rate=excluded.funding_rate, source=excluded.source
            ''',
            rows,
        )
        conn.commit()
    return len(rows)


def list_symbols(interval: str = '5') -> list[str]:
    with get_conn() as conn:
        rows = conn.execute(
            'SELECT DISTINCT symbol FROM candles WHERE interval = ? ORDER BY symbol',
            (interval,),
        ).fetchall()
    return [row[0] for row in rows]


def load_candles(symbols: Iterable[str], interval: str = '5') -> pd.DataFrame:
    symbols = [s.upper() for s in symbols]
    if not symbols:
        return pd.DataFrame(columns=['symbol', 'ts', 'open', 'high', 'low', 'close', 'volume'])
    placeholders = ','.join(['?'] * len(symbols))
    query = f'''
        SELECT symbol, interval, ts, open, high, low, close, volume, source
        FROM candles
        WHERE interval = ? AND symbol IN ({placeholders})
        ORDER BY symbol, ts
    '''
    params = [interval, *symbols]
    with get_conn() as conn:
        df = pd.read_sql_query(query, conn, params=params)
    if df.empty:
        return df
    df['ts'] = _coerce_ts_to_utc_series(df['ts'])
    return df


def load_funding(symbols: Iterable[str]) -> pd.DataFrame:
    symbols = [s.upper() for s in symbols]
    if not symbols:
        return pd.DataFrame(columns=['symbol', 'ts', 'funding_rate'])
    placeholders = ','.join(['?'] * len(symbols))
    query = f'''
        SELECT symbol, ts, funding_rate, source
        FROM funding_rates
        WHERE symbol IN ({placeholders})
        ORDER BY symbol, ts
    '''
    with get_conn() as conn:
        df = pd.read_sql_query(query, conn, params=symbols)
    if df.empty:
        return df
    df['ts'] = _coerce_ts_to_utc_series(df['ts'])
    return df


def import_csv_file(file_storage, symbol: str, interval: str) -> dict:
    if interval not in SUPPORTED_INTERVALS:
        raise DataValidationError(f'Unsupported interval: {interval}')
    content = file_storage.read()
    df = pd.read_csv(io.BytesIO(content))
    candles = normalize_candles(df, symbol=symbol, interval=interval, source='csv-upload')
    inserted = upsert_candles(candles)
    return {
        'symbol': symbol.upper(),
        'interval': interval,
        'rows_inserted': inserted,
    }


def _synthetic_path(base_price: float, periods: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    drift = rng.normal(0.00015, 0.00012, periods)
    shock = rng.normal(0, 0.0045, periods)
    regime = np.sin(np.linspace(0, 12 * math.pi, periods)) * 0.0012
    returns = drift + shock + regime
    close = base_price * np.exp(np.cumsum(returns))
    open_ = np.insert(close[:-1], 0, base_price)
    spread = np.maximum(close * (0.0018 + np.abs(rng.normal(0, 0.0009, periods))), 0.25)
    high = np.maximum(open_, close) + spread
    low = np.minimum(open_, close) - spread
    volume = rng.lognormal(mean=10.3, sigma=0.35, size=periods)
    return pd.DataFrame({
        'open': open_,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume,
    })


def generate_demo_market_data(symbols: list[str], interval: str = '5', days: int = 20) -> dict:
    if interval != '5':
        raise DataValidationError('Demo generator currently supports 5-minute data only.')
    periods = max(days * 24 * 12, 800)
    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    ts = pd.date_range(end=end, periods=periods, freq='5min', tz='UTC')

    defaults = {
        'BTCUSDT': 65000,
        'ETHUSDT': 3400,
        'SOLUSDT': 140,
        'XRPUSDT': 0.62,
        'BNBUSDT': 560,
        'DOGEUSDT': 0.18,
    }

    total_candles = 0
    total_funding = 0
    for idx, symbol in enumerate(symbols):
        base = defaults.get(symbol.upper(), 100 + idx * 15)
        candle_df = _synthetic_path(base_price=base, periods=periods, seed=42 + idx)
        candle_df.insert(0, 'timestamp', ts)
        candles = normalize_candles(candle_df, symbol=symbol, interval=interval, source='demo-generator')
        total_candles += upsert_candles(candles)

        funding_ts = pd.date_range(end=end, periods=max(8, days * 3), freq='8h', tz='UTC')
        funding_rng = np.random.default_rng(100 + idx)
        funding_df = pd.DataFrame({
            'timestamp': funding_ts,
            'funding_rate': funding_rng.normal(0, 0.00015, len(funding_ts)),
        })
        funding = normalize_funding(funding_df, symbol=symbol, source='demo-generator')
        total_funding += upsert_funding(funding)

    return {
        'symbols': [s.upper() for s in symbols],
        'interval': interval,
        'candles_upserted': total_candles,
        'funding_upserted': total_funding,
    }


def _fetch_bybit_kline_chunk(symbol: str, interval: str, start_ms: int | None = None, end_ms: int | None = None, limit: int = 1000) -> list[list[str]]:
    params = {
        'category': 'linear',
        'symbol': symbol.upper(),
        'interval': interval,
        'limit': limit,
    }
    if start_ms is not None:
        params['start'] = int(start_ms)
    if end_ms is not None:
        params['end'] = int(end_ms)
    response = requests.get(f'{BYBIT_BASE_URL}/v5/market/kline', params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if payload.get('retCode') != 0:
        raise RuntimeError(payload.get('retMsg', 'Bybit kline request failed'))
    return payload['result']['list']


def _fetch_bybit_funding_chunk(symbol: str, end_ms: int | None = None, limit: int = 200) -> list[dict]:
    params = {
        'category': 'linear',
        'symbol': symbol.upper(),
        'limit': limit,
    }
    if end_ms is not None:
        params['endTime'] = int(end_ms)
    response = requests.get(f'{BYBIT_BASE_URL}/v5/market/funding/history', params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if payload.get('retCode') != 0:
        raise RuntimeError(payload.get('retMsg', 'Bybit funding request failed'))
    return payload['result']['list']


def sync_bybit_public(symbols: list[str], interval: str = '5', days: int = 14) -> dict:
    if interval not in SUPPORTED_INTERVALS:
        raise DataValidationError(f'Unsupported interval: {interval}')
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    total_candles = 0
    total_funding = 0

    for symbol in symbols:
        all_rows: list[list[str]] = []
        cursor_end = end_ms
        while True:
            chunk = _fetch_bybit_kline_chunk(symbol=symbol, interval=interval, start_ms=start_ms, end_ms=cursor_end, limit=1000)
            if not chunk:
                break
            all_rows.extend(chunk)
            oldest = min(int(row[0]) for row in chunk)
            if oldest <= start_ms or len(chunk) < 1000:
                break
            cursor_end = oldest - 1

        if all_rows:
            kline_df = pd.DataFrame(all_rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
            candles = normalize_candles(kline_df, symbol=symbol, interval=interval, source='bybit-public-api')
            candles = candles[candles['ts'] >= _to_utc_iso(start)]
            total_candles += upsert_candles(candles)

        funding_rows: list[dict] = []
        cursor_end = end_ms
        while True:
            chunk = _fetch_bybit_funding_chunk(symbol=symbol, end_ms=cursor_end, limit=200)
            if not chunk:
                break
            funding_rows.extend(chunk)
            oldest = min(int(row['fundingRateTimestamp']) for row in chunk)
            if oldest <= start_ms or len(chunk) < 200:
                break
            cursor_end = oldest - 1
        if funding_rows:
            funding_df = pd.DataFrame({
                'timestamp': [row['fundingRateTimestamp'] for row in funding_rows],
                'funding_rate': [row['fundingRate'] for row in funding_rows],
            })
            funding = normalize_funding(funding_df, symbol=symbol, source='bybit-public-api')
            funding = funding[funding['ts'] >= _to_utc_iso(start)]
            total_funding += upsert_funding(funding)

    return {
        'symbols': [s.upper() for s in symbols],
        'interval': interval,
        'days': days,
        'candles_upserted': total_candles,
        'funding_upserted': total_funding,
    }
