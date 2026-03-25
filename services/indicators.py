from __future__ import annotations

import numpy as np
import pandas as pd


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    prev_close = df['close'].shift(1)
    tr = pd.concat(
        [
            (df['high'] - df['low']).abs(),
            (df['high'] - prev_close).abs(),
            (df['low'] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(period, min_periods=period).mean()


def rolling_vwap(df: pd.DataFrame, window: int = 48) -> pd.Series:
    typical = (df['high'] + df['low'] + df['close']) / 3.0
    pv = typical * df['volume']
    return pv.rolling(window, min_periods=window).sum() / df['volume'].rolling(window, min_periods=window).sum()


def rolling_minmax_scale(series: pd.Series, window: int = 288, min_periods: int | None = None) -> pd.Series:
    min_periods = min_periods or max(48, window // 4)
    roll_min = series.rolling(window, min_periods=min_periods).min()
    roll_max = series.rolling(window, min_periods=min_periods).max()
    denom = (roll_max - roll_min).replace(0, np.nan)
    scaled = (series - roll_min) / denom
    return scaled.clip(0, 1)


def resample_ohlcv(symbol_df: pd.DataFrame, rule: str) -> pd.DataFrame:
    frame = symbol_df.copy().set_index('ts')
    out = frame.resample(rule).agg(
        {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
        }
    ).dropna()
    out.index.name = 'ts'
    return out


def align_feature(feature_df: pd.DataFrame, base_index: pd.DatetimeIndex, prefix: str) -> pd.DataFrame:
    aligned = feature_df.reindex(base_index, method='ffill')
    aligned.columns = [f'{prefix}_{col}' for col in aligned.columns]
    return aligned


def prepare_symbol_features(symbol_df: pd.DataFrame) -> pd.DataFrame:
    base = symbol_df.copy().sort_values('ts').set_index('ts')
    base['ret_1'] = base['close'].pct_change()
    base['atr_14'] = atr(base.reset_index()).values
    base['atr_pct'] = (base['atr_14'] / base['close'].replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)
    base['range_pct'] = ((base['high'] - base['low']) / base['close'].replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)
    base['vol_sma_20'] = base['volume'].rolling(20, min_periods=20).mean()
    base['rolling_high_12'] = base['high'].rolling(12, min_periods=12).max().shift(1)
    base['rolling_low_12'] = base['low'].rolling(12, min_periods=12).min().shift(1)
    base['rolling_high_20'] = base['high'].rolling(20, min_periods=20).max().shift(1)
    base['rolling_low_20'] = base['low'].rolling(20, min_periods=20).min().shift(1)
    base['vwap_48'] = rolling_vwap(base.reset_index(), 48).values
    dev = (base['close'] - base['vwap_48']) / base['close'].replace(0, np.nan)
    base['dev_mean_96'] = dev.rolling(96, min_periods=40).mean()
    base['dev_std_96'] = dev.rolling(96, min_periods=40).std().replace(0, np.nan)
    base['dev_zscore'] = (dev - base['dev_mean_96']) / base['dev_std_96']
    base['rv_48'] = base['ret_1'].rolling(48, min_periods=24).std()
    base['rv_288'] = base['ret_1'].rolling(288, min_periods=96).std()
    base['volatility_score'] = rolling_minmax_scale(base['atr_pct'], window=288, min_periods=72)

    f15 = resample_ohlcv(symbol_df, '15min')
    f15['break_high_8'] = f15['high'].rolling(8, min_periods=8).max().shift(1)
    f15['break_low_8'] = f15['low'].rolling(8, min_periods=8).min().shift(1)
    f15['vol_sma_20'] = f15['volume'].rolling(20, min_periods=20).mean()

    h1 = resample_ohlcv(symbol_df, '1h')
    h1['ema_20'] = ema(h1['close'], 20)
    h1['ema_50'] = ema(h1['close'], 50)
    h1['atr_14'] = atr(h1.reset_index()).values
    h1['atr_pct'] = (h1['atr_14'] / h1['close'].replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)

    h4 = resample_ohlcv(symbol_df, '4h')
    h4['ema_50'] = ema(h4['close'], 50)
    h4['ema_200'] = ema(h4['close'], 200)

    # Use only fully closed higher-timeframe bars. Without this shift, a 5m bar can
    # see aggregated values from the still-forming 15m/1h/4h candle, which creates
    # look-ahead bias in backtests and live signal previews.
    f15_closed = f15[['break_high_8', 'break_low_8', 'vol_sma_20']].shift(1)
    h1_closed = h1[['close', 'ema_20', 'ema_50', 'atr_14', 'atr_pct']].shift(1)
    h4_closed = h4[['close', 'ema_50', 'ema_200']].shift(1)

    combined = base.copy()
    combined = combined.join(align_feature(f15_closed, combined.index, 'm15'))
    combined = combined.join(align_feature(h1_closed, combined.index, 'h1'))
    combined = combined.join(align_feature(h4_closed, combined.index, 'h4'))

    combined['trend_strength'] = (
        ((combined['h4_ema_50'] - combined['h4_ema_200']).abs() / combined['close'].replace(0, np.nan)) * 0.65
        + ((combined['h1_ema_20'] - combined['h1_ema_50']).abs() / combined['close'].replace(0, np.nan)) * 0.35
    )
    combined['trend_alignment'] = np.where(
        (combined['h4_ema_50'] > combined['h4_ema_200']) & (combined['h1_ema_20'] > combined['h1_ema_50']),
        1,
        np.where(
            (combined['h4_ema_50'] < combined['h4_ema_200']) & (combined['h1_ema_20'] < combined['h1_ema_50']),
            -1,
            0,
        ),
    )
    combined = combined.reset_index()
    return combined
