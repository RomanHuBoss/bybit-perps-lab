from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import pandas as pd

from services.indicators import prepare_symbol_features


@dataclass
class StrategySignal:
    ts: pd.Timestamp
    symbol: str
    regime: str
    side: str
    score: float
    entry_price: float
    stop_price: float
    tp1_price: float
    tp2_price: float
    stop_distance: float
    tp1_r_multiple: float
    tp2_r_multiple: float
    notes: str


class StrategyFactory:
    def __init__(self, settings: dict[str, Any]):
        self.settings = settings

    def build_symbol_signals(self, symbol_df: pd.DataFrame, funding_df: pd.DataFrame | None = None) -> tuple[pd.DataFrame, list[StrategySignal]]:
        features = prepare_symbol_features(symbol_df)
        if funding_df is not None and not funding_df.empty:
            tmp = funding_df.copy().sort_values('ts')
            features = pd.merge_asof(
                features.sort_values('ts'),
                tmp[['ts', 'funding_rate']].sort_values('ts'),
                on='ts',
                direction='backward',
                tolerance=pd.Timedelta('8h'),
            )
            features['funding_rate'] = features['funding_rate'].fillna(0.0)
        else:
            features['funding_rate'] = 0.0

        signals: list[StrategySignal] = []
        symbol = str(features['symbol'].iloc[0]) if not features.empty else ''
        funding_cap = float(self.settings.get('funding_rate_cap_abs', 0.0005))
        volume_multiplier = float(self.settings.get('volume_multiplier', 1.2))
        trend_stop_mult = float(self.settings.get('atr_stop_mult_trend', 1.4))
        rev_stop_mult = float(self.settings.get('atr_stop_mult_reversion', 1.0))
        trend_tp1 = float(self.settings.get('trend_tp1_r', 1.0))
        trend_tp2 = float(self.settings.get('trend_tp2_r', 2.2))
        rev_tp1 = float(self.settings.get('reversion_tp1_r', 0.8))
        rev_tp2 = float(self.settings.get('reversion_tp2_r', 1.5))
        trend_strength_min = float(self.settings.get('trend_strength_min', 0.0025))
        reversion_zscore_threshold = float(self.settings.get('reversion_zscore_threshold', 2.1))
        chop_trend_strength_max = float(self.settings.get('chop_trend_strength_max', 0.0015))
        chop_abs_dev_zscore_max = float(self.settings.get('chop_abs_dev_zscore_max', 1.0))
        reversion_volatility_min = float(self.settings.get('reversion_volatility_min', 0.35))
        volatility_score_min = float(self.settings.get('volatility_score_min', 0.18))
        volatility_score_max = float(self.settings.get('volatility_score_max', 0.92))
        volatility_filter_enabled = bool(self.settings.get('volatility_filter_enabled', True))
        regime_filter_enabled = bool(self.settings.get('regime_filter_enabled', True))
        no_trade_filter_enabled = bool(self.settings.get('no_trade_filter_enabled', True))

        for row in features.itertuples(index=False):
            required_cols = [
                'atr_14', 'vol_sma_20', 'h4_ema_50', 'h4_ema_200', 'h1_ema_20', 'h1_ema_50', 'm15_break_high_8', 'm15_break_low_8',
                'trend_strength', 'volatility_score', 'dev_zscore',
            ]
            if any(pd.isna(getattr(row, col)) for col in required_cols):
                continue

            close = float(row.close)
            atr_14 = float(row.atr_14)
            funding = float(row.funding_rate)
            trend_strength = float(row.trend_strength)
            volatility_score = float(row.volatility_score)
            dev_zscore = float(row.dev_zscore)
            market_regime = self._classify_regime(
                trend_alignment=int(row.trend_alignment),
                trend_strength=trend_strength,
                dev_zscore=dev_zscore,
                volatility_score=volatility_score,
                trend_strength_min=trend_strength_min,
                chop_trend_strength_max=chop_trend_strength_max,
                chop_abs_dev_zscore_max=chop_abs_dev_zscore_max,
                reversion_volatility_min=reversion_volatility_min,
                reversion_zscore_threshold=reversion_zscore_threshold,
            )

            if volatility_filter_enabled and not (volatility_score_min <= volatility_score <= volatility_score_max):
                continue
            if no_trade_filter_enabled and market_regime in {'chop', 'neutral'}:
                continue

            notes: list[str] = [
                f'ctx:{market_regime}',
                f'vol:{volatility_score:.2f}',
                f'trend:{trend_strength:.4f}',
            ]
            trend_score = 0.0
            trend_side = None
            # Compare volume on the same 5m timeframe. Using a 5m bar against a 15m SMA
            # suppresses valid breakout signals by mixing incompatible units.
            volume_ratio_5m = row.volume / max(row.vol_sma_20, 1e-9)

            if bool(self.settings.get('trend_enabled', True)):
                if regime_filter_enabled and market_regime != 'trend':
                    pass
                elif row.h4_ema_50 > row.h4_ema_200 and row.h1_close >= row.h1_ema_20 and close > row.m15_break_high_8 and volume_ratio_5m > volume_multiplier and funding <= funding_cap:
                    trend_score = self._bounded_score(
                        0.45
                        + self._scale_positive((row.h4_ema_50 - row.h4_ema_200) / max(close, 1e-9), 0.0, 0.03) * 0.16
                        + self._scale_positive((close - row.m15_break_high_8) / max(close, 1e-9), 0.0, 0.01) * 0.16
                        + self._scale_positive(volume_ratio_5m - 1.0, 0.0, 1.4) * 0.1
                        + self._scale_positive(trend_strength, trend_strength_min * 0.7, trend_strength_min * 3.0) * 0.13
                        + self._scale_positive(volatility_score, volatility_score_min, volatility_score_max) * 0.1
                        + self._scale_positive(-funding, -funding_cap, funding_cap) * 0.1
                    )
                    trend_side = 'LONG'
                    notes.append('trend-long breakout')
                elif row.h4_ema_50 < row.h4_ema_200 and row.h1_close <= row.h1_ema_20 and close < row.m15_break_low_8 and volume_ratio_5m > volume_multiplier and funding >= -funding_cap:
                    trend_score = self._bounded_score(
                        0.45
                        + self._scale_positive((row.h4_ema_200 - row.h4_ema_50) / max(close, 1e-9), 0.0, 0.03) * 0.16
                        + self._scale_positive((row.m15_break_low_8 - close) / max(close, 1e-9), 0.0, 0.01) * 0.16
                        + self._scale_positive(volume_ratio_5m - 1.0, 0.0, 1.4) * 0.1
                        + self._scale_positive(trend_strength, trend_strength_min * 0.7, trend_strength_min * 3.0) * 0.13
                        + self._scale_positive(volatility_score, volatility_score_min, volatility_score_max) * 0.1
                        + self._scale_positive(funding, -funding_cap, funding_cap) * 0.1
                    )
                    trend_side = 'SHORT'
                    notes.append('trend-short breakdown')

            rev_score = 0.0
            rev_side = None
            if bool(self.settings.get('reversion_enabled', True)) and not math.isnan(dev_zscore):
                candle_bias = (close - row.low) / max(row.high - row.low, 1e-9)
                if regime_filter_enabled and market_regime != 'reversion':
                    pass
                elif dev_zscore <= -reversion_zscore_threshold and close > row.open and candle_bias > 0.65 and row.low <= row.rolling_low_20 and funding <= funding_cap:
                    rev_score = self._bounded_score(
                        0.4
                        + self._scale_positive(-dev_zscore, reversion_zscore_threshold - 0.1, 4.5) * 0.22
                        + self._scale_positive(candle_bias, 0.5, 1.0) * 0.12
                        + self._scale_positive(volatility_score, reversion_volatility_min, volatility_score_max) * 0.12
                        + self._scale_positive(-funding, -funding_cap, funding_cap) * 0.08
                        + self._scale_positive((row.vwap_48 - close) / max(close, 1e-9), 0.0, 0.03) * 0.12
                    )
                    rev_side = 'LONG'
                    notes.append('reversion-long exhaustion')
                elif dev_zscore >= reversion_zscore_threshold and close < row.open and candle_bias < 0.35 and row.high >= row.rolling_high_20 and funding >= -funding_cap:
                    rev_score = self._bounded_score(
                        0.4
                        + self._scale_positive(dev_zscore, reversion_zscore_threshold - 0.1, 4.5) * 0.22
                        + self._scale_positive(1.0 - candle_bias, 0.5, 1.0) * 0.12
                        + self._scale_positive(volatility_score, reversion_volatility_min, volatility_score_max) * 0.12
                        + self._scale_positive(funding, -funding_cap, funding_cap) * 0.08
                        + self._scale_positive((close - row.vwap_48) / max(close, 1e-9), 0.0, 0.03) * 0.12
                    )
                    rev_side = 'SHORT'
                    notes.append('reversion-short exhaustion')

            selected = None
            if trend_side and rev_side:
                selected = ('trend', trend_side, trend_score) if trend_score >= rev_score else ('reversion', rev_side, rev_score)
            elif trend_side:
                selected = ('trend', trend_side, trend_score)
            elif rev_side:
                selected = ('reversion', rev_side, rev_score)

            if not selected:
                continue

            regime, side, score = selected
            stop_mult = trend_stop_mult if regime == 'trend' else rev_stop_mult
            stop_distance = max(atr_14 * stop_mult, close * 0.0025)
            if side == 'LONG':
                stop = close - stop_distance
                tp1 = close + stop_distance * (trend_tp1 if regime == 'trend' else rev_tp1)
                tp2 = close + stop_distance * (trend_tp2 if regime == 'trend' else rev_tp2)
            else:
                stop = close + stop_distance
                tp1 = close - stop_distance * (trend_tp1 if regime == 'trend' else rev_tp1)
                tp2 = close - stop_distance * (trend_tp2 if regime == 'trend' else rev_tp2)

            tp1_r_multiple = float(trend_tp1 if regime == 'trend' else rev_tp1)
            tp2_r_multiple = float(trend_tp2 if regime == 'trend' else rev_tp2)
            signals.append(
                StrategySignal(
                    ts=row.ts,
                    symbol=symbol,
                    regime=regime,
                    side=side,
                    score=round(score, 4),
                    entry_price=round(close, 8),
                    stop_price=round(stop, 8),
                    tp1_price=round(tp1, 8),
                    tp2_price=round(tp2, 8),
                    stop_distance=round(stop_distance, 8),
                    tp1_r_multiple=round(tp1_r_multiple, 8),
                    tp2_r_multiple=round(tp2_r_multiple, 8),
                    notes='; '.join(notes),
                )
            )

        return features, signals

    @staticmethod
    def _classify_regime(
        trend_alignment: int,
        trend_strength: float,
        dev_zscore: float,
        volatility_score: float,
        trend_strength_min: float,
        chop_trend_strength_max: float,
        chop_abs_dev_zscore_max: float,
        reversion_volatility_min: float,
        reversion_zscore_threshold: float,
    ) -> str:
        if abs(dev_zscore) >= reversion_zscore_threshold and volatility_score >= reversion_volatility_min:
            return 'reversion'
        if trend_alignment != 0 and trend_strength >= trend_strength_min:
            return 'trend'
        if trend_strength <= chop_trend_strength_max and abs(dev_zscore) <= chop_abs_dev_zscore_max:
            return 'chop'
        return 'neutral'

    @staticmethod
    def _scale_positive(value: float, lower: float, upper: float) -> float:
        if upper == lower:
            return 0.0
        clipped = min(max(value, lower), upper)
        return max((clipped - lower) / (upper - lower), 0.0)

    @staticmethod
    def _bounded_score(value: float) -> float:
        return max(0.0, min(0.99, value))
