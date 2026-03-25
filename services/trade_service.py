from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, InvalidOperation
from typing import Any

import pandas as pd
import requests

from config import DEFAULT_SETTINGS
from database import log_live_order
from services.data_service import load_candles, load_funding
from services.live_adapter import live_adapter
from services.strategy_engine import StrategyFactory, StrategySignal
from services.simulator import SimulationHelpers


LOCAL_INSTRUMENT_FALLBACKS: dict[str, dict[str, str]] = {
    'BTCUSDT': {'tickSize': '0.10', 'qtyStep': '0.001', 'minOrderQty': '0.001', 'minNotionalValue': '5'},
    'ETHUSDT': {'tickSize': '0.01', 'qtyStep': '0.01', 'minOrderQty': '0.01', 'minNotionalValue': '5'},
    'SOLUSDT': {'tickSize': '0.001', 'qtyStep': '0.1', 'minOrderQty': '0.1', 'minNotionalValue': '5'},
    'XRPUSDT': {'tickSize': '0.0001', 'qtyStep': '1', 'minOrderQty': '1', 'minNotionalValue': '5'},
    'DOGEUSDT': {'tickSize': '0.00001', 'qtyStep': '1', 'minOrderQty': '1', 'minNotionalValue': '5'},
}


@dataclass
class SignalView:
    symbol: str
    last_bar_ts: str | None
    signal: dict[str, Any] | None
    bars_since_signal: int | None
    has_fresh_signal: bool
    note: str


class TradePlanner:
    def __init__(self, settings: dict[str, Any]):
        self.settings = settings
        self.strategy = StrategyFactory(settings)

    def get_latest_signals(self, symbols: list[str], interval: str = '5') -> list[dict[str, Any]]:
        symbols = [s.upper() for s in symbols]
        candles = load_candles(symbols, interval=interval)
        funding = load_funding(symbols)
        if candles.empty:
            raise ValueError('Нет локальных свечей. Сначала загрузите demo-данные, CSV или синхронизируйте Bybit public.')
        max_age_bars = int(self.settings.get('live_signal_max_age_bars', DEFAULT_SETTINGS['live_signal_max_age_bars']))
        views: list[dict[str, Any]] = []
        for symbol in symbols:
            sym_candles = candles[candles['symbol'] == symbol].copy()
            if sym_candles.empty:
                views.append(asdict(SignalView(symbol, None, None, None, False, 'Нет свечей по символу.')))
                continue
            sym_funding = funding[funding['symbol'] == symbol].copy() if not funding.empty else pd.DataFrame()
            features, signals = self.strategy.build_symbol_signals(sym_candles, sym_funding)
            if features.empty:
                views.append(asdict(SignalView(symbol, None, None, None, False, 'Недостаточно истории для features.')))
                continue
            last_bar_ts = pd.Timestamp(features['ts'].iloc[-1])
            if not signals:
                views.append(asdict(SignalView(symbol, _iso(last_bar_ts), None, None, False, 'Свежего сигнала нет.')))
                continue
            last_signal = signals[-1]
            feature_idx = features.index[features['ts'] == last_bar_ts]
            signal_idx = features.index[features['ts'] == last_signal.ts]
            bars_since = int(feature_idx[-1] - signal_idx[-1]) if len(feature_idx) and len(signal_idx) else None
            has_fresh = bars_since is not None and bars_since <= max_age_bars
            note = 'OK' if has_fresh else f'Сигнал устарел на {bars_since} бар(ов).' if bars_since is not None else 'Не удалось посчитать age сигнала.'
            views.append(
                asdict(
                    SignalView(
                        symbol=symbol,
                        last_bar_ts=_iso(last_bar_ts),
                        signal=_signal_to_dict(last_signal),
                        bars_since_signal=bars_since,
                        has_fresh_signal=has_fresh,
                        note=note,
                    )
                )
            )
        views.sort(key=lambda row: (0 if row['has_fresh_signal'] else 1, -(row['signal']['score'] if row['signal'] else -1)))
        return views

    def build_trade_plan(self, symbol: str, fixed_notional_usdt: float | None = None, require_fresh_signal: bool = True) -> dict[str, Any]:
        symbol = symbol.upper()
        fixed_notional_usdt = float(fixed_notional_usdt if fixed_notional_usdt is not None else self.settings.get('live_fixed_notional_usdt', 10.0))
        signal_view = self.get_latest_signals([symbol])[0]
        if not signal_view['signal']:
            raise ValueError(f'{symbol}: нет сигнала для входа.')
        if require_fresh_signal and not signal_view['has_fresh_signal']:
            raise ValueError(f'{symbol}: сигнал не свежий. {signal_view["note"]}')

        signal = signal_view['signal']
        market = self._get_market_snapshot(symbol, signal_side=signal['side'])
        entry_price = Decimal(str(market['entry_price']))
        instrument = market['instrument']
        tick = Decimal(str(instrument['tickSize']))
        qty_step = Decimal(str(instrument['qtyStep']))
        min_qty = Decimal(str(instrument['minOrderQty']))
        min_notional = Decimal(str(instrument['minNotionalValue']))

        requested_notional = Decimal(str(fixed_notional_usdt))
        raw_qty = requested_notional / entry_price
        qty = _round_down_step(raw_qty, qty_step)
        if qty < min_qty:
            qty = min_qty
        actual_notional = qty * entry_price
        if actual_notional < min_notional:
            qty = _round_up_min_notional(min_notional, entry_price, qty_step)
            actual_notional = qty * entry_price

        stop_distance = float(signal.get('stop_distance') or abs(float(signal['entry_price']) - float(signal['stop_price'])))
        tp1_r_multiple = float(signal.get('tp1_r_multiple') or (abs(float(signal['tp1_price']) - float(signal['entry_price'])) / max(stop_distance, 1e-9)))
        tp2_r_multiple = float(signal.get('tp2_r_multiple') or (abs(float(signal['tp2_price']) - float(signal['entry_price'])) / max(stop_distance, 1e-9)))
        recomputed_stop, _, recomputed_tp2 = SimulationHelpers.recompute_levels(float(entry_price), signal['side'], stop_distance, tp1_r_multiple, tp2_r_multiple)
        stop_price = _round_to_tick(Decimal(str(recomputed_stop)), tick)
        tp_price = _round_to_tick(Decimal(str(recomputed_tp2)), tick)
        risk_to_stop = abs(entry_price - stop_price) * qty
        reward_to_tp = abs(tp_price - entry_price) * qty
        min_notional_estimate = max(min_notional, min_qty * entry_price)

        if requested_notional < min_notional_estimate:
            affordable = False
            affordability_note = f'${requested_notional} меньше минимального реального размера для {symbol}: около ${min_notional_estimate.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)}.'
        else:
            affordable = True
            affordability_note = 'OK'

        plan = {
            'symbol': symbol,
            'side': 'Buy' if signal['side'] == 'LONG' else 'Sell',
            'requested_notional_usdt': float(requested_notional),
            'actual_notional_usdt': float(actual_notional),
            'qty': _decimal_to_str(qty),
            'entry_price': float(entry_price),
            'stop_price': float(stop_price),
            'take_profit_price': float(tp_price),
            'risk_to_stop_usdt': float(risk_to_stop),
            'reward_to_take_profit_usdt': float(reward_to_tp),
            'rr_to_tp2': float((reward_to_tp / risk_to_stop) if risk_to_stop > 0 else Decimal('0')),
            'affordable_for_requested_size': affordable,
            'affordability_note': affordability_note,
            'bars_since_signal': signal_view['bars_since_signal'],
            'signal': signal,
            'market': market,
        }
        return plan

    def execute_trade_plan(self, plan: dict[str, Any]) -> dict[str, Any]:
        response_payload = live_adapter.place_order(
            symbol=plan['symbol'],
            side=plan['side'],
            qty=str(plan['qty']),
            order_type='Market',
            time_in_force='IOC',
            reduce_only=False,
            take_profit=_float_to_clean_str(plan['take_profit_price']),
            stop_loss=_float_to_clean_str(plan['stop_price']),
            tpsl_mode='Full',
            tp_order_type='Market',
            sl_order_type='Market',
        )
        mode = response_payload.get('mode', 'live')
        log_id = log_live_order(plan, response_payload, mode=mode)
        return {
            'log_id': log_id,
            'plan': plan,
            'adapter_response': response_payload,
        }

    def _get_market_snapshot(self, symbol: str, signal_side: str | None = None) -> dict[str, Any]:
        ticker = None
        instrument = None
        source = 'bybit-public'
        try:
            ticker = live_adapter.public_ticker(symbol)
            info = live_adapter.public_instrument_info(symbol)
            instrument = {
                'tickSize': info['priceFilter']['tickSize'],
                'qtyStep': info['lotSizeFilter']['qtyStep'],
                'minOrderQty': info['lotSizeFilter']['minOrderQty'],
                'minNotionalValue': info['lotSizeFilter']['minNotionalValue'],
            }
            preferred_field = 'ask1Price' if signal_side != 'SHORT' else 'bid1Price'
            entry_price = float(
                ticker.get(preferred_field)
                or ticker.get('lastPrice')
                or ticker.get('markPrice')
                or ticker.get('ask1Price')
                or ticker.get('bid1Price')
            )
            return {
                'source': source,
                'entry_price': entry_price,
                'entry_price_field': preferred_field,
                'ticker': ticker,
                'instrument': instrument,
            }
        except (requests.RequestException, RuntimeError, KeyError, InvalidOperation, ValueError):
            fallback = LOCAL_INSTRUMENT_FALLBACKS.get(symbol, {'tickSize': '0.01', 'qtyStep': '0.1', 'minOrderQty': '0.1', 'minNotionalValue': '5'})
            candles = load_candles([symbol], interval='5')
            if candles.empty:
                raise ValueError(f'Не удалось получить публичные данные и нет локальных свечей по {symbol}.')
            last_close = float(candles[candles['symbol'] == symbol]['close'].iloc[-1])
            return {
                'source': 'local-fallback',
                'entry_price': last_close,
                'entry_price_field': 'last_close',
                'ticker': {'symbol': symbol, 'lastPrice': str(last_close)},
                'instrument': fallback,
            }


def _iso(ts: pd.Timestamp | None) -> str | None:
    if ts is None:
        return None
    return pd.Timestamp(ts).isoformat().replace('+00:00', 'Z')


def _signal_to_dict(signal: StrategySignal) -> dict[str, Any]:
    return {
        'ts': _iso(signal.ts),
        'symbol': signal.symbol,
        'regime': signal.regime,
        'side': signal.side,
        'score': signal.score,
        'entry_price': signal.entry_price,
        'stop_price': signal.stop_price,
        'tp1_price': signal.tp1_price,
        'tp2_price': signal.tp2_price,
        'stop_distance': signal.stop_distance,
        'tp1_r_multiple': signal.tp1_r_multiple,
        'tp2_r_multiple': signal.tp2_r_multiple,
        'notes': signal.notes,
    }


def _round_down_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    units = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return units * step


def _round_up_min_notional(min_notional: Decimal, entry_price: Decimal, step: Decimal) -> Decimal:
    if entry_price <= 0:
        raise ValueError('entry_price must be positive.')
    raw_qty = min_notional / entry_price
    units = (raw_qty / step).to_integral_value(rounding=ROUND_DOWN)
    qty = units * step
    if qty * entry_price < min_notional:
        qty += step
    return qty


def _round_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        return value
    units = (value / tick).to_integral_value(rounding=ROUND_HALF_UP)
    return units * tick


def _decimal_to_str(value: Decimal) -> str:
    normalized = value.normalize()
    text = format(normalized, 'f')
    return text.rstrip('0').rstrip('.') if '.' in text else text


def _float_to_clean_str(value: float) -> str:
    dec = Decimal(str(value)).normalize()
    text = format(dec, 'f')
    return text.rstrip('0').rstrip('.') if '.' in text else text
