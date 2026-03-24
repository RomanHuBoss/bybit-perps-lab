from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Any

import numpy as np
import pandas as pd

from services.strategy_engine import StrategyFactory, StrategySignal


@dataclass
class Position:
    symbol: str
    regime: str
    side: str
    entry_ts: pd.Timestamp
    entry_price: float
    stop_price: float
    tp1_price: float
    tp2_price: float
    qty: float
    initial_qty: float
    risk_per_unit: float
    score: float
    notes: str
    max_hold_bars: int
    bars_held: int = 0
    tp1_taken: bool = False
    fees: float = 0.0
    funding_pnl: float = 0.0
    realized_gross: float = 0.0


@dataclass
class PreparedMarket:
    symbol_bars: dict[str, pd.DataFrame]
    signals_by_symbol: dict[str, dict[pd.Timestamp, StrategySignal]]
    signal_rows: list[dict[str, Any]]
    funding_maps: dict[str, dict[pd.Timestamp, float]]
    all_times: list[pd.Timestamp]


class SimulationHelpers:
    @staticmethod
    def count_same_side_positions(positions: dict[str, Position], side: str) -> int:
        return sum(1 for pos in positions.values() if pos.side == side)

    @staticmethod
    def apply_slippage(price: float, side: str, slippage_bps: float, is_entry: bool) -> float:
        impact = slippage_bps / 10000.0
        if side == 'LONG':
            return price * (1.0 + impact) if is_entry else price * (1.0 - impact)
        return price * (1.0 - impact) if is_entry else price * (1.0 + impact)

    @staticmethod
    def leg_pnl(pos: Position, exit_price: float, qty: float) -> float:
        delta = exit_price - pos.entry_price
        if pos.side == 'SHORT':
            delta *= -1
        return qty * delta


    @staticmethod
    def recompute_levels(entry_price: float, side: str, stop_distance: float, tp1_r_multiple: float, tp2_r_multiple: float) -> tuple[float, float, float]:
        stop_distance = max(float(stop_distance), 1e-9)
        if side == 'LONG':
            stop_price = entry_price - stop_distance
            tp1_price = entry_price + stop_distance * float(tp1_r_multiple)
            tp2_price = entry_price + stop_distance * float(tp2_r_multiple)
        else:
            stop_price = entry_price + stop_distance
            tp1_price = entry_price - stop_distance * float(tp1_r_multiple)
            tp2_price = entry_price - stop_distance * float(tp2_r_multiple)
        return stop_price, tp1_price, tp2_price

    @staticmethod
    def marked_equity(cash_equity: float, positions: dict[str, Position], bar_maps: dict[str, pd.DataFrame], ts: pd.Timestamp) -> float:
        open_mark_to_market = 0.0
        for symbol, pos in positions.items():
            bar_df = bar_maps.get(symbol)
            if bar_df is None or ts not in bar_df.index:
                continue
            current_close = float(bar_df.loc[ts]['close'])
            open_mark_to_market += SimulationHelpers.leg_pnl(pos, current_close, pos.qty)
        return float(cash_equity + open_mark_to_market)

    @classmethod
    def close_position(cls, pos: Position, exit_ts: pd.Timestamp, exit_price: float, exit_reason: str, fee_rate: float) -> dict[str, Any]:
        gross = pos.realized_gross + cls.leg_pnl(pos, exit_price, pos.qty)
        fee = pos.fees + (pos.qty * exit_price * fee_rate)
        net = gross - fee + pos.funding_pnl
        r_multiple = net / max(pos.initial_qty * pos.risk_per_unit, 1e-9)
        return {
            'symbol': pos.symbol,
            'regime': pos.regime,
            'side': pos.side,
            'entry_ts': pos.entry_ts,
            'exit_ts': exit_ts,
            'entry_price': round(pos.entry_price, 8),
            'exit_price': round(exit_price, 8),
            'stop_price': round(pos.stop_price, 8),
            'tp1_price': round(pos.tp1_price, 8),
            'tp2_price': round(pos.tp2_price, 8),
            'qty': round(pos.initial_qty, 8),
            'fees': round(fee, 8),
            'funding_pnl': round(pos.funding_pnl, 8),
            'gross_pnl': round(gross, 8),
            'net_pnl': round(net, 8),
            'r_multiple': round(r_multiple, 8),
            'exit_reason': exit_reason,
            'bars_held': int(pos.bars_held),
            'score': pos.score,
            'notes': pos.notes,
        }

    @classmethod
    def incremental_close_cash(cls, pos: Position, exit_price: float, fee_rate: float) -> float:
        leg_pnl = cls.leg_pnl(pos, exit_price, pos.qty)
        exit_fee = pos.qty * exit_price * fee_rate
        return leg_pnl - exit_fee

    @staticmethod
    def compute_metrics(trades_df: pd.DataFrame, equity_df: pd.DataFrame, starting_equity: float) -> dict[str, Any]:
        if equity_df.empty:
            return {
                'starting_equity': starting_equity,
                'ending_equity': starting_equity,
                'total_return_pct': 0.0,
                'max_drawdown_pct': 0.0,
                'win_rate': 0.0,
                'profit_factor': 0.0,
                'sharpe': 0.0,
                'trades_count': 0,
                'avg_r': 0.0,
                'expectancy_pct': 0.0,
                'stop_rate': 0.0,
            }
        curve = equity_df.copy().sort_values('ts')
        curve['peak'] = curve['equity'].cummax()
        curve['drawdown_pct'] = (curve['equity'] / curve['peak'] - 1.0) * 100.0
        ending_equity = float(curve['equity'].iloc[-1])
        total_return_pct = (ending_equity / starting_equity - 1.0) * 100.0
        max_dd = float(curve['drawdown_pct'].min())

        if trades_df.empty:
            win_rate = 0.0
            profit_factor = 0.0
            sharpe = 0.0
            avg_r = 0.0
            expectancy_pct = 0.0
            stop_rate = 0.0
        else:
            wins = (trades_df['net_pnl'] > 0).sum()
            win_rate = wins / len(trades_df) * 100.0
            gross_profit = trades_df.loc[trades_df['net_pnl'] > 0, 'net_pnl'].sum()
            gross_loss = -trades_df.loc[trades_df['net_pnl'] < 0, 'net_pnl'].sum()
            profit_factor = float(gross_profit / gross_loss) if gross_loss > 0 else float('inf')
            returns = curve['equity'].pct_change().dropna()
            sharpe = float((returns.mean() / returns.std()) * sqrt(365 * 24 * 12)) if len(returns) > 2 and returns.std() > 0 else 0.0
            avg_r = float(trades_df['r_multiple'].mean())
            expectancy_pct = float((trades_df['net_pnl'].mean() / starting_equity) * 100.0)
            stop_rate = float(((trades_df['exit_reason'] == 'stop').sum() / len(trades_df)) * 100.0)

        return {
            'starting_equity': float(round(starting_equity, 8)),
            'ending_equity': float(round(ending_equity, 8)),
            'total_return_pct': float(round(total_return_pct, 4)),
            'max_drawdown_pct': float(round(max_dd, 4)),
            'win_rate': float(round(win_rate, 4)),
            'profit_factor': float(round(profit_factor, 4)) if np.isfinite(profit_factor) else 'inf',
            'sharpe': float(round(sharpe, 4)),
            'trades_count': int(len(trades_df)),
            'avg_r': float(round(avg_r, 4)),
            'expectancy_pct': float(round(expectancy_pct, 4)),
            'stop_rate': float(round(stop_rate, 4)),
        }


def prepare_market(settings: dict[str, Any], candles: pd.DataFrame, funding: pd.DataFrame, symbols: list[str]) -> PreparedMarket:
    strategy = StrategyFactory(settings)
    symbol_bars: dict[str, pd.DataFrame] = {}
    signal_rows: list[dict[str, Any]] = []
    signals_by_symbol: dict[str, dict[pd.Timestamp, StrategySignal]] = {}

    for symbol in symbols:
        sym_candles = candles[candles['symbol'] == symbol].copy()
        if sym_candles.empty:
            continue
        sym_funding = funding[funding['symbol'] == symbol].copy() if not funding.empty else pd.DataFrame()
        features, sym_signals = strategy.build_symbol_signals(sym_candles, sym_funding)
        if features.empty:
            continue
        symbol_bars[symbol] = features
        signals_by_symbol[symbol] = {signal.ts: signal for signal in sym_signals}
        for signal in sym_signals:
            signal_rows.append(
                {
                    'symbol': signal.symbol,
                    'ts': signal.ts,
                    'regime': signal.regime,
                    'side': signal.side,
                    'score': signal.score,
                    'notes': signal.notes,
                }
            )

    if not symbol_bars:
        raise ValueError('Signals could not be built from the stored data. Try a longer history or use demo data.')

    funding_maps: dict[str, dict[pd.Timestamp, float]] = {}
    if funding is not None and not funding.empty:
        funding_sorted = funding.copy()
        funding_sorted['ts'] = pd.to_datetime(funding_sorted['ts'], utc=True)
        for symbol in symbols:
            part = funding_sorted[funding_sorted['symbol'] == symbol]
            funding_maps[symbol] = {row.ts: float(row.funding_rate) for row in part.itertuples(index=False)}

    bar_maps = {symbol: df.set_index('ts').sort_index() for symbol, df in symbol_bars.items()}
    all_times = sorted(set().union(*[set(df.index) for df in bar_maps.values()]))
    return PreparedMarket(
        symbol_bars=symbol_bars,
        signals_by_symbol=signals_by_symbol,
        signal_rows=signal_rows,
        funding_maps=funding_maps,
        all_times=all_times,
    )


def simulate_market(
    settings: dict[str, Any],
    candles: pd.DataFrame,
    funding: pd.DataFrame,
    symbols: list[str],
    starting_equity: float | None = None,
    close_open_positions_at_end: bool = True,
    trade_start_ts: str | pd.Timestamp | None = None,
) -> dict[str, Any]:
    symbols = [s.upper() for s in symbols]
    if candles.empty:
        raise ValueError('No candle data provided for simulation.')
    prepared = prepare_market(settings, candles, funding, symbols)
    bar_maps = {symbol: df.set_index('ts').sort_index() for symbol, df in prepared.symbol_bars.items()}
    trade_start_cutoff = None
    if trade_start_ts is not None:
        trade_start_cutoff = pd.Timestamp(trade_start_ts)
        trade_start_cutoff = trade_start_cutoff.tz_localize('UTC') if trade_start_cutoff.tzinfo is None else trade_start_cutoff.tz_convert('UTC')

    equity = float(starting_equity if starting_equity is not None else settings.get('starting_equity', 10000.0))
    starting_equity = equity
    max_concurrent = int(settings.get('max_concurrent_positions', 3))
    max_same_side = int(settings.get('max_same_side_positions', 2))
    daily_loss_limit = float(settings.get('daily_loss_limit', 0.02))
    max_daily_stopouts = int(settings.get('max_daily_stopouts', 3))
    cooldown_bars_after_stop = int(settings.get('cooldown_bars_after_stop', 12))
    max_consecutive_losses = int(settings.get('max_consecutive_losses', 4))
    max_leverage = float(settings.get('max_leverage', 3.0))
    entry_fee_rate = float(settings.get('entry_fee_rate', 0.0002))
    tp_exit_fee_rate = float(settings.get('exit_fee_rate_take_profit', 0.0002))
    stop_exit_fee_rate = float(settings.get('exit_fee_rate_stop', 0.00055))
    entry_slippage_bps = float(settings.get('entry_slippage_bps', 1.0))
    tp_exit_slippage_bps = float(settings.get('exit_slippage_bps_tp', 1.0))
    stop_exit_slippage_bps = float(settings.get('exit_slippage_bps_stop', 4.0))

    positions: dict[str, Position] = {}
    trades: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    run_signals: list[dict[str, Any]] = []
    daily_realized: dict[pd.Timestamp, float] = {}
    daily_stopouts: dict[pd.Timestamp, int] = {}
    symbol_cooldowns: dict[str, pd.Timestamp] = {}
    disabled_days: set[pd.Timestamp] = set()
    consecutive_losses = 0
    current_day: pd.Timestamp | None = None
    daily_start_equity = float(starting_equity)

    for ts in prepared.all_times:
        day = ts.normalize()
        daily_realized.setdefault(day, 0.0)
        daily_stopouts.setdefault(day, 0)

        if current_day is None or day != current_day:
            current_day = day
            consecutive_losses = 0
            daily_start_equity = SimulationHelpers.marked_equity(equity, positions, bar_maps, ts)

        for symbol, pos in list(positions.items()):
            rate = prepared.funding_maps.get(symbol, {}).get(ts)
            if rate is not None:
                notional = pos.qty * pos.entry_price
                funding_cash = notional * rate * (1 if pos.side == 'LONG' else -1)
                pos.funding_pnl -= funding_cash
                equity -= funding_cash
                daily_realized[day] -= funding_cash

        for symbol, pos in list(positions.items()):
            bar_df = bar_maps[symbol]
            if ts not in bar_df.index:
                continue
            bar = bar_df.loc[ts]
            pos.bars_held += 1

            if pos.side == 'LONG':
                stop_hit = bar['low'] <= pos.stop_price
                tp1_hit = (not pos.tp1_taken) and bar['high'] >= pos.tp1_price
                tp2_hit = pos.tp1_taken and bar['high'] >= pos.tp2_price
            else:
                stop_hit = bar['high'] >= pos.stop_price
                tp1_hit = (not pos.tp1_taken) and bar['low'] <= pos.tp1_price
                tp2_hit = pos.tp1_taken and bar['low'] <= pos.tp2_price

            if stop_hit:
                fill_price = SimulationHelpers.apply_slippage(pos.stop_price, pos.side, stop_exit_slippage_bps, is_entry=False)
                cash_delta = SimulationHelpers.incremental_close_cash(pos, fill_price, stop_exit_fee_rate)
                trade = SimulationHelpers.close_position(pos, ts, fill_price, 'stop', stop_exit_fee_rate)
                trades.append(trade)
                equity += cash_delta
                daily_realized[day] += cash_delta
                daily_stopouts[day] += 1
                symbol_cooldowns[symbol] = ts + pd.Timedelta(minutes=5 * cooldown_bars_after_stop)
                if trade['net_pnl'] <= 0:
                    consecutive_losses += 1
                else:
                    consecutive_losses = 0
                if daily_stopouts[day] >= max_daily_stopouts or consecutive_losses >= max_consecutive_losses:
                    disabled_days.add(day)
                positions.pop(symbol, None)
                continue

            if tp1_hit:
                close_qty = pos.qty * 0.5
                fill = SimulationHelpers.apply_slippage(pos.tp1_price, pos.side, tp_exit_slippage_bps, is_entry=False)
                pnl = SimulationHelpers.leg_pnl(pos, fill, close_qty)
                fee = close_qty * fill * tp_exit_fee_rate
                pos.qty -= close_qty
                pos.realized_gross += pnl
                pos.fees += fee
                pos.tp1_taken = True
                pos.stop_price = pos.entry_price
                equity += pnl - fee
                daily_realized[day] += pnl - fee

            if tp2_hit:
                fill_price = SimulationHelpers.apply_slippage(pos.tp2_price, pos.side, tp_exit_slippage_bps, is_entry=False)
                cash_delta = SimulationHelpers.incremental_close_cash(pos, fill_price, tp_exit_fee_rate)
                trade = SimulationHelpers.close_position(pos, ts, fill_price, 'tp2', tp_exit_fee_rate)
                trades.append(trade)
                equity += cash_delta
                daily_realized[day] += cash_delta
                consecutive_losses = 0 if trade['net_pnl'] > 0 else consecutive_losses + 1
                positions.pop(symbol, None)
                continue

            if pos.bars_held >= pos.max_hold_bars:
                raw_exit = float(bar['close'])
                fill_price = SimulationHelpers.apply_slippage(raw_exit, pos.side, tp_exit_slippage_bps, is_entry=False)
                cash_delta = SimulationHelpers.incremental_close_cash(pos, fill_price, tp_exit_fee_rate)
                trade = SimulationHelpers.close_position(pos, ts, fill_price, 'time_exit', tp_exit_fee_rate)
                trades.append(trade)
                equity += cash_delta
                daily_realized[day] += cash_delta
                consecutive_losses = 0 if trade['net_pnl'] > 0 else consecutive_losses + 1
                positions.pop(symbol, None)
                continue

        marked_equity = SimulationHelpers.marked_equity(equity, positions, bar_maps, ts)
        if trade_start_cutoff is None or ts >= trade_start_cutoff:
            equity_curve.append({'ts': ts, 'equity': round(marked_equity, 8)})

        if day in disabled_days:
            continue
        if marked_equity <= daily_start_equity * (1.0 - daily_loss_limit):
            disabled_days.add(day)
            continue

        if trade_start_cutoff is not None and ts < trade_start_cutoff:
            continue

        ranked_signals: list[StrategySignal] = []
        for symbol, signal_map in prepared.signals_by_symbol.items():
            signal = signal_map.get(ts)
            if signal is None:
                continue
            if symbol in positions:
                continue
            if symbol in symbol_cooldowns and ts < symbol_cooldowns[symbol]:
                continue
            ranked_signals.append(signal)
        ranked_signals.sort(key=lambda s: s.score, reverse=True)

        for signal in ranked_signals:
            if len(positions) >= max_concurrent:
                break
            if SimulationHelpers.count_same_side_positions(positions, signal.side) >= max_same_side:
                continue
            next_bar_df = bar_maps[signal.symbol]
            loc = next_bar_df.index.get_indexer([ts])[0]
            if loc + 1 >= len(next_bar_df):
                continue
            next_bar = next_bar_df.iloc[loc + 1]
            actual_entry_ts = pd.Timestamp(next_bar.name)
            next_entry = SimulationHelpers.apply_slippage(float(next_bar['open']), signal.side, entry_slippage_bps, is_entry=True)
            stop_distance = float(getattr(signal, 'stop_distance', abs(signal.entry_price - signal.stop_price)))
            tp1_r_multiple = float(getattr(signal, 'tp1_r_multiple', abs(signal.tp1_price - signal.entry_price) / max(stop_distance, 1e-9)))
            tp2_r_multiple = float(getattr(signal, 'tp2_r_multiple', abs(signal.tp2_price - signal.entry_price) / max(stop_distance, 1e-9)))
            actual_stop, actual_tp1, actual_tp2 = SimulationHelpers.recompute_levels(next_entry, signal.side, stop_distance, tp1_r_multiple, tp2_r_multiple)
            marked_equity_for_sizing = SimulationHelpers.marked_equity(equity, positions, bar_maps, ts)
            risk_capital = marked_equity_for_sizing * float(settings.get('risk_per_trade', 0.004))
            risk_per_unit = abs(next_entry - actual_stop)
            if risk_per_unit <= 0:
                continue
            qty = risk_capital / risk_per_unit
            max_notional = marked_equity_for_sizing * max_leverage
            if qty * next_entry > max_notional:
                qty = max_notional / max(next_entry, 1e-9)
            if qty <= 0:
                continue

            entry_fee = qty * next_entry * entry_fee_rate
            if entry_fee >= equity:
                continue
            equity -= entry_fee
            pos = Position(
                symbol=signal.symbol,
                regime=signal.regime,
                side=signal.side,
                entry_ts=actual_entry_ts,
                entry_price=next_entry,
                stop_price=actual_stop,
                tp1_price=actual_tp1,
                tp2_price=actual_tp2,
                qty=qty,
                initial_qty=qty,
                risk_per_unit=risk_per_unit,
                score=signal.score,
                notes=signal.notes,
                max_hold_bars=int(settings.get('trend_max_hold_bars' if signal.regime == 'trend' else 'reversion_max_hold_bars', 96)),
                fees=entry_fee,
            )
            positions[signal.symbol] = pos
            run_signals.append(
                {
                    'symbol': signal.symbol,
                    'ts': ts,
                    'entry_ts': actual_entry_ts,
                    'regime': signal.regime,
                    'side': signal.side,
                    'score': signal.score,
                    'notes': signal.notes,
                }
            )

    if prepared.all_times:
        last_ts = prepared.all_times[-1]
        if close_open_positions_at_end:
            for symbol, pos in list(positions.items()):
                bar_df = bar_maps[symbol]
                last_close = float(bar_df.iloc[-1]['close'])
                fill_price = SimulationHelpers.apply_slippage(last_close, pos.side, tp_exit_slippage_bps, is_entry=False)
                cash_delta = SimulationHelpers.incremental_close_cash(pos, fill_price, tp_exit_fee_rate)
                trades.append(SimulationHelpers.close_position(pos, last_ts, fill_price, 'end_of_test', tp_exit_fee_rate))
                equity += cash_delta
                positions.pop(symbol, None)
            equity_curve.append({'ts': last_ts, 'equity': round(equity, 8)})

    if trade_start_cutoff is not None:
        trades = [trade for trade in trades if pd.Timestamp(trade['entry_ts']).tz_convert('UTC') >= trade_start_cutoff]
        equity_curve = [row for row in equity_curve if pd.Timestamp(row['ts']).tz_convert('UTC') >= trade_start_cutoff]
        if not equity_curve and trade_start_cutoff is not None:
            equity_curve = [{'ts': trade_start_cutoff, 'equity': round(starting_equity, 8)}]

    trades_df = pd.DataFrame(trades)
    equity_df = pd.DataFrame(equity_curve)
    metrics = SimulationHelpers.compute_metrics(trades_df, equity_df, starting_equity)
    open_positions = [
        {
            'symbol': pos.symbol,
            'regime': pos.regime,
            'side': pos.side,
            'entry_ts': pos.entry_ts.isoformat().replace('+00:00', 'Z'),
            'entry_price': round(pos.entry_price, 8),
            'stop_price': round(pos.stop_price, 8),
            'tp1_price': round(pos.tp1_price, 8),
            'tp2_price': round(pos.tp2_price, 8),
            'qty': round(pos.qty, 8),
            'initial_qty': round(pos.initial_qty, 8),
            'bars_held': pos.bars_held,
            'score': pos.score,
            'notes': pos.notes,
            'tp1_taken': pos.tp1_taken,
            'floating_pnl': round(
                SimulationHelpers.leg_pnl(
                    pos,
                    float(bar_maps[pos.symbol].iloc[-1]['close']),
                    pos.qty,
                ),
                8,
            ) if pos.symbol in bar_maps and not bar_maps[pos.symbol].empty else 0.0,
        }
        for pos in positions.values()
    ]
    return {
        'summary': metrics,
        'trades': trades,
        'equity_curve': [
            {'ts': row['ts'].isoformat().replace('+00:00', 'Z'), 'equity': row['equity']}
            for row in equity_curve
        ],
        'signals': run_signals,
        'signals_count': len(prepared.signal_rows),
        'open_positions': open_positions,
        'last_ts': prepared.all_times[-1].isoformat().replace('+00:00', 'Z') if prepared.all_times else None,
    }
