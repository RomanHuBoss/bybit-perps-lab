from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from database import get_conn, utc_now_iso
from services.data_service import load_candles, load_funding
from services.simulator import Position, PreparedMarket, SimulationHelpers, prepare_market


@dataclass
class RunnerState:
    stop_event: threading.Event
    thread: threading.Thread


@dataclass
class SessionRuntime:
    session_id: int
    symbols: list[str]
    settings: dict[str, Any]
    prepared: PreparedMarket
    starting_equity: float
    current_index: int = -1
    equity_cash: float = 0.0
    positions: dict[str, Position] = field(default_factory=dict)
    trades: list[dict[str, Any]] = field(default_factory=list)
    equity_curve: list[dict[str, Any]] = field(default_factory=list)
    run_signals: list[dict[str, Any]] = field(default_factory=list)
    daily_realized: dict[pd.Timestamp, float] = field(default_factory=dict)
    daily_stopouts: dict[pd.Timestamp, int] = field(default_factory=dict)
    symbol_cooldowns: dict[str, pd.Timestamp] = field(default_factory=dict)
    disabled_days: set[pd.Timestamp] = field(default_factory=set)
    consecutive_losses: int = 0
    current_day: pd.Timestamp | None = None
    daily_start_equity: float = 0.0
    last_persisted_trade_count: int = 0
    last_persisted_equity_count: int = 0
    finalized_at_end: bool = False

    def __post_init__(self) -> None:
        if not self.equity_cash:
            self.equity_cash = self.starting_equity
        if not self.daily_start_equity:
            self.daily_start_equity = self.starting_equity
        self.bar_maps = {symbol: df.set_index('ts').sort_index() for symbol, df in self.prepared.symbol_bars.items()}
        self.all_times = self.prepared.all_times

    @property
    def current_ts(self) -> pd.Timestamp | None:
        if self.current_index < 0 or self.current_index >= len(self.all_times):
            return None
        return self.all_times[self.current_index]

    @property
    def current_equity(self) -> float:
        if self.equity_curve:
            return float(self.equity_curve[-1]['equity'])
        return float(self.equity_cash)


class PaperTradingManager:
    def __init__(self):
        self._runners: dict[int, RunnerState] = {}
        self._runtime_cache: dict[int, SessionRuntime] = {}
        self._lock = threading.Lock()

    def create_session(self, name: str, symbols: list[str], settings: dict[str, Any], poll_seconds: float | None = None, auto_steps: int | None = None) -> dict[str, Any]:
        symbols = [s.upper() for s in symbols]
        runtime = self._build_runtime(symbols=symbols, settings=settings, session_id=-1)
        initial_state = {
            'signals_count': len(runtime.prepared.signal_rows),
            'open_positions': [],
            'trades_count': 0,
            'last_completed_ts': None,
            'bars_available': len(runtime.all_times),
        }
        created_at = utc_now_iso()
        poll_seconds = float(poll_seconds if poll_seconds is not None else settings.get('paper_poll_seconds', 2.0))
        auto_steps = int(auto_steps if auto_steps is not None else settings.get('paper_auto_steps', 1))
        with get_conn() as conn:
            cursor = conn.execute(
                '''
                INSERT INTO paper_sessions(created_at, updated_at, name, status, symbols_json, config_json,
                                           current_index, current_ts, starting_equity, current_equity,
                                           poll_seconds, auto_steps, state_json, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    created_at,
                    created_at,
                    name,
                    'paused',
                    json.dumps(symbols),
                    json.dumps(settings),
                    -1,
                    None,
                    float(settings.get('starting_equity', 10000.0)),
                    float(settings.get('starting_equity', 10000.0)),
                    poll_seconds,
                    auto_steps,
                    json.dumps(initial_state),
                    json.dumps({'bars_available': len(runtime.all_times)}),
                ),
            )
            session_id = int(cursor.lastrowid)
            conn.commit()
        runtime.session_id = session_id
        self._runtime_cache[session_id] = runtime
        self._log_event(session_id, 'info', f'Создана paper-сессия {name}. Доступно баров: {len(runtime.all_times)}.')
        return self.get_session(session_id)

    def step_session(self, session_id: int, steps: int = 1) -> dict[str, Any]:
        runtime = self._get_or_build_runtime(session_id)
        session = self._get_session_row(session_id)
        if not runtime.all_times:
            raise ValueError('No timestamps available for paper session.')
        if runtime.current_index >= len(runtime.all_times) - 1:
            self.stop_background(session_id)
            self._update_status(session_id, 'completed')
            return self.get_session(session_id)

        target_index = min(runtime.current_index + max(int(steps), 1), len(runtime.all_times) - 1)
        for idx in range(runtime.current_index + 1, target_index + 1):
            self._process_bar(runtime, idx)
            runtime.current_index = idx

        if runtime.current_index >= len(runtime.all_times) - 1 and not runtime.finalized_at_end:
            self._finalize_runtime_at_end(runtime)
        self._persist_runtime_delta(session_id, session, runtime)
        if runtime.current_index >= len(runtime.all_times) - 1:
            self.stop_background(session_id)
            self._update_status(session_id, 'completed')
            self._log_event(session_id, 'success', 'Paper replay завершён: достигнут конец истории.')
        else:
            self._update_status(session_id, 'running' if session['status'] == 'running' else 'paused')
        return self.get_session(session_id)

    def start_background(self, session_id: int) -> dict[str, Any]:
        session = self._get_session_row(session_id)
        with self._lock:
            if session_id in self._runners:
                return self.get_session(session_id)
            stop_event = threading.Event()
            poll_seconds = float(session['poll_seconds'])
            auto_steps = int(session['auto_steps'])
            thread = threading.Thread(
                target=self._run_loop,
                args=(session_id, stop_event, poll_seconds, auto_steps),
                daemon=True,
            )
            self._runners[session_id] = RunnerState(stop_event=stop_event, thread=thread)
            self._update_status(session_id, 'running')
            self._log_event(session_id, 'info', f'Запущен background replay: {auto_steps} шаг(ов) каждые {poll_seconds:.2f} сек.')
            thread.start()
        return self.get_session(session_id)

    def stop_background(self, session_id: int) -> dict[str, Any]:
        with self._lock:
            runner = self._runners.pop(session_id, None)
        if runner:
            runner.stop_event.set()
            self._update_status(session_id, 'paused')
            self._log_event(session_id, 'warn', 'Background replay остановлен.')
        return self.get_session(session_id)

    def list_sessions(self, limit: int = 20) -> list[dict[str, Any]]:
        with get_conn(row_factory=True) as conn:
            rows = conn.execute('SELECT * FROM paper_sessions ORDER BY id DESC LIMIT ?', (limit,)).fetchall()
        return [self._decorate_session_row(row) for row in rows]

    def get_session(self, session_id: int) -> dict[str, Any]:
        session = self._get_session_row(session_id)
        with get_conn(row_factory=True) as conn:
            trades = conn.execute('SELECT * FROM paper_trades WHERE paper_session_id = ? ORDER BY entry_ts DESC LIMIT 200', (session_id,)).fetchall()
            all_trades = conn.execute('SELECT net_pnl, r_multiple, exit_reason FROM paper_trades WHERE paper_session_id = ? ORDER BY entry_ts ASC', (session_id,)).fetchall()
            equity = conn.execute('SELECT ts, equity FROM paper_equity_curve WHERE paper_session_id = ? ORDER BY ts ASC', (session_id,)).fetchall()
            events = conn.execute('SELECT created_at, level, message FROM paper_events WHERE paper_session_id = ? ORDER BY id DESC LIMIT 100', (session_id,)).fetchall()
        summary = SimulationHelpers.compute_metrics(pd.DataFrame(all_trades), pd.DataFrame(equity), float(session['starting_equity']))
        equity = self._downsample_equity(equity, max_points=220)
        return {
            'session': self._decorate_session_row(session),
            'summary': summary,
            'trades': trades,
            'equity': equity,
            'events': events,
        }

    def _run_loop(self, session_id: int, stop_event: threading.Event, poll_seconds: float, auto_steps: int) -> None:
        while not stop_event.is_set():
            try:
                self.step_session(session_id, steps=auto_steps)
            except Exception as exc:  # pragma: no cover
                self._log_event(session_id, 'error', f'Ошибка paper loop: {exc}')
                self._update_status(session_id, 'error')
                with self._lock:
                    self._runners.pop(session_id, None)
                return
            stop_event.wait(poll_seconds)
        with self._lock:
            self._runners.pop(session_id, None)

    def _build_runtime(self, symbols: list[str], settings: dict[str, Any], session_id: int) -> SessionRuntime:
        candles = load_candles(symbols, interval='5')
        if candles.empty:
            raise ValueError('No 5-minute candle data found for paper session.')
        funding = load_funding(symbols)
        prepared = prepare_market(settings, candles, funding, symbols)
        return SessionRuntime(
            session_id=session_id,
            symbols=symbols,
            settings=settings,
            prepared=prepared,
            starting_equity=float(settings.get('starting_equity', 10000.0)),
        )

    def _get_or_build_runtime(self, session_id: int) -> SessionRuntime:
        runtime = self._runtime_cache.get(session_id)
        if runtime is not None:
            return runtime
        session = self._get_session_row(session_id)
        symbols = json.loads(session['symbols_json'])
        settings = json.loads(session['config_json'])
        runtime = self._build_runtime(symbols=symbols, settings=settings, session_id=session_id)
        target_index = int(session.get('current_index', -1))
        if target_index >= 0:
            for idx in range(0, min(target_index, len(runtime.all_times) - 1) + 1):
                self._process_bar(runtime, idx)
                runtime.current_index = idx
        runtime.last_persisted_trade_count = len(runtime.trades)
        runtime.last_persisted_equity_count = len(runtime.equity_curve)
        self._runtime_cache[session_id] = runtime
        return runtime

    def _process_bar(self, runtime: SessionRuntime, index: int) -> None:
        ts = runtime.all_times[index]
        settings = runtime.settings
        day = ts.normalize()
        runtime.daily_realized.setdefault(day, 0.0)
        runtime.daily_stopouts.setdefault(day, 0)

        if runtime.current_day is None or day != runtime.current_day:
            runtime.current_day = day
            runtime.consecutive_losses = 0
            runtime.daily_start_equity = SimulationHelpers.marked_equity(runtime.equity_cash, runtime.positions, runtime.bar_maps, ts)

        entry_fee_rate = float(settings.get('entry_fee_rate', 0.0002))
        tp_exit_fee_rate = float(settings.get('exit_fee_rate_take_profit', 0.0002))
        stop_exit_fee_rate = float(settings.get('exit_fee_rate_stop', 0.00055))
        entry_slippage_bps = float(settings.get('entry_slippage_bps', 1.0))
        tp_exit_slippage_bps = float(settings.get('exit_slippage_bps_tp', 1.0))
        stop_exit_slippage_bps = float(settings.get('exit_slippage_bps_stop', 4.0))
        max_concurrent = int(settings.get('max_concurrent_positions', 3))
        max_same_side = int(settings.get('max_same_side_positions', 2))
        daily_loss_limit = float(settings.get('daily_loss_limit', 0.02))
        max_daily_stopouts = int(settings.get('max_daily_stopouts', 3))
        cooldown_bars_after_stop = int(settings.get('cooldown_bars_after_stop', 12))
        max_consecutive_losses = int(settings.get('max_consecutive_losses', 4))
        max_leverage = float(settings.get('max_leverage', 3.0))

        for symbol, pos in list(runtime.positions.items()):
            rate = runtime.prepared.funding_maps.get(symbol, {}).get(ts)
            if rate is not None:
                notional = pos.qty * pos.entry_price
                funding_cash = notional * rate * (1 if pos.side == 'LONG' else -1)
                pos.funding_pnl -= funding_cash
                runtime.equity_cash -= funding_cash
                runtime.daily_realized[day] -= funding_cash

        for symbol, pos in list(runtime.positions.items()):
            bar_df = runtime.bar_maps[symbol]
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
                runtime.trades.append(trade)
                runtime.equity_cash += cash_delta
                runtime.daily_realized[day] += cash_delta
                runtime.daily_stopouts[day] += 1
                runtime.symbol_cooldowns[symbol] = ts + pd.Timedelta(minutes=5 * cooldown_bars_after_stop)
                runtime.consecutive_losses = runtime.consecutive_losses + 1 if trade['net_pnl'] <= 0 else 0
                if runtime.daily_stopouts[day] >= max_daily_stopouts or runtime.consecutive_losses >= max_consecutive_losses:
                    runtime.disabled_days.add(day)
                runtime.positions.pop(symbol, None)
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
                runtime.equity_cash += pnl - fee
                runtime.daily_realized[day] += pnl - fee

            if tp2_hit:
                fill_price = SimulationHelpers.apply_slippage(pos.tp2_price, pos.side, tp_exit_slippage_bps, is_entry=False)
                cash_delta = SimulationHelpers.incremental_close_cash(pos, fill_price, tp_exit_fee_rate)
                trade = SimulationHelpers.close_position(pos, ts, fill_price, 'tp2', tp_exit_fee_rate)
                runtime.trades.append(trade)
                runtime.equity_cash += cash_delta
                runtime.daily_realized[day] += cash_delta
                runtime.consecutive_losses = 0 if trade['net_pnl'] > 0 else runtime.consecutive_losses + 1
                runtime.positions.pop(symbol, None)
                continue

            if pos.bars_held >= pos.max_hold_bars:
                raw_exit = float(bar['close'])
                fill_price = SimulationHelpers.apply_slippage(raw_exit, pos.side, tp_exit_slippage_bps, is_entry=False)
                cash_delta = SimulationHelpers.incremental_close_cash(pos, fill_price, tp_exit_fee_rate)
                trade = SimulationHelpers.close_position(pos, ts, fill_price, 'time_exit', tp_exit_fee_rate)
                runtime.trades.append(trade)
                runtime.equity_cash += cash_delta
                runtime.daily_realized[day] += cash_delta
                runtime.consecutive_losses = 0 if trade['net_pnl'] > 0 else runtime.consecutive_losses + 1
                runtime.positions.pop(symbol, None)
                continue

        marked_equity = SimulationHelpers.marked_equity(runtime.equity_cash, runtime.positions, runtime.bar_maps, ts)
        runtime.equity_curve.append({'ts': ts.isoformat().replace('+00:00', 'Z'), 'equity': round(marked_equity, 8)})

        if day in runtime.disabled_days:
            return
        if marked_equity <= runtime.daily_start_equity * (1.0 - daily_loss_limit):
            runtime.disabled_days.add(day)
            return

        ranked_signals = []
        for symbol, signal_map in runtime.prepared.signals_by_symbol.items():
            signal = signal_map.get(ts)
            if signal is None or symbol in runtime.positions:
                continue
            if symbol in runtime.symbol_cooldowns and ts < runtime.symbol_cooldowns[symbol]:
                continue
            ranked_signals.append(signal)
        ranked_signals.sort(key=lambda s: s.score, reverse=True)

        for signal in ranked_signals:
            if len(runtime.positions) >= max_concurrent:
                break
            if SimulationHelpers.count_same_side_positions(runtime.positions, signal.side) >= max_same_side:
                continue
            next_bar_df = runtime.bar_maps[signal.symbol]
            loc = next_bar_df.index.get_indexer([ts])[0]
            if loc < 0 or loc + 1 >= len(next_bar_df):
                continue
            next_bar = next_bar_df.iloc[loc + 1]
            actual_entry_ts = pd.Timestamp(next_bar.name)
            next_entry = SimulationHelpers.apply_slippage(float(next_bar['open']), signal.side, entry_slippage_bps, is_entry=True)
            stop_distance = float(getattr(signal, 'stop_distance', abs(signal.entry_price - signal.stop_price)))
            tp1_r_multiple = float(getattr(signal, 'tp1_r_multiple', abs(signal.tp1_price - signal.entry_price) / max(stop_distance, 1e-9)))
            tp2_r_multiple = float(getattr(signal, 'tp2_r_multiple', abs(signal.tp2_price - signal.entry_price) / max(stop_distance, 1e-9)))
            actual_stop, actual_tp1, actual_tp2 = SimulationHelpers.recompute_levels(next_entry, signal.side, stop_distance, tp1_r_multiple, tp2_r_multiple)
            marked_equity_for_sizing = SimulationHelpers.marked_equity(runtime.equity_cash, runtime.positions, runtime.bar_maps, ts)
            risk_capital = marked_equity_for_sizing * float(settings.get('risk_per_trade', 0.004))
            risk_per_unit = abs(next_entry - actual_stop)
            if risk_per_unit <= 0:
                continue
            qty = risk_capital / risk_per_unit
            open_notional = SimulationHelpers.total_open_notional(runtime.positions, runtime.bar_maps, ts)
            max_total_notional = marked_equity_for_sizing * max_leverage
            available_notional = max(max_total_notional - open_notional, 0.0)
            if available_notional <= 0:
                continue
            if qty * next_entry > available_notional:
                qty = available_notional / max(next_entry, 1e-9)
            if qty <= 0:
                continue
            entry_fee = qty * next_entry * entry_fee_rate
            if entry_fee >= runtime.equity_cash:
                continue
            runtime.equity_cash -= entry_fee
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
            runtime.positions[signal.symbol] = pos
            runtime.run_signals.append({'symbol': signal.symbol, 'ts': ts, 'entry_ts': actual_entry_ts, 'regime': signal.regime, 'side': signal.side, 'score': signal.score, 'notes': signal.notes})

    def _finalize_runtime_at_end(self, runtime: SessionRuntime) -> None:
        if runtime.finalized_at_end:
            return
        if not runtime.all_times:
            runtime.finalized_at_end = True
            return
        last_ts = runtime.all_times[-1]
        tp_exit_fee_rate = float(runtime.settings.get('exit_fee_rate_take_profit', 0.0002))
        tp_exit_slippage_bps = float(runtime.settings.get('exit_slippage_bps_tp', 1.0))
        for symbol, pos in list(runtime.positions.items()):
            bar_df = runtime.bar_maps[symbol]
            last_close = float(bar_df.iloc[-1]['close'])
            fill_price = SimulationHelpers.apply_slippage(last_close, pos.side, tp_exit_slippage_bps, is_entry=False)
            cash_delta = SimulationHelpers.incremental_close_cash(pos, fill_price, tp_exit_fee_rate)
            trade = SimulationHelpers.close_position(pos, last_ts, fill_price, 'end_of_test', tp_exit_fee_rate)
            runtime.trades.append(trade)
            runtime.equity_cash += cash_delta
            runtime.positions.pop(symbol, None)
        runtime.equity_curve.append({'ts': last_ts.isoformat().replace('+00:00', 'Z'), 'equity': round(runtime.equity_cash, 8)})
        runtime.finalized_at_end = True

    def _persist_runtime_delta(self, session_id: int, session: dict[str, Any], runtime: SessionRuntime) -> None:
        new_trades = runtime.trades[runtime.last_persisted_trade_count:]
        new_equity_rows = runtime.equity_curve[runtime.last_persisted_equity_count:]
        current_ts = runtime.current_ts
        state_json = {
            'signals_count': len(runtime.prepared.signal_rows),
            'open_positions': self._serialize_open_positions(runtime),
            'trades_count': len(runtime.trades),
            'last_completed_ts': current_ts.isoformat().replace('+00:00', 'Z') if current_ts is not None else None,
            'bars_available': len(runtime.all_times),
        }
        with get_conn() as conn:
            if new_trades:
                conn.executemany(
                    '''
                    INSERT INTO paper_trades(paper_session_id, symbol, regime, side, entry_ts, exit_ts, entry_price,
                                             exit_price, net_pnl, r_multiple, exit_reason, bars_held, score, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    [
                        (
                            session_id,
                            trade['symbol'],
                            trade['regime'],
                            trade['side'],
                            trade['entry_ts'].isoformat().replace('+00:00', 'Z'),
                            trade['exit_ts'].isoformat().replace('+00:00', 'Z'),
                            trade['entry_price'],
                            trade['exit_price'],
                            trade['net_pnl'],
                            trade['r_multiple'],
                            trade['exit_reason'],
                            trade['bars_held'],
                            trade['score'],
                            json.dumps({
                                **trade,
                                'entry_ts': trade['entry_ts'].isoformat().replace('+00:00', 'Z'),
                                'exit_ts': trade['exit_ts'].isoformat().replace('+00:00', 'Z'),
                            }),
                        )
                        for trade in new_trades
                    ],
                )
            if new_equity_rows:
                conn.executemany(
                    'INSERT INTO paper_equity_curve(paper_session_id, ts, equity) VALUES (?, ?, ?)',
                    [(session_id, row['ts'], row['equity']) for row in new_equity_rows],
                )
            conn.execute(
                '''
                UPDATE paper_sessions
                SET updated_at = ?, current_index = ?, current_ts = ?, current_equity = ?, state_json = ?
                WHERE id = ?
                ''',
                (
                    utc_now_iso(),
                    runtime.current_index,
                    current_ts.isoformat().replace('+00:00', 'Z') if current_ts is not None else None,
                    runtime.current_equity,
                    json.dumps(state_json),
                    session_id,
                ),
            )
            conn.commit()
        runtime.last_persisted_trade_count = len(runtime.trades)
        runtime.last_persisted_equity_count = len(runtime.equity_curve)
        self._log_event(session_id, 'success', f'Paper step #{runtime.current_index}: equity={runtime.current_equity:.2f}, trades={len(runtime.trades)}, open={len(runtime.positions)}')

    def _serialize_open_positions(self, runtime: SessionRuntime) -> list[dict[str, Any]]:
        open_positions = []
        for pos in runtime.positions.values():
            current_close = None
            if pos.symbol in runtime.bar_maps and runtime.current_ts in runtime.bar_maps[pos.symbol].index:
                current_close = float(runtime.bar_maps[pos.symbol].loc[runtime.current_ts]['close'])
            floating_pnl = 0.0 if current_close is None else SimulationHelpers.leg_pnl(pos, current_close, pos.qty)
            open_positions.append({
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
                'floating_pnl': round(floating_pnl, 8),
            })
        return open_positions

    def _downsample_equity(self, rows: list[dict[str, Any]], max_points: int = 220) -> list[dict[str, Any]]:
        if len(rows) <= max_points:
            return rows
        step = max(1, len(rows) // max_points)
        sampled = rows[::step]
        if sampled[-1] != rows[-1]:
            sampled.append(rows[-1])
        return sampled

    def _decorate_session_row(self, row: dict[str, Any]) -> dict[str, Any]:
        out = dict(row)
        out['symbols'] = json.loads(out['symbols_json'])
        out['config'] = json.loads(out['config_json'])
        out['state'] = json.loads(out['state_json']) if out.get('state_json') else {}
        out['is_background_running'] = out['id'] in self._runners
        return out

    def _get_session_row(self, session_id: int) -> dict[str, Any]:
        with get_conn(row_factory=True) as conn:
            row = conn.execute('SELECT * FROM paper_sessions WHERE id = ?', (session_id,)).fetchone()
        if not row:
            raise ValueError(f'Paper session {session_id} not found.')
        return row

    def _update_status(self, session_id: int, status: str) -> None:
        with get_conn() as conn:
            conn.execute('UPDATE paper_sessions SET updated_at = ?, status = ? WHERE id = ?', (utc_now_iso(), status, session_id))
            conn.commit()

    def _log_event(self, session_id: int, level: str, message: str) -> None:
        with get_conn() as conn:
            conn.execute(
                'INSERT INTO paper_events(paper_session_id, created_at, level, message) VALUES (?, ?, ?, ?)',
                (session_id, utc_now_iso(), level, message),
            )
            conn.commit()


paper_manager = PaperTradingManager()
