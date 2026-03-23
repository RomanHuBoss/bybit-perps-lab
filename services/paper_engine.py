from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from typing import Any

import pandas as pd

from database import get_conn, utc_now_iso
from services.data_service import load_candles, load_funding
from services.simulator import simulate_market


@dataclass
class RunnerState:
    stop_event: threading.Event
    thread: threading.Thread


class PaperTradingManager:
    def __init__(self):
        self._runners: dict[int, RunnerState] = {}
        self._lock = threading.Lock()

    def create_session(self, name: str, symbols: list[str], settings: dict[str, Any], poll_seconds: float | None = None, auto_steps: int | None = None) -> dict[str, Any]:
        symbols = [s.upper() for s in symbols]
        candles = load_candles(symbols, interval='5')
        if candles.empty:
            raise ValueError('No 5-minute candle data found for paper session.')
        all_times = sorted(pd.to_datetime(candles['ts'], utc=True).unique())
        initial_state = {
            'signals_count': 0,
            'open_positions': [],
            'trades_count': 0,
            'last_completed_ts': None,
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
                    0,
                    None,
                    float(settings.get('starting_equity', 10000.0)),
                    float(settings.get('starting_equity', 10000.0)),
                    poll_seconds,
                    auto_steps,
                    json.dumps(initial_state),
                    json.dumps({'bars_available': len(all_times)}),
                ),
            )
            session_id = int(cursor.lastrowid)
            conn.commit()
        self._log_event(session_id, 'info', f'Создана paper-сессия {name}. Доступно баров: {len(all_times)}.')
        return self.get_session(session_id)

    def step_session(self, session_id: int, steps: int = 1) -> dict[str, Any]:
        session = self._get_session_row(session_id)
        symbols = json.loads(session['symbols_json'])
        settings = json.loads(session['config_json'])
        candles = load_candles(symbols, interval='5')
        funding = load_funding(symbols)
        if candles.empty:
            raise ValueError('No candles available for paper session.')
        all_times = sorted(pd.to_datetime(candles['ts'], utc=True).unique())
        if not all_times:
            raise ValueError('No timestamps available for paper session.')

        new_index = min(int(session['current_index']) + int(steps), len(all_times) - 1)
        current_ts = all_times[new_index]
        subset_candles = candles[candles['ts'] <= current_ts].copy()
        subset_funding = funding[funding['ts'] <= current_ts].copy() if not funding.empty else funding
        result = simulate_market(
            settings,
            subset_candles,
            subset_funding,
            symbols,
            starting_equity=float(session['starting_equity']),
            close_open_positions_at_end=False,
        )
        self._replace_session_snapshot(session_id, session, result, new_index, current_ts)
        if new_index >= len(all_times) - 1:
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
            trades = conn.execute('SELECT * FROM paper_trades WHERE paper_session_id = ? ORDER BY entry_ts DESC LIMIT 300', (session_id,)).fetchall()
            equity = conn.execute('SELECT ts, equity FROM paper_equity_curve WHERE paper_session_id = ? ORDER BY ts', (session_id,)).fetchall()
            events = conn.execute('SELECT created_at, level, message FROM paper_events WHERE paper_session_id = ? ORDER BY id DESC LIMIT 100', (session_id,)).fetchall()
        return {
            'session': self._decorate_session_row(session),
            'trades': trades,
            'equity': equity,
            'events': events,
        }

    def _run_loop(self, session_id: int, stop_event: threading.Event, poll_seconds: float, auto_steps: int) -> None:
        while not stop_event.is_set():
            try:
                self.step_session(session_id, steps=auto_steps)
            except Exception as exc:  # pragma: no cover - defensive logging path
                self._log_event(session_id, 'error', f'Ошибка paper loop: {exc}')
                self._update_status(session_id, 'error')
                with self._lock:
                    self._runners.pop(session_id, None)
                return
            stop_event.wait(poll_seconds)
        with self._lock:
            self._runners.pop(session_id, None)

    def _replace_session_snapshot(self, session_id: int, session: dict[str, Any], result: dict[str, Any], current_index: int, current_ts: pd.Timestamp) -> None:
        summary = result['summary']
        state_json = {
            'signals_count': result['signals_count'],
            'open_positions': result['open_positions'],
            'trades_count': summary['trades_count'],
            'last_completed_ts': result['last_ts'],
        }
        with get_conn() as conn:
            conn.execute('DELETE FROM paper_trades WHERE paper_session_id = ?', (session_id,))
            conn.execute('DELETE FROM paper_equity_curve WHERE paper_session_id = ?', (session_id,))
            if result['trades']:
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
                        for trade in result['trades']
                    ],
                )
            if result['equity_curve']:
                conn.executemany(
                    'INSERT INTO paper_equity_curve(paper_session_id, ts, equity) VALUES (?, ?, ?)',
                    [(session_id, row['ts'], row['equity']) for row in result['equity_curve']],
                )
            conn.execute(
                '''
                UPDATE paper_sessions
                SET updated_at = ?, current_index = ?, current_ts = ?, current_equity = ?, state_json = ?
                WHERE id = ?
                ''',
                (
                    utc_now_iso(),
                    current_index,
                    current_ts.isoformat().replace('+00:00', 'Z'),
                    summary['ending_equity'],
                    json.dumps(state_json),
                    session_id,
                ),
            )
            conn.commit()
        self._log_event(
            session_id,
            'success',
            f"Paper step до {current_ts.isoformat().replace('+00:00', 'Z')}: equity={summary['ending_equity']:.2f}, trades={summary['trades_count']}, open={len(result['open_positions'])}",
        )

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
