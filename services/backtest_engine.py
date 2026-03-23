from __future__ import annotations

import json
from typing import Any

from database import get_conn, utc_now_iso
from services.data_service import load_candles, load_funding
from services.simulator import simulate_market


class BacktestEngine:
    def __init__(self, settings: dict[str, Any]):
        self.settings = settings

    def run(self, symbols: list[str]) -> dict[str, Any]:
        symbols = [s.upper() for s in symbols]
        candles = load_candles(symbols, interval='5')
        if candles.empty:
            raise ValueError('No 5-minute candle data found. Load demo data, import CSV, or sync Bybit public data first.')
        funding = load_funding(symbols)
        result = simulate_market(self.settings, candles, funding, symbols)
        run_id = self._persist_run(symbols, result)
        result['run_id'] = run_id
        result['trades'] = result['trades'][-200:]
        result['signals'] = result['signals'][-300:]
        return result

    def _persist_run(self, symbols: list[str], result: dict[str, Any]) -> int:
        metrics = result['summary']
        trades = result['trades']
        equity_curve = result['equity_curve']
        signals = result['signals']
        with get_conn() as conn:
            cursor = conn.execute(
                '''
                INSERT INTO backtest_runs(created_at, config_json, symbols_json, status, starting_equity, ending_equity,
                                          total_return_pct, max_drawdown_pct, win_rate, profit_factor, sharpe, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    utc_now_iso(),
                    json.dumps(self.settings),
                    json.dumps(symbols),
                    'completed',
                    metrics['starting_equity'],
                    metrics['ending_equity'],
                    metrics['total_return_pct'],
                    metrics['max_drawdown_pct'],
                    metrics['win_rate'],
                    metrics['profit_factor'] if metrics['profit_factor'] != 'inf' else None,
                    metrics['sharpe'],
                    json.dumps({
                        'avg_r': metrics['avg_r'],
                        'expectancy_pct': metrics['expectancy_pct'],
                        'stop_rate': metrics['stop_rate'],
                        'open_positions': result.get('open_positions', []),
                    }),
                ),
            )
            run_id = int(cursor.lastrowid)

            if trades:
                trade_rows = [
                    (
                        run_id,
                        trade['symbol'],
                        trade['regime'],
                        trade['side'],
                        trade['entry_ts'].isoformat().replace('+00:00', 'Z'),
                        trade['exit_ts'].isoformat().replace('+00:00', 'Z'),
                        trade['entry_price'],
                        trade['exit_price'],
                        trade['stop_price'],
                        trade['tp1_price'],
                        trade['tp2_price'],
                        trade['qty'],
                        trade['fees'],
                        trade['funding_pnl'],
                        trade['gross_pnl'],
                        trade['net_pnl'],
                        trade['r_multiple'],
                        trade['exit_reason'],
                        trade['bars_held'],
                        trade['score'],
                    )
                    for trade in trades
                ]
                conn.executemany(
                    '''
                    INSERT INTO trades(run_id, symbol, regime, side, entry_ts, exit_ts, entry_price, exit_price,
                                       stop_price, tp1_price, tp2_price, qty, fees, funding_pnl, gross_pnl,
                                       net_pnl, r_multiple, exit_reason, bars_held, score)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    trade_rows,
                )

            if equity_curve:
                curve_rows = [(run_id, row['ts'], row['equity']) for row in equity_curve]
                conn.executemany('INSERT INTO equity_curve(run_id, ts, equity) VALUES (?, ?, ?)', curve_rows)

            if signals:
                signal_rows = [
                    (run_id, row['symbol'], row['ts'].isoformat().replace('+00:00', 'Z'), row['regime'], row['side'], row['score'], row['notes'])
                    for row in signals
                ]
                conn.executemany(
                    'INSERT INTO signals(run_id, symbol, ts, regime, side, score, notes) VALUES (?, ?, ?, ?, ?, ?, ?)',
                    signal_rows,
                )
            conn.commit()
        return run_id


def get_run_details(run_id: int) -> dict[str, Any]:
    with get_conn(row_factory=True) as conn:
        run = conn.execute('SELECT * FROM backtest_runs WHERE id = ?', (run_id,)).fetchone()
        if not run:
            raise ValueError(f'Run {run_id} not found.')
        trades = conn.execute('SELECT * FROM trades WHERE run_id = ? ORDER BY entry_ts DESC', (run_id,)).fetchall()
        equity = conn.execute('SELECT ts, equity FROM equity_curve WHERE run_id = ? ORDER BY ts', (run_id,)).fetchall()
        signals = conn.execute('SELECT symbol, ts, regime, side, score, notes FROM signals WHERE run_id = ? ORDER BY ts DESC LIMIT 300', (run_id,)).fetchall()
    return {'run': run, 'trades': trades, 'equity': equity, 'signals': signals}


def list_runs(limit: int = 20) -> list[dict[str, Any]]:
    with get_conn(row_factory=True) as conn:
        rows = conn.execute('SELECT * FROM backtest_runs ORDER BY id DESC LIMIT ?', (limit,)).fetchall()
    return rows
