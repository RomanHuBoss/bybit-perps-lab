from __future__ import annotations

import json
from itertools import product
from typing import Any

import pandas as pd

from database import get_conn, utc_now_iso
from services.data_service import load_candles, load_funding
from services.simulator import simulate_market, SimulationHelpers


HARD_REJECT_SCORE = -1_000_000_000.0


class WalkForwardEngine:
    def __init__(self, settings: dict[str, Any]):
        self.settings = settings

    def run(self, symbols: list[str]) -> dict[str, Any]:
        symbols = [s.upper() for s in symbols]
        candles = load_candles(symbols, interval='5')
        if candles.empty:
            raise ValueError('No 5-minute candle data found for walk-forward.')
        funding = load_funding(symbols)
        all_times = sorted(pd.to_datetime(candles['ts'], utc=True).unique())
        train_bars = int(self.settings.get('walkforward_train_bars', 2016))
        test_bars = int(self.settings.get('walkforward_test_bars', 576))
        step_bars = int(self.settings.get('walkforward_step_bars', test_bars))
        self._validate_window_params(train_bars, test_bars, step_bars)
        if len(all_times) < train_bars + test_bars + 10:
            raise ValueError('Not enough bars for walk-forward. Load a longer history or reduce train/test window sizes.')

        candidate_grid = self._build_candidate_grid()
        segment_no = 0
        max_segments = int(self.settings.get('walkforward_max_segments', 10))
        carry_equity = float(self.settings.get('starting_equity', 10000.0))
        aggregate_trades: list[dict[str, Any]] = []
        aggregate_equity: list[dict[str, Any]] = []
        segments: list[dict[str, Any]] = []
        cursor = train_bars

        while cursor + test_bars <= len(all_times) and segment_no < max_segments:
            segment_no += 1
            train_start = all_times[cursor - train_bars]
            train_end = all_times[cursor - 1]
            test_start = all_times[cursor]
            test_end = all_times[cursor + test_bars - 1]
            warmup_train_start = all_times[max(0, cursor - train_bars * 2)]

            train_candles = candles[(candles['ts'] >= warmup_train_start) & (candles['ts'] <= train_end)].copy()
            train_funding = funding[(funding['ts'] >= warmup_train_start - pd.Timedelta(hours=8)) & (funding['ts'] <= train_end)].copy() if not funding.empty else funding
            test_candles = candles[(candles['ts'] >= train_start) & (candles['ts'] <= test_end)].copy()
            test_funding = funding[(funding['ts'] >= train_start - pd.Timedelta(hours=8)) & (funding['ts'] <= test_end)].copy() if not funding.empty else funding

            best_params = None
            best_objective = float('-inf')
            best_train_result = None
            min_trades = int(self.settings.get('walkforward_min_trades_train', 6))

            for params in candidate_grid:
                train_settings = dict(self.settings)
                train_settings.update(params)
                train_settings['starting_equity'] = carry_equity
                try:
                    train_result = simulate_market(train_settings, train_candles, train_funding, symbols, starting_equity=carry_equity, trade_start_ts=train_start)
                except Exception:
                    continue
                if not self._passes_min_trades_filter(train_result['summary'], min_trades):
                    continue
                objective = self._score_train_result(train_result['summary'])
                if objective > best_objective:
                    best_objective = objective
                    best_params = params
                    best_train_result = train_result

            if best_params is None or best_train_result is None:
                raise ValueError(
                    f'No walk-forward training candidate satisfied walkforward_min_trades_train={min_trades} '
                    f'in segment {segment_no}. Reduce the threshold, widen the candidate grid, or load a longer history.'
                )

            test_settings = dict(self.settings)
            test_settings.update(best_params)
            test_settings['starting_equity'] = carry_equity
            test_result = simulate_market(test_settings, test_candles, test_funding, symbols, starting_equity=carry_equity, trade_start_ts=test_start)

            carry_equity = float(test_result['summary']['ending_equity'])
            aggregate_trades.extend(test_result['trades'])
            if not aggregate_equity:
                aggregate_equity.extend(test_result['equity_curve'])
            else:
                aggregate_equity.extend(test_result['equity_curve'][1:])

            segments.append(
                {
                    'segment_no': segment_no,
                    'train_start_ts': train_start.isoformat().replace('+00:00', 'Z'),
                    'train_end_ts': train_end.isoformat().replace('+00:00', 'Z'),
                    'test_start_ts': test_start.isoformat().replace('+00:00', 'Z'),
                    'test_end_ts': test_end.isoformat().replace('+00:00', 'Z'),
                    'best_params': best_params,
                    'objective_score': round(best_objective, 6),
                    'train_summary': best_train_result['summary'],
                    'test_summary': test_result['summary'],
                }
            )
            cursor += step_bars

        agg_trades_df = pd.DataFrame(aggregate_trades)
        agg_equity_df = pd.DataFrame(aggregate_equity)
        metrics = SimulationHelpers.compute_metrics(agg_trades_df, agg_equity_df, float(self.settings.get('starting_equity', 10000.0)))
        run_id = self._persist_run(symbols, train_bars, test_bars, step_bars, segments, metrics)
        return {
            'walkforward_run_id': run_id,
            'summary': metrics,
            'segments': segments,
            'trades': aggregate_trades[-200:],
            'equity_curve': aggregate_equity,
        }

    def _build_candidate_grid(self) -> list[dict[str, Any]]:
        trend_strength_values = [0.0022, 0.0028]
        reversion_values = [1.9, 2.3]
        volume_values = [1.1, 1.25]
        volatility_min_values = [0.16]
        candidates = []
        for trend_strength, reversion_z, volume_multiplier, volatility_min in product(
            trend_strength_values,
            reversion_values,
            volume_values,
            volatility_min_values,
        ):
            candidates.append(
                {
                    'trend_strength_min': trend_strength,
                    'reversion_zscore_threshold': reversion_z,
                    'volume_multiplier': volume_multiplier,
                    'volatility_score_min': volatility_min,
                }
            )
        limit = int(self.settings.get('walkforward_candidate_limit', len(candidates)))
        return candidates[:limit]

    def _score_train_result(self, summary: dict[str, Any]) -> float:
        min_trades = int(self.settings.get('walkforward_min_trades_train', 6))
        trades = int(summary.get('trades_count', 0))
        profit_factor = summary.get('profit_factor', 0.0)
        if profit_factor == 'inf':
            profit_factor = 3.0
        profit_factor = min(float(profit_factor), 3.0)
        total_return_pct = float(summary.get('total_return_pct', 0.0))
        avg_r = float(summary.get('avg_r', 0.0))
        expectancy_pct = float(summary.get('expectancy_pct', 0.0))
        win_rate = float(summary.get('win_rate', 0.0))
        max_drawdown_pct = abs(float(summary.get('max_drawdown_pct', 0.0)))
        stop_rate = float(summary.get('stop_rate', 0.0))

        score = (
            total_return_pct * 3.0
            + profit_factor * 4.0
            + avg_r * 18.0
            + expectancy_pct * 10.0
            + win_rate * 0.02
            - max_drawdown_pct * 1.5
            - stop_rate * 0.04
        )
        if trades < min_trades:
            return HARD_REJECT_SCORE
        if total_return_pct <= 0.0:
            score -= 8.0 + abs(total_return_pct) * 1.5
        if profit_factor < 1.0:
            score -= (1.0 - profit_factor) * 16.0
        if avg_r <= 0.0:
            score -= abs(avg_r) * 40.0 + 4.0
        if expectancy_pct <= 0.0:
            score -= abs(expectancy_pct) * 16.0 + 2.0
        return score


    @staticmethod
    def _passes_min_trades_filter(summary: dict[str, Any], min_trades: int) -> bool:
        return int(summary.get('trades_count', 0)) >= int(min_trades)

    @staticmethod
    def _validate_window_params(train_bars: int, test_bars: int, step_bars: int) -> None:
        if train_bars <= 0 or test_bars <= 0 or step_bars <= 0:
            raise ValueError('Walk-forward windows must be positive integers.')
        if step_bars < test_bars:
            raise ValueError('Walk-forward step bars must be greater than or equal to test bars to avoid overlapping out-of-sample windows.')

    def _persist_run(
        self,
        symbols: list[str],
        train_bars: int,
        test_bars: int,
        step_bars: int,
        segments: list[dict[str, Any]],
        metrics: dict[str, Any],
    ) -> int:
        with get_conn() as conn:
            cursor = conn.execute(
                '''
                INSERT INTO walkforward_runs(created_at, config_json, symbols_json, status, train_bars, test_bars, step_bars,
                                             starting_equity, ending_equity, total_return_pct, max_drawdown_pct, win_rate,
                                             profit_factor, sharpe, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    utc_now_iso(),
                    json.dumps(self.settings),
                    json.dumps(symbols),
                    'completed',
                    train_bars,
                    test_bars,
                    step_bars,
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
                        'segments_count': len(segments),
                    }),
                ),
            )
            run_id = int(cursor.lastrowid)
            segment_rows = [
                (
                    run_id,
                    seg['segment_no'],
                    seg['train_start_ts'],
                    seg['train_end_ts'],
                    seg['test_start_ts'],
                    seg['test_end_ts'],
                    json.dumps(seg['best_params']),
                    json.dumps({'train_summary': seg['train_summary'], 'test_summary': seg['test_summary']}),
                    seg['objective_score'],
                )
                for seg in segments
            ]
            if segment_rows:
                conn.executemany(
                    '''
                    INSERT INTO walkforward_segments(walkforward_run_id, segment_no, train_start_ts, train_end_ts,
                                                     test_start_ts, test_end_ts, best_params_json, metrics_json, objective_score)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    segment_rows,
                )
            conn.commit()
        return run_id


def list_walkforward_runs(limit: int = 20) -> list[dict[str, Any]]:
    with get_conn(row_factory=True) as conn:
        rows = conn.execute('SELECT * FROM walkforward_runs ORDER BY id DESC LIMIT ?', (limit,)).fetchall()
    return rows


def get_walkforward_details(run_id: int) -> dict[str, Any]:
    with get_conn(row_factory=True) as conn:
        run = conn.execute('SELECT * FROM walkforward_runs WHERE id = ?', (run_id,)).fetchone()
        if not run:
            raise ValueError(f'Walk-forward run {run_id} not found.')
        segments = conn.execute(
            'SELECT * FROM walkforward_segments WHERE walkforward_run_id = ? ORDER BY segment_no',
            (run_id,),
        ).fetchall()
    return {'run': run, 'segments': segments}
