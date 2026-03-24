from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from database import get_conn, utc_now_iso
from services.data_service import load_candles, load_funding
from services.simulator import SimulationHelpers, simulate_market


@dataclass
class ParamSpec:
    name: str
    lower: float
    upper: float
    step: float


PARAM_SPECS: list[ParamSpec] = [
    ParamSpec('risk_per_trade', 0.0015, 0.0045, 0.0005),
    ParamSpec('trend_strength_min', 0.0008, 0.0030, 0.0001),
    ParamSpec('reversion_zscore_threshold', 1.8, 3.2, 0.1),
    ParamSpec('volatility_score_min', 0.08, 0.25, 0.01),
    ParamSpec('volatility_score_max', 0.75, 0.98, 0.01),
    ParamSpec('volume_multiplier', 1.0, 1.5, 0.1),
    ParamSpec('atr_stop_mult_trend', 1.0, 2.0, 0.1),
    ParamSpec('atr_stop_mult_reversion', 0.8, 1.5, 0.1),
]


class OptimizerEngine:
    def __init__(self, settings: dict[str, Any]):
        self.settings = dict(settings)
        self.rng = np.random.default_rng(int(self.settings.get('optimizer_random_seed', 42)))

    def run(self, symbols: list[str], trials: int | None = None) -> dict[str, Any]:
        symbols = [s.upper() for s in symbols]
        candles = load_candles(symbols, interval='5')
        if candles.empty:
            raise ValueError('No 5-minute candle data found for optimizer.')
        funding = load_funding(symbols)
        all_times = sorted(pd.to_datetime(candles['ts'], utc=True).unique())

        train_bars = int(self.settings.get('optimizer_train_bars', self.settings.get('walkforward_train_bars', 2016)))
        test_bars = int(self.settings.get('optimizer_test_bars', self.settings.get('walkforward_test_bars', 576)))
        step_bars = int(self.settings.get('optimizer_step_bars', self.settings.get('walkforward_step_bars', test_bars)))
        max_segments = int(self.settings.get('optimizer_max_segments', self.settings.get('walkforward_max_segments', 8)))
        trials_count = int(trials or self.settings.get('optimizer_trials', 24))

        if len(all_times) < train_bars + test_bars + 10:
            raise ValueError('Not enough bars for optimizer. Load a longer history or reduce optimizer windows.')

        trial_results: list[dict[str, Any]] = []
        best_result: dict[str, Any] | None = None
        baseline = self._baseline_params()

        for trial_no in range(1, trials_count + 1):
            params = baseline if trial_no == 1 else self._sample_params(best_result['params'] if best_result else None, trial_no, trials_count)
            result = self._evaluate_params(params, candles, funding, symbols, all_times, train_bars, test_bars, step_bars, max_segments)
            trial_results.append(
                {
                    'trial_no': trial_no,
                    'params': params,
                    'score': result['score'],
                    'summary': result['summary'],
                    'segments': result['segments'],
                    'equity_curve': result['equity_curve'],
                }
            )
            if best_result is None or result['score'] > best_result['score']:
                best_result = {'trial_no': trial_no, **result}

        if best_result is None:
            raise ValueError('Optimizer failed to evaluate any candidate.')

        run_id = self._persist_run(symbols, trials_count, train_bars, test_bars, step_bars, max_segments, trial_results, best_result)
        top_trials = [self._strip_trial_payload(t) for t in sorted(trial_results, key=lambda x: x['score'], reverse=True)[:30]]
        return {
            'optimizer_run_id': run_id,
            'best_params': best_result['params'],
            'best_summary': best_result['summary'],
            'best_score': best_result['score'],
            'best_trial_no': best_result['trial_no'],
            'trials': top_trials,
            'best_equity_curve': best_result['equity_curve'],
            'segments': best_result['segments'],
        }

    def _baseline_params(self) -> dict[str, Any]:
        out = {}
        for spec in PARAM_SPECS:
            current = float(self.settings.get(spec.name, spec.lower))
            out[spec.name] = self._round_to_step(current, spec)
        return out

    def _sample_params(self, best_params: dict[str, Any] | None, trial_no: int, trials_count: int) -> dict[str, Any]:
        exploit = best_params is not None and trial_no > max(3, int(trials_count * 0.45))
        params: dict[str, Any] = {}
        for spec in PARAM_SPECS:
            if exploit:
                center = float(best_params.get(spec.name, self.settings.get(spec.name, spec.lower)))
                span = (spec.upper - spec.lower) * (0.35 if trial_no < trials_count * 0.75 else 0.18)
                raw = float(self.rng.normal(center, max(span, spec.step) / 2.0))
            else:
                raw = float(self.rng.uniform(spec.lower, spec.upper))
            params[spec.name] = self._round_to_step(raw, spec)

        if params['volatility_score_max'] <= params['volatility_score_min'] + 0.05:
            params['volatility_score_max'] = min(0.99, round(params['volatility_score_min'] + 0.08, 2))
        if params['atr_stop_mult_trend'] < params['atr_stop_mult_reversion']:
            params['atr_stop_mult_trend'] = max(params['atr_stop_mult_reversion'], params['atr_stop_mult_trend'])
        return params

    def _evaluate_params(
        self,
        params: dict[str, Any],
        candles: pd.DataFrame,
        funding: pd.DataFrame,
        symbols: list[str],
        all_times: list[pd.Timestamp],
        train_bars: int,
        test_bars: int,
        step_bars: int,
        max_segments: int,
    ) -> dict[str, Any]:
        starting_equity = float(self.settings.get('starting_equity', 10000.0))
        carry_equity = starting_equity
        aggregate_trades: list[dict[str, Any]] = []
        aggregate_equity: list[dict[str, Any]] = []
        segments: list[dict[str, Any]] = []
        cursor = train_bars
        segment_no = 0
        parsed_candles = candles.copy()
        parsed_candles['ts'] = pd.to_datetime(parsed_candles['ts'], utc=True)
        parsed_funding = funding.copy()
        if not parsed_funding.empty:
            parsed_funding['ts'] = pd.to_datetime(parsed_funding['ts'], utc=True)

        while cursor + test_bars <= len(all_times) and segment_no < max_segments:
            segment_no += 1
            warmup_start = all_times[max(0, cursor - train_bars)]
            test_start = all_times[cursor]
            test_end = all_times[cursor + test_bars - 1]
            segment_candles = parsed_candles[(parsed_candles['ts'] >= warmup_start) & (parsed_candles['ts'] <= test_end)].copy()
            segment_funding = parsed_funding[(parsed_funding['ts'] >= warmup_start - pd.Timedelta(hours=8)) & (parsed_funding['ts'] <= test_end)].copy() if not parsed_funding.empty else parsed_funding
            segment_settings = dict(self.settings)
            segment_settings.update(params)
            segment_settings['starting_equity'] = carry_equity
            result = simulate_market(
                segment_settings,
                segment_candles,
                segment_funding,
                symbols,
                starting_equity=carry_equity,
                trade_start_ts=test_start,
            )
            carry_equity = float(result['summary']['ending_equity'])
            aggregate_trades.extend(result['trades'])
            if not aggregate_equity:
                aggregate_equity.extend(result['equity_curve'])
            elif result['equity_curve']:
                aggregate_equity.extend(result['equity_curve'][1:])
            segments.append(
                {
                    'segment_no': segment_no,
                    'test_start_ts': test_start.isoformat().replace('+00:00', 'Z'),
                    'test_end_ts': test_end.isoformat().replace('+00:00', 'Z'),
                    'summary': result['summary'],
                }
            )
            cursor += step_bars

        trades_df = pd.DataFrame(aggregate_trades)
        equity_df = pd.DataFrame(aggregate_equity)
        summary = SimulationHelpers.compute_metrics(trades_df, equity_df, starting_equity)
        score = self._objective(summary)
        return {
            'params': params,
            'summary': summary,
            'score': float(round(score, 6)),
            'segments': segments,
            'equity_curve': aggregate_equity,
            'trades': aggregate_trades,
        }

    def _objective(self, summary: dict[str, Any]) -> float:
        min_trades = int(self.settings.get('optimizer_min_trades_test', 12))
        trades = int(summary.get('trades_count', 0))
        pf = summary.get('profit_factor', 0.0)
        if pf == 'inf':
            pf = 3.0
        pf = min(float(pf), 3.0)
        score = (
            float(summary.get('total_return_pct', 0.0)) * 1.4
            + pf * 6.0
            + float(summary.get('avg_r', 0.0)) * 12.0
            + float(summary.get('expectancy_pct', 0.0)) * 8.0
            + float(summary.get('win_rate', 0.0)) * 0.04
            - abs(float(summary.get('max_drawdown_pct', 0.0))) * 1.5
            - float(summary.get('stop_rate', 0.0)) * 0.08
        )
        if trades < min_trades:
            score -= (min_trades - trades) * 1.8
        return score

    def _strip_trial_payload(self, trial: dict[str, Any]) -> dict[str, Any]:
        return {
            'trial_no': trial['trial_no'],
            'score': trial['score'],
            'params': trial['params'],
            'summary': trial['summary'],
        }

    @staticmethod
    def _round_to_step(value: float, spec: ParamSpec) -> float:
        clipped = min(max(value, spec.lower), spec.upper)
        steps = round((clipped - spec.lower) / spec.step)
        rounded = spec.lower + steps * spec.step
        precision = max(0, len(str(spec.step).split('.')[-1]) if '.' in str(spec.step) else 0)
        return round(min(max(rounded, spec.lower), spec.upper), precision)

    def _persist_run(
        self,
        symbols: list[str],
        trials_count: int,
        train_bars: int,
        test_bars: int,
        step_bars: int,
        max_segments: int,
        trial_results: list[dict[str, Any]],
        best_result: dict[str, Any],
    ) -> int:
        summary = best_result['summary']
        notes = {
            'best_trial_no': best_result['trial_no'],
            'best_equity_curve': best_result['equity_curve'],
            'segments': best_result['segments'],
            'search_space': {spec.name: {'lower': spec.lower, 'upper': spec.upper, 'step': spec.step} for spec in PARAM_SPECS},
        }
        with get_conn() as conn:
            cursor = conn.execute(
                '''
                INSERT INTO optimizer_runs(
                    created_at, config_json, symbols_json, status, search_method,
                    trials_count, train_bars, test_bars, step_bars, max_segments,
                    best_score, best_params_json, best_summary_json, notes_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    utc_now_iso(),
                    json.dumps(self.settings),
                    json.dumps(symbols),
                    'completed',
                    'adaptive-random-search',
                    trials_count,
                    train_bars,
                    test_bars,
                    step_bars,
                    max_segments,
                    best_result['score'],
                    json.dumps(best_result['params']),
                    json.dumps(summary),
                    json.dumps(notes),
                ),
            )
            run_id = int(cursor.lastrowid)
            conn.executemany(
                '''
                INSERT INTO optimizer_trials(optimizer_run_id, trial_no, score, params_json, summary_json, notes_json)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                [
                    (
                        run_id,
                        trial['trial_no'],
                        trial['score'],
                        json.dumps(trial['params']),
                        json.dumps(trial['summary']),
                        json.dumps({'segments_count': len(trial.get('segments', []))}),
                    )
                    for trial in trial_results
                ],
            )
            conn.commit()
        return run_id


def list_optimizer_runs(limit: int = 20) -> list[dict[str, Any]]:
    with get_conn(row_factory=True) as conn:
        rows = conn.execute('SELECT * FROM optimizer_runs ORDER BY id DESC LIMIT ?', (limit,)).fetchall()
    return rows


def get_optimizer_details(run_id: int) -> dict[str, Any]:
    with get_conn(row_factory=True) as conn:
        run = conn.execute('SELECT * FROM optimizer_runs WHERE id = ?', (run_id,)).fetchone()
        if not run:
            raise ValueError(f'Optimizer run {run_id} not found.')
        trials = conn.execute('SELECT * FROM optimizer_trials WHERE optimizer_run_id = ? ORDER BY score DESC, trial_no', (run_id,)).fetchall()
    best_params = json.loads(run['best_params_json']) if run.get('best_params_json') else {}
    best_summary = json.loads(run['best_summary_json']) if run.get('best_summary_json') else {}
    notes = json.loads(run['notes_json']) if run.get('notes_json') else {}
    parsed_trials = []
    for trial in trials:
        row = dict(trial)
        row['params'] = json.loads(row['params_json']) if row.get('params_json') else {}
        row['summary'] = json.loads(row['summary_json']) if row.get('summary_json') else {}
        row['notes'] = json.loads(row['notes_json']) if row.get('notes_json') else {}
        parsed_trials.append(row)
    return {
        'run': run,
        'best_params': best_params,
        'best_summary': best_summary,
        'best_equity_curve': notes.get('best_equity_curve', []),
        'segments': notes.get('segments', []),
        'trials': parsed_trials,
        'search_space': notes.get('search_space', {}),
    }
