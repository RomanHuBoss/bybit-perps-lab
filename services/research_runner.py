from __future__ import annotations

import csv
import json
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from typing import Any
from uuid import uuid4

from config import INSTANCE_DIR
from database import get_settings, utc_now_iso
from services.optimizer_engine import DEFAULT_PARAM_SPECS, OptimizerEngine, get_optimizer_details


RESEARCH_DIR = INSTANCE_DIR / 'research_runs'


def _mode_overrides(mode: str) -> dict[str, Any]:
    mode = (mode or 'mixed').lower()
    if mode == 'reversion_only':
        return {'trend_enabled': False, 'reversion_enabled': True}
    if mode == 'trend_only':
        return {'trend_enabled': True, 'reversion_enabled': False}
    return {'trend_enabled': True, 'reversion_enabled': True}


DEFAULT_PARAM_SPEC_DICT = {
    spec.name: {'lower': spec.lower, 'upper': spec.upper, 'step': spec.step}
    for spec in DEFAULT_PARAM_SPECS
}


def get_research_presets() -> list[dict[str, Any]]:
    return [
        {
            'name': 'reversion_60d_auto',
            'label': 'Только реверсия / 60д / 2 этапа',
            'description': 'Автоматический двухэтапный поиск для 60 дней 5m истории с hard min-trades фильтром.',
            'symbol_mode': 'single',
            'stages': [
                {
                    'name': 'reversion_stage1',
                    'mode': 'reversion_only',
                    'trials': 90,
                    'optimizer': {
                        'optimizer_train_bars': 9000,
                        'optimizer_test_bars': 1008,
                        'optimizer_step_bars': 1008,
                        'optimizer_max_segments': 8,
                        'optimizer_min_trades_test': 24,
                    },
                    'fixed_overrides': {
                        'regime_filter_enabled': True,
                        'volatility_filter_enabled': True,
                        'no_trade_filter_enabled': True,
                        'risk_per_trade': 0.0020,
                        'atr_stop_mult_trend': 1.5,
                    },
                    'param_specs': {
                        'risk_per_trade': {'lower': 0.0020, 'upper': 0.0020, 'step': 0.0005},
                        'trend_strength_min': {'lower': 0.0008, 'upper': 0.0016, 'step': 0.0001},
                        'reversion_zscore_threshold': {'lower': 1.9, 'upper': 2.2, 'step': 0.1},
                        'volatility_score_min': {'lower': 0.20, 'upper': 0.25, 'step': 0.01},
                        'volatility_score_max': {'lower': 0.95, 'upper': 0.98, 'step': 0.01},
                        'volume_multiplier': {'lower': 1.0, 'upper': 1.4, 'step': 0.1},
                        'atr_stop_mult_trend': {'lower': 1.5, 'upper': 1.5, 'step': 0.1},
                        'atr_stop_mult_reversion': {'lower': 1.3, 'upper': 1.5, 'step': 0.1},
                    },
                },
                {
                    'name': 'reversion_stage2',
                    'mode': 'reversion_only',
                    'trials': 180,
                    'optimizer': {
                        'optimizer_train_bars': 9000,
                        'optimizer_test_bars': 1008,
                        'optimizer_step_bars': 1008,
                        'optimizer_max_segments': 8,
                        'optimizer_min_trades_test': 24,
                    },
                    'fixed_overrides': {
                        'regime_filter_enabled': True,
                        'volatility_filter_enabled': True,
                        'no_trade_filter_enabled': True,
                        'risk_per_trade': 0.0020,
                        'atr_stop_mult_trend': 1.5,
                    },
                    'fallback_param_specs': {
                        'risk_per_trade': {'lower': 0.0020, 'upper': 0.0020, 'step': 0.0005},
                        'trend_strength_min': {'lower': 0.0008, 'upper': 0.0012, 'step': 0.0001},
                        'reversion_zscore_threshold': {'lower': 1.95, 'upper': 2.05, 'step': 0.1},
                        'volatility_score_min': {'lower': 0.24, 'upper': 0.26, 'step': 0.01},
                        'volatility_score_max': {'lower': 0.96, 'upper': 0.98, 'step': 0.01},
                        'volume_multiplier': {'lower': 1.2, 'upper': 1.4, 'step': 0.1},
                        'atr_stop_mult_trend': {'lower': 1.5, 'upper': 1.5, 'step': 0.1},
                        'atr_stop_mult_reversion': {'lower': 1.35, 'upper': 1.45, 'step': 0.1},
                    },
                    'narrow_from_previous': {'top_n': 8, 'prefer_profitable': True, 'margin_steps': 1},
                },
            ],
        },
        {
            'name': 'trend_60d_auto',
            'label': 'Только тренд / 60д / 2 этапа',
            'description': 'Автоматический двухэтапный поиск только для трендового модуля. Использовать как исследовательскую ветку.',
            'symbol_mode': 'single',
            'stages': [
                {
                    'name': 'trend_stage1',
                    'mode': 'trend_only',
                    'trials': 70,
                    'optimizer': {
                        'optimizer_train_bars': 9000,
                        'optimizer_test_bars': 1008,
                        'optimizer_step_bars': 1008,
                        'optimizer_max_segments': 8,
                        'optimizer_min_trades_test': 20,
                    },
                    'fixed_overrides': {
                        'regime_filter_enabled': True,
                        'volatility_filter_enabled': True,
                        'no_trade_filter_enabled': True,
                        'risk_per_trade': 0.0015,
                        'atr_stop_mult_reversion': 1.4,
                    },
                    'param_specs': {
                        'risk_per_trade': {'lower': 0.0015, 'upper': 0.0015, 'step': 0.0005},
                        'trend_strength_min': {'lower': 0.0011, 'upper': 0.0016, 'step': 0.0001},
                        'reversion_zscore_threshold': {'lower': 1.9, 'upper': 2.2, 'step': 0.1},
                        'volatility_score_min': {'lower': 0.20, 'upper': 0.23, 'step': 0.01},
                        'volatility_score_max': {'lower': 0.94, 'upper': 0.98, 'step': 0.01},
                        'volume_multiplier': {'lower': 1.2, 'upper': 1.5, 'step': 0.1},
                        'atr_stop_mult_trend': {'lower': 1.5, 'upper': 1.8, 'step': 0.1},
                        'atr_stop_mult_reversion': {'lower': 1.4, 'upper': 1.4, 'step': 0.1},
                    },
                },
                {
                    'name': 'trend_stage2',
                    'mode': 'trend_only',
                    'trials': 120,
                    'optimizer': {
                        'optimizer_train_bars': 9000,
                        'optimizer_test_bars': 1008,
                        'optimizer_step_bars': 1008,
                        'optimizer_max_segments': 8,
                        'optimizer_min_trades_test': 20,
                    },
                    'fixed_overrides': {
                        'regime_filter_enabled': True,
                        'volatility_filter_enabled': True,
                        'no_trade_filter_enabled': True,
                        'risk_per_trade': 0.0015,
                        'atr_stop_mult_reversion': 1.4,
                    },
                    'fallback_param_specs': {
                        'risk_per_trade': {'lower': 0.0015, 'upper': 0.0015, 'step': 0.0005},
                        'trend_strength_min': {'lower': 0.0012, 'upper': 0.0015, 'step': 0.0001},
                        'reversion_zscore_threshold': {'lower': 1.9, 'upper': 2.1, 'step': 0.1},
                        'volatility_score_min': {'lower': 0.20, 'upper': 0.22, 'step': 0.01},
                        'volatility_score_max': {'lower': 0.95, 'upper': 0.98, 'step': 0.01},
                        'volume_multiplier': {'lower': 1.3, 'upper': 1.5, 'step': 0.1},
                        'atr_stop_mult_trend': {'lower': 1.5, 'upper': 1.7, 'step': 0.1},
                        'atr_stop_mult_reversion': {'lower': 1.4, 'upper': 1.4, 'step': 0.1},
                    },
                    'narrow_from_previous': {'top_n': 8, 'prefer_profitable': True, 'margin_steps': 1},
                },
            ],
        },
        {
            'name': 'dual_60d_auto',
            'label': 'Сдвоенный режим / 60д / сравнение тренда и реверсии',
            'description': 'Сравнительный пакет: один этап только трендового режима и один этап только реверсионного режима без ручного переключения флажков.',
            'symbol_mode': 'single',
            'stages': [
                {
                    'name': 'dual_reversion',
                    'mode': 'reversion_only',
                    'trials': 120,
                    'optimizer': {
                        'optimizer_train_bars': 9000,
                        'optimizer_test_bars': 1008,
                        'optimizer_step_bars': 1008,
                        'optimizer_max_segments': 8,
                        'optimizer_min_trades_test': 24,
                    },
                    'fixed_overrides': {
                        'regime_filter_enabled': True,
                        'volatility_filter_enabled': True,
                        'no_trade_filter_enabled': True,
                        'risk_per_trade': 0.0020,
                        'atr_stop_mult_trend': 1.5,
                    },
                    'param_specs': {
                        'risk_per_trade': {'lower': 0.0020, 'upper': 0.0020, 'step': 0.0005},
                        'trend_strength_min': {'lower': 0.0008, 'upper': 0.0014, 'step': 0.0001},
                        'reversion_zscore_threshold': {'lower': 1.9, 'upper': 2.1, 'step': 0.1},
                        'volatility_score_min': {'lower': 0.22, 'upper': 0.25, 'step': 0.01},
                        'volatility_score_max': {'lower': 0.96, 'upper': 0.98, 'step': 0.01},
                        'volume_multiplier': {'lower': 1.0, 'upper': 1.3, 'step': 0.1},
                        'atr_stop_mult_trend': {'lower': 1.5, 'upper': 1.5, 'step': 0.1},
                        'atr_stop_mult_reversion': {'lower': 1.3, 'upper': 1.5, 'step': 0.1},
                    },
                },
                {
                    'name': 'dual_trend',
                    'mode': 'trend_only',
                    'trials': 120,
                    'optimizer': {
                        'optimizer_train_bars': 9000,
                        'optimizer_test_bars': 1008,
                        'optimizer_step_bars': 1008,
                        'optimizer_max_segments': 8,
                        'optimizer_min_trades_test': 20,
                    },
                    'fixed_overrides': {
                        'regime_filter_enabled': True,
                        'volatility_filter_enabled': True,
                        'no_trade_filter_enabled': True,
                        'risk_per_trade': 0.0015,
                        'atr_stop_mult_reversion': 1.4,
                    },
                    'param_specs': {
                        'risk_per_trade': {'lower': 0.0015, 'upper': 0.0015, 'step': 0.0005},
                        'trend_strength_min': {'lower': 0.0012, 'upper': 0.0016, 'step': 0.0001},
                        'reversion_zscore_threshold': {'lower': 1.9, 'upper': 2.1, 'step': 0.1},
                        'volatility_score_min': {'lower': 0.20, 'upper': 0.22, 'step': 0.01},
                        'volatility_score_max': {'lower': 0.95, 'upper': 0.98, 'step': 0.01},
                        'volume_multiplier': {'lower': 1.3, 'upper': 1.5, 'step': 0.1},
                        'atr_stop_mult_trend': {'lower': 1.5, 'upper': 1.7, 'step': 0.1},
                        'atr_stop_mult_reversion': {'lower': 1.4, 'upper': 1.4, 'step': 0.1},
                    },
                },
            ],
        },
    ]


class AutoResearchRunner:
    def __init__(self, base_settings: dict[str, Any] | None = None):
        self.base_settings = deepcopy(base_settings or get_settings())
        RESEARCH_DIR.mkdir(parents=True, exist_ok=True)

    def run(self, *, symbols: list[str], plan: dict[str, Any], plan_name: str | None = None, note: str | None = None) -> dict[str, Any]:
        symbols = [str(symbol).upper() for symbol in (symbols or []) if str(symbol).strip()]
        if not symbols:
            raise ValueError('Для автоисследования нужен хотя бы один символ.')
        if len(symbols) != 1:
            raise ValueError('Автоисследование сейчас поддерживает ровно один символ на сценарий.')
        plan = deepcopy(plan)
        created_at = utc_now_iso()
        research_run_id = f"research_{created_at.replace(':', '').replace('-', '').replace('+00:00', 'Z').replace('T', '_')}_{uuid4().hex[:8]}"
        run_dir = RESEARCH_DIR / research_run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        stage_reports: list[dict[str, Any]] = []
        combined_rows: list[dict[str, Any]] = []
        previous_stage: dict[str, Any] | None = None

        for index, stage in enumerate(plan.get('stages', []), start=1):
            stage_result = self._run_stage(
                symbols=symbols,
                stage=stage,
                stage_index=index,
                run_dir=run_dir,
                previous_stage=previous_stage,
            )
            stage_reports.append(stage_result)
            previous_stage = stage_result
            combined_rows.extend(stage_result['combined_rows'])

        winner = self._select_global_winner(stage_reports)
        paper_config = self._build_paper_config(winner)
        manifest = {
            'research_run_id': research_run_id,
            'created_at': created_at,
            'plan_name': plan_name or plan.get('name') or 'custom',
            'plan_label': plan.get('label') or plan_name or plan.get('name') or 'custom',
            'note': note,
            'symbols': symbols,
            'stages': [self._manifest_stage_view(stage) for stage in stage_reports],
            'winner': winner,
            'paper_config': paper_config,
            'artifacts': [
                {'name': 'manifest.json', 'label': 'Manifest JSON'},
                {'name': 'report.md', 'label': 'Markdown report'},
                {'name': 'combined_best_configs.csv', 'label': 'Combined best configs CSV'},
            ] + [artifact for stage in stage_reports for artifact in stage['artifacts']],
        }

        self._write_json(run_dir / 'manifest.json', manifest)
        self._write_csv(run_dir / 'combined_best_configs.csv', combined_rows)
        report_text = self._build_markdown_report(manifest)
        (run_dir / 'report.md').write_text(report_text, encoding='utf-8')
        return manifest

    def _run_stage(
        self,
        *,
        symbols: list[str],
        stage: dict[str, Any],
        stage_index: int,
        run_dir: Path,
        previous_stage: dict[str, Any] | None,
    ) -> dict[str, Any]:
        stage_name = stage.get('name') or f'stage_{stage_index}'
        mode = stage.get('mode', 'mixed')
        fixed_overrides = {**_mode_overrides(mode), **deepcopy(stage.get('fixed_overrides') or {})}
        param_specs = self._resolve_stage_param_specs(stage, previous_stage)
        optimizer_settings = deepcopy(self.base_settings)
        optimizer_settings.update(fixed_overrides)
        optimizer_settings.update(stage.get('optimizer') or {})
        optimizer_settings['optimizer_param_specs'] = param_specs

        engine = OptimizerEngine(optimizer_settings)
        result = engine.run(symbols=symbols, trials=int(stage.get('trials', optimizer_settings.get('optimizer_trials', 24))))
        details = get_optimizer_details(int(result['optimizer_run_id']))
        ranked_trials = self._rank_trials(details['trials'])
        eligible_trials = [trial for trial in ranked_trials if trial.get('notes', {}).get('eligible', True)]
        profitable_trials = [trial for trial in eligible_trials if self._is_profitable_trial(trial)]
        winner_trial = profitable_trials[0] if profitable_trials else (eligible_trials[0] if eligible_trials else None)

        stage_summary = {
            'stage_name': stage_name,
            'stage_index': stage_index,
            'mode': mode,
            'optimizer_run_id': result['optimizer_run_id'],
            'trials': int(stage.get('trials', optimizer_settings.get('optimizer_trials', 24))),
            'optimizer_settings': {
                'optimizer_train_bars': int(optimizer_settings.get('optimizer_train_bars')),
                'optimizer_test_bars': int(optimizer_settings.get('optimizer_test_bars')),
                'optimizer_step_bars': int(optimizer_settings.get('optimizer_step_bars')),
                'optimizer_max_segments': int(optimizer_settings.get('optimizer_max_segments')),
                'optimizer_min_trades_test': int(optimizer_settings.get('optimizer_min_trades_test')),
            },
            'fixed_overrides': fixed_overrides,
            'param_specs': param_specs,
            'eligible_trials_count': len(eligible_trials),
            'profitable_eligible_trials_count': len(profitable_trials),
            'positive_return_eligible_trials_count': sum(1 for trial in eligible_trials if float((trial.get('summary') or {}).get('total_return_pct', 0.0)) > 0.0),
            'best_summary': result['best_summary'],
            'best_params': result['best_params'],
            'best_equity_curve': result.get('best_equity_curve', []),
            'winner_trial': self._flatten_trial(winner_trial, stage_name) if winner_trial else None,
            'top_trials': [self._flatten_trial(trial, stage_name) for trial in ranked_trials[:10]],
        }

        rows = [self._flatten_trial(trial, stage_name) for trial in ranked_trials]
        stage_csv_name = f'{stage_index:02d}_{stage_name}_trials.csv'
        stage_json_name = f'{stage_index:02d}_{stage_name}_summary.json'
        self._write_csv(run_dir / stage_csv_name, rows)
        self._write_json(run_dir / stage_json_name, stage_summary)
        return {
            **stage_summary,
            'combined_rows': rows[:20],
            'artifacts': [
                {'name': stage_csv_name, 'label': f'{stage_name} trials CSV'},
                {'name': stage_json_name, 'label': f'{stage_name} summary JSON'},
            ],
        }

    def _resolve_stage_param_specs(self, stage: dict[str, Any], previous_stage: dict[str, Any] | None) -> dict[str, Any]:
        explicit = deepcopy(stage.get('param_specs') or {})
        if explicit:
            return self._normalized_param_specs(explicit)
        fallback = self._normalized_param_specs(deepcopy(stage.get('fallback_param_specs') or {}))
        narrow = stage.get('narrow_from_previous') or {}
        if not previous_stage:
            return fallback or deepcopy(DEFAULT_PARAM_SPEC_DICT)
        candidates = previous_stage.get('top_trials') or []
        if not candidates:
            return fallback or deepcopy(DEFAULT_PARAM_SPEC_DICT)
        top_n = int(narrow.get('top_n', 6))
        prefer_profitable = bool(narrow.get('prefer_profitable', True))
        margin_steps = int(narrow.get('margin_steps', 1))
        if prefer_profitable:
            profitable = [row for row in candidates if self._is_profitable_flattened(row)]
            if profitable:
                candidates = profitable
        candidates = candidates[:top_n]
        base_specs = fallback or deepcopy(DEFAULT_PARAM_SPEC_DICT)
        narrowed: dict[str, Any] = {}
        for name, base in base_specs.items():
            values = [float(row[name]) for row in candidates if row.get(name) is not None]
            if not values:
                narrowed[name] = base
                continue
            step = float(base['step'])
            low = min(values)
            high = max(values)
            if low == high:
                low -= step * max(margin_steps, 1)
                high += step * max(margin_steps, 1)
            else:
                low -= step * margin_steps
                high += step * margin_steps
            narrowed[name] = {
                'lower': round(max(float(base['lower']), low), 10),
                'upper': round(min(float(base['upper']), high), 10),
                'step': step,
            }
            if narrowed[name]['upper'] < narrowed[name]['lower']:
                narrowed[name] = base
        return narrowed

    @staticmethod
    def _normalized_param_specs(specs: dict[str, Any]) -> dict[str, Any]:
        if not specs:
            return {}
        out: dict[str, Any] = {}
        for name, default in DEFAULT_PARAM_SPEC_DICT.items():
            raw = specs.get(name)
            if raw is None:
                out[name] = deepcopy(default)
                continue
            out[name] = {
                'lower': float(raw.get('lower', default['lower'])),
                'upper': float(raw.get('upper', default['upper'])),
                'step': float(raw.get('step', default['step'])),
            }
        return out

    @staticmethod
    def _trial_sort_key(trial: dict[str, Any]) -> tuple[Any, ...]:
        summary = trial.get('summary') or {}
        notes = trial.get('notes') or {}
        eligible = bool(notes.get('eligible', trial.get('eligible', True)))
        profitable = AutoResearchRunner._is_profitable_trial(trial)
        return (
            int(eligible),
            int(profitable),
            float(summary.get('total_return_pct', 0.0)),
            float(summary.get('profit_factor', 0.0) if summary.get('profit_factor') != 'inf' else 3.0),
            float(summary.get('avg_r', 0.0)),
            float(trial.get('score', 0.0)),
        )

    @classmethod
    def _rank_trials(cls, trials: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(trials, key=cls._trial_sort_key, reverse=True)

    @staticmethod
    def _is_profitable_trial(trial: dict[str, Any]) -> bool:
        summary = trial.get('summary') or {}
        return (
            float(summary.get('total_return_pct', 0.0)) > 0.0
            and float(summary.get('profit_factor', 0.0) if summary.get('profit_factor') != 'inf' else 3.0) >= 1.0
            and float(summary.get('avg_r', 0.0)) > 0.0
            and float(summary.get('sharpe', 0.0)) > 0.0
        )

    @staticmethod
    def _is_profitable_flattened(row: dict[str, Any]) -> bool:
        return (
            float(row.get('total_return_pct', 0.0)) > 0.0
            and float(row.get('profit_factor', 0.0) if row.get('profit_factor') != 'inf' else 3.0) >= 1.0
            and float(row.get('avg_r', 0.0)) > 0.0
            and float(row.get('sharpe', 0.0)) > 0.0
        )

    @staticmethod
    def _flatten_trial(trial: dict[str, Any] | None, stage_name: str) -> dict[str, Any]:
        if not trial:
            return {}
        summary = trial.get('summary') or {}
        params = trial.get('params') or {}
        notes = trial.get('notes') or {}
        return {
            'stage_name': stage_name,
            'trial_no': trial.get('trial_no'),
            'score': trial.get('score'),
            'eligible': bool(notes.get('eligible', trial.get('eligible', True))),
            'rejection_reason': notes.get('rejection_reason', trial.get('rejection_reason')),
            **summary,
            **params,
        }

    @staticmethod
    def _select_global_winner(stages: list[dict[str, Any]]) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        for stage in stages:
            if stage.get('winner_trial'):
                winner = dict(stage['winner_trial'])
                winner['mode'] = stage.get('mode')
                winner['optimizer_run_id'] = stage.get('optimizer_run_id')
                winner['best_equity_curve'] = stage.get('best_equity_curve', [])
                candidates.append(winner)
        if not candidates:
            return None
        candidates.sort(
            key=lambda row: (
                int(AutoResearchRunner._is_profitable_flattened(row)),
                float(row.get('total_return_pct', 0.0)),
                float(row.get('profit_factor', 0.0) if row.get('profit_factor') != 'inf' else 3.0),
                float(row.get('avg_r', 0.0)),
                float(row.get('score', 0.0)),
            ),
            reverse=True,
        )
        return candidates[0]

    @staticmethod
    def _build_paper_config(winner: dict[str, Any] | None) -> dict[str, Any] | None:
        if not winner:
            return None
        config = {
            'trend_enabled': winner.get('mode') != 'reversion_only',
            'reversion_enabled': winner.get('mode') != 'trend_only',
            'risk_per_trade': winner.get('risk_per_trade'),
            'trend_strength_min': winner.get('trend_strength_min'),
            'reversion_zscore_threshold': winner.get('reversion_zscore_threshold'),
            'volatility_score_min': winner.get('volatility_score_min'),
            'volatility_score_max': winner.get('volatility_score_max'),
            'volume_multiplier': winner.get('volume_multiplier'),
            'atr_stop_mult_trend': winner.get('atr_stop_mult_trend'),
            'atr_stop_mult_reversion': winner.get('atr_stop_mult_reversion'),
            'regime_filter_enabled': True,
            'volatility_filter_enabled': True,
            'no_trade_filter_enabled': True,
        }
        return {key: value for key, value in config.items() if value is not None}

    @staticmethod
    def _manifest_stage_view(stage: dict[str, Any]) -> dict[str, Any]:
        return {
            'stage_name': stage['stage_name'],
            'stage_index': stage['stage_index'],
            'mode': stage['mode'],
            'optimizer_run_id': stage['optimizer_run_id'],
            'trials': stage['trials'],
            'optimizer_settings': stage['optimizer_settings'],
            'eligible_trials_count': stage['eligible_trials_count'],
            'profitable_eligible_trials_count': stage['profitable_eligible_trials_count'],
            'positive_return_eligible_trials_count': stage['positive_return_eligible_trials_count'],
            'best_summary': stage['best_summary'],
            'best_params': stage['best_params'],
            'winner_trial': stage['winner_trial'],
            'artifacts': stage['artifacts'],
        }

    @staticmethod
    def _build_markdown_report(manifest: dict[str, Any]) -> str:
        winner = manifest.get('winner') or {}
        lines = [
            f"# Auto research report: {manifest.get('plan_label')}",
            '',
            f"- Run ID: `{manifest.get('research_run_id')}`",
            f"- Created at: `{manifest.get('created_at')}`",
            f"- Symbols: `{', '.join(manifest.get('symbols') or [])}`",
            f"- Preset: `{manifest.get('plan_name')}`",
        ]
        if manifest.get('note'):
            lines.append(f"- Note: {manifest['note']}")
        lines.append('')
        lines.append('## Stage summary')
        lines.append('')
        for stage in manifest.get('stages') or []:
            lines.extend([
                f"### {stage['stage_index']}. {stage['stage_name']} ({stage['mode']})",
                '',
                f"- Optimizer run: `{stage['optimizer_run_id']}`",
                f"- Eligible trials: `{stage['eligible_trials_count']}`",
                f"- Profitable eligible trials: `{stage['profitable_eligible_trials_count']}`",
                f"- Positive-return eligible trials: `{stage['positive_return_eligible_trials_count']}`",
                f"- Best return: `{stage['best_summary'].get('total_return_pct', 0.0):.4f}%`",
                f"- Best profit factor: `{stage['best_summary'].get('profit_factor', 0.0)}`",
                f"- Best avg R: `{stage['best_summary'].get('avg_r', 0.0):.4f}`",
                f"- Best trades: `{stage['best_summary'].get('trades_count', 0)}`",
                '',
            ])
        lines.append('## Winner')
        lines.append('')
        if winner:
            lines.extend([
                f"- Stage: `{winner.get('stage_name')}` / mode `{winner.get('mode')}`",
                f"- Return: `{winner.get('total_return_pct', 0.0):.4f}%`",
                f"- Profit factor: `{winner.get('profit_factor')}`",
                f"- Avg R: `{winner.get('avg_r', 0.0):.4f}`",
                f"- Sharpe: `{winner.get('sharpe', 0.0):.4f}`",
                f"- Trades: `{winner.get('trades_count', 0)}`",
                '',
                '### Suggested paper config',
                '',
                '```json',
                json.dumps(manifest.get('paper_config') or {}, indent=2, ensure_ascii=False),
                '```',
            ])
        else:
            lines.append('No eligible winner found.')
        lines.append('')
        lines.append('## Artifacts')
        lines.append('')
        for artifact in manifest.get('artifacts') or []:
            lines.append(f"- `{artifact['name']}` — {artifact.get('label', artifact['name'])}")
        lines.append('')
        return '\n'.join(lines)

    @staticmethod
    def _write_json(path: Path, payload: Any) -> None:
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding='utf-8')

    @staticmethod
    def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
        if not rows:
            rows = [{'status': 'empty'}]
        fieldnames: list[str] = []
        for row in rows:
            for key in row.keys():
                if key not in fieldnames:
                    fieldnames.append(key)
        with path.open('w', encoding='utf-8', newline='') as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                normalized = {key: (json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value) for key, value in row.items()}
                writer.writerow(normalized)


def list_research_runs(limit: int = 20) -> list[dict[str, Any]]:
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    manifests = sorted(RESEARCH_DIR.glob('*/manifest.json'), key=lambda p: p.stat().st_mtime, reverse=True)
    out = []
    for manifest_path in manifests[:limit]:
        try:
            out.append(json.loads(manifest_path.read_text(encoding='utf-8')))
        except Exception:
            continue
    return out


def get_research_run_details(run_id: str) -> dict[str, Any]:
    manifest_path = RESEARCH_DIR / run_id / 'manifest.json'
    if not manifest_path.exists():
        raise ValueError(f'Research run {run_id} not found.')
    return json.loads(manifest_path.read_text(encoding='utf-8'))


def resolve_research_artifact(run_id: str, filename: str) -> Path:
    base = RESEARCH_DIR / run_id
    target = (base / filename).resolve()
    if base.resolve() not in target.parents and target != base.resolve():
        raise ValueError('Invalid artifact path.')
    if not target.exists() or not target.is_file():
        raise ValueError(f'Artifact {filename} not found for research run {run_id}.')
    return target
