from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import app
from database import ensure_database, get_settings
from services import simulator as simulator_module
from services import strategy_engine as strategy_module
from services.backtest_engine import BacktestEngine
from services.data_service import generate_demo_market_data, load_candles, load_funding
from services.indicators import prepare_symbol_features
from services.optimizer_engine import OptimizerEngine
from services.paper_engine import PaperTradingManager, SessionRuntime, paper_manager
from services.simulator import Position, PreparedMarket, SimulationHelpers, simulate_market
from services.trade_service import _round_stop_price, _round_take_profit_price
from services.walkforward_engine import WalkForwardEngine


def main() -> None:
    ensure_database()
    print(generate_demo_market_data(['BTCUSDT', 'ETHUSDT', 'SOLUSDT'], days=12))
    settings = get_settings()

    candles = load_candles(['BTCUSDT'], '5')
    assert (candles['ts'].dt.minute % 5 == 0).all()
    print('demo timestamp alignment regression: OK')
    sym = candles[candles.symbol == 'BTCUSDT'].copy()
    feat = prepare_symbol_features(sym)
    probe = feat.iloc[500]
    ts = probe['ts']
    base = sym.sort_values('ts').set_index('ts')
    prev_hour = ts.floor('1h') - pd.Timedelta(hours=1)
    prev_hour_close = base[(base.index >= prev_hour) & (base.index < prev_hour + pd.Timedelta(hours=1))]['close'].iloc[-1]
    assert abs(float(probe['h1_close']) - float(prev_hour_close)) < 1e-9
    print('lookahead regression: OK')

    back = BacktestEngine(settings).run(['BTCUSDT', 'ETHUSDT', 'SOLUSDT'])
    print('backtest summary:', back['summary'])

    wf = WalkForwardEngine(settings).run(['BTCUSDT', 'ETHUSDT'])
    print('walkforward summary:', wf['summary'])

    opt = OptimizerEngine(settings).run(['SOLUSDT'], trials=3)
    print('optimizer best summary:', opt['best_summary'])

    client = app.test_client()
    resp = client.post('/api/config', json={'volume_multiplier': '1,05', 'trend_enabled': 'false'})
    assert resp.status_code == 200
    assert abs(resp.json['volume_multiplier'] - 1.05) < 1e-12
    assert resp.json['trend_enabled'] is False
    client.post('/api/config', json={'volume_multiplier': settings['volume_multiplier'], 'trend_enabled': settings['trend_enabled']})
    print('config coercion regression: OK')

    session = paper_manager.create_session(name='self-check', symbols=['BTCUSDT', 'ETHUSDT'], settings=settings, poll_seconds=0.1, auto_steps=1)
    step = paper_manager.step_session(session['session']['id'], steps=5)
    print('paper summary:', step['summary'])

    all_symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
    all_candles = load_candles(all_symbols, '5')
    all_funding = load_funding(all_symbols)
    expected = simulate_market(settings, all_candles, all_funding, all_symbols)
    manager = PaperTradingManager()
    runtime = manager._build_runtime(symbols=all_symbols, settings=settings, session_id=-1)
    for idx in range(len(runtime.all_times)):
        manager._process_bar(runtime, idx)
        runtime.current_index = idx
    manager._finalize_runtime_at_end(runtime)
    paper_metrics = SimulationHelpers.compute_metrics(pd.DataFrame(runtime.trades), pd.DataFrame(runtime.equity_curve), settings['starting_equity'])
    assert len(runtime.trades) == len(expected['trades'])
    assert abs(float(paper_metrics['ending_equity']) - float(expected['summary']['ending_equity'])) < 1e-6
    print('paper/backtest consistency regression: OK')

    from decimal import Decimal
    assert _round_stop_price(Decimal('99.996'), Decimal('0.01'), 'LONG', Decimal('100.00')) == Decimal('99.99')
    assert _round_take_profit_price(Decimal('100.004'), Decimal('0.01'), 'LONG', Decimal('100.00')) == Decimal('100.01')
    assert _round_stop_price(Decimal('100.004'), Decimal('0.01'), 'SHORT', Decimal('100.00')) == Decimal('100.01')
    assert _round_take_profit_price(Decimal('99.996'), Decimal('0.01'), 'SHORT', Decimal('100.00')) == Decimal('99.99')
    print('directional tick rounding regression: OK')

    original_prepare_symbol_features = strategy_module.prepare_symbol_features
    try:
        strategy_module.prepare_symbol_features = lambda _df: pd.DataFrame([
            {
                'ts': pd.Timestamp('2026-01-01T00:00:00Z'),
                'symbol': 'BTCUSDT',
                'open': 100.0,
                'high': 102.0,
                'low': 99.0,
                'close': 101.0,
                'volume': 150.0,
                'vol_sma_20': 100.0,
                'atr_14': 1.5,
                'h4_ema_50': 110.0,
                'h4_ema_200': 100.0,
                'h1_close': 101.0,
                'h1_ema_20': 100.0,
                'h1_ema_50': 99.0,
                'm15_break_high_8': 100.0,
                'm15_break_low_8': 98.0,
                'm15_vol_sma_20': 400.0,
                'trend_strength': 0.01,
                'volatility_score': 0.5,
                'dev_zscore': 0.0,
                'trend_alignment': 1,
                'rolling_low_20': 95.0,
                'rolling_high_20': 105.0,
                'vwap_48': 100.0,
            }
        ])
        _, signals = strategy_module.StrategyFactory(settings).build_symbol_signals(pd.DataFrame([{'symbol': 'BTCUSDT'}]))
        assert signals and signals[0].side == 'LONG'
    finally:
        strategy_module.prepare_symbol_features = original_prepare_symbol_features
    print('volume timeframe consistency regression: OK')

    scenario_settings = {
        'starting_equity': 10000.0,
        'risk_per_trade': 0.01,
        'max_concurrent_positions': 1,
        'max_same_side_positions': 1,
        'daily_loss_limit': 1.0,
        'max_daily_stopouts': 10,
        'cooldown_bars_after_stop': 0,
        'max_consecutive_losses': 10,
        'max_leverage': 100.0,
        'entry_fee_rate': 0.0,
        'exit_fee_rate_take_profit': 0.0,
        'exit_fee_rate_stop': 0.0,
        'entry_slippage_bps': 0.0,
        'exit_slippage_bps_tp': 0.0,
        'exit_slippage_bps_stop': 0.0,
    }

    def _run_custom_simulation(bar_rows):
        signal = strategy_module.StrategySignal(
            ts=bar_rows[0]['ts'], symbol='BTCUSDT', regime='trend', side='LONG', score=0.9,
            entry_price=100.0, stop_price=90.0, tp1_price=110.0, tp2_price=120.0,
            stop_distance=10.0, tp1_r_multiple=1.0, tp2_r_multiple=2.0, notes='scenario'
        )
        prepared = PreparedMarket(
            symbol_bars={'BTCUSDT': pd.DataFrame(bar_rows)},
            signals_by_symbol={'BTCUSDT': {bar_rows[0]['ts']: signal}},
            signal_rows=[],
            funding_maps={},
            all_times=[row['ts'] for row in bar_rows],
        )
        original_prepare_market = simulator_module.prepare_market
        try:
            simulator_module.prepare_market = lambda _settings, _candles, _funding, _symbols: prepared
            return simulator_module.simulate_market(scenario_settings, pd.DataFrame({'dummy': [1]}), pd.DataFrame(), ['BTCUSDT'])
        finally:
            simulator_module.prepare_market = original_prepare_market

    gap_stop_result = _run_custom_simulation([
        {'ts': pd.Timestamp('2026-01-01T00:00:00Z'), 'symbol': 'BTCUSDT', 'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.0, 'volume': 1.0},
        {'ts': pd.Timestamp('2026-01-01T00:05:00Z'), 'symbol': 'BTCUSDT', 'open': 105.0, 'high': 106.0, 'low': 104.0, 'close': 105.0, 'volume': 1.0},
        {'ts': pd.Timestamp('2026-01-01T00:10:00Z'), 'symbol': 'BTCUSDT', 'open': 80.0, 'high': 81.0, 'low': 79.0, 'close': 80.0, 'volume': 1.0},
    ])
    assert gap_stop_result['trades'][0]['exit_reason'] == 'stop'
    assert abs(float(gap_stop_result['trades'][0]['exit_price']) - 80.0) < 1e-9
    print('gap-through-stop regression: OK')

    tp_ladder_result = _run_custom_simulation([
        {'ts': pd.Timestamp('2026-01-01T00:00:00Z'), 'symbol': 'BTCUSDT', 'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.0, 'volume': 1.0},
        {'ts': pd.Timestamp('2026-01-01T00:05:00Z'), 'symbol': 'BTCUSDT', 'open': 105.0, 'high': 106.0, 'low': 104.0, 'close': 105.0, 'volume': 1.0},
        {'ts': pd.Timestamp('2026-01-01T00:10:00Z'), 'symbol': 'BTCUSDT', 'open': 116.0, 'high': 126.0, 'low': 112.0, 'close': 113.0, 'volume': 1.0},
    ])
    assert tp_ladder_result['trades'][0]['exit_reason'] == 'tp2'
    assert abs(float(tp_ladder_result['trades'][0]['gross_pnl']) - 155.0) < 1e-9
    print('same-bar tp ladder regression: OK')

    optimizer_probe = OptimizerEngine(settings)
    profitable_summary = {
        'total_return_pct': 0.05,
        'profit_factor': 1.01,
        'avg_r': 0.01,
        'expectancy_pct': 0.002,
        'win_rate': 55.0,
        'max_drawdown_pct': -0.8,
        'stop_rate': 65.0,
        'trades_count': 28,
    }
    cosmetically_good_loser = {
        'total_return_pct': -0.12,
        'profit_factor': 0.95,
        'avg_r': -0.02,
        'expectancy_pct': -0.004,
        'win_rate': 56.0,
        'max_drawdown_pct': -0.63,
        'stop_rate': 65.5,
        'trades_count': 32,
    }
    assert optimizer_probe._objective(profitable_summary) > optimizer_probe._objective(cosmetically_good_loser)
    print('optimizer profitability-first objective regression: OK')

    try:
        OptimizerEngine(settings | {'optimizer_train_bars': 1000, 'optimizer_test_bars': 600, 'optimizer_step_bars': 500})._validate_window_params(1000, 600, 500)
        raise AssertionError('optimizer overlap validation failed')
    except ValueError:
        pass
    try:
        WalkForwardEngine(settings | {'walkforward_train_bars': 1000, 'walkforward_test_bars': 600, 'walkforward_step_bars': 500})._validate_window_params(1000, 600, 500)
        raise AssertionError('walk-forward overlap validation failed')
    except ValueError:
        pass
    print('window-overlap validation regression: OK')

    ts = pd.Timestamp('2026-01-01T00:00:00Z')
    dummy_bars = pd.DataFrame([{'ts': ts, 'symbol': 'BTCUSDT', 'open': 100.0, 'high': 100.0, 'low': 100.0, 'close': 100.0, 'volume': 1.0}])
    prepared = PreparedMarket(symbol_bars={'BTCUSDT': dummy_bars}, signals_by_symbol={'BTCUSDT': {}}, signal_rows=[], funding_maps={}, all_times=[ts])
    dummy_runtime = SessionRuntime(session_id=-1, symbols=['BTCUSDT'], settings=settings, prepared=prepared, starting_equity=10000.0)
    dummy_runtime.positions['BTCUSDT'] = Position(
        symbol='BTCUSDT', regime='trend', side='LONG', entry_ts=ts, entry_price=95.0, stop_price=90.0,
        tp1_price=100.0, tp2_price=105.0, qty=1.0, initial_qty=1.0, risk_per_unit=5.0, score=0.9, notes='dummy', max_hold_bars=10
    )
    manager._finalize_runtime_at_end(dummy_runtime)
    assert not dummy_runtime.positions
    assert dummy_runtime.trades[-1]['exit_reason'] == 'end_of_test'
    assert dummy_runtime.finalized_at_end is True
    print('paper end-of-test liquidation regression: OK')

    print('ALL CHECKS PASSED')


if __name__ == '__main__':
    main()
