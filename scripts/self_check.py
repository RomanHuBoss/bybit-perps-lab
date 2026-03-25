from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import app
from database import ensure_database, get_settings
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
