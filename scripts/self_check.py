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
from services.paper_engine import paper_manager
from services.walkforward_engine import WalkForwardEngine


def main() -> None:
    ensure_database()
    print(generate_demo_market_data(['BTCUSDT', 'ETHUSDT', 'SOLUSDT'], days=12))
    settings = get_settings()

    candles = load_candles(['BTCUSDT'], '5')
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

    print('ALL CHECKS PASSED')


if __name__ == '__main__':
    main()
