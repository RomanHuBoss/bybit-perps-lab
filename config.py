from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
INSTANCE_DIR = BASE_DIR / 'instance'
DATA_DIR = BASE_DIR / 'data'
DB_PATH = INSTANCE_DIR / 'trading.db'

APP_CONFIG = {
    'SECRET_KEY': 'local-dev-only',
    'JSON_SORT_KEYS': False,
    'UPLOAD_FOLDER': str(DATA_DIR),
    'MAX_CONTENT_LENGTH': 25 * 1024 * 1024,
}

DEFAULT_SETTINGS = {
    'starting_equity': 10000.0,
    'risk_per_trade': 0.004,
    'daily_loss_limit': 0.02,
    'max_concurrent_positions': 3,
    'max_positions_per_symbol': 1,
    'max_same_side_positions': 2,
    'entry_fee_rate': 0.0002,
    'exit_fee_rate_take_profit': 0.0002,
    'exit_fee_rate_stop': 0.00055,
    'entry_slippage_bps': 1.0,
    'exit_slippage_bps_tp': 1.0,
    'exit_slippage_bps_stop': 4.0,
    'max_leverage': 3.0,
    'funding_rate_cap_abs': 0.0005,
    'trend_enabled': True,
    'reversion_enabled': True,
    'regime_filter_enabled': True,
    'volatility_filter_enabled': True,
    'no_trade_filter_enabled': True,
    'trend_strength_min': 0.0025,
    'reversion_zscore_threshold': 2.1,
    'chop_trend_strength_max': 0.0015,
    'chop_abs_dev_zscore_max': 1.0,
    'reversion_volatility_min': 0.35,
    'volatility_score_min': 0.18,
    'volatility_score_max': 0.92,
    'trend_tp1_r': 1.0,
    'trend_tp2_r': 2.2,
    'reversion_tp1_r': 0.8,
    'reversion_tp2_r': 1.5,
    'trend_max_hold_bars': 96,
    'reversion_max_hold_bars': 36,
    'max_daily_stopouts': 3,
    'cooldown_bars_after_stop': 12,
    'max_consecutive_losses': 4,
    'volume_multiplier': 1.2,
    'atr_stop_mult_trend': 1.4,
    'atr_stop_mult_reversion': 1.0,
    'walkforward_train_bars': 2016,
    'walkforward_test_bars': 576,
    'walkforward_step_bars': 576,
    'walkforward_min_trades_train': 6,
    'walkforward_candidate_limit': 8,
    'walkforward_max_segments': 10,
    'paper_poll_seconds': 2.0,
    'paper_auto_steps': 1,
    'optimizer_trials': 24,
    'optimizer_train_bars': 2016,
    'optimizer_test_bars': 576,
    'optimizer_step_bars': 576,
    'optimizer_max_segments': 8,
    'optimizer_min_trades_test': 12,
    'optimizer_random_seed': 42,
    'demo_symbols': ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT'],
    'demo_interval': '5',
    'live_fixed_notional_usdt': 10.0,
    'live_signal_max_age_bars': 2,
    'live_entry_mode': 'signal',
}

LIVE_ADAPTER_CONFIG = {
    'enabled': os.getenv('BYBIT_LIVE_ENABLED', 'false').lower() == 'true',
    'dry_run': os.getenv('BYBIT_LIVE_DRY_RUN', 'true').lower() != 'false',
    'testnet': os.getenv('BYBIT_TESTNET', 'true').lower() != 'false',
    'api_key_present': bool(os.getenv('BYBIT_API_KEY')),
    'api_secret_present': bool(os.getenv('BYBIT_API_SECRET')),
    'recv_window': int(os.getenv('BYBIT_RECV_WINDOW', '5000')),
}
