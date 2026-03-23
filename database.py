from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from config import DB_PATH, DEFAULT_SETTINGS, INSTANCE_DIR


SCHEMA_SQL = '''
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS candles (
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    ts TEXT NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    source TEXT NOT NULL,
    PRIMARY KEY (symbol, interval, ts)
);

CREATE TABLE IF NOT EXISTS funding_rates (
    symbol TEXT NOT NULL,
    ts TEXT NOT NULL,
    funding_rate REAL NOT NULL,
    source TEXT NOT NULL,
    PRIMARY KEY (symbol, ts)
);

CREATE TABLE IF NOT EXISTS backtest_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    config_json TEXT NOT NULL,
    symbols_json TEXT NOT NULL,
    status TEXT NOT NULL,
    starting_equity REAL NOT NULL,
    ending_equity REAL,
    total_return_pct REAL,
    max_drawdown_pct REAL,
    win_rate REAL,
    profit_factor REAL,
    sharpe REAL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    regime TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_ts TEXT NOT NULL,
    exit_ts TEXT NOT NULL,
    entry_price REAL NOT NULL,
    exit_price REAL NOT NULL,
    stop_price REAL NOT NULL,
    tp1_price REAL NOT NULL,
    tp2_price REAL NOT NULL,
    qty REAL NOT NULL,
    fees REAL NOT NULL,
    funding_pnl REAL NOT NULL,
    gross_pnl REAL NOT NULL,
    net_pnl REAL NOT NULL,
    r_multiple REAL NOT NULL,
    exit_reason TEXT NOT NULL,
    bars_held INTEGER NOT NULL,
    score REAL NOT NULL DEFAULT 0,
    FOREIGN KEY (run_id) REFERENCES backtest_runs(id)
);

CREATE TABLE IF NOT EXISTS equity_curve (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    ts TEXT NOT NULL,
    equity REAL NOT NULL,
    FOREIGN KEY (run_id) REFERENCES backtest_runs(id)
);

CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    ts TEXT NOT NULL,
    regime TEXT NOT NULL,
    side TEXT NOT NULL,
    score REAL NOT NULL,
    notes TEXT,
    FOREIGN KEY (run_id) REFERENCES backtest_runs(id)
);

CREATE TABLE IF NOT EXISTS walkforward_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    config_json TEXT NOT NULL,
    symbols_json TEXT NOT NULL,
    status TEXT NOT NULL,
    train_bars INTEGER NOT NULL,
    test_bars INTEGER NOT NULL,
    step_bars INTEGER NOT NULL,
    starting_equity REAL NOT NULL,
    ending_equity REAL,
    total_return_pct REAL,
    max_drawdown_pct REAL,
    win_rate REAL,
    profit_factor REAL,
    sharpe REAL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS walkforward_segments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    walkforward_run_id INTEGER NOT NULL,
    segment_no INTEGER NOT NULL,
    train_start_ts TEXT NOT NULL,
    train_end_ts TEXT NOT NULL,
    test_start_ts TEXT NOT NULL,
    test_end_ts TEXT NOT NULL,
    best_params_json TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    objective_score REAL NOT NULL,
    FOREIGN KEY (walkforward_run_id) REFERENCES walkforward_runs(id)
);

CREATE TABLE IF NOT EXISTS paper_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    symbols_json TEXT NOT NULL,
    config_json TEXT NOT NULL,
    current_index INTEGER NOT NULL DEFAULT 0,
    current_ts TEXT,
    starting_equity REAL NOT NULL,
    current_equity REAL NOT NULL,
    poll_seconds REAL NOT NULL,
    auto_steps INTEGER NOT NULL DEFAULT 1,
    state_json TEXT NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS paper_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    paper_session_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    regime TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_ts TEXT NOT NULL,
    exit_ts TEXT NOT NULL,
    entry_price REAL NOT NULL,
    exit_price REAL NOT NULL,
    net_pnl REAL NOT NULL,
    r_multiple REAL NOT NULL,
    exit_reason TEXT NOT NULL,
    bars_held INTEGER NOT NULL,
    score REAL NOT NULL DEFAULT 0,
    raw_json TEXT NOT NULL,
    FOREIGN KEY (paper_session_id) REFERENCES paper_sessions(id)
);

CREATE TABLE IF NOT EXISTS paper_equity_curve (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    paper_session_id INTEGER NOT NULL,
    ts TEXT NOT NULL,
    equity REAL NOT NULL,
    FOREIGN KEY (paper_session_id) REFERENCES paper_sessions(id)
);

CREATE TABLE IF NOT EXISTS paper_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    paper_session_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    FOREIGN KEY (paper_session_id) REFERENCES paper_sessions(id)
);

CREATE TABLE IF NOT EXISTS live_order_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    symbol TEXT NOT NULL,
    mode TEXT NOT NULL,
    side TEXT NOT NULL,
    notional_usdt REAL NOT NULL,
    qty TEXT NOT NULL,
    entry_price REAL NOT NULL,
    stop_price REAL NOT NULL,
    take_profit_price REAL NOT NULL,
    signal_ts TEXT,
    signal_score REAL,
    plan_json TEXT NOT NULL,
    response_json TEXT NOT NULL
);


CREATE INDEX IF NOT EXISTS idx_candles_interval_symbol_ts ON candles(interval, symbol, ts);
CREATE INDEX IF NOT EXISTS idx_funding_symbol_ts ON funding_rates(symbol, ts);
CREATE INDEX IF NOT EXISTS idx_trades_run_id ON trades(run_id, entry_ts);
CREATE INDEX IF NOT EXISTS idx_equity_curve_run_id_ts ON equity_curve(run_id, ts);
CREATE INDEX IF NOT EXISTS idx_signals_run_id_ts ON signals(run_id, ts);
CREATE INDEX IF NOT EXISTS idx_paper_trades_session_ts ON paper_trades(paper_session_id, entry_ts);
CREATE INDEX IF NOT EXISTS idx_paper_equity_session_ts ON paper_equity_curve(paper_session_id, ts);
CREATE INDEX IF NOT EXISTS idx_paper_events_session_id ON paper_events(paper_session_id, id);
'''


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def dict_factory(cursor: sqlite3.Cursor, row: Iterable[Any]) -> dict[str, Any]:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def ensure_database() -> None:
    INSTANCE_DIR.mkdir(parents=True, exist_ok=True)
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(SCHEMA_SQL)
        now = utc_now_iso()
        for key, value in DEFAULT_SETTINGS.items():
            conn.execute(
                '''
                INSERT INTO settings(key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO NOTHING
                ''',
                (key, json.dumps(value), now),
            )
        conn.commit()


@contextmanager
def get_conn(row_factory: bool = False):
    ensure_database()
    conn = sqlite3.connect(DB_PATH)
    if row_factory:
        conn.row_factory = dict_factory
    try:
        yield conn
    finally:
        conn.close()


def get_settings() -> dict[str, Any]:
    with get_conn(row_factory=True) as conn:
        rows = conn.execute('SELECT key, value FROM settings').fetchall()
    result: dict[str, Any] = {}
    for row in rows:
        result[row['key']] = json.loads(row['value'])
    return result


def update_settings(new_settings: dict[str, Any]) -> dict[str, Any]:
    now = utc_now_iso()
    with get_conn() as conn:
        for key, value in new_settings.items():
            conn.execute(
                '''
                INSERT INTO settings(key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                ''',
                (key, json.dumps(value), now),
            )
        conn.commit()
    return get_settings()


def log_live_order(plan: dict[str, Any], response_payload: dict[str, Any], mode: str) -> int:
    signal = plan.get('signal') or {}
    with get_conn() as conn:
        cursor = conn.execute(
            '''
            INSERT INTO live_order_logs(
                created_at, symbol, mode, side, notional_usdt, qty, entry_price, stop_price,
                take_profit_price, signal_ts, signal_score, plan_json, response_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                utc_now_iso(),
                str(plan.get('symbol', '')),
                mode,
                str(plan.get('side', '')),
                float(plan.get('requested_notional_usdt', 0.0)),
                str(plan.get('qty', '0')),
                float(plan.get('entry_price', 0.0)),
                float(plan.get('stop_price', 0.0)),
                float(plan.get('take_profit_price', 0.0)),
                signal.get('ts'),
                float(signal.get('score', 0.0)) if signal.get('score') is not None else None,
                json.dumps(plan, ensure_ascii=False),
                json.dumps(response_payload, ensure_ascii=False),
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)


def list_live_orders(limit: int = 50) -> list[dict[str, Any]]:
    with get_conn(row_factory=True) as conn:
        rows = conn.execute('SELECT * FROM live_order_logs ORDER BY id DESC LIMIT ?', (limit,)).fetchall()
    return rows
