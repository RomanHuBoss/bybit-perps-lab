from __future__ import annotations

import csv
import io
import json
from pathlib import Path

from flask import Flask, Response, jsonify, render_template, request

from config import APP_CONFIG, DEFAULT_SETTINGS
from database import ensure_database, get_settings, update_settings, list_live_orders
from services.backtest_engine import BacktestEngine, get_run_details, list_runs
from services.data_service import (
    DataValidationError,
    generate_demo_market_data,
    import_csv_file,
    list_symbols,
    sync_bybit_public,
)
from services.live_adapter import live_adapter
from services.paper_engine import paper_manager
from services.trade_service import TradePlanner
from services.walkforward_engine import WalkForwardEngine, get_walkforward_details, list_walkforward_runs


def create_app() -> Flask:
    app = Flask(__name__)
    app.config.update(APP_CONFIG)
    ensure_database()

    @app.get('/')
    def index():
        return render_template('index.html')

    @app.get('/api/health')
    def health():
        return jsonify({'status': 'ok'})

    @app.get('/api/config')
    def api_get_config():
        return jsonify(get_settings())

    @app.post('/api/config')
    def api_set_config():
        payload = request.get_json(force=True, silent=False) or {}
        merged = get_settings()
        for key, default in DEFAULT_SETTINGS.items():
            if key not in payload:
                continue
            value = payload[key]
            if isinstance(default, bool):
                merged[key] = bool(value)
            elif isinstance(default, int) and not isinstance(default, bool):
                merged[key] = int(value)
            elif isinstance(default, float):
                merged[key] = float(value)
            else:
                merged[key] = value
        saved = update_settings(merged)
        return jsonify(saved)

    @app.get('/api/symbols')
    def api_symbols():
        return jsonify({'symbols': list_symbols(interval=request.args.get('interval', '5'))})

    @app.post('/api/load-demo-data')
    def api_load_demo_data():
        payload = request.get_json(force=True, silent=True) or {}
        symbols = payload.get('symbols') or get_settings().get('demo_symbols', DEFAULT_SETTINGS['demo_symbols'])
        days = int(payload.get('days', 20))
        interval = str(payload.get('interval', '5'))
        result = generate_demo_market_data(symbols=symbols, interval=interval, days=days)
        return jsonify(result)

    @app.post('/api/sync-bybit-public')
    def api_sync_bybit_public():
        payload = request.get_json(force=True, silent=True) or {}
        symbols = payload.get('symbols') or ['BTCUSDT', 'ETHUSDT']
        interval = str(payload.get('interval', '5'))
        days = int(payload.get('days', 14))
        result = sync_bybit_public(symbols=symbols, interval=interval, days=days)
        return jsonify(result)

    @app.post('/api/import-csv')
    def api_import_csv():
        if 'file' not in request.files:
            raise DataValidationError('Missing file field in multipart form data.')
        file_storage = request.files['file']
        symbol = request.form.get('symbol', '').upper().strip()
        interval = request.form.get('interval', '5').strip()
        if not symbol:
            raise DataValidationError('symbol is required for CSV import.')
        result = import_csv_file(file_storage, symbol=symbol, interval=interval)
        return jsonify(result)

    @app.post('/api/run-backtest')
    def api_run_backtest():
        payload = request.get_json(force=True, silent=True) or {}
        symbols = payload.get('symbols') or list_symbols('5')
        settings = get_settings()
        settings.update(payload.get('overrides', {}))
        engine = BacktestEngine(settings)
        return jsonify(engine.run(symbols=symbols))

    @app.get('/api/runs')
    def api_runs():
        limit = int(request.args.get('limit', 20))
        return jsonify({'runs': list_runs(limit=limit)})

    @app.get('/api/runs/<int:run_id>')
    def api_run_details(run_id: int):
        return jsonify(get_run_details(run_id))

    @app.get('/api/runs/<int:run_id>/export/trades.csv')
    def api_export_run_trades(run_id: int):
        details = get_run_details(run_id)
        return _csv_response(f'run_{run_id}_trades.csv', details['trades'])

    @app.post('/api/run-walkforward')
    def api_run_walkforward():
        payload = request.get_json(force=True, silent=True) or {}
        symbols = payload.get('symbols') or list_symbols('5')
        settings = get_settings()
        settings.update(payload.get('overrides', {}))
        engine = WalkForwardEngine(settings)
        return jsonify(engine.run(symbols=symbols))

    @app.get('/api/walkforward-runs')
    def api_walkforward_runs():
        limit = int(request.args.get('limit', 20))
        return jsonify({'runs': list_walkforward_runs(limit=limit)})

    @app.get('/api/walkforward-runs/<int:run_id>')
    def api_walkforward_details(run_id: int):
        return jsonify(get_walkforward_details(run_id))

    @app.get('/api/walkforward-runs/<int:run_id>/export/segments.csv')
    def api_export_walkforward_segments(run_id: int):
        details = get_walkforward_details(run_id)
        rows = []
        for seg in details['segments']:
            row = dict(seg)
            if row.get('best_params_json'):
                row['best_params'] = json.loads(row['best_params_json'])
            if row.get('metrics_json'):
                row['metrics'] = json.loads(row['metrics_json'])
            rows.append(row)
        return _csv_response(f'walkforward_{run_id}_segments.csv', rows)

    @app.post('/api/paper-sessions')
    def api_create_paper_session():
        payload = request.get_json(force=True, silent=True) or {}
        symbols = payload.get('symbols') or list_symbols('5')
        settings = get_settings()
        settings.update(payload.get('overrides', {}))
        name = payload.get('name') or f'paper-{Path(".").resolve().name}'
        poll_seconds = payload.get('poll_seconds')
        auto_steps = payload.get('auto_steps')
        return jsonify(paper_manager.create_session(name=name, symbols=symbols, settings=settings, poll_seconds=poll_seconds, auto_steps=auto_steps))

    @app.get('/api/paper-sessions')
    def api_list_paper_sessions():
        limit = int(request.args.get('limit', 20))
        return jsonify({'sessions': paper_manager.list_sessions(limit=limit)})

    @app.get('/api/paper-sessions/<int:session_id>')
    def api_paper_session_details(session_id: int):
        return jsonify(paper_manager.get_session(session_id))

    @app.post('/api/paper-sessions/<int:session_id>/step')
    def api_paper_session_step(session_id: int):
        payload = request.get_json(force=True, silent=True) or {}
        steps = int(payload.get('steps', 1))
        return jsonify(paper_manager.step_session(session_id, steps=steps))

    @app.post('/api/paper-sessions/<int:session_id>/start')
    def api_paper_session_start(session_id: int):
        return jsonify(paper_manager.start_background(session_id))

    @app.post('/api/paper-sessions/<int:session_id>/stop')
    def api_paper_session_stop(session_id: int):
        return jsonify(paper_manager.stop_background(session_id))

    @app.get('/api/paper-sessions/<int:session_id>/export/trades.csv')
    def api_export_paper_trades(session_id: int):
        session = paper_manager.get_session(session_id)
        return _csv_response(f'paper_session_{session_id}_trades.csv', session['trades'])

    @app.get('/api/live-adapter/status')
    def api_live_adapter_status():
        return jsonify(live_adapter.status())

    @app.get('/api/live-adapter/wallet')
    def api_live_adapter_wallet():
        coin = request.args.get('coin', 'USDT')
        return jsonify(live_adapter.get_wallet_balance(coin=coin))

    @app.get('/api/live-adapter/positions')
    def api_live_adapter_positions():
        symbol = request.args.get('symbol')
        return jsonify(live_adapter.get_positions(symbol=symbol))

    @app.get('/api/signals/latest')
    def api_latest_signals():
        symbols_raw = request.args.get('symbols', '')
        symbols = [s.strip().upper() for s in symbols_raw.split(',') if s.strip()] or list_symbols('5')
        settings = get_settings()
        planner = TradePlanner(settings)
        return jsonify({'signals': planner.get_latest_signals(symbols)})

    @app.get('/api/tiny-live/candidates')
    def api_tiny_live_candidates():
        symbols_raw = request.args.get('symbols', '')
        symbols = [s.strip().upper() for s in symbols_raw.split(',') if s.strip()] or list_symbols('5')
        fixed_notional_usdt = float(request.args.get('fixed_notional_usdt', get_settings().get('live_fixed_notional_usdt', 10.0)))
        require_fresh_signal = request.args.get('require_fresh_signal', 'true').lower() != 'false'
        settings = get_settings()
        planner = TradePlanner(settings)
        rows = []
        for symbol in symbols:
            try:
                plan = planner.build_trade_plan(symbol=symbol, fixed_notional_usdt=fixed_notional_usdt, require_fresh_signal=require_fresh_signal)
                rows.append({'symbol': symbol, 'ok': True, 'plan': plan})
            except Exception as exc:
                rows.append({'symbol': symbol, 'ok': False, 'error': str(exc)})
        return jsonify({'candidates': rows})

    @app.post('/api/tiny-live/plan')
    def api_tiny_live_plan():
        payload = request.get_json(force=True, silent=True) or {}
        symbol = str(payload.get('symbol', '')).upper().strip()
        if not symbol:
            raise ValueError('symbol is required.')
        settings = get_settings()
        planner = TradePlanner(settings)
        fixed_notional_usdt = payload.get('fixed_notional_usdt', settings.get('live_fixed_notional_usdt', 10.0))
        require_fresh_signal = bool(payload.get('require_fresh_signal', True))
        return jsonify(planner.build_trade_plan(symbol=symbol, fixed_notional_usdt=fixed_notional_usdt, require_fresh_signal=require_fresh_signal))

    @app.post('/api/tiny-live/execute')
    def api_tiny_live_execute():
        payload = request.get_json(force=True, silent=True) or {}
        symbol = str(payload.get('symbol', '')).upper().strip()
        if not symbol:
            raise ValueError('symbol is required.')
        settings = get_settings()
        planner = TradePlanner(settings)
        fixed_notional_usdt = payload.get('fixed_notional_usdt', settings.get('live_fixed_notional_usdt', 10.0))
        require_fresh_signal = bool(payload.get('require_fresh_signal', True))
        plan = planner.build_trade_plan(symbol=symbol, fixed_notional_usdt=fixed_notional_usdt, require_fresh_signal=require_fresh_signal)
        if not plan.get('affordable_for_requested_size', False):
            raise ValueError(plan['affordability_note'])
        return jsonify(planner.execute_trade_plan(plan))

    @app.get('/api/tiny-live/logs')
    def api_tiny_live_logs():
        limit = int(request.args.get('limit', 50))
        return jsonify({'logs': list_live_orders(limit=limit)})

    @app.errorhandler(DataValidationError)
    @app.errorhandler(ValueError)
    def handle_value_error(exc):
        return jsonify({'error': str(exc)}), 400

    @app.errorhandler(Exception)
    def handle_exception(exc):
        return jsonify({'error': str(exc)}), 500

    return app


def _csv_response(filename: str, rows: list[dict]) -> Response:
    buffer = io.StringIO()
    if not rows:
        buffer.write('empty\n')
    else:
        normalized = []
        for row in rows:
            norm = {}
            for key, value in dict(row).items():
                if isinstance(value, (dict, list)):
                    norm[key] = json.dumps(value, ensure_ascii=False)
                else:
                    norm[key] = value
            normalized.append(norm)
        writer = csv.DictWriter(buffer, fieldnames=list(normalized[0].keys()))
        writer.writeheader()
        writer.writerows(normalized)
    return Response(
        buffer.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={filename}'},
    )


app = create_app()


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8010, debug=True)
