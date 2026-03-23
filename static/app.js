const state = {
  symbols: [],
  selectedSymbols: new Set(),
  latestRunId: null,
  latestWalkforwardId: null,
  latestPaperSessionId: null,
  config: {},
  viewMode: 'idle',
};

const els = {
  loadDemoBtn: document.getElementById('loadDemoBtn'),
  syncBybitBtn: document.getElementById('syncBybitBtn'),
  refreshSymbolsBtn: document.getElementById('refreshSymbolsBtn'),
  runBacktestBtn: document.getElementById('runBacktestBtn'),
  runWalkforwardBtn: document.getElementById('runWalkforwardBtn'),
  createPaperBtn: document.getElementById('createPaperBtn'),
  loadLatestRunBtn: document.getElementById('loadLatestRunBtn'),
  loadLatestWfBtn: document.getElementById('loadLatestWfBtn'),
  exportRunBtn: document.getElementById('exportRunBtn'),
  exportWfBtn: document.getElementById('exportWfBtn'),
  exportPaperBtn: document.getElementById('exportPaperBtn'),
  paperStartBtn: document.getElementById('paperStartBtn'),
  paperStepBtn: document.getElementById('paperStepBtn'),
  paperStopBtn: document.getElementById('paperStopBtn'),
  paperRefreshBtn: document.getElementById('paperRefreshBtn'),
  demoSymbols: document.getElementById('demoSymbols'),
  demoDays: document.getElementById('demoDays'),
  syncSymbols: document.getElementById('syncSymbols'),
  syncDays: document.getElementById('syncDays'),
  paperName: document.getElementById('paperName'),
  paperSteps: document.getElementById('paperSteps'),
  symbolsList: document.getElementById('symbolsList'),
  symbolsCountBadge: document.getElementById('symbolsCountBadge'),
  runBadge: document.getElementById('runBadge'),
  logBox: document.getElementById('logBox'),
  primaryTbody: document.getElementById('primaryTbody'),
  secondaryTbody: document.getElementById('secondaryTbody'),
  configForm: document.getElementById('configForm'),
  csvForm: document.getElementById('csvForm'),
  equityCanvas: document.getElementById('equityCanvas'),
  paperStatus: document.getElementById('paperStatus'),
  paperMeta: document.getElementById('paperMeta'),
  liveStatus: document.getElementById('liveStatus'),
  liveMeta: document.getElementById('liveMeta'),
  metricStart: document.getElementById('metricStart'),
  metricEnd: document.getElementById('metricEnd'),
  metricReturn: document.getElementById('metricReturn'),
  metricDrawdown: document.getElementById('metricDrawdown'),
  metricWinRate: document.getElementById('metricWinRate'),
  metricProfitFactor: document.getElementById('metricProfitFactor'),
  metricSharpe: document.getElementById('metricSharpe'),
  metricTrades: document.getElementById('metricTrades'),
  metricAvgR: document.getElementById('metricAvgR'),
  metricExpectancy: document.getElementById('metricExpectancy'),
  metricStopRate: document.getElementById('metricStopRate'),
  metricMode: document.getElementById('metricMode'),
};

function log(message, type = 'info') {
  const div = document.createElement('div');
  div.className = `log-entry ${type}`;
  div.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
  els.logBox.prepend(div);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      ...(options.body instanceof FormData ? {} : { 'Content-Type': 'application/json' }),
      ...(options.headers || {}),
    },
    ...options,
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || 'Request failed');
  }
  return payload;
}

function parseSymbols(input) {
  return input.split(',').map((s) => s.trim().toUpperCase()).filter(Boolean);
}

function selectedSymbols() {
  return [...state.selectedSymbols];
}

function formatNum(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
  return Number(value).toLocaleString('ru-RU', { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

function fillConfigForm(config) {
  state.config = config;
  for (const [key, value] of Object.entries(config)) {
    const input = els.configForm.elements.namedItem(key);
    if (!input) continue;
    if (input.type === 'checkbox') input.checked = Boolean(value);
    else input.value = value;
  }
}

function collectConfigForm() {
  const out = {};
  const inputs = els.configForm.querySelectorAll('input');
  inputs.forEach((input) => {
    if (!input.name) return;
    if (input.type === 'checkbox') out[input.name] = input.checked;
    else if (input.type === 'number') out[input.name] = Number(input.value);
    else out[input.name] = input.value;
  });
  return out;
}

function renderSymbols() {
  els.symbolsList.innerHTML = '';
  state.symbols.forEach((symbol) => {
    const chip = document.createElement('button');
    chip.type = 'button';
    chip.className = `chip ${state.selectedSymbols.has(symbol) ? 'active' : ''}`;
    chip.textContent = symbol;
    chip.addEventListener('click', () => {
      if (state.selectedSymbols.has(symbol)) state.selectedSymbols.delete(symbol);
      else state.selectedSymbols.add(symbol);
      renderSymbols();
    });
    els.symbolsList.appendChild(chip);
  });
  els.symbolsCountBadge.textContent = `${state.symbols.length} символов / выбрано ${state.selectedSymbols.size}`;
}

function renderMetrics(summary = {}, mode = '—') {
  els.metricStart.textContent = formatNum(summary.starting_equity);
  els.metricEnd.textContent = formatNum(summary.ending_equity);
  els.metricReturn.textContent = `${formatNum(summary.total_return_pct)}%`;
  els.metricDrawdown.textContent = `${formatNum(summary.max_drawdown_pct)}%`;
  els.metricWinRate.textContent = `${formatNum(summary.win_rate)}%`;
  els.metricProfitFactor.textContent = String(summary.profit_factor ?? '—');
  els.metricSharpe.textContent = formatNum(summary.sharpe, 3);
  els.metricTrades.textContent = String(summary.trades_count ?? '—');
  els.metricAvgR.textContent = formatNum(summary.avg_r, 3);
  els.metricExpectancy.textContent = `${formatNum(summary.expectancy_pct, 3)}%`;
  els.metricStopRate.textContent = `${formatNum(summary.stop_rate, 2)}%`;
  els.metricMode.textContent = mode;
}

function drawEquityCurve(points) {
  const canvas = els.equityCanvas;
  const ctx = canvas.getContext('2d');
  const width = canvas.width;
  const height = canvas.height;
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = '#09111f';
  ctx.fillRect(0, 0, width, height);

  if (!points || points.length < 2) {
    ctx.fillStyle = '#91a0c2';
    ctx.font = '16px Arial';
    ctx.fillText('Нет данных equity curve', 32, 48);
    return;
  }

  const values = points.map((p) => Number(p.equity));
  const minVal = Math.min(...values);
  const maxVal = Math.max(...values);
  const padding = 42;
  const plotW = width - padding * 2;
  const plotH = height - padding * 2;
  const range = Math.max(maxVal - minVal, 1e-9);

  ctx.strokeStyle = 'rgba(255,255,255,0.08)';
  ctx.lineWidth = 1;
  for (let i = 0; i < 5; i += 1) {
    const y = padding + (plotH / 4) * i;
    ctx.beginPath();
    ctx.moveTo(padding, y);
    ctx.lineTo(width - padding, y);
    ctx.stroke();
  }

  ctx.strokeStyle = '#6ca4ff';
  ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((point, idx) => {
    const x = padding + (idx / (points.length - 1)) * plotW;
    const y = padding + ((maxVal - Number(point.equity)) / range) * plotH;
    if (idx === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();

  ctx.fillStyle = '#edf1f7';
  ctx.font = '12px Arial';
  ctx.fillText(formatNum(maxVal), 10, padding + 4);
  ctx.fillText(formatNum(minVal), 10, height - padding + 4);
}

function setPrimaryHeader(labels) {
  const headerCells = document.querySelectorAll('.bottom-grid .table-panel:first-child table thead tr th');
  labels.forEach((label, idx) => {
    if (headerCells[idx]) headerCells[idx].textContent = label;
  });
}

function renderBacktestTrades(trades = []) {
  setPrimaryHeader(['Symbol', 'Regime', 'Side', 'Entry', 'Exit', 'Net PnL', 'R', 'Reason']);
  els.primaryTbody.innerHTML = '';
  trades.forEach((trade) => {
    const tr = document.createElement('tr');
    const pnlClass = Number(trade.net_pnl) >= 0 ? 'pnl-pos' : 'pnl-neg';
    tr.innerHTML = `
      <td>${trade.symbol}</td>
      <td class="regime-${trade.regime}">${trade.regime}</td>
      <td class="side-${trade.side.toLowerCase()}">${trade.side}</td>
      <td>${new Date(trade.entry_ts).toLocaleString()}</td>
      <td>${new Date(trade.exit_ts).toLocaleString()}</td>
      <td class="${pnlClass}">${formatNum(trade.net_pnl, 2)}</td>
      <td>${formatNum(trade.r_multiple, 2)}</td>
      <td>${trade.exit_reason}</td>
    `;
    els.primaryTbody.appendChild(tr);
  });
}

function renderWalkforwardSegments(segments = []) {
  setPrimaryHeader(['#', 'Train', 'Test', 'Best params', 'Return', 'Max DD', 'Trades', 'Objective']);
  els.primaryTbody.innerHTML = '';
  segments.forEach((segment) => {
    const testSummary = segment.test_summary || JSON.parse(segment.metrics_json || '{}').test_summary || {};
    const bestParams = segment.best_params || JSON.parse(segment.best_params_json || '{}');
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${segment.segment_no}</td>
      <td>${new Date(segment.train_start_ts).toLocaleDateString()} → ${new Date(segment.train_end_ts).toLocaleDateString()}</td>
      <td>${new Date(segment.test_start_ts).toLocaleDateString()} → ${new Date(segment.test_end_ts).toLocaleDateString()}</td>
      <td>${JSON.stringify(bestParams)}</td>
      <td class="${Number(testSummary.total_return_pct) >= 0 ? 'pnl-pos' : 'pnl-neg'}">${formatNum(testSummary.total_return_pct, 2)}%</td>
      <td>${formatNum(testSummary.max_drawdown_pct, 2)}%</td>
      <td>${testSummary.trades_count ?? '—'}</td>
      <td>${formatNum(segment.objective_score, 3)}</td>
    `;
    els.primaryTbody.appendChild(tr);
  });
}

function renderPaperTrades(trades = []) {
  setPrimaryHeader(['Symbol', 'Regime', 'Side', 'Entry', 'Exit', 'Net PnL', 'R', 'Reason']);
  els.primaryTbody.innerHTML = '';
  trades.forEach((trade) => {
    const payload = trade.raw_json ? JSON.parse(trade.raw_json) : trade;
    const tr = document.createElement('tr');
    const pnlClass = Number(payload.net_pnl) >= 0 ? 'pnl-pos' : 'pnl-neg';
    tr.innerHTML = `
      <td>${trade.symbol}</td>
      <td class="regime-${trade.regime}">${trade.regime}</td>
      <td class="side-${trade.side.toLowerCase()}">${trade.side}</td>
      <td>${new Date(trade.entry_ts).toLocaleString()}</td>
      <td>${new Date(trade.exit_ts).toLocaleString()}</td>
      <td class="${pnlClass}">${formatNum(trade.net_pnl, 2)}</td>
      <td>${formatNum(trade.r_multiple, 2)}</td>
      <td>${trade.exit_reason}</td>
    `;
    els.primaryTbody.appendChild(tr);
  });
}

function renderSecondaryRows(rows = [], mode = 'events') {
  els.secondaryTbody.innerHTML = '';
  rows.forEach((row) => {
    const tr = document.createElement('tr');
    if (mode === 'events') {
      tr.innerHTML = `
        <td>${new Date(row.created_at).toLocaleString()}</td>
        <td>${row.level}</td>
        <td>${row.message}</td>
      `;
    } else {
      tr.innerHTML = `
        <td>${row.entry_ts || row.ts || ''}</td>
        <td>${row.symbol || row.side || ''}</td>
        <td>${JSON.stringify(row)}</td>
      `;
    }
    els.secondaryTbody.appendChild(tr);
  });
}

async function refreshSymbols() {
  const payload = await api('/api/symbols');
  state.symbols = payload.symbols || [];
  if (state.selectedSymbols.size === 0) state.symbols.forEach((symbol) => state.selectedSymbols.add(symbol));
  else state.selectedSymbols = new Set([...state.selectedSymbols].filter((s) => state.symbols.includes(s)));
  renderSymbols();
}

async function loadConfig() {
  fillConfigForm(await api('/api/config'));
}

async function loadLiveStatus() {
  const status = await api('/api/live-adapter/status');
  els.liveStatus.textContent = status.enabled ? (status.dry_run ? 'dry-run' : 'live') : 'disabled';
  els.liveMeta.textContent = `${status.testnet ? 'testnet' : 'mainnet'} / key=${status.api_key_present ? 'yes' : 'no'} / secret=${status.api_secret_present ? 'yes' : 'no'}`;
}

async function loadLatestRun() {
  const data = await api('/api/runs?limit=1');
  const run = data.runs?.[0];
  if (!run) {
    log('Backtest-запусков пока нет', 'warn');
    return;
  }
  state.latestRunId = run.id;
  const details = await api(`/api/runs/${run.id}`);
  let extra = {};
  if (run.notes) {
    try { extra = JSON.parse(run.notes); } catch { extra = {}; }
  }
  renderMetrics({
    starting_equity: run.starting_equity,
    ending_equity: run.ending_equity,
    total_return_pct: run.total_return_pct,
    max_drawdown_pct: run.max_drawdown_pct,
    win_rate: run.win_rate,
    profit_factor: run.profit_factor,
    sharpe: run.sharpe,
    trades_count: details.trades.length,
    avg_r: extra.avg_r,
    expectancy_pct: extra.expectancy_pct,
    stop_rate: extra.stop_rate,
  }, 'backtest');
  drawEquityCurve(details.equity);
  renderBacktestTrades(details.trades);
  renderSecondaryRows(details.signals.map((s) => ({ created_at: s.ts, level: s.side, message: `${s.symbol} ${s.regime} ${formatNum(s.score, 3)} ${s.notes || ''}` })), 'events');
  els.runBadge.textContent = `Backtest #${run.id}`;
  state.viewMode = 'backtest';
}

async function loadLatestWalkforward() {
  const data = await api('/api/walkforward-runs?limit=1');
  const run = data.runs?.[0];
  if (!run) {
    log('Walk-forward запусков пока нет', 'warn');
    return;
  }
  state.latestWalkforwardId = run.id;
  const details = await api(`/api/walkforward-runs/${run.id}`);
  let extra = {};
  if (run.notes) {
    try { extra = JSON.parse(run.notes); } catch { extra = {}; }
  }
  renderMetrics({
    starting_equity: run.starting_equity,
    ending_equity: run.ending_equity,
    total_return_pct: run.total_return_pct,
    max_drawdown_pct: run.max_drawdown_pct,
    win_rate: run.win_rate,
    profit_factor: run.profit_factor,
    sharpe: run.sharpe,
    trades_count: extra.segments_count,
    avg_r: extra.avg_r,
    expectancy_pct: extra.expectancy_pct,
    stop_rate: extra.stop_rate,
  }, 'walk-forward');
  renderWalkforwardSegments(details.segments.map((seg) => ({
    ...seg,
    best_params: JSON.parse(seg.best_params_json),
    ...JSON.parse(seg.metrics_json),
  })));
  const pseudoEquity = details.segments.map((seg, idx) => {
    const payload = JSON.parse(seg.metrics_json);
    return { ts: seg.test_end_ts, equity: payload.test_summary.ending_equity || idx };
  });
  if (pseudoEquity.length > 1) drawEquityCurve(pseudoEquity);
  els.runBadge.textContent = `Walk-forward #${run.id}`;
  renderSecondaryRows(details.segments.map((seg) => ({ created_at: seg.test_end_ts, level: `segment ${seg.segment_no}`, message: seg.best_params_json })), 'events');
  state.viewMode = 'walk-forward';
}

async function loadPaperSession(sessionId = state.latestPaperSessionId) {
  if (!sessionId) {
    const sessions = await api('/api/paper-sessions?limit=1');
    const latest = sessions.sessions?.[0];
    if (!latest) {
      log('Paper-сессий пока нет', 'warn');
      return;
    }
    sessionId = latest.id;
  }
  state.latestPaperSessionId = sessionId;
  const data = await api(`/api/paper-sessions/${sessionId}`);
  const session = data.session;
  const stateJson = session.state || {};
  els.paperStatus.textContent = `${session.status} / #${session.id}`;
  els.paperMeta.textContent = `index=${session.current_index} / equity=${formatNum(session.current_equity)} / open=${(stateJson.open_positions || []).length}`;
  renderMetrics({
    starting_equity: session.starting_equity,
    ending_equity: session.current_equity,
    total_return_pct: ((session.current_equity / session.starting_equity) - 1) * 100,
    max_drawdown_pct: 0,
    win_rate: 0,
    profit_factor: 0,
    sharpe: 0,
    trades_count: stateJson.trades_count || data.trades.length,
    avg_r: 0,
    expectancy_pct: 0,
    stop_rate: 0,
  }, 'paper');
  drawEquityCurve(data.equity);
  renderPaperTrades(data.trades);
  renderSecondaryRows(data.events, 'events');
  els.runBadge.textContent = `Paper #${session.id}`;
  state.viewMode = 'paper';
}

els.loadDemoBtn.addEventListener('click', async () => {
  try {
    const result = await api('/api/load-demo-data', { method: 'POST', body: JSON.stringify({ symbols: parseSymbols(els.demoSymbols.value), days: Number(els.demoDays.value), interval: '5' }) });
    log(`Demo-данные: ${result.candles_upserted} свечей, funding ${result.funding_upserted}`, 'success');
    await refreshSymbols();
  } catch (error) { log(error.message, 'error'); }
});

els.syncBybitBtn.addEventListener('click', async () => {
  try {
    const result = await api('/api/sync-bybit-public', { method: 'POST', body: JSON.stringify({ symbols: parseSymbols(els.syncSymbols.value), days: Number(els.syncDays.value), interval: '5' }) });
    log(`Bybit sync: ${result.candles_upserted} свечей, funding ${result.funding_upserted}`, 'success');
    await refreshSymbols();
  } catch (error) { log(error.message, 'error'); }
});

els.refreshSymbolsBtn.addEventListener('click', async () => {
  try { await refreshSymbols(); log('Universe обновлён', 'success'); } catch (error) { log(error.message, 'error'); }
});

els.runBacktestBtn.addEventListener('click', async () => {
  try {
    const result = await api('/api/run-backtest', { method: 'POST', body: JSON.stringify({ symbols: selectedSymbols(), overrides: collectConfigForm() }) });
    state.latestRunId = result.run_id;
    renderMetrics(result.summary, 'backtest');
    drawEquityCurve(result.equity_curve);
    renderBacktestTrades(result.trades);
    renderSecondaryRows(result.signals.slice(-100).reverse().map((s) => ({ created_at: s.ts, level: s.side, message: `${s.symbol} ${s.regime} ${formatNum(s.score, 3)} ${s.notes}` })), 'events');
    els.runBadge.textContent = `Backtest #${result.run_id}`;
    state.viewMode = 'backtest';
    log(`Backtest завершён: run #${result.run_id}, trades=${result.summary.trades_count}, signals=${result.signals_count}`, 'success');
  } catch (error) { log(error.message, 'error'); }
});

els.runWalkforwardBtn.addEventListener('click', async () => {
  try {
    const result = await api('/api/run-walkforward', { method: 'POST', body: JSON.stringify({ symbols: selectedSymbols(), overrides: collectConfigForm() }) });
    state.latestWalkforwardId = result.walkforward_run_id;
    renderMetrics(result.summary, 'walk-forward');
    drawEquityCurve(result.equity_curve);
    renderWalkforwardSegments(result.segments);
    renderSecondaryRows(result.segments.map((seg) => ({ created_at: seg.test_end_ts, level: `segment ${seg.segment_no}`, message: JSON.stringify(seg.best_params) })), 'events');
    els.runBadge.textContent = `Walk-forward #${result.walkforward_run_id}`;
    state.viewMode = 'walk-forward';
    log(`Walk-forward завершён: run #${result.walkforward_run_id}, segments=${result.segments.length}`, 'success');
  } catch (error) { log(error.message, 'error'); }
});

els.createPaperBtn.addEventListener('click', async () => {
  try {
    const result = await api('/api/paper-sessions', {
      method: 'POST',
      body: JSON.stringify({
        name: els.paperName.value.trim() || 'paper-demo',
        symbols: selectedSymbols(),
        overrides: collectConfigForm(),
        auto_steps: Number(els.paperSteps.value),
      }),
    });
    state.latestPaperSessionId = result.session.id;
    await loadPaperSession(result.session.id);
    log(`Создана paper-сессия #${result.session.id}`, 'success');
  } catch (error) { log(error.message, 'error'); }
});

els.paperStepBtn.addEventListener('click', async () => {
  try {
    if (!state.latestPaperSessionId) throw new Error('Сначала создай paper-сессию');
    await api(`/api/paper-sessions/${state.latestPaperSessionId}/step`, { method: 'POST', body: JSON.stringify({ steps: Number(els.paperSteps.value) || 1 }) });
    await loadPaperSession(state.latestPaperSessionId);
    log('Paper-сессия продвинута', 'success');
  } catch (error) { log(error.message, 'error'); }
});

els.paperStartBtn.addEventListener('click', async () => {
  try {
    if (!state.latestPaperSessionId) throw new Error('Сначала создай paper-сессию');
    await api(`/api/paper-sessions/${state.latestPaperSessionId}/start`, { method: 'POST' });
    await loadPaperSession(state.latestPaperSessionId);
    log('Background replay запущен', 'success');
  } catch (error) { log(error.message, 'error'); }
});

els.paperStopBtn.addEventListener('click', async () => {
  try {
    if (!state.latestPaperSessionId) throw new Error('Нет paper-сессии');
    await api(`/api/paper-sessions/${state.latestPaperSessionId}/stop`, { method: 'POST' });
    await loadPaperSession(state.latestPaperSessionId);
    log('Background replay остановлен', 'warn');
  } catch (error) { log(error.message, 'error'); }
});

els.paperRefreshBtn.addEventListener('click', async () => {
  try { await loadPaperSession(); } catch (error) { log(error.message, 'error'); }
});

els.loadLatestRunBtn.addEventListener('click', async () => {
  try { await loadLatestRun(); } catch (error) { log(error.message, 'error'); }
});

els.loadLatestWfBtn.addEventListener('click', async () => {
  try { await loadLatestWalkforward(); } catch (error) { log(error.message, 'error'); }
});

els.exportRunBtn.addEventListener('click', () => {
  if (!state.latestRunId) return log('Нет backtest run для экспорта', 'warn');
  window.open(`/api/runs/${state.latestRunId}/export/trades.csv`, '_blank');
});

els.exportWfBtn.addEventListener('click', () => {
  if (!state.latestWalkforwardId) return log('Нет walk-forward run для экспорта', 'warn');
  window.open(`/api/walkforward-runs/${state.latestWalkforwardId}/export/segments.csv`, '_blank');
});

els.exportPaperBtn.addEventListener('click', () => {
  if (!state.latestPaperSessionId) return log('Нет paper session для экспорта', 'warn');
  window.open(`/api/paper-sessions/${state.latestPaperSessionId}/export/trades.csv`, '_blank');
});

els.configForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  try {
    const result = await api('/api/config', { method: 'POST', body: JSON.stringify(collectConfigForm()) });
    fillConfigForm(result);
    log('Конфигурация сохранена', 'success');
  } catch (error) { log(error.message, 'error'); }
});

els.csvForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  try {
    const result = await api('/api/import-csv', { method: 'POST', body: new FormData(els.csvForm) });
    log(`CSV импортирован: ${result.rows_inserted} свечей для ${result.symbol}`, 'success');
    await refreshSymbols();
  } catch (error) { log(error.message, 'error'); }
});

(async function init() {
  try {
    await loadConfig();
    await refreshSymbols();
    await loadLiveStatus();
    await loadLatestRun();
  } catch (error) {
    log(error.message, 'error');
  }
})();
