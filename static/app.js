const state = {
  symbols: [],
  symbolCatalog: [],
  symbolCatalogMeta: {},
  selectedSymbols: new Set(),
  latestRunId: null,
  latestWalkforwardId: null,
  latestOptimizerId: null,
  latestOptimizerBestParams: null,
  latestPaperSessionId: null,
  latestResearchId: null,
  latestResearchManifest: null,
  researchPresets: [],
  config: {},
  viewMode: 'idle',
  pickers: {},
  activeTab: localStorage.getItem('bybit-v4-active-tab') || 'research',
};

const els = {
  tabButtons: Array.from(document.querySelectorAll('[data-tab-target]')),
  tabPanels: Array.from(document.querySelectorAll('[data-tab-panel]')),
  workspaceTitle: document.getElementById('workspaceTitle'),
  workspaceSubtitle: document.getElementById('workspaceSubtitle'),
  loadDemoBtn: document.getElementById('loadDemoBtn'),
  syncBybitBtn: document.getElementById('syncBybitBtn'),
  refreshSymbolsBtn: document.getElementById('refreshSymbolsBtn'),
  runBacktestBtn: document.getElementById('runBacktestBtn'),
  runWalkforwardBtn: document.getElementById('runWalkforwardBtn'),
  runOptimizerBtn: document.getElementById('runOptimizerBtn'),
  applyBestOptimizerBtn: document.getElementById('applyBestOptimizerBtn'),
  createPaperBtn: document.getElementById('createPaperBtn'),
  loadLatestRunBtn: document.getElementById('loadLatestRunBtn'),
  loadLatestWfBtn: document.getElementById('loadLatestWfBtn'),
  loadLatestOptimizerBtn: document.getElementById('loadLatestOptimizerBtn'),
  exportRunBtn: document.getElementById('exportRunBtn'),
  exportWfBtn: document.getElementById('exportWfBtn'),
  exportPaperBtn: document.getElementById('exportPaperBtn'),
  exportOptimizerBtn: document.getElementById('exportOptimizerBtn'),
  paperStartBtn: document.getElementById('paperStartBtn'),
  paperStepBtn: document.getElementById('paperStepBtn'),
  paperStopBtn: document.getElementById('paperStopBtn'),
  paperRefreshBtn: document.getElementById('paperRefreshBtn'),
  demoPicker: document.getElementById('demoPicker'),
  demoPickerHint: document.getElementById('demoPickerHint'),
  demoDays: document.getElementById('demoDays'),
  csvPicker: document.getElementById('csvPicker'),
  csvPickerHint: document.getElementById('csvPickerHint'),
  syncPicker: document.getElementById('syncPicker'),
  syncPickerHint: document.getElementById('syncPickerHint'),
  optimizerPicker: document.getElementById('optimizerPicker'),
  researchPicker: document.getElementById('researchPicker'),
  optimizerPickerHint: document.getElementById('optimizerPickerHint'),
  researchPickerHint: document.getElementById('researchPickerHint'),
  syncDays: document.getElementById('syncDays'),
  tinyPicker: document.getElementById('tinyPicker'),
  tinyPickerHint: document.getElementById('tinyPickerHint'),
  optimizerTrials: document.getElementById('optimizerTrials'),
  optimizerTrainBars: document.getElementById('optimizerTrainBars'),
  optimizerTestBars: document.getElementById('optimizerTestBars'),
  optimizerStepBars: document.getElementById('optimizerStepBars'),
  optimizerMaxSegments: document.getElementById('optimizerMaxSegments'),
  optimizerMinTrades: document.getElementById('optimizerMinTrades'),
  researchPreset: document.getElementById('researchPreset'),
  researchNote: document.getElementById('researchNote'),
  applyResearchPresetBtn: document.getElementById('applyResearchPresetBtn'),
  runResearchBtn: document.getElementById('runResearchBtn'),
  loadLatestResearchBtn: document.getElementById('loadLatestResearchBtn'),
  downloadResearchReportBtn: document.getElementById('downloadResearchReportBtn'),
  downloadResearchCsvBtn: document.getElementById('downloadResearchCsvBtn'),
  researchStatus: document.getElementById('researchStatus'),
  researchMeta: document.getElementById('researchMeta'),
  researchEffectiveSummary: document.getElementById('researchEffectiveSummary'),
  paperName: document.getElementById('paperName'),
  paperSteps: document.getElementById('paperSteps'),
  symbolsList: document.getElementById('symbolsList'),
  symbolsCountBadge: document.getElementById('symbolsCountBadge'),
  runBadge: document.getElementById('runBadge'),
  logBox: document.getElementById('logBox'),
  primaryTbody: document.getElementById('primaryTbody'),
  secondaryTbody: document.getElementById('secondaryTbody'),
  paperTradesTbody: document.getElementById('paperTradesTbody'),
  paperEventsTbody: document.getElementById('paperEventsTbody'),
  paperRunBadge: document.getElementById('paperRunBadge'),
  paperRunMeta: document.getElementById('paperRunMeta'),
  configForm: document.getElementById('configForm'),
  csvForm: document.getElementById('csvForm'),
  csvSymbol: document.getElementById('csvSymbol'),
  equityCanvas: document.getElementById('equityCanvas'),
  paperEquityCanvas: document.getElementById('paperEquityCanvas'),
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
  paperMetricReturn: document.getElementById('paperMetricReturn'),
  paperMetricProfitFactor: document.getElementById('paperMetricProfitFactor'),
  paperMetricTrades: document.getElementById('paperMetricTrades'),
  paperMetricAvgR: document.getElementById('paperMetricAvgR'),
  universeHint: document.getElementById('universeHint'),
};

const TAB_META = {
  research: {
    title: 'Исследование',
    subtitle: 'Автоисследование, ручной бэктест / walk-forward и единая аналитическая зона для проверки гипотез.',
  },
  paper: {
    title: 'Бумажный прогон',
    subtitle: 'Бумажный прогон и разбор его результатов без смешивания с исследованием и контуром исполнения.',
  },
  live: {
    title: 'Исполнение',
    subtitle: 'Отдельная рабочая зона исполнения: статус адаптера, план, подтверждение и журнал сухого прогона.',
  },
  data: {
    title: 'Данные',
    subtitle: 'Синхронизация, загрузка демо-данных и импорт CSV вынесены в отдельную техническую вкладку.',
  },
};

const MODE_LABELS = {
  mixed: 'смешанный режим',
  trend_only: 'только трендовый модуль',
  reversion_only: 'только реверсионный модуль',
};

const PARAM_LABELS = {
  risk_per_trade: 'риск на сделку',
  trend_strength_min: 'мин. сила тренда',
  reversion_zscore_threshold: 'Z-оценка возврата',
  volatility_score_min: 'мин. score волатильности',
  volatility_score_max: 'макс. score волатильности',
  volume_multiplier: 'множитель объёма',
  atr_stop_mult_trend: 'ATR-стоп тренда',
  atr_stop_mult_reversion: 'ATR-стоп реверсии',
};

function labelForMode(mode) {
  return MODE_LABELS[mode] || mode || '—';
}

function labelForViewMode(mode) {
  if (!mode) return '—';
  if (String(mode).startsWith('research:')) return `автоисследование: ${String(mode).slice('research:'.length)}`;
  const map = {
    backtest: 'бэктест',
    'walk-forward': 'прогон по окнам',
    optimizer: 'ручной оптимизатор',
    'ручной оптимизатор': 'ручной оптимизатор',
    paper: 'бумажный прогон',
    research: 'автоисследование',
  };
  return map[mode] || mode;
}

function summarizeResearchPreset(preset) {
  if (!preset || !els.researchEffectiveSummary) return;
  const stage = preset.stages?.[0] || {};
  const optimizer = stage.optimizer || {};
  const paramSpecs = stage.param_specs || stage.fallback_param_specs || {};
  const symbol = selectedPickerValue('research') || 'не выбран';
  const searchParams = Object.keys(paramSpecs).map((name) => PARAM_LABELS[name] || name).join(', ') || 'нет';
  const windows = optimizer.optimizer_train_bars ? `обучение ${optimizer.optimizer_train_bars} / проверка ${optimizer.optimizer_test_bars} / шаг ${optimizer.optimizer_step_bars} / сегментов ${optimizer.optimizer_max_segments} / мин. сделок ${optimizer.optimizer_min_trades_test}` : 'окна не заданы';
  els.researchEffectiveSummary.textContent = `Символ: ${symbol}. Этап 1: ${labelForMode(stage.mode)}; прогонов ${stage.trials || '—'}; ${windows}. Поиск: ${searchParams}. База: текущие поля «Базового профиля стратегии».`;
}

function setActiveTab(nextTab) {
  const tab = TAB_META[nextTab] ? nextTab : 'research';
  state.activeTab = tab;
  localStorage.setItem('bybit-v4-active-tab', tab);
  els.tabButtons.forEach((button) => {
    const active = button.dataset.tabTarget === tab;
    button.classList.toggle('active', active);
    button.setAttribute('aria-selected', active ? 'true' : 'false');
  });
  els.tabPanels.forEach((panel) => {
    const active = panel.dataset.tabPanel === tab;
    panel.toggleAttribute('hidden', !active);
    panel.classList.toggle('active', active);
  });
  if (els.workspaceTitle) els.workspaceTitle.textContent = TAB_META[tab].title;
  if (els.workspaceSubtitle) els.workspaceSubtitle.textContent = TAB_META[tab].subtitle;
}

function initTabs() {
  els.tabButtons.forEach((button) => {
    button.addEventListener('click', () => setActiveTab(button.dataset.tabTarget || 'research'));
  });
  setActiveTab(state.activeTab);
}

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

function parseLocaleNumber(raw, { fallback = null } = {}) {
  if (raw === null || raw === undefined) return fallback;
  const text = String(raw).trim();
  if (!text) return fallback;
  const normalized = text.replace(/\s+/g, '').replace(',', '.');
  const value = Number(normalized);
  if (!Number.isFinite(value)) throw new Error(`Некорректное число: ${raw}`);
  return value;
}

window.parseLocaleNumber = parseLocaleNumber;

function readNumberInput(input, { fallback = null, allowEmpty = false } = {}) {
  const value = parseLocaleNumber(input?.value, { fallback: allowEmpty ? fallback : null });
  if (value === null && !allowEmpty) {
    const label = input?.name || input?.id || 'value';
    throw new Error(`Некорректное число в поле ${label}`);
  }
  return value;
}

async function waitForJob(jobId, { intervalMs = 700, onTick } = {}) {
  while (true) {
    const job = await api(`/api/jobs/${jobId}`);
    if (onTick) onTick(job);
    if (job.status === 'success') return job.result;
    if (job.status === 'error') throw new Error(job.error || 'Фоновая задача завершилась ошибкой');
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}

async function runAsyncJob(kindLabel, path, body, onComplete) {
  const queued = await api(path, { method: 'POST', body: JSON.stringify({ ...body, async: true }) });
  log(`${kindLabel}: задача ${queued.job_id} поставлена в очередь`, 'info');
  let lastStatus = '';
  const result = await waitForJob(queued.job_id, {
    intervalMs: 700,
    onTick(job) {
      if (job.status !== lastStatus) {
        lastStatus = job.status;
        if (job.status === 'running') log(`${kindLabel}: задача ${job.id} выполняется`, 'info');
      }
    },
  });
  if (onComplete) await onComplete(result);
  return result;
}

function normalizeSymbols(items) {
  return [...new Set((items || []).map((s) => String(s || '').trim().toUpperCase()).filter(Boolean))];
}

function loadSavedPickerSelection(key) {
  try {
    return normalizeSymbols(JSON.parse(localStorage.getItem(`bybit-v4-picker-${key}`) || '[]'));
  } catch (_) {
    return [];
  }
}

function savePickerSelection(key) {
  const picker = state.pickers[key];
  if (!picker) return;
  localStorage.setItem(`bybit-v4-picker-${key}`, JSON.stringify([...picker.selected]));
}

function selectedSymbols() {
  return [...state.selectedSymbols];
}

function selectedPickerSymbols(key) {
  return state.pickers[key] ? [...state.pickers[key].selected] : [];
}

function selectedPickerValue(key) {
  return selectedPickerSymbols(key)[0] || '';
}

function pickerSummary(picker, selected) {
  const count = selected.length;
  if (picker.multi) {
    if (!count) return { title: 'Выбрать символы' };
    return { title: `${count} выбрано` };
  }
  if (!count) return { title: 'Выбрать символ' };
  return { title: selected[0] };
}

function formatCompact(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
  return Number(value).toLocaleString('en-US', { notation: 'compact', maximumFractionDigits: 1 });
}

function buildPicker(root, key, { multi = true, placeholder = 'Выбрать символы' } = {}) {
  if (!root) return;
  root.innerHTML = `
    <button type="button" class="multi-select-toggle">
      <span class="picker-toggle-title">${placeholder}</span>
      <span class="picker-toggle-subtitle">Ничего не выбрано</span>
    </button>
    <div class="multi-select-menu" hidden>
      <div class="multi-select-toolbar">
        <input type="text" class="multi-select-search" placeholder="Поиск по символу или монете" />
        <div class="multi-select-actions ${multi ? '' : 'single-actions'}">
          ${multi ? '<button type="button" data-action="all">Выбрать всё</button>' : ''}
          <button type="button" data-action="clear">${multi ? 'Сбросить всё' : 'Очистить'}</button>
        </div>
      </div>
      <div class="multi-select-selected"></div>
      <div class="multi-select-options"></div>
    </div>
  `;

  const picker = {
    root,
    key,
    multi,
    placeholder,
    toggle: root.querySelector('.multi-select-toggle'),
    menu: root.querySelector('.multi-select-menu'),
    search: root.querySelector('.multi-select-search'),
    selectedWrap: root.querySelector('.multi-select-selected'),
    optionsWrap: root.querySelector('.multi-select-options'),
    selected: new Set(loadSavedPickerSelection(key)),
    filter: '',
    valueInput: document.getElementById(key === 'csv' ? 'csvSymbol' : key === 'tiny' ? 'tinySymbol' : ''),
  };
  state.pickers[key] = picker;

  picker.toggle.addEventListener('click', (event) => {
    event.stopPropagation();
    const willOpen = picker.menu.hasAttribute('hidden');
    closeAllPickers();
    if (willOpen) {
      picker.menu.removeAttribute('hidden');
      picker.root.classList.add('open');
      picker.search.focus();
      renderPicker(key);
    }
  });

  picker.search.addEventListener('input', () => {
    picker.filter = picker.search.value.trim().toUpperCase();
    renderPicker(key);
  });

  picker.menu.addEventListener('click', (event) => {
    const action = event.target.dataset.action;
    if (action === 'all' && picker.multi) {
      filteredCatalogForPicker(key).forEach((row) => picker.selected.add(row.symbol));
      savePickerSelection(key);
      renderPicker(key);
      return;
    }
    if (action === 'clear') {
      picker.selected.clear();
      savePickerSelection(key);
      renderPicker(key);
      return;
    }
    const removeSymbol = event.target.dataset.removeSymbol;
    if (removeSymbol) {
      picker.selected.delete(removeSymbol);
      savePickerSelection(key);
      renderPicker(key);
    }
  });
}

function closeAllPickers() {
  Object.values(state.pickers).forEach((picker) => {
    picker.menu.setAttribute('hidden', 'hidden');
    picker.root.classList.remove('open');
  });
}

function filteredCatalogForPicker(key) {
  const picker = state.pickers[key];
  const filter = (picker?.filter || '').trim().toUpperCase();
  const rows = state.symbolCatalog || [];
  if (!filter) return rows;
  return rows.filter((row) => [row.symbol, row.base_coin, row.quote_coin].filter(Boolean).join(' ').toUpperCase().includes(filter));
}

function setSingleDefault(picker, desiredList) {
  if (!picker || picker.selected.size > 0) return;
  const first = desiredList.find(Boolean);
  if (first) picker.selected = new Set([first]);
}

function ensurePickerDefaults() {
  const demoDefaults = normalizeSymbols(state.config.demo_symbols || ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT']);
  const syncDefaults = normalizeSymbols(loadSavedPickerSelection('sync').length ? loadSavedPickerSelection('sync') : ['BTCUSDT', 'ETHUSDT']);
  const catalogSymbols = new Set((state.symbolCatalog || []).map((row) => row.symbol));
  const availableSymbols = state.symbols.length ? state.symbols : [...catalogSymbols];
  const fallbackDemo = demoDefaults.filter((symbol) => catalogSymbols.has(symbol));
  const fallbackSync = syncDefaults.filter((symbol) => catalogSymbols.has(symbol));
  if (state.pickers.demo && state.pickers.demo.selected.size === 0) {
    (fallbackDemo.length ? fallbackDemo : demoDefaults).forEach((symbol) => state.pickers.demo.selected.add(symbol));
  }
  if (state.pickers.sync && state.pickers.sync.selected.size === 0) {
    (fallbackSync.length ? fallbackSync : syncDefaults).forEach((symbol) => state.pickers.sync.selected.add(symbol));
  }
  setSingleDefault(state.pickers.csv, [loadSavedPickerSelection('csv')[0], availableSymbols[0], demoDefaults[0], 'BTCUSDT'].filter((symbol) => catalogSymbols.has(symbol) || symbol === 'BTCUSDT'));
  setSingleDefault(state.pickers.tiny, [loadSavedPickerSelection('tiny')[0], 'DOGEUSDT', availableSymbols[0], demoDefaults[0]].filter((symbol) => catalogSymbols.has(symbol)));
  setSingleDefault(state.pickers.optimizer, [loadSavedPickerSelection('optimizer')[0], availableSymbols[0], demoDefaults[0], 'BTCUSDT'].filter((symbol) => catalogSymbols.has(symbol) || symbol === 'BTCUSDT'));
  setSingleDefault(state.pickers.research, [loadSavedPickerSelection('research')[0], loadSavedPickerSelection('optimizer')[0], availableSymbols[0], demoDefaults[0], 'BTCUSDT'].filter((symbol) => catalogSymbols.has(symbol) || symbol === 'BTCUSDT'));
  ['demo', 'sync', 'csv', 'tiny', 'optimizer', 'research'].forEach(savePickerSelection);
}

function renderPicker(key) {
  const picker = state.pickers[key];
  if (!picker) return;
  const allRows = filteredCatalogForPicker(key);
  const selected = [...picker.selected];
  const summary = pickerSummary(picker, selected);
  picker.toggle.innerHTML = `<span class="picker-toggle-main">${summary.title}</span>`;
  picker.toggle.title = selected.join(', ');
  if (picker.valueInput) picker.valueInput.value = selected[0] || '';

  picker.selectedWrap.innerHTML = '';
  if (picker.multi) {
    if (selected.length) {
      selected.slice(0, 12).forEach((symbol) => {
        const tag = document.createElement('button');
        tag.type = 'button';
        tag.className = 'mini-chip';
        tag.dataset.removeSymbol = symbol;
        tag.textContent = symbol;
        picker.selectedWrap.appendChild(tag);
      });
    } else {
      const empty = document.createElement('div');
      empty.className = 'picker-empty';
      empty.textContent = 'Ничего не выбрано';
      picker.selectedWrap.appendChild(empty);
    }
    picker.selectedWrap.hidden = false;
  } else {
    picker.selectedWrap.hidden = true;
  }

  picker.optionsWrap.innerHTML = '';
  if (!allRows.length) {
    const empty = document.createElement('div');
    empty.className = 'picker-empty';
    empty.textContent = 'Ничего не найдено';
    picker.optionsWrap.appendChild(empty);
    return;
  }
  allRows.slice(0, 120).forEach((row) => {
    const option = document.createElement('label');
    option.className = `multi-option ${picker.multi ? '' : 'single-option'}`;
    option.innerHTML = `
      <input type="${picker.multi ? 'checkbox' : 'radio'}" name="picker-${key}" ${picker.selected.has(row.symbol) ? 'checked' : ''} />
      <div class="multi-option-main">
        <strong>${row.symbol}</strong>
        <small>${row.base_coin || ''}/${row.quote_coin || ''}</small>
      </div>
      <div class="multi-option-meta">${formatCompact(row.turnover24h)}</div>
    `;
    option.querySelector('input').addEventListener('change', (event) => {
      if (picker.multi) {
        if (event.target.checked) picker.selected.add(row.symbol);
        else picker.selected.delete(row.symbol);
      } else {
        picker.selected = event.target.checked ? new Set([row.symbol]) : new Set();
        closeAllPickers();
      }
      savePickerSelection(key);
      renderPicker(key);
      if (key === 'research') summarizeResearchPreset(selectedResearchPreset());
    });
    picker.optionsWrap.appendChild(option);
  });
  if (key === 'research') summarizeResearchPreset(selectedResearchPreset());
}

function updateCatalogHints() {
  const meta = state.symbolCatalogMeta || {};
  const src = meta.source || '—';
  const when = meta.fetched_at ? new Date(meta.fetched_at).toLocaleString() : '—';
  const suffix = meta.error ? ` Ошибка обновления: ${meta.error}` : '';
  const baseHint = `Каталог: ${src}. Bybit linear / торговля / USDT, сортировка по обороту за 24ч. Обновлено: ${when}.${suffix}`;
  if (els.demoPickerHint) els.demoPickerHint.textContent = `${baseHint} Для демо-данных можно выбрать несколько символов.`;
  if (els.syncPickerHint) els.syncPickerHint.textContent = `${baseHint} Для sync можно выбрать несколько символов.`;
  if (els.csvPickerHint) els.csvPickerHint.textContent = `${baseHint} Для CSV нужен один символ — метка импортируемого файла.`;
  if (els.tinyPickerHint) els.tinyPickerHint.textContent = `${baseHint} Для малого контура исполнения используется один символ.`;
  if (els.optimizerPickerHint) els.optimizerPickerHint.textContent = `${baseHint} Для ручного оптимизатора используется один символ и скользящая train/test проверка.`;
  if (els.researchPickerHint) els.researchPickerHint.textContent = `${baseHint} Для автоисследования используется один символ. Этот выбор не зависит от ручного оптимизатора.`;
}

function initPickers() {
  buildPicker(els.demoPicker, 'demo', { multi: true, placeholder: 'Выбрать символы' });
  buildPicker(els.syncPicker, 'sync', { multi: true, placeholder: 'Выбрать символы' });
  buildPicker(els.csvPicker, 'csv', { multi: false, placeholder: 'Выбрать символ' });
  buildPicker(els.tinyPicker, 'tiny', { multi: false, placeholder: 'Выбрать символ' });
  buildPicker(els.optimizerPicker, 'optimizer', { multi: false, placeholder: 'Выбрать символ' });
  buildPicker(els.researchPicker, 'research', { multi: false, placeholder: 'Выбрать символ' });
}

document.addEventListener('click', (event) => {
  if (!event.target.closest('.multi-select')) closeAllPickers();
});

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
  summarizeResearchPreset(selectedResearchPreset());
}

function collectConfigForm() {
  const out = {};
  const inputs = els.configForm.querySelectorAll('input');
  inputs.forEach((input) => {
    if (!input.name) return;
    if (input.type === 'checkbox') out[input.name] = input.checked;
    else if (input.type === 'number') {
      const parsed = readNumberInput(input, { allowEmpty: true });
      if (parsed !== null) out[input.name] = parsed;
    } else out[input.name] = input.value;
  });
  return out;
}

function collectOptimizerOverrides() {
  return {
    optimizer_trials: readNumberInput(els.optimizerTrials, { fallback: 24 }),
    optimizer_train_bars: readNumberInput(els.optimizerTrainBars, { fallback: 2016 }),
    optimizer_test_bars: readNumberInput(els.optimizerTestBars, { fallback: 576 }),
    optimizer_step_bars: readNumberInput(els.optimizerStepBars, { fallback: 576 }),
    optimizer_max_segments: readNumberInput(els.optimizerMaxSegments, { fallback: 8 }),
    optimizer_min_trades_test: readNumberInput(els.optimizerMinTrades, { fallback: 12 }),
  };
}


function selectedResearchPreset() {
  return state.researchPresets.find((preset) => preset.name === els.researchPreset?.value) || null;
}

function setResearchStatus(title, meta = '') {
  if (els.researchStatus) els.researchStatus.textContent = title || '—';
  if (els.researchMeta) els.researchMeta.textContent = meta || '—';
}

function fillResearchPresetOptions() {
  if (!els.researchPreset) return;
  els.researchPreset.innerHTML = '';
  state.researchPresets.forEach((preset) => {
    const option = document.createElement('option');
    option.value = preset.name;
    option.textContent = preset.label || preset.name;
    els.researchPreset.appendChild(option);
  });
  if (state.researchPresets.length && !els.researchPreset.value) {
    els.researchPreset.value = state.researchPresets[0].name;
  }
  const preset = selectedResearchPreset();
  if (preset) {
    setResearchStatus(preset.label || preset.name, preset.description || '');
    summarizeResearchPreset(preset);
  }
}

function applyResearchPresetToForm(preset) {
  if (!preset || !preset.stages?.length) return;
  const stage = preset.stages[0];
  const nextConfig = { ...state.config, ...(stage.fixed_overrides || {}) };
  const paramSpecs = stage.param_specs || stage.fallback_param_specs || {};
  Object.entries(paramSpecs).forEach(([name, spec]) => {
    const lower = Number(spec.lower);
    const upper = Number(spec.upper);
    const step = Number(spec.step || 0);
    const value = lower === upper ? lower : Number(((lower + upper) / 2).toFixed(step >= 1 ? 0 : 6));
    nextConfig[name] = value;
  });
  fillConfigForm(nextConfig);
  setResearchStatus(`Сценарий: ${preset.label || preset.name}`, `${preset.description || ''}`);
  summarizeResearchPreset(preset);
  log(`Сценарий загружен в базовый профиль: ${preset.label || preset.name}`, 'success');
}

function renderResearchStages(stages = []) {
  setPrimaryHeader(['Этап', 'Режим', 'Запуск', 'Допущено', 'Прибыльных', 'Доходность', 'PF', 'Победитель']);
  els.primaryTbody.innerHTML = '';
  stages.forEach((stage) => {
    const best = stage.winner_trial || stage.best_summary || {};
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${stage.stage_index}. ${stage.stage_name}</td>
      <td>${labelForMode(stage.mode)}</td>
      <td>${stage.optimizer_run_id}</td>
      <td>${stage.eligible_trials_count}</td>
      <td>${stage.profitable_eligible_trials_count}</td>
      <td class="${Number(best.total_return_pct) >= 0 ? 'pnl-pos' : 'pnl-neg'}">${formatNum(best.total_return_pct, 2)}%</td>
      <td>${best.profit_factor ?? '—'}</td>
      <td>${stage.winner_trial ? JSON.stringify(stage.winner_trial).slice(0, 120) : '—'}</td>
    `;
    els.primaryTbody.appendChild(tr);
  });
}

function renderResearchRun(manifest) {
  if (!manifest) return;
  state.latestResearchId = manifest.research_run_id;
  state.latestResearchManifest = manifest;
  const winner = manifest.winner || {};
  renderMetrics({
    starting_equity: state.config.starting_equity,
    ending_equity: null,
    total_return_pct: winner.total_return_pct,
    max_drawdown_pct: winner.max_drawdown_pct,
    win_rate: winner.win_rate,
    profit_factor: winner.profit_factor,
    sharpe: winner.sharpe,
    trades_count: winner.trades_count,
    avg_r: winner.avg_r,
    expectancy_pct: winner.expectancy_pct,
    stop_rate: winner.stop_rate,
  }, `research:${manifest.plan_label || manifest.plan_name}`);
  if (winner.best_equity_curve?.length > 1) drawEquityCurve(winner.best_equity_curve);
  else drawEquityCurve([]);
  renderResearchStages(manifest.stages || []);
  renderSecondaryRows((manifest.stages || []).map((stage) => ({
    created_at: manifest.created_at,
    level: `${stage.stage_name} / ${labelForMode(stage.mode)}`,
    message: `допущено=${stage.eligible_trials_count}, прибыльных=${stage.profitable_eligible_trials_count}, победитель=${JSON.stringify(stage.winner_trial || {})}`
  })), 'events');
  els.runBadge.textContent = `Автоисследование ${manifest.research_run_id}`;
  state.viewMode = 'research';
  setResearchStatus(manifest.plan_label || manifest.plan_name, `победитель: ${winner.stage_name || '—'} / доходность=${formatNum(winner.total_return_pct, 2)}% / PF=${winner.profit_factor ?? '—'}`);
  summarizeResearchPreset(selectedResearchPreset());
  setActiveTab('research');
}

async function loadResearchPresets() {
  const payload = await api('/api/research-presets');
  state.researchPresets = payload.presets || [];
  fillResearchPresetOptions();
}

async function loadLatestResearch() {
  const payload = await api('/api/research-runs?limit=1');
  const run = payload.runs?.[0];
  if (!run) {
    log('Запусков автоисследования пока нет', 'warn');
    return;
  }
  const details = await api(`/api/research-runs/${run.research_run_id}`);
  renderResearchRun(details);
  setActiveTab('research');
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
  els.metricMode.textContent = labelForViewMode(mode);
}

function renderPaperMetrics(summary = {}) {
  if (els.paperMetricReturn) els.paperMetricReturn.textContent = `${formatNum(summary.total_return_pct)}%`;
  if (els.paperMetricProfitFactor) els.paperMetricProfitFactor.textContent = String(summary.profit_factor ?? '—');
  if (els.paperMetricTrades) els.paperMetricTrades.textContent = String(summary.trades_count ?? '—');
  if (els.paperMetricAvgR) els.paperMetricAvgR.textContent = formatNum(summary.avg_r, 3);
}

function drawCurveOnCanvas(canvas, points, emptyLabel = 'Нет данных equity curve') {
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const width = canvas.width;
  const height = canvas.height;
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = '#09111f';
  ctx.fillRect(0, 0, width, height);

  if (!points || points.length < 2) {
    ctx.fillStyle = '#91a0c2';
    ctx.font = '16px Arial';
    ctx.fillText(emptyLabel, 32, 48);
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

function drawEquityCurve(points) {
  drawCurveOnCanvas(els.equityCanvas, points, 'Нет данных equity curve');
}

function drawPaperEquityCurve(points) {
  drawCurveOnCanvas(els.paperEquityCanvas, points, 'Нет данных по кривой капитала');
}

function setPrimaryHeader(labels) {
  const table = els.primaryTbody?.closest('table');
  const headerCells = table ? table.querySelectorAll('thead tr th') : [];
  labels.forEach((label, idx) => {
    if (headerCells[idx]) headerCells[idx].textContent = label;
  });
}

function renderBacktestTrades(trades = []) {
  setPrimaryHeader(['Символ', 'Режим', 'Сторона', 'Вход', 'Выход', 'Чистый PnL', 'R', 'Причина']);
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
  setPrimaryHeader(['#', 'Обучение', 'Проверка', 'Лучшие параметры', 'Доходность', 'Макс. просадка', 'Сделки', 'Целевая оценка']);
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


function renderOptimizerTrials(trials = []) {
  setPrimaryHeader(['#', 'Балл', 'Доходность', 'Макс. просадка', 'PF', 'Сделки', 'Стоп-выходы', 'Параметры']);
  els.primaryTbody.innerHTML = '';
  trials.forEach((trial) => {
    const summary = trial.summary || {};
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${trial.trial_no}</td>
      <td>${formatNum(trial.score, 3)}</td>
      <td class="${Number(summary.total_return_pct) >= 0 ? 'pnl-pos' : 'pnl-neg'}">${formatNum(summary.total_return_pct, 2)}%</td>
      <td>${formatNum(summary.max_drawdown_pct, 2)}%</td>
      <td>${summary.profit_factor ?? '—'}</td>
      <td>${summary.trades_count ?? '—'}</td>
      <td>${formatNum(summary.stop_rate, 1)}%</td>
      <td>${JSON.stringify(trial.params)}</td>
    `;
    els.primaryTbody.appendChild(tr);
  });
}

function renderPaperTrades(trades = []) {
  if (!els.paperTradesTbody) return;
  els.paperTradesTbody.innerHTML = '';
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
    els.paperTradesTbody.appendChild(tr);
  });
}

function renderPaperEvents(rows = []) {
  if (!els.paperEventsTbody) return;
  els.paperEventsTbody.innerHTML = '';
  rows.forEach((row) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${new Date(row.created_at).toLocaleString()}</td>
      <td>${row.level}</td>
      <td>${row.message}</td>
    `;
    els.paperEventsTbody.appendChild(tr);
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

async function refreshSymbols({ refreshCatalog = false } = {}) {
  const [localPayload, catalogPayload] = await Promise.all([
    api('/api/symbols'),
    api(`/api/symbol-catalog?limit=120${refreshCatalog ? '&refresh=1' : ''}`),
  ]);
  state.symbols = localPayload.symbols || [];
  state.symbolCatalog = catalogPayload.symbols || [];
  state.symbolCatalogMeta = catalogPayload;
  ensurePickerDefaults();
  renderPicker('demo');
  renderPicker('sync');
  renderPicker('csv');
  renderPicker('tiny');
  renderPicker('optimizer');
  renderPicker('research');
  updateCatalogHints();

  const preferred = selectedPickerSymbols('demo').filter((symbol) => state.symbols.includes(symbol));
  if (state.selectedSymbols.size === 0) {
    (preferred.length ? preferred : state.symbols).forEach((symbol) => state.selectedSymbols.add(symbol));
  } else {
    state.selectedSymbols = new Set([...state.selectedSymbols].filter((s) => state.symbols.includes(s)));
    if (state.selectedSymbols.size === 0) {
      (preferred.length ? preferred : state.symbols).forEach((symbol) => state.selectedSymbols.add(symbol));
    }
  }
  renderSymbols();
  if (els.universeHint) {
    els.universeHint.textContent = `Universe = локально загруженные символы из candles (interval=5). Сейчас: ${state.symbols.length}.`;
  }
}

async function loadConfig() {
  const config = await api('/api/config');
  fillConfigForm(config);
  if (els.optimizerTrials) els.optimizerTrials.value = config.optimizer_trials ?? 24;
  if (els.optimizerTrainBars) els.optimizerTrainBars.value = config.optimizer_train_bars ?? 2016;
  if (els.optimizerTestBars) els.optimizerTestBars.value = config.optimizer_test_bars ?? 576;
  if (els.optimizerStepBars) els.optimizerStepBars.value = config.optimizer_step_bars ?? 576;
  if (els.optimizerMaxSegments) els.optimizerMaxSegments.value = config.optimizer_max_segments ?? 8;
  if (els.optimizerMinTrades) els.optimizerMinTrades.value = config.optimizer_min_trades_test ?? 12;
}

async function loadLiveStatus() {
  const status = await api('/api/live-adapter/status');
  els.liveStatus.textContent = status.enabled ? (status.dry_run ? 'сухой прогон' : 'боевой режим') : 'отключён';
  els.liveMeta.textContent = `${status.testnet ? 'тестовая сеть' : 'основная сеть'} / ключ=${status.api_key_present ? 'есть' : 'нет'} / секрет=${status.api_secret_present ? 'есть' : 'нет'}`;
}

async function loadLatestRun() {
  const data = await api('/api/runs?limit=1');
  const run = data.runs?.[0];
  if (!run) {
    log('Запусков бэктеста пока нет', 'warn');
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
  els.runBadge.textContent = `Бэктест #${run.id}`;
  state.viewMode = 'backtest';
  setActiveTab('research');
}

async function loadLatestWalkforward() {
  const data = await api('/api/walkforward-runs?limit=1');
  const run = data.runs?.[0];
  if (!run) {
    log('Запусков прогона по окнам пока нет', 'warn');
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
  els.runBadge.textContent = `Прогон по окнам #${run.id}`;
  renderSecondaryRows(details.segments.map((seg) => ({ created_at: seg.test_end_ts, level: `segment ${seg.segment_no}`, message: seg.best_params_json })), 'events');
  state.viewMode = 'walk-forward';
  setActiveTab('research');
}


async function loadLatestOptimizer() {
  const data = await api('/api/optimizer-runs?limit=1');
  const run = data.runs?.[0];
  if (!run) {
    log('Запусков ручного оптимизатора пока нет', 'warn');
    return;
  }
  state.latestOptimizerId = run.id;
  const details = await api(`/api/optimizer-runs/${run.id}`);
  state.latestOptimizerBestParams = details.best_params || null;
  renderMetrics(details.best_summary, 'ручной оптимизатор');
  drawEquityCurve(details.best_equity_curve || []);
  renderOptimizerTrials(details.trials || []);
  renderSecondaryRows((details.segments || []).map((seg) => ({
    created_at: seg.test_end_ts,
    level: `seg ${seg.segment_no}`,
    message: `ret=${formatNum(seg.summary?.total_return_pct, 2)}% pf=${seg.summary?.profit_factor ?? '—'} trades=${seg.summary?.trades_count ?? '—'}`,
  })), 'events');
  els.runBadge.textContent = `Ручной оптимизатор #${run.id}`;
  state.viewMode = 'optimizer';
  setActiveTab('research');
}

async function loadPaperSession(sessionId = state.latestPaperSessionId) {
  if (!sessionId) {
    const sessions = await api('/api/paper-sessions?limit=1');
    const latest = sessions.sessions?.[0];
    if (!latest) {
      log('Сессий бумажного прогона пока нет', 'warn');
      return;
    }
    sessionId = latest.id;
  }
  state.latestPaperSessionId = sessionId;
  const data = await api(`/api/paper-sessions/${sessionId}`);
  const session = data.session;
  const stateJson = session.state || {};
  els.paperStatus.textContent = `${session.status} / #${session.id}`;
  els.paperMeta.textContent = `индекс=${session.current_index} / капитал=${formatNum(session.current_equity)} / открыто позиций=${(stateJson.open_positions || []).length}`;
  const summary = data.summary || {
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
  };
  renderPaperMetrics(summary);
  drawPaperEquityCurve(data.equity);
  renderPaperTrades(data.trades);
  renderPaperEvents(data.events || []);
  if (els.paperRunBadge) els.paperRunBadge.textContent = `Бумажный прогон #${session.id}`;
  if (els.paperRunMeta) els.paperRunMeta.textContent = `доходность=${formatNum(summary.total_return_pct, 2)}% / PF=${summary.profit_factor ?? '—'} / сделки=${summary.trades_count ?? '—'}`;
  state.viewMode = 'paper';
  setActiveTab('paper');
  return data;
}

els.loadDemoBtn.addEventListener('click', async () => {
  try {
    setActiveTab('data');
    const result = await runAsyncJob('Demo-данные', '/api/load-demo-data', { symbols: selectedPickerSymbols('demo'), days: readNumberInput(els.demoDays, { fallback: 20 }), interval: '5' });
    log(`Demo-данные: ${result.candles_upserted} свечей, funding ${result.funding_upserted}`, 'success');
    await refreshSymbols();
  } catch (error) { log(error.message.split('\n')[0], 'error'); }
});

els.syncBybitBtn.addEventListener('click', async () => {
  try {
    setActiveTab('data');
    const result = await runAsyncJob('Bybit sync', '/api/sync-bybit-public', { symbols: selectedPickerSymbols('sync'), days: readNumberInput(els.syncDays, { fallback: 14 }), interval: '5' });
    log(`Bybit sync: ${result.candles_upserted} свечей, funding ${result.funding_upserted}`, 'success');
    await refreshSymbols();
  } catch (error) { log(error.message.split('\n')[0], 'error'); }
});

els.refreshSymbolsBtn.addEventListener('click', async () => {
  try { setActiveTab('data'); await refreshSymbols({ refreshCatalog: true }); log('Universe и каталог обновлены', 'success'); } catch (error) { log(error.message, 'error'); }
});

els.runBacktestBtn.addEventListener('click', async () => {
  try {
    const result = await runAsyncJob('Бэктест', '/api/run-backtest', { symbols: selectedSymbols(), overrides: collectConfigForm() });
    state.latestRunId = result.run_id;
    renderMetrics(result.summary, 'backtest');
    drawEquityCurve(result.equity_curve);
    renderBacktestTrades(result.trades);
    renderSecondaryRows(result.signals.slice(-100).reverse().map((s) => ({ created_at: s.ts, level: s.side, message: `${s.symbol} ${s.regime} ${formatNum(s.score, 3)} ${s.notes}` })), 'events');
    els.runBadge.textContent = `Бэктест #${result.run_id}`;
    state.viewMode = 'backtest';
    setActiveTab('research');
    log(`Бэктест завершён: запуск #${result.run_id}, сделок=${result.summary.trades_count}, сигналов=${result.signals_count}`, 'success');
  } catch (error) { log(error.message.split('\n')[0], 'error'); }
});

els.runWalkforwardBtn.addEventListener('click', async () => {
  try {
    const result = await runAsyncJob('Прогон по окнам', '/api/run-walkforward', { symbols: selectedSymbols(), overrides: collectConfigForm() });
    state.latestWalkforwardId = result.walkforward_run_id;
    renderMetrics(result.summary, 'walk-forward');
    drawEquityCurve(result.equity_curve);
    renderWalkforwardSegments(result.segments);
    renderSecondaryRows(result.segments.map((seg) => ({ created_at: seg.test_end_ts, level: `segment ${seg.segment_no}`, message: JSON.stringify(seg.best_params) })), 'events');
    els.runBadge.textContent = `Прогон по окнам #${result.walkforward_run_id}`;
    state.viewMode = 'walk-forward';
    setActiveTab('research');
    log(`Прогон по окнам завершён: запуск #${result.walkforward_run_id}, сегментов=${result.segments.length}`, 'success');
  } catch (error) { log(error.message.split('\n')[0], 'error'); }
});


els.runOptimizerBtn?.addEventListener('click', async () => {
  try {
    const symbol = selectedPickerValue('optimizer');
    if (!symbol) throw new Error('Выбери символ для ручного оптимизатора');
    const overrides = { ...collectConfigForm(), ...collectOptimizerOverrides() };
    const trials = readNumberInput(els.optimizerTrials, { fallback: 24 });
    const result = await runAsyncJob('Ручной оптимизатор', '/api/run-optimizer', { symbols: [symbol], trials, overrides });
    state.latestOptimizerId = result.optimizer_run_id;
    state.latestOptimizerBestParams = result.best_params || null;
    renderMetrics(result.best_summary, 'ручной оптимизатор');
    drawEquityCurve(result.best_equity_curve || []);
    renderOptimizerTrials(result.trials || []);
    renderSecondaryRows((result.segments || []).map((seg) => ({
      created_at: seg.test_end_ts,
      level: `seg ${seg.segment_no}`,
      message: `ret=${formatNum(seg.summary?.total_return_pct, 2)}% pf=${seg.summary?.profit_factor ?? '—'} trades=${seg.summary?.trades_count ?? '—'}`,
    })), 'events');
    els.runBadge.textContent = `Ручной оптимизатор #${result.optimizer_run_id}`;
    state.viewMode = 'optimizer';
    setActiveTab('research');
    log(`Ручной оптимизатор завершён: запуск #${result.optimizer_run_id}, лучший балл=${formatNum(result.best_score, 3)}`, 'success');
  } catch (error) {
    log(error.message.split('\n')[0], 'error');
  }
});

els.applyBestOptimizerBtn?.addEventListener('click', async () => {
  try {
    if (!state.latestOptimizerId) throw new Error('Сначала запусти ручной оптимизатор');
    const payload = await api(`/api/optimizer-runs/${state.latestOptimizerId}/apply-best`, { method: 'POST' });
    fillConfigForm(payload.config);
    log('Лучший набор ручного оптимизатора применён в базовый профиль', 'success');
  } catch (error) { log(error.message, 'error'); }
});

els.researchPreset?.addEventListener('change', () => {
  const preset = selectedResearchPreset();
  if (preset) {
    setResearchStatus(preset.label || preset.name, preset.description || '');
    summarizeResearchPreset(preset);
  }
});

els.applyResearchPresetBtn?.addEventListener('click', () => {
  const preset = selectedResearchPreset();
  if (!preset) return log('Сценарий не найден', 'warn');
  applyResearchPresetToForm(preset);
});

els.runResearchBtn?.addEventListener('click', async () => {
  try {
    const symbol = selectedPickerValue('research');
    if (!symbol) throw new Error('Выбери символ для автоисследования');
    const preset = selectedResearchPreset();
    if (!preset) throw new Error('Сначала выбери сценарий автоисследования');
    setResearchStatus('выполняется', `${preset.label || preset.name} / ${symbol}`);
    setActiveTab('research');
    const result = await runAsyncJob('Автоисследование', '/api/run-auto-research', {
      symbols: [symbol],
      preset_name: preset.name,
      note: els.researchNote?.value?.trim() || '',
      base_overrides: collectConfigForm(),
    });
    renderResearchRun(result);
    log(`Автоисследование завершено: ${result.research_run_id}`, 'success');
  } catch (error) {
    log(error.message.split('\n')[0], 'error');
    setResearchStatus('ошибка', error.message.split('\n')[0]);
  }
});

els.loadLatestResearchBtn?.addEventListener('click', async () => {
  try { await loadLatestResearch(); } catch (error) { log(error.message, 'error'); }
});

els.downloadResearchReportBtn?.addEventListener('click', () => {
  if (!state.latestResearchId) return log('Нет запуска автоисследования для экспорта', 'warn');
  window.open(`/api/research-runs/${state.latestResearchId}/artifact/report.md`, '_blank');
});

els.downloadResearchCsvBtn?.addEventListener('click', () => {
  if (!state.latestResearchId) return log('Нет запуска автоисследования для экспорта', 'warn');
  window.open(`/api/research-runs/${state.latestResearchId}/artifact/combined_best_configs.csv`, '_blank');
});

els.createPaperBtn.addEventListener('click', async () => {
  try {
    const result = await api('/api/paper-sessions', {
      method: 'POST',
      body: JSON.stringify({
        name: els.paperName.value.trim() || 'paper-demo',
        symbols: selectedSymbols(),
        overrides: collectConfigForm(),
        auto_steps: readNumberInput(els.paperSteps, { fallback: 1 }),
      }),
    });
    state.latestPaperSessionId = result.session.id;
    await loadPaperSession(result.session.id);
    setActiveTab('paper');
    log(`Создана сессия бумажного прогона #${result.session.id}`, 'success');
  } catch (error) { log(error.message, 'error'); }
});

els.paperStepBtn.addEventListener('click', async () => {
  try {
    if (!state.latestPaperSessionId) throw new Error('Сначала создай сессию бумажного прогона');
    await api(`/api/paper-sessions/${state.latestPaperSessionId}/step`, { method: 'POST', body: JSON.stringify({ steps: readNumberInput(els.paperSteps, { fallback: 1 }) || 1 }) });
    await loadPaperSession(state.latestPaperSessionId);
    log('Сессия бумажного прогона продвинута', 'success');
  } catch (error) { log(error.message, 'error'); }
});

els.paperStartBtn.addEventListener('click', async () => {
  try {
    if (!state.latestPaperSessionId) throw new Error('Сначала создай сессию бумажного прогона');
    await api(`/api/paper-sessions/${state.latestPaperSessionId}/start`, { method: 'POST' });
    await loadPaperSession(state.latestPaperSessionId);
    log('Background replay запущен', 'success');
    if (window.paperPollTimer) clearInterval(window.paperPollTimer);
    window.paperPollTimer = setInterval(async () => {
      try {
        const payload = await loadPaperSession(state.latestPaperSessionId);
        if (!payload.session.is_background_running) {
          clearInterval(window.paperPollTimer);
          window.paperPollTimer = null;
        }
      } catch (_) {}
    }, 1500);
  } catch (error) { log(error.message, 'error'); }
});

els.paperStopBtn.addEventListener('click', async () => {
  try {
    if (!state.latestPaperSessionId) throw new Error('Нет сессии бумажного прогона');
    await api(`/api/paper-sessions/${state.latestPaperSessionId}/stop`, { method: 'POST' });
    if (window.paperPollTimer) { clearInterval(window.paperPollTimer); window.paperPollTimer = null; }
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

els.loadLatestOptimizerBtn?.addEventListener('click', async () => {
  try { await loadLatestOptimizer(); } catch (error) { log(error.message, 'error'); }
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
  if (!state.latestPaperSessionId) return log('Нет сессии бумажного прогона для экспорта', 'warn');
  window.open(`/api/paper-sessions/${state.latestPaperSessionId}/export/trades.csv`, '_blank');
});

els.exportOptimizerBtn?.addEventListener('click', () => {
  if (!state.latestOptimizerId) return log('Нет запуска ручного оптимизатора для экспорта', 'warn');
  window.open(`/api/optimizer-runs/${state.latestOptimizerId}/export/trials.csv`, '_blank');
});

els.configForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  try {
    const result = await api('/api/config', { method: 'POST', body: JSON.stringify(collectConfigForm()) });
    fillConfigForm(result);
    log('Базовый профиль сохранён', 'success');
  } catch (error) { log(error.message, 'error'); }
});

document.addEventListener('change', (event) => {
  const input = event.target;
  if (!(input instanceof HTMLInputElement) || input.type !== 'number') return;
  try {
    const parsed = parseLocaleNumber(input.value, { fallback: null });
    if (parsed !== null) input.value = String(parsed);
  } catch (_) {}
});

els.csvForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  try {
    setActiveTab('data');
    const result = await api('/api/import-csv', { method: 'POST', body: new FormData(els.csvForm) });
    log(`CSV импортирован: ${result.rows_inserted} свечей для ${result.symbol}`, 'success');
    await refreshSymbols();
  } catch (error) { log(error.message, 'error'); }
});

(async function init() {
  try {
    initTabs();
    initPickers();
    drawEquityCurve([]);
    drawPaperEquityCurve([]);
    await loadConfig();
    await loadResearchPresets();
    await refreshSymbols();
    await loadLiveStatus();
    await loadLatestRun();
    setActiveTab(state.activeTab);
  } catch (error) {
    log(error.message, 'error');
  }
})();
