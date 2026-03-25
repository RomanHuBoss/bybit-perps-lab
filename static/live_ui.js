(function () {
  const $ = (id) => document.getElementById(id);
  const els = {
    tinySymbol: $('tinySymbol'),
    tinyNotional: $('tinyNotional'),
    tinyFreshOnly: $('tinyFreshOnly'),
    loadSignalsBtn: $('loadSignalsBtn'),
    buildPlanBtn: $('buildPlanBtn'),
    executeTinyBtn: $('executeTinyBtn'),
    refreshTinyLogsBtn: $('refreshTinyLogsBtn'),
    tinyArm: $('tinyArm'),
    tinyPlanTitle: $('tinyPlanTitle'),
    tinyPlanMeta: $('tinyPlanMeta'),
    tinyMode: $('tinyMode'),
    tinyModeMeta: $('tinyModeMeta'),
    tinyOutput: $('tinyOutput'),
    tinyLogs: $('tinyLogs'),
  };

  if (!els.tinySymbol) return;

  let lastPlan = null;


  function syncArmState() {
    if (!els.executeTinyBtn) return;
    const armed = Boolean(els.tinyArm?.checked);
    els.executeTinyBtn.disabled = !armed;
  }

  function parseNumber(raw, fallback = null) {
    if (typeof window.parseLocaleNumber === 'function') {
      return window.parseLocaleNumber(raw, { fallback });
    }
    if (raw === null || raw === undefined) return fallback;
    const text = String(raw).trim();
    if (!text) return fallback;
    const value = Number(text.replace(',', '.'));
    if (!Number.isFinite(value)) throw new Error(`Некорректное число: ${raw}`);
    return value;
  }

  function showJson(el, data) {
    el.textContent = JSON.stringify(data, null, 2);
  }

  function note(msg, type = 'info') {
    if (typeof log === 'function') log(msg, type);
  }

  async function callApi(path, options = {}) {
    if (typeof api === 'function') return api(path, options);
    const res = await fetch(path, {
      headers: {
        ...(options.body instanceof FormData ? {} : { 'Content-Type': 'application/json' }),
        ...(options.headers || {}),
      },
      ...options,
    });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'Ошибка запроса');
    return payload;
  }

  async function refreshMode() {
    const status = await callApi('/api/live-adapter/status');
    els.tinyMode.textContent = status.enabled ? (status.dry_run ? 'сухой прогон' : 'боевой режим') : 'отключён';
    els.tinyModeMeta.textContent = `${status.testnet ? 'тестовая сеть' : 'основная сеть'} / ключ=${status.api_key_present ? 'есть' : 'нет'} / секрет=${status.api_secret_present ? 'есть' : 'нет'}`;
  }

  async function loadSignals() {
    const symbol = els.tinySymbol.value.trim().toUpperCase();
    const payload = await callApi(`/api/signals/latest?symbols=${encodeURIComponent(symbol)}`);
    showJson(els.tinyOutput, payload);
    const row = payload.signals?.[0];
    if (row?.signal) {
      els.tinyPlanTitle.textContent = `${row.symbol} / ${row.signal.side} / ${row.signal.regime}`;
      els.tinyPlanMeta.textContent = `оценка=${row.signal.score}; возраст сигнала=${row.bars_since_signal}; ${row.note}`;
    } else {
      els.tinyPlanTitle.textContent = `${symbol}`;
      els.tinyPlanMeta.textContent = row?.note || 'Сигнал не найден';
    }
    note(`Сигналы обновлены для ${symbol}`);
  }

  async function buildPlan() {
    const body = {
      symbol: els.tinySymbol.value.trim().toUpperCase(),
      fixed_notional_usdt: parseNumber(els.tinyNotional.value),
      require_fresh_signal: els.tinyFreshOnly.checked,
    };
    const payload = await callApi('/api/tiny-live/plan', { method: 'POST', body: JSON.stringify(body) });
    lastPlan = payload;
    showJson(els.tinyOutput, payload);
    els.tinyPlanTitle.textContent = `${payload.symbol} / ${payload.side}`;
    els.tinyPlanMeta.textContent = `кол-во=${payload.qty}; вход=${payload.entry_price}; стоп=${payload.stop_price}; тейк=${payload.take_profit_price}`;
    note(`План собран: ${payload.symbol} ${payload.side} количество=${payload.qty}`);
  }

  async function executePlan() {
    if (!els.tinyArm?.checked) throw new Error('Сначала вооружите кнопку отправки');
    const symbol = els.tinySymbol.value.trim().toUpperCase();
    const notional = parseNumber(els.tinyNotional.value);
    const fresh = els.tinyFreshOnly.checked;
    const status = await callApi('/api/live-adapter/status');
    const modeText = status.enabled ? (status.dry_run ? 'сухой прогон' : 'боевой режим') : 'отключён';
    const approved = window.confirm(`Отправить малый ордер по ${symbol} на ${notional} USDT? Режим адаптера: ${modeText}.`);
    if (!approved) return;
    const payload = await callApi('/api/tiny-live/execute', {
      method: 'POST',
      body: JSON.stringify({ symbol, fixed_notional_usdt: notional, require_fresh_signal: fresh }),
    });
    lastPlan = payload.plan;
    showJson(els.tinyOutput, payload);
    note(`Ордер отправлен через адаптер. идентификатор лога=${payload.log_id}`, 'warn');
    if (els.tinyArm) els.tinyArm.checked = false;
    syncArmState();
    await refreshLogs();
    await refreshMode();
  }

  async function refreshLogs() {
    const payload = await callApi('/api/tiny-live/logs?limit=10');
    showJson(els.tinyLogs, payload.logs || []);
  }

  els.loadSignalsBtn.addEventListener('click', () => loadSignals().catch((e) => note(e.message, 'error')));
  els.buildPlanBtn.addEventListener('click', () => buildPlan().catch((e) => note(e.message, 'error')));
  els.executeTinyBtn.addEventListener('click', () => executePlan().catch((e) => note(e.message, 'error')));
  els.refreshTinyLogsBtn.addEventListener('click', () => refreshLogs().catch((e) => note(e.message, 'error')));
  els.tinyArm?.addEventListener('change', syncArmState);

  syncArmState();
  refreshMode().catch(() => {});
  refreshLogs().catch(() => {});
})();
