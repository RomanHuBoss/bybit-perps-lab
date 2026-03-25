# Bybit Perps Lab v4 Tiny Live

Локальный проект для исследования и аккуратного запуска маленьких входов на **Bybit USDT perpetuals**.

Стек:
- Python + Flask
- SQLite
- HTML/CSS/Vanilla JS
- backtest
- walk-forward
- paper replay
- latest signals / tiny-live plan
- Bybit V5 REST adapter с `dry-run` по умолчанию

## Что нового в v4 Tiny Live

Добавлено поверх v3:
- endpoint для **свежих сигналов** по локальным свечам
- endpoint для **trade plan** под фиксированный `notional_usdt`
- endpoint для **tiny-live execute**
- лог отправленных tiny-order запросов в SQLite
- UI-блок для `dry-run / live`
- проверка минимального реального размера позиции через `tickSize / qtyStep / minOrderQty / minNotionalValue`

## Быстрый старт

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Открыть:

```text
http://127.0.0.1:8010
```

## Подготовка данных

В UI доступны 3 варианта:
- demo-данные
- импорт CSV
- синхронизация Bybit public data

CSV должен содержать колонки:
- `timestamp`
- `open`
- `high`
- `low`
- `close`
- `volume`

## Tiny live режим

### По умолчанию

Адаптер работает безопасно:
- `BYBIT_LIVE_ENABLED=false`
- `BYBIT_LIVE_DRY_RUN=true`

То есть проект строит план и показывает/логирует payload, но **не отправляет реальный ордер**.

### Dry-run

Оставь так:

```bash
BYBIT_LIVE_ENABLED=true
BYBIT_LIVE_DRY_RUN=true
BYBIT_TESTNET=true
BYBIT_API_KEY=...
BYBIT_API_SECRET=...
```

### Реальная отправка

Для testnet/mainnet:

```bash
BYBIT_LIVE_ENABLED=true
BYBIT_LIVE_DRY_RUN=false
BYBIT_TESTNET=true   # или false для mainnet
BYBIT_API_KEY=...
BYBIT_API_SECRET=...
BYBIT_RECV_WINDOW=5000
```

## Что делает tiny-live execute

Процесс такой:
1. Берётся **последний сигнал** по выбранному символу
2. Проверяется его свежесть
3. Берётся текущая цена и спецификация инструмента
4. Считается `qty` из заданного `notional_usdt`
5. Если `10 USDT` меньше допустимого минимума по инструменту, запрос блокируется
6. Если всё ок, формируется `Market` order с `takeProfit` и `stopLoss`
7. Результат и payload пишутся в `live_order_logs`

## Практическое замечание про $10

$10 подойдёт не для каждого линейного контракта.

Примерно:
- `BTCUSDT` часто потребует больше из-за `minOrderQty`
- `ETHUSDT` тоже может оказаться выше $10
- более дешёвые символы обычно подходят чаще

Поэтому сначала жми:
- **Последние сигналы**
- **Собрать план**

Если проект пишет, что $10 меньше минимального реального размера по символу, просто выбирай другой инструмент.

## Основные API маршруты

### Data
- `POST /api/load-demo-data`
- `POST /api/sync-bybit-public`
- `POST /api/import-csv`
- `GET /api/symbols`

### Research
- `POST /api/run-backtest`
- `GET /api/runs`
- `GET /api/runs/<id>`
- `POST /api/run-walkforward`
- `GET /api/walkforward-runs`
- `GET /api/walkforward-runs/<id>`

### Paper
- `POST /api/paper-sessions`
- `GET /api/paper-sessions`
- `GET /api/paper-sessions/<id>`
- `POST /api/paper-sessions/<id>/step`
- `POST /api/paper-sessions/<id>/start`
- `POST /api/paper-sessions/<id>/stop`

### Tiny live
- `GET /api/signals/latest?symbols=BTCUSDT,ETHUSDT`
- `GET /api/tiny-live/candidates?fixed_notional_usdt=10&require_fresh_signal=false`
- `POST /api/tiny-live/plan`
- `POST /api/tiny-live/execute`
- `GET /api/tiny-live/logs`
- `GET /api/live-adapter/status`
- `GET /api/live-adapter/wallet`
- `GET /api/live-adapter/positions`

## Полезные curl-примеры

### Сигналы

```bash
curl "http://127.0.0.1:8010/api/signals/latest?symbols=SOLUSDT,XRPUSDT"
```

### План на $10

```bash
curl -X POST http://127.0.0.1:8010/api/tiny-live/plan \
  -H "Content-Type: application/json" \
  -d '{"symbol":"DOGEUSDT","fixed_notional_usdt":10,"require_fresh_signal":false}'
```

### Dry-run execute

```bash
curl -X POST http://127.0.0.1:8010/api/tiny-live/execute \
  -H "Content-Type: application/json" \
  -d '{"symbol":"DOGEUSDT","fixed_notional_usdt":10,"require_fresh_signal":false}'
```

## Что исправлено в этой редакции

- demo-генератор теперь создаёт **строго выровненные закрытые 5m-бары** и перед загрузкой очищает старые demo-данные по выбранным символам, чтобы прогоны были воспроизводимыми
- sync Bybit public теперь **отбрасывает текущую незакрытую свечу**, чтобы не тянуть в бэктест look-ahead через open candle
- paper replay теперь **принудительно закрывает открытые позиции в конце истории**, как и backtest engine
- tiny-live planner теперь **округляет stop/take-profit в безопасную сторону по tickSize**, чтобы не подтягивать стоп ближе к входу из-за округления
- self-check расширен: проверка выравнивания demo-времени, paper/backtest-consistency, directional tick rounding, end-of-test liquidation

## Ограничения

Это не production OMS и не обещание доходности.

Здесь нет:
- private WebSocket order-state machine
- live reconciliation loop
- server-side scheduler
- distributed queue
- production-grade secret management
- полноценного portfolio risk engine на уровне кластера

Но как локальный рабочий проект для:
- загрузки данных,
- backtest,
- walk-forward,
- paper,
- и tiny live / dry-run отправки,

он уже пригоден.


## v4.1 performance patch

- background jobs for demo load, Bybit public sync, backtest, walk-forward
- incremental paper replay engine with in-memory prepared market cache
- reduced paper payload size via downsampled equity and limited rows
- threaded Flask launch for smoother local UX
- added SQLite indexes for faster reads
