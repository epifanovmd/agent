# Наблюдение: метрики, подписки, показатели, логи

Что агент присылает о себе и об узле, как попросить присылать чаще и подробнее, пока на узел
смотрят, и где всё это видно в бэкенде. Справочник API — [sdk/README.md](../README.md), формат —
[sdk/spec §6.2](../spec/README.md#62-статус-и-метрики),
[§6.8](../spec/README.md#68-лог-агента-и-воркеров-log),
[§6.9](../spec/README.md#69-подписка-сервера), образцы —
[observe.json](../spec/examples/observe.json), [connection.json](../spec/examples/connection.json)
(`welcome.subscription`, `config.subscription`).

- [Статус агента](#статус-агента)
- [Метрики узла](#метрики-узла)
- [Метрики без связи и время точки](#метрики-без-связи-и-время-точки)
- [Подписки](#подписки)
- [Показатели воркеров](#показатели-воркеров)
- [Лог агента и воркеров](#лог-агента-и-воркеров)
- [Сведения об узле](#сведения-об-узле)
- [История метрик](#история-метрик)
- [Событие metrics](#событие-metrics)

## Статус агента

**Бэкенд.**

```ts
const agent = await agents.getAgent(agentId);
agent?.status; // { state, message?, slots, capacity, jobs, workers, outbox }
```

```go
agent, err := agents.Agent(agentID) // agent.Status *message.Status
```

```python
agent = await agents.get_agent(agent_id)  # agent.status — dict
```

Частота — `statusIntervalMs` (по умолчанию 5000; в Go — `StatusInterval`), чаще — по
[подписке](#подписки).

**Что уходит по сети.** `status` — при каждом изменении и не реже `statusIntervalMs`:

| Поле       | Что                                                                                                           |
| ---------- | ------------------------------------------------------------------------------------------------------------- |
| `state`    | `starting`, `idle`, `busy`, `draining` (дорабатывает, новое не берёт), `updating`, `degraded` (что-то не так) |
| `message`  | почему `degraded`: воркеры перезапускаются, воркер сообщил о неполадке                                        |
| `slots`    | очередь → сколько задач ещё можно взять                                                                       |
| `capacity` | очередь → сколько всего                                                                                       |
| `jobs`     | `[{jobId, attempt, queue, startedAt}]` — задачи в работе (продлевают их срок)                                 |
| `workers`  | `[{name, state, instances, version, release?, health?, message?, paused?}]`                                   |
| `outbox`   | сколько важных сообщений ждут подтверждения                                                                   |

**Агент** (`internal/runtime`) собирает статус из задач, воркеров и очереди на диске;
несколько изменений подряд — один `status`.

**Воркер** влияет на статус через места очередей, [здоровье](workers.md#здоровье) и
[паузу](workers.md#пауза-очередей).

**Результат.** `agent.status`; событие `change` `{kind: "agent"}` — только если статус
изменился (одинаковый «пульс» уведомления не вызывает). По статусу приходят уведомления
`degraded`, `workerDown`, `workerDegraded` ([events.md](events.md#уведомления-о-проблемах)).

## Метрики узла

**Бэкенд** задаёт частоту: `metricsIntervalMs` (по умолчанию 15000; в Go — `MetricsInterval`).
Какие группы собирать — решает узел, бэкенд может добавить группы [подпиской](#подписки).

**Что уходит по сети.** `metrics {collectedAt, clockOffsetMs, backfill?, host, gpus, channels}`
раз в `metricsIntervalMs`. В `host` — поля включённых групп (нет группы — нет полей):

| Группа         | Поля `metrics.host`                                                                                                |
| -------------- | ------------------------------------------------------------------------------------------------------------------ |
| `cpu`          | `cpuPercent`                                                                                                       |
| `cpu.cores`    | `cpuCores: [процент по ядрам]`                                                                                     |
| `load`         | `load1`, `load5`, `load15`                                                                                         |
| `memory`       | `memUsedBytes`, `memTotalBytes`, `memAvailableBytes`                                                               |
| `swap`         | `swapUsedBytes`, `swapTotalBytes`                                                                                  |
| `disk`         | `diskUsedBytes`, `diskTotalBytes` (корень `/`), `disks: [{mount, usedBytes, totalBytes, inodesUsed, inodesTotal}]` |
| `diskio`       | `diskReadBps`, `diskWriteBps`, `diskReadIops`, `diskWriteIops` (сумма по физическим дискам)                        |
| `network`      | `netRxBps`, `netTxBps`, `netErrors`, `netDrops` (за интервал)                                                      |
| `interfaces`   | `interfaces: [{name, rxBps, txBps, errors, drops}]`                                                                |
| `conntrack`    | `conntrack`, `conntrackMax` (Linux)                                                                                |
| `sockets`      | `tcp: {established, timeWait, closeWait, listen}`                                                                  |
| `processes`    | `processes`, `threads`                                                                                             |
| `fds`          | `fdsOpen`, `fdsMax` (Linux)                                                                                        |
| `uptime`       | `uptimeSec`                                                                                                        |
| `temperatures` | `temperatures: {maxC, sensors: [{name, c}]}` (где есть датчики)                                                    |

Видеокарты — отдельно, `gpus: [{index, name, utilPercent, memUsedBytes, memTotalBytes,
temperatureC}]`. Показатели воркеров — `channels` ([ниже](#показатели-воркеров)).

**Агент** (`internal/telemetry`, настройки — [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md#настройки)):

```yaml
telemetry:
  metrics: [cpu, load, memory, swap, disk, network, interfaces, conntrack, uptime] # по умолчанию; [] — без метрик узла
  disks: ["/", "/data"] # для metrics.host.disks; ["all"] — все реальные файловые системы
  gpu: auto # auto — через nvidia-smi, если он есть; off
  excludeInterfaces: [lo, veth, docker, br-] # префиксы интерфейсов, которые не показывать
```

Неизвестная группа — ошибка настроек. Всё это перечитывается на ходу (`systemctl reload
agent`). Скорости считаются по разнице с прошлым сбором.

**Воркер** не участвует (кроме своих показателей).

**Результат.** `agent.metrics` — последняя точка (без досланных); каждая точка — событие
[`metrics`](#событие-metrics); история — [listMetrics](#история-метрик).

## Метрики без связи и время точки

**Бэкенд** ничего не вызывает: `Agents` сам ставит каждой точке время по часам сервера.

**Что уходит по сети.** `clockOffsetMs` — насколько часы сервера впереди часов агента (агент
считает по `welcome.serverTime`); `backfill: true` — точка собрана без связи и дослана позже.

**Агент** (`internal/runtime`): без связи продолжает собирать метрики — до `telemetry.backlog`
точек в памяти (по умолчанию 720; `0` — не копить), после `welcome` досылает их по порядку с
`backfill: true`. Перезапуск агента накопленное теряет.

**Воркер** не участвует.

**Результат.** `MetricsPoint {at, backfill, metrics}`: `at = collectedAt + clockOffsetMs` (без
смещения — время получения). Досланные точки попадают в историю и событие `metrics`, но не
меняют `agent.metrics` — по ним не принимают решений «здесь и сейчас».

## Подписки

Подписка — «присылай это, так часто, столько времени»: например, пока человек смотрит на узел в
интерфейсе. Подписчиков у агента может быть несколько, у каждой подписки свой id и срок.

**Бэкенд.**

```ts
const sub = await agents.subscribe(agentId, {
  ttlMs: 30_000, // срок (30 с); продлить — вызвать снова с тем же id
  status: { intervalMs: 1000 },
  metrics: { intervalMs: 1000, groups: ["diskio", "sockets", "temperatures"] },
  logs: { level: "debug" },
  channels: { "example.report": { intervalMs: 1000 } },
}); // → { id, until }

await agents.subscribe(agentId, { id: sub.id, ttlMs: 30_000, metrics: { intervalMs: 1000 } }); // продлить и заменить
await agents.unsubscribe(agentId, sub.id);
```

```go
sub, err := agents.Subscribe(agentID, server.SubscribeRequest{
	TTL:      30 * time.Second,
	Status:   &server.IntervalSpec{IntervalMs: 1000},
	Metrics:  &server.MetricsSpec{IntervalMs: 1000, Groups: []string{"diskio", "sockets"}},
	Logs:     &server.LogsSpec{Level: "debug"},
	Channels: map[string]server.IntervalSpec{"example.report": {IntervalMs: 1000}},
}) // sub.ID, sub.Until
err = agents.Unsubscribe(agentID, sub.ID)
```

```python
sub = await agents.subscribe(agent_id, ttl_ms=30000,
                             status={"intervalMs": 1000},
                             metrics={"intervalMs": 1000, "groups": ["diskio", "sockets"]},
                             logs={"level": "debug"},
                             channels={"example.report": {"intervalMs": 1000}})   # {"id", "until"}
await agents.unsubscribe(agent_id, sub["id"])
```

- все части необязательны: нет части — подписка её не касается;
- интервалы — не меньше 200 мс; группы — по форме имени (`^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$`),
  незнакомые агент пропустит; уровень — `debug`, `info`, `warn`, `error`; каналы — по правилу
  имён. Иначе — `MESSAGE_INVALID`; агента нет — `AGENT_NOT_FOUND`; отозван — `AGENT_REVOKED`;
- снять несуществующую подписку — не ошибка;
- подписки хранятся в записи агента (`agent.subscriptions`) и видны всем процессам бэкенда:
  агенту на связи с другим процессом их доставит `refresh`
  ([connection.md](connection.md#несколько-процессов-бэкенда)); истёкшие удаляет сверка (раз в
  секунду); `revoke` удаляет все подписки агента.

Типичное использование — продлевать подписку, пока открыта страница узла:

```ts
const id = `ui:${sessionId}:${agentId}`;
const keep = setInterval(
  () => void agents.subscribe(agentId, { id, ttlMs: 30_000, metrics: { intervalMs: 1000 } }),
  10_000,
);
// страница закрыта:
clearInterval(keep);
await agents.unsubscribe(agentId, id);
```

**Что уходит по сети.** Агенту — одна **сводная подписка** по всем действующим
([§6.9](../spec/README.md#69-подписка-сервера)):

```json
{
  "statusIntervalMs": 1000,
  "metricsIntervalMs": 1000,
  "metrics": ["diskio", "sockets"],
  "logLevel": "debug",
  "channels": { "example.report": 1000 }
}
```

- интервалы — наименьшие из подписок, группы — объединение без повторов, уровень лога — самый
  подробный, каналы — наименьший интервал по каждому;
- в `welcome.config.subscription` — текущая сводная (подписок нет — поля нет);
- изменилась (подписку добавили, сняли, она истекла) — `config {subscription}` целиком;
  `config {subscription: {}}` — подписок больше нет. Одинаковую повторно сервер не шлёт.

**Агент** (`internal/runtime`, `internal/telemetry`, `internal/logx`) — подписка только
ужесточает его настройки:

- статус — не реже меньшего из `statusIntervalMs` и подписки;
- метрики — не реже меньшего из `metricsIntervalMs`, `metricsIntervalMs` подписки и **всех**
  интервалов `channels` (показатели воркеров уходят в `metrics`);
- группы метрик — `telemetry.metrics` плюс группы подписки;
- лог — более подробный уровень из `log.forward` и `logLevel`;
- воркерам — новый `worker.context` (`metricsIntervalMs`, `channels`, `logLevel`).

Новая сессия начинается без подписки: агент берёт ту, что пришла в `welcome`.

**Воркер** узнаёт о подписке из [контекста](workers.md#контекст-воркера) и может собирать
показатели с её частотой ([ниже](#показатели-воркеров)).

**Результат.** `agent.subscriptions: [{id, until, status?, metrics?, logs?, channels?}]`; чаще
приходят события `metrics` и `log`, в точках — группы подписки.

## Показатели воркеров

Показатели — цифры воркера, которые уходят вместе с метриками узла: длина очереди, число
подключений. Что внутри — решает воркер.

**Бэкенд** читает их из точки метрик и может ускорить подпиской на канал:

```ts
agents.on("metrics", (agentId, point) => chart(agentId, point.at, point.metrics.channels?.["example.report"]));
await agents.subscribe(agentId, { channels: { "example.report": { intervalMs: 1000 } } });
```

```go
server.Options{OnMetrics: func(agentID string, p server.MetricsPoint) {
	raw := p.Metrics.Channels["example.report"] // json.RawMessage
	_ = raw
}}
```

```python
agents.on("metrics", lambda agent_id, point: chart(agent_id, point.metrics.get("channels", {}).get("example.report")))
```

**Что уходит по сети.** На узле — `telemetry {channel, data}`; серверу — `metrics.channels:
{канал: data}`; канал объявлен в `capabilities.telemetry.channels`.

**Агент** (`internal/telemetry`) хранит **последние** данные каждого канала и кладёт их в
ближайшую точку `metrics`. Воркер остановился — его показатели пропадают из `metrics`, пока он не
пришлёт новые.

**Воркер** — два способа:

```python
@worker.telemetry("example.report", interval="auto")   # агент спрашивает сам; секунды или "auto"
def stats() -> dict:
    return {"queue": queue_len()}

worker.channel("example.events")                      # шлёт сам, когда хочет
worker.report("example.events", {"connections": 42})
```

```go
w.Telemetry("example.report", worker.AutoInterval, func() any { return map[string]int{"queue": queueLen()} })
w.Channel("example.events")
_ = w.Report("example.events", map[string]int{"connections": 42})
```

```ts
worker.telemetry("example.report", { intervalMs: AUTO_INTERVAL }, () => ({ queue: queueLen() })); // "auto"
worker.channel("example.events");
worker.report("example.events", { connections: 42 });
```

**Авто-интервал** (`"auto"`, `worker.AutoInterval`, `AUTO_INTERVAL`) — частота подписки сервера
на этот канал (`context.channels`), если она есть; иначе действующая частота метрик агента
(`context.metricsIntervalMs`); пока неизвестна — 15 с. Меняется на ходу вместе с контекстом.
Явный интервал: Python — секунды (`interval=15`), Go — `time.Duration`, Node — `intervalMs`
(по умолчанию 15000). Источник опрашивается после регистрации; ошибка — в лог, воркер не
падает; в Go источник может вернуть `nil` — тогда пропуск.

**Результат.** `point.metrics.channels["example.report"]`, `agent.metrics.channels`.

## Лог агента и воркеров

**Бэкенд** получает записи событием — SDK их не хранит:

```ts
agents.on("log", (agentId, entries) => saveLogs(agentId, entries)); // [{ at, level, source, msg, attrs? }]
await agents.subscribe(agentId, { logs: { level: "debug" } }); // подробнее на время
```

```go
server.Options{OnLog: func(agentID string, entries []message.LogEntry) { saveLogs(agentID, entries) }}
```

```python
agents.on("log", lambda agent_id, entries: save_logs(agent_id, entries))
```

Последние строки по запросу — встроенная команда `agent.logs`
([commands.md](commands.md#встроенные-команды-агента)).

**Что уходит по сети.** `log {entries: [{at, level, source, msg, attrs?}]}` — `level`: `debug` |
`info` | `warn` | `error`; `source` — `agent` или имя воркера.

**Агент** (`internal/logx`):

- отправляет записи своего лога и вывод воркеров от уровня `log.forward` (`off`, `error`, `warn`,
  `info`, `debug`; по умолчанию `warn`), подписка может временно сделать подробнее;
- пачками не чаще раза в секунду, до 500 записей; буфер — 1000 записей, лишние отбрасываются с
  записью «пропущено N»;
- без связи записи не копятся.

**Воркер** пишет как обычно — в stdout/stderr (лог SDK — тоже в stderr); агент добавляет строки
в свой лог с именем воркера. Действующий уровень — `context.logLevel`.

**Результат.** Событие `log` с пачкой записей.

## Сведения об узле

**Бэкенд.**

```ts
const agent = await agents.getAgent(agentId);
agent?.inventory; // { collectedAt, os, cpu, memoryBytes, disks, interfaces, gpus, ports }
```

```go
agent, _ := agents.Agent(agentID) // agent.Inventory
```

```python
agent = await agents.get_agent(agent_id)  # agent.inventory
```

**Что уходит по сети.** `inventory` — ОС (`hostname`, `platform`, `kernel`, `arch`,
`virtualization`), процессор (`model`, `cores`, `threads`), память, диски (`mount`, `fs`,
`totalBytes`), сетевые интерфейсы (`name`, `mac`, `addresses`), видеокарты, открытые порты
(`ports: {tcp, udp}`). Ещё — краткие сведения в `hello.host`.

**Агент** (`internal/telemetry/inventory.go`) присылает сведения после каждого подключения и
при изменении; проверяет раз в `telemetry.inventoryInterval` (по умолчанию 10 минут; `0` — не
присылать вовсе).

**Воркер** не участвует.

**Результат.** `agent.inventory` (последние присланные), `agent.hello.host`; событие `change`
`{kind: "agent"}`.

## История метрик

**Бэкенд.**

```ts
const points = await agents.listMetrics(agentId, { since: Date.now() - 3600_000 }); // по возрастанию at
new Agents({ enrollToken, metricsStoreIntervalMs: 15_000, metricsRetentionMs: 7 * 24 * 3600_000 });
```

```go
points, err := agents.Metrics(agentID, time.Now().Add(-time.Hour).UnixMilli())
server.New(server.Options{EnrollToken: token, MetricsStoreInterval: 15 * time.Second, MetricsRetention: 7 * 24 * time.Hour})
```

```python
points = await agents.list_metrics(agent_id, since=now_ms() - 3600_000)
Agents(enroll_token=token, metrics_store_interval_ms=15000, metrics_retention_ms=7 * 24 * 3600_000)
```

- `since` — только точки строго позже;
- **прореживание**: в `Store` точка сохраняется не чаще раза в `metricsStoreIntervalMs` (15 с)
  на агента — иначе при частых метриках по подписке история быстро разрастается. Досланные
  точки прореживаются отдельно от живых. Событие `metrics` и `agent.metrics` получают **каждую**
  точку;
- **срок хранения**: точки старше `metricsRetentionMs` (7 суток) удаляются при запуске и раз в
  час (`Store.pruneMetrics`);
- `0` в Node и Python — «каждую точку» и «хранить всегда»; в Go — отрицательное значение (ноль
  — значение по умолчанию);
- `MemoryStore` держит ещё не больше 4320 точек на агента.

**Что уходит по сети** — обычные `metrics`.

**Агент** и **воркер** не участвуют.

**Результат.** `MetricsPoint[]`: `at` (мс, часы сервера), `backfill`, `metrics` (сообщение
целиком: узел, видеокарты, показатели воркеров).

## Событие metrics

Для живых графиков каждая принятая точка — отдельное событие, включая досланные и не
сохранённые в историю прореживанием.

```ts
agents.on("metrics", (agentId, point) => ws.broadcast({ agentId, at: point.at, cpu: point.metrics.host?.cpuPercent }));
```

```go
server.Options{OnMetrics: func(agentID string, p server.MetricsPoint) { broadcast(agentID, p) }}
```

```python
agents.on("metrics", lambda agent_id, point: broadcast(agent_id, point.to_dict()))
```

Обработчик не должен блокировать: вызывается после обработки сообщения (в Go — вне блокировки
`Agents`, из него можно вызывать методы `Agents`).
