# API бэкендов стенда

У обоих бэкендов стенда одно и то же API: [server](server) (Node, `agent-sdk/server`) и
[server-python](server-python) (Python, `agent_sdk.server`). На нём работают веб-интерфейс
[web](web) и самопроверка. Всё, что связано с агентами, делает `Agents` из SDK. Здесь — только то,
что бэкенд отдаёт своему интерфейсу. Авторизации нет: это пример для запуска у себя.

## REST

| Метод  | Путь                                         | Ответ                                                                                                         |
| ------ | -------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| GET    | `/api/snapshot`                              | всё сразу: `{serverTime, agents, jobs, commands, states, events}` (у задач — `files`)                         |
| POST   | `/api/jobs`                                  | поставить задачу `{queue, data, maxAttempts, leaseSeconds, inputs, outputs, agentId}` → Job                   |
| GET    | `/api/jobs/:id`                              | Job                                                                                                           |
| POST   | `/api/jobs/:id/cancel`, `/api/jobs/:id/stop` | отменить / закончить пораньше → Job                                                                           |
| POST   | `/api/commands`                              | команда `{name, args, timeoutSec, agentId}` → Command                                                         |
| GET    | `/api/commands/:id`                          | Command                                                                                                       |
| PUT    | `/api/state/:domain?agentId=`                | задать состояние (тело — снимок); без `agentId` — всем → DesiredState                                         |
| DELETE | `/api/state/:domain?agentId=`                | с `agentId` — вернуть агента на общее состояние; без — удалить общий снимок → `{state: DesiredState \| null}` |
| GET    | `/api/state/:domain/history?agentId=&limit=` | история версий раздела, от новых к старым → DesiredState[]                                                    |
| POST   | `/api/state/:domain/rollback`                | откат: тело `{version, agentId?}` — тот же снимок под новой версией → DesiredState                            |
| GET    | `/api/agents/:id/metrics?since=<мс>`         | история метрик MetricsPoint[] по времени; `since` — строго позже                                              |
| POST   | `/api/agents/:id/subscriptions`              | подписка на агента (тело — как у `agents.subscribe`, ниже) → `{id, until}`                                    |
| DELETE | `/api/agents/:id/subscriptions/:subId`       | снять подписку → `{}`                                                                                         |
| POST   | `/api/agents/:id/revoke`                     | отозвать агента → Agent                                                                                       |
| POST   | `/api/agents/:id/update`                     | обновить агента → Command (`agent.update`)                                                                    |
| POST   | `/api/agents/:id/rotate-key`                 | сменить ключ агента → Command (`agent.rotateKey`)                                                             |
| POST   | `/api/agents/:id/workers/:name/update`       | обновить воркер из выпуска → Command (`worker.update`)                                                        |
| POST   | `/api/agents/:id/workers/:name/pause`        | пауза воркера с сервера, тело `{queues?}` (без `queues` — все очереди) → Command (`worker.pause`)             |
| POST   | `/api/agents/:id/workers/:name/resume`       | снять паузу сервера, тело `{queues?}` → Command (`worker.resume`)                                             |
| GET    | `/api/releases`                              | `{release, candidates, installCommand, workerCandidates}` — выпуск, кого можно обновить, команда установки    |

Кто делает изменение, бэкенд берёт из заголовка `X-Actor` (нет — `web`) и пишет в журнал аудита.
В снимке `GET /api/snapshot` есть ещё `alerts` — активные проблемы агентов (среди них
`workerDegraded` — воркер сообщил, что он не в порядке).

**Подписка** — «присылай это, так часто, столько времени». Тело `POST …/subscriptions`:

```jsonc
{
  "id": "my-panel", // нет — сервер создаст; тот же id — продлить срок и заменить содержимое
  "ttlMs": 30000, // срок, по умолчанию 30 с; чтобы подписка жила дольше, её продлевают
  "status": { "intervalMs": 1000 },
  "metrics": { "intervalMs": 1000, "groups": ["diskio", "sockets"] }, // группы метрик узла сверх настройки агента
  "logs": { "level": "debug" }, // debug | info | warn | error
  "channels": { "example.app": { "intervalMs": 1000 } }, // показатели воркеров
}
```

Все части необязательные. Интервал меньше 200 мс, неверное имя группы или канала, неизвестный
уровень — ошибка `MESSAGE_INVALID`. Подписки агента видны в его записи (`subscriptions`).

Ошибки — `{code, message}` с HTTP-статусом: `404 *_NOT_FOUND`, `409 UPDATE_NOT_AVAILABLE` и т. д.

## Изменения вживую — WebSocket `/api/ws`

Сервер сам присылает изменения, а интерфейс подписывается на тех агентов, которых показывает
подробно. Одно сообщение WebSocket — один JSON `{type, …}`.

**Сервер → интерфейс:**

| `type`         | Поля                              | Когда                                                                            |
| -------------- | --------------------------------- | -------------------------------------------------------------------------------- |
| `snapshot`     | `data` — как `GET /api/snapshot`  | сразу после подключения                                                          |
| `agent`        | `data` — Agent                    | агент изменился: связь, статус, метрики, сведения об узле, состояние             |
| `job`          | `data` — Job (с `files`)          | задача изменилась                                                                |
| `command`      | `data` — Command                  | команда изменилась                                                               |
| `state`        | `data` — DesiredState             | состояние изменилось                                                             |
| `stateDeleted` | `domain`, `agentId` (нет — общий) | снимок состояния удалён                                                          |
| `alert`        | `data` — Alert                    | проблема началась (`active: true`) или закончилась (`false`)                     |
| `audit`        | `data` — AuditEntry               | кто-то что-то изменил                                                            |
| `event`        | `data` — AgentEvent               | новое событие                                                                    |
| `metrics`      | `agentId`, `point` — MetricsPoint | новая точка метрик — **только если вы подписаны на агента**                      |
| `log`          | `agentId`, `entries`              | записи лога агента и воркеров — **только для подписанных**, от выбранного уровня |

**Интерфейс → сервер:**

| `type`        | Поля                   | Что делает                                                                                                                |
| ------------- | ---------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `subscribe`   | `agentId`, `logLevel?` | присылать точки метрик и лог агента; `logLevel` — с какого уровня (`debug`, `info`, `warn`, `error`; по умолчанию `info`) |
| `logLevel`    | `agentId`, `level`     | поменять уровень лога для подписки                                                                                        |
| `unsubscribe` | `agentId`              | перестать присылать                                                                                                       |

Пока клиент подписан на агента, бэкенд держит за него подписку `agents.subscribe` (id — соединение
и агент) и продлевает её каждые 20 с:

- метрики раз в секунду и группы `diskio`, `sockets`, `processes`, `temperatures` сверх настройки
  агента;
- показатели всех воркеров агента раз в секунду;
- лог с уровня клиента.

`unsubscribe` или закрытие соединения снимают подписку, и агент возвращается к своим настройкам.

История для графика — `GET /api/agents/:id/metrics?since=` при открытии, дальше точки идут по
WebSocket. Досланные точки помечены `backfill` и могут прийти не по порядку. Подписки живут,
пока открыто соединение. После переподключения приходит новый `snapshot`, и подписываться нужно
заново.
