# API сервера стенда

Небольшой HTTP API сервера стенда [server](server) (Node, `agent-sdk/server`) — для сквозных
тестов и ручной работы (`curl`). Всё, что связано с агентами, делает `Agents` из SDK; здесь —
только тонкий слой над ним. Авторизации нет: это пример для запуска у себя. Кто действует —
заголовок `X-Actor` (по умолчанию `api`): он попадает в записи настроек, итоги действий и в аудит.
История — события воркеров, точки метрик, итоги действий — хранится в памяти сервера стенда
(последние 10 000 событий, 2000 точек и 200 действий на агента) и пропадает при его перезапуске.

Ошибки — `{ code, message }` со статусом из `AgentsError` ([sdk/README.md](../sdk/README.md#ошибки));
нет агента — `404 AGENT_NOT_FOUND`, нет такого маршрута API — `404 NOT_FOUND`.

| Метод  | Путь                                                 | Что                                                                                                                                                                                                                    |
| ------ | ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| GET    | `/api/agents`                                        | `Agent[]`                                                                                                                                                                                                              |
| GET    | `/api/agents/:id`                                    | `Agent`; манифест воркера — `workers[].manifest`                                                                                                                                                                       |
| DELETE | `/api/agents/:id`                                    | удалить запись агента и всё его → `{}`                                                                                                                                                                                 |
| POST   | `/api/agents/:id/revoke`                             | отозвать → `Agent`                                                                                                                                                                                                     |
| POST   | `/api/agents/:id/rotate-key`                         | сменить ключ (`agent.rotateKey`) → `{}`                                                                                                                                                                                |
| POST   | `/api/agents/:id/update`                             | обновить агента до новой версии (`agent.update`) → `{ version, previous }`                                                                                                                                             |
| GET    | `/api/agents/:id/actions?limit=`                     | итоги встроенных действий агента, новые первыми (`limit` — по умолчанию 50) → `ActionRecord[]`                                                                                                                         |
| GET    | `/api/agents/:id/configs`                            | `{ configs: ConfigRecord[], status: ConfigStatus[] }` — значения и статус применения                                                                                                                                   |
| PUT    | `/api/agents/:id/configs/:worker/:key`               | задать: тело — значение (любой JSON) → `ConfigRecord` с новой `version`                                                                                                                                                |
| DELETE | `/api/agents/:id/configs/:worker/:key`               | удалить ключ на сервере и у агента → `{ deleted }`                                                                                                                                                                     |
| GET    | `/api/agents/:id/metrics?since=&limit=`              | история метрик `MetricsPoint[]` по времени; `since` — строго позже, мс; `limit` — последние                                                                                                                            |
| GET    | `/api/agents/:id/logs?worker=&lines=`                | последние строки журнала агента или воркера с узла (`agent.logs`) → `LogEntry[]`                                                                                                                                       |
| GET    | `/api/agents/:id/watch?logLevel=`                    | поток Server-Sent Events, см. ниже                                                                                                                                                                                     |
| \*     | `/api/agents/:id/workers/:worker/fetch/<путь>?<…>`   | запрос к воркеру, см. ниже                                                                                                                                                                                             |
| POST   | `/api/agents/:id/workers/:worker/restart`            | перезапустить воркер (`worker.restart`) → `{ deferred: false }`; воркер занят — сразу `{ deferred: true, pending, actionId }`, итог — в `…/actions`; тело `{ force: true }` — не ждать, `{ wait: true }` — ждать итога |
| POST   | `/api/agents/:id/workers/:worker/update`             | обновить воркер сборкой с сервера (`worker.update`) → `{ version, previous, deferred: false }` или, как выше, `{ deferred: true, … }`; тело — так же                                                                   |
| POST   | `/api/agents/:id/workers/:worker/jobs`               | задача воркеру (`runJob`): тело `{ type, jobId?, data?, files?, timeoutMs? }` → `{ jobId, id?, state, progress?, result?, error? }`                                                                                    |
| GET    | `/api/agents/:id/workers/:worker/jobs/:jobId`        | состояние задачи (`jobStatus`) → `{ id, state, progress?, result?, error? }`                                                                                                                                           |
| POST   | `/api/agents/:id/workers/:worker/jobs/:jobId/cancel` | прервать задачу (`cancelJob`) → `{ id, state }`                                                                                                                                                                        |
| GET    | `/api/agents/:id/workers/:worker/supports?…`         | есть ли в манифесте воркера маршрут (`method`, `path`), ключ (`config`), событие (`event`) → `{ supported }`                                                                                                           |
| GET    | `/api/agents/:id/workers/:worker/capabilities`       | что умеет воркер (`capabilities`) → `{ version, configs, routes, events, jobs, requests }` со схемами; воркер себя не описал — `404 WORKER_UNKNOWN`                                                                    |
| GET    | `/api/events?agentId=&worker=&type=&before=&limit=`  | события воркеров, новые первыми (`limit` — по умолчанию 100); `before` — `receivedAt` последнего на странице                                                                                                           |
| GET    | `/api/alerts?agentId=`                               | текущие проблемы `Alert[]`                                                                                                                                                                                             |
| GET    | `/api/releases`                                      | `{ release, candidates, workerCandidates, installCommand }` — сборки на сервере, кого можно обновить, команда установки                                                                                                |

**Манифест.** Воркеры-примеры описывают себя (`GET /manifest`): агент передаёт манифест в
`status`, и он виден в `GET /api/agents/:id` — `workers[].manifest` с версией, ключами настроек,
маршрутами, событиями, задачами и запросами к бэкенду со схемами; то же по одному воркеру —
`…/capabilities`. С `VALIDATE_CONFIGS=1` сервер стенда проверяет значение
`PUT …/configs/:worker/:key` по схеме ключа до отправки агенту: не подходит — `400
CONFIG_INVALID` с замечаниями. `VALIDATE_REQUESTS=1` — так же тело запроса к воркеру
(`400 REQUEST_INVALID`) и `data` запросов воркеров; `VALIDATE_EVENTS=log` или `reject` — `data`
событий.

```bash
curl localhost:8080/api/agents/<id> | jq '.workers[] | {name, manifest}'
curl 'localhost:8080/api/agents/<id>/workers/echo/supports?method=POST&path=/echo'
curl localhost:8080/api/agents/<id>/workers/echo/capabilities | jq .requests
```

**Запрос к воркеру** передаётся как есть: метод, `<путь>` с параметрами, тело и `Content-Type`.
Агент пропускает к воркеру только маршруты из его манифеста: другой метод или путь —
`404 ROUTE_UNDECLARED`, тип задачи в `fetch/jobs` не из манифеста — `409 JOB_UNKNOWN`;
срок — заголовок `X-Timeout-Ms` (по умолчанию 30 с). Ответ воркера — статус, заголовки и тело —
приходит потоком, по мере того как воркер его отдаёт; закрыли запрос — запрос к воркеру
отменяется. Ошибка до ответа воркера — статус и код из [sdk/docs/fetch.md](../sdk/docs/fetch.md#ошибки):
`PATH_FORBIDDEN` (служебный путь), `ROUTE_UNDECLARED`, `JOB_UNKNOWN`, `WORKER_UNKNOWN`,
`WORKER_UNAVAILABLE`, `WORKER_INVALID`, `TIMEOUT`, `BODY_TOO_LARGE`, `BUSY`, `AGENT_OFFLINE`.

```bash
curl -X POST localhost:8080/api/agents/<id>/workers/echo/fetch/echo -d '{"text":"привет"}'
curl -N localhost:8080/api/agents/<id>/workers/echo/fetch/stream?n=5
```

**Задачи** ([sdk/docs/workers.md](../sdk/docs/workers.md#задачи)): тип — из манифеста воркера
(нет — `409 JOB_UNKNOWN`); долгая задача, не закончившаяся за `timeoutMs` (по умолчанию 30 с), —
`state: running`, её события `job.*` — в `/api/events`.

```bash
curl -X POST localhost:8080/api/agents/<id>/workers/echo/jobs -d '{"type":"echo.quick","data":{"text":"привет"}}'
curl -X POST localhost:8080/api/agents/<id>/workers/echo/jobs -d '{"type":"echo.long","data":{"steps":10},"timeoutMs":1}'
curl localhost:8080/api/agents/<id>/workers/echo/jobs/<id задачи>
```

**Запросы воркеров к бэкенду** ([sdk/docs/workers.md](../sdk/docs/workers.md#запросы-к-бэкенду)):
сервер стенда отвечает на `echo.lookup` префиксом `<имя агента>: `. Его спрашивают `echo` и
`node-echo` в задаче `echo.quick` с `lookup: true`; на другие запросы — отказ
`REQUEST_UNHANDLED`.

```bash
curl -X POST localhost:8080/api/agents/<id>/workers/echo/jobs -d '{"type":"echo.quick","data":{"text":"привет","lookup":true}}'
```

**Поток `watch`** (`text/event-stream`): пока он открыт, сервер держит наблюдателя
`agents.watch` — агент присылает метрики раз в секунду и журнал с уровня `logLevel` (по
умолчанию `info`); закрыли поток — наблюдатель снимается, агент возвращается к обычной частоте.
События: `metrics` (`MetricsEvent`), `log` (`LogEntry[]`), `event` (`AgentEvent`) этого агента.

```bash
curl -N 'localhost:8080/api/agents/<id>/watch?logLevel=debug'
```
