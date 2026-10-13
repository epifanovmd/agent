# API сервера стенда

Небольшой HTTP API сервера стенда [server](server) (Node, `agent-sdk/server`) — для сквозных
тестов и ручной работы (`curl`). Всё, что связано с агентами, делает `Agents` из SDK; здесь —
только тонкий слой над ним. Авторизации нет: это пример для запуска у себя. История — события воркеров, точки метрик, итоги действий — хранится в памяти сервера стенда
(последние 10 000 событий, 2000 точек и 200 действий на агента) и пропадает при его перезапуске.

Ошибки — `{ code, message }` со статусом из `AgentsError` ([sdk/README.md](../sdk/README.md#ошибки));
нет агента — `404 AGENT_NOT_FOUND`, нет такого маршрута API — `404 NOT_FOUND`.

| Метод  | Путь                                                | Что                                                                                                                                                                                                                    |
| ------ | --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| GET    | `/api/agents`                                       | `Agent[]`                                                                                                                                                                                                              |
| GET    | `/api/agents/:id`                                   | `Agent`; манифест воркера — `workers[].manifest`                                                                                                                                                                       |
| DELETE | `/api/agents/:id`                                   | удалить запись агента и всё его → `{}`                                                                                                                                                                                 |
| POST   | `/api/agents/:id/revoke`                            | отозвать → `Agent`                                                                                                                                                                                                     |
| POST   | `/api/agents/:id/rotate-key`                        | сменить ключ (`agent.rotateKey`) → `{}`                                                                                                                                                                                |
| POST   | `/api/agents/:id/update`                            | обновить агента (`agent.update`): тело `{ version }`, без него — до новой версии, которую агент нашёл в своём каталоге сборок (`update.latest`; нет — `409 UPDATE_NOT_AVAILABLE`) → `{ version, previous }`            |
| GET    | `/api/agents/:id/actions?limit=`                    | итоги встроенных действий агента, новые первыми (`limit` — по умолчанию 50) → `ActionRecord[]`                                                                                                                         |
| GET    | `/api/agents/:id/configs`                           | `{ configs: ConfigRecord[], status: ConfigStatus[] }` — значения и статус применения                                                                                                                                   |
| PUT    | `/api/agents/:id/configs/:worker/:key`              | задать: тело — значение (любой JSON) → `ConfigRecord` с новой `version`                                                                                                                                                |
| DELETE | `/api/agents/:id/configs/:worker/:key`              | удалить ключ на сервере и у агента → `{ deleted }`                                                                                                                                                                     |
| GET    | `/api/agents/:id/metrics?since=&limit=`             | история метрик `MetricsPoint[]` по времени; `since` — строго позже, мс; `limit` — последние                                                                                                                            |
| GET    | `/api/agents/:id/logs?worker=&lines=`               | последние строки журнала агента или воркера с узла (`agent.logs`) → `LogEntry[]`                                                                                                                                       |
| GET    | `/api/agents/:id/watch?logLevel=`                   | поток Server-Sent Events, см. ниже                                                                                                                                                                                     |
| \*     | `/api/agents/:id/workers/:worker/fetch/<путь>?<…>`  | запрос к воркеру, см. ниже                                                                                                                                                                                             |
| POST   | `/api/agents/:id/workers/:worker/restart`           | перезапустить воркер (`worker.restart`) → `{ deferred: false }`; воркер занят — сразу `{ deferred: true, pending, actionId }`, итог — в `…/actions`; тело `{ force: true }` — не ждать, `{ wait: true }` — ждать итога |
| POST   | `/api/agents/:id/workers/:worker/update`            | обновить воркер сборкой с сервера (`worker.update`) → `{ version, previous, deferred: false }` или, как выше, `{ deferred: true, … }`; тело — так же                                                                   |
| GET    | `/api/agents/:id/workers/:worker/manifest`          | манифест воркера (`workerManifest`): версия, ключи настроек, маршруты, события, задачи и запросы со схемами; воркер себя не описал — `404 WORKER_UNKNOWN`                                                              |
| GET    | `/api/events?agentId=&worker=&type=&before=&limit=` | события воркеров, новые первыми (`limit` — по умолчанию 100); `before` — `receivedAt` последнего на странице                                                                                                           |
| GET    | `/api/releases`                                     | `{ release, installCommand }` — сборки воркеров на сервере (`release()`), команда установки: `curl … <сервер>/install.sh \| sudo sh -s -- --server … --token … [--update-key …]`                                       |
| GET    | `/install.sh`                                       | установщик агента из `RELEASES_DIR` (`install.sh` релиза агента); нет каталога или файла — `404`                                                                                                                       |

**Манифест.** Воркеры-примеры описывают себя (`GET /manifest`): агент передаёт манифест в
`status`, и он виден в `GET /api/agents/:id` — `workers[].manifest` с версией, ключами настроек,
маршрутами, событиями, задачами и запросами к бэкенду со схемами; то же по одному воркеру —
`…/manifest`. С `VALIDATE=1` сервер стенда проверяет по схемам из манифеста (`schemaProblems`)
значение `PUT …/configs/:worker/:key` до отправки агенту (не подходит — `400 CONFIG_INVALID` с
замечаниями) и тело запроса к воркеру (`400 REQUEST_INVALID`).

```bash
curl localhost:8080/api/agents/<id> | jq '.workers[] | {name, manifest}'
curl localhost:8080/api/agents/<id>/workers/echo/manifest | jq .requests
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

**Задачи** ([sdk/docs/workers.md](../sdk/docs/workers.md#задачи)) — запрос к воркеру `POST /jobs`:
тип — из манифеста воркера (нет — `409 JOB_UNKNOWN` от агента); быстрая задача — итог сразу
(`200`), долгая — `202` с `id`, её события `job.*` — в `/api/events`; состояние — `GET
/jobs/<id>`, отмена — `POST /jobs/<id>/cancel`.

```bash
curl -X POST localhost:8080/api/agents/<id>/workers/echo/fetch/jobs -d '{"type":"echo.quick","jobId":"j1","data":{"text":"привет"}}'
curl -X POST localhost:8080/api/agents/<id>/workers/echo/fetch/jobs -d '{"type":"echo.long","jobId":"j2","data":{"steps":10}}'
curl localhost:8080/api/agents/<id>/workers/echo/fetch/jobs/<id задачи>
```

**Запросы воркеров к бэкенду** ([sdk/docs/workers.md](../sdk/docs/workers.md#запросы-к-бэкенду)):
сервер стенда отвечает на `echo.lookup` префиксом `<имя агента>: `. Его спрашивают `echo` и
`node-echo` в задаче `echo.quick` с `lookup: true`; на другие запросы — отказ
`REQUEST_UNHANDLED`.

```bash
curl -X POST localhost:8080/api/agents/<id>/workers/echo/fetch/jobs -d '{"type":"echo.quick","jobId":"j3","data":{"text":"привет","lookup":true}}'
```

**Поток `watch`** (`text/event-stream`): пока он открыт, сервер держит наблюдателя
`agents.watch` — агент присылает метрики раз в секунду и журнал с уровня `logLevel` (по
умолчанию `info`); закрыли поток — наблюдатель снимается, агент возвращается к обычной частоте.
События: `metrics` (`MetricsEvent`), `log` (`LogEntry[]`), `event` (`AgentEvent`) этого агента.

```bash
curl -N 'localhost:8080/api/agents/<id>/watch?logLevel=debug'
```
