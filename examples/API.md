# API сервера стенда

Небольшой HTTP API сервера стенда [server](server) (Node, `agent-sdk/server`) — для сквозных
тестов и ручной работы (`curl`). Всё, что связано с агентами, делает `Agents` из SDK; здесь —
только тонкий слой над ним. Авторизации нет: это пример для запуска у себя. Кто действует —
заголовок `X-Actor` (по умолчанию `api`): он попадает в записи настроек, итоги действий и в аудит.
История — события воркеров, точки метрик, итоги действий — хранится в памяти сервера стенда
(последние 10 000 событий, 2000 точек и 200 действий на агента) и пропадает при его перезапуске.

Ошибки — `{ code, message }` со статусом из `AgentsError` ([sdk/README.md](../sdk/README.md#ошибки));
нет агента — `404 AGENT_NOT_FOUND`, нет такого маршрута API — `404 NOT_FOUND`.

| Метод  | Путь                                                | Что                                                                                                          |
| ------ | --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| GET    | `/api/agents`                                       | `Agent[]`                                                                                                    |
| GET    | `/api/agents/:id`                                   | `Agent`; манифест воркера — `workers[].manifest`                                                             |
| DELETE | `/api/agents/:id`                                   | удалить запись агента и всё его → `{}`                                                                       |
| POST   | `/api/agents/:id/revoke`                            | отозвать → `Agent`                                                                                           |
| POST   | `/api/agents/:id/rotate-key`                        | сменить ключ (`agent.rotateKey`) → `{}`                                                                      |
| POST   | `/api/agents/:id/update`                            | обновить агента до версии выпуска (`agent.update`) → `{ version, previous }`                                 |
| GET    | `/api/agents/:id/actions?limit=`                    | итоги встроенных действий агента, новые первыми (`limit` — по умолчанию 50) → `ActionRecord[]`               |
| GET    | `/api/agents/:id/configs`                           | `{ configs: ConfigRecord[], status: ConfigStatus[] }` — значения и статус применения                         |
| PUT    | `/api/agents/:id/configs/:worker/:key`              | задать: тело — значение (любой JSON) → `ConfigRecord` с новой `version`                                      |
| DELETE | `/api/agents/:id/configs/:worker/:key`              | удалить ключ на сервере и у агента → `{ deleted }`                                                           |
| GET    | `/api/agents/:id/metrics?since=&limit=`             | история метрик `MetricsPoint[]` по времени; `since` — строго позже, мс; `limit` — последние                  |
| GET    | `/api/agents/:id/logs?worker=&lines=`               | последние строки журнала агента или воркера с узла (`agent.logs`) → `LogEntry[]`                             |
| GET    | `/api/agents/:id/watch?logLevel=`                   | поток Server-Sent Events, см. ниже                                                                           |
| \*     | `/api/agents/:id/workers/:worker/fetch/<путь>?<…>`  | запрос к воркеру, см. ниже                                                                                   |
| POST   | `/api/agents/:id/workers/:worker/restart`           | перезапустить воркер (`worker.restart`) → `{}`; тело `{ force: true }` — не ждать, пока воркер занят         |
| POST   | `/api/agents/:id/workers/:worker/update`            | обновить воркер из выпуска (`worker.update`) → `{ version, previous }`; тело `{ force: true }` — так же      |
| GET    | `/api/agents/:id/workers/:worker/supports?…`        | есть ли в манифесте воркера маршрут (`method`, `path`), ключ (`config`), событие (`event`) → `{ supported }` |
| GET    | `/api/events?agentId=&worker=&type=&before=&limit=` | события воркеров, новые первыми (`limit` — по умолчанию 100); `before` — `receivedAt` последнего на странице |
| GET    | `/api/alerts?agentId=`                              | текущие проблемы `Alert[]`                                                                                   |
| GET    | `/api/releases`                                     | `{ release, candidates, workerCandidates, installCommand }` — выпуск, кого можно обновить, команда установки |

**Манифест.** Воркеры-примеры описывают себя (`GET /manifest`): агент передаёт манифест в
`status`, и он виден в `GET /api/agents/:id` — `workers[].manifest` с версией, ключами настроек
и их схемами, маршрутами и событиями. С `VALIDATE_CONFIGS=1` сервер стенда проверяет значение
`PUT …/configs/:worker/:key` по схеме ключа до отправки агенту: не подходит — `400
CONFIG_INVALID` с замечаниями.

```bash
curl localhost:8080/api/agents/<id> | jq '.workers[] | {name, manifest}'
curl 'localhost:8080/api/agents/<id>/workers/echo/supports?method=POST&path=/echo'
```

**Запрос к воркеру** передаётся как есть: метод, `<путь>` с параметрами, тело и `Content-Type`;
срок — заголовок `X-Timeout-Ms` (по умолчанию 30 с). Ответ воркера — статус, заголовки и тело —
приходит потоком, по мере того как воркер его отдаёт; закрыли запрос — запрос к воркеру
отменяется. Ошибка до ответа воркера — статус и код из [sdk/docs/fetch.md](../sdk/docs/fetch.md#ошибки):
`PATH_FORBIDDEN` (служебный путь), `WORKER_UNKNOWN`, `WORKER_UNAVAILABLE`, `WORKER_INVALID`,
`TIMEOUT`, `BODY_TOO_LARGE`, `BUSY`, `AGENT_OFFLINE`.

```bash
curl -X POST localhost:8080/api/agents/<id>/workers/echo/fetch/echo -d '{"text":"привет"}'
curl -N localhost:8080/api/agents/<id>/workers/echo/fetch/stream?n=5
```

**Поток `watch`** (`text/event-stream`): пока он открыт, сервер держит наблюдателя
`agents.watch` — агент присылает метрики раз в секунду и журнал с уровня `logLevel` (по
умолчанию `info`); закрыли поток — наблюдатель снимается, агент возвращается к обычной частоте.
События: `metrics` (`MetricsEvent`), `log` (`LogEntry[]`), `event` (`AgentEvent`) этого агента.

```bash
curl -N 'localhost:8080/api/agents/<id>/watch?logLevel=debug'
```
