# SDK бэкенда: agent-sdk для Node.js

Агент на узле держит связь с бэкендом, запускает воркеры и передаёт им запросы, настройки,
метрики и события. Бэкенду для этого нужен один пакет — `agent-sdk` (Node.js ≥ 24, TypeScript,
ESM; зависимости — `ws`, `zod`, `semver`, `lru-cache`, `@cfworker/json-schema`). Воркерам SDK не нужен:
воркер — обычный HTTP-сервис на unix-сокете на любом языке ([docs/workers.md](docs/workers.md)).

```
бэкенд (agent-sdk) ──WebSocket──► агент ──HTTP по unix-сокету──► воркер
```

## Установка

```bash
# Готовый архив из GitHub Release (TypeScript уже собран)
npm install https://github.com/epifanovmd/agent/releases/download/v<версия>/agent-sdk-<версия>.tgz
# или из репозитория
npm install ./sdk/node   # затем: cd sdk/node && npm run build
```

Импорт — `agent-sdk/server`. Корень пакета `agent-sdk` отдаёт то же самое и ещё `SDK_VERSION` — версию
SDK.

## Документация

| Где                                      | О чём                                                               |
| ---------------------------------------- | ------------------------------------------------------------------- |
| [docs/README.md](docs/README.md)         | с чего начать: бэкенд, агент и воркер за 10 минут                   |
| [docs/connection.md](docs/connection.md) | регистрация, связь, отзыв, смена ключа, несколько копий, фреймворки |
| [docs/workers.md](docs/workers.md)       | воркер без SDK на Python, Node и Go                                 |
| [docs/fetch.md](docs/fetch.md)           | запросы к воркеру                                                   |
| [docs/configs.md](docs/configs.md)       | настройки воркеров                                                  |
| [docs/observe.md](docs/observe.md)       | метрики, watch, журнал, события, проблемы                           |
| [docs/releases.md](docs/releases.md)     | сборки, установка, обновления                                       |
| [docs/store.md](docs/store.md)           | хранилище, пример на Postgres                                       |
| [spec/README.md](spec/README.md)         | формат сообщений — источник правды; образцы — `spec/examples`       |

## Agents

```ts
import { createServer } from "node:http";
import { Agents } from "agent-sdk/server";

const agents = new Agents({ enrollToken: process.env.AGENT_ENROLL_TOKEN });
const server = createServer(async (req, res) => {
  if (await agents.handle(req, res)) return;
  res.writeHead(404).end();
});
agents.attach(server);
server.listen(8080);
```

### Настройки

| Опция                    | По умолчанию                            | Что это                                                                                                                                                    |
| ------------------------ | --------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `enrollToken`            | —                                       | токен регистрации агентов                                                                                                                                  |
| `enroll(token, info)`    | —                                       | своя проверка: `false`, `null` или `undefined` — отказ, `true` или `{ labels?, name? }` — принять                                                          |
| `store`                  | `MemoryStore`                           | хранилище ([store.md](docs/store.md))                                                                                                                      |
| `instanceId`             | случайный                               | имя копии бэкенда в `agent.session.instance`                                                                                                               |
| `relay(instanceId, req)` | —                                       | доставить вызов в копию с соединением агента ([connection](docs/connection.md#пересылка-вызовов-relay)); без неё — `AGENT_ELSEWHERE`                       |
| `relaySecret`            | —                                       | общий секрет копий: заголовок `x-agents-relay-secret`, без него `handleRelay` — 401                                                                        |
| `statusIntervalMs`       | 30 000                                  | как часто агент шлёт `status`                                                                                                                              |
| `metricsIntervalMs`      | 10 000                                  | как часто агент шлёт метрики                                                                                                                               |
| `onEvent(event)`         | —                                       | обработчик событий воркеров; подтверждение агенту — после него ([observe](docs/observe.md#события-воркеров))                                               |
| `onWorkerRequest(req)`   | —                                       | ответ на запрос воркера к бэкенду: результат — `data` ответа, ошибка — отказ ([workers](docs/workers.md#запросы-к-бэкенду))                                |
| `offlineGraceMs`         | 3000                                    | агент после обрыва ещё `online` столько                                                                                                                    |
| `pingIntervalMs`         | 5000                                    | ping агенту; на два ping подряд нет pong — соединение закрывается                                                                                          |
| `offlineAfterMs`         | max(3 × statusIntervalMs, 30 с) + grace | без вестей дольше — `offline` (копия с соединением упала)                                                                                                  |
| `actionTimeoutMs`        | 60 000                                  | срок итога действия                                                                                                                                        |
| `updateTimeoutMs`        | 300 000                                 | срок итога `updateAgent`, `updateWorker`                                                                                                                   |
| `releasesDir`            | —                                       | каталог сборок: manifest.json, сборки, install.sh; с `agentReleases` — воркеры проекта ([releases](docs/releases.md#откуда-бэкенд-берёт-агента))           |
| `agentReleases`          | —                                       | откуда брать агента и netprobe: `{ github: "owner/repo", range?, token?, checkIntervalMs?, proxy? }` или `{ url }`; SDK сам следит за новыми версиями      |
| `publicKey`              | —                                       | ключ проверки сборок для install.sh                                                                                                                        |
| `updatePublicKeys`       | `[]`                                    | ещё ключи проверки (ключи проекта) для install.sh                                                                                                          |
| `baseUrl`                | из запроса                              | публичный адрес бэкенда для install.sh и `installCommand`                                                                                                  |
| `enrollFailureLimit`     | 10                                      | неудачных регистраций с адреса за окно, дальше — 429 (`0` — без предела)                                                                                   |
| `enrollFailureWindowMs`  | 60 000                                  | окно подсчёта неудачных регистраций                                                                                                                        |
| `trustProxy`             | `false`                                 | адрес клиента из `X-Forwarded-For`, адрес сервера — из `X-Forwarded-Host/Proto`                                                                            |
| `validateConfigs`        | `false`                                 | `setConfig` проверяет значение по схеме ключа из манифеста воркера ([configs](docs/configs.md#проверка-по-схеме))                                          |
| `validateJobs`           | `false`                                 | `runJob` проверяет `data` по схеме типа задачи из манифеста воркера ([workers](docs/workers.md#задачи))                                                    |
| `validateRequests`       | `false`                                 | тело `fetch` — по `routes[].request` ([fetch](docs/fetch.md#проверка-тела-по-схеме)), `data` запроса воркера — по `requests[].schema`                      |
| `validateEvents`         | `"off"`                                 | `data` события — по `events[].schema`: `log` — журнал и `invalidEvent`, `reject` — ещё и не передавать ([observe](docs/observe.md#проверка-data-по-схеме)) |
| `log(msg, extra)`        | строки `agents: …` в stdout             | журнал SDK: в `extra` — имена, id, коды и причины отказа, без значений настроек, тел запросов, `data` событий и токенов                                    |

### Методы

| Метод                                                                               | Результат                                                         | Раздел                                                                    |
| ----------------------------------------------------------------------------------- | ----------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `handle(req, res)`                                                                  | `Promise<boolean>` — запрос обработан                             | [connection](docs/connection.md#подключение-к-http-серверу-и-фреймворкам) |
| `attach(server)`                                                                    | WebSocket агентов на этом сервере                                 | там же                                                                    |
| `close()`                                                                           | `Promise<void>`; соединения — код 1012                            | там же                                                                    |
| `handleRelay(req, res)`                                                             | принять вызов, пересланный другой копией                          | [connection](docs/connection.md#пересылка-вызовов-relay)                  |
| `by(actor)`                                                                         | `Actor` — изменяющие методы с автором                             | [connection](docs/connection.md#агенты-список-отзыв-удаление)             |
| `listAgents()`, `getAgent(id)`                                                      | `Agent[]`, `Agent \| undefined`                                   | там же                                                                    |
| `revoke(id)`, `deleteAgent(id)`                                                     | `Agent`, `void`                                                   | там же                                                                    |
| `rotateKey(id, { timeoutMs? })`                                                     | `void`                                                            | [connection](docs/connection.md#смена-ключа)                              |
| `refresh(agentId?)`                                                                 | применить изменения других копий                                  | [connection](docs/connection.md#несколько-копий-бэкенда)                  |
| `fetch(id, worker, path, { method, headers, body, timeoutMs, signal })`             | `Response`                                                        | [fetch](docs/fetch.md)                                                    |
| `runJob(id, worker, { type, jobId?, data?, files?, timeoutMs?, signal? })`          | `{ jobId, id?, state, progress?, result?, error? }`               | [workers](docs/workers.md#задачи)                                         |
| `jobStatus(id, worker, jobId)`, `cancelJob(id, worker, jobId)`                      | `{ id, state, progress?, result?, error? }`                       | там же                                                                    |
| `setConfig(id, worker, key, data)`                                                  | `ConfigRecord` (с `version`)                                      | [configs](docs/configs.md)                                                |
| `getConfig(id, worker, key)`, `listConfigs(id)`                                     | `ConfigRecord \| undefined`, `ConfigRecord[]`                     | там же                                                                    |
| `deleteConfig(id, worker, key)`                                                     | `boolean` — ключ был                                              | там же                                                                    |
| `configStatus(id, worker?)`                                                         | `ConfigStatus[]`                                                  | там же                                                                    |
| `capabilities(id, worker)`                                                          | `WorkerCapabilities \| undefined` — что умеет воркер, со схемами  | [ниже](#манифест-воркера)                                                 |
| `subscribeEvents({ agentId?, worker, type }, handler)`                              | `Promise<() => void>` — отписка                                   | [observe](docs/observe.md#подписки-subscribeevents-и-waitevent)           |
| `waitEvent(id, worker, type, { match?, timeoutMs?, signal? })`                      | `AgentEvent` — первое подходящее событие после вызова             | там же                                                                    |
| `watch(id, { id?, metricsIntervalMs?, logLevel?, ttlMs? })`, `unwatch(id, watchId)` | `{ id, until }`, `Promise<void>`                                  | [observe](docs/observe.md#наблюдение-watch)                               |
| `listAlerts(agentId?)`                                                              | `Alert[]` — текущие проблемы                                      | [observe](docs/observe.md#проблемы)                                       |
| `logs(id, { worker?, lines? })`                                                     | `LogEntry[]`                                                      | [observe](docs/observe.md#журнал)                                         |
| `restartWorker(id, name, { force?, wait?, timeoutMs? })`                            | `{ deferred: false }` или `{ deferred: true, pending, actionId }` | [workers](docs/workers.md#замена-занятого-воркера)                        |
| `updateWorker(id, name, { force?, wait?, timeoutMs? })`                             | `{ version, previous?, deferred: false }` или как выше            | [releases](docs/releases.md)                                              |
| `updateAgent(id, { version?, timeoutMs? })`                                         | `{ version, previous? }`; с `version` — сборка из каталога агента | там же                                                                    |
| `release()`, `updateCandidates()`, `workerUpdateCandidates()`                       | итоговый набор сборок (у сборок — `source`, `url`) и кандидаты    | [releases](docs/releases.md)                                              |
| `checkRelease()`                                                                    | проверить удалённый источник сейчас → итоговый набор сборок       | [releases](docs/releases.md#откуда-бэкенд-берёт-агента)                   |
| `installCommand(opts)`                                                              | строка `curl … \| sudo sh -s -- …`                                | [releases](docs/releases.md#установка-одной-командой)                     |

`fetch`, задачи, действия (`restartWorker`, `updateWorker`, `updateAgent`, `rotateKey`, `logs`)
и `watch` выполняются в копии бэкенда, у которой соединение агента: из другой копии вызов
пересылается туда (опция `relay`), без неё — `AGENT_ELSEWHERE`; остальное — в любой копии. Итог
действия — результат промиса вызова и событие `action`. Замена занятого воркера (`health.busy`)
откладывается: `restartWorker` и `updateWorker` сразу возвращают `{ deferred: true, pending,
actionId }`, а итог замены приходит событием `action` с тем же `id` (`deferred: true`);
`wait: true` — ждать итога (срок `timeoutMs` отсчитывается заново, пока замена ждёт), `force:
true` — заменить сразу.

### Манифест воркера

Воркер обязан описать себя манифестом — версия (обязательно), ключи настроек, маршруты, события,
задачи и запросы к бэкенду, со схемами ([workers](docs/workers.md#манифест-что-воркер-умеет)).
Без корректного манифеста агент не регистрирует воркер (`state: invalid`), а с ним пропускает
только объявленное: необъявленный маршрут — `ROUTE_UNDECLARED`, тип задачи — `JOB_UNKNOWN`
([fetch](docs/fetch.md#только-объявленные-маршруты)). SDK принимает манифест от агента терпимо:
неверные и незнакомые поля пропускаются. Бэкенд видит его в `agent.workers[].manifest`
(`WorkerManifest`), по одному воркеру — `agents.capabilities(agentId, worker)`:

```ts
const caps = await agents.capabilities(agentId, "echo"); // undefined — воркер себя не описал
caps?.routes; // [{ method, path, description?, request?, response? }]
caps?.events; // [{ type, description?, schema? }]
caps?.jobs; // [{ type, description?, schema? }]
caps?.configs; // [{ key, description?, schema? }]
caps?.requests; // [{ type, description?, schema?, response? }] — запросы воркера к бэкенду
```

Проверять удобно помощниками:

```ts
import { declaresEvent, findRoute, matchRoute, supports, workerManifest } from "agent-sdk/server";

const agent = await agents.getAgent(agentId);
if (!agent) throw new Error("нет агента");

supports(agent, "echo", { route: { method: "POST", path: "/echo" } }); // true — маршрут описан
supports(agent, "echo", { config: "settings", event: "echo.started" }); // все условия сразу
workerManifest(agent, "echo"); // WorkerManifest | undefined
matchRoute("/items/{id}", "/items/42?full=1"); // true: {id} — один сегмент, ?… не учитывается
findRoute(workerManifest(agent, "echo"), "POST", "/echo"); // маршрут манифеста со схемами
declaresEvent(workerManifest(agent, "echo"), "echo.started"); // агент примет это событие
```

`supports` — `true`, если в манифесте воркера есть всё заданное: маршрут (метод без учёта
регистра, путь по шаблону), ключ настроек, тип события. Нет манифеста — `false`.

Историю SDK не хранит: метрики, события воркеров, журнал, итоги действий, проблемы и аудит
приходят событиями (и `onEvent`), а последнее состояние агента — `hello`, `status`, последняя
точка метрик, текущие проблемы — в записи агента (`getAgent`, `listAlerts`). Нужна история —
сохраняйте её сами из событий ([observe](docs/observe.md)).

### События

```ts
agents.on("agent", (a: Agent) => {}); // регистрация, подключение, отключение, status, отзыв
agents.on("event", (e: AgentEvent) => {}); // событие воркера (после onEvent)
agents.on("invalidEvent", (e: InvalidEvent) => {}); // data события не по схеме (validateEvents)
agents.on("metrics", (m: MetricsEvent) => {}); // каждая точка метрик
agents.on("log", ({ agentId, entries }: LogEvent) => {}); // журнал агента
agents.on("config", (s: ConfigStatus) => {}); // статус ключа изменился; state: deleted — агент удалил ключ
agents.on("alert", (a: AlertEvent) => {}); // проблема началась (active) или закончилась
agents.on("action", (a: ActionRecord) => {}); // итог действия (и отложенной замены — deferred: true)
agents.on("audit", (e: AuditEntry) => {}); // кто что сделал
agents.on("change", ({ agentId, reason }: ChangeEvent) => {}); // другим копиям — refresh(agentId)
agents.on("release", ({ version, previous, from }: ReleaseEvent) => {}); // новая версия агента в agentReleases
```

Ошибка в подписчике не мешает работе `Agents`: она попадает в журнал.

### Ошибки

Методы отклоняются с `AgentsError`: `code` (строка), `message`, `status` — HTTP-статус, с которым
ошибку удобно вернуть клиенту бэкенда, `retryAfterSec` — для 429.

| Код                    | Статус   | Когда                                                                                                                                                                                                                                                                                                                                                                                         |
| ---------------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `MESSAGE_INVALID`      | 400      | неверные аргументы: имя воркера или ключа, путь, значение                                                                                                                                                                                                                                                                                                                                     |
| `AGENT_NOT_FOUND`      | 404      | нет агента                                                                                                                                                                                                                                                                                                                                                                                    |
| `AGENT_REVOKED`        | 409      | агент отозван                                                                                                                                                                                                                                                                                                                                                                                 |
| `AGENT_OFFLINE`        | 503      | агент без связи                                                                                                                                                                                                                                                                                                                                                                               |
| `AGENT_ELSEWHERE`      | 421      | соединение агента в другой копии бэкенда, а `relay` не задан                                                                                                                                                                                                                                                                                                                                  |
| `RELAY_FAILED`         | 502      | `relay`: вызов не доставлен в копию с соединением                                                                                                                                                                                                                                                                                                                                             |
| `UNAUTHORIZED`         | 401      | `handleRelay`: неверный `relaySecret`                                                                                                                                                                                                                                                                                                                                                         |
| `JOB_UNKNOWN`          | 409      | `runJob` (и `fetch` `POST /jobs` — от агента): типа задачи нет в `manifest.jobs` воркера                                                                                                                                                                                                                                                                                                      |
| `ROUTE_UNDECLARED`     | 404      | `fetch`: метода и пути нет в `routes` манифеста воркера ([fetch](docs/fetch.md#только-объявленные-маршруты))                                                                                                                                                                                                                                                                                  |
| `REQUEST_INVALID`      | 400      | `validateRequests`: тело `fetch` не JSON или не по `routes[].request`                                                                                                                                                                                                                                                                                                                         |
| `EVENT_UNDECLARED`     | 409      | `subscribeEvents`, `waitEvent`: типа события нет в манифесте воркера                                                                                                                                                                                                                                                                                                                          |
| `JOB_INVALID`          | 400      | `validateJobs`: `data` не подходит под схему типа задачи                                                                                                                                                                                                                                                                                                                                      |
| `JOB_REJECTED`         | 4xx, 502 | воркер отказал в задаче (его статус и `message`) или ответил не по §12                                                                                                                                                                                                                                                                                                                        |
| `JOB_NOT_FOUND`        | 404      | `jobStatus`, `cancelJob`: у воркера нет задачи                                                                                                                                                                                                                                                                                                                                                |
| `TIMEOUT`              | 504      | нет ответа или итога в срок                                                                                                                                                                                                                                                                                                                                                                   |
| `CANCELLED`            | 499      | `fetch` или ожидание задачи отменены (`signal`); действие ждало итога, а `Agents` остановили (`close()`)                                                                                                                                                                                                                                                                                      |
| `CONFLICT`             | 409      | запись агента в Store всё время меняется: условная запись не удалась 20 раз подряд ([store](docs/store.md#правила))                                                                                                                                                                                                                                                                           |
| `BODY_TOO_LARGE`       | 413      | значение настройки больше 4 МБ, тело запроса больше 4 МБ                                                                                                                                                                                                                                                                                                                                      |
| `UPDATE_NOT_AVAILABLE` | 409      | нет манифеста или сборки под агента                                                                                                                                                                                                                                                                                                                                                           |
| `WORKER_NOT_RELEASED`  | 409      | воркер прописан командой                                                                                                                                                                                                                                                                                                                                                                      |
| `CONFIG_INVALID`       | 400      | `validateConfigs`: значение не подходит под схему ключа из манифеста воркера                                                                                                                                                                                                                                                                                                                  |
| `WORKER_INVALID`       | 502      | `fetch`: воркер запущен, но не зарегистрирован — не ответил как нужно на `GET /health` или `GET /manifest` ([workers](docs/workers.md#обязательный-минимум))                                                                                                                                                                                                                                  |
| коды агента            | 409, 5xx | `fetch` — [fetch.md](docs/fetch.md#ошибки); настройки (`ConfigStatus.error`, не исключение) — `CONFIG_REJECTED`, `CONFIG_KEY_UNKNOWN`, `WORKER_INVALID` ([configs.md](docs/configs.md#статус-применения)); действия — `ACTION_UNKNOWN`, `WORKER_UNKNOWN`, `BUSY`, `UPDATE_NOT_VERIFIED`, `UPDATE_NOT_SUPPORTED`, `UPDATE_FAILED`, `ACTION_FAILED` ([spec §15](spec/README.md#15-коды-ошибок)) |

### Модели

```ts
interface Agent {
  id: string;
  name: string;
  labels: Record<string, string>;
  online: boolean;
  revoked: boolean;
  enrolledAt: number;
  lastSeenAt?: number;
  connectedAt?: number;
  address?: string; // IP последнего подключения
  version?: string; // версия агента
  bootId?: string;
  startedAt?: number;
  host?: { os; arch; hostname; kernel? };
  workers: AgentWorker[]; // из hello и последнего status: name, state, message, version, release, builtin, restarts, health (с busy), pending, configs, manifest
  // state: "starting" | "running" | "invalid" | "backoff" | "stopped"; message — причина invalid
  hello?: Hello;
  status?: Status;
  statusAt?: number;
  metrics?: MetricsPoint; // последняя точка
  update?: { latest; checkedAt }; // новая версия агента, которую он нашёл сам (status важнее hello); нет — новее нет
  alerts: Alert[];
  session?: { id; instance; since }; // какая копия бэкенда держит соединение
}

interface ConfigRecord {
  agentId;
  worker;
  key;
  version: number;
  data: unknown;
  updatedAt;
  actor?;
}
interface ConfigStatus {
  agentId;
  worker;
  key;
  version: number | null;
  delivered?;
  applied?;
  state: "pending" | "applying" | "applied" | "failed" | "deleting" | "deleted"; // deleted — только в событии config
  error?;
  result?; // подробный итог от воркера (тело ответа на PUT /config) — только в state applied
  updatedAt?;
}
interface AgentEvent {
  id;
  agentId;
  worker;
  type;
  data?;
  at;
  receivedAt;
}
interface InvalidEvent {
  event: AgentEvent;
  problems: string[];
  rejected; // validateEvents: "reject" — событие подтверждено, но дальше не передано
}
interface WorkerRequest {
  id;
  agentId;
  worker;
  type; // из manifest.requests
  data?;
  agent: Agent;
  timeoutMs;
  signal: AbortSignal; // истёк срок или оборвалась связь
}
interface WorkerCapabilities {
  version?;
  description?;
  configs;
  routes;
  events;
  jobs;
  requests;
}
interface MetricsPoint {
  at;
  collectedAt;
  host?: Record<string, unknown>;
  workers?: Record<string, unknown>;
}
interface Alert {
  key;
  agentId;
  agentName;
  type: "offline" | "workerDown" | "workerInvalid" | "workerUnhealthy" | "configFailed";
  worker?;
  configKey?;
  message;
  since;
}
interface ActionRecord {
  id;
  agentId;
  name: "worker.restart" | "worker.update" | "agent.update" | "agent.rotateKey" | "agent.logs";
  args?;
  actor?;
  status: "done" | "failed";
  result?;
  error?;
  createdAt;
  finishedAt;
  deferred?: true; // итог отложенной замены воркера (action.done)
}
interface LogEntry {
  at;
  level: "debug" | "info" | "warn" | "error";
  source;
  msg;
  attrs?;
}
```

Типы сообщений (`Hello`, `Status`, `WorkerStatus`, `WorkerManifest`, `Metrics`, `Envelope`, …) и константы
(`LINK_PATH`, `WS_CHANNEL`, `Close`, `NAME_PATTERN`) тоже экспортируются.

### Store

`Store` — интерфейс хранилища, `MemoryStore` — в памяти. Свой Store и пример на Postgres —
[docs/store.md](docs/store.md).

### HTTP-помощники

`readBody(req, limit)`, `readJSON(req, limit)`, `sendJSON(res, status, body)`,
`baseUrl(req, trustProxy)`, `clientAddress(req, trustProxy)` — для своих маршрутов без
фреймворка.

## Проверки

```bash
cd sdk/node && npm ci && npm run lint && npm run typecheck && npm test && npm run build
```

Тесты (`node:test`) прогоняют все образцы `spec/examples` между сервером и агентом через SDK с
фейковым агентом на WebSocket, а также сценарии: регистрацию с её пределами, коды закрытия,
повторы важного и потока, `fetch` с потоком, отменой и сроком, настройки и их сверку, манифест
воркера и проверку настроек по схеме, сводку `watch`, метрики, проблемы, смену ключа, сборки,
задачи воркера, отложенную замену занятого воркера, обнаружение обрыва (ping), несколько копий с
общим Store и пересылку вызовов между ними через настоящий HTTP.
