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

Импорт — `agent-sdk/server`. Корень пакета `agent-sdk` отдаёт то же самое.

## Документация

| Где                                      | О чём                                                               |
| ---------------------------------------- | ------------------------------------------------------------------- |
| [docs/README.md](docs/README.md)         | с чего начать: бэкенд, агент и воркер за 10 минут                   |
| [docs/connection.md](docs/connection.md) | регистрация, связь, отзыв, смена ключа, несколько копий, фреймворки |
| [docs/workers.md](docs/workers.md)       | воркер без SDK на Python, Node и Go                                 |
| [docs/fetch.md](docs/fetch.md)           | запросы к воркеру                                                   |
| [docs/configs.md](docs/configs.md)       | настройки воркеров                                                  |
| [docs/observe.md](docs/observe.md)       | метрики, watch, журнал, события, проблемы, аудит                    |
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

Незнакомое имя опции — ошибка `MESSAGE_INVALID` при создании `Agents`.

| Опция                   | По умолчанию                            | Что это                                                                                                                     |
| ----------------------- | --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `enrollToken`           | —                                       | токен регистрации агентов                                                                                                   |
| `enroll(token, info)`   | —                                       | своя проверка: `false`, `null` или `undefined` — отказ, `true` или `{ labels?, name? }` — принять                           |
| `store`                 | `MemoryStore`                           | хранилище ([store.md](docs/store.md))                                                                                       |
| `instanceId`            | случайный                               | имя копии бэкенда в `agent.session.instance`                                                                                |
| `statusIntervalMs`      | 30 000                                  | как часто агент шлёт `status`                                                                                               |
| `metricsIntervalMs`     | 10 000                                  | как часто агент шлёт метрики                                                                                                |
| `onEvent(event)`        | —                                       | обработчик событий воркеров; подтверждение агенту — после него ([observe](docs/observe.md#события-воркеров))                |
| `onWorkerRequest(req)`  | —                                       | ответ на запрос воркера к бэкенду: результат — `data` ответа, ошибка — отказ ([workers](docs/workers.md#запросы-к-бэкенду)) |
| `offlineGraceMs`        | 3000                                    | агент после обрыва ещё `online` столько                                                                                     |
| `pingIntervalMs`        | 5000                                    | ping агенту; на два ping подряд нет pong — соединение закрывается                                                           |
| `offlineAfterMs`        | max(3 × statusIntervalMs, 30 с) + grace | без вестей дольше — `offline` (копия с соединением упала)                                                                   |
| `actionTimeoutMs`       | 60 000                                  | срок итога действия                                                                                                         |
| `updateTimeoutMs`       | 300 000                                 | срок итога `updateAgent`, `updateWorker`                                                                                    |
| `releasesDir`           | —                                       | каталог сборок воркеров проекта: manifest.json от `agent manifest` и сборки ([releases](docs/releases.md#раздать-сборки))   |
| `enrollFailureLimit`    | 10                                      | неудачных регистраций с адреса за окно, дальше — 429 (`0` — без предела)                                                    |
| `enrollFailureWindowMs` | 60 000                                  | окно подсчёта неудачных регистраций                                                                                         |
| `trustProxy`            | `false`                                 | адрес клиента из `X-Forwarded-For`, адрес сервера — из `X-Forwarded-Host/Proto`                                             |
| `log(msg, extra)`       | строки `agents: …` в stdout             | журнал SDK: в `extra` — имена, id, коды и причины отказа, без значений настроек, тел запросов, `data` событий и токенов     |

### Методы

| Метод                                                                               | Результат                                                                                        | Раздел                                                                     |
| ----------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------- |
| `handle(req, res)`                                                                  | `Promise<boolean>` — запрос обработан; не бросает: ошибку SDK отвечает сам (`{ code, message }`) | [connection](docs/connection.md#подключение-к-http-серверу-и-фреймворкам)  |
| `attach(server)`                                                                    | WebSocket агентов на этом сервере                                                                | там же                                                                     |
| `close()`                                                                           | `Promise<void>`; соединения — код 1012                                                           | там же                                                                     |
| `listAgents()`, `getAgent(id)`                                                      | `Agent[]`, `Agent \| undefined`                                                                  | [connection](docs/connection.md#агенты-список-отзыв-удаление)              |
| `revoke(id)`, `deleteAgent(id)`                                                     | `Agent`, `void`                                                                                  | там же                                                                     |
| `rotateKey(id, { timeoutMs? })`                                                     | `void`                                                                                           | [connection](docs/connection.md#смена-ключа)                               |
| `refresh(agentId?)`                                                                 | применить изменения других копий                                                                 | [connection](docs/connection.md#несколько-копий-бэкенда)                   |
| `fetch(id, worker, path, { method, headers, body, timeoutMs, signal })`             | `Response`                                                                                       | [fetch](docs/fetch.md); задачи воркера — [workers](docs/workers.md#задачи) |
| `setConfig(id, worker, key, data)`                                                  | `ConfigRecord` (с `version`)                                                                     | [configs](docs/configs.md)                                                 |
| `getConfig(id, worker, key)`, `listConfigs(id)`                                     | `ConfigRecord \| undefined`, `ConfigRecord[]`                                                    | там же                                                                     |
| `deleteConfig(id, worker, key)`                                                     | `boolean` — ключ был                                                                             | там же                                                                     |
| `configStatus(id, worker?)`                                                         | `ConfigStatus[]`                                                                                 | там же                                                                     |
| `watch(id, { id?, metricsIntervalMs?, logLevel?, ttlMs? })`, `unwatch(id, watchId)` | `Watch` — `{ id, until, close() }`, продлевается сам до `close()`; `Promise<void>`               | [observe](docs/observe.md#наблюдение-watch)                                |
| `logs(id, { worker?, lines? })`                                                     | `LogEntry[]`                                                                                     | [observe](docs/observe.md#журнал)                                          |
| `restartWorker(id, name, { force?, wait?, timeoutMs? })`                            | `{ deferred: false }` или `{ deferred: true, pending, actionId }`                                | [workers](docs/workers.md#замена-занятого-воркера)                         |
| `updateWorker(id, name, { force?, wait?, timeoutMs? })`                             | `{ version, previous?, deferred: false }` или как выше                                           | [releases](docs/releases.md#обновление-воркеров-с-сервера)                 |
| `updateAgent(id, { version, timeoutMs? })`                                          | `{ version, previous? }` — агент берёт сборку версии из своего каталога сборок                   | [releases](docs/releases.md#обновление-агента)                             |
| `release()`                                                                         | `ReleaseView \| null` — manifest.json из `releasesDir`, у каждой сборки — `url`                  | [releases](docs/releases.md#раздать-сборки)                                |

`fetch`, действия (`restartWorker`, `updateWorker`, `updateAgent`, `rotateKey`, `logs`)
и `watch` выполняются в копии бэкенда, у которой соединение агента: в другой копии —
`AGENT_ELSEWHERE`; остальное — в любой копии. Итог
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
(`WorkerManifest`), по одному воркеру — помощник `workerManifest`:

```ts
import { schemaProblems, workerManifest } from "agent-sdk/server";

const agent = await agents.getAgent(agentId);
const m = agent && workerManifest(agent, "echo"); // WorkerManifest | undefined — воркер себя не описал
m?.routes; // [{ method, path, description?, request?, response? }]
m?.events; // [{ type, description?, schema? }]
m?.jobs; // [{ type, description?, schema? }]
m?.configs; // [{ key, description?, schema? }]
m?.requests; // [{ type, description?, schema?, response? }] — запросы воркера к бэкенду
```

Значения по схемам агент не проверяет — это делает бэкенд, где ему нужно, помощником
`schemaProblems(agent, worker, target, data)`: замечания вида «поле: что не так» (не больше трёх),
пустой список — подходит или проверять нечем (нет манифеста, записи или схемы); схему, которую
нельзя применить, он не применяет — бросает исключение. `target` (`SchemaTarget`) — чьей схемой
проверять:

| `target`                      | Схема               | Где удобно                                                                                                           |
| ----------------------------- | ------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `{ config: key }`             | `configs[].schema`  | перед `setConfig` ([configs](docs/configs.md#проверка-по-схеме))                                                     |
| `{ route: { method, path } }` | `routes[].request`  | перед `fetch`; тело — разобранный JSON ([fetch](docs/fetch.md#проверка-тела-по-схеме))                               |
| `{ job: type }`               | `jobs[].schema`     | перед `POST /jobs` ([workers](docs/workers.md#задачи))                                                               |
| `{ event: type }`             | `events[].schema`   | в `onEvent` ([observe](docs/observe.md#проверка-data-по-схеме))                                                      |
| `{ request: type }`           | `requests[].schema` | в `onWorkerRequest`: отказ — `AgentsError("REQUEST_INVALID", …, 400)` ([workers](docs/workers.md#запросы-к-бэкенду)) |

```ts
const problems = schemaProblems(agent, "echo", { config: "settings" }, value);
if (problems.length) throw new AgentsError("MESSAGE_INVALID", problems.join("; "));
```

Историю SDK не хранит: метрики, события воркеров, журнал и итоги действий приходят событиями (и
`onEvent`), а последнее состояние агента — `hello`, `status`, последняя точка метрик — в записи
агента (`getAgent`). Нужна история — сохраняйте её сами из событий ([observe](docs/observe.md)).
Проблемы узлов ([observe](docs/observe.md#проблемы)), аудит
([observe](docs/observe.md#итоги-действий-и-аудит)) и кандидаты на обновление
([releases](docs/releases.md#обновление-агента)) бэкенд ведёт сам — по `Agent` и своим вызовам.

### События

```ts
agents.on("agent", (a: Agent) => {}); // регистрация, подключение, отключение, status, отзыв
agents.on("event", (e: AgentEvent) => {}); // событие воркера (после onEvent)
agents.on("metrics", (m: MetricsEvent) => {}); // каждая точка метрик
agents.on("log", ({ agentId, entries }: LogEvent) => {}); // журнал агента
agents.on("config", (s: ConfigStatus) => {}); // статус ключа изменился; state: deleted — агент удалил ключ
agents.on("action", (a: ActionRecord) => {}); // итог действия (и отложенной замены — deferred: true)
agents.on("change", ({ agentId, reason }: ChangeEvent) => {}); // другим копиям — refresh(agentId)
```

Ошибка в подписчике не мешает работе `Agents`: она попадает в журнал.

### Ошибки

Методы отклоняются с `AgentsError`: `code` (строка), `message`, `status` — HTTP-статус, с которым
ошибку удобно вернуть клиенту бэкенда, `retryAfterSec` — для 429. `retryable` — повтор того же
вызова позже может удаться: `true` у `AGENT_OFFLINE`, `AGENT_ELSEWHERE`, `WORKER_UNAVAILABLE`,
`WORKER_INVALID`, `TIMEOUT`, `BUSY`, `DISCONNECTED`, `RATE_LIMITED`, `INTERNAL`; у неверного
вызова — `false`.

| Код                    | Статус   | Когда                                                                                                                                                                                                                                                                                                                                                                                         |
| ---------------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `MESSAGE_INVALID`      | 400      | неверные аргументы: имя воркера или ключа, путь, значение                                                                                                                                                                                                                                                                                                                                     |
| `AGENT_NOT_FOUND`      | 404      | нет агента                                                                                                                                                                                                                                                                                                                                                                                    |
| `AGENT_REVOKED`        | 409      | агент отозван                                                                                                                                                                                                                                                                                                                                                                                 |
| `AGENT_OFFLINE`        | 503      | агент без связи                                                                                                                                                                                                                                                                                                                                                                               |
| `AGENT_ELSEWHERE`      | 421      | соединение агента в другой копии бэкенда                                                                                                                                                                                                                                                                                                                                                      |
| `JOB_UNKNOWN`          | 409      | `fetch` `POST /jobs` (от агента): типа задачи нет в `manifest.jobs` воркера                                                                                                                                                                                                                                                                                                                   |
| `ROUTE_UNDECLARED`     | 404      | `fetch`: метода и пути нет в `routes` манифеста воркера ([fetch](docs/fetch.md#только-объявленные-маршруты))                                                                                                                                                                                                                                                                                  |
| `TIMEOUT`              | 504      | нет ответа или итога в срок                                                                                                                                                                                                                                                                                                                                                                   |
| `CANCELLED`            | 499      | `fetch` отменён (`signal`); действие ждало итога, а `Agents` остановили (`close()`)                                                                                                                                                                                                                                                                                                           |
| `CONFLICT`             | 409      | запись агента в Store всё время меняется: условная запись не удалась 20 раз подряд ([store](docs/store.md#правила))                                                                                                                                                                                                                                                                           |
| `BODY_TOO_LARGE`       | 413      | значение настройки больше 4 МБ, тело запроса больше 4 МБ                                                                                                                                                                                                                                                                                                                                      |
| `UPDATE_NOT_AVAILABLE` | 409      | `updateWorker`: нет `releasesDir`, манифеста или сборки воркера под агента                                                                                                                                                                                                                                                                                                                    |
| `WORKER_NOT_RELEASED`  | 409      | `updateWorker`: воркер прописан командой, а не со сборкой с сервера                                                                                                                                                                                                                                                                                                                           |
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
  session?: { id; instance; since }; // какая копия бэкенда держит соединение
}

interface ConfigRecord {
  agentId;
  worker;
  key;
  version: number;
  data: unknown;
  updatedAt;
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
interface MetricsPoint {
  at;
  collectedAt;
  host?: Record<string, unknown>;
  workers?: Record<string, unknown>;
}
interface ActionRecord {
  id;
  agentId;
  name: "worker.restart" | "worker.update" | "agent.update" | "agent.rotateKey" | "agent.logs";
  args?;
  status: "done" | "failed";
  result?;
  error?;
  createdAt;
  finishedAt;
  deferred?: true; // итог отложенной замены воркера (action.done)
}
interface ReleaseView {
  // manifest.json из releasesDir
  version;
  publicKey?;
  artifacts: { os; arch; file; sha256; signature?; url }[];
  workers?: { name; version; os; arch; file; sha256; signature?; stopTimeout?; command?; url }[];
}
interface LogEntry {
  at;
  level: "debug" | "info" | "warn" | "error";
  source;
  msg;
  attrs?;
}
```

Типы сообщений (`Hello`, `Status`, `WorkerStatus`, `WorkerManifest`, `Metrics`, `Envelope`, …),
помощники `workerManifest` и `schemaProblems` (тип `SchemaTarget`), константы `LINK_PATH` (путь
связи агентов) и `JOB_EVENTS` (типы событий задач) тоже экспортируются.

### Store

`Store` — интерфейс хранилища, `MemoryStore` — в памяти. Свой Store и пример на Postgres —
[docs/store.md](docs/store.md).

### HTTP-помощники

`sendJSON(res, status, body)` — ответ JSON для своих маршрутов без фреймворка.

## Проверки

```bash
cd sdk/node && npm ci && npm run lint && npm run typecheck && npm test && npm run build
```

Тесты (`node:test`) прогоняют все образцы `spec/examples` между сервером и агентом через SDK с
фейковым агентом на WebSocket, а также сценарии: регистрацию с её пределами, коды закрытия,
повторы важного и потока, `fetch` с потоком, отменой и сроком, настройки и их сверку, манифест
воркера и `schemaProblems`, сводку `watch`, метрики, смену ключа, сборки, отложенную замену
занятого воркера, обнаружение обрыва (ping), несколько копий с
общим Store.
