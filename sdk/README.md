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

| Где                                      | О чём                                                                   |
| ---------------------------------------- | ----------------------------------------------------------------------- |
| [docs/README.md](docs/README.md)         | с чего начать: бэкенд, агент и воркер за 10 минут                       |
| [docs/connection.md](docs/connection.md) | регистрация, связь, отзыв, смена ключа, несколько процессов, фреймворки |
| [docs/workers.md](docs/workers.md)       | воркер без SDK на Python, Node и Go                                     |
| [docs/fetch.md](docs/fetch.md)           | запросы к воркеру                                                       |
| [docs/configs.md](docs/configs.md)       | настройки воркеров                                                      |
| [docs/observe.md](docs/observe.md)       | метрики, watch, журнал, события, проблемы                               |
| [docs/releases.md](docs/releases.md)     | выпуск, установка, обновления                                           |
| [docs/store.md](docs/store.md)           | хранилище, пример на Postgres                                           |
| [spec/README.md](spec/README.md)         | формат сообщений — источник правды; образцы — `spec/examples`           |

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

| Опция                   | По умолчанию                            | Что это                                                                                                           |
| ----------------------- | --------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `enrollToken`           | —                                       | токен регистрации агентов                                                                                         |
| `enroll(token, info)`   | —                                       | своя проверка: `false`, `null` или `undefined` — отказ, `true` или `{ labels?, name? }` — принять                 |
| `store`                 | `MemoryStore`                           | хранилище ([store.md](docs/store.md))                                                                             |
| `instanceId`            | случайный                               | имя процесса в `agent.session.instance`                                                                           |
| `statusIntervalMs`      | 30 000                                  | как часто агент шлёт `status`                                                                                     |
| `metricsIntervalMs`     | 10 000                                  | как часто агент шлёт метрики                                                                                      |
| `onEvent(event)`        | —                                       | обработчик событий воркеров; подтверждение агенту — после него ([observe](docs/observe.md#события-воркеров))      |
| `offlineGraceMs`        | 20 000                                  | агент после обрыва ещё `online` столько                                                                           |
| `offlineAfterMs`        | max(3 × statusIntervalMs, 30 с) + grace | без вестей дольше — `offline` (процесс с соединением упал)                                                        |
| `actionTimeoutMs`       | 60 000                                  | срок итога действия                                                                                               |
| `updateTimeoutMs`       | 300 000                                 | срок итога `updateAgent`, `updateWorker`                                                                          |
| `releasesDir`           | —                                       | каталог выпуска: manifest.json, сборки, install.sh                                                                |
| `publicKey`             | —                                       | ключ проверки выпуска для install.sh                                                                              |
| `baseUrl`               | из запроса                              | публичный адрес бэкенда для install.sh и `installCommand`                                                         |
| `enrollFailureLimit`    | 10                                      | неудачных регистраций с адреса за окно, дальше — 429 (`0` — без предела)                                          |
| `enrollFailureWindowMs` | 60 000                                  | окно подсчёта неудачных регистраций                                                                               |
| `trustProxy`            | `false`                                 | адрес клиента из `X-Forwarded-For`, адрес сервера — из `X-Forwarded-Host/Proto`                                   |
| `validateConfigs`       | `false`                                 | `setConfig` проверяет значение по схеме ключа из манифеста воркера ([configs](docs/configs.md#проверка-по-схеме)) |
| `log(msg, extra)`       | строки `agents: …` в stdout             | журнал SDK                                                                                                        |

### Методы

| Метод                                                                               | Результат                                     | Раздел                                                                    |
| ----------------------------------------------------------------------------------- | --------------------------------------------- | ------------------------------------------------------------------------- |
| `handle(req, res)`                                                                  | `Promise<boolean>` — запрос обработан         | [connection](docs/connection.md#подключение-к-http-серверу-и-фреймворкам) |
| `attach(server)`                                                                    | WebSocket агентов на этом сервере             | там же                                                                    |
| `close()`                                                                           | `Promise<void>`; соединения — код 1012        | там же                                                                    |
| `by(actor)`                                                                         | `Actor` — изменяющие методы с автором         | [connection](docs/connection.md#агенты-список-отзыв-удаление)             |
| `listAgents()`, `getAgent(id)`                                                      | `Agent[]`, `Agent \| undefined`               | там же                                                                    |
| `revoke(id)`, `deleteAgent(id)`                                                     | `Agent`, `void`                               | там же                                                                    |
| `rotateKey(id, { timeoutMs? })`                                                     | `void`                                        | [connection](docs/connection.md#смена-ключа)                              |
| `refresh(agentId?)`                                                                 | применить изменения других процессов          | [connection](docs/connection.md#несколько-процессов-бэкенда)              |
| `fetch(id, worker, path, { method, headers, body, timeoutMs, signal })`             | `Response`                                    | [fetch](docs/fetch.md)                                                    |
| `setConfig(id, worker, key, data)`                                                  | `ConfigRecord` (с `version`)                  | [configs](docs/configs.md)                                                |
| `getConfig(id, worker, key)`, `listConfigs(id)`                                     | `ConfigRecord \| undefined`, `ConfigRecord[]` | там же                                                                    |
| `deleteConfig(id, worker, key)`                                                     | `boolean` — ключ был                          | там же                                                                    |
| `configStatus(id, worker?)`                                                         | `ConfigStatus[]`                              | там же                                                                    |
| `watch(id, { id?, metricsIntervalMs?, logLevel?, ttlMs? })`, `unwatch(id, watchId)` | `{ id, until }`, `void`                       | [observe](docs/observe.md#наблюдение-watch)                               |
| `listAlerts(agentId?)`                                                              | `Alert[]` — текущие проблемы                  | [observe](docs/observe.md#проблемы)                                       |
| `logs(id, { worker?, lines? })`                                                     | `LogEntry[]`                                  | [observe](docs/observe.md#журнал)                                         |
| `restartWorker(id, name, { force?, timeoutMs? })`                                   | `void`                                        | [workers](docs/workers.md#долгая-работа)                                  |
| `updateWorker(id, name, { force?, timeoutMs? })`, `updateAgent(id, { timeoutMs? })` | `{ version, previous? }`                      | [releases](docs/releases.md)                                              |
| `release()`, `updateCandidates()`, `workerUpdateCandidates()`                       | манифест и кандидаты на обновление            | [releases](docs/releases.md)                                              |
| `installCommand(opts)`                                                              | строка `curl … \| sudo sh -s -- …`            | [releases](docs/releases.md#установка-одной-командой)                     |

`fetch` и действия (`restartWorker`, `updateWorker`, `updateAgent`, `rotateKey`, `logs`)
работают в процессе, у которого соединение агента, `watch` действует там же; остальное — в
любом. Итог действия — результат промиса вызова и событие `action`. `restartWorker` и
`updateWorker` занятого воркера (`health.busy`) ждут окончания его работы (`pending` в status;
срок `timeoutMs` отсчитывается заново, пока замена ждёт); `force: true` — заменить сразу.

### Манифест воркера

Воркер обязан описать себя манифестом — версия (обязательно), ключи настроек со схемой,
маршруты, события ([workers](docs/workers.md#манифест-что-воркер-умеет)). Без корректного
манифеста агент не регистрирует воркер (`state: invalid`). SDK принимает манифест от агента
терпимо: неверные и незнакомые поля пропускаются. Бэкенд видит его в
`agent.workers[].manifest` (`WorkerManifest`), а проверять удобно помощниками:

```ts
import { matchRoute, supports, workerManifest } from "agent-sdk/server";

const agent = await agents.getAgent(agentId);
if (!agent) throw new Error("нет агента");

supports(agent, "echo", { route: { method: "POST", path: "/echo" } }); // true — маршрут описан
supports(agent, "echo", { config: "settings", event: "echo.done" }); // все условия сразу
workerManifest(agent, "echo"); // WorkerManifest | undefined
matchRoute("/items/{id}", "/items/42?full=1"); // true: {id} — один сегмент, ?… не учитывается
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
agents.on("metrics", (m: MetricsEvent) => {}); // каждая точка метрик
agents.on("log", ({ agentId, entries }: LogEvent) => {}); // журнал агента
agents.on("config", (s: ConfigStatus) => {}); // статус ключа настроек изменился
agents.on("alert", (a: AlertEvent) => {}); // проблема началась (active) или закончилась
agents.on("action", (a: ActionRecord) => {}); // итог действия
agents.on("audit", (e: AuditEntry) => {}); // кто что сделал
agents.on("change", ({ agentId, reason }: ChangeEvent) => {}); // другим процессам — refresh(agentId)
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
| `AGENT_ELSEWHERE`      | 421      | соединение агента в другом процессе                                                                                                                                                                                                                                                                                                                                                           |
| `TIMEOUT`              | 504      | нет ответа или итога в срок                                                                                                                                                                                                                                                                                                                                                                   |
| `CANCELLED`            | 499      | `fetch` отменён (`signal`); действие ждало итога, а `Agents` остановили (`close()`)                                                                                                                                                                                                                                                                                                           |
| `CONFLICT`             | 409      | запись агента в Store всё время меняется: условная запись не удалась 20 раз подряд ([store](docs/store.md#правила))                                                                                                                                                                                                                                                                           |
| `BODY_TOO_LARGE`       | 413      | значение настройки больше 4 МБ, тело запроса больше 4 МБ                                                                                                                                                                                                                                                                                                                                      |
| `UPDATE_NOT_AVAILABLE` | 409      | нет выпуска или сборки под агента                                                                                                                                                                                                                                                                                                                                                             |
| `WORKER_NOT_RELEASED`  | 409      | воркер не из выпуска                                                                                                                                                                                                                                                                                                                                                                          |
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
  alerts: Alert[];
  session?: { id; instance; since }; // какой процесс держит соединение
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
  state: "pending" | "applying" | "applied" | "failed" | "deleting";
  error?;
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
воркера и проверку настроек по схеме, сводку `watch`, метрики, проблемы, смену ключа, выпуск и
несколько процессов с общим Store.
