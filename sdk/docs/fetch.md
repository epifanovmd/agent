# Запрос к воркеру

`agents.fetch` — HTTP-запрос к воркеру на узле через агента, как обычный `fetch`: ответ —
стандартный `Response` с потоком тела. Формат — [sdk/spec §7](../spec/README.md#7-запрос-к-воркеру-fetch),
образцы — [fetch.json](../spec/examples/fetch.json).

## Вызов

```ts
const res = await agents.fetch(agentId, "echo", "/echo?upper=1", {
  method: "POST", // по умолчанию GET
  headers: { "content-type": "application/json" }, // объект или Headers
  body: JSON.stringify({ text: "привет" }), // string | Uint8Array | ArrayBuffer
  timeoutMs: 5000, // по умолчанию 30 000, не больше 600 000
  signal: AbortSignal.timeout(10_000), // отмена
});
res.status; // 200
res.headers.get("content-type");
await res.json(); // или text(), arrayBuffer(), bytes(), body — ReadableStream
```

`Response` приходит, как только воркер ответил заголовком, — тело ещё может идти. Строковое тело
уходит как есть, двоичное — в base64. Тело запроса — до 4 МБ, ответа — до 32 МБ.

**Что уходит по сети.** `fetch {worker, method, path, headers, body, timeoutMs}` → агент делает
запрос к сокету воркера → `fetch.head {status, headers}`, куски `fetch.chunk` (до 64 КБ), конец
`fetch.end`. Отмена — `fetch.cancel`.

## Поток

Тело читается по мере прихода кусков — так можно отдавать большие файлы и длинные ответы,
не собирая их целиком:

```ts
const res = await agents.fetch(agentId, "report", "/export");
for await (const chunk of res.body!) out.write(chunk);
```

Отдать ответ воркера клиенту бэкенда как есть:

```ts
import { pipeline } from "node:stream/promises";

const r = await agents.fetch(agentId, "report", "/export");
res.writeHead(r.status, Object.fromEntries(r.headers));
await pipeline(r.body!, res);
```

Перестали читать — `res.body.cancel()`: агент получит `fetch.cancel` и прервёт запрос к воркеру.

## Ошибки

Ошибка до ответа воркера — `fetch` отклоняется с `AgentsError` (`code`, `message`, `status` —
HTTP-статус, с которым её удобно вернуть клиенту). Ошибка после заголовка — ошибка чтения тела
с тем же `AgentsError`.

| Код                  | Статус | Когда                                                                                                                                |
| -------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| `AGENT_NOT_FOUND`    | 404    | нет такого агента                                                                                                                    |
| `AGENT_REVOKED`      | 409    | агент отозван                                                                                                                        |
| `AGENT_OFFLINE`      | 503    | агент без связи                                                                                                                      |
| `AGENT_ELSEWHERE`    | 421    | соединение агента в другой копии бэкенда ([connection.md](connection.md#несколько-копий-бэкенда))                                    |
| `WORKER_UNKNOWN`     | 404    | воркера нет в настройках агента                                                                                                      |
| `WORKER_UNAVAILABLE` | 502    | воркер не запущен или не отвечает                                                                                                    |
| `WORKER_INVALID`     | 502    | воркер не зарегистрирован: не ответил как нужно на `GET /health` или `GET /manifest` ([workers.md](workers.md#обязательный-минимум)) |
| `TIMEOUT`            | 504    | истёк `timeoutMs`                                                                                                                    |
| `CANCELLED`          | 499    | отменено (`signal`)                                                                                                                  |
| `PATH_FORBIDDEN`     | 403    | служебный путь: `/config/*`, `/metrics`, `/health`, `/cleanup`                                                                       |
| `ROUTE_UNDECLARED`   | 404    | метода и пути нет в `routes` манифеста воркера, или задачи (`/jobs*`) у воркера без `jobs` ([ниже](#только-объявленные-маршруты))    |
| `JOB_UNKNOWN`        | 409    | `POST /jobs` с типом, которого нет в `manifest.jobs`                                                                                 |
| `BODY_TOO_LARGE`     | 413    | тело запроса больше 4 МБ или ответа больше 32 МБ                                                                                     |
| `BUSY`               | 503    | у агента уже 64 запроса                                                                                                              |
| `DISCONNECTED`       | 502    | связь с агентом оборвалась до конца ответа                                                                                           |
| `MESSAGE_INVALID`    | 400    | неверное имя воркера, путь или метод                                                                                                 |

Ответ воркера с ошибкой (например, `500`) — не ошибка `fetch`: это обычный `Response` со
статусом воркера.

**Повторы.** Запрос живёт только в текущем соединении: при обрыве он не повторяется
(`DISCONNECTED`). Повторять ли — решает бэкенд: для неидемпотентных действий воркеру лучше
принимать свой ключ повтора — так устроены задачи ([ниже](#задачи)).

## Только объявленные маршруты

Агент пропускает к воркеру только то, что описано в его манифесте ([workers.md](workers.md#манифест-что-воркер-умеет)):

- метод и путь должны подойти под один из `routes`: метод — точно, путь — по сегментам, `{name}`
  — один любой непустой сегмент (кроме `.` и `..`), параметры после `?` не учитываются, `%XX`
  раскрываются. Иначе — `ROUTE_UNDECLARED`, воркер запроса не видит;
- задачи (`POST /jobs`, `GET /jobs/{id}`, `POST /jobs/{id}/cancel`) — только у воркера с `jobs`;
  в `POST /jobs` агент читает из тела `type` и сверяет его с `jobs` (`JOB_UNKNOWN`);
- `GET /manifest` проходит всегда, служебные пути — никогда (`PATH_FORBIDDEN`).

Что объявил воркер, видно в его манифесте — `workerManifest(agent, worker)`
([sdk/README.md](../README.md#манифест-воркера)).

## Проверка тела по схеме

Тело запроса можно проверить до отправки по `routes[].request` подходящего маршрута — помощником
`schemaProblems` (тело — разобранный JSON); `data` задачи — так же, по `jobs[].schema`:

```ts
import { schemaProblems } from "agent-sdk/server";

const agent = await agents.getAgent(agentId);
const body = { text: "привет" };
const problems = agent ? schemaProblems(agent, "echo", { route: { method: "POST", path: "/echo" } }, body) : [];
if (problems.length) throw new Error(problems.join("; ")); // до агента запрос не уходит
await agents.fetch(agentId, "echo", "/echo", { method: "POST", body: JSON.stringify(body) });
// задача: schemaProblems(agent, "report", { job: "example.report" }, data)
```

Нет манифеста, маршрута или схемы — пустой список; схему, которую нельзя применить,
`schemaProblems` не применяет — бросает исключение. `routes[].response` не проверяется: это
описание ответа.

## Задачи

Работу «сделай и сообщи итог» удобнее давать воркеру задачей: `POST /jobs` через тот же `fetch`
с телом `{ type, jobId, data?, files? }`. Агент сверяет тип с манифестом воркера
(`JOB_UNKNOWN`), `jobId` от бэкенда не даёт повтору начать вторую задачу, итог долгой задачи
приходит событиями `job.*`. Файлы задача получает ссылками (`files`) — воркер качает и загружает
их сам, мимо пределов `fetch`. Подробно — [workers.md](workers.md#задачи).
