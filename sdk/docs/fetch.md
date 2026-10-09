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
| `AGENT_ELSEWHERE`    | 421    | соединение агента в другой копии бэкенда, а `relay` не задан ([connection.md](connection.md#несколько-копий-бэкенда))                |
| `RELAY_FAILED`       | 502    | `relay`: запрос не доставлен в копию бэкенда с соединением агента                                                                    |
| `WORKER_UNKNOWN`     | 404    | воркера нет в настройках агента                                                                                                      |
| `WORKER_UNAVAILABLE` | 502    | воркер не запущен или не отвечает                                                                                                    |
| `WORKER_INVALID`     | 502    | воркер не зарегистрирован: не ответил как нужно на `GET /health` или `GET /manifest` ([workers.md](workers.md#обязательный-минимум)) |
| `TIMEOUT`            | 504    | истёк `timeoutMs`                                                                                                                    |
| `CANCELLED`          | 499    | отменено (`signal`)                                                                                                                  |
| `PATH_FORBIDDEN`     | 403    | служебный путь: `/config/*`, `/metrics`, `/health`, `/cleanup`                                                                       |
| `ROUTE_UNDECLARED`   | 404    | метода и пути нет в `routes` манифеста воркера, или задачи (`/jobs*`) у воркера без `jobs` ([ниже](#только-объявленные-маршруты))    |
| `JOB_UNKNOWN`        | 409    | `POST /jobs` с типом, которого нет в `manifest.jobs`                                                                                 |
| `REQUEST_INVALID`    | 400    | опция `validateRequests`: тело не JSON или не по `routes[].request` ([ниже](#проверка-тела-по-схеме))                                |
| `BODY_TOO_LARGE`     | 413    | тело запроса больше 4 МБ или ответа больше 32 МБ                                                                                     |
| `BUSY`               | 503    | у агента уже 64 запроса                                                                                                              |
| `DISCONNECTED`       | 502    | связь с агентом оборвалась до конца ответа                                                                                           |
| `MESSAGE_INVALID`    | 400    | неверное имя воркера, путь или метод                                                                                                 |

Ответ воркера с ошибкой (например, `500`) — не ошибка `fetch`: это обычный `Response` со
статусом воркера.

**Повторы.** Запрос живёт только в текущем соединении: при обрыве он не повторяется
(`DISCONNECTED`). Повторять ли — решает бэкенд: для неидемпотентных действий воркеру лучше
принимать свой ключ повтора — так устроены задачи ([ниже](#задачи)).

**Другая копия бэкенда.** Соединение агента — в другой копии, а опция `relay` задана: запрос
уходит туда, ответ воркера приходит потоком так же, отмена и `timeoutMs` действуют
([connection.md](connection.md#пересылка-вызовов-relay)).

## Только объявленные маршруты

Агент пропускает к воркеру только то, что описано в его манифесте ([workers.md](workers.md#манифест-что-воркер-умеет)):

- метод и путь должны подойти под один из `routes`: метод — точно, путь — по сегментам, `{name}`
  — один любой непустой сегмент (кроме `.` и `..`), параметры после `?` не учитываются, `%XX`
  раскрываются. Иначе — `ROUTE_UNDECLARED`, воркер запроса не видит;
- задачи (`POST /jobs`, `GET /jobs/{id}`, `POST /jobs/{id}/cancel`) — только у воркера с `jobs`;
  в `POST /jobs` агент читает из тела `type` и сверяет его с `jobs` (`JOB_UNKNOWN`);
- `GET /manifest` проходит всегда, служебные пути — никогда (`PATH_FORBIDDEN`).

Проверить заранее — `agents.capabilities(agentId, worker)` или помощник `supports`
([sdk/README.md](../README.md#манифест-воркера)). Узел, на котором воркер ещё не описал свои
маршруты, может выключить проверку для него — `routes: open` в `agent.yaml`
([ARCHITECTURE.md](../../docs/ARCHITECTURE.md#настройки)).

## Проверка тела по схеме

С опцией `validateRequests: true` SDK до отправки проверяет тело запроса по `routes[].request`
подходящего маршрута: тело должно быть JSON (строка или байты UTF-8; нет тела — проверяется
`null`) и подходить под схему. Не подходит — `AgentsError` `REQUEST_INVALID` (400) с замечаниями,
до агента запрос не уходит. У маршрута нет схемы или схему нельзя применить — без проверки (второе —
с записью в журнал). `routes[].response` не проверяется: это описание ответа.

## Задачи

Работу «сделай и сообщи итог» удобнее давать воркеру задачей: `agents.runJob(agentId, worker,
{ type, jobId?, data?, files? })` — это `POST /jobs` через тот же `fetch`, но SDK сам проверяет
тип по манифесту воркера, задаёт `jobId` (повтор не начинает вторую задачу) и ждёт итога долгой
задачи по событиям `job.*`. Файлы задача получает ссылками (`files`) — воркер качает и
загружает их сам, мимо пределов `fetch`. Подробно — [workers.md](workers.md#задачи).

## Аудит

`agents.by(actor).fetch(...)` — то же с записью в событие `audit` (`action: "fetch"`, воркер,
метод, путь). Без `by` запросы в аудит не попадают.
