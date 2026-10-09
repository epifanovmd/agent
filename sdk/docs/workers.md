# Как написать воркер

Воркер — обычный HTTP-сервис на unix-сокете, на любом языке и без SDK. Агент запускает его,
передаёт ему запросы бэкенда и настройки, спрашивает метрики и самочувствие, а воркер сам
сообщает о событиях. Формат — [sdk/spec §12–§13](../spec/README.md#12-воркер), образцы HTTP —
[worker.json](../spec/examples/worker.json).

- [Обязательный минимум](#обязательный-минимум)
- [Что агент даёт воркеру](#что-агент-даёт-воркеру)
- [Что воркер обслуживает](#что-воркер-обслуживает)
- [Манифест: что воркер умеет](#манифест-что-воркер-умеет)
- [Что агент обслуживает для воркера](#что-агент-обслуживает-для-воркера)
- [Python](#python)
- [Node.js](#nodejs)
- [Go](#go)
- [Запуск, остановка, перезапуск](#запуск-остановка-перезапуск)
- [Долгая работа](#долгая-работа)

## Обязательный минимум

Воркер обязан отвечать на два запроса — `GET /health` и `GET /manifest`. Всё остальное
необязательно.

| Запрос          | Ответ                                                                                      |
| --------------- | ------------------------------------------------------------------------------------------ |
| `GET /health`   | `2xx` и JSON с полем `ok` (`true` или `false`): `{ ok, busy?, message?, info? }`           |
| `GET /manifest` | `2xx` и манифест не больше 64 КБ с непустым `version` — [ниже](#манифест-что-воркер-умеет) |

**Регистрация.** После каждого запуска воркера, как только его сокет начал отвечать, агент
задаёт оба запроса (срок ответа — 2 с).

- Оба ответа подходят — воркер **зарегистрирован**: в `status` у него `state: running`. Только
  такой воркер получает настройки и запросы `agents.fetch(...)`, у него спрашивают метрики.
  `ok: false` в `GET /health` регистрации не мешает: это самочувствие воркера, а не ошибка ответа.
- Ответ не подошёл (нет маршрута, не `2xx`, не JSON, нет `ok` или `version`, ошибка в манифесте) —
  в `status` у воркера `state: invalid`, причина — в `status.workers[].message`, бэкенд видит
  проблему `workerInvalid` ([observe.md](observe.md#проблемы)). Процесс работает дальше, но
  настроек и запросов не получает: `agents.fetch(...)` и применение настроек заканчиваются
  ошибкой `WORKER_INVALID`.
- Агент пишет предупреждение в свой журнал и повторяет проверку с растущей паузой. Ответы стали
  подходить — воркер регистрируется и сразу получает сохранённые настройки.
- Зависший воркер (нет ответа на `GET /health` три раза подряд) агент перезапускает и в состоянии
  `invalid`.
- Событие, отправленное до конца первой проверки, ждёт её итога.

Самый простой воркер слушает HTTP на сокете `AGENT_WORKER_SOCKET` и отвечает только на эти два
запроса.

Python (только стандартная библиотека):

```python
import json, os, socketserver
from http.server import BaseHTTPRequestHandler

MANIFEST = {"version": "1.0.0"}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            return self.reply(200, {"ok": True})
        if self.path == "/manifest":
            return self.reply(200, MANIFEST)
        self.reply(404, {"message": "нет маршрута"})

    def reply(self, status, body):
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, fmt, *args):  # журнал — в stdout, агент его сохранит
        print(fmt % args, flush=True)


class Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


path = os.environ["AGENT_WORKER_SOCKET"]
if os.path.exists(path):
    os.unlink(path)
Server(path, Handler).serve_forever()
```

Node.js (только `node:http`):

```js
import { rmSync } from "node:fs";
import { createServer } from "node:http";

const MANIFEST = { version: "1.0.0" };

const json = (res, status, body) =>
  res.writeHead(status, { "content-type": "application/json" }).end(JSON.stringify(body));

const server = createServer((req, res) => {
  if (req.method === "GET" && req.url === "/health") return json(res, 200, { ok: true });
  if (req.method === "GET" && req.url === "/manifest") return json(res, 200, MANIFEST);
  json(res, 404, { message: "нет маршрута" });
});

rmSync(process.env.AGENT_WORKER_SOCKET, { force: true });
server.listen(process.env.AGENT_WORKER_SOCKET);
process.on("SIGTERM", () => server.close(() => process.exit(0)));
```

Go (только стандартная библиотека):

```go
package main

import (
	"encoding/json"
	"net"
	"net/http"
	"os"
)

func main() {
	reply := func(w http.ResponseWriter, body any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, map[string]any{"ok": true})
	})
	mux.HandleFunc("GET /manifest", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, map[string]any{"version": "1.0.0"})
	})

	path := os.Getenv("AGENT_WORKER_SOCKET")
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		panic(err)
	}
	panic(http.Serve(ln, mux))
}
```

Полные примеры с настройками, метриками и событиями — [Python](#python), [Node.js](#nodejs),
[Go](#go).

## Что агент даёт воркеру

Переменные окружения при запуске:

| Переменная            | Что это                                                            |
| --------------------- | ------------------------------------------------------------------ |
| `AGENT_WORKER`        | имя воркера из настроек агента                                     |
| `AGENT_WORKER_SOCKET` | путь unix-сокета, на котором воркер слушает HTTP                   |
| `AGENT_SOCKET`        | путь unix-сокета агента: события, свои настройки, сведения об узле |
| `AGENT_WORKER_TOKEN`  | токен для запросов к агенту: `Authorization: Bearer <токен>`       |
| `AGENT_VERSION`       | версия агента на момент запуска воркера (текущая — `GET /context`) |

Агент сам создаёт каталог сокета. Перед `listen` удалите старый файл сокета, если он остался.

## Что воркер обслуживает

`GET /health` и `GET /manifest` — обязательно ([выше](#обязательный-минимум)). Остальное
необязательно: нет маршрута — `404`, агент считает, что воркер этого не умеет.

| Запрос                 | Когда                                                       | Ответ                                                                      |
| ---------------------- | ----------------------------------------------------------- | -------------------------------------------------------------------------- |
| `PUT /config/{key}`    | после запуска — все сохранённые ключи; потом — новые версии | тело `{ version, data }`; `2xx` — применено, иначе `{ message }` или текст |
| `DELETE /config/{key}` | бэкенд удалил ключ                                          | `2xx` (`404` — тоже успех)                                                 |
| `GET /metrics`         | раз в `metricsIntervalMs` (чаще — пока на узел смотрят)     | любой JSON — до бэкенда дойдёт как есть                                    |
| `GET /health`          | обязательно: при регистрации, потом раз в 10 с              | `{ ok, busy?, message?, info? }`                                           |
| `POST /cleanup`        | удаление агента с узла (`agent uninstall`)                  | убрать всё, что воркер создал на узле; `2xx` — готово                      |
| `GET /manifest`        | обязательно: при регистрации после каждого запуска          | что воркер умеет — [ниже](#манифест-что-воркер-умеет)                      |
| остальные пути         | `agents.fetch(...)` с бэкенда                               | что угодно                                                                 |

- **Настройки.** Агент передаёт только ключи, объявленные в манифесте (`configs`), по одному и
  ждёт ответа (до 30 с). Отказ (не `2xx`) — бэкенд увидит ошибку с текстом ответа, агент
  повторит через 25 с и после следующей регистрации воркера. Применять ли частично — решает
  воркер; версия только растёт.
- **Самочувствие.** Не `2xx` или неверное тело — `ok: false`. Три раза подряд нет ответа —
  агент считает воркер зависшим и перезапускает. `info` — то, что полезно видеть бэкенду:
  версия, занятые порты и т. п. `busy: true` — идёт долгая работа: агент не заменяет воркер, пока
  она не закончится ([ниже](#долгая-работа)). Отвечать на `GET /health` нужно и во время работы.
- **Метрики.** Срок ответа — 2 с, размер — до 1 МБ. Нет ответа — в этой точке воркера нет.
- **Уборка.** Обычная остановка (`SIGTERM`) ничего не убирает: созданное воркером продолжает
  работать. Убирают только по `POST /cleanup`.

## Манифест: что воркер умеет

Манифест — ответ на `GET /manifest`: версия воркера, его ключи настроек (со схемой значения),
маршруты и события. Обязательно только `version`; ключи настроек и события, которыми воркер
пользуется, нужно перечислить — агент сверяется с ними. Бэкенд видит манифест в
`agent.workers[].manifest` и может:

- узнать, умеет ли воркер нужное, — помощник `supports`
  ([sdk/README.md](../README.md#манифест-воркера));
- проверить значение настройки по схеме до отправки агенту — опция `validateConfigs`
  ([configs.md](configs.md#проверка-по-схеме)).

```json
{
  "version": "1.2.0",
  "description": "Эхо",
  "configs": [
    {
      "key": "settings",
      "description": "Как отвечать",
      "schema": {
        "type": "object",
        "properties": { "prefix": { "type": "string", "maxLength": 64 } },
        "additionalProperties": false
      }
    }
  ],
  "routes": [
    { "method": "POST", "path": "/echo", "description": "Текст с префиксом" },
    { "method": "GET", "path": "/items/{id}", "description": "Один элемент" }
  ],
  "events": [{ "type": "example.echoed", "description": "Текст отправлен" }]
}
```

- Обязательно только `version` (от 1 до 64 символов). `schema` —
  [JSON Schema](https://json-schema.org) значения ключа (по умолчанию версия 2020-12; другую
  задаёт `$schema`). `{id}` в `path` — один любой сегмент пути.
- Агент спрашивает манифест после каждого запуска воркера (срок — 2 с) и держит его в памяти.
  Новая сборка с другим манифестом — бэкенд сразу получает новый `status`.
- `version` манифеста бэкенд видит как версию воркера. У воркера из выпуска это версия сборки
  ([releases.md](releases.md)), манифест передаётся как есть.
- Нет маршрута (`404`), нет ответа, нет `version`, манифест больше 64 КБ или с ошибкой (имя ключа
  не по правилу, метод не заглавными буквами, путь без `/` в начале — все пределы в
  [§16](../spec/README.md#16-пределы-и-сроки)) — воркер не зарегистрирован (`state: invalid`,
  [выше](#обязательный-минимум)).
- **Что агент проверяет по манифесту.** Событие, типа которого нет в `events`, агент отклоняет:
  `POST /events` отвечает `400 EVENT_UNDECLARED`, до бэкенда оно не доходит. Ключ настроек,
  которого нет в `configs`, агент воркеру не передаёт: бэкенд видит ошибку `CONFIG_KEY_UNKNOWN`
  ([configs.md](configs.md#статус-применения)); агент проверит ключ снова, когда воркер
  зарегистрируется в следующий раз (например, после обновления).
- **Что агент не проверяет.** Значение настройки по `schema` проверяет бэкенд (опция
  `validateConfigs`), а не агент. Маршруты агент тоже не сверяет: запрос `agents.fetch(...)`
  уходит воркеру как есть.

Python (обработчик из [примера ниже](#python)):

```python
MANIFEST = {
    "version": "1.2.0",
    "configs": [{"key": "settings", "schema": {"type": "object"}}],
    "routes": [{"method": "POST", "path": "/echo"}],
    "events": [{"type": "example.echoed"}],
}

# в do_GET:
if self.path == "/manifest":
    return self.reply(200, MANIFEST)
```

Node.js:

```js
const MANIFEST = {
  version: "1.2.0",
  configs: [{ key: "settings", schema: { type: "object" } }],
  routes: [{ method: "POST", path: "/echo" }],
  events: [{ type: "example.echoed" }],
};

// в обработчике запросов:
if (method === "GET" && url === "/manifest") return json(res, 200, MANIFEST);
```

Go:

```go
mux.HandleFunc("GET /manifest", func(w http.ResponseWriter, _ *http.Request) {
	reply(w, 200, map[string]any{
		"version": "1.2.0",
		"configs": []any{map[string]any{"key": "settings", "schema": map[string]any{"type": "object"}}},
		"routes":  []any{map[string]any{"method": "POST", "path": "/echo"}},
		"events":  []any{map[string]any{"type": "example.echoed"}},
	})
})
```

## Что агент обслуживает для воркера

На сокете `AGENT_SOCKET`, с заголовком `Authorization: Bearer $AGENT_WORKER_TOKEN` (без токена —
`401`):

| Запрос              | Что делает                                                                                                                                                                                                                                 |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `POST /events`      | тело `{ type, data? }` → `202` после записи на диск; бэкенд получит событие и после обрыва связи; очередь полна — `503 OUTBOX_FULL`, не записано по другой причине — `500 INTERNAL`; типа нет в `manifest.events` — `400 EVENT_UNDECLARED` |
| `GET /config/{key}` | `{ version, data }` — сохранённые настройки этого воркера; нет ключа — `404`                                                                                                                                                               |
| `GET /context`      | `{ agent: { id, name, version, labels }, online }` — `online`: есть ли связь с бэкендом                                                                                                                                                    |

Тип события — `^[a-z][a-z0-9._-]{0,63}$`, `data` — до 64 КБ. Агент добавляет имя воркера и время.
Ошибки — JSON `{ code, message }`: неверное тело или тип — `400 MESSAGE_INVALID`, `data` больше
64 КБ — `413 BODY_TOO_LARGE`, нет пути — `404 NOT_FOUND`.

Всё, что воркер пишет в stdout и stderr, агент сохраняет в журнале: бэкенд читает его через
`agents.logs(agentId, { worker })` ([observe.md](observe.md#журнал)).

## Python

Только стандартная библиотека, Python ≥ 3.10.

```python
import http.client, json, os, socket, socketserver, threading
from http.server import BaseHTTPRequestHandler

configs = {}           # ключ → data
lock = threading.Lock()
sent = 0
MANIFEST = {
    "version": "1.2.0",
    "configs": [{"key": "settings", "schema": {"type": "object"}}],
    "routes": [{"method": "POST", "path": "/echo"}],
    "events": [{"type": "example.echoed"}],
}


class AgentConnection(http.client.HTTPConnection):
    """HTTP к агенту по unix-сокету AGENT_SOCKET."""

    def __init__(self):
        super().__init__("agent")

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(os.environ["AGENT_SOCKET"])


def agent(method, path, body=None):
    conn = AgentConnection()
    headers = {"authorization": "Bearer " + os.environ["AGENT_WORKER_TOKEN"]}
    raw = None
    if body is not None:
        raw = json.dumps(body).encode()
        headers["content-type"] = "application/json"
    conn.request(method, path, raw, headers)
    res = conn.getresponse()
    data = res.read()
    conn.close()
    return res.status, (json.loads(data) if data else None)


class Handler(BaseHTTPRequestHandler):
    def reply(self, status, body=None):
        raw = b"" if body is None else json.dumps(body).encode()
        self.send_response(status)
        if body is not None:
            self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def body(self):
        n = int(self.headers.get("content-length") or 0)
        return json.loads(self.rfile.read(n) or b"null")

    def do_PUT(self):
        if self.path.startswith("/config/"):
            req = self.body()
            if not isinstance(req["data"], dict):
                return self.reply(422, {"message": "data: нужен объект"})
            with lock:
                configs[self.path[8:]] = req["data"]
            return self.reply(204)
        self.reply(404)

    def do_DELETE(self):
        if self.path.startswith("/config/"):
            with lock:
                configs.pop(self.path[8:], None)
            return self.reply(204)
        self.reply(404)

    def do_GET(self):
        if self.path == "/health":
            return self.reply(200, {"ok": True, "info": {"keys": sorted(configs)}})
        if self.path == "/manifest":
            return self.reply(200, MANIFEST)
        if self.path == "/metrics":
            return self.reply(200, {"sent": sent})
        self.reply(404, {"message": "нет маршрута"})

    def do_POST(self):
        global sent
        if self.path == "/cleanup":
            # убрать созданное на узле: файлы, правила, службы
            return self.reply(204)
        if self.path == "/echo":
            req = self.body()
            sent += 1
            agent("POST", "/events", {"type": "example.echoed", "data": {"text": req.get("text")}})
            return self.reply(200, {"echo": req})
        self.reply(404, {"message": "нет маршрута"})

    def log_message(self, fmt, *args):  # журнал — в stdout, агент его сохранит
        print(fmt % args, flush=True)


class Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


path = os.environ["AGENT_WORKER_SOCKET"]
if os.path.exists(path):
    os.unlink(path)
Server(path, Handler).serve_forever()
```

## Node.js

Только `node:http`, Node ≥ 18.

```js
import { createServer, request } from "node:http";
import { rmSync } from "node:fs";

const configs = new Map();
let sent = 0;
const MANIFEST = {
  version: "1.2.0",
  configs: [{ key: "settings", schema: { type: "object" } }],
  routes: [{ method: "POST", path: "/echo" }],
  events: [{ type: "example.echoed" }],
};

/** HTTP к агенту по unix-сокету AGENT_SOCKET. */
function agent(method, path, body) {
  return new Promise((resolve, reject) => {
    const raw = body === undefined ? undefined : JSON.stringify(body);
    const req = request(
      {
        socketPath: process.env.AGENT_SOCKET,
        method,
        path,
        headers: {
          authorization: `Bearer ${process.env.AGENT_WORKER_TOKEN}`,
          ...(raw ? { "content-type": "application/json" } : {}),
        },
      },
      (res) => {
        let data = "";
        res.on("data", (c) => (data += c));
        res.on("end", () => resolve({ status: res.statusCode, body: data ? JSON.parse(data) : null }));
      },
    );
    req.on("error", reject);
    req.end(raw);
  });
}

const json = (res, status, body) => {
  if (body === undefined) return res.writeHead(status).end();
  res.writeHead(status, { "content-type": "application/json" }).end(JSON.stringify(body));
};

const readBody = async (req) => {
  let data = "";
  for await (const c of req) data += c;
  return data ? JSON.parse(data) : null;
};

const server = createServer(async (req, res) => {
  const { method, url } = req;
  if (url.startsWith("/config/")) {
    const key = url.slice(8);
    if (method === "PUT") {
      configs.set(key, (await readBody(req)).data);
      return json(res, 204);
    }
    if (method === "DELETE") {
      configs.delete(key);
      return json(res, 204);
    }
  }
  if (method === "GET" && url === "/health") return json(res, 200, { ok: true });
  if (method === "GET" && url === "/manifest") return json(res, 200, MANIFEST);
  if (method === "GET" && url === "/metrics") return json(res, 200, { sent });
  if (method === "POST" && url === "/cleanup") return json(res, 204);
  if (method === "POST" && url === "/echo") {
    const body = await readBody(req);
    sent++;
    await agent("POST", "/events", { type: "example.echoed", data: { text: body?.text } });
    return json(res, 200, { echo: body });
  }
  json(res, 404, { message: "нет маршрута" });
});

rmSync(process.env.AGENT_WORKER_SOCKET, { force: true });
server.listen(process.env.AGENT_WORKER_SOCKET);
process.on("SIGTERM", () => server.close(() => process.exit(0)));
```

## Go

Только стандартная библиотека.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
)

// agent — HTTP-клиент к агенту по unix-сокету AGENT_SOCKET.
var agent = &http.Client{Transport: &http.Transport{
	DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", os.Getenv("AGENT_SOCKET"))
	},
}}

func event(typ string, data any) error {
	body, _ := json.Marshal(map[string]any{"type": typ, "data": data})
	req, _ := http.NewRequest("POST", "http://agent/events", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+os.Getenv("AGENT_WORKER_TOKEN"))
	req.Header.Set("Content-Type", "application/json")
	res, err := agent.Do(req)
	if err != nil {
		return err
	}
	return res.Body.Close()
}

func main() {
	var (
		mu      sync.Mutex
		configs = map[string]json.RawMessage{}
		sent    atomic.Int64
	)
	reply := func(w http.ResponseWriter, status int, body any) {
		if body == nil {
			w.WriteHeader(status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /config/{key}", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Version int64           `json:"version"`
			Data    json.RawMessage `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			reply(w, 422, map[string]string{"message": err.Error()})
			return
		}
		mu.Lock()
		configs[r.PathValue("key")] = req.Data
		mu.Unlock()
		reply(w, 204, nil)
	})
	mux.HandleFunc("DELETE /config/{key}", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		delete(configs, r.PathValue("key"))
		mu.Unlock()
		reply(w, 204, nil)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, 200, map[string]any{"ok": true})
	})
	mux.HandleFunc("GET /manifest", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, 200, map[string]any{
			"version": "1.2.0",
			"configs": []any{map[string]any{"key": "settings", "schema": map[string]any{"type": "object"}}},
			"routes":  []any{map[string]any{"method": "POST", "path": "/echo"}},
			"events":  []any{map[string]any{"type": "example.echoed"}},
		})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, 200, map[string]any{"sent": sent.Load()})
	})
	mux.HandleFunc("POST /cleanup", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, 204, nil) // убрать созданное на узле
	})
	mux.HandleFunc("POST /echo", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		sent.Add(1)
		_ = event("example.echoed", map[string]any{"text": body["text"]})
		reply(w, 200, map[string]any{"echo": body})
	})

	path := os.Getenv("AGENT_WORKER_SOCKET")
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		panic(err)
	}
	srv := &http.Server{Handler: mux}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	go func() { <-ctx.Done(); _ = srv.Shutdown(context.Background()) }()
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		panic(err)
	}
}
```

## Запуск, остановка, перезапуск

Воркеры описываются в настройках агента (`agent.yaml`); добавить или убрать воркер бэкенд не
может.

```yaml
workers:
  - name: echo # ^[a-z][a-z0-9-]{0,31}$
    command: ["python3", "/opt/example/echo.py"]
    env: { LOG_LEVEL: info }
    user: example # по умолчанию — пользователь агента; другой — только у агента от root
    lifecycle: # всё — необязательно; здесь — значения по умолчанию
      onAgentRestart: keep # перезапуск агента воркер не трогает
      onAgentStop: keep # остановка агента — тоже (stop — остановить вместе с ним)
      restart: on-failure # always | on-failure | never — после выхода процесса
      backoff: { min: 1s, max: 30s } # пауза перед перезапуском после падения
      maxRestarts: 0 # падений подряд не больше (0 — без предела)
      startTimeout: 60s # сокет должен начать отвечать
      stopTimeout: 30s
      keepChildren: false # true — дочерние процессы переживают остановку воркера
      busy: { wait: true, timeout: 24h }
      health: { interval: 10s, timeout: 2s, failures: 3 }
      probeTimeout: 2s # срок GET /manifest, GET /metrics и регистрации
      configRetry: 25s # повтор неприменённых настроек
      updateHealthyTimeout: 1m # новая сборка должна ответить ok: true
    logs: { maxSize: 10MB, maxFiles: 1 } # файлы вывода в <dataDir>/logs
  - name: report
    release: true # сборка — из выпуска на бэкенде (releases.md)
```

- **Запуск.** Агент запускает процесс и ждёт, пока сокет начнёт отвечать (до 60 с), затем
  регистрирует воркер (`GET /health` и `GET /manifest`, [выше](#обязательный-минимум)) и передаёт
  ему все сохранённые настройки по `PUT /config/{key}`.
- **Вывод.** stdout и stderr воркера — файлы `<dataDir>/logs/<имя>.log` и `<имя>.err.log`; агент
  читает их в свой журнал (`agents.logs(agentId, { worker })`): строки stdout — с уровнем `info`,
  stderr — `warn`; строка длиннее 16 КБ обрезается. Пишите в них построчно.
- **Остановка воркера** (перезапуск, обновление воркера) — `SIGTERM`; не вышел за `stopTimeout` —
  `SIGKILL`. Копия воркера всегда одна: новая запускается после ухода прежней.
- **Перезапуск и обновление агента** воркер не трогают: он работает дальше, новый агент его
  подхватывает (тот же процесс, тот же токен). Пока агента нет, сокет агента не отвечает —
  повторяйте `POST /events` позже.
- **Выход процесса** — по `lifecycle.restart`; перезапуск — с растущей паузой (1–30 с), в
  `status` — `state: backoff`. Без перезапуска (или после `maxRestarts` падений подряд) —
  `state: stopped` до перезапуска с бэкенда или изменения настроек воркера. В обоих случаях
  бэкенд видит проблему `workerDown`.
- **Перезапуск с бэкенда** — `agents.restartWorker(agentId, "echo")`; пока воркер занят, замена
  ждёт, `{ force: true }` — сразу.

## Долгая работа

Работа, которая идёт минуты и часы (отчёт, обучение, выгрузка), не должна прерываться
перезапусками агента и плановой заменой воркера. Схема:

1. **Запуск — `202`.** Бэкенд вызывает маршрут воркера (`agents.fetch(agentId, "echo", "/work",
{ method: "POST", body })`), воркер начинает работу в фоне и сразу отвечает `202 { id }`: ответ
   `fetch` не ждёт конца работы.
2. **Ход — событиями.** Воркер шлёт `POST /events` на шаге и в конце (`example.progress`,
   `example.done`); бэкенд получает их событием `event` (`agents.on("event", …)`). События хранятся
   на диске агента, пока бэкенд их не подтвердит; если сокет агента не отвечает (агент
   перезапускается), воркер повторяет отправку.
3. **Состояние и отмена — маршрутами.** `GET /work/{id}` — ход работы, `POST /work/{id}/cancel` —
   прервать. Маршруты опишите в манифесте.
4. **`busy` в `GET /health`.** Пока работа идёт, воркер отвечает `{ ok: true, busy: true, message:
"шаг 40 из 100" }`. Агент не заменяет занятый воркер: `worker.restart`, `worker.update` и
   изменение его настроек в `agent.yaml` ждут окончания работы (не дольше `lifecycle.busy.timeout`,
   по умолчанию 24 ч), в `status` — `pending: "restart"` или `"update"`. Итог действия на бэкенде
   приходит после замены; `{ force: true }` — заменить сразу.
5. **Ход — на диск.** Воркер сохраняет ход работы после каждого шага (файл на работу, запись через
   временный файл и переименование) и, запустившись, продолжает незаконченные. Тогда работу не
   теряет ни `force`, ни падение воркера, ни перезагрузка узла.

Перезапуск и обновление самого агента работу не прерывают вовсе: воркер их не замечает. Зависший
воркер (нет ответа на `GET /health` три раза подряд) агент перезапускает и во время работы —
поэтому отвечайте на `GET /health` из другого потока, чем идёт работа.

Пример — `POST /work` воркера echo ([examples/workers/echo](../../examples/workers/echo/main.py)):
ход хранится в `ECHO_WORK_DIR`.

Все настройки агента — [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md). Готовые воркеры-примеры
на Python, Node и Go — [examples/README.md](../../examples/README.md).
