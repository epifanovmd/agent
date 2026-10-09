# С чего начать: бэкенд, агент и воркер

Здесь — путь от нуля до работающей связки на своей машине: бэкенд принимает агента, агент
запускает воркер, бэкенд отправляет воркеру запрос и задаёт ему настройки. Подробности по
каждой возможности — в разделах ниже, справочник API — [sdk/README.md](../README.md), формат
сообщений — [sdk/spec](../spec/README.md).

## Кто за что отвечает

| Часть                                      | Где работает          | Что делает                                                                                                                                       |
| ------------------------------------------ | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| **бэкенд** + SDK (`Agents` из `agent-sdk`) | ваш сервер            | решает, **что** и **кому** сделать: шлёт запросы воркерам, задаёт настройки, смотрит метрики и события; хранит всё в вашей БД                    |
| **агент**                                  | каждый узел           | держит связь с бэкендом, запускает воркеры и следит за ними, хранит настройки и важные сообщения на диске, собирает метрики узла, обновляет себя |
| **воркер** — обычный HTTP-сервис, без SDK  | узел, рядом с агентом | решает, **как** сделать: отвечает на запросы, применяет настройки, отдаёт свои метрики и самочувствие, присылает события                         |

Агент один на все проекты и ничего не знает о том, что делают воркеры: он передаёт запросы,
настройки, события и метрики, не разбирая их.

```
бэкенд (Agents) ──WebSocket──► агент ──HTTP по unix-сокету──► воркер
   fetch(…)        fetch ─────────►   PUT/GET/POST … ─────────►  ваш обработчик
   ответ      ◄──── fetch.head/chunk/end  ◄─────────── ответ HTTP
```

Соединение всегда открывает агент: узлу не нужен открытый порт.

## Шаг 1. Бэкенд

SDK — пакет `agent-sdk` для Node.js ≥ 24 (установка — [sdk/README.md](../README.md#установка)).
`Agents` сам принимает регистрацию, WebSocket, подтверждения и повторы; бэкенду остаются его
API и решения.

```ts
// server.ts
import { createServer } from "node:http";
import { Agents } from "agent-sdk/server";

const agents = new Agents({ enrollToken: "demo-token" }); // хранилище — в памяти (MemoryStore)

const server = createServer(async (req, res) => {
  if (await agents.handle(req, res)) return; // регистрация, выпуск, install.sh
  res.writeHead(404).end(); // здесь — ваше API
});
agents.attach(server); // WebSocket агентов на том же порту
server.listen(8080);

agents.on("agent", (a) => console.log("агент", a.name, a.online ? "на связи" : "без связи"));
agents.on("event", (e) => console.log("событие", e.worker, e.type, e.data));
```

Без `enrollToken` (или своей проверки `enroll`) новые агенты не зарегистрируются. Как устроены
регистрация и связь, как подключить к Express, Fastify и NestJS — [connection.md](connection.md).

## Шаг 2. Воркер

Воркер — любая программа, которая слушает HTTP на unix-сокете из переменной
`AGENT_WORKER_SOCKET`. Обязательны два маршрута: `GET /health` (самочувствие) и `GET /manifest`
(что воркер умеет, минимум `version`) — без них агент не зарегистрирует воркер и не передаст ему
ни настроек, ни запросов. Остальные служебные маршруты необязательны: нет маршрута — `404`, агент
считает, что воркер этого не умеет. Ключи настроек и маршруты воркер перечисляет в манифесте.

```python
# echo.py — Python ≥ 3.10, только стандартная библиотека
import json, os, socketserver
from http.server import BaseHTTPRequestHandler

config = {}
MANIFEST = {
    "version": "1.0.0",
    "configs": [{"key": "main"}],                   # ключи настроек, которые воркер принимает
    "routes": [{"method": "POST", "path": "/echo"}],
}

class Handler(BaseHTTPRequestHandler):
    def reply(self, status, body=None):
        raw = b"" if body is None else json.dumps(body).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def body(self):
        return json.loads(self.rfile.read(int(self.headers.get("content-length") or 0)) or b"{}")

    def do_GET(self):
        if self.path == "/health":
            return self.reply(200, {"ok": True, "message": "работаю"})
        if self.path == "/manifest":
            return self.reply(200, MANIFEST)
        if self.path == "/metrics":
            return self.reply(200, {"keys": len(config)})
        self.reply(404, {"message": "нет маршрута"})

    def do_PUT(self):
        if self.path.startswith("/config/"):          # настройки от бэкенда
            config[self.path[8:]] = self.body()["data"]
            return self.reply(204)
        self.reply(404)

    def do_POST(self):
        if self.path == "/echo":                      # свой маршрут — для agents.fetch
            return self.reply(200, {"echo": self.body(), "config": config})
        self.reply(404)

    def log_message(self, fmt, *args):                # журнал — в stdout
        print(fmt % args, flush=True)

class Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True

path = os.environ["AGENT_WORKER_SOCKET"]
if os.path.exists(path):
    os.unlink(path)
Server(path, Handler).serve_forever()
```

Воркер на Node и Go, события, уборка при удалении агента — [workers.md](workers.md).

## Шаг 3. Агент

Агент на своей машине собирается из исходников: `make build` в корне репозитория кладёт его в
`dist/1.0.0/agent-<os>-<arch>` под эту машину (например, `agent-darwin-arm64`). Go ставить не
нужно — сборка идёт в контейнере, нужен Docker. Рядом с `echo.py` положите настройки агента:

```yaml
# agent.yaml
server:
  url: http://127.0.0.1:8080
name: dev-01
dataDir: ./.agent-data
workers:
  - name: echo
    command: ["python3", "./echo.py"]
```

```bash
cp <репозиторий>/dist/1.0.0/agent-darwin-arm64 ./agent   # сборка под эту машину
AGENT_ENROLL_TOKEN=demo-token ./agent run -config agent.yaml
```

Агент регистрируется по токену, сохраняет свой ключ в `dataDir`, подключается и запускает
воркер. Все настройки агента — [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md). Готовый
стенд со сквозными тестами — [examples/README.md](../../examples/README.md).

## Шаг 4. Работа с воркером

```ts
const [agent] = await agents.listAgents();

// Запрос к воркеру — как обычный fetch, ответ — Response.
const res = await agents.fetch(agent.id, "echo", "/echo", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ text: "привет" }),
});
console.log(res.status, await res.json());

// Настройки: агент хранит их на диске и передаёт воркеру, в том числе после перезапуска.
const { version } = await agents.setConfig(agent.id, "echo", "main", { greeting: "привет" });
console.log(await agents.configStatus(agent.id, "echo")); // [{ key: "main", version, state: "applied", … }]
```

## Шаг 5. Настоящий узел

Бэкенд собирает команду установки, её выполняют на узле (SSH, cloud-init, Ansible):

```ts
agents.installCommand({ token: "demo-token", name: "node-01", baseUrl: "https://api.example.com" });
// curl -fsSL 'https://api.example.com/api/v1/agent-link/install.sh' | sudo sh -s -- --token 'demo-token' --name 'node-01'
```

Выпуск, установка и обновления — [releases.md](releases.md).

## Разделы

| Раздел                         | О чём                                                                           |
| ------------------------------ | ------------------------------------------------------------------------------- |
| [connection.md](connection.md) | регистрация, связь, отзыв, смена ключа, несколько копий и пересылка, фреймворки |
| [workers.md](workers.md)       | как написать воркер без SDK на Python, Node и Go; задачи; долгая работа         |
| [fetch.md](fetch.md)           | запросы к воркеру: поток, отмена, сроки, ошибки                                 |
| [configs.md](configs.md)       | настройки воркеров: версии, доставка, статус применения                         |
| [observe.md](observe.md)       | метрики, наблюдение (watch), журнал, события, проблемы                          |
| [releases.md](releases.md)     | выпуск, установка, обновление агента и воркеров, удаление                       |
| [store.md](store.md)           | хранилище: свой Store, пример на Postgres                                       |
