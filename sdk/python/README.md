# agent-sdk — SDK агента на Python

Python ≥ 3.10, без зависимостей — только стандартная библиотека. Здесь — то, что относится только
к Python. API обеих частей одинаково во всех языках:

- справочник API (Go, Node, Python) — [sdk/README.md](../README.md);
- с чего начать и разбор каждой возможности с примерами — [sdk/docs](../docs/README.md).

| Модуль              | Для чего                                                                                   |
| ------------------- | ------------------------------------------------------------------------------------------ |
| `agent_sdk.worker`  | воркер на узле: задачи, команды, состояние, показатели, события, контекст, здоровье, пауза |
| `agent_sdk.server`  | бэкенд: `Agents` принимает агентов и держит с ними связь (WebSocket или HTTP)              |
| `agent_sdk.message` | константы и помощники для сообщений — если работаете без `worker`/`server`                 |

```
sdk/python/
├── agent_sdk/
│   ├── message.py                 # общее для обеих частей
│   ├── worker/                    # Worker, Job, Command, WorkerContext, Channel, ошибки, файлы задач
│   └── server/
│       ├── agents.py              # Agents: API для бэкенда
│       ├── session.py             # подключение одного агента, AgentsError, HttpReply
│       ├── transport.py           # регистрация, HTTP, WebSocket, файлы, раздача выпуска
│       ├── model.py               # Agent, Job, Command, DesiredState, AgentEvent, MetricsPoint, Alert, …
│       ├── store.py               # Store (интерфейс хранилища) и MemoryStore
│       ├── files.py               # Files (ссылки на файлы задач) и MemoryFiles
│       ├── install.py             # команда установки агента
│       ├── seal.py                # запечатывание секретов (нужен cryptography)
│       └── websockets_adapter.py  # по желанию: WebSocket на библиотеке websockets
├── tests/
└── pyproject.toml                 # пакет agent-sdk
```

## Установка

Готовый пакет из GitHub Release (`<версия>` — номер выпуска, например `1.1.0`):

```bash
pip install https://github.com/epifanovmd/agent/releases/download/v<версия>/agent_sdk-<версия>-py3-none-any.whl
```

Из репозитория — `pip install ./sdk/python`. Необязательные дополнения:

| Дополнение              | Что даёт                                                       |
| ----------------------- | -------------------------------------------------------------- |
| `agent-sdk[websockets]` | `agent_sdk.server.websockets_adapter` на библиотеке websockets |
| `agent-sdk[crypto]`     | `agents.seal` — запечатывание секретов (пакет `cryptography`)  |

Например: `pip install "agent_sdk-<версия>-py3-none-any.whl[crypto]"`.

## Воркер (`agent_sdk.worker`)

Обработчики объявляются декораторами и работают **в потоках**: обычные функции, без asyncio.

```python
from agent_sdk.worker import Command, CommandFailed, Job, JobFailed, StateFailed, Worker, WorkerContext

worker = Worker("report", version="1.0.0")             # имя по умолчанию — из AGENT_WORKER


@worker.job("example.render", concurrency=2)          # задачи из очереди
def render(job: Job) -> dict:
    source = job.input_path("source")
    job.progress(0.5, "строю отчёт")
    if job.stop_requested:                             # просят закончить пораньше
        return {"pages": 1}
    job.check_cancelled()                              # отменили — бросит Cancelled
    if "reportId" not in job.data:
        raise JobFailed("BAD_INPUT", "нет reportId", retryable=False)
    job.upload("report", b"...")
    return {"pages": 3}


@worker.command("example.report.reload")              # команда на этом узле
def reload(cmd: Command) -> dict:
    cmd.write("перечитываю\n")
    if cmd.args.get("strict"):
        raise CommandFailed("RELOAD_FAILED", "шаблоны повреждены")
    return {"templates": 12}


@worker.state("example.report")                       # состояние: повторный вызов с тем же снимком ничего не ломает
def apply(version: int, spec: dict) -> dict:
    failed = apply_templates(spec["templates"])
    if failed:
        raise StateFailed("не применились шаблоны", report={"failed": failed})
    return {"templates": len(spec["templates"])}


@worker.telemetry("example.report", interval="auto")  # с частотой подписки на канал или метрик агента
def stats() -> dict:
    return {"queue": 0}


@worker.on_context                                    # контекст агента: связь, частота метрик, подписки
def changed(ctx: WorkerContext) -> None:
    print("связь:", ctx.online, "метрики раз в", ctx.metrics_interval_ms, "мс", flush=True)


@worker.cleanup                                       # уборка при удалении агента с узла
def cleanup() -> None:
    remove_interfaces()


if __name__ == "__main__":
    worker.set_health(True)                           # или set_health(False, "нет связи с базой")
    worker.run()
```

Запись в настройках агента:

```yaml
workers:
  - name: report
    command: ["/opt/report/.venv/bin/python", "-m", "report_worker"]
    stopTimeout: 5m
```

Особенности Python:

- имена — в `snake_case`: `job.lease_seconds`, `job.stop_requested`, `job.input_path(name)`,
  `job.refresh_urls(inputs, outputs)`, `cmd.timeout_sec`, `worker.set_health`,
  `worker.request_restart`, `ctx.metrics_interval_ms`, `ctx.log_level`;
- отмена задачи и истёкший срок команды — свойство `cancelled` или `check_cancelled()`, который
  бросает `Cancelled`;
- ошибки — исключения `JobFailed(code, message, retryable=True)`, `CommandFailed(code, message)`,
  `StateFailed(message, report=None)`; `AgentError` — агент отклонил запрос воркера;
- `interval` у `telemetry` — секунды (по умолчанию 15) или `"auto"` (`AUTO_INTERVAL`);
- `worker.context` — `WorkerContext` (неизменяемый dataclass): `mode`, `agent` (`ContextAgent`:
  `id`, `name`, `version`, `labels`), `online`, `metrics_interval_ms`, `channels`, `log_level`;
  `@worker.on_context` (или `worker.on_context(fn)`) вызывается в потоке приёма сообщений —
  долгую работу выносите в свой поток;
- `worker.set_health`, `worker.pause(queues=None)`, `worker.resume(queues=None)`,
  `worker.request_restart(reason=None)` можно звать и до `run()` — уйдут сразу после регистрации;
- `worker.stopping` — `threading.Event`: воркер останавливается, пора гасить свои фоновые циклы
  (`while not worker.stopping.wait(10): …`); `worker.drain()` — остановиться самому;
- `worker.rejected` — множество имён, которые агент не принял;
- `Worker(name, version=…, channel=Channel(sock))` — свой канал связи для тестов;
- `worker.run(install_signals=True)` — сам ставит обработчики SIGTERM/SIGINT; `False` — если
  сигналы обрабатывает ваша программа;
- лог SDK — модуль `logging`, логгер `agent_sdk.worker`, вывод в stderr (агент пишет его в свой
  журнал).

## Сервер (`agent_sdk.server`)

`Agents` работает на asyncio и не привязан к веб-фреймворку: все методы API — корутины
(`await agents.enqueue(…)`), кроме `on`, `off`, `by`, `alerts` и `install_command`. Бэкенд
передаёт ему запросы своего HTTP-сервера, а он сам делает всё, что связано с агентами. Полный
пример на стандартной библиотеке — [examples/server-python](../../examples/server-python/README.md).

```python
from agent_sdk.server import Agents

agents = Agents(enroll_token="demo-token", releases_dir="/srv/agent-release", public_key="<base64>")

job = await agents.enqueue("example.render", {"reportId": 42}, max_attempts=3)
cmd = await agents.call("example.report.reload", agent_id=agent_id, timeout_sec=30)
await agents.set_state("example.report", {"header": "ACME"}, agent_id=agent_id)
sub = await agents.subscribe(agent_id, ttl_ms=30000, metrics={"intervalMs": 1000, "groups": ["diskio"]},
                             logs={"level": "debug"}, channels={"example.report": {"intervalMs": 1000}})
await agents.unsubscribe(agent_id, sub["id"])
await agents.pause_worker(agent_id, "report", queues=["example.render"])
agents.on("alert", lambda a: a.active and print(a.type, a.agent_name, a.message))
```

Настройки — те же, что в [sdk/README.md](../README.md#настройки-agents), в `snake_case`:
`enroll_token`, `enroll`, `store`, `files`, `status_interval_ms`, `metrics_interval_ms`,
`offline_grace_ms`, `offline_after_ms`, `metrics_store_interval_ms`, `metrics_retention_ms`,
`enroll_failure_limit`, `enroll_failure_window_ms`, `releases_dir`, `public_key`, `base_url`,
`trust_proxy`, `log` (`logging.Logger` или `fn(msg, extra)`). `enroll(token)` может быть функцией
или корутиной и возвращает `{"labels": {…}}` или `None`.

**Подключение к веб-фреймворку.** Вместо одного `handle` у Python-версии — отдельный метод на
каждый маршрут. Пример на FastAPI/Starlette:

```python
@app.post("/api/v1/agent-link/enroll")
async def enroll(request: Request):
    reply = await agents.handle_enroll(await request.body(), remote=request.client.host)
    status, body = reply
    return JSONResponse(body, status, headers=reply.headers)

@app.post("/api/v1/agent-link/sync")
async def sync(request: Request):
    status, body = await agents.handle_sync(request.headers.get("authorization"), await request.body(),
                                            request.is_disconnected, base_url=str(request.base_url),
                                            remote=request.client.host,
                                            forwarded_for=request.headers.get("x-forwarded-for"))
    return JSONResponse(body, status)

@app.websocket("/api/v1/agent-link")
async def link(ws: WebSocket):
    auth = ws.headers.get("authorization")
    if await agents.authenticate(auth) is None:
        return await ws.close(4401)            # Starlette: закрыть до accept — отказ в подключении
    await ws.accept(subprotocol="agent.v1")
    await agents.serve_websocket(auth, ws, remote=ws.client.host,
                                 forwarded_for=ws.headers.get("x-forwarded-for"))

@app.api_route("/files/{key:path}", methods=["GET", "PUT"])
async def files(key: str, request: Request):
    status, data = await agents.handle_file(request.method, key, await request.body())
    return Response(data, status)

@app.get("/api/v1/agent-link/releases/{name}")
@app.get("/api/v1/agent-link/install.sh")
async def release(request: Request):
    status, data, content_type = await agents.handle_release(request.url.path, str(request.base_url))
    return Response(data, status, media_type=content_type)
```

| Метод                                                                                                                                     | Что                                                                                                                                                              |
| ----------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `await agents.handle_enroll(body, remote=None)` → `HttpReply`                                                                             | регистрация; `remote` — адрес клиента для ограничения попыток. Ответ распаковывается как `(status, dict)`, заголовки (`Retry-After` у `429`) — в `reply.headers` |
| `await agents.authenticate(authorization)` → `Agent \| None`                                                                              | проверить ключ агента до того, как принять WebSocket                                                                                                             |
| `await agents.handle_sync(authorization, body, is_disconnected=None, *, base_url="", remote=None, forwarded_for=None)` → `(status, dict)` | обмен по HTTP; `is_disconnected()` (функция или корутина) — клиент ушёл, неотданное остаётся до следующего запроса                                               |
| `await agents.serve_websocket(authorization, conn, *, base_url="", remote=None, forwarded_for=None)`                                      | обслужить принятый WebSocket: у `conn` нужны `receive_text()`, `send_text(str)`, `close(code)`; ping — забота веб-сервера                                        |
| `await agents.handle_file(method, key, body)` → `(status, bytes)`                                                                         | `GET`/`PUT /files/<jobId>/(in\|out)/<имя>` для `MemoryFiles`                                                                                                     |
| `await agents.handle_release(path, base_url)` → `(status, bytes, content_type)`                                                           | манифест, сборки (только из манифеста) и `install.sh` с вписанными адресом и ключом                                                                              |
| `await agent_sdk.server.websockets_adapter.serve(agents, host, port)`                                                                     | готовый WebSocket-сервер для агентов на библиотеке websockets (HTTP-маршруты — своим сервером)                                                                   |

`remote` и `forwarded_for` дают адрес агента (`Agent.address`); `X-Forwarded-For` учитывается
только с `trust_proxy=True`.

Прочее:

- `agents.on(event, fn)` принимает функцию или корутину и возвращает функцию отписки
  (`agents.off(event, fn)` — тоже); события — `change` (`fn(Change)`), `metrics`
  (`fn(agent_id, point)`), `stateDeleted` (`fn(domain, agent_id)`), `audit` (`fn(AuditEntry)`),
  `alert` (`fn(Alert)`), `log` (`fn(agent_id, entries)`);
- методы — с именованными аргументами: `enqueue(queue, data, *, max_attempts=1, lease_seconds=60,
inputs=None, outputs=None, agent_id=None)`, `command(name, args, *, timeout_sec=60,
agent_id=None)`, `subscribe(agent_id, *, id=None, ttl_ms=30000, status=None, metrics=None,
logs=None, channels=None)` (части подписки — словари с ключами в camelCase:
  `{"intervalMs": 1000}`), `install_command(token=…, packages=[…], …)`;
- ошибки — `AgentsError(code, message, status)`;
- модели — dataclass'ы: `to_dict()` — JSON для интерфейса, без ключа агента и служебных полей;
  `to_record()` / `from_record()` — все поля, для своего `Store`;
- `Store` — асинхронный интерфейс с методами в `snake_case` (`create_agent`, `set_state(domain,
agent_id, spec, *, actor=None)`, `prune_metrics(before)`, …), список и правила —
  [sdk/docs/store.md](../docs/store.md).

## Тесты

```bash
make sdk-test      # или: cd sdk/python && python3 -m unittest discover -s tests -t .
```

Тесты WebSocket на библиотеке websockets и запечатывания секретов пропускаются, если
`websockets` или `cryptography` не установлены.
