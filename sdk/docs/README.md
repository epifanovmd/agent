# С чего начать: бэкенд, агент и воркер за 10 минут

Здесь — путь от нуля до работающей связки на своей машине: бэкенд принимает агента, агент
запускает воркер, бэкенд ставит задачу, отправляет команду и задаёт состояние. Потом — то же
самое на настоящем узле. Подробности по каждой возможности — в разделах ниже, справочник API
всех трёх языков — в [sdk/README.md](../README.md).

## Кто за что отвечает

| Часть                                                                              | Где работает                 | Что делает                                                                                                                        |
| ---------------------------------------------------------------------------------- | ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| **бэкенд** + серверная часть SDK (объект `Agents`)                                 | ваш сервер                   | решает, **что** и **кому** сделать: ставит задачи, отправляет команды, задаёт состояние; принимает агентов, хранит всё в вашей БД |
| **агент**                                                                          | каждый узел                  | держит связь, работает без связи, хранит важное на диске, запускает воркеры, присылает метрики, обновляет себя                    |
| **воркер** + SDK воркера (`agent_sdk.worker`, `agent-sdk/worker`, `sdk/go/worker`) | каждый узел, рядом с агентом | решает, **как** сделать: выполняет задачи и команды, приводит узел к состоянию, присылает свои показатели и события               |

Агент один на все проекты: своей сборки под проект нет. Всё, что нужно проекту на узле, — это
его воркеры.

```
 бэкенд (Agents)  ◀── WebSocket или HTTP, соединение открывает агент ──▶  агент  ◀── канал на узле (fd 3) ──▶  воркер
   ставит задачу          job.assign  ─────────────────────────────────▶   выбирает воркер   ── job.assign ──▶  обработчик
   видит итог     ◀─────────────────────────────────  job.complete  ◀──   хранит на диске   ◀── job.complete ──  return
```

## Что выбрать: задача, команда или состояние

| Что           | Когда                                                                                     | Раздел                     |
| ------------- | ----------------------------------------------------------------------------------------- | -------------------------- |
| **задача**    | работу нужно сделать один раз на любом подходящем узле (или на конкретном), с повтором    | [jobs.md](jobs.md)         |
| **команда**   | действие на конкретном узле прямо сейчас, с ответом; если узел недоступен — уже не нужно  | [commands.md](commands.md) |
| **состояние** | «на узле должно быть так» — узел придёт к этому и после выключения, и после ручных правок | [state.md](state.md)       |

## Шаг 1. Бэкенд

Установка SDK — в [sdk/README.md](../README.md#установка). Бэкенд создаёт `Agents` и отдаёт ему
маршруты агентов (`/api/v1/agent-link/…`, `/files/…`). Всё остальное — регистрацию, WebSocket и
HTTP, подтверждения, повторы, раздачу задач — `Agents` делает сам.

```ts
// server.ts — Node.js ≥ 24
import { createServer } from "node:http";
import { Agents } from "agent-sdk/server";

const agents = new Agents({ enrollToken: "demo-token" }); // хранилище — в памяти (MemoryStore)

const server = createServer(async (req, res) => {
  if (await agents.handle(req, res)) return; // регистрация, HTTP sync, файлы задач, выпуск, install.sh
  res.writeHead(404).end(); // здесь — ваше API
});
agents.attach(server); // WebSocket агентов на том же порту
server.listen(8080);

agents.on("agent", (a) => console.log("агент", a.name, a.online ? "на связи" : "без связи"));
agents.on("job", (j) => console.log("задача", j.id, j.status, j.result ?? ""));
```

То же на Go и Python:

```go
agents := server.New(server.Options{EnrollToken: "demo-token"})
defer agents.Close()
http.ListenAndServe(":8080", agents.Handler()) // или agents.Mount(mux) рядом со своим API
```

```python
from agent_sdk.server import Agents
from agent_sdk.server.asgi import AgentsApp

agents = Agents(enroll_token="demo-token")
app = AgentsApp(agents, fallback=api)  # маршруты агентов, остальное — api (FastAPI и т. п.)
# запуск — любой ASGI-сервер: uvicorn module:app
```

В Python маршруты агентов отдаёт ASGI-приложение `AgentsApp(agents)` или, без ASGI, методы
`handle_enroll`, `handle_sync`, `serve_websocket`, `handle_file`, `handle_release`. Подключение к
FastAPI/Starlette — в [sdk/python/README.md](../python/README.md#сервер-agent_sdkserver), сервер на
стандартной библиотеке — [examples/server-python](../../examples/server-python/README.md).

Без `enrollToken` (или своей проверки `enroll`) агенты не зарегистрируются. Как устроены
регистрация и связь — [connection.md](connection.md).

## Шаг 2. Воркер

Воркер — обычная программа, которая ждёт работу. Её запускает агент и сразу даёт канал связи
(дескриптор из `AGENT_IPC_FD`); SDK находит его сам.

```python
# report_worker.py — Python ≥ 3.10, pip install agent_sdk-<версия>-py3-none-any.whl
from agent_sdk.worker import Command, Job, Worker

worker = Worker("report", version="1.0.0")


@worker.job("example.render", concurrency=2)        # задачи очереди
def render(job: Job) -> dict:
    job.progress(0.5, "строю отчёт")
    return {"pages": 3, "title": job.data.get("title")}


@worker.command("example.report.reload")            # команда «сделай сейчас»
def reload(cmd: Command) -> dict:
    cmd.write("перечитываю шаблоны\n")
    return {"templates": 12}


@worker.state("example.report")                     # состояние: снимок целиком
def apply(version: int, spec: dict) -> dict:
    return {"header": spec.get("header")}           # отчёт о применении


@worker.telemetry("example.report", interval="auto")  # показатели — вместе с метриками узла
def stats() -> dict:
    return {"queue": 0}


if __name__ == "__main__":
    worker.run()
```

То же на Go и Node:

```go
w := worker.New("report", "1.0.0")
w.Job("example.render", 2, func(ctx context.Context, j *worker.Job) (any, error) {
	j.Progress(0.5, "строю отчёт")
	return map[string]any{"pages": 3}, nil
})
w.Command("example.report.reload", func(ctx context.Context, c *worker.Command) (any, error) {
	fmt.Fprintln(c, "перечитываю шаблоны")
	return map[string]int{"templates": 12}, nil
})
w.State("example.report", func(ctx context.Context, version int64, spec json.RawMessage) (any, error) {
	return map[string]any{"applied": version}, nil
})
w.Telemetry("example.report", worker.AutoInterval, func() any { return map[string]int{"queue": 0} })
if err := w.Run(context.Background()); err != nil {
	log.Fatal(err)
}
```

```ts
import { AUTO_INTERVAL, Worker } from "agent-sdk/worker";

const worker = new Worker({ name: "report", version: "1.0.0" });
worker.job("example.render", { concurrency: 2 }, async (job) => {
  job.progress(0.5, "строю отчёт");
  return { pages: 3 };
});
worker.command("example.report.reload", async (cmd) => {
  cmd.write("перечитываю шаблоны\n");
  return { templates: 12 };
});
worker.state("example.report", async (version, spec) => ({ header: spec.header }));
worker.telemetry("example.report", { intervalMs: AUTO_INTERVAL }, () => ({ queue: 0 }));
await worker.run();
```

Всё о воркере — регистрация, контекст, здоровье, пауза, замена, уборка — в
[workers.md](workers.md).

## Шаг 3. Агент

Готовая сборка агента лежит в GitHub Release — файл `agent-<os>-<arch>` под свою машину
(`<os>` — `linux` или `darwin`, `<arch>` — `amd64` или `arm64`, `<версия>` — номер без `v`,
например `1.1.0`):

```bash
curl -fLO https://github.com/epifanovmd/agent/releases/download/v<версия>/agent-<os>-<arch>
chmod +x agent-<os>-<arch>
xattr -d com.apple.quarantine agent-<os>-<arch>   # только macOS: иначе система не даст запустить
```

Собрать самому из репозитория — `make build` (Go ставить не нужно, сборка идёт в контейнере):
программа появится в `dist/<версия>/agent-<os>-<arch>`. Настройки:

```yaml
# agent.yaml
server:
  url: http://localhost:8080
dataDir: ./.agent-data # ключ агента, очередь важных сообщений, состояние
name: node-01
labels:
  zone: eu
enroll:
  token: demo-token
workers:
  - name: report
    command: ["python3", "report_worker.py"]
```

```bash
./agent-<os>-<arch> run -config agent.yaml
```

Агент зарегистрируется по токену, запустит воркер и подключится к бэкенду. В бэкенде появится
событие `agent` с `online: true`, а `agents.listAgents()` вернёт агента: в
`agent.capabilities` — очередь `example.render`, команда `example.report.reload`, раздел
`example.report`, канал `example.report`.

На настоящем узле агент работает службой systemd (Debian/Ubuntu, RHEL/Fedora, SUSE и т. п.) и
ставится одной командой, которую собирает сам бэкенд (`agents.installCommand(…)`), — см.
[releases.md](releases.md#установка-одной-командой). Без раздачи с бэкенда — из GitHub Release:

```bash
curl -fLO https://github.com/epifanovmd/agent/releases/download/v<версия>/agent-linux-amd64
curl -fLO https://github.com/epifanovmd/agent/releases/download/v<версия>/install.sh
sudo sh install.sh --binary ./agent-linux-amd64 --server https://api.example.com --token <токен>
```

На узлах без systemd (Alpine и другие) агент работает в контейнере. Установка, флаги, удаление и
резервная копия — в [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md#установка), все настройки
агента — [там же](../../docs/ARCHITECTURE.md#настройки).

## Шаг 4. Бэкенд управляет узлом

```ts
// задача: выполнит один подходящий агент; если агент пропадёт — повтор
const job = await agents.enqueue({ queue: "example.render", data: { title: "Итоги" }, maxAttempts: 3 });

// команда конкретному агенту — дождаться итога
const [agent] = await agents.listAgents();
const cmd = await agents.call({ name: "example.report.reload", agentId: agent.id, timeoutSec: 30 });
console.log(cmd.status, cmd.output, cmd.result); // succeeded "перечитываю шаблоны\n" { templates: 12 }

// состояние: всем агентам или одному (личный снимок главнее общего)
await agents.setState("example.report", { header: "ACME" });
await agents.setState("example.report", { header: "ACME EU" }, { agentId: agent.id });

// пока смотрите на узел — метрики раз в секунду и подробный лог
await agents.subscribe(agent.id, { metrics: { intervalMs: 1000 }, logs: { level: "debug" } });
agents.on("metrics", (agentId, point) => console.log(agentId, point.metrics.host?.cpuPercent));
```

```go
job, err := agents.Enqueue(server.JobRequest{Queue: "example.render", Data: map[string]any{"title": "Итоги"}, MaxAttempts: 3})
cmd, err := agents.Call(ctx, server.CommandRequest{Name: "example.report.reload", AgentID: agentID, TimeoutSec: 30})
st, err := agents.SetState("example.report", map[string]any{"header": "ACME"}, "")
```

```python
job = await agents.enqueue("example.render", {"title": "Итоги"}, max_attempts=3)
cmd = await agents.call("example.report.reload", agent_id=agent_id, timeout_sec=30)
await agents.set_state("example.report", {"header": "ACME"})
```

## Шаг 5. Хранилище и несколько процессов

`MemoryStore` хранит всё в памяти — для разработки. В работе бэкенду нужен свой `Store` поверх
БД (агенты, задачи, команды, состояние, события, метрики): что в нём и какие правила — в
[store.md](store.md); записи он меняет условно (по номеру версии `rev`), поэтому процессы не
затирают изменения друг друга. Если процессов бэкенда несколько, у них общий `Store`, а
изменения доставляет тот, к кому подключён агент, — по `refresh`; HTTP-канал агента за
балансировщиком требует «липких» сессий
([connection.md](connection.md#несколько-процессов-бэкенда)).

## Все разделы

| Раздел                         | О чём                                                                                                                 |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------- |
| [connection.md](connection.md) | регистрация и токен, связь (WebSocket, HTTP, несколько адресов, прокси, TLS), смена ключа, отзыв, несколько процессов |
| [jobs.md](jobs.md)             | задачи: постановка, выбор агента, срок, повторы, отмена, прогресс, события, файлы, сверка после переподключения       |
| [commands.md](commands.md)     | команды: `command` и `call`, вывод, срок, ошибки, встроенные команды агента                                           |
| [state.md](state.md)           | состояние: общий и личный снимок, версии, удаление, история и откат, повторное применение, секреты                    |
| [workers.md](workers.md)       | воркер: имена, контекст, здоровье, пауза, просьба о перезапуске, замена, уборка, ограничения, воркеры из выпуска      |
| [observe.md](observe.md)       | метрики узла, подписки, показатели воркеров, логи, сведения об узле, история метрик                                   |
| [events.md](events.md)         | события воркеров, уведомления о проблемах, журнал аудита                                                              |
| [releases.md](releases.md)     | выпуск, обновление агента и воркеров, установка одной командой, удаление                                              |
| [store.md](store.md)           | хранилище: методы по языкам, правила, пример на SQL                                                                   |

Формат сообщений для тех, кто работает без SDK, — [sdk/spec](../spec/README.md), образцы
сообщений по темам — [sdk/spec/examples](../spec/examples). Готовый стенд (бэкенды на Node и
Python, веб-интерфейс, воркеры на трёх языках) — [examples/README.md](../../examples/README.md).

## Возможности одним списком

- **Связь:** WebSocket, а если не проходит — HTTP; несколько адресов одного бэкенда; HTTP-прокси;
  свой корневой и клиентский сертификат. Входящие порты на узле не нужны.
- **Работа без связи:** задачи доделываются, итоги и события лежат на диске до подтверждения,
  метрики копятся и досылаются, состояние применяется с диска.
- **Задачи:** очередь, свободные места, наименее загруженный агент, срок и продление, повторы,
  отмена и «закончи пораньше», прогресс, журнал, события задачи, файлы по ссылкам.
- **Команды:** вывод по мере работы, срок, свои коды ошибок, ожидание итога (`call`) даже через
  другой процесс бэкенда, десять встроенных команд агента.
- **Состояние:** общее и личное, только растущие версии, история и откат, повторное применение
  раз в 10 минут, запечатанные секреты, отчёт при ошибке.
- **Воркеры:** любой язык, несколько копий, замена без простоя или «сначала старый», здоровье,
  пауза очередей, просьба о перезапуске, ограничения ресурсов, запуск от другого пользователя,
  уборка при удалении, воркеры из выпуска с обновлением и откатом.
- **Наблюдение:** метрики узла по группам и видеокарты, подписки «чаще и подробнее, пока
  смотрят», показатели воркеров, лог агента и воркеров, сведения об узле, история с
  прореживанием и сроком хранения.
- **Уведомления и аудит:** `offline`, `stateFailed`, `workerDown`, `workerDegraded`, `degraded`;
  журнал «кто что сделал» через `by(actor)`.
- **Безопасность:** токен регистрации с ограничением попыток, в БД — только хеш ключа, смена
  ключа, отзыв, подпись выпуска, выключение встроенных команд на узле.
- **Сопровождение:** установка одной командой, перечитывание настроек без перезапуска,
  обновление агента и воркеров с откатом, уборка при удалении.
