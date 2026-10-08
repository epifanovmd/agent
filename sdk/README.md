# SDK агента: Go, Node.js, Python

У SDK две части. Обе есть на трёх языках и работают одинаково:

| Часть      | Для кого            | Что берёт на себя                                                                                                      |
| ---------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| **worker** | для воркера на узле | связь с агентом, задачи, команды, состояние, показатели, события, контекст, здоровье, пауза, остановка, уборка         |
| **server** | для бэкенда         | приём агентов, связь с ними, задачи и команды, доставка состояния, подписки, метрики, уведомления, выпуск и обновление |

Этот документ — обзор и **справочник API** (точные сигнатуры на трёх языках). Разбор каждой
возможности по шагам «бэкенд → сеть → агент → воркер → результат» с примерами — в
[sdk/docs](docs/README.md).

```
sdk/
├── README.md     этот документ — обзор и справочник API
├── docs/         разбор по темам с примерами: с чего начать, связь, задачи, команды, состояние,
│                 воркер, наблюдение, события, выпуск, хранилище
├── spec/         формат сообщений (README.md) и образцы сообщений по темам (examples/)
├── go/worker     Go: воркер
├── go/server     Go: бэкенд
├── go/message    Go: типы сообщений (общие с агентом)
├── go/sealed     Go: запечатанные секреты в состоянии
├── node/         npm-пакет agent-sdk: agent-sdk/worker, agent-sdk/server — особенности: node/README.md
└── python/       пакет agent-sdk: agent_sdk.worker, agent_sdk.server, agent_sdk.message — особенности: python/README.md
```

## Установка

Всё ставится с GitHub, без npm и PyPI. Версия SDK совпадает с версией агента:

```bash
# Go — прямо из репозитория (или @v<версия>)
go get github.com/epifanovmd/agent/sdk/go/server@latest   # и sdk/go/worker
# Node ≥ 24 — готовый архив из GitHub Release (TypeScript уже собран)
npm install https://github.com/epifanovmd/agent/releases/download/v<версия>/agent-sdk-<версия>.tgz
# Python ≥ 3.10 — готовый пакет из GitHub Release (зависимостей нет)
pip install https://github.com/epifanovmd/agent/releases/download/v<версия>/agent_sdk-<версия>-py3-none-any.whl
```

`<версия>` — номер выпуска без `v`, например `1.1.0`; список выпусков —
<https://github.com/epifanovmd/agent/releases>. Особенности языков — [node/README.md](node/README.md),
[python/README.md](python/README.md).

**Без SDK тоже можно.** Агент, воркер и сервер обмениваются строками JSON. Формат — в
[sdk/spec](spec/README.md), образцы сообщений по темам — в [spec/examples](spec/examples). SDK
ничего своего в сообщения не добавляет.

**Одинаково во всех языках.** Одно понятие называется одинаково, с поправкой на стиль языка:
`maxAttempts` / `MaxAttempts` / `max_attempts`. Каждый SDK проверяется на тех же образцах.

## Документация

| Раздел                                    | О чём                                                                                        |
| ----------------------------------------- | -------------------------------------------------------------------------------------------- |
| [С чего начать](docs/README.md)           | бэкенд, агент и воркер за 10 минут; что выбрать: задача, команда или состояние               |
| [Регистрация и связь](docs/connection.md) | токен, метки, WebSocket и HTTP, несколько адресов, прокси, TLS, смена ключа, отзыв, процессы |
| [Задачи](docs/jobs.md)                    | постановка, места, срок, повторы, отмена, прогресс, события, файлы, сверка                   |
| [Команды](docs/commands.md)               | `command` и `call`, вывод, срок, ошибки, встроенные команды агента                           |
| [Состояние](docs/state.md)                | общий и личный снимок, версии, удаление, история и откат, повтор, секреты, отчёт             |
| [Воркер](docs/workers.md)                 | регистрация, контекст, здоровье, пауза, перезапуск, замена, уборка, ограничения, выпуск      |
| [Наблюдение](docs/observe.md)             | статус, метрики узла, подписки, показатели воркеров, лог, сведения об узле, история          |
| [События](docs/events.md)                 | события воркеров, `change`, уведомления о проблемах, аудит                                   |
| [Выпуск](docs/releases.md)                | сборка и раздача выпуска, установка одной командой, обновление агента и воркеров, удаление   |
| [Хранилище](docs/store.md)                | методы `Store` по языкам, правила, пример на SQL                                             |

## Как это устроено

```
бэкенд ── Agents (server SDK) ◀──── WebSocket / HTTP ────▶ агент ◀── канал на узле (fd 3) ──▶ воркер (worker SDK)
  enqueue / command / setState        job.assign, cmd.run,          выбирает воркер,           обработчики job,
  subscribe / revoke / …              state.put, config             хранит важное на диске     command, state, …
  события change, metrics, alert ◀─── status, metrics, итоги ◀───── повторяет, досылает ◀───── итоги, показатели
```

- Соединение открывает **только агент**: входящие порты на узле не нужны.
- **Сервер хранит, что должно быть сделано** (задачи, команды, состояние), и присылает это
  заново после каждого подключения; агент узнаёт повторы по id.
- **Агент** держит связь, хранит важные сообщения на диске до подтверждения, работает без связи,
  запускает воркеры. **Воркер** знает только свою предметную область.

---

## worker — воркер на узле

Воркер запускает агент (список `workers` в его настройках) и сразу даёт ему канал связи —
дескриптор из `AGENT_IPC_FD`. SDK находит канал сам; для тестов можно подставить свой. Разбор —
[docs/workers.md](docs/workers.md).

### Worker

| Что                                 | Go (`sdk/go/worker`)                                                  | Node (`agent-sdk/worker`)                                             | Python (`agent_sdk.worker`)                                               |
| ----------------------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| создать                             | `worker.New(name, version string, opts ...Option) *Worker`            | `new Worker({ name?, version?, transport?, log? })`                   | `Worker(name=None, *, version="0.0.0", channel=None)`                     |
| очередь задач                       | `w.Job(queue string, concurrency int, fn JobHandler)`                 | `w.job(queue, { concurrency? }, handler)` или `w.job(queue, handler)` | `@w.job(queue, concurrency=1)` или `w.register(queue, fn, concurrency=1)` |
| команда                             | `w.Command(name string, fn CommandHandler)`                           | `w.command(name, handler)`                                            | `@w.command(name)`                                                        |
| раздел состояния                    | `w.State(domain string, fn StateHandler)`                             | `w.state(domain, (version, spec) => report)`                          | `@w.state(domain)` → `fn(version, spec)`                                  |
| показатели (агент спрашивает сам)   | `w.Telemetry(channel string, interval time.Duration, fn func() any)`  | `w.telemetry(channel, { intervalMs?: number \| "auto" }, fn)`         | `@w.telemetry(channel, interval=15.0)` — секунды или `"auto"`             |
| объявить канал показателей          | `w.Channel(name string)`                                              | `w.channel(name)`                                                     | `w.channel(name)`                                                         |
| прислать показатели                 | `w.Report(channel string, data any) error`                            | `w.report(channel, data)`                                             | `w.report(channel, data)`                                                 |
| событие                             | `w.Event(typ string, data any) error`                                 | `w.event(type, data?)`                                                | `w.event(type, data=None)`                                                |
| уборка при удалении агента          | `w.Cleanup(fn func(ctx) error)`                                       | `w.cleanup(async () => {})`                                           | `@w.cleanup`                                                              |
| контекст агента                     | `w.Context() worker.Context`, `w.OnContext(fn func(worker.Context))`  | `w.context`, `w.on("context", fn)`                                    | `w.context`, `@w.on_context`                                              |
| здоровье                            | `w.SetHealth(ok bool, msg string) error`                              | `w.setHealth(ok, message?)`                                           | `w.set_health(ok, message=None)`                                          |
| пауза / снять паузу очередей        | `w.Pause(queues ...string) error`, `w.Resume(queues ...string) error` | `w.pause(queues?)`, `w.resume(queues?)`                               | `w.pause(queues=None)`, `w.resume(queues=None)`                           |
| попросить замену                    | `w.RequestRestart(reason string) error`                               | `w.requestRestart(reason?)`                                           | `w.request_restart(reason=None)`                                          |
| запустить                           | `w.Run(ctx context.Context) error`                                    | `await w.run({ signals? })`                                           | `w.run(install_signals=True)`                                             |
| сигнал «пора останавливаться»       | `<-w.Stopping()`                                                      | `w.stopping` (`AbortSignal`)                                          | `w.stopping` (`threading.Event`)                                          |
| остановиться (доработать и выйти)   | `w.Drain()`                                                           | `w.drain()`                                                           | `w.drain()`                                                               |
| имена, которые агент не принял      | `w.Rejected() []string`                                               | `w.rejected: string[]`                                                | `w.rejected: set`                                                         |
| свой канал связи (тесты)            | `worker.WithConn(io.ReadWriteCloser)`                                 | `new Worker({ transport: Duplex })`                                   | `Worker(…, channel=Channel(sock))`                                        |
| свой лог, без обработчиков сигналов | `worker.WithLogger(*slog.Logger)`, `worker.WithoutSignals()`          | `log: (level, msg) => …`, `run({ signals: false })`                   | логгер `agent_sdk.worker`, `run(install_signals=False)`                   |

Обработчики: Go — `JobHandler func(ctx, *Job) (any, error)`, `CommandHandler func(ctx,
*Command) (any, error)`, `StateHandler func(ctx, version int64, spec json.RawMessage) (any,
error)`; Node — `async (job) => result`, `async (cmd) => result`, `async (version, spec) =>
report`; Python — обычные функции, работают в потоках. Авто-интервал показателей —
`worker.AutoInterval`, `AUTO_INTERVAL` (`"auto"`), `"auto"`: частота подписки на канал, иначе
частота метрик агента, иначе 15 с ([docs/observe.md](docs/observe.md#показатели-воркеров)).

### Job

| Что                               | Go (`*worker.Job`)                                               | Node (`Job`)                                     | Python (`Job`)                                    |
| --------------------------------- | ---------------------------------------------------------------- | ------------------------------------------------ | ------------------------------------------------- |
| что пришло                        | `ID`, `Queue`, `Data json.RawMessage`, `Attempt`, `LeaseSeconds` | `id`, `queue`, `data`, `attempt`, `leaseSeconds` | `id`, `queue`, `data`, `attempt`, `lease_seconds` |
| прогресс 0..1 и текст             | `Progress(value float64, text ...string)`                        | `progress(value, text?)`                         | `progress(value, text=None)`                      |
| строка журнала                    | `Log(line string)`                                               | `log(line)`                                      | `log(line)`                                       |
| событие задачи                    | `Event(typ string, data any) error`                              | `event(type, data?)`                             | `event(type, data=None)`                          |
| отменили                          | `ctx.Done()`                                                     | `signal` (`AbortSignal`), `cancelled`            | `cancelled`, `check_cancelled()` → `Cancelled`    |
| просят закончить пораньше         | `StopRequested() <-chan struct{}`                                | `stopRequested`                                  | `stop_requested`                                  |
| имена файлов                      | `Inputs() []string`, `Outputs() []string`                        | `inputs`, `outputs`                              | `inputs`, `outputs`                               |
| входной файл во временный каталог | `InputPath(ctx, name) (string, error)`                           | `await inputPath(name)`                          | `input_path(name) -> Path`                        |
| скачать в своё место              | `Download(ctx, name, path string) error`                         | `await download(name, target)`                   | `download(name, target) -> Path`                  |
| загрузить результат               | `Upload(ctx, name, data []byte)`, `UploadFile(ctx, name, path)`  | `await upload(name, Uint8Array \| путь)`         | `upload(name, bytes \| путь)`                     |
| свежие ссылки                     | `RefreshURLs(ctx, inputs, outputs []string) error`               | `await refreshUrls(inputs?, outputs?)`           | `refresh_urls(inputs=None, outputs=None)`         |

Подробно — [docs/jobs.md](docs/jobs.md).

### Command

| Что        | Go (`*worker.Command`)                             | Node (`Command`)                   | Python (`Command`)                             |
| ---------- | -------------------------------------------------- | ---------------------------------- | ---------------------------------------------- |
| что пришло | `ID`, `Name`, `Args json.RawMessage`, `TimeoutSec` | `id`, `name`, `args`, `timeoutSec` | `id`, `name`, `args`, `timeout_sec`            |
| вывод      | `Write(p []byte)` — это `io.Writer`                | `write(text)`                      | `write(text)`                                  |
| срок истёк | `ctx.Done()`                                       | `signal`, `cancelled`              | `cancelled`, `check_cancelled()` → `Cancelled` |

Подробно — [docs/commands.md](docs/commands.md).

### Ошибки

|                                     | Go                                                    | Node                                      | Python                                     |
| ----------------------------------- | ----------------------------------------------------- | ----------------------------------------- | ------------------------------------------ |
| задача: свой код                    | `worker.Fail(code, msg string, retryable bool) error` | `new JobError(code, msg, { retryable? })` | `JobFailed(code, message, retryable=True)` |
| команда: свой код                   | `worker.CommandError(code, msg string) error`         | `new CommandError(code, msg)`             | `CommandFailed(code, message)`             |
| состояние: не применилось с отчётом | `worker.StateFailed(msg string, report any) error`    | `new StateError(msg, report?)`            | `StateFailed(message, report=None)`        |
| агент отклонил запрос               | `*message.Error`                                      | `AgentError(code, message, retryable)`    | `AgentError(code, message, retryable)`     |

Код — `^[A-Z0-9_]{1,64}$`. Любая другая ошибка, исключение или паника: задача — `WORKER_ERROR`
(сервер повторит), команда — `COMMAND_FAILED`, состояние — «не применилось» с текстом (агент
повторит). Воркер при этом не падает. Константы Go: `worker.CodeWorkerError`,
`worker.CodeWorkerStopping`, `worker.CodeCommandFailed`, `worker.CodeCommandUnknown`.

### Контекст

`worker.context` — что агент сообщает о себе: `mode` (`run` | `cleanup`), `agent` (`id`, `name`,
`version`, `labels`), `online`, `metricsIntervalMs`, `channels` (подписки на каналы этого
воркера: канал → мс), `logLevel`. Go — `worker.Context` (= `message.WorkerContext`), Node —
`WorkerContext`, Python — `WorkerContext` (`metrics_interval_ms`, `log_level`). Подробно —
[docs/workers.md](docs/workers.md#контекст-воркера).

### Как ведёт себя SDK воркера (во всех языках одинаково)

- При старте сообщает агенту очереди (с `concurrency`), команды, разделы состояния, каналы.
  Неверное имя — сразу ошибка: Python и Node — исключение при объявлении, Go — из `Run`.
- Задачи выполняет параллельно — до `concurrency` на очередь; команды и состояние —
  параллельно с задачами. Один раздел состояния не применяется дважды одновременно.
- Прогресс и журнал прореживает (агенту — не чаще двух раз в секунду), вывод команд режет на
  куски до 64 КБ, ссылки на файлы обновляет сам, загрузку повторяет до 4 раз.
- Задача, пришедшая во время остановки, возвращается с `WORKER_STOPPING` — сервер отдаст её
  другим.
- Остановка (SIGTERM или `worker.drain`): новых задач не берёт, текущие задачи и команды
  дорабатывает, выходит. Пропала связь с агентом — всё отменяет и выходит.
- Показатели начинает собирать после того, как агент принял воркер; ошибка сбора — в лог.
- Свой лог пишет в stderr — агент добавляет его в свой журнал с именем воркера.

---

## server — бэкенд, к которому подключаются агенты

Бэкенд создаёт **`Agents`** — «все подключённые агенты» — и подключает его к своему
HTTP-серверу. Всю работу со связью он делает сам; бэкенду остаются его данные и решения.
Разбор — [docs/README.md](docs/README.md) и разделы по темам.

### Настройки `Agents`

| Настройка (Node)                                          | Go (`server.Options`)                  | Python (`Agents(…)`)                          | По умолчанию                             | Что                                                                    |
| --------------------------------------------------------- | -------------------------------------- | --------------------------------------------- | ---------------------------------------- | ---------------------------------------------------------------------- |
| `enrollToken`                                             | `EnrollToken string`                   | `enroll_token`                                | —                                        | общий токен регистрации                                                |
| `enroll(token, {name, labels, host}) → {labels?} \| null` | `Enroll func(token) (labels, ok bool)` | `enroll(token) → {labels?} \| None`           | —                                        | своя проверка токена; `labels` — выданные метки                        |
| `store`                                                   | `Store`                                | `store`                                       | `MemoryStore`                            | хранилище ([docs/store.md](docs/store.md))                             |
| `files`                                                   | `Files`                                | `files`                                       | `MemoryFiles`                            | где лежат файлы задач                                                  |
| `statusIntervalMs`                                        | `StatusInterval time.Duration`         | `status_interval_ms`                          | 5000                                     | как часто агент присылает статус                                       |
| `metricsIntervalMs`                                       | `MetricsInterval`                      | `metrics_interval_ms`                         | 15000                                    | как часто агент присылает метрики                                      |
| `offlineGraceMs`                                          | `OfflineGrace`                         | `offline_grace_ms`                            | 20000                                    | сколько агент считается на связи после обрыва                          |
| `offlineAfterMs`                                          | `OfflineAfter`                         | `offline_after_ms`                            | max(3 × статус, 30 с) + `offlineGraceMs` | без вестей дольше — без связи, даже если его процесс бэкенда упал      |
| `metricsStoreIntervalMs`                                  | `MetricsStoreInterval`                 | `metrics_store_interval_ms`                   | 15000                                    | не чаще скольких мс сохранять точку метрик в `Store`                   |
| `metricsRetentionMs`                                      | `MetricsRetention`                     | `metrics_retention_ms`                        | 7 суток                                  | сколько хранить историю метрик                                         |
| `enrollFailureLimit`                                      | `EnrollFailureLimit int`               | `enroll_failure_limit`                        | 10                                       | неудачных регистраций с одного адреса за окно                          |
| `enrollFailureWindowMs`                                   | `EnrollFailureWindow`                  | `enroll_failure_window_ms`                    | 60000                                    | окно подсчёта неудачных регистраций                                    |
| `releasesDir`                                             | `ReleasesDir`                          | `releases_dir`                                | —                                        | каталог выпуска ([docs/releases.md](docs/releases.md))                 |
| `publicKey`                                               | `PublicKey`                            | `public_key`                                  | —                                        | открытый ключ подписи (base64), вписывается в `install.sh`             |
| `baseUrl`                                                 | `PublicURL` (или `SetPublicURL(url)`)  | `base_url`                                    | из запроса                               | публичный адрес: ссылки на файлы задач, `install.sh`, `installCommand` |
| `trustProxy`                                              | `TrustProxy bool`                      | `trust_proxy`                                 | `false`                                  | адрес агента — первый из `X-Forwarded-For`                             |
| `log: (msg, extra) => …`                                  | `Log *slog.Logger`                     | `log` — `logging.Logger` или `fn(msg, extra)` | —                                        | лог                                                                    |

`0` в `metricsStoreIntervalMs`, `metricsRetentionMs`, `enrollFailureLimit` — в Node и Python
«каждую точку», «хранить всегда», «без ограничения»; в Go для этого — отрицательное значение
(ноль — значение по умолчанию). Без `enrollToken` и `enroll` агенты не зарегистрируются; Node и
Python сразу бросают ошибку при создании `Agents`. События в Go — функции в `Options`
([ниже](#события)).

### Подключение к HTTP-серверу

| Go                                                      | Node                                                                                | Python                                                                                                                                                                                                                                                                               |
| ------------------------------------------------------- | ----------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `agents := server.New(opts)`                            | `const agents = new Agents(opts)`                                                   | `agents = Agents(…)`                                                                                                                                                                                                                                                                 |
| `agents.Handler() http.Handler` или `agents.Mount(mux)` | `await agents.handle(req, res)` → обработан ли; `agents.attach(server)` — WebSocket | `handle_enroll(body, remote=None)`, `handle_sync(auth, body, is_disconnected=None, *, base_url, remote, forwarded_for)`, `authenticate(auth)`, `serve_websocket(auth, conn, *, base_url, remote, forwarded_for)`, `handle_file(method, key, body)`, `handle_release(path, base_url)` |
| `agents.Close()`                                        | `agents.close()`                                                                    | `await agents.close()`                                                                                                                                                                                                                                                               |

Маршруты: `POST /api/v1/agent-link/enroll`, `GET /api/v1/agent-link` (WebSocket), `POST
/api/v1/agent-link/sync`, `GET /api/v1/agent-link/releases/<file>`, `GET
/api/v1/agent-link/install.sh`, `GET`/`PUT /files/<jobId>/(in|out)/<имя>` (для `MemoryFiles`).
`close` закрывает сессии кодом `1012` — агенты переподключатся сразу.

### Методы

Go — методы `*server.Agents` (ошибки — `error`, чаще `*message.Error` с `Code`); Node и Python —
асинхронные, кроме `on`, `off`, `by`, `alerts`, `installCommand` / `install_command`.

**Задачи** — [docs/jobs.md](docs/jobs.md)

| Что                | Go                                                | Node                                                           | Python                                                                                                            |
| ------------------ | ------------------------------------------------- | -------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| поставить          | `Enqueue(req JobRequest) (*Job, error)`           | `enqueue(req: JobRequest): Promise<Job>`                       | `enqueue(queue, data=None, *, max_attempts=1, lease_seconds=60, inputs=None, outputs=None, agent_id=None) -> Job` |
| отменить           | `CancelJob(id string) error`                      | `cancelJob(id): Promise<Job>`                                  | `cancel_job(job_id) -> Job`                                                                                       |
| закончить пораньше | `StopJob(id string) error`                        | `stopJob(id): Promise<Job>`                                    | `stop_job(job_id) -> Job`                                                                                         |
| прочитать          | `Job(id) (*Job, error)`, `Jobs() ([]*Job, error)` | `getJob(id)`, `listJobs(filter?: {status?, queue?, agentId?})` | `get_job(job_id)`, `list_jobs(*, status=None, queue=None, agent_id=None)`                                         |

Списки задач — новые первыми.

`JobRequest`: `queue`, `data`, `maxAttempts` (1), `leaseSeconds` (60, не меньше 5), `inputs` (имя →
содержимое или ссылка), `outputs` (имена файлов-результатов), `agentId` (закрепить).

**Команды** — [docs/commands.md](docs/commands.md)

| Что                    | Go                                                | Node                                                           | Python                                                                    |
| ---------------------- | ------------------------------------------------- | -------------------------------------------------------------- | ------------------------------------------------------------------------- |
| отправить              | `Command(req CommandRequest) (*Command, error)`   | `command(req: CommandRequest): Promise<Command>`               | `command(name, args=None, *, timeout_sec=60, agent_id=None) -> Command`   |
| отправить и ждать итог | `Call(ctx, req CommandRequest) (*Command, error)` | `call(req): Promise<Command>`                                  | `call(name, args=None, *, timeout_sec=60, agent_id=None) -> Command`      |
| прочитать              | `CommandByID(id) (*Command, error)`, `Commands()` | `getCommand(id)`, `listCommands(filter?: {status?, agentId?})` | `get_command(command_id)`, `list_commands(*, status=None, agent_id=None)` |

Списки команд — новые первыми.

`CommandRequest`: `name`, `args`, `timeoutSec` (60), `agentId` (без него — агент на связи,
объявивший команду).

**Состояние** — [docs/state.md](docs/state.md)

| Что               | Go                                                                                   | Node                                                                  | Python                                                                |
| ----------------- | ------------------------------------------------------------------------------------ | --------------------------------------------------------------------- | --------------------------------------------------------------------- |
| задать снимок     | `SetState(domain string, spec any, agentID string) (*DesiredState, error)`           | `setState(domain, spec, { agentId? }): Promise<DesiredState>`         | `set_state(domain, spec, agent_id=None) -> DesiredState`              |
| удалить снимок    | `DeleteState(domain, agentID string) (*DesiredState, error)`                         | `deleteState(domain, { agentId? }): Promise<DesiredState \| null>`    | `delete_state(domain, agent_id=None) -> DesiredState \| None`         |
| история раздела   | `StateHistory(domain, agentID string, limit int) ([]*DesiredState, error)`           | `stateHistory(domain, { agentId?, limit? }): Promise<DesiredState[]>` | `state_history(domain, agent_id=None, limit=20)`                      |
| откатить к версии | `RollbackState(domain string, version int64, agentID string) (*DesiredState, error)` | `rollbackState(domain, version, { agentId? })`                        | `rollback_state(domain, version, agent_id=None)`                      |
| все снимки        | `States() ([]*DesiredState, error)`                                                  | `listStates()`                                                        | `list_states()`                                                       |
| запечатать секрет | `Seal(agentID string, value any) (json.RawMessage, error)`                           | `seal(agentId, value): Promise<Sealed>`                               | `seal(agent_id, value) -> {"$sealed": …}` (нужен `agent-sdk[crypto]`) |

**Агенты и связь** — [docs/connection.md](docs/connection.md)

| Что                                  | Go                                                                                         | Node                                        | Python                                 |
| ------------------------------------ | ------------------------------------------------------------------------------------------ | ------------------------------------------- | -------------------------------------- |
| все агенты                           | `List() ([]*Agent, error)`                                                                 | `listAgents(): Promise<Agent[]>`            | `list_agents() -> List[Agent]`         |
| агент                                | `Agent(id) (*Agent, error)`                                                                | `getAgent(id): Promise<Agent \| undefined>` | `get_agent(agent_id) -> Agent \| None` |
| отозвать                             | `Revoke(agentID string) error`                                                             | `revoke(agentId): Promise<Agent>`           | `revoke(agent_id) -> Agent`            |
| сменить ключ                         | `RotateKey(agentID string) (*Command, error)`                                              | `rotateKey(agentId): Promise<Command>`      | `rotate_key(agent_id) -> Command`      |
| доставить изменения других процессов | `Refresh(agentID string)` — `""` — все                                                     | `refresh(agentId?): Promise<void>`          | `refresh(agent_id=None)`               |
| регистрация вручную                  | `Enroll(token, name string, labels map[string]string) (agentID, secret string, err error)` | —                                           | —                                      |

**Воркеры** — [docs/workers.md](docs/workers.md)

| Что                    | Go                                                                       | Node                                                        | Python                                                 |
| ---------------------- | ------------------------------------------------------------------------ | ----------------------------------------------------------- | ------------------------------------------------------ |
| пауза очередей воркера | `PauseWorker(agentID, name string, queues ...string) (*Command, error)`  | `pauseWorker(agentId, name, { queues? }): Promise<Command>` | `pause_worker(agent_id, name, queues=None) -> Command` |
| снять паузу сервера    | `ResumeWorker(agentID, name string, queues ...string) (*Command, error)` | `resumeWorker(agentId, name, { queues? })`                  | `resume_worker(agent_id, name, queues=None)`           |

**Наблюдение** — [docs/observe.md](docs/observe.md)

| Что               | Go                                                                      | Node                                                                   | Python                                                                                                                  |
| ----------------- | ----------------------------------------------------------------------- | ---------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| подписка          | `Subscribe(agentID string, req SubscribeRequest) (Subscription, error)` | `subscribe(agentId, opts: SubscribeOptions): Promise<SubscriptionRef>` | `subscribe(agent_id, *, id=None, ttl_ms=30000, status=None, metrics=None, logs=None, channels=None) -> {"id", "until"}` |
| снять подписку    | `Unsubscribe(agentID, id string) error`                                 | `unsubscribe(agentId, id): Promise<void>`                              | `unsubscribe(agent_id, id)`                                                                                             |
| история метрик    | `Metrics(agentID string, since int64) ([]MetricsPoint, error)`          | `listMetrics(agentId, { since? }): Promise<MetricsPoint[]>`            | `list_metrics(agent_id, since=None) -> List[MetricsPoint]`                                                              |
| события воркеров  | `Events(n int) ([]AgentEvent, error)` — новые первыми                   | `listEvents(limit = 100)` — новые первыми                              | `list_events(limit=100)` — новые первыми                                                                                |
| активные проблемы | `Alerts() []Alert`                                                      | `alerts(): Alert[]`                                                    | `alerts() -> List[Alert]`                                                                                               |

Подписка: `id?`, `ttlMs` (30000; Go — `TTL time.Duration`), `status: {intervalMs}`, `metrics:
{intervalMs?, groups?}`, `logs: {level}`, `channels: {канал: {intervalMs}}`; Go —
`server.SubscribeRequest{ID, TTL, Status *IntervalSpec, Metrics *MetricsSpec, Logs *LogsSpec,
Channels map[string]IntervalSpec}`. Интервалы — не меньше 200 мс.

**Выпуск и обновление** — [docs/releases.md](docs/releases.md)

| Что                    | Go                                                          | Node                                             | Python                                         |
| ---------------------- | ----------------------------------------------------------- | ------------------------------------------------ | ---------------------------------------------- |
| манифест выпуска       | `Release() *Release`                                        | `release(): Promise<ReleaseManifest \| null>`    | `release() -> dict \| None`                    |
| кого обновить          | `UpdateCandidates() ([]UpdateCandidate, error)`             | `updateCandidates(): Promise<UpdateCandidate[]>` | `update_candidates() -> List[UpdateCandidate]` |
| обновить агента        | `UpdateAgent(agentID string) (*Command, error)`             | `updateAgent(agentId): Promise<Command>`         | `update_agent(agent_id) -> Command`            |
| какие воркеры обновить | `WorkerUpdateCandidates() ([]WorkerUpdateCandidate, error)` | `workerUpdateCandidates()`                       | `worker_update_candidates()`                   |
| обновить воркер        | `UpdateWorker(agentID, name string) (*Command, error)`      | `updateWorker(agentId, name): Promise<Command>`  | `update_worker(agent_id, name) -> Command`     |
| команда установки      | `InstallCommand(opts InstallOptions) (string, error)`       | `installCommand(opts: InstallOptions): string`   | `install_command(**opts) -> str`               |

**От имени пользователя** — `by(actor)` ([docs/events.md](docs/events.md#журнал-аудита)): Go
`agents.By(actor) *server.Actor`, Node `agents.by(actor): Actor`, Python `agents.by(actor) ->
Actor` — те же изменяющие методы (задачи, команды, состояние, `revoke`, `rotateKey`,
`updateAgent`, `updateWorker`, `pauseWorker`, `resumeWorker`) с отметкой `actor`.

### Команда установки

`installCommand(opts)` собирает строку `curl -fsSL '<адрес>/api/v1/agent-link/install.sh' | sudo
sh -s -- --token '…' [флаги]` — по флагу `install.sh` на каждую заданную опцию:

| Опция (Node)                      | Go (`InstallOptions`) | Python                | Флаг                                   |
| --------------------------------- | --------------------- | --------------------- | -------------------------------------- |
| `token` или `tokenFile`           | `Token`, `TokenFile`  | `token`, `token_file` | `--token`, `--token-file`              |
| `name`                            | `Name`                | `name`                | `--name`                               |
| `user`                            | `User`                | `user`                | `--user`                               |
| `config`                          | `Config`              | `config`              | `--config`                             |
| `privileged`                      | `Privileged`          | `privileged`          | `--privileged`                         |
| `killMode` (`process` \| `mixed`) | `KillMode`            | `kill_mode`           | `--kill-mode`                          |
| `packages`                        | `Packages`            | `packages`            | `--packages`                           |
| `packagesByManager`               | `PackagesByManager`   | `packages_by_manager` | `--packages-apt` … `--packages-zypper` |
| `sysctl`                          | `Sysctl`              | `sysctl`              | `--sysctl` (по алфавиту ключей)        |
| `rwPaths`                         | `RWPaths`             | `rw_paths`            | `--rw-path`                            |
| `caFile`                          | `CAFile`              | `ca_file`             | `--ca-file`                            |
| `workers`                         | `Workers`             | `workers`             | `--worker`                             |
| `stopTimeout`                     | `StopTimeout`         | `stop_timeout`        | `--stop-timeout`                       |
| `baseUrl`                         | `BaseURL`             | `base_url`            | адрес (иначе — `baseUrl` `Agents`)     |

Что делает каждый флаг — [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md#установка); примеры —
[docs/releases.md](docs/releases.md#установка-одной-командой).

### События

| Событие         | Go (`server.Options`)                                    | Node (`agents.on(…)`)                                       | Python (`agents.on(…)`) | Что                                  |
| --------------- | -------------------------------------------------------- | ----------------------------------------------------------- | ----------------------- | ------------------------------------ |
| `change`        | `OnChange func(Change)`                                  | `(c: Change) => …`                                          | `fn(Change)`            | что изменилось: `{kind, id}`         |
| `metrics`       | `OnMetrics func(agentID string, p MetricsPoint)`         | `(agentId, point) => …`                                     | `fn(agent_id, point)`   | каждая точка метрик                  |
| `log`           | `OnLog func(agentID string, entries []message.LogEntry)` | `(agentId, entries) => …`                                   | `fn(agent_id, entries)` | пачка записей лога агента и воркеров |
| `alert`         | `OnAlert func(Alert)`                                    | `(a: Alert) => …`                                           | `fn(Alert)`             | проблема началась или закончилась    |
| `audit`         | `OnAudit func(AuditEntry)`                               | `(e: AuditEntry) => …`                                      | `fn(AuditEntry)`        | изменяющее действие API              |
| `stateDeleted`  | `OnStateDeleted func(domain, agentID string)`            | `({ domain, agentId? }) => …`                               | `fn(domain, agent_id)`  | снимок удалён                        |
| по видам (Node) | —                                                        | `agent`, `job`, `command`, `state`, `stateApplied`, `event` | —                       | готовые объекты                      |

Go вызывает обработчики синхронно, вне блокировки `Agents` — не блокируйте, методы `Agents`
вызывать можно. Python принимает функцию или корутину; `on` возвращает функцию отписки, есть
`off(event, fn)`. Node — типизированный `EventEmitter`. Подробно —
[docs/events.md](docs/events.md).

### Модели

Поля одинаковые во всех SDK; в JSON — camelCase, в Python — `snake_case` (dataclass'ы,
`to_dict()` — JSON для интерфейса).

- **Agent**: `id`, `name`, `labels`, `online`, `revoked`, `transport` (`ws` | `http`),
  `address`, `enrolledAt`, `lastSeenAt`, `hello`, `capabilities`, `status`, `metrics` (последняя
  живая точка), `inventory`, `stateApplied` (раздел → последний отчёт), `subscriptions`. Ключ
  агента наружу не отдаётся.
- **Job**: `id`, `queue`, `data`, `status` (`queued` | `running` | `completed` | `failed` |
  `cancelled`), `attempt`, `maxAttempts`, `leaseSeconds`, `agentId`, `pinnedAgentId`,
  `accepted`, `progress`, `text`, `log` (последние 500 строк), `events` (`[{seq, type, data,
at}]`), `result`, `error` (`{code, message}`), `stopRequested`, `inputs`, `outputs`,
  `createdAt`, `finishedAt`, `actor`.
- **Command**: `id`, `agentId`, `name`, `args`, `timeoutSec`, `status` (`pending` | `running` |
  `succeeded` | `failed`), `output` (последние 256 КБ), `result`, `error`, `exitCode` (код выхода
  из `cmd.done`), `createdAt`, `finishedAt`, `actor`.
- **DesiredState**: `domain`, `agentId` (пусто — общий), `version`, `spec`, `updatedAt`, `actor`.
- **AgentEvent**: `agentId`, `agentName`, `source`, `type`, `data`, `at`.
- **MetricsPoint**: `at` (мс, часы сервера), `backfill`, `metrics`.
- **Alert**: `type`, `agentId`, `agentName`, `active`, `message`, `at`, `domain?`, `worker?`.
- **AuditEntry**: `at`, `actor`, `action`, `agentId?`, `target`, `details?`.
- **Change**: `kind` (`agent` | `job` | `command` | `state` | `event`), `id`.
- **UpdateCandidate**: `agentId`, `name`, `online`, `current`, `target`, `os`, `arch`;
  **WorkerUpdateCandidate**: `agentId`, `agentName`, `online`, `worker`, `current`, `target`, `os`,
  `arch`.

### Хранилище

`Store` — интерфейс хранения агентов, задач, команд, состояния с историей, событий и метрик;
`MemoryStore` — в памяти, для разработки. Методы по языкам, правила (растущие версии, запись
агента в транзакции, история, чистка метрик) и пример на SQL — [docs/store.md](docs/store.md).

### Файлы задач

`Files` — откуда агент и воркер берут ссылки на файлы задач. Go — `Inputs(job, inputs) error`,
`URLs(job, baseURL) (message.JobURLs, error)` (если провайдер ещё и `http.Handler`, `Agents`
обслуживает им `/files/`); Node — `saveInputs(jobId, inputs)`, `urls(job, baseUrl, only?)`,
`handle?(req, res, path)`; Python — `prepare(job, inputs)`, `urls(job, base_url)`,
`handle(method, key, body)`. `MemoryFiles` — файлы в памяти, ссылки на час. Подробно —
[docs/jobs.md](docs/jobs.md#файлы-задачи).

### Ошибки API

Node — `AgentsError` (`code`, `status` — HTTP-статус для ответа, `retryAfterSec`), Python —
`AgentsError(code, message, status)`, Go — `*message.Error` (`Code`, `Message`) или
`server.ErrNotFound` при чтении.

| Код                               | Когда                                                                        |
| --------------------------------- | ---------------------------------------------------------------------------- |
| `MESSAGE_INVALID`                 | неверные аргументы: имя не по правилу, интервал меньше 200 мс, нет токена    |
| `AGENT_NOT_FOUND`                 | агента нет                                                                   |
| `AGENT_REVOKED`                   | агент отозван (`subscribe`, `rotateKey`, `pauseWorker`, …)                   |
| `JOB_NOT_FOUND`, `JOB_NOT_ACTIVE` | `cancelJob` / `stopJob`: задачи нет или она уже завершена                    |
| `COMMAND_NOT_SUPPORTED`           | ни один агент (или этот агент) не объявил команду                            |
| `STATE_VERSION_NOT_FOUND`         | `rollbackState`: версии нет в истории                                        |
| `SEAL_NOT_AVAILABLE`              | `seal`: агент не сообщил ключ шифрования (в Python — ещё нет `cryptography`) |
| `UPDATE_NOT_AVAILABLE`            | `updateAgent` / `updateWorker`: нет выпуска, сборки или агент не умеет       |
| `ENROLL_RATE_LIMITED`             | регистрация: слишком много неудачных попыток (`429`)                         |
| `AGENT_ENROLLMENT_TOKEN_INVALID`  | регистрация: неверный токен (`401`)                                          |

---

## Проверки

- Каждый SDK проверяется на образцах сообщений [spec/examples](spec/examples) и своими тестами.
- Настоящий агент проходит один и тот же сценарий против серверов на всех трёх SDK
  (`test/conformance`): Go — `test/testserver`, Node — `examples/server`, Python —
  `examples/server-python`.
- Как менять формат сообщений и SDK — [CONTRIBUTING.md](../CONTRIBUTING.md).
