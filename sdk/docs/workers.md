# Воркер

Воркер — программа проекта на любом языке, которую агент запускает на узле рядом с собой.
Агент держит связь с сервером, хранит важное на диске, повторяет и обновляет; воркер знает
только свою предметную область. Какие воркеры работают на узле, решают только настройки
агента (`agent.yaml`): сервер не может запустить на узле свою программу. Справочник API —
[sdk/README.md](../README.md#worker--воркер-на-узле), формат —
[sdk/spec §10](../spec/README.md#10-связь-воркера-с-агентом), образцы —
[workers.json](../spec/examples/workers.json).

- [Запуск и регистрация](#запуск-и-регистрация)
- [Регистрация и имена](#регистрация-и-имена)
- [Контекст воркера](#контекст-воркера)
- [Здоровье](#здоровье)
- [Пауза очередей](#пауза-очередей)
- [Просьба о перезапуске](#просьба-о-перезапуске)
- [Замена воркера](#замена-воркера)
- [Остановка и падение](#остановка-и-падение)
- [Уборка при удалении агента](#уборка-при-удалении-агента)
- [Ограничения ресурсов и пользователь](#ограничения-ресурсов-и-пользователь)
- [Воркеры из выпуска](#воркеры-из-выпуска)
- [Изменить воркеры на ходу](#изменить-воркеры-на-ходу)

## Запуск и регистрация

**Бэкенд** ничего не вызывает: воркер появляется, когда его запустил агент.

**Агент** (`internal/worker`) запускает воркеры из `agent.yaml`:

```yaml
workers:
  - name: report # обязательно
    command: ["/opt/report/.venv/bin/python", "-m", "report_worker"] # обязательно (кроме release: true)
    args: ["--verbose"] # дописываются к command
    dir: /opt/report # рабочий каталог
    env: { REPORT_MODE: fast } # переменные окружения
    replicas: 2 # копий процесса (1)
    queues: [example.render] # ограничить очереди на этом узле (по умолчанию — все объявленные)
    stopTimeout: 5m # сколько ждать доработки задач при остановке (30s)
    restart: rolling # rolling | stop-first
    limits: { memory: 512M, cpu: "50%", pids: 256 }
    user: report # от какого пользователя (агент — от root)
  - name: batch
    release: true # воркер из выпуска: файл ведёт агент
```

- каждой копии — свой процесс и канал связи: unix socketpair, дескриптор из `AGENT_IPC_FD`
  (3); по строке JSON на сообщение;
- переменные окружения: `AGENT_IPC_FD`, `AGENT_WORKER` (имя), `AGENT_VERSION`; воркеру из
  выпуска — ещё `AGENT_WORKER_RELEASE_VERSION`; со своим корневым сертификатом сервера —
  `AGENT_SERVER_CA_FILE` (Node — ещё `NODE_EXTRA_CA_CERTS`);
- stdout и stderr воркера агент пишет в свой лог с именем воркера построчно — они видны в
  `agent.logs` и уходят на сервер по `log.forward` ([observe.md](observe.md#лог-агента-и-воркеров));
- воркер должен зарегистрироваться не позже 120 с после запуска, иначе агент считает запуск
  неудачным.

**Что уходит по сети.** На узле: `worker.register {name, version, sdk, queues, commands,
domains, channels, ping}` → `worker.ready {agentVersion, rejected?}` → `worker.context`. Серверу
агент сообщает новые возможности сообщением `capabilities` (или в следующем `hello`).

**Проверка «жив ли».** Все SDK пишут в `worker.register` `ping: true` и отвечают на
`worker.ping` сообщением `worker.pong` (сами, без кода воркера). Агент шлёт `worker.ping` каждой
копии раз в 30 с; три запроса подряд без ответа за 10 с — копия зависла, агент перезапускает её
как упавшую. Обработчики задач и команд проверке не мешают: SDK отвечает из цикла приёма
сообщений (Python и Go — свой поток, Node — если обработчик не занимает цикл событий надолго).

**Воркер.**

```python
worker = Worker("report", version="1.0.0")   # имя по умолчанию — из AGENT_WORKER
...
worker.run()                                  # регистрация, работа до остановки; SIGTERM/SIGINT — сам
```

```go
w := worker.New("report", "1.0.0") // опции: worker.WithConn(conn), worker.WithLogger(l), worker.WithoutSignals()
if err := w.Run(ctx); err != nil {
	log.Fatal(err)
}
```

```ts
const worker = new Worker({ name: "report", version: "1.0.0" }); // name по умолчанию — AGENT_WORKER
await worker.run(); // { signals: false } — не ставить обработчики SIGTERM/SIGINT
```

Для тестов канал можно подменить: Go — `worker.WithConn(io.ReadWriteCloser)`, Node —
`new Worker({ transport: Duplex })`, Python — `Worker(…, channel=Channel(sock))`.

**Результат.** `agent.capabilities` пополняется очередями, командами, разделами, каналами;
`agent.status.workers[] = {name, state, instances, version, release?, health?, message?,
paused?}`.

## Регистрация и имена

**Бэкенд** видит, что объявлено, в `agent.capabilities`.

**Что уходит по сети.** `worker.ready.rejected` — имена, которые воркеру не достались.

**Агент** (`internal/worker/bridge.go`):

- имена очередей, команд, разделов и каналов — латиница, цифры, `.`, `_`, `-`, с буквы или
  цифры, до 64 символов (`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`);
- имена на `agent.` и `worker.` принадлежат агенту;
- каждое имя команды, раздела или канала принадлежит **первому** объявившему его воркеру;
  остальным — отказ в `rejected`. Очереди могут обслуживать несколько воркеров.

**Воркер.** Неверное имя SDK отклоняет сразу: Python и Node — исключение при объявлении, Go —
ошибка из `Run`. Не доставшиеся имена:

```python
worker.rejected        # множество имён
```

```go
w.Rejected() // []string
```

```ts
worker.rejected; // string[]
```

**Результат.** Отклонённые имена не попадают в `agent.capabilities`.

## Контекст воркера

Агент сообщает воркеру о себе: режим, кто он, есть ли связь, как часто сейчас уходят метрики,
на какие каналы воркера есть подписки, с какого уровня уходит лог.

**Бэкенд** напрямую контекст не задаёт: он меняется вместе со связью, подписками
([observe.md](observe.md#подписки)) и настройками агента.

**Что уходит по сети.** `worker.context` (агент → воркер, без ответа) — сразу после
`worker.ready` и при каждом изменении любого поля, не чаще раза в 100 мс; одинаковый повторно не
шлётся:

| Поле                | Что                                                                                             |
| ------------------- | ----------------------------------------------------------------------------------------------- |
| `mode`              | `run` — обычная работа; `cleanup` — запущен командой `agent cleanup` только для уборки          |
| `agent.id`          | id агента; пусто, пока агент не зарегистрирован                                                 |
| `agent.name`        | имя агента                                                                                      |
| `agent.version`     | версия агента                                                                                   |
| `agent.labels`      | метки агента                                                                                    |
| `online`            | есть ли связь с сервером                                                                        |
| `metricsIntervalMs` | действующая частота метрик агента, мс (с учётом подписок)                                       |
| `channels`          | `{канал: мс}` — подписки сервера на каналы **этого** воркера; нет подписок — поля нет           |
| `logLevel`          | с какого уровня агент сейчас отправляет лог на сервер (`off`, `error`, `warn`, `info`, `debug`) |

**Агент** (`internal/app`, `internal/worker/control.go`) собирает контекст из своего
состояния и рассылает всем копиям воркеров; каждая видит только свои каналы.

**Воркер.**

```python
from agent_sdk.worker import WorkerContext

@worker.on_context                       # или worker.on_context(fn)
def changed(ctx: WorkerContext) -> None:
    log.info("связь: %s, метрики раз в %d мс", ctx.online, ctx.metrics_interval_ms)

worker.context.agent.id                  # последний контекст: mode, agent, online, metrics_interval_ms, channels, log_level
```

```go
w.OnContext(func(c worker.Context) {
	log.Printf("связь: %v, метрики раз в %d мс", c.Online, c.MetricsIntervalMs)
})
id := w.Context().Agent.ID // Mode, Agent{ID, Name, Version, Labels}, Online, MetricsIntervalMs, Channels, LogLevel
```

```ts
worker.on("context", (ctx) => console.log(ctx.online, ctx.metricsIntervalMs));
worker.context.agent.id; // mode, agent, online, metricsIntervalMs, channels, logLevel
```

До первого сообщения — значения по умолчанию: `mode: run`, `online: false`,
`metricsIntervalMs: 0`. Обработчик вызывается из цикла приёма сообщений — долгую работу
выносите отдельно; исключение пишется в лог.

**Результат.** Ничего не уходит на сервер: контекст — для самого воркера (например, частота
своих проверок, пропуск подготовки в режиме `cleanup`, показатели с частотой подписки —
[observe.md](observe.md#показатели-воркеров)).

## Здоровье

Воркер сам сообщает «в порядке» или «не в порядке и почему»: например, потерял базу данных.

**Бэкенд** узнаёт об этом из статуса и уведомления:

```ts
agents.on("alert", (a) => a.type === "workerDegraded" && notify(a.agentName, a.worker, a.message));
```

```go
server.Options{OnAlert: func(a server.Alert) { /* a.Type == server.AlertWorkerDegraded */ }}
```

```python
agents.on("alert", lambda a: a.type == "workerDegraded" and notify(a))
```

**Что уходит по сети.** На узле — `worker.health {ok, message?}`; серверу — `status` с
`workers[].health = "ok" | "degraded"`, `workers[].message` и `state: "degraded"`.

**Агент** запоминает оценку каждой копии; хоть одна не в порядке — воркер `degraded`, и весь
агент — `status.state = degraded` с причиной в `status.message`.

**Воркер.**

```python
worker.set_health(False, "нет связи с базой")
worker.set_health(True)
```

```go
_ = w.SetHealth(false, "нет связи с базой")
_ = w.SetHealth(true, "")
```

```ts
worker.setHealth(false, "нет связи с базой");
worker.setHealth(true);
```

Здоровье, паузу, просьбу о перезапуске, события и показатели можно отправлять и до запуска
(`run` / `Run`): во всех SDK сообщения копятся и уходят сразу после того, как агент принял
воркер (`worker.ready`), по порядку. Копится не больше 1000 сообщений — лишние вытесняют самые
старые (запись в лог воркера).

**Результат.** `agent.status.workers[].health`, `.message`; `alert` `workerDegraded`
(`worker`, `message`) — начало при `degraded`, конец при `ok` или исчезновении воркера; плюс
`degraded` для агента ([events.md](events.md#уведомления-о-проблемах)).

## Пауза очередей

Воркер (или сервер) может временно не брать новые задачи своих очередей: выданные
доделываются. Пауза воркера и пауза сервера **независимы**: возобновление от сервера не снимает
паузу, выставленную воркером, и наоборот.

**Бэкенд.**

```ts
await agents.pauseWorker(agentId, "report", { queues: ["example.render"] }); // без queues — все очереди
await agents.resumeWorker(agentId, "report");
```

```go
cmd, err := agents.PauseWorker(agentID, "report", "example.render")
cmd, err = agents.ResumeWorker(agentID, "report")
```

```python
await agents.pause_worker(agent_id, "report", queues=["example.render"])
await agents.resume_worker(agent_id, "report")
```

Это встроенные команды `worker.pause` / `worker.resume` со сроком 30 с. Ошибки:
`AGENT_NOT_FOUND`, `AGENT_REVOKED`, `MESSAGE_INVALID` (имя не по правилу),
`COMMAND_NOT_SUPPORTED` (у агента нет воркеров или команда выключена). Аудит `worker.pause`,
`worker.resume` с `details: {worker, queues?, commandId}`.

**Что уходит по сети.** От сервера — `cmd.run {name: "worker.pause", args: {name, queues?}}` →
`cmd.done {result: {name, paused}}`. От воркера на узле — `worker.pause {queues?}` /
`worker.resume {queues?}`. Серверу — `status` с `slots` = 0 по этим очередям и
`workers[].paused: true`.

**Агент** (`internal/worker/control.go`, `internal/jobs`): места воркера по очередям на паузе —
0, новых `job.assign` ему не достаётся. Пауза сервера хранится у воркера в памяти агента и
переживает перезапуск копий воркера; пауза воркера — у его копии. Неизвестный воркер —
`WORKER_UNKNOWN`.

**Воркер.**

```python
worker.pause(["example.render"])   # без списка — все свои очереди
worker.resume(["example.render"])
```

```go
_ = w.Pause("example.render")
_ = w.Resume("example.render")
```

```ts
worker.pause(["example.render"]);
worker.resume(["example.render"]);
```

**Результат.** `agent.status.workers[].paused`, `agent.status.slots[queue] = 0`; задачи очереди
ждут в `queued` или уходят другим агентам.

## Просьба о перезапуске

Воркер может попросить агента заменить себя — например, заметил утечку памяти. Замена идёт
штатно, его способом (`rolling` или `stop-first`), без статуса сбоя и без уведомлений.

**Бэкенд** ничего не вызывает; заменить воркер с сервера — команда `worker.restart`
([ниже](#замена-воркера)).

**Что уходит по сети.** На узле — `worker.restart {reason?}`; серверу — обычные изменения
`status.workers`.

**Агент** делает то же, что по команде `worker.restart`. Повторные просьбы во время замены и
просьбы уходящей копии не учитываются. Причина — в лог агента.

**Воркер.**

```python
worker.request_restart("память выросла до 2 ГБ")
```

```go
_ = w.RequestRestart("память выросла до 2 ГБ")
```

```ts
worker.requestRestart("память выросла до 2 ГБ");
```

Этот процесс затем получит `worker.drain` (при `rolling`) или SIGTERM.

**Результат.** Новая копия воркера; задачи старой доделываются.

## Замена воркера

**Бэкенд.**

```ts
await agents.call({ name: "worker.restart", agentId, args: { name: "report" } }); // без name — все
```

```go
cmd, err := agents.Call(ctx, server.CommandRequest{Name: "worker.restart", AgentID: agentID, Args: map[string]string{"name": "report"}})
```

```python
cmd = await agents.call("worker.restart", {"name": "report"}, agent_id=agent_id)
```

**Что уходит по сети.** `cmd.run {name: "worker.restart", args: {name?}}` → `cmd.done {result:
{restarted: [...]}}`; ошибка — `WORKER_RESTART`.

**Агент** заменяет копии по `restart` воркера в `agent.yaml`:

- `rolling` (по умолчанию) — новая копия запускается рядом; когда она зарегистрировалась,
  старая получает `worker.drain`, перестаёт брать задачи и уходит, доделав текущие. Без
  простоя;
- `stop-first` — сначала уходит старая (не успела за `stopTimeout` — SIGTERM, затем SIGKILL),
  потом запускается новая. Для воркеров, которые держат то, что нельзя делить: порт, сетевой
  интерфейс, файл.

Новая копия снова получает последние снимки своих разделов состояния.

**Воркер.** По `worker.drain` SDK перестаёт брать задачи, дорабатывает текущие и выходит.
Свои фоновые циклы воркер останавливает по сигналу `stopping`:

```python
while not worker.stopping.wait(10):    # threading.Event
    probe()
```

```go
for {
	select {
	case <-w.Stopping():
		return
	case <-time.After(10 * time.Second):
		probe()
	}
}
```

```ts
const timer = setInterval(probe, 10_000);
worker.stopping.addEventListener("abort", () => clearInterval(timer)); // AbortSignal
```

**Результат.** `status.workers[].state`: `starting` → `running`; `instances`, `version`.

## Остановка и падение

**Бэкенд** ничего не вызывает.

**Что уходит по сети.** Серверу — `status.workers[].state` (`running`, `starting`, `backoff`,
`stopped`) и итоги задач.

**Агент:**

- **остановка** (SIGTERM агенту, перезапуск, замена) — воркер получает SIGTERM, дорабатывает
  задачи не дольше `stopTimeout`, затем SIGKILL. Всё, что воркер создал на узле (интерфейсы,
  правила, файлы), **остаётся** — следующий запуск это подхватит;
- **падение** — задачи воркера проваливаются с `WORKER_CRASHED` (сервер их повторит), агент
  перезапускает процесс с растущей паузой от 1 до 30 с (проработал больше минуты — пауза снова
  с начала); пока воркер в паузе, `state: backoff`, агент — `degraded`;
- **отмена задачи** — место задачи занято, пока воркер не подтвердит отмену (`job.fail` с кодом
  `CANCELLED`; SDK шлёт его сам, когда обработчик вернулся). Нет подтверждения за
  `jobs.cancelTimeout` (30 с) — агент заменяет копию воркера новой
  ([jobs.md](jobs.md#отмена-и-закончи-пораньше)).

**Воркер.** SDK сам: по SIGTERM (если не выключено) — `drain`: новых задач не брать, текущие
задачи и команды доработать и выйти. Пропала связь с агентом — всё отменить (задачи, ждущие
места в очереди, убираются без вызова обработчика) и выйти. Вручную — `worker.drain()` /
`w.Drain()`. Зависший воркер (не отвечает на `worker.ping`) агент перезапускает как упавший.

**Результат.** `alert` `workerDown` (воркер в `backoff`) — начало и конец
([events.md](events.md#уведомления-о-проблемах)).

## Уборка при удалении агента

Когда агента удаляют с узла, каждый воркер убирает за собой всё, что создал: интерфейсы,
правила, файлы. Связи с сервером при этом нет, и сервер уборку запустить не может;
перезапуск, замена и обычная остановка её не вызывают.

**Бэкенд** ничего не вызывает (удаление — `install.sh --uninstall` на узле,
[releases.md](releases.md#удаление-агента-с-узла)).

**Что уходит по сети** — только на узле: `worker.context {mode: "cleanup"}` →
`worker.cleanup` (запрос) → `worker.cleaned {ok, error?}`.

**Агент** (`agent cleanup`, `internal/worker/cleanup.go`): по очереди запускает каждый воркер из
настроек одной копией, без задач, после регистрации шлёт `worker.cleanup` и ждёт ответа не
дольше `stopTimeout`, затем останавливает воркер. Итог — в вывод команды; хоть один не убрал —
код выхода не ноль.

**Воркер.**

```python
@worker.cleanup
def cleanup() -> None:
    remove_interfaces()                 # исключение — {ok: false, error}

@worker.on_context
def prepare(ctx):
    if ctx.mode == "run":
        start_background_loops()        # в режиме cleanup подготовку можно пропустить
```

```go
w.Cleanup(func(ctx context.Context) error { return removeInterfaces() })
```

```ts
worker.cleanup(async () => {
  await removeInterfaces();
});
```

Нет обработчика — сразу `ok`. Воркер может объявить только уборку — без очередей, команд,
разделов и каналов; ничего не объявив, `run` завершается ошибкой. Показатели и события в режиме
уборки отбрасываются: сервера нет.

**Результат.** На сервере — ничего (агент удалён). На узле — вывод `agent cleanup`.

## Ограничения ресурсов и пользователь

**Бэкенд** не участвует: ограничения — настройки узла. Чтобы агент мог их ставить, установщик
пишет в службу `Delegate=yes` ([docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md#установка)).

**Агент** (`internal/cgroup`, `internal/worker/limits.go`):

- `limits` — на все копии воркера вместе, через cgroup v2: `memory` (`512M`, `2G`, байты),
  `cpu` (`50%` — половина ядра, `200%` или `2` — два ядра), `pids` (процессов и потоков). Агент
  переносит себя в подгруппу `agent` и заводит воркеру подгруппу `worker-<имя>`;
- cgroup v2 недоступны (не Linux, нет делегирования) — предупреждение в лог, воркер запускается
  без ограничений;
- `user` — запуск от другого пользователя (uid, gid, группы, `HOME`, `USER`, `LOGNAME` — его);
  нужно, чтобы агент работал от root.

**Воркер** ничего не делает.

**Результат.** Превысил память — процесс убивает ядро, это обычное [падение](#остановка-и-падение).

## Воркеры из выпуска

Воркер с `release: true` агент ведёт сам: исполняемый файл лежит в
`<dataDir>/workers/<name>/current`, `command` не задаётся. Ставит его `install.sh --worker`,
обновляет сервер командой `worker.update` с проверкой подписи и откатом. Добавить или удалить
воркер сервер не может. Подробно — [releases.md](releases.md#обновление-воркеров-из-выпуска).

**Воркер** из выпуска должен сообщать при регистрации ту же версию, что у его сборки в выпуске:
агент передаёт её в `AGENT_WORKER_RELEASE_VERSION`.

```go
w := worker.New("batch", os.Getenv("AGENT_WORKER_RELEASE_VERSION"))
```

## Изменить воркеры на ходу

Правка `agent.yaml` и `systemctl reload agent` (SIGHUP) — без остановки агента:

- новые воркеры запускаются, убранные доделывают задачи и уходят (их команды, разделы и каналы
  снимаются), изменённые заменяются своим способом, `replicas` меняется на лету;
- убранный воркер или выключенные возможности — агент переподключается с новым `hello`, очередь
  важных сообщений и начатые задачи сохраняются; добавленные воркеры связь не рвут.

Подробно — [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md#изменить-настройки-не-останавливая-агента).
