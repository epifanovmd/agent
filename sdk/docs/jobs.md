# Задачи

Задача — работа из очереди сервера: бэкенд ставит её, `Agents` отдаёт подходящему агенту,
агент — своему воркеру, итог возвращается в бэкенд. Задача переживает обрыв связи и
перезапуск агента, а если агент пропал — повторяется на другом. Справочник API —
[sdk/README.md](../README.md), формат — [sdk/spec §6.3](../spec/README.md#63-задачи-capabilitiesjobs),
образцы — [jobs.json](../spec/examples/jobs.json).

- [Поставить задачу](#поставить-задачу)
- [Места и выбор агента](#места-и-выбор-агента)
- [Срок и продление](#срок-и-продление)
- [Итог и повторы](#итог-и-повторы)
- [Отмена и «закончи пораньше»](#отмена-и-закончи-пораньше)
- [Прогресс, журнал, события задачи](#прогресс-журнал-события-задачи)
- [Файлы задачи](#файлы-задачи)
- [Сверка после переподключения](#сверка-после-переподключения)
- [Прочитать задачи](#прочитать-задачи)

## Поставить задачу

**Бэкенд.**

```ts
const job = await agents.enqueue({
  queue: "example.render", // обязательно; имя — по правилу имён
  data: { reportId: 42 }, // любые данные JSON
  maxAttempts: 3, // попыток всего (1)
  leaseSeconds: 300, // сколько задача живёт без вестей от агента (60, не меньше 5)
  inputs: { source: "…" }, // входные файлы: имя → содержимое (или URL — по провайдеру файлов)
  outputs: ["report"], // имена файлов-результатов
  agentId, // закрепить за агентом (необязательно)
});
```

```go
job, err := agents.Enqueue(server.JobRequest{
	Queue: "example.render", Data: map[string]any{"reportId": 42},
	MaxAttempts: 3, LeaseSeconds: 300, Outputs: []string{"report"}, AgentID: agentID,
})
```

```python
job = await agents.enqueue("example.render", {"reportId": 42}, max_attempts=3, lease_seconds=300,
                           outputs=["report"], agent_id=agent_id)
```

Ошибки: `MESSAGE_INVALID` (нет очереди, имя не по правилу), `AGENT_NOT_FOUND` (закрепили за
несуществующим агентом). От имени пользователя — `agents.by("ivan").enqueue(…)`
([events.md](events.md#журнал-аудита)).

**Что уходит по сети.** Сразу — ничего: задача ждёт в `Store` со статусом `queued`, пока у
какого-нибудь агента не освободится место ([следующий раздел](#места-и-выбор-агента)). Тогда —
`job.assign {jobId, attempt, queue, data, leaseSeconds, inputs, outputs, urlsExpireAt}`.

**Агент** — см. ниже.

**Воркер** объявляет очередь и число задач одновременно:

```python
@worker.job("example.render", concurrency=2)
def render(job: Job) -> dict:
    return {"pages": 3, "reportId": job.data["reportId"]}
```

```go
w.Job("example.render", 2, func(ctx context.Context, j *worker.Job) (any, error) {
	var in struct{ ReportID int `json:"reportId"` }
	if err := json.Unmarshal(j.Data, &in); err != nil {
		return nil, worker.Fail("BAD_INPUT", err.Error(), false)
	}
	return map[string]int{"pages": 3}, nil
})
```

```ts
worker.job("example.render", { concurrency: 2 }, async (job) => ({ pages: 3, reportId: job.data.reportId }));
```

Что есть у задачи в обработчике: `id`, `queue`, `data`, `attempt` (номер попытки с 0),
`leaseSeconds` (Go — поля `ID`, `Queue`, `Data`, `Attempt`, `LeaseSeconds`; Python —
`lease_seconds`).

**Результат.** `Job` со `status: "queued"`, `attempt: 0`, `actor`; событие `change`
`{kind: "job"}` (в Node — ещё `job`); аудит `job.enqueue`.

## Места и выбор агента

**Бэкенд** ничего не вызывает: `Agents` раздаёт задачи сам — при постановке, при каждом
`status` агента, после итога и по `refresh`.

- Задачу получает агент на связи, у которого есть **свободное место** в этой очереди
  (`status.slots` минус уже выданные, но ещё не подтверждённые).
- Из нескольких — **наименее загруженный** (задач в работе + выданных).
- Закреплённую (`agentId`) — только свой агент.
- Агентам в состоянии `draining`, `updating`, `starting` задачи не выдаются.

**Что уходит по сети.** От агента — `status {slots, capacity, jobs}` при каждом изменении
мест; от сервера — `job.assign`. Агент отвечает `job.accept` или, если взять не может,
`job.reject {code}` ([§6.3](../spec/README.md#63-задачи-capabilitiesjobs)).

**Агент** (`internal/jobs`, `internal/worker`):

- места очереди — сумма `concurrency` всех запущенных копий воркеров, обслуживающих её, минус
  задачи в работе. Копий — `replicas` в `agent.yaml`; `queues` воркера в `agent.yaml`
  ограничивает очереди, которые он берёт на этом узле;
- задачу получает копия воркера с наибольшим числом свободных мест;
- места — ноль, если агент дорабатывает (`agent.drain`) или очередь на паузе
  ([workers.md](workers.md#пауза-очередей));
- взять не может — `job.reject` с кодом `QUEUE_NOT_SERVED` (очередь здесь никто не обслуживает)
  или `QUEUE_BUSY` (мест нет); сервер возвращает задачу в очередь той же попыткой;
- повтор `job.assign` той же попытки (после переподключения) — только `job.accept`, задача не
  запускается дважды.

**Воркер.** `concurrency` — сколько задач очереди он выполняет одновременно. Задача, пришедшая,
когда воркер уже останавливается, возвращается с `job.fail WORKER_STOPPING` (`retryable`), и
сервер отдаст её другим.

**Результат.** `job.status: "running"`, `job.agentId`, затем `job.accepted: true`; у агента —
`agent.status.slots`, `agent.status.capacity`, `agent.status.jobs`.

## Срок и продление

Задача выполняется под сроком `leaseSeconds`: так сервер узнаёт, что агент пропал.

**Бэкенд** задаёт срок при постановке (`leaseSeconds`, по умолчанию 60, не меньше 5). Для
долгих задач это и есть время, которое агент может пробыть без связи, не потеряв задачу.

**Что уходит по сети.** Каждый `status`, где задача упомянута в `jobs`, продлевает срок до
`сейчас + leaseSeconds`.

**Агент** перечисляет свои задачи в каждом `status` (не реже `statusIntervalMs`, по умолчанию
5 с). Без связи срок не продлевается, но задача продолжает выполняться.

**Воркер** ничего не делает: продлевает агент.

**Результат.**

- агент вернулся до конца срока — задача продолжается как ни в чём не бывало;
- срок истёк — попытка проваливается с кодом `LEASE_EXPIRED` и уходит на повтор (если попытки
  остались); агенту, если он на связи, — `job.cancel`;
- агент вернулся позже — сервер шлёт ему `job.cancel`, а итог старой попытки отклоняет
  (`JOB_LEASE_LOST`): агент удаляет его из очереди на диске.

## Итог и повторы

**Бэкенд** задаёт число попыток (`maxAttempts`, по умолчанию 1) и узнаёт итог из событий:

```ts
agents.on("job", (job) => {
  if (job.status === "completed") save(job.id, job.result);
  if (job.status === "failed") alarm(job.id, job.error); // { code, message }
});
```

```go
var agents *server.Agents
agents = server.New(server.Options{EnrollToken: token, OnChange: func(c server.Change) {
	if c.Kind == server.ChangeJob {
		job, _ := agents.Job(c.ID) // OnChange вызывается вне блокировки — читать можно
		handle(job)
	}
}})
```

```python
async def on_change(change):
    if change.kind == "job":
        job = await agents.get_job(change.id)

agents.on("change", on_change)
```

**Что уходит по сети.** `job.complete {jobId, attempt, result}` или `job.fail {jobId, attempt,
code, message, retryable}` — важные сообщения: доходят хотя бы раз, повтор сервер узнаёт.

**Агент** (`internal/jobs`, `internal/outbox`):

- итог записывается в `<dataDir>/outbox` и лежит там, пока сервер не подтвердит, — переживает
  обрыв связи и перезапуск агента;
- воркер упал — все его задачи проваливаются с `WORKER_CRASHED` (`retryable`), агент
  перезапускает воркер;
- задачу не удалось передать воркеру — `WORKER_ERROR`.

**Воркер.** Вернуть значение — успех; ошибка — провал:

```python
from agent_sdk.worker import JobFailed

@worker.job("example.render")
def render(job: Job) -> dict:
    if "reportId" not in job.data:
        raise JobFailed("BAD_INPUT", "нет reportId", retryable=False)  # без повторов
    raise RuntimeError("временная ошибка")                            # WORKER_ERROR, повтор
```

```go
return nil, worker.Fail("BAD_INPUT", "нет reportId", false) // свой код, без повторов
return nil, errors.New("временная ошибка")                   // WORKER_ERROR, повтор
```

```ts
import { JobError } from "agent-sdk/worker";
throw new JobError("BAD_INPUT", "нет reportId", { retryable: false });
throw new Error("временная ошибка"); // WORKER_ERROR, повтор
```

Код — заглавные латинские буквы, цифры и `_`, до 64 символов; иначе SDK подставит
`WORKER_ERROR`. Паника или исключение в обработчике — `WORKER_ERROR`, воркер не падает.

**Результат.**

- успех — `status: "completed"`, `result`, `progress: 1`, `finishedAt`;
- провал с `retryable: true` и оставшимися попытками — снова `queued`, `attempt + 1`,
  `error` — ошибка прошлой попытки; задача уйдёт любому подходящему агенту;
- иначе — `status: "failed"`, `error: {code, message}`, `finishedAt`.

Коды, которые ставят агент, SDK и сервер: `WORKER_ERROR`, `WORKER_CRASHED`, `WORKER_STOPPING`,
`AGENT_LOST`, `LEASE_EXPIRED`.

## Отмена и «закончи пораньше»

**Бэкенд.**

```ts
await agents.cancelJob(jobId); // прервать, итог не нужен → Job со status "cancelled"
await agents.stopJob(jobId); // доделать текущий шаг и вернуть результат
```

```go
err := agents.CancelJob(jobID)
err = agents.StopJob(jobID)
```

```python
await agents.cancel_job(job_id)
await agents.stop_job(job_id)
```

Задача ещё в очереди — обе её просто отменяют. Ошибки: `JOB_NOT_FOUND`, `JOB_NOT_ACTIVE`
(задача уже завершена). Аудит — `job.cancel`, `job.stop`.

**Что уходит по сети.** `job.cancel {jobId, attempt}` или `job.stop {jobId, attempt}`. Агент не
на связи — `job.stop` придёт при переподключении (флаг `stopRequested` хранится в задаче);
отменённую задачу агент из `hello` получит `job.cancel`.

**Агент** (`internal/jobs`): `job.cancel` — передаёт воркеру и забывает задачу, итог не
отправит; отмена пришла раньше назначения — помнит её 10 минут и назначение пропустит.
`job.stop` — передаёт воркеру.

**Воркер.**

```python
@worker.job("example.batch")
def batch(job: Job) -> dict:
    done = 0
    for item in job.data["items"]:
        job.check_cancelled()          # отменили — бросит Cancelled, итог не уйдёт
        if job.stop_requested:         # просят закончить пораньше
            break
        process(item)
        done += 1
    return {"done": done}
```

```go
w.Job("example.batch", 1, func(ctx context.Context, j *worker.Job) (any, error) {
	done := 0
	for _, item := range items(j) {
		select {
		case <-ctx.Done(): // отменили
			return nil, ctx.Err()
		case <-j.StopRequested(): // закончить пораньше
			return map[string]int{"done": done}, nil
		default:
		}
		process(item)
		done++
	}
	return map[string]int{"done": done}, nil
})
```

```ts
worker.job("example.batch", async (job) => {
  let done = 0;
  for (const item of job.data.items) {
    if (job.signal.aborted) return; // отменили: итог не отправится
    if (job.stopRequested) break;
    await process(item, { signal: job.signal });
    done++;
  }
  return { done };
});
```

**Результат.** Отмена — `status: "cancelled"`, `finishedAt`. Остановка — `stopRequested:
true`, затем обычный `completed` с тем, что успели.

## Прогресс, журнал, события задачи

**Бэкенд** читает поля задачи и событие `job`:

```ts
agents.on("job", (job) => ui.update(job.id, { progress: job.progress, text: job.text, log: job.log }));
```

```go
job, _ := agents.Job(jobID) // job.Progress, job.Text, job.Log, job.Events
```

```python
job = await agents.get_job(job_id)  # job.progress, job.text, job.log, job.events
```

**Что уходит по сети.**

- `job.progress {progress?, text?, log?}` — обычное сообщение: может потеряться, следующее его
  заменит;
- `job.event {seq, type, data}` — важное: доходит гарантированно, по порядку; `seq` с 1 внутри
  попытки, повтор сервер отбрасывает.

**Агент** пересылает их серверу, только пока задача за ним; `job.event` — через очередь на
диске.

**Воркер.**

```python
job.progress(0.4, "страница 4 из 10")   # 0..1 и текст до 200 символов
job.log("загружен шаблон")               # строка журнала до 1000 символов
job.event("page.ready", {"page": 4})     # событие этапа, тип до 50 символов
```

```go
j.Progress(0.4, "страница 4 из 10")
j.Log("загружен шаблон")
_ = j.Event("page.ready", map[string]int{"page": 4})
```

```ts
job.progress(0.4, "страница 4 из 10");
job.log("загружен шаблон");
job.event("page.ready", { page: 4 });
```

SDK прореживает прогресс и журнал: агенту — не чаще двух раз в секунду, журнал — пачками до 100
строк. Перед событием накопленный прогресс уходит сразу.

**Результат.** `job.progress`, `job.text`, `job.log` (последние 500 строк), `job.events`
(`[{seq, type, data, at}]`); событие `change` `{kind: "job"}`.

## Файлы задачи

Файлы хранит бэкенд (например, в S3). Агент и воркер получают **ссылки**: скачать входные,
загрузить результаты (`PUT`).

**Бэкенд.** По умолчанию — `MemoryFiles`: файлы в памяти, `inputs` — само содержимое, `Agents`
сам обслуживает `GET`/`PUT /files/<jobId>/(in|out)/<имя>`.

```ts
const job = await agents.enqueue({
  queue: "example.render",
  inputs: { source: "<csv>…" },
  outputs: ["report"],
});
// после завершения (MemoryFiles):
const pdf = files.get(job.id, "out", "report"); // const files = new MemoryFiles(); new Agents({ files, … })
```

Свой провайдер — подписанные ссылки своего хранилища:

```ts
import type { Files } from "agent-sdk/server";

const s3Files: Files = {
  async saveInputs(jobId, inputs) {
    /* записать входные файлы при постановке */
  },
  async urls(job, baseUrl, only) {
    // only — имена из запроса job.urls; можно отдать и все ссылки
    return {
      inputs: Object.fromEntries(job.inputs.map((n) => [n, presignGet(`${job.id}/in/${n}`)])),
      outputs: Object.fromEntries(job.outputs.map((n) => [n, { url: presignPut(`${job.id}/out/${n}`) }])),
      expiresAt: Date.now() + 3600_000,
    };
  },
};
new Agents({ enrollToken, files: s3Files });
```

```go
// server.Files: Inputs(job, inputs) error и URLs(job, baseURL) (message.JobURLs, error)
agents := server.New(server.Options{EnrollToken: token, Files: myS3Files{}})
```

```python
class S3Files(Files):                       # from agent_sdk.server import Files
    async def prepare(self, job, inputs): ...
    async def urls(self, job, base_url):
        return {"inputs": {...}, "outputs": {...}, "expiresAt": now_ms() + 3600_000}

agents = Agents(enroll_token=token, files=S3Files())
```

**Что уходит по сети.** Ссылки — в `job.assign` (`inputs`, `outputs`, `urlsExpireAt`). Свежие —
запросом `job.urls {jobId, attempt, inputs?, outputs?}` → ответ `{inputs, outputs, expiresAt}`
(перечислены имена — только они).

**Агент** пересылает запрос `job.urls` серверу и ответ воркеру (ждёт до 30 с). Не получилось
(связи нет, задача уже не за агентом, сервер ответил ошибкой) — воркеру ошибка `URLS_UNAVAILABLE`. Сами файлы агент не трогает:
скачивает и загружает воркер, со своим корневым сертификатом из `AGENT_SERVER_CA_FILE`.

**Воркер.**

```python
@worker.job("example.render")
def render(job: Job) -> dict:
    source = job.input_path("source")        # скачать во временный каталог задачи (один раз)
    pdf = build(source.read_text())
    job.upload("report", pdf)                # bytes или путь к файлу
    return {"size": len(pdf)}
```

```go
path, err := j.InputPath(ctx, "source")
if err != nil {
	return nil, err
}
if err := j.Upload(ctx, "report", pdf); err != nil { // или j.UploadFile(ctx, "report", "/tmp/report.pdf")
	return nil, err
}
```

```ts
const source = await job.inputPath("source");
await job.upload("report", pdfBytes); // Uint8Array или путь к файлу
```

Ещё: `download(name, target)` — скачать в своё место; `inputs`, `outputs` — имена файлов;
`refreshUrls(inputs?, outputs?)` (`refresh_urls`, `RefreshURLs`) — свежие ссылки вручную. Ссылка
истекает меньше чем через минуту — SDK берёт свежую сам. Загрузка не удалась — до 4 попыток с
растущей паузой, каждая со свежей ссылкой. Временный каталог задачи удаляется после её конца.

**Результат.** `job.inputs`, `job.outputs` — имена файлов; содержимое — в вашем хранилище (у
`MemoryFiles` — `files.get(jobId, "out", name)`, в Node при загрузке — ещё событие `job`).

## Сверка после переподключения

**Бэкенд** ничего не вызывает.

**Что уходит по сети.** В `hello.jobs` агент перечисляет свои задачи: выполняющиеся и
**завершённые без связи, чей итог ещё не подтверждён**. Сервер сверяет список раньше, чем
получит итоги ([§6.3](../spec/README.md#63-задачи-capabilitiesjobs)).

**Агент** (`internal/jobs`, `internal/outbox`): берёт выполняющиеся задачи из памяти и ссылки на
задачи из неподтверждённых итогов в `<dataDir>/outbox`; после `welcome` досылает итоги.
Перезапуск агента выполняющиеся задачи теряет (их нет на диске) — их итогов нет и в outbox.

**Воркер** не участвует.

**Результат** — для каждой задачи, которую сервер считает выполняющейся на агенте:

| Агент в `hello`                       | Что делает сервер                                                               |
| ------------------------------------- | ------------------------------------------------------------------------------- |
| перечислил                            | задача продолжается; была просьба «закончить пораньше» — повторяет `job.stop`   |
| не перечислил и не подтверждал        | присылает её снова (`job.assign`)                                               |
| не перечислил, но подтверждал         | задача потеряна: попытка проваливается с `AGENT_LOST` и повторяется по правилам |
| перечислил задачу, которой у него нет | `job.cancel` (её уже отдали другому или отменили)                               |

## Прочитать задачи

```ts
const job = await agents.getJob(id); // Job | undefined
const running = await agents.listJobs({ status: "running", queue: "example.render", agentId }); // новые первыми
```

```go
job, err := agents.Job(id) // server.ErrNotFound — нет
all, err := agents.Jobs()  // новые первыми
```

```python
job = await agents.get_job(job_id)  # None — нет
jobs = await agents.list_jobs(status="running", queue="example.render", agent_id=agent_id)  # новые первыми
```

Поля `Job`: `id`, `queue`, `data`, `status` (`queued` | `running` | `completed` | `failed` |
`cancelled`), `attempt`, `maxAttempts`, `leaseSeconds`, `agentId`, `pinnedAgentId`, `accepted`,
`progress`, `text`, `log`, `events`, `result`, `error`, `stopRequested`, `inputs`, `outputs`,
`createdAt`, `finishedAt`, `actor`. `MemoryStore` хранит ограниченное число завершённых задач;
в работе — свой `Store` ([store.md](store.md)).
