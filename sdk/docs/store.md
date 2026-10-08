# Хранилище

`Store` — интерфейс, через который `Agents` хранит агентов, задачи, команды, состояние с
историей, события и историю метрик. `MemoryStore` держит всё в памяти процесса — для разработки
и одного процесса. Для работы по-настоящему реализуйте `Store` поверх своей БД: всё, что должно
пережить перезапуск бэкенда, `Agents` пишет только туда. Справочник API —
[sdk/README.md](../README.md).

- [Что хранит Store](#что-хранит-store)
- [Методы по языкам](#методы-по-языкам)
- [Служебные поля](#служебные-поля)
- [Правила](#правила)
- [Уборка](#уборка)
- [MemoryStore](#memorystore)
- [Пример на SQL (Postgres)](#пример-на-sql-postgres)

## Что хранит Store

| Что               | Модель                                                      | Кто пишет                                                                             |
| ----------------- | ----------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| агенты            | `Agent` (+ служебные поля)                                  | регистрация, каждое сообщение агента (`lastSeenAt`, статус, метрики), подписки, отзыв |
| задачи            | `Job` (+ служебные поля)                                    | постановка, выдача, прогресс, события, итог, сроки                                    |
| команды           | `Command` (+ `rev`)                                         | создание, начало, вывод, итог, срок, отмена                                           |
| снимки состояния  | `DesiredState` по ключу (раздел, агент; общий — без агента) | `setState`, `deleteState`, `rollbackState`                                            |
| история состояния | `DesiredState`                                              | каждый `setState` (и откат, и переиздание общего)                                     |
| события           | `AgentEvent`                                                | сообщения `event` воркеров                                                            |
| история метрик    | `MetricsPoint` по агенту                                    | точки `metrics` (с прореживанием), чистка по сроку                                    |

Активные уведомления о проблемах хранятся в записи агента (служебное поле `alerts`), поэтому
`alerts()` видит их из любого процесса. Аудит и логи `Agents` **не** хранит — они приходят
событиями ([events.md](events.md), [observe.md](observe.md#лог-агента-и-воркеров)).

## Методы по языкам

В Go методы обычные и возвращают `error` (нет записи — `server.ErrNotFound`); в Node и Python —
асинхронные (нет записи — `undefined` / `None`). Задачи и команды — новые первыми (в SQL —
`ORDER BY created_at DESC, id DESC`): `Agents` отдаёт их наружу как есть, а раздаёт задачи и
доставляет команды агенту старыми первыми. Остальные списки — в порядке создания, если не
сказано иное.

| Что                  | Go (`server.Store`)                                                                                    | Node (`Store`)                                                                                 | Python (`Store`)                                                                 |
| -------------------- | ------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| создать агента       | `CreateAgent(a *Agent) error`                                                                          | `createAgent(agent: AgentRecord): Promise<void>`                                               | `create_agent(agent: Agent) -> None`                                             |
| прочитать агента     | `GetAgent(id string) (*Agent, error)`                                                                  | `getAgent(id): Promise<AgentRecord \| undefined>`                                              | `get_agent(agent_id) -> Agent \| None`                                           |
| записать агента      | `UpdateAgent(a *Agent) (bool, error)` — по `Rev`                                                       | `updateAgent(agent: AgentRecord): Promise<boolean>` — по `rev`                                 | `update_agent(agent: Agent) -> bool` — по `rev`                                  |
| удалить агента       | `DeleteAgent(id string) (bool, error)` — был ли                                                        | `deleteAgent(id): Promise<boolean>`                                                            | `delete_agent(agent_id) -> bool`                                                 |
| все агенты           | `ListAgents() ([]*Agent, error)`                                                                       | `listAgents(): Promise<AgentRecord[]>`                                                         | `list_agents() -> List[Agent]`                                                   |
| создать задачу       | `CreateJob(j *Job) error`                                                                              | `createJob(job: JobRecord)`                                                                    | `create_job(job: Job)`                                                           |
| прочитать задачу     | `GetJob(id) (*Job, error)`                                                                             | `getJob(id): Promise<JobRecord \| undefined>`                                                  | `get_job(job_id) -> Job \| None`                                                 |
| записать задачу      | `UpdateJob(j *Job) (bool, error)` — по `Rev`                                                           | `updateJob(job: JobRecord): Promise<boolean>` — по `rev`                                       | `update_job(job: Job) -> bool` — по `rev`                                        |
| задачи               | `ListJobs(f JobFilter) ([]*Job, error)` — `{Status, Queue, AgentID, Limit, After}`                     | `listJobs(filter?: {status?, queue?, agentId?, limit?, after?})` — `status` может быть списком | `list_jobs(*, status=None, queue=None, agent_id=None, limit=0, after=None)`      |
| создать команду      | `CreateCommand(c *Command) error`                                                                      | `createCommand(cmd: CommandRecord)`                                                            | `create_command(command: Command)`                                               |
| прочитать команду    | `GetCommand(id) (*Command, error)`                                                                     | `getCommand(id): Promise<CommandRecord \| undefined>`                                          | `get_command(command_id) -> Command \| None`                                     |
| записать команду     | `UpdateCommand(c *Command) (bool, error)` — по `Rev`                                                   | `updateCommand(cmd: CommandRecord): Promise<boolean>` — по `rev`                               | `update_command(command: Command) -> bool` — по `rev`                            |
| команды              | `ListCommands(f CommandFilter)` — `{Status, AgentID, Limit, After}`                                    | `listCommands(filter?: {status?, agentId?, limit?, after?})` — `status` может быть списком     | `list_commands(*, status=None, agent_id=None, limit=0, after=None)`              |
| новый снимок         | `SetState(domain, agentID string, spec json.RawMessage, actor string) (*DesiredState, error)`          | `setState(domain, agentId: string \| undefined, spec, actor?): Promise<DesiredState>`          | `set_state(domain, agent_id, spec, *, actor=None) -> DesiredState`               |
| прочитать снимок     | `GetState(domain, agentID) (*DesiredState, error)`                                                     | `getState(domain, agentId?)`                                                                   | `get_state(domain, agent_id) -> DesiredState \| None`                            |
| удалить снимок       | `DeleteState(domain, agentID) (bool, error)` — был ли                                                  | `deleteState(domain, agentId?): Promise<boolean>`                                              | `delete_state(domain, agent_id) -> bool`                                         |
| все снимки           | `ListStates() ([]*DesiredState, error)`                                                                | `listStates()`                                                                                 | `list_states()`                                                                  |
| история раздела      | `ListStateHistory(domain, agentID, limit) ([]*DesiredState, error)` — новые первыми, `limit ≤ 0` — все | `listStateHistory(domain, agentId \| undefined, limit)` — новые первыми, `limit ≤ 0` — все     | `list_state_history(domain, agent_id, limit)` — новые первыми, `limit ≤ 0` — все |
| событие              | `AddEvent(e AgentEvent) error`                                                                         | `addEvent(event: AgentEvent)`                                                                  | `add_event(event: AgentEvent)`                                                   |
| события              | `ListEvents(limit) ([]AgentEvent, error)` — последние, новые первыми; `limit ≤ 0` — все                | `listEvents(limit?)` — последние, новые первыми; `limit ≤ 0` — все                             | `list_events(limit=100)` — последние, новые первыми; `limit ≤ 0` — все           |
| точка метрик         | `AddMetrics(agentID string, p MetricsPoint) error`                                                     | `addMetrics(agentId, point: MetricsPoint)`                                                     | `add_metrics(agent_id, point: MetricsPoint)`                                     |
| история метрик       | `ListMetrics(agentID string, since int64) ([]MetricsPoint, error)`                                     | `listMetrics(agentId, since?): Promise<MetricsPoint[]>`                                        | `list_metrics(agent_id, since=None)`                                             |
| удалить старые точки | `PruneMetrics(before int64) (int, error)` — сколько удалено                                            | `pruneMetrics(before): Promise<number>`                                                        | `prune_metrics(before) -> int`                                                   |
| уборка               | `Prune(p PruneBefore) (int, error)` — `{Jobs, Commands, Events}`                                       | `prune(before: {jobsBefore?, commandsBefore?, eventsBefore?}): Promise<number>`                | `prune(*, jobs_before=None, commands_before=None, events_before=None) -> int`    |

- `ListMetrics` — точки строго позже `since` по возрастанию `at`; `PruneMetrics` — удалить точки
  **всех** агентов с `at < before`.
- **Страницы** (`limit`, `after`): `after` — id последней записи прошлой страницы; страница —
  записи **после** неё в том же порядке (новые первыми), не больше `limit`; `limit ≤ 0` — все.
  Место `after` ищется среди всех записей без учёта фильтра (сама запись `after` может под фильтр
  не подходить), фильтр применяется к записям после неё. Записи `after` нет (например, её удалила
  уборка) — страница пустая. В SQL — сравнение `(created_at, id)` с записью `after`, как в
  [примере](#пример-на-sql-postgres).
- `DeleteAgent` удаляет запись агента и его историю метрик; задачи, команды, события и снимки
  состояния остаются.
- `Prune` удаляет завершённые задачи и команды с `finishedAt` раньше `Jobs` / `Commands` и
  события с `at` раньше `Events` (мс; `0` или нет поля — этот вид не трогать); возвращает,
  сколько записей удалено.

## Служебные поля

В записях есть поля, которые наружу (в JSON для интерфейса) не отдаются, но **хранить их
нужно**. Набор одинаковый во всех SDK:

| Запись  | Go (`json:"-"`)                                                                                       | Node (`AgentRecord`, `JobRecord`, `CommandRecord`)                                                    | Python (`to_record()` / `from_record()`)                                                                     |
| ------- | ----------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| агент   | `SecretHash`, `PendingSecretHash`, `GrantedLabels`, `BootID`, `LastSeq`, `MetricsAt`, `Alerts`, `Rev` | `secretHash`, `pendingSecretHash`, `grantedLabels`, `bootId`, `lastSeq`, `metricsAt`, `alerts`, `rev` | `secret_hash`, `pending_secret_hash`, `granted_labels`, `boot_id`, `last_seq`, `metrics_at`, `alerts`, `rev` |
| задача  | `LeaseUntil`, `EventSeq`, `Rev`                                                                       | `leaseUntil`, `eventSeq`, `rev`                                                                       | `lease_until`, `event_seq`, `rev`                                                                            |
| команда | `Rev`                                                                                                 | `rev`                                                                                                 | `rev`                                                                                                        |

- `secretHash` — sha256 ключа агента; без него агент не войдёт. `pendingSecretHash` — ключ после
  смены, пока агент не вошёл с ним ([connection.md](connection.md#смена-ключа));
- `grantedLabels` — метки, выданные при регистрации: агент не может их переписать;
- `bootId`, `lastSeq` — запуск агента и последний принятый номер его обычных сообщений
  ([§4](../spec/README.md#4-конверт-и-способы-доставки)): повтор с тем же или меньшим номером
  только подтверждается. Номер в `Store`, а не в памяти процесса, поэтому повтор после
  переподключения к другому процессу бэкенда не обрабатывается второй раз;
- `metricsAt` — время текущей точки `metrics` агента по часам сервера: текущими остаются
  метрики самой поздней живой точки;
- `alerts` — активные уведомления о проблемах агента
  ([events.md](events.md#уведомления-о-проблемах));
- `leaseUntil` — срок попытки задачи; `eventSeq` — последний принятый номер события задачи;
- `rev` — номер версии записи для условной записи ([правила](#правила)).

В Python `to_dict()` — JSON для интерфейса без служебных полей; `to_record()` — всё для БД,
`Model.from_record(dict)` — обратно. В Node — `publicAgent(record)` / `publicJob(record)` /
`publicCommand(record)` делает `Agents` сам, `Store` получает и возвращает записи целиком.

## Правила

1. **Условная запись.** `update*` (агент, задача, команда) пишет запись, только если в
   хранилище у неё тот же `rev`, что у переданной, — то есть запись не менялась с тех пор, как
   её прочитали. Записанная получает `rev + 1` (и у переданного объекта `rev` тоже
   увеличивается на 1). Не совпало — запись не трогается, результат `false`. Проверка и запись —
   одним действием: в SQL — `UPDATE … WHERE id = $1 AND rev = $2`, а не чтение и запись
   отдельными запросами. `create*` сохраняет `rev` как есть.
2. **Чтение — изменение — запись.** `Agents` меняет записи только так: прочитал, изменил свою
   часть, записал условно; `false` — перечитал и повторил (до 8 раз). Поэтому несколько
   процессов бэкенда не выдают одну задачу дважды, не затирают отзыв агента, его подписки и
   новый ключ, а итог команды или задачи принимается только из незавершённого состояния. Все 8
   попыток не записались — метод `Agents` завершается ошибкой `STORE_CONFLICT` (HTTP 409); сообщение
   агента, на котором это случилось, получает `error` с `retryable: true` (или остаётся без
   подтверждения), и агент присылает его снова.
3. **Версия состояния только растёт** — в пределах раздела, общая для общего и личных снимков,
   и между перезапусками бэкенда: `версия = max(наибольшая выданная в разделе + 1, текущее время
в мс)`. `deleteState` счётчик не сбрасывает. Агент пропускает снимок, если он не новее
   применённого, — поэтому «откат» версии на сервере сломал бы доставку.
4. **История**: каждый `setState` — запись в историю раздела (общего или агента);
   `deleteState` историю не стирает. Сколько хранить — решаете вы (`MemoryStore` — 50 на
   раздел и агента). `rollbackState` ищет версию в истории.
5. **Копии.** `get*` возвращает копию, `Agents` меняет её и сохраняет `update*`. Не отдавайте
   из `Store` объекты, которые потом меняете сами.
6. **Одновременные вызовы.** `Store` вызывают одновременно несколько процессов бэкенда, а в Go —
   и несколько горутин одного процесса: методы должны это выдерживать (пул соединений БД,
   мьютекс в памяти).
7. **Метрики**: точки досланных метрик (`backfill`) приходят с `at` раньше уже сохранённых —
   вставляйте по `at`; `pruneMetrics` вызывается при запуске и раз в час
   (`metricsRetentionMs`).
8. **Чистка** завершённых задач, команд и событий — `prune` по вашему расписанию
   ([ниже](#уборка)): сам `Agents` их не удаляет.

## Уборка

`prune` у `Agents` удаляет из `Store` старые завершённые задачи и команды и старые события.
Запускайте его по своему расписанию, например раз в час:

```ts
const removed = await agents.prune({
  jobsOlderThanMs: 30 * 86_400_000,
  commandsOlderThanMs: 7 * 86_400_000,
  eventsOlderThanMs: 7 * 86_400_000,
});
```

```go
removed, err := agents.Prune(server.PruneOptions{
	JobsOlderThan: 30 * 24 * time.Hour, CommandsOlderThan: 7 * 24 * time.Hour, EventsOlderThan: 7 * 24 * time.Hour,
})
```

```python
removed = await agents.prune(jobs_older_than_ms=30 * 86_400_000,
                             commands_older_than_ms=7 * 86_400_000,
                             events_older_than_ms=7 * 86_400_000)
```

Не задан срок — этот вид записей не трогается. Задачи в очереди и в работе и незавершённые
команды не удаляются никогда. Удалить запись агента — `deleteAgent(id)`, только после
`revoke` ([connection.md](connection.md#отзыв-агента)).

## MemoryStore

```ts
new Agents({ enrollToken, store: new MemoryStore({ keepJobs: 2000, keepMetrics: 4320 }) });
```

```go
store := server.NewMemoryStore()
store.KeepJobs = 2000
agents := server.New(server.Options{EnrollToken: token, Store: store})
```

```python
agents = Agents(enroll_token=token, store=MemoryStore(keep_jobs=2000, keep_metrics=4320))
```

| Сколько хранит (по умолчанию, во всех SDK) | Go                 | Node               | Python               | Сколько |
| ------------------------------------------ | ------------------ | ------------------ | -------------------- | ------- |
| завершённых задач                          | `KeepJobs`         | `keepJobs`         | `keep_jobs`          | 1000    |
| завершённых команд                         | `KeepCommands`     | `keepCommands`     | `keep_commands`      | 500     |
| событий                                    | `KeepEvents`       | `keepEvents`       | `keep_events`        | 1000    |
| точек метрик на агента                     | `KeepMetrics`      | `keepMetrics`      | `keep_metrics`       | 4320    |
| снимков истории на раздел и агента         | `KeepStateHistory` | `keepStateHistory` | `keep_state_history` | 50      |

Задачи в очереди и в работе и незавершённые команды не удаляются; отменённые команды
считаются завершёнными. После перезапуска бэкенда
всё пропадает; версии состояния остаются растущими, потому что начинаются не меньше текущего
времени.

## Пример на SQL (Postgres)

Схематично: записи — в `jsonb`, ключевые поля и `rev` — отдельными колонками для фильтров и
условной записи.

```sql
CREATE TABLE agents   (id text PRIMARY KEY, rev bigint NOT NULL, created_at bigint NOT NULL, record jsonb NOT NULL);
CREATE TABLE jobs     (id text PRIMARY KEY, rev bigint NOT NULL, status text NOT NULL, queue text NOT NULL,
                       agent_id text, created_at bigint NOT NULL, finished_at bigint, record jsonb NOT NULL);
CREATE TABLE commands (id text PRIMARY KEY, rev bigint NOT NULL, status text NOT NULL, agent_id text NOT NULL,
                       created_at bigint NOT NULL, finished_at bigint, record jsonb NOT NULL);
CREATE TABLE state_versions (domain text PRIMARY KEY, last bigint NOT NULL);
CREATE TABLE states   (domain text, agent_id text NOT NULL DEFAULT '', version bigint NOT NULL,
                       record jsonb NOT NULL, created_at bigint NOT NULL, PRIMARY KEY (domain, agent_id));
CREATE TABLE state_history (domain text, agent_id text NOT NULL DEFAULT '', version bigint,
                       record jsonb NOT NULL, PRIMARY KEY (domain, agent_id, version));
CREATE TABLE events   (seq bigserial PRIMARY KEY, at bigint NOT NULL, record jsonb NOT NULL);
CREATE TABLE metrics  (agent_id text, at bigint, record jsonb NOT NULL);
CREATE INDEX metrics_agent_at ON metrics (agent_id, at);
CREATE INDEX jobs_status ON jobs (status, queue);
CREATE INDEX jobs_order ON jobs (created_at DESC, id DESC);
CREATE INDEX commands_order ON commands (created_at DESC, id DESC);
```

```ts
import type { AgentRecord, DesiredState, JobFilter, JobRecord, Store } from "agent-sdk/server";
import type { Pool } from "pg";

export class PgStore implements Store {
  constructor(private readonly pg: Pool) {}

  async getAgent(id: string) {
    const r = await this.pg.query("SELECT record FROM agents WHERE id = $1", [id]);
    return r.rows[0]?.record as AgentRecord | undefined;
  }

  async updateAgent(agent: AgentRecord) {
    // условная запись: только если запись не менялась с чтения (тот же rev)
    const next = { ...agent, rev: agent.rev + 1 };
    const r = await this.pg.query("UPDATE agents SET record = $2, rev = $3 WHERE id = $1 AND rev = $4", [
      agent.id,
      next,
      next.rev,
      agent.rev,
    ]);
    if (r.rowCount !== 1) return false;
    agent.rev = next.rev;
    return true;
  }

  async updateJob(job: JobRecord) {
    // так же для задач: выдача queued → running, итог, истёкшая аренда — без двойной выдачи
    const next = { ...job, rev: job.rev + 1 };
    const r = await this.pg.query(
      `UPDATE jobs SET record = $2, rev = $3, status = $4, agent_id = $5, finished_at = $6
       WHERE id = $1 AND rev = $7`,
      [job.id, next, next.rev, job.status, job.agentId ?? null, job.finishedAt ?? null, job.rev],
    );
    if (r.rowCount !== 1) return false;
    job.rev = next.rev;
    return true;
  }

  async listJobs(f: JobFilter = {}) {
    const where: string[] = [];
    const args: unknown[] = [];
    const add = (sql: string, v: unknown) => where.push(sql.replace("?", `$${args.push(v)}`));
    if (f.status) add("status = ANY(?)", Array.isArray(f.status) ? f.status : [f.status]);
    if (f.queue) add("queue = ?", f.queue);
    if (f.agentId) add("agent_id = ?", f.agentId);
    // страница: записи после after в том же порядке (новые первыми)
    if (f.after) add("(created_at, id) < (SELECT created_at, id FROM jobs WHERE id = ?)", f.after);
    const limit = f.limit && f.limit > 0 ? `LIMIT ${Math.floor(f.limit)}` : "";
    const r = await this.pg.query(
      `SELECT record FROM jobs ${where.length ? "WHERE " + where.join(" AND ") : ""}
       ORDER BY created_at DESC, id DESC ${limit}`,
      args,
    );
    return r.rows.map((x) => x.record as JobRecord);
  }

  async setState(domain: string, agentId: string | undefined, spec: unknown, actor?: string) {
    const client = await this.pg.connect();
    try {
      await client.query("BEGIN");
      const now = Date.now();
      // версия только растёт: max(прежняя + 1, now) — одна на раздел
      const v = await client.query(
        `INSERT INTO state_versions (domain, last) VALUES ($1, $2)
         ON CONFLICT (domain) DO UPDATE SET last = GREATEST(state_versions.last + 1, $2)
         RETURNING last`,
        [domain, now],
      );
      const st: DesiredState = { domain, version: Number(v.rows[0].last), spec, updatedAt: now };
      if (agentId) st.agentId = agentId;
      if (actor) st.actor = actor;
      await client.query(
        `INSERT INTO states (domain, agent_id, version, record, created_at) VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (domain, agent_id) DO UPDATE SET version = $3, record = $4`,
        [domain, agentId ?? "", st.version, st, now],
      );
      await client.query("INSERT INTO state_history (domain, agent_id, version, record) VALUES ($1, $2, $3, $4)", [
        domain,
        agentId ?? "",
        st.version,
        st,
      ]);
      await client.query("COMMIT");
      return st;
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  }

  async deleteState(domain: string, agentId?: string) {
    // state_versions не трогаем: счётчик версий раздела не сбрасывается
    const r = await this.pg.query("DELETE FROM states WHERE domain = $1 AND agent_id = $2", [domain, agentId ?? ""]);
    return (r.rowCount ?? 0) > 0;
  }

  async listStateHistory(domain: string, agentId: string | undefined, limit: number) {
    // limit ≤ 0 — все хранимые
    const r = await this.pg.query(
      `SELECT record FROM state_history WHERE domain = $1 AND agent_id = $2 ORDER BY version DESC
       ${limit > 0 ? "LIMIT $3" : ""}`,
      limit > 0 ? [domain, agentId ?? "", limit] : [domain, agentId ?? ""],
    );
    return r.rows.map((x) => x.record as DesiredState);
  }

  async deleteAgent(id: string) {
    await this.pg.query("DELETE FROM metrics WHERE agent_id = $1", [id]);
    const r = await this.pg.query("DELETE FROM agents WHERE id = $1", [id]);
    return (r.rowCount ?? 0) > 0;
  }

  async prune(before: { jobsBefore?: number; commandsBefore?: number; eventsBefore?: number }) {
    let n = 0;
    if (before.jobsBefore)
      n +=
        (
          await this.pg.query(
            "DELETE FROM jobs WHERE status IN ('completed', 'failed', 'cancelled') AND finished_at < $1",
            [before.jobsBefore],
          )
        ).rowCount ?? 0;
    if (before.commandsBefore)
      n +=
        (
          await this.pg.query(
            "DELETE FROM commands WHERE status IN ('succeeded', 'failed', 'cancelled') AND finished_at < $1",
            [before.commandsBefore],
          )
        ).rowCount ?? 0;
    if (before.eventsBefore)
      n += (await this.pg.query("DELETE FROM events WHERE at < $1", [before.eventsBefore])).rowCount ?? 0;
    return n;
  }

  async pruneMetrics(before: number) {
    const r = await this.pg.query("DELETE FROM metrics WHERE at < $1", [before]);
    return r.rowCount ?? 0;
  }

  // … createAgent, listAgents, createJob, getJob, createCommand, getCommand, updateCommand
  // (как updateJob), listCommands (как listJobs), getState, listStates, addEvent, listEvents,
  // addMetrics, listMetrics
}
```

На Go и Python — те же запросы: Go — `json.Marshal` записи целиком (служебные поля с
`json:"-"` храните отдельными колонками или своей структурой), Python — `to_record()` /
`Model.from_record()`. Главное — `UPDATE … WHERE id = … AND rev = …` одним запросом и `false`,
если не затронута ни одна строка.

Если процессов бэкенда несколько, изменения нужно ещё и доставить агенту: процесс, изменивший
данные, сообщает остальным (например, Postgres `NOTIFY`), и каждый вызывает `refresh` —
[connection.md](connection.md#несколько-процессов-бэкенда).
