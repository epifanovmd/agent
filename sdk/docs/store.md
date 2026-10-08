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
- [MemoryStore](#memorystore)
- [Пример на SQL (Postgres)](#пример-на-sql-postgres)

## Что хранит Store

| Что               | Модель                                                      | Кто пишет                                                                             |
| ----------------- | ----------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| агенты            | `Agent` (+ служебные поля)                                  | регистрация, каждое сообщение агента (`lastSeenAt`, статус, метрики), подписки, отзыв |
| задачи            | `Job` (+ `leaseUntil`, `eventSeq`)                          | постановка, выдача, прогресс, события, итог, сроки                                    |
| команды           | `Command`                                                   | создание, начало, вывод, итог, срок                                                   |
| снимки состояния  | `DesiredState` по ключу (раздел, агент; общий — без агента) | `setState`, `deleteState`, `rollbackState`                                            |
| история состояния | `DesiredState`                                              | каждый `setState` (и откат, и переиздание общего)                                     |
| события           | `AgentEvent`                                                | сообщения `event` воркеров                                                            |
| история метрик    | `MetricsPoint` по агенту                                    | точки `metrics` (с прореживанием), чистка по сроку                                    |

Аудит, логи и уведомления о проблемах `Agents` **не** хранит — они приходят событиями
([events.md](events.md), [observe.md](observe.md#лог-агента-и-воркеров)).

## Методы по языкам

В Go методы обычные и возвращают `error` (нет записи — `server.ErrNotFound`); в Node и Python —
асинхронные (нет записи — `undefined` / `None`). Задачи и команды — новые первыми (в SQL —
`ORDER BY created_at DESC`): `Agents` отдаёт их наружу как есть, а раздаёт задачи и доставляет
команды агенту старыми первыми. Остальные списки — в порядке создания, если не сказано иное.

| Что                  | Go (`server.Store`)                                                                                    | Node (`Store`)                                                                             | Python (`Store`)                                                                 |
| -------------------- | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------- |
| создать агента       | `CreateAgent(a *Agent) error`                                                                          | `createAgent(agent: AgentRecord): Promise<void>`                                           | `create_agent(agent: Agent) -> None`                                             |
| прочитать агента     | `GetAgent(id string) (*Agent, error)`                                                                  | `getAgent(id): Promise<AgentRecord \| undefined>`                                          | `get_agent(agent_id) -> Agent \| None`                                           |
| записать агента      | `UpdateAgent(a *Agent) error`                                                                          | `updateAgent(agent: AgentRecord): Promise<void>`                                           | `update_agent(agent: Agent) -> None`                                             |
| все агенты           | `ListAgents() ([]*Agent, error)`                                                                       | `listAgents(): Promise<AgentRecord[]>`                                                     | `list_agents() -> List[Agent]`                                                   |
| создать задачу       | `CreateJob(j *Job) error`                                                                              | `createJob(job: JobRecord)`                                                                | `create_job(job: Job)`                                                           |
| прочитать задачу     | `GetJob(id) (*Job, error)`                                                                             | `getJob(id): Promise<JobRecord \| undefined>`                                              | `get_job(job_id) -> Job \| None`                                                 |
| записать задачу      | `UpdateJob(j *Job) error`                                                                              | `updateJob(job: JobRecord)`                                                                | `update_job(job: Job)`                                                           |
| задачи               | `ListJobs(f JobFilter) ([]*Job, error)` — `{Status, Queue, AgentID}`, новые первыми                    | `listJobs(filter?: {status?, queue?, agentId?})` — `status` может быть списком             | `list_jobs(*, status=None, queue=None, agent_id=None)`                           |
| создать команду      | `CreateCommand(c *Command) error`                                                                      | `createCommand(cmd: Command)`                                                              | `create_command(command: Command)`                                               |
| прочитать команду    | `GetCommand(id) (*Command, error)`                                                                     | `getCommand(id): Promise<Command \| undefined>`                                            | `get_command(command_id) -> Command \| None`                                     |
| записать команду     | `UpdateCommand(c *Command) error`                                                                      | `updateCommand(cmd: Command)`                                                              | `update_command(command: Command)`                                               |
| команды              | `ListCommands(f CommandFilter)` — `{Status, AgentID}`, новые первыми                                   | `listCommands(filter?: {status?, agentId?})`                                               | `list_commands(*, status=None, agent_id=None)`                                   |
| новый снимок         | `SetState(domain, agentID string, spec json.RawMessage, actor string) (*DesiredState, error)`          | `setState(domain, agentId: string \| undefined, spec, actor?): Promise<DesiredState>`      | `set_state(domain, agent_id, spec, *, actor=None) -> DesiredState`               |
| прочитать снимок     | `GetState(domain, agentID) (*DesiredState, error)`                                                     | `getState(domain, agentId?)`                                                               | `get_state(domain, agent_id) -> DesiredState \| None`                            |
| удалить снимок       | `DeleteState(domain, agentID) (bool, error)` — был ли                                                  | `deleteState(domain, agentId?): Promise<boolean>`                                          | `delete_state(domain, agent_id) -> bool`                                         |
| все снимки           | `ListStates() ([]*DesiredState, error)`                                                                | `listStates()`                                                                             | `list_states()`                                                                  |
| история раздела      | `ListStateHistory(domain, agentID, limit) ([]*DesiredState, error)` — новые первыми, `limit ≤ 0` — все | `listStateHistory(domain, agentId \| undefined, limit)` — новые первыми, `limit ≤ 0` — все | `list_state_history(domain, agent_id, limit)` — новые первыми, `limit ≤ 0` — все |
| событие              | `AddEvent(e AgentEvent) error`                                                                         | `addEvent(event: AgentEvent)`                                                              | `add_event(event: AgentEvent)`                                                   |
| события              | `ListEvents(limit) ([]AgentEvent, error)` — последние, новые первыми; `limit ≤ 0` — все                | `listEvents(limit?)` — последние, новые первыми; `limit ≤ 0` — все                         | `list_events(limit=100)` — последние, новые первыми; `limit ≤ 0` — все           |
| точка метрик         | `AddMetrics(agentID string, p MetricsPoint) error`                                                     | `addMetrics(agentId, point: MetricsPoint)`                                                 | `add_metrics(agent_id, point: MetricsPoint)`                                     |
| история метрик       | `ListMetrics(agentID string, since int64) ([]MetricsPoint, error)`                                     | `listMetrics(agentId, since?): Promise<MetricsPoint[]>`                                    | `list_metrics(agent_id, since=None)`                                             |
| удалить старые точки | `PruneMetrics(before int64) (int, error)` — сколько удалено                                            | `pruneMetrics(before): Promise<number>`                                                    | `prune_metrics(before) -> int`                                                   |

`ListMetrics` — точки строго позже `since` по возрастанию `at`; `PruneMetrics` — удалить точки
**всех** агентов с `at < before`.

## Служебные поля

В записях есть поля, которые наружу (в JSON для интерфейса) не отдаются, но **хранить их
нужно**:

| Запись | Go (`json:"-"`)                                                 | Node (`AgentRecord`, `JobRecord`)                                       | Python (`to_record()` / `from_record()`)                                      |
| ------ | --------------------------------------------------------------- | ----------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| агент  | `SecretHash`, `PendingSecretHash`, `GrantedLabels`, `MetricsAt` | `secretHash`, `pendingSecretHash`, `grantedLabels`, `bootId`, `lastSeq` | `secret_hash`, `pending_secret_hash`, `granted_labels`, `boot_id`, `last_seq` |
| задача | `LeaseUntil`, `EventSeq`                                        | `leaseUntil`, `eventSeq`                                                | `lease_until`, `event_seq`                                                    |

- `secretHash` — sha256 ключа агента; без него агент не войдёт. `pendingSecretHash` — ключ после
  смены, пока агент не вошёл с ним ([connection.md](connection.md#смена-ключа));
- `grantedLabels` — метки, выданные при регистрации: агент не может их переписать;
- `leaseUntil` — срок попытки задачи; `eventSeq` — последний принятый номер события задачи.

В Python `to_dict()` — JSON для интерфейса без служебных полей; `to_record()` — всё для БД,
`Model.from_record(dict)` — обратно. В Node — `publicAgent(record)` / `publicJob(record)` делает
`Agents` сам, `Store` получает и возвращает записи целиком.

## Правила

1. **Версия состояния только растёт** — в пределах раздела, общая для общего и личных снимков,
   и между перезапусками бэкенда: `версия = max(наибольшая выданная в разделе + 1, текущее время
в мс)`. `deleteState` счётчик не сбрасывает. Агент пропускает снимок, если он не новее
   применённого, — поэтому «откат» версии на сервере сломал бы доставку.
2. **История**: каждый `setState` — запись в историю раздела (общего или агента);
   `deleteState` историю не стирает. Сколько хранить — решаете вы (`MemoryStore` — 50 на
   раздел и агента). `rollbackState` ищет версию в истории.
3. **Запись агента целиком.** `updateAgent` получает запись целиком. Если процессов бэкенда
   несколько, обновляйте её **в транзакции** или только изменившимися полями: иначе два
   процесса затрут изменения друг друга — например, отзыв (`revoked`), подписки или новый
   ключ.
4. **Копии.** `get*` возвращает копию, `Agents` меняет её и сохраняет `update*`. Не отдавайте
   из `Store` объекты, которые потом меняете сами.
5. **Порядок.** В одном процессе `Agents` вызывает `Store` по одному (изменения выполняются
   последовательно), поэтому транзакции между вызовами в одном процессе не нужны.
6. **Метрики**: точки досланных метрик (`backfill`) приходят с `at` раньше уже сохранённых —
   вставляйте по `at`; `pruneMetrics` вызывается при запуске и раз в час
   (`metricsRetentionMs`).
7. **Чистка** завершённых задач, команд и событий — на вашей стороне: `Agents` их не удаляет.

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

Задачи в очереди и в работе и незавершённые команды не удаляются. После перезапуска бэкенда
всё пропадает; версии состояния остаются растущими, потому что начинаются не меньше текущего
времени.

## Пример на SQL (Postgres)

Схематично: записи — в `jsonb`, ключевые поля — отдельными колонками для фильтров.

```sql
CREATE TABLE agents   (id text PRIMARY KEY, created_at bigint NOT NULL, record jsonb NOT NULL);
CREATE TABLE jobs     (id text PRIMARY KEY, status text NOT NULL, queue text NOT NULL, agent_id text,
                       created_at bigint NOT NULL, record jsonb NOT NULL);
CREATE TABLE commands (id text PRIMARY KEY, status text NOT NULL, agent_id text NOT NULL,
                       created_at bigint NOT NULL, record jsonb NOT NULL);
CREATE TABLE state_versions (domain text PRIMARY KEY, last bigint NOT NULL);
CREATE TABLE states   (domain text, agent_id text NOT NULL DEFAULT '', version bigint NOT NULL,
                       record jsonb NOT NULL, created_at bigint NOT NULL, PRIMARY KEY (domain, agent_id));
CREATE TABLE state_history (domain text, agent_id text NOT NULL DEFAULT '', version bigint,
                       record jsonb NOT NULL, PRIMARY KEY (domain, agent_id, version));
CREATE TABLE events   (seq bigserial PRIMARY KEY, record jsonb NOT NULL);
CREATE TABLE metrics  (agent_id text, at bigint, record jsonb NOT NULL);
CREATE INDEX metrics_agent_at ON metrics (agent_id, at);
CREATE INDEX jobs_status ON jobs (status, queue);
```

```ts
import type { AgentRecord, DesiredState, Store } from "agent-sdk/server";
import type { Pool } from "pg";

export class PgStore implements Store {
  constructor(private readonly pg: Pool) {}

  async getAgent(id: string) {
    const r = await this.pg.query("SELECT record FROM agents WHERE id = $1", [id]);
    return r.rows[0]?.record as AgentRecord | undefined;
  }

  async updateAgent(agent: AgentRecord) {
    // запись целиком; при нескольких процессах — в транзакции с SELECT … FOR UPDATE
    await this.pg.query("UPDATE agents SET record = $2 WHERE id = $1", [agent.id, agent]);
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
    const r = await this.pg.query(
      "SELECT record FROM state_history WHERE domain = $1 AND agent_id = $2 ORDER BY version DESC LIMIT $3",
      [domain, agentId ?? "", limit],
    );
    return r.rows.map((x) => x.record as DesiredState);
  }

  async pruneMetrics(before: number) {
    const r = await this.pg.query("DELETE FROM metrics WHERE at < $1", [before]);
    return r.rowCount ?? 0;
  }

  // … createAgent, listAgents, createJob, getJob, updateJob, listJobs, createCommand, getCommand,
  // updateCommand, listCommands, getState, listStates, addEvent, listEvents, addMetrics, listMetrics
}
```

На Go и Python — те же запросы: Go — `json.Marshal` записи целиком (служебные поля с
`json:"-"` храните отдельными колонками или своей структурой), Python — `to_record()` /
`Model.from_record()`.

Если процессов бэкенда несколько, изменения нужно ещё и доставить агенту: процесс, изменивший
данные, сообщает остальным (например, Postgres `NOTIFY`), и каждый вызывает `refresh` —
[connection.md](connection.md#несколько-процессов-бэкенда).
