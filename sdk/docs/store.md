# Хранилище

`Agents` хранит в Store две вещи: записи агентов и настройки воркеров. По умолчанию —
`MemoryStore` в памяти процесса (разработка, тесты, один процесс). Для настоящего бэкенда
напишите свой Store поверх своей БД — тогда агенты и настройки переживают перезапуск, а
несколько процессов работают с общими данными
([connection.md](connection.md#несколько-процессов-бэкенда)).

```ts
import { Agents, MemoryStore } from "agent-sdk/server";

const agents = new Agents({ enrollToken, store: new PgStore(pool) }); // свой
const dev = new Agents({ enrollToken, store: new MemoryStore() });
```

## Что хранит Store

| Что       | Запись                                                                                                                                                                         | Кто пишет                                              |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------ |
| агенты    | `AgentRecord`: имя, метки, хеш ключа и ожидающий новый ключ, отзыв, учёт потока, последние `hello` и `status`, последняя точка метрик, итоги настроек, текущие проблемы, `rev` | регистрация, каждое сообщение потока, действия бэкенда |
| настройки | `ConfigRecord`: агент, воркер, ключ, версия, значение, время, автор                                                                                                            | `setConfig`, `deleteConfig`                            |

Истории в Store нет. Метрики, события воркеров, журнал, проблемы, итоги действий и аудит
приходят событиями `Agents` (и обработчиком `onEvent`) — сохраните сами то, что нужно
([observe.md](observe.md)). Зрители `watch` и ожидание итогов действий живут в памяти процесса.

## Методы

```ts
interface Store {
  createAgent(agent: AgentRecord): Promise<void>;
  getAgent(id: string): Promise<AgentRecord | undefined>;
  listAgents(): Promise<AgentRecord[]>;
  updateAgent(agent: AgentRecord): Promise<boolean>; // условная запись по rev
  deleteAgent(id: string): Promise<boolean>; // и его настройки

  setConfig(agentId, worker, key, data, opts?: { actor?; minVersion? }): Promise<ConfigRecord>;
  listConfigs(agentId): Promise<ConfigRecord[]>; // без удалённых
  deleteConfig(agentId, worker, key): Promise<boolean>;
}
```

## Правила

Методы вызываются одновременно, в том числе из нескольких процессов. Чтобы ничего не
потерялось, Store обязан выполнять два правила.

1. **Условная запись агента.** `updateAgent` пишет запись, только если `rev` в хранилище такой
   же, как у переданной, и сохраняет её с `rev + 1` (переданной записи тоже ставит `rev + 1`).
   Иначе — `false`: `Agents` перечитает запись и повторит изменение. Запись хранится целиком,
   одним JSON: незнакомые поля — как есть.
2. **Версия настроек только растёт.** `setConfig` даёт версию `max(счётчик + 1, minVersion)`;
   счётчик ключа хранится и после `deleteConfig`. Две записи одновременно получают разные
   версии.

Остальное — обычное чтение и запись. `listAgents` вызывается раз в секунду (проверка агентов без
вестей) — в большой базе держите для него лёгкий запрос.

## MemoryStore

`new MemoryStore()` — записи копируются на входе и выходе, как у настоящей БД. После
перезапуска процесса всё пропадает — агенты с ключами тоже, им придётся регистрироваться
заново. Один `MemoryStore` можно отдать нескольким `Agents` в одном процессе — так устроены
тесты нескольких процессов.

## Пример на SQL (Postgres)

Запись агента — `jsonb` с отдельной колонкой `rev` для условной записи. Настройки — строка на
ключ: после удаления остаются `version` (счётчик) и `data = NULL`.

```sql
CREATE TABLE agents  (id text PRIMARY KEY, rev bigint NOT NULL, record jsonb NOT NULL);
CREATE TABLE configs (agent_id text, worker text, key text, version bigint NOT NULL, data jsonb,
                      updated_at bigint, actor text, PRIMARY KEY (agent_id, worker, key));
```

```ts
import type { AgentRecord, ConfigRecord, SetConfigOptions, Store } from "agent-sdk/server";
import type { Pool } from "pg";

const config = (r: any): ConfigRecord => ({
  agentId: r.agent_id,
  worker: r.worker,
  key: r.key,
  version: Number(r.version),
  data: r.data,
  updatedAt: Number(r.updated_at),
  ...(r.actor ? { actor: r.actor } : {}),
});

export class PgStore implements Store {
  private readonly pg: Pool;

  constructor(pg: Pool) {
    this.pg = pg;
  }

  async createAgent(a: AgentRecord) {
    await this.pg.query("INSERT INTO agents VALUES ($1, $2, $3)", [a.id, a.rev, a]);
  }
  async getAgent(id: string) {
    return (await this.pg.query("SELECT record FROM agents WHERE id = $1", [id])).rows[0]?.record;
  }
  async listAgents() {
    return (await this.pg.query("SELECT record FROM agents")).rows.map((r) => r.record);
  }
  async updateAgent(a: AgentRecord) {
    const next = { ...a, rev: a.rev + 1 };
    const r = await this.pg.query("UPDATE agents SET rev = $2, record = $3 WHERE id = $1 AND rev = $4", [
      a.id,
      next.rev,
      next,
      a.rev,
    ]);
    if (r.rowCount === 1) a.rev = next.rev;
    return r.rowCount === 1;
  }
  async deleteAgent(id: string) {
    await this.pg.query("DELETE FROM configs WHERE agent_id = $1", [id]);
    return (await this.pg.query("DELETE FROM agents WHERE id = $1", [id])).rowCount === 1;
  }
  async setConfig(agentId: string, worker: string, key: string, data: unknown, o: SetConfigOptions = {}) {
    const r = await this.pg.query(
      `INSERT INTO configs VALUES ($1, $2, $3, GREATEST(1, $4), $5, $6, $7)
       ON CONFLICT (agent_id, worker, key) DO UPDATE SET version = GREATEST(configs.version + 1, $4),
         data = $5, updated_at = $6, actor = $7
       RETURNING *`,
      [agentId, worker, key, o.minVersion ?? 0, JSON.stringify(data), Date.now(), o.actor ?? null],
    );
    return config(r.rows[0]);
  }
  async listConfigs(agentId: string) {
    const r = await this.pg.query("SELECT * FROM configs WHERE agent_id = $1 AND data IS NOT NULL", [agentId]);
    return r.rows.map(config);
  }
  async deleteConfig(agentId: string, worker: string, key: string) {
    const r = await this.pg.query(
      "UPDATE configs SET data = NULL WHERE agent_id = $1 AND worker = $2 AND key = $3 AND data IS NOT NULL",
      [agentId, worker, key],
    );
    return r.rowCount === 1;
  }
}
```

`data` хранится как JSON-текст в `jsonb`, поэтому значение `null` — это `'null'::jsonb`, а не
SQL `NULL`, и удалённым не считается. Запись агента обновляется на каждое сообщение потока
(`status`, `metrics`, `log`): по первичному ключу `UPDATE … WHERE id = … AND rev = …` — быстрый
запрос.
