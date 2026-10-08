# События, уведомления о проблемах, аудит

Три потока, которые бэкенд получает от `Agents`: **события** воркеров («что-то случилось на
узле»), **уведомления о проблемах** (агент пропал, состояние не применилось, воркер упал) и
**журнал аудита** («кто что сделал через API»). Плюс уведомления об изменениях данных —
`change`. Справочник API — [sdk/README.md](../README.md), формат —
[sdk/spec §6.6](../spec/README.md#66-события-event), образцы —
[events.json](../spec/examples/events.json).

- [События воркеров](#события-воркеров)
- [Уведомления об изменениях](#уведомления-об-изменениях)
- [Уведомления о проблемах](#уведомления-о-проблемах)
- [Журнал аудита](#журнал-аудита)

## События воркеров

Событие — сообщение воркера серверу «что-то случилось», не связанное с задачей: переключили
маршрут, заполнился диск, закончилось резервное копирование. Доходит гарантированно. События
внутри задачи (итог этапа) — `job.event` ([jobs.md](jobs.md#прогресс-журнал-события-задачи)).

**Бэкенд.**

```ts
agents.on("event", (e) => console.log(e.agentName, e.source, e.type, e.data)); // AgentEvent
const recent = await agents.listEvents(100); // новые первыми
```

```go
events, err := agents.Events(100) // новые первыми
// уведомление — Options.OnChange: Change{Kind: server.ChangeEvent, ID: <id сообщения>}
```

```python
events = await agents.list_events(100)   # новые первыми
agents.on("change", lambda c: c.kind == "event" and refresh_events())
```

**Что уходит по сети.** На узле — `event {type, data?}`; серверу — `event {source, type, data?}`
(важное сообщение), `source` — имя воркера.

**Агент** (`internal/worker`) дописывает `source` и кладёт событие в `<dataDir>/outbox`: оно
дойдёт и после обрыва связи, и после перезапуска агента. Во время уборки (`agent cleanup`)
событий никто не ждёт — они отбрасываются.

**Воркер.**

```python
worker.event("backup.done", {"sizeBytes": 1_234_567})   # тип до 50 символов
```

```go
_ = w.Event("backup.done", map[string]int{"sizeBytes": 1234567})
```

```ts
worker.event("backup.done", { sizeBytes: 1234567 });
```

Событие можно отправить и до запуска воркера — оно уйдёт после регистрации
([workers.md](workers.md#здоровье)). Данные не в JSON — ошибка сразу у вызова; событие больше
16 МБ не отправляется, запись в лог воркера.

**Результат.** `AgentEvent {agentId, agentName, source, type, data, at}` в `Store`; повтор
сервер узнаёт по `id` сообщения и не сохраняет второй раз. Событие `change` `{kind: "event", id}`
с `id` сообщения, в Node — ещё `event` с готовым объектом. `listEvents` / `Events` /
`list_events` отдают последние события, новые первыми; `limit ≤ 0` — все.

## Уведомления об изменениях

`change` — «что изменилось», подробности читайте отдельно. Удобно для живого интерфейса и
для `refresh` между процессами.

```ts
agents.on("change", (c) => ui.invalidate(c.kind, c.id)); // { kind: "agent" | "job" | "command" | "state" | "event", id }
agents.on("job", (job) => ui.job(job)); // Node: готовые объекты — agent, job, command, state, stateApplied, event
```

```go
server.New(server.Options{EnrollToken: token, OnChange: func(c server.Change) { ui.Invalidate(c.Kind, c.ID) }})
```

```python
agents.on("change", lambda c: ui.invalidate(c.kind, c.id))   # функция или корутина; возвращает отписку
```

| `kind`    | `id`                 | Когда                                                                                                                            |
| --------- | -------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `agent`   | id агента            | регистрация, связь, статус изменился, новая точка метрик (не досланная), сведения об узле, возможности, отчёт о состоянии, отзыв |
| `job`     | id задачи            | постановка, выдача, подтверждение, прогресс, событие, итог, отмена                                                               |
| `command` | id команды           | создание, начало, вывод, итог                                                                                                    |
| `state`   | раздел               | снимок задан (общий или личный), удалён, откатан; отчёт агента о применении                                                      |
| `event`   | id сообщения события | новое событие воркера                                                                                                            |

Повторный одинаковый статус уведомления не вызывает.

## Уведомления о проблемах

Событие `alert` — проблема агента **началась** (`active: true`) или **закончилась**
(`active: false`). Каждое начало и конец приходят один раз. Отправлять письма или вебхуки —
дело бэкенда.

**Бэкенд.**

```ts
agents.on("alert", (a) => {
  // { type, agentId, agentName, active, message, at, domain?, worker? }
  if (a.active) notify(`${a.agentName}: ${a.type} — ${a.message}`);
  else resolve(a.type, a.agentId, a.domain ?? a.worker);
});
const active = await agents.alerts(); // активные сейчас
```

```go
agents := server.New(server.Options{EnrollToken: token, OnAlert: func(a server.Alert) { notify(a) }})
active, err := agents.Alerts()
```

```python
agents.on("alert", notify)   # Alert(type, agent_id, agent_name, active, message, at, domain, worker)
active = await agents.alerts()
```

| `type`           | Когда начинается                                                                                                                            | `message` при начале                                        | Когда заканчивается                        | Доп. поле |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- | ------------------------------------------ | --------- |
| `offline`        | агент пропал и не вернулся за `offlineGraceMs` (20 с); или от него нет вестей дольше `offlineAfterMs`, а процесс бэкенда с его сессией упал | «Агент без связи»                                           | агент снова на связи                       | —         |
| `stateFailed`    | `state.applied {ok: false}` по разделу                                                                                                      | ошибка из `state.applied`                                   | раздел применился (`ok: true`)             | `domain`  |
| `workerDown`     | воркер в `status.workers` в состоянии сбоя (`backoff` — перезапуск после падения)                                                           | «Воркер `<имя>`: `<состояние>`»                             | воркер снова работает или исчез из статуса | `worker`  |
| `workerDegraded` | воркер сам сообщил, что не в порядке (`worker.health`, `status.workers[].health = degraded`)                                                | сообщение воркера, без него — «Воркер `<имя>` не в порядке» | воркер снова в порядке или исчез           | `worker`  |
| `degraded`       | агент в `status.state = degraded` (воркеры перезапускаются, воркер не в порядке)                                                            | `status.message`, без него — «Агент не в порядке»           | агент снова в порядке                      | —         |

Тексты одинаковые во всех SDK. Уведомление о конце проблемы (`active: false`) несёт тот же
`message`, что было при её начале.

**Что уходит по сети** — ничего особого: уведомления сервер выводит из `status`,
`state.applied` и связи.

**Агент** выставляет `degraded` и состояния воркеров в `status` ([observe.md](observe.md#статус-агента)).

**Воркер** влияет через [здоровье](workers.md#здоровье) и [падения](workers.md#остановка-и-падение).

**Результат.** `alert` и `agents.alerts()`. Активные уведомления хранятся в записи агента в
`Store` ([store.md](store.md#служебные-поля)), поэтому `alerts()` в любом процессе бэкенда
отдаёт одно и то же, а начало и конец проблемы приходят событием `alert` один раз — в том
процессе, который их записал. Отозванный агент — все его уведомления заканчиваются.

## Журнал аудита

На каждое изменяющее действие API — с `by` и без — приходит запись аудита. SDK журнал не
хранит: бэкенд пишет его куда хочет.

**Бэкенд.** `by(actor)` — те же изменяющие методы, но с отметкой, кто это сделал:

```ts
await agents.by(user.login).setState("example.report", spec, { agentId });
await agents.by(user.login).enqueue({ queue: "example.render", data });
agents.on("audit", (e) => db.audit.insert(e)); // { at, actor, action, agentId?, target, details? }
```

```go
_, err := agents.By(user.Login).SetState("example.report", spec, agentID)
server.Options{OnAudit: func(e server.AuditEntry) { saveAudit(e) }}
```

```python
await agents.by(user.login).set_state("example.report", spec, agent_id=agent_id)
agents.on("audit", save_audit)
```

`by(actor)` есть у: `enqueue`, `cancelJob`, `stopJob`, `command`, `call`, `cancelCommand`,
`setState`, `deleteState`, `rollbackState`, `revoke`, `deleteAgent`, `updateAgent`, `updateWorker`, `rotateKey`,
`pauseWorker`, `resumeWorker` (в Go и Python — те же имена в стиле языка).

| `action`          | Метод             | `target`   | `details`                      |
| ----------------- | ----------------- | ---------- | ------------------------------ |
| `job.enqueue`     | `enqueue`         | id задачи  | `{queue}`                      |
| `job.cancel`      | `cancelJob`       | id задачи  | —                              |
| `job.stop`        | `stopJob`         | id задачи  | —                              |
| `command`         | `command`, `call` | id команды | `{name}`                       |
| `command.cancel`  | `cancelCommand`   | id команды | —                              |
| `state.set`       | `setState`        | раздел     | `{version}`                    |
| `state.delete`    | `deleteState`     | раздел     | —                              |
| `state.rollback`  | `rollbackState`   | раздел     | `{fromVersion, version}`       |
| `agent.revoke`    | `revoke`          | id агента  | —                              |
| `agent.delete`    | `deleteAgent`     | id агента  | —                              |
| `agent.update`    | `updateAgent`     | id агента  | `{commandId, version}`         |
| `agent.rotateKey` | `rotateKey`       | id агента  | `{commandId}`                  |
| `worker.update`   | `updateWorker`    | id агента  | `{worker, version, commandId}` |
| `worker.pause`    | `pauseWorker`     | id агента  | `{worker, queues?, commandId}` |
| `worker.resume`   | `resumeWorker`    | id агента  | `{worker, queues?, commandId}` |

`agentId` в записи — агент, к которому относится действие (если есть).

**Что уходит по сети.** Ничего: аудит — только в бэкенде.

**Агент** и **воркер** не участвуют.

**Результат.** `actor` попадает в `Job.actor`, `Command.actor`, `DesiredState.actor` и
`AuditEntry.actor`; без `by` — пусто.
