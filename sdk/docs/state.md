# Состояние

Состояние — то, как всё должно быть на узле: «такие пользователи», «такие настройки». Оно
делится на **разделы** (`domain`, например `example.report`). Бэкенд задаёт снимок раздела
целиком, агент хранит его на диске и отдаёт воркеру, воркер приводит узел к снимку. Узел придёт
к нему и после выключения, и после ручных правок. Справочник API — [sdk/README.md](../README.md),
формат — [sdk/spec §6.5](../spec/README.md#65-состояние-capabilitiesstate), образцы —
[state.json](../spec/examples/state.json).

- [Общий снимок](#общий-снимок)
- [Личный снимок агента](#личный-снимок-агента)
- [Версии](#версии)
- [Удалить снимок](#удалить-снимок)
- [История и откат](#история-и-откат)
- [Как агент применяет снимок](#как-агент-применяет-снимок)
- [Повторное применение](#повторное-применение)
- [Секреты в снимке](#секреты-в-снимке)
- [Ошибка с отчётом](#ошибка-с-отчётом)

## Общий снимок

**Бэкенд.**

```ts
const st = await agents.setState("example.report", { header: "ACME", templates: ["daily", "weekly"] });
// → DesiredState { domain, version, spec, updatedAt, actor? }
```

```go
st, err := agents.SetState("example.report", map[string]any{"header": "ACME"}, "") // "" — общий
```

```python
st = await agents.set_state("example.report", {"header": "ACME"})
```

Снимок всегда **целиком**: он заменяет прежний, а не склеивается с ним. Раздел ещё никто не
объявлял (опечатка, воркер не подключался) — снимок всё равно сохраняется, в лог пишется
предупреждение; агентам он уйдёт, когда воркер объявит раздел. Ошибки: `MESSAGE_INVALID` (имя
раздела не по правилу). От имени пользователя — `agents.by("ivan").setState(…)`, аудит
`state.set`.

**Что уходит по сети.** `state.put {domain, version, spec}` каждому агенту на связи, который
объявил раздел и у которого версия старше; агенту без связи — после подключения. Ответ —
`state.applied {domain, version, ok, error?, report?}` (важное сообщение).

**Агент** — [как применяет](#как-агент-применяет-снимок).

**Воркер** объявляет раздел и применяет снимок:

```python
@worker.state("example.report")
def apply(version: int, spec: dict) -> dict:
    write_config(spec)                      # повторное применение того же снимка — без вреда
    return {"templates": len(spec["templates"])}   # отчёт — как хотите
```

```go
w.State("example.report", func(ctx context.Context, version int64, spec json.RawMessage) (any, error) {
	var cfg struct{ Header string `json:"header"` }
	if err := json.Unmarshal(spec, &cfg); err != nil {
		return nil, err
	}
	return map[string]string{"header": cfg.Header}, writeConfig(cfg)
})
```

```ts
worker.state("example.report", async (version, spec) => {
  await writeConfig(spec);
  return { templates: spec.templates.length };
});
```

Ошибка или исключение — «не применилось» с текстом ошибки; агент повторит.

**Результат.** `DesiredState` в `Store` и событие `change` `{kind: "state", id: domain}` (в Node
— ещё `state`). После применения — `agent.stateApplied[domain] = {domain, version, ok, error?,
report?}` и события `change` `{kind: "agent"}` и `{kind: "state", id: domain}`; в Node — ещё
`stateApplied` (с `agentId`).

## Личный снимок агента

**Бэкенд.**

```ts
await agents.setState("example.report", { header: "ACME EU" }, { agentId });
```

```go
st, err := agents.SetState("example.report", map[string]any{"header": "ACME EU"}, agentID)
```

```python
await agents.set_state("example.report", {"header": "ACME EU"}, agent_id=agent_id)
```

Пока у агента есть личный снимок, он получает только его, а изменения общего до него не
доходят. Неизвестный агент — `AGENT_NOT_FOUND`.

**Что уходит по сети** — `state.put` только этому агенту. **Агент** и **воркер** не отличают
личный снимок от общего.

**Результат.** `DesiredState` с `agentId`; `change` — `{kind: "state", id: domain}`, как у общего.

## Версии

**Бэкенд** версии не задаёт: их выдаёт `Store`. Номер версии раздела **только растёт** — общий
и личные снимки одного раздела делят один счётчик, и он растёт даже после перезапуска сервера:
`версия = max(прежняя + 1, текущее время в мс)`. Для своего `Store` это главное правило
([store.md](store.md#правила)).

**Что уходит по сети.** `state.put.version`. Сервер шлёт снимок, только если версия новее той,
что агент применил: по `hello.capabilities.state.domains` (раздел → применённая версия) и
последнему успешному `state.applied`.

**Агент** помнит применённую версию на диске и пропускает снимок, если он не новее. Тот же
снимок, уже применённый, — снова отвечает `state.applied ok` (подтверждение могло потеряться).
Пришло несколько снимков подряд — применит только последний.

**Воркер** получает `version` и может её сохранить.

**Результат.** `DesiredState.version`, `agent.stateApplied[domain].version`. Сравнивайте их,
чтобы показать «применено / ждёт / ошибка».

## Удалить снимок

**Бэкенд.**

```ts
await agents.deleteState("example.report", { agentId }); // личный: агент вернётся на общий
await agents.deleteState("example.report"); // общий
```

```go
shared, err := agents.DeleteState("example.report", agentID)
_, err = agents.DeleteState("example.report", "")
```

```python
shared = await agents.delete_state("example.report", agent_id=agent_id)
await agents.delete_state("example.report")
```

- **Личный** удалён, а общий есть — общий сохраняется заново **с новой версией** и
  доставляется: иначе агент, уже применивший личный снимок с большей версией, общий не примет.
  Остальные агенты получат тот же общий ещё раз — это безопасно. Метод возвращает этот общий
  снимок. Общего нет — `null` (`nil`, `None`), у агента остаётся последнее применённое.
- Личного и не было — ничего не меняется и не отправляется; метод возвращает текущий общий.
- **Общий** удалён — агентам ничего не отправляется (у них остаётся последнее применённое),
  личные снимки не трогаются; метод возвращает `null`.

Аудит `state.delete`. Историю удаление не стирает, счётчик версий не сбрасывает.

**Что уходит по сети.** При удалении личного — `state.put` общего с новой версией.

**Агент** и **воркер.** Удаления как такового на узле нет: воркер получает снимок, который
теперь действует. Чтобы «убрать всё», задайте пустой снимок (`{}`).

**Результат.** Событие `stateDeleted` (`domain`, `agentId`; без `agentId` — общий) — раньше
`change` этой же операции: Node — `on("stateDeleted", ({ domain, agentId }) => …)`, Go —
`Options.OnStateDeleted(domain, agentID)`, Python — `on("stateDeleted", fn(domain, agent_id))`.

## История и откат

**Бэкенд.**

```ts
const history = await agents.stateHistory("example.report", { agentId, limit: 20 }); // новые первыми
await agents.rollbackState("example.report", history[3].version, { agentId });
```

```go
history, err := agents.StateHistory("example.report", "", 20)
st, err := agents.RollbackState("example.report", history[3].Version, "")
```

```python
history = await agents.state_history("example.report", agent_id=None, limit=20)
st = await agents.rollback_state("example.report", history[3].version)
```

Каждый снимок раздела попадает в историю (`MemoryStore` — последние 50 на раздел и агента).
Откат берёт содержимое старой версии и задаёт его снова — **под новой версией**: агент применит
его как обычное изменение. Нет такой версии в истории — `STATE_VERSION_NOT_FOUND`. Аудит
`state.rollback` с `details: {fromVersion, version}`.

**Что уходит по сети, агент, воркер** — как у [общего снимка](#общий-снимок).

**Результат** — новый `DesiredState` с содержимым старой версии.

## Как агент применяет снимок

**Агент** (`internal/state`, `internal/worker`):

1. получает `state.put`, пропускает его, если версия не новее последней;
2. сохраняет снимок в `<dataDir>/state/<domain>.json` (как пришёл, с запечатанными секретами);
3. раскрывает секреты в памяти и передаёт воркеру — владельцу раздела;
4. ждёт ответа; следующий снимок раздела воркер получит только после ответа на предыдущий —
   один раздел никогда не применяется дважды одновременно;
5. отправляет `state.applied` через очередь на диске.

Ещё:

- **ошибка** — повтор через 30 с; серверу итог уходит, только если он изменился;
- **после перезапуска агента** — снимок с диска применяется сразу, не дожидаясь сервера;
- **новый запуск воркера** (перезапуск, замена, обновление) — получает последний снимок заново,
  поэтому сервер может получить `state.applied` одной версии повторно;
- воркер раздела не запущен — «не применилось», повтор.

**Воркер.** Применение должно быть **идемпотентным**: тот же снимок может прийти несколько раз.
Разные разделы применяются параллельно с задачами и командами.

**Результат.** `agent.stateApplied[domain]`; не применилось — уведомление `alert` `stateFailed`
(`domain`, текст ошибки), применилось — его конец ([events.md](events.md#уведомления-о-проблемах)).

## Повторное применение

Раз в `state.resyncInterval` (настройка агента, по умолчанию 10 минут; `0` — выключено) агент
заново отдаёт воркеру последний применённый снимок каждого раздела. Так воркер исправляет то, что
на узле поменяли руками, даже если сервер ничего нового не присылал.

**Бэкенд** ничего не делает.

**Что уходит по сети.** `state.applied` той же версии — только если итог изменился (например,
был `ok`, стал ошибкой).

**Агент** пропускает раздел, если последний снимок ещё не применён или воркер-владелец не
запущен. Интервал меняется на ходу (`systemctl reload agent`).

**Воркер** получает тот же `version` и `spec` — должен привести узел к снимку снова.

**Результат.** Обычно — ничего нового; если ручная правка сломала применение — `stateFailed`.

## Секреты в снимке

Любое значение в снимке можно **запечатать** так, что раскрыть его может только один агент.
В БД и на диске узла значение лежит зашифрованным, агент раскрывает его только в памяти, перед
передачей воркеру.

**Бэкенд.**

```ts
const password = await agents.seal(agentId, "s3cr3t"); // { $sealed: "v1.…" }
await agents.setState("example.report", { smtp: { user: "report", password } }, { agentId });
```

```go
password, err := agents.Seal(agentID, "s3cr3t") // json.RawMessage
st, err := agents.SetState("example.report", map[string]any{"smtp": map[string]any{"password": password}}, agentID)
```

```python
password = await agents.seal(agent_id, "s3cr3t")   # нужен agent-sdk[crypto]
await agents.set_state("example.report", {"smtp": {"password": password}}, agent_id=agent_id)
```

Запечатанное значение годится только для своего агента — поэтому секреты кладут в **личный**
снимок. Ошибки: `AGENT_NOT_FOUND`, `SEAL_NOT_AVAILABLE` (агент ещё не подключался и не сообщил
свой ключ; в Python — ещё нет пакета `cryptography`).

**Что уходит по сети.** `{"$sealed": "v1.<eph>.<nonce>.<ct>"}` на месте значения: X25519 +
HKDF-SHA256 + AES-256-GCM; открытый ключ агента — `hello.agent.encryptionKey`
([§6.5](../spec/README.md#65-состояние-capabilitiesstate)).

**Агент** создаёт ключ `<dataDir>/encryption.key` при первом запуске, раскрывает `$sealed`
перед передачей воркеру. Не раскрылось (агента поставили заново — ключ сменился, данные
повреждены) — `state.applied {ok: false}` с ошибкой «секрет не расшифрован», воркер снимок не
получает.

**Воркер** получает уже раскрытое значение: `spec["smtp"]["password"] == "s3cr3t"`.

**Результат.** В `Store` и `DesiredState.spec` — запечатанное значение. После переустановки
агента снимок нужно запечатать заново.

## Ошибка с отчётом

Если снимок применился не целиком, воркер может приложить отчёт о том, что получилось.

**Бэкенд** читает `agent.stateApplied[domain].report` вместе с `error`.

**Что уходит по сети.** `state.applied {ok: false, error, report}`.

**Агент** передаёт отчёт серверу как есть и повторяет применение через 30 с.

**Воркер.**

```python
from agent_sdk.worker import StateFailed

@worker.state("example.report")
def apply(version: int, spec: dict) -> dict:
    done, failed = apply_templates(spec["templates"])
    if failed:
        raise StateFailed(f"не применились: {', '.join(failed)}", report={"applied": done, "failed": failed})
    return {"applied": done}
```

```go
if len(failed) > 0 {
	return nil, worker.StateFailed("не применились: "+strings.Join(failed, ", "),
		map[string]any{"applied": done, "failed": failed})
}
```

```ts
import { StateError } from "agent-sdk/worker";
throw new StateError(`не применились: ${failed.join(", ")}`, { applied: done, failed });
```

**Результат.** `agent.stateApplied[domain] = {ok: false, error: "не применились: …", report:
{applied, failed}}`, уведомление `stateFailed`.
