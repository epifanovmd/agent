# Команды

Команда — действие «сделай сейчас» на конкретном узле, с ответом. В отличие от задачи она не
повторяется на другом агенте: если узел не ответил в срок, команда завершается ошибкой
`TIMEOUT`. Справочник API — [sdk/README.md](../README.md), формат —
[sdk/spec §6.4](../spec/README.md#64-команды-capabilitiescommands), образцы —
[commands.json](../spec/examples/commands.json).

- [Отправить команду](#отправить-команду)
- [Команда с ожиданием итога](#команда-с-ожиданием-итога)
- [Вывод по мере работы](#вывод-по-мере-работы)
- [Срок команды](#срок-команды)
- [Отменить команду](#отменить-команду)
- [Ошибки](#ошибки)
- [Встроенные команды агента](#встроенные-команды-агента)
- [Прочитать команды](#прочитать-команды)

## Отправить команду

**Бэкенд.**

```ts
const cmd = await agents.command({
  name: "example.report.reload", // обязательно; имя — по правилу имён
  args: { force: true }, // любые данные JSON
  timeoutSec: 30, // срок (60)
  agentId, // кому; без него — агенту на связи, который объявил команду
});
```

```go
cmd, err := agents.Command(server.CommandRequest{
	Name: "example.report.reload", Args: map[string]bool{"force": true}, TimeoutSec: 30, AgentID: agentID,
})
```

```python
cmd = await agents.command("example.report.reload", {"force": True}, timeout_sec=30, agent_id=agent_id)
```

Без `agentId` команда уходит агенту на связи, который её объявил; если такого нет — любому
объявившему (она дождётся его подключения или срока). Никто не объявлял — ошибка
`COMMAND_NOT_SUPPORTED`. Неизвестный `agentId` — `AGENT_NOT_FOUND`. От имени пользователя —
`agents.by("ivan").command(…)`, аудит `command`.

**Что уходит по сети.** `cmd.run {commandId, name, args, timeoutSec}` — сразу, если агент на
связи с этим процессом и объявил команду; иначе — когда подключится, объявит её
(воркер запустился) или по `refresh` другого процесса. Агент отвечает `cmd.accept`, по ходу —
`cmd.output`, в конце — `cmd.done {ok, result?, error?}` (важное сообщение).

**Агент** (`internal/commands`, `internal/worker`):

- выполняет только команды из своего списка: встроенные и объявленные воркерами;
- повтор той же команды (после переподключения) узнаёт по `commandId` и не выполняет второй раз
  (помнит час);
- команду воркера передаёт копии воркера, которая сейчас принимает работу (из нескольких —
  самой новой); воркер не запущен — `WORKER_UNAVAILABLE`;
- итог `cmd.done` кладёт в `<dataDir>/outbox` — он дойдёт и после обрыва связи.

**Воркер.**

```python
from agent_sdk.worker import Command

@worker.command("example.report.reload")
def reload(cmd: Command) -> dict:
    force = cmd.args.get("force", False)    # cmd.id, cmd.name, cmd.args, cmd.timeout_sec
    return {"templates": load_templates(force)}
```

```go
w.Command("example.report.reload", func(ctx context.Context, c *worker.Command) (any, error) {
	var args struct{ Force bool `json:"force"` } // c.ID, c.Name, c.Args, c.TimeoutSec
	_ = json.Unmarshal(c.Args, &args)
	return map[string]int{"templates": loadTemplates(args.Force)}, nil
})
```

```ts
worker.command("example.report.reload", async (cmd) => {
  // cmd.id, cmd.name, cmd.args, cmd.timeoutSec
  return { templates: await loadTemplates(cmd.args.force) };
});
```

Имена на `agent.` и `worker.` принадлежат агенту: воркер их объявить не может
([workers.md](workers.md#регистрация-и-имена)).

**Результат.** `Command` со `status: "pending"`, затем `running` (после `cmd.accept`), затем
`succeeded`, `failed` или `cancelled` ([отмена](#отменить-команду)); поля `output`, `result`, `error`, `finishedAt`. Событие
`change` `{kind: "command"}` (в Node — ещё `command`).

## Команда с ожиданием итога

**Бэкенд.**

```ts
const cmd = await agents.call({ name: "example.report.reload", agentId, timeoutSec: 30 });
if (cmd.status === "succeeded") console.log(cmd.result);
else console.log(cmd.error); // { code, message }
```

```go
cmd, err := agents.Call(ctx, server.CommandRequest{Name: "example.report.reload", AgentID: agentID, TimeoutSec: 30})
// err — только ошибка постановки или отмена ctx; неуспех команды — в cmd.Status и cmd.Error
```

```python
cmd = await agents.call("example.report.reload", agent_id=agent_id, timeout_sec=30)
```

`call` = `command` + ожидание. Неуспех команды — не исключение, а `status: "failed"`. Итог
`call` ловит тремя путями: событие своего процесса, проверка `Store` раз в секунду и
перечитывание по каждому `refresh` — поэтому работает и когда агент подключён к другому
процессу бэкенда. Дольше срока команды + 20 с `call` не ждёт и возвращает команду как есть.

**Что уходит по сети, агент, воркер** — как у [command](#отправить-команду).

**Результат** — завершённая `Command` (`succeeded` | `failed` | `cancelled`).

## Вывод по мере работы

**Бэкенд** читает `command.output` (последние 256 КБ) — по событию `command`/`change` или в итоге
`call`.

**Что уходит по сети.** `cmd.output {commandId, chunk}` — обычные сообщения, куски до 64 КБ.

**Агент** копит вывод и отправляет кусками не реже раза в 250 мс; остаток — до `cmd.done`.

**Воркер.**

```python
cmd.write("шаг 1 из 3\n")
```

```go
fmt.Fprintf(c, "шаг %d из %d\n", 1, 3) // Command — io.Writer
```

```ts
cmd.write("шаг 1 из 3\n");
```

**Результат.** `cmd.output` растёт по мере работы; на сервере хранится хвост 256 КБ.

## Срок команды

**Бэкенд** задаёт `timeoutSec` (по умолчанию 60).

**Что уходит по сети.** `cmd.run.timeoutSec`; агенту не ответивший воркер получает `cmd.cancel`
(сообщение канала воркера), а сервер — `cmd.done {ok: false, error: {code: "TIMEOUT"}}`.

**Агент** (`internal/commands`) выполняет команду под сроком `timeoutSec`; не успела — итог
`TIMEOUT`, воркеру — `cmd.cancel`.

**Сервер** сам завершает команду с `TIMEOUT` («Нет итога от агента»), если итога нет через
`timeoutSec` + 15 с **от создания** — так команда не висит вечно, если агент пропал или так и не
подключился.

**Воркер** узнаёт об истёкшем сроке и прекращает работу — итог уже никуда не отправится:

```python
for row in rows:
    cmd.check_cancelled()       # бросит Cancelled; или проверять cmd.cancelled
    handle(row)
if cmd.wait(2):                 # пауза, которую прерывает отмена: True — срок истёк
    raise Cancelled(cmd.id)
```

```go
select {
case <-ctx.Done(): // срок истёк
	return nil, ctx.Err()
default:
}
```

```ts
await fetch(url, { signal: cmd.signal }); // или проверять cmd.cancelled
```

**Результат.** `status: "failed"`, `error: {code: "TIMEOUT", …}`.

## Отменить команду

**Бэкенд.**

```ts
const cmd = await agents.cancelCommand(id); // или agents.by(user).cancelCommand(id)
```

```go
cmd, err := agents.CancelCommand(id) // или agents.By(user).CancelCommand(id)
```

```python
cmd = await agents.cancel_command(command_id)  # или agents.by(user).cancel_command(…)
```

Команда сразу получает `status: "cancelled"`, `error: {code: "CANCELLED", message: "Команду
отменили"}`, `finishedAt`; ждущие `call` возвращают её. Ошибки: `COMMAND_NOT_FOUND` (нет такой
команды), `COMMAND_NOT_ACTIVE` (уже завершена). Аудит `command.cancel`.

**Что уходит по сети** ([§6.4](../spec/README.md#64-команды-capabilitiescommands)):

- команда ещё ждёт отправки агенту — ничего, сервер отменяет её у себя;
- уже отправлена или выполняется — `cmd.cancel {commandId}` агенту, если его сессия в этом
  процессе.
- Сессия агента в другом процессе бэкенда — `cmd.cancel` отправит тот процесс при `refresh`
  ([connection.md](connection.md#несколько-процессов-бэкенда)), один раз. Процесс с сессией
  отслеживает команды, которые отправил агенту в этой сессии, и команды, которые при подключении
  агента уже выполнялись (`running`, в том числе принятые в прошлой сессии у другого процесса);
  при `refresh` отменённым из них он шлёт `cmd.cancel`.
- Агент без связи в момент отмены — `cmd.cancel` не уходит: выполнявшаяся команда доработает
  на узле до конца или до срока, её итог сервер не учтёт.

**Агент** прерывает команду (команде воркера передаёт `cmd.cancel`) и присылает итог `cmd.done`
с ошибкой `CANCELLED`. Сервер его подтверждает, но итог не меняет: команда остаётся
`cancelled`, как и при любом позднем итоге. Поздний `cmd.output` тоже не записывается. Поздний
итог `agent.rotateKey` новый ключ не запоминает.

**Воркер** узнаёт об отмене так же, как об истёкшем сроке ([выше](#срок-команды)):
`cmd.cancelled`, `ctx.Done()`, `cmd.signal`.

**Результат.** `status: "cancelled"`, событие `change` `{kind: "command"}`.

## Ошибки

**Воркер** возвращает свою ошибку с кодом:

```python
from agent_sdk.worker import CommandFailed
raise CommandFailed("RELOAD_FAILED", "шаблоны повреждены")
```

```go
return nil, worker.CommandError("RELOAD_FAILED", "шаблоны повреждены")
```

```ts
import { CommandError } from "agent-sdk/worker";
throw new CommandError("RELOAD_FAILED", "шаблоны повреждены");
```

Любая другая ошибка, исключение или паника — `COMMAND_FAILED` с текстом ошибки; воркер не
падает. Код не по правилу (`^[A-Z0-9_]{1,64}$`) SDK заменяет на `COMMAND_FAILED`, исходный
дописывает в начало текста. Результат, который не превращается в JSON, — `COMMAND_FAILED`;
итог больше 16 МБ (предел строки канала с агентом) — `RESULT_TOO_LARGE`.

**Результат** — `cmd.status: "failed"`, `cmd.error: {code, message}`. Коды, которые бывают:

| Код                    | Кто ставит | Когда                                                                  |
| ---------------------- | ---------- | ---------------------------------------------------------------------- |
| свой (`RELOAD_FAILED`) | воркер     | `CommandFailed` / `worker.CommandError` / `CommandError`               |
| `COMMAND_FAILED`       | SDK, агент | любая другая ошибка или паника обработчика                             |
| `RESULT_TOO_LARGE`     | SDK        | итог команды больше 16 МБ                                              |
| `COMMAND_UNKNOWN`      | агент, SDK | команды нет в списке (выключена, воркер её больше не объявляет)        |
| `WORKER_UNAVAILABLE`   | агент      | воркер, объявивший команду, сейчас не запущен                          |
| `TIMEOUT`              | агент, SDK | срок истёк ([выше](#срок-команды))                                     |
| `CANCELLED`            | сервер     | команду отменил бэкенд ([выше](#отменить-команду)); статус `cancelled` |
| коды встроенных команд | агент      | [таблица ниже](#встроенные-команды-агента)                             |

Ошибки самого вызова (`command`, `call`): `MESSAGE_INVALID`, `AGENT_NOT_FOUND`,
`COMMAND_NOT_SUPPORTED`; `cancelCommand` — `COMMAND_NOT_FOUND` (HTTP 404), `COMMAND_NOT_ACTIVE`
(409); любой изменяющий вызов — `STORE_CONFLICT` (409), если запись так и не удалось записать
([store.md](store.md#правила)). В Node и Python — `AgentsError` (`code`, `message`, `status` —
HTTP-статус для ответа), в Go — `*message.Error` (`Code`, `Message`).

## Встроенные команды агента

Агент сам объявляет и выполняет команды из списка `config.BuiltinCommands`. Любую можно
выключить на узле: `commands.disabled` в `agent.yaml` (или `AGENT_COMMANDS_DISABLED`) — агент её не
объявит, и сервер получит `COMMAND_NOT_SUPPORTED`.

| Команда           | args                                      | Итог (`result`)                | Когда объявлена                                             | Метод SDK                        |
| ----------------- | ----------------------------------------- | ------------------------------ | ----------------------------------------------------------- | -------------------------------- |
| `agent.logs`      | `{lines?}` — по умолчанию 500, до 5000    | `{lines}`; строки — в `output` | всегда                                                      | `call({name: "agent.logs", …})`  |
| `agent.drain`     | —                                         | `status` агента                | всегда                                                      | `call`                           |
| `agent.resume`    | —                                         | `status` агента                | всегда                                                      | `call`                           |
| `agent.restart`   | —                                         | —                              | всегда                                                      | `call`                           |
| `agent.update`    | `{version, url, sha256, signature}`       | `{version}`                    | `update.mode: self`                                         | `updateAgent(agentId)`           |
| `agent.rotateKey` | —                                         | `{secretHash}`                 | всегда                                                      | `rotateKey(agentId)`             |
| `worker.restart`  | `{name?}` — без имени все воркеры         | `{restarted: [имена]}`         | есть воркеры                                                | `call`                           |
| `worker.update`   | `{name, version, url, sha256, signature}` | `{name, version, previous}`    | есть воркер с `release: true` и `update.mode` не `disabled` | `updateWorker(agentId, name)`    |
| `worker.pause`    | `{name, queues?}`                         | `{name, paused}`               | есть воркеры                                                | `pauseWorker(agentId, name, …)`  |
| `worker.resume`   | `{name, queues?}`                         | `{name, paused}`               | есть воркеры                                                | `resumeWorker(agentId, name, …)` |

Что делает каждая (`internal/app`):

- **`agent.logs`** — последние строки лога агента и вывода воркеров из кольцевого буфера в
  памяти (5000 строк).
- **`agent.drain`** — не брать новые задачи (места — 0, `status.state = draining`), текущие
  доделываются. **`agent.resume`** — снова брать.
- **`agent.restart`** — через секунду после ответа агент штатно останавливается (дорабатывает
  задачи не дольше `stopTimeout` воркеров), запускает его снова менеджер процессов (systemd,
  Docker).
- **`agent.update`** — обновление программы агента с проверкой подписи
  ([releases.md](releases.md#обновление-агента)). Ошибки `UPDATE_ARGS`, `UPDATE_NOT_VERIFIED`
  (нет `update.publicKey`), `UPDATE_FAILED`.
- **`agent.rotateKey`** — новый секрет агента ([connection.md](connection.md#смена-ключа)).
  Ошибка `ROTATE_KEY`.
- **`worker.restart`** — замена воркера новой копией его способом (`rolling` или `stop-first`,
  [workers.md](workers.md#замена-воркера)). Ошибка `WORKER_RESTART`.
- **`worker.update`** — новая сборка воркера из выпуска с откатом
  ([releases.md](releases.md#обновление-воркеров-из-выпуска)). Ошибки `UPDATE_ARGS`,
  `WORKER_NOT_RELEASED`, `UPDATE_NOT_VERIFIED`, `WORKER_UPDATE_FAILED`,
  `WORKER_UPDATE_IN_PROGRESS`.
- **`worker.pause`** / **`worker.resume`** — пауза очередей воркера от сервера
  ([workers.md](workers.md#пауза-очередей)). Ошибки `WORKER_ARGS` (нет `name`), `WORKER_UNKNOWN`
  (нет такого воркера в настройках агента).

```ts
const logs = await agents.call({ name: "agent.logs", agentId, args: { lines: 200 } });
console.log(logs.output);
await agents.call({ name: "agent.drain", agentId }); // перед обслуживанием узла
await agents.call({ name: "worker.restart", agentId, args: { name: "report" } });
```

```go
logs, err := agents.Call(ctx, server.CommandRequest{Name: "agent.logs", AgentID: agentID, Args: map[string]int{"lines": 200}})
```

```python
logs = await agents.call("agent.logs", {"lines": 200}, agent_id=agent_id)
```

## Прочитать команды

```ts
const cmd = await agents.getCommand(id); // Command | undefined
const running = await agents.listCommands({ status: ["pending", "running"], agentId }); // новые первыми
```

```go
cmd, err := agents.CommandByID(id) // server.ErrNotFound — нет
running, err := agents.Commands(server.CommandFilter{Status: server.CommandRunning, AgentID: agentID}) // новые первыми
```

```python
cmd = await agents.get_command(command_id)
cmds = await agents.list_commands(status="running", agent_id=agent_id)  # новые первыми
```

Списки читаются страницами: `limit` и `after` — id последней команды прошлой страницы (Go —
`CommandFilter{Limit, After}`, Python — `limit=`, `after=`); команды `after` нет — страница
пустая ([store.md](store.md#методы-по-языкам)).

Поля `Command`: `id`, `agentId`, `name`, `args`, `timeoutSec`, `status` (`pending` | `running` |
`succeeded` | `failed` | `cancelled`), `output`, `result`, `error`, `exitCode` (код выхода из `cmd.done`),
`createdAt`, `finishedAt`, `actor`.
