# Выпуск, установка, обновление, удаление

Выпуск — подписанные сборки агента (и при желании воркеров) с `manifest.json`. Бэкенд раздаёт
их по ссылкам, ставит агента на узел одной командой и обновляет агентов и воркеры; узел
проверяет подпись и откатывается, если новая версия не заработала. Справочник API —
[sdk/README.md](../README.md), формат — [sdk/spec §7](../spec/README.md#7-обновление-агента),
образцы — [commands.json](../spec/examples/commands.json) (`cmd.run.update`,
`cmd.run.workerUpdate`, `cmd.done.workerUpdate`). Флаги установщика и настройки агента —
[docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md#установка).

- [Ключи подписи](#ключи-подписи)
- [Собрать выпуск](#собрать-выпуск)
- [Раздать выпуск](#раздать-выпуск)
- [Установка одной командой](#установка-одной-командой)
- [Обновление агента](#обновление-агента)
- [Обновление воркеров из выпуска](#обновление-воркеров-из-выпуска)
- [Удаление агента с узла](#удаление-агента-с-узла)

## Ключи подписи

```bash
agent keygen
# AGENT_SIGNING_KEY=…        закрытый: только для сборки выпуска (секреты CI), не на бэкенде
# AGENT_UPDATE_PUBLIC_KEY=…  открытый: агентам (update.publicKey) и бэкенду (опция publicKey)
```

Подпись — Ed25519 над hex sha256 файла. Без открытого ключа агент не ставит ни обновление
агента, ни сборку воркера (`UPDATE_NOT_VERIFIED`).

## Собрать выпуск

```bash
AGENT_SIGNING_KEY=… make release            # → dist/<VERSION>/
# или: scripts/release.sh DIR VERSION [--worker NAME=VERSION[,restart=…][,stopTimeout=…]]…
```

`scripts/release.sh` (без Go на машине — `scripts/go.sh release …`) собирает агента для
linux/darwin × amd64/arm64 (`agent-<os>-<arch>`), пишет подписанный `manifest.json`
(`agent release-manifest`) и кладёт `install.sh`.

**Сборки воркеров** попадают в выпуск так: положите файлы `<name>-<version>-<os>-<arch>` в тот же
каталог и передайте `--worker NAME=VERSION` (можно несколько раз). `restart` и `stopTimeout` —
значения по умолчанию для записи воркера в `agent.yaml` при установке.

```json
{
  "version": "1.1.0",
  "artifacts": [{ "os": "linux", "arch": "amd64", "file": "agent-linux-amd64", "sha256": "…", "signature": "…" }],
  "workers": [
    {
      "name": "batch",
      "version": "2.0.0",
      "os": "linux",
      "arch": "amd64",
      "file": "batch-2.0.0-linux-amd64",
      "sha256": "…",
      "signature": "…",
      "restart": "stop-first",
      "stopTimeout": "5m"
    }
  ]
}
```

В CI выпуск попадает в GitHub Release и в образ `ghcr.io/epifanovmd/agent-dist:<версия>`
(каталог `/dist/<версия>/`), который бэкенд может взять к себе `COPY --from=…`.

## Раздать выпуск

**Бэкенд.**

```ts
const agents = new Agents({
  enrollToken,
  releasesDir: "/srv/agent-release", // manifest.json, agent-<os>-<arch>, сборки воркеров, install.sh
  publicKey: process.env.AGENT_UPDATE_PUBLIC_KEY, // вписывается в install.sh
  baseUrl: "https://api.example.com", // публичный адрес (иначе — из запроса)
});
const manifest = await agents.release(); // ReleaseManifest | null
```

```go
agents := server.New(server.Options{EnrollToken: token, ReleasesDir: "/srv/agent-release",
	PublicKey: os.Getenv("AGENT_UPDATE_PUBLIC_KEY"), PublicURL: "https://api.example.com"})
rel := agents.Release() // *server.Release или nil
```

```python
agents = Agents(enroll_token=token, releases_dir="/srv/agent-release",
                public_key=os.environ["AGENT_UPDATE_PUBLIC_KEY"], base_url="https://api.example.com")
manifest = await agents.release()   # dict или None
# маршруты — await agents.handle_release(path, base_url) → (status, bytes, content_type)
```

**Что уходит по сети.** `Agents` сам раздаёт без авторизации — сборки подписаны, секретов в
них нет:

| Ссылка                                          | Что отдаёт                                                    |
| ----------------------------------------------- | ------------------------------------------------------------- |
| `GET /api/v1/agent-link/releases/manifest.json` | манифест выпуска                                              |
| `GET /api/v1/agent-link/releases/<file>`        | сборки агента и воркеров — только файлы из манифеста          |
| `GET /api/v1/agent-link/install.sh`             | установщик, в который уже вписаны адрес сервера и `publicKey` |

**Агент** скачивает отсюда обновления. **Воркер** не участвует.

**Результат.** `release()` — манифест или пусто (нет `releasesDir` или манифест не читается).

## Установка одной командой

**Бэкенд** собирает готовую строку для узла — по флагу `install.sh` на каждую заданную опцию.
Выполнить её на машине (SSH, cloud-init, Ansible) — дело бэкенда: SDK на узлы не заходит.

```ts
const cmd = agents.installCommand({
  token: await issueEnrollToken(), // или tokenFile: "/run/secrets/agent-token" — ровно одно из двух
  name: "node-01",
  user: "agent",
  config: "/etc/agent/node.yaml",
  privileged: false,
  killMode: "mixed", // process | mixed
  packages: ["jq", "curl"],
  packagesByManager: { apk: ["bind-tools"] },
  sysctl: { "vm.max_map_count": "262144" },
  rwPaths: ["/srv/data"],
  caFile: "/etc/ssl/example-ca.pem",
  workers: ["batch"], // воркеры из выпуска (--worker)
  stopTimeout: "15min",
  baseUrl: "https://api.example.com", // иначе — опция baseUrl
});
// curl -fsSL 'https://api.example.com/api/v1/agent-link/install.sh' | sudo sh -s -- --token '…' --name 'node-01' --user 'agent' --config '/etc/agent/node.yaml' --kill-mode 'mixed' --packages 'jq curl' --packages-apk 'bind-tools' --sysctl 'vm.max_map_count=262144' --rw-path '/srv/data' --ca-file '/etc/ssl/example-ca.pem' --worker 'batch' --stop-timeout '15min'
```

```go
cmd, err := agents.InstallCommand(server.InstallOptions{
	Token: token, Name: "node-01", Packages: []string{"jq", "curl"},
	Sysctl: map[string]string{"vm.max_map_count": "262144"}, Workers: []string{"batch"},
})
```

```python
cmd = agents.install_command(token=token, name="node-01", packages=["jq", "curl"],
                             sysctl={"vm.max_map_count": "262144"}, workers=["batch"])
```

Значения взяты в одинарные кавычки; адрес, имена пакетов и менеджеров (`apt`, `dnf`, `yum`,
`apk`, `zypper`), ключи `sysctl`, `killMode` и имена воркеров проверяются; перевод строки в
значении — ошибка. Ошибки — `MESSAGE_INVALID`. `privileged: true` (`--privileged`) нужен, только
если воркер настраивает сам узел: сеть, firewall, системные файлы. Что делает каждый флаг —
[docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md#установка).

**Что уходит по сети.** Установщик берёт у бэкенда сборку под эту машину (`releases/<file>`),
сверяет контрольную сумму; с `--worker` — так же сборку воркера.

**Агент** ставится службой systemd: программа `/opt/agent/bin/agent`, настройки
`/etc/agent/agent.yaml`, токен — `/etc/agent/agent.env`, данные — `/var/lib/agent`. При первом
запуске регистрируется по токену ([connection.md](connection.md#регистрация-по-токену)).
Повторный запуск установщика обновляет программу и службу, не трогая ключ агента.

**Воркер** из выпуска (`--worker batch`) ставится в `/var/lib/agent/workers/batch/current` и
прописывается в `agent.yaml` с `release: true`. Свой воркер — положить программу на узел и
прописать её в `agent.yaml` ([workers.md](workers.md#запуск-и-регистрация)).

**Результат.** Агент появляется в `agents.listAgents()` (событие `agent`/`change`).

## Обновление агента

**Бэкенд.**

```ts
const candidates = await agents.updateCandidates(); // [{ agentId, name, online, current, target, os, arch }]
for (const c of candidates) await agents.updateAgent(c.agentId); // команда agent.update, срок 300 с
```

```go
candidates, err := agents.UpdateCandidates()
cmd, err := agents.UpdateAgent(agentID)
```

```python
candidates = await agents.update_candidates()
cmd = await agents.update_agent(agent_id)
```

Кандидаты — агенты с `update.mode: self`, чья версия (`hello.agent.version`) отличается от
выпуска и для чьих ОС и процессора есть сборка. Нет выпуска, сборки или агент не обновляет себя
сам — `UPDATE_NOT_AVAILABLE`. Аудит `agent.update`.

**Что уходит по сети.** `cmd.run {name: "agent.update", args: {version, url, sha256, signature}}`
(`url` — от корня, `/api/v1/agent-link/releases/<file>`) → `cmd.output` по ходу → `cmd.done
{result: {version}}`.

**Агент** (`internal/update`, `internal/app`):

1. дополняет `url` адресом своего сервера и скачивает сборку во временный файл со своим ключом;
2. сверяет sha256 и подпись открытым ключом `update.publicKey`;
3. сохраняет текущую программу как `.prev`, заменяет её одним шагом (`status.state = updating`);
4. через секунду после ответа штатно останавливается — задачи дорабатываются — и systemd
   запускает новую версию;
5. новая версия за три запуска так и не получила `welcome` — возвращается `.prev`. Под systemd
   это делает прежняя версия ещё до запуска новой (`agent.prev boot-guard`), поэтому откат
   срабатывает, даже если новая падает сразу.

Ошибки команды: `UPDATE_ARGS`, `UPDATE_NOT_VERIFIED`, `UPDATE_FAILED`. `update.mode: external`
(контейнер) — агент только сообщает версию, обновляют его новым образом; `disabled` — не
обновляется.

**Воркер** не участвует: воркеры перезапускаются вместе с агентом.

**Результат.** `Command` `succeeded`; после переподключения — `agent.hello.agent.version` новый.

## Обновление воркеров из выпуска

**Бэкенд.**

```ts
const list = await agents.workerUpdateCandidates();
// [{ agentId, agentName, online, worker, current, target, os, arch }]
for (const c of list) await agents.updateWorker(c.agentId, c.worker); // команда worker.update, срок 300 с
```

```go
list, err := agents.WorkerUpdateCandidates()
cmd, err := agents.UpdateWorker(agentID, "batch")
```

```python
candidates = await agents.worker_update_candidates()
cmd = await agents.update_worker(agent_id, "batch")
```

Кандидаты — воркеры с `release: true` в `status.workers` неотозванных агентов, объявивших
`worker.update`, чья версия отличается от старшей версии этого воркера в манифесте под ОС и
процессор агента. Переустановить ту же версию можно. Ошибки: `MESSAGE_INVALID`,
`AGENT_NOT_FOUND`, `UPDATE_NOT_AVAILABLE` (нет выпуска или сборки, воркер не из выпуска, агент не
объявил команду). Аудит `worker.update`.

**Что уходит по сети.** `cmd.run {name: "worker.update", args: {name, version, url, sha256,
signature}}` → `cmd.done {result: {name, version, previous}}`.

**Агент** (`internal/worker/release.go`):

1. скачивает сборку рядом с текущей, сверяет sha256 и подпись тем же ключом, что у агента;
2. откладывает текущую сборку как `previous` (с её версией), новая становится `current`
   (`<dataDir>/workers/<name>/`);
3. заменяет воркер его способом (`rolling` или `stop-first`) и ждёт, что каждая новая копия
   зарегистрируется **с этой версией** — не дольше `max(stopTimeout, 60 с)`;
4. не зарегистрировалась или сообщила другую версию — возвращает `previous`, перезапускает
   воркер; итог — `WORKER_UPDATE_FAILED`.

Другие ошибки: `WORKER_NOT_RELEASED` (воркер не из выпуска), `UPDATE_NOT_VERIFIED` (нет ключа),
`WORKER_UPDATE_IN_PROGRESS` (уже обновляется), `UPDATE_ARGS`. Добавить или удалить воркер
сервер не может — это решает `agent.yaml`.

**Воркер** сообщает в регистрации ту же версию, что у сборки в выпуске; агент передаёт её в
`AGENT_WORKER_RELEASE_VERSION`:

```python
worker = Worker("batch", version=os.environ.get("AGENT_WORKER_RELEASE_VERSION", "0.0.0"))
```

```go
w := worker.New("batch", os.Getenv("AGENT_WORKER_RELEASE_VERSION"))
```

```ts
const worker = new Worker({ name: "batch", version: process.env.AGENT_WORKER_RELEASE_VERSION });
```

Или впишите версию в сборку при компиляции — главное, чтобы она совпадала с `--worker
NAME=VERSION` выпуска.

**Результат.** `cmd.result {name, version, previous}`; `agent.status.workers[].version` —
новая.

## Удаление агента с узла

**Бэкенд.** Выполнить на узле (как и установку):

```bash
curl -fsSL https://api.example.com/api/v1/agent-link/install.sh | sudo sh -s -- --uninstall [--purge]
```

и отозвать агента, чтобы его ключ больше не принимался: `agents.revoke(agentId)`
([connection.md](connection.md#отзыв-агента)).

**Что уходит по сети.** Ничего: уборка идёт без связи с сервером.

**Агент.** `install.sh --uninstall` сначала вызывает `agent cleanup` — каждый воркер убирает за
собой ([workers.md](workers.md#уборка-при-удалении-агента)), — потом удаляет службу, программу и
настройки ядра из `--sysctl` с возвратом прежних значений. С `--purge` — ещё настройки, данные и
пакеты, которые поставила эта установка (по журналу `/etc/agent/install-state`).

**Воркер** — обработчик `cleanup`.

**Результат.** В бэкенде агент становится `online: false` (уведомление `offline`); после `revoke`
— `revoked: true`.
