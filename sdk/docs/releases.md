# Выпуск, установка, обновление, удаление

Выпуск — подписанные сборки агента и воркеров с `manifest.json`. Бэкенд раздаёт их, ставит
агента на узел одной командой и обновляет агентов и воркеры; узел проверяет подпись и
возвращает прежнюю сборку, если новая не заработала. Формат —
[sdk/spec §10–§11](../spec/README.md#11-выпуск-и-обновление), образцы —
[actions.json](../spec/examples/actions.json). Флаги установщика и настройки агента —
[docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md).

- [Ключи подписи](#ключи-подписи)
- [Собрать выпуск](#собрать-выпуск)
- [Раздать выпуск](#раздать-выпуск)
- [Установка одной командой](#установка-одной-командой)
- [Обновление агента](#обновление-агента)
- [Обновление воркеров из выпуска](#обновление-воркеров-из-выпуска)
- [Удаление агента с узла](#удаление-агента-с-узла)

## Ключи подписи

```bash
scripts/go.sh go run ./cmd/agent-release keygen   # Go в контейнере; где Go есть — go run ./cmd/agent-release keygen
# AGENT_SIGNING_KEY=…        закрытый: только для сборки выпуска (секреты CI), не на бэкенде
# AGENT_UPDATE_PUBLIC_KEY=…  открытый: вшивается в сборки агента при make release (или update.publicKey
#                            в настройках агента) и нужен бэкенду (опция publicKey)
```

Подпись — Ed25519 над строкой `agent-release/1\n<имя>\n<версия>\n<os>\n<arch>\n<sha256>`: она
не подходит к другой сборке, версии или платформе. Без открытого ключа агент не ставит ни
обновление агента, ни сборку воркера (`UPDATE_NOT_VERIFIED`).

## Собрать выпуск

```bash
AGENT_SIGNING_KEY=… AGENT_UPDATE_PUBLIC_KEY=… make release   # → dist/<VERSION>/
```

`scripts/release.sh` собирает агента под linux и darwin × amd64 и arm64 (`agent-<os>-<arch>`),
пишет подписанный `manifest.json` утилитой `agent-release manifest` и кладёт `install.sh`. Сборки
своих воркеров (файл или архив `<имя>-<версия>-<os>-<arch>.tar.gz`) кладут в тот же каталог заранее и
вносят в манифест флагом `--worker`:
`scripts/go.sh release --worker report=1.5.0,command=bin/report,stopTimeout=30s`
([docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md)).

```json
{
  "version": "1.0.0",
  "artifacts": [{ "os": "linux", "arch": "amd64", "file": "agent-linux-amd64", "sha256": "…", "signature": "…" }],
  "workers": [
    {
      "name": "report",
      "version": "1.5.0",
      "os": "linux",
      "arch": "amd64",
      "file": "report-1.5.0-linux-amd64.tar.gz",
      "sha256": "…",
      "signature": "…",
      "command": "bin/report",
      "stopTimeout": "30s"
    }
  ]
}
```

## Раздать выпуск

```ts
const agents = new Agents({
  enrollToken,
  releasesDir: "/srv/agent-release", // manifest.json, сборки, install.sh
  publicKey: process.env.AGENT_UPDATE_PUBLIC_KEY, // вписывается в install.sh
  baseUrl: "https://api.example.com", // публичный адрес (иначе — из запроса)
});
const manifest = await agents.release(); // ReleaseManifest | null
```

`Agents` раздаёт без авторизации — сборки подписаны, секретов в них нет:

| Ссылка                                          | Что отдаёт                                                    |
| ----------------------------------------------- | ------------------------------------------------------------- |
| `GET /api/v1/agent-link/releases/manifest.json` | манифест выпуска                                              |
| `GET /api/v1/agent-link/releases/<file>`        | сборки агента и воркеров — только файлы из манифеста          |
| `GET /api/v1/agent-link/install.sh`             | установщик, в который уже вписаны адрес сервера и `publicKey` |

## Установка одной командой

Бэкенд собирает строку для узла — по флагу `agent install` на каждую заданную опцию. Выполнить
её на машине (SSH, cloud-init, Ansible) — дело бэкенда.

```ts
const cmd = agents.installCommand({
  token: await issueEnrollToken(), // или tokenFile: "/run/secrets/agent-token" — ровно одно из двух
  name: "node-01",
  user: "agent",
  config: "/etc/agent/node.yaml",
  privileged: false,
  killMode: "process", // process (по умолчанию: воркеры переживают перезапуск агента) | mixed
  packages: ["jq", "curl"],
  packagesByManager: { apk: ["bind-tools"] },
  sysctl: { "vm.max_map_count": "262144" },
  rwPaths: ["/srv/data"],
  caFile: "/etc/ssl/example-ca.pem",
  workers: ["report"], // воркеры из выпуска (--worker)
  stopTimeout: "15min",
  releases: "https://cdn.example.com/agent", // необязательно: другой источник сборок
  baseUrl: "https://api.example.com", // иначе — опция baseUrl
});
// curl -fsSL 'https://api.example.com/api/v1/agent-link/install.sh' | sudo sh -s -- --token '…' --name 'node-01' …
```

Значения взяты в одинарные кавычки; адрес, имена пакетов и менеджеров (`apt`, `dnf`, `yum`,
`apk`, `zypper`), ключи `sysctl`, `killMode` и имена воркеров проверяются. Ошибка
`MESSAGE_INVALID` — неверное значение, перевод строки в значении, не ровно одно из `token` и
`tokenFile` или нет адреса (ни `baseUrl` в вызове, ни опции `baseUrl`). `privileged: true` нужен, только если воркер настраивает
сам узел: сеть, firewall, системные файлы.

`install.sh` скачивает с бэкенда сборку агента под машину, сверяет контрольную сумму и
запускает `agent install` с этими флагами. Агент ставится службой systemd, при первом запуске
регистрируется по токену ([connection.md](connection.md#регистрация-по-токену)) и появляется в
`agents.listAgents()`. Воркер из выпуска (`--worker report`) ставится в
`/var/lib/agent/workers/report/current` и прописывается в `agent.yaml` с `release: true`.

## Обновление агента

```ts
const candidates = await agents.updateCandidates(); // [{ agentId, name, online, current, target, os, arch }]
for (const c of candidates) await agents.updateAgent(c.agentId); // → { version, previous }
```

Кандидаты — агенты, чья версия отличается от выпуска и для чьих ОС и процессора есть сборка.
Нет выпуска или сборки — `UPDATE_NOT_AVAILABLE`. Аудит — `agent.update`.

**Что уходит по сети.** `action {name: "agent.update", args: {version, url, sha256, signature}}`
(`url` — от корня) → после запуска новой версии — `action.result {ok, result: {version,
previous}}`. Итог приходит уже в новом соединении; срок ожидания — `updateTimeoutMs` (по
умолчанию 5 мин).

**Агент** скачивает сборку со своим ключом, сверяет sha256 и подпись, сохраняет текущую
программу как `.prev`, заменяет её и перезапускается. Новая версия за три запуска не получила
`welcome` — возвращается `.prev`, итог — `UPDATE_FAILED`. Агент в контейнере себя не обновляет —
`UPDATE_NOT_SUPPORTED` (`update.mode: external`, обновляют образ; так же при `update.mode: disabled`). Воркеры обновление агента не замечают: они работают
дальше, новая версия их подхватывает (`lifecycle.onAgentRestart: keep`), — долгая работа не
прерывается и ждать её окончания не нужно.

## Обновление воркеров из выпуска

```ts
const list = await agents.workerUpdateCandidates();
// [{ agentId, agentName, online, worker, current, target, os, arch }]
for (const c of list) await agents.updateWorker(c.agentId, c.worker); // → { version, previous }
await agents.updateWorker(agentId, "report", { force: true }); // не ждать окончания работы воркера
```

Кандидаты — воркеры с `release: true`, чья версия отличается от новейшей сборки этого воркера в
манифесте под ОС и процессор агента. Переустановить ту же версию можно. Воркер не из выпуска —
`WORKER_NOT_RELEASED`, нет сборки — `UPDATE_NOT_AVAILABLE`. Аудит — `worker.update`.

**Агент** скачивает и проверяет сборку; если воркер занят (`GET /health` → `busy: true`), ждёт
окончания его работы (в `status` — `pending: "update"`, не дольше `lifecycle.busy.timeout`;
`force: true` — не ждёт), останавливает прежний процесс, запускает новый и ждёт `ok: true` на
`GET /health` до минуты (`lifecycle.updateHealthyTimeout`). Не дождался — возвращает прежнюю сборку, итог —
`UPDATE_FAILED`. Уже обновляется — `BUSY`. Добавить или удалить воркер бэкенд не может — это
решает `agent.yaml`.

## Удаление агента с узла

Выполнить на узле (как и установку):

```bash
sudo agent uninstall [--purge]
# или тем же установщиком:
curl -fsSL https://api.example.com/api/v1/agent-link/install.sh | sudo sh -s -- --uninstall [--purge]
```

и отозвать агента, чтобы его ключ больше не принимался: `agents.revoke(agentId)`.

Агент запускает воркеры, вызывает у каждого `POST /cleanup` — воркер убирает всё, что создал на
узле ([workers.md](workers.md#что-воркер-обслуживает)), — останавливает их и удаляет себя. С
`--purge` — ещё настройки, данные и пакеты, которые поставила установка. Бэкенд уборку запустить
не может. В бэкенде агент становится `online: false` (проблема `offline`); после `revoke` —
`revoked: true`.
