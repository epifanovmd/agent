# Сборки, установка, обновление, удаление

Сборки — подписанные файлы агента и воркеров с `manifest.json`. Бэкенд берёт агента из
релизов GitHub (GitHub Releases) или своего каталога, раздаёт сборки, ставит агента на узел одной командой и
обновляет агентов и воркеры; узел проверяет подпись и
возвращает прежнюю сборку, если новая не заработала. Формат —
[sdk/spec §10–§11](../spec/README.md#11-сборки-и-обновление), образцы —
[actions.json](../spec/examples/actions.json). Флаги установщика и настройки агента —
[docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md).

- [Откуда бэкенд берёт агента](#откуда-бэкенд-берёт-агента)
- [Ключи подписи](#ключи-подписи)
- [Собрать сборки](#собрать-сборки)
- [Раздать сборки](#раздать-сборки)
- [Установка одной командой](#установка-одной-командой)
- [Обновление агента](#обновление-агента)
- [Обновление воркеров с сервера](#обновление-воркеров-с-сервера)
- [Удаление агента с узла](#удаление-агента-с-узла)

## Откуда бэкенд берёт агента

Агент один на все проекты и выходит сам по себе. Чтобы новая версия агента не требовала
пересобирать бэкенд, бэкенд заранее настраивают, **откуда брать агента**, — дальше SDK сам следит
за новыми версиями, и обновление становится доступно (`updateCandidates`). Бэкенд пересобирают,
только когда меняется его код или его воркеры.

```ts
const agents = new Agents({
  enrollToken,
  // Агент и его стандартные воркеры (netprobe) — из релизов GitHub (GitHub Releases).
  agentReleases: {
    github: "epifanovmd/agent",
    range: "^1", // необязательно; по умолчанию — та же мажорная версия, что у SDK
    checkIntervalMs: 3_600_000, // необязательно; по умолчанию 1 ч, первая проверка — при старте
    token: process.env.GITHUB_TOKEN, // необязательно: лимиты API GitHub
    proxy: false, // true — сборки узлам через бэкенд потоком (узлы без доступа к GitHub)
  },
  // Воркеры проекта: manifest.json от agent-release и их сборки.
  releasesDir: "/srv/agent-release",
  updatePublicKeys: [process.env.PROJECT_UPDATE_PUBLIC_KEY!], // ключ проекта — в install.sh
  baseUrl: "https://api.example.com",
});
agents.on("release", ({ version, previous }) => {}); // в источнике появилась новая версия
await agents.checkRelease(); // проверить сейчас, не дожидаясь checkIntervalMs
```

**Удалённый источник** (`agentReleases`):

- `{ github: "owner/repo" }` — SDK запрашивает API GitHub (`GET /repos/{owner}/{repo}/releases`),
  берёт релизы без `prerelease` и не `draft`, выбирает старшую версию в диапазоне `range` (semver;
  по умолчанию `^<мажорная версия SDK>`, например `^1` — новая мажорная версия агента сама не
  подхватывается) и скачивает её `manifest.json` и `install.sh`. Нужен публичный репозиторий;
  `token` — только для лимитов запросов к API.
- `{ url: "https://…/releases/download/v1.1.0" }` — адрес, откуда берутся сборки: `<url>/manifest.json`,
  `<url>/<file>`, `<url>/install.sh`. Так закрепляют одну версию, берут сборки со своего
  зеркала или из каталога `agent-dist` на своём сервере.

Проверка — при старте и раз в `checkIntervalMs` (`checkRelease()` — сейчас). Полученный манифест сборок
хранится в памяти процесса; ошибка сети или источника — предупреждение в журнал, остаётся прежний
манифест. Новая версия — событие `release` (`{ version, previous?, from }`). Подписи сборок SDK не
проверяет — это делает агент своими ключами.

**Итоговый набор сборок** — то, что бэкенд раздаёт и чем обновляет:

- агент и его воркеры (netprobe) — из удалённого источника; пока он ни разу не получен — из
  `releasesDir` (если там есть сборки агента);
- воркеры проекта — из `releasesDir`; при совпадении имён воркер проекта важнее (воркер с этим
  именем из удалённого источника не раздаётся).

`release()` возвращает итоговый набор сборок: у каждой сборки — `source` (`remote` или `local`) и `url`
(ссылка источника или путь от корня бэкенда), у набора — `remote` (`{ version, from, checkedAt,
publicKey? }`). `updateCandidates()`, `workerUpdateCandidates()`, `updateAgent()`,
`updateWorker()` работают по нему. Узлам `…/releases/manifest.json` отдаёт тот же набор сборок без
источников: `file` — имя файла.

**Сборки из удалённого источника.** `GET …/releases/<file>` отвечает перенаправлением `302` на
ссылку источника (GitHub): узлу нужен доступ к GitHub, бэкенд не тратит трафик. Узлам без доступа
к GitHub — `agentReleases.proxy: true`: бэкенд скачивает сборку и отдаёт её потоком. В
`agent.update` и `worker.update` ссылка всегда от корня бэкенда: агент идёт к бэкенду со своим
ключом, а при перенаправлении на другой хост ключ не передаёт.

**Воркеры проекта в `releasesDir`.** Сборки — файлы или архивы
`<name>-<version>-<os>-<arch>[.tar.gz]`, `manifest.json` — от `agent-release`, без сборок агента
(подпись — ключом проекта):

```bash
AGENT_SIGNING_KEY=<ключ проекта> agent-release manifest /srv/agent-release 2.0.0 --worker report=2.0.0
```

**Ключи.** У агента может быть несколько ключей проверки; подпись принимается, если сходится с
любым:

- **ключ автора агента** — им подписаны агент и netprobe в релизах GitHub; он вшит в эти сборки, а
  `manifest.json` сборок называет его в поле `publicKey`;
- **ключ проекта** — им проект подписывает свои воркеры; узлу его передаёт установщик
  (`--update-key`), в настройках агента — `update.publicKeys`.

`install.sh` из удалённого источника SDK отдаёт с подставленными адресом бэкенда и ключами:
`publicKey`, `updatePublicKeys` и ключ автора агента (`agentReleases.publicKey` или `publicKey`
из `manifest.json` источника). Нет `install.sh` в источнике — берётся из `releasesDir`.

## Ключи подписи

```bash
scripts/go.sh go run ./cmd/agent-release keygen   # Go в контейнере; где Go есть — go run ./cmd/agent-release keygen
# AGENT_SIGNING_KEY=…        закрытый: только для подписи сборок (секреты CI), не на бэкенде
# AGENT_UPDATE_PUBLIC_KEY=…  открытый: вшивается в сборки агента при make release (или update.publicKeys
#                            в настройках агента) и нужен бэкенду (опция publicKey или updatePublicKeys)
```

Подпись — Ed25519 над строкой `agent-release/1\n<имя>\n<версия>\n<os>\n<arch>\n<sha256>`: она
не подходит к другой сборке, версии или платформе. Без открытого ключа агент не ставит ни
обновление агента, ни сборку воркера (`UPDATE_NOT_VERIFIED`). Ключей может быть несколько —
[выше](#откуда-бэкенд-берёт-агента).

## Собрать сборки

```bash
AGENT_SIGNING_KEY=… AGENT_UPDATE_PUBLIC_KEY=… make release   # → dist/<VERSION>/
```

`scripts/release.sh` собирает агента под linux и darwin × amd64 и arm64 (`agent-<os>-<arch>`) и
стандартный воркер netprobe (`netprobe-<версия>-<os>-<arch>`), пишет подписанный
`manifest.json` утилитой `agent-release manifest` (с `--worker netprobe=<версия>` и `publicKey` —
открытым ключом подписи) и кладёт `install.sh`. Так же собираются сборки для релиза на GitHub (CI по тегу). Сборки
своих воркеров (файл или архив `<имя>-<версия>-<os>-<arch>.tar.gz`) кладут в тот же каталог заранее и
вносят в манифест флагом `--worker`:
`scripts/go.sh release --worker report=1.5.0,command=bin/report,stopTimeout=30s`
([docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md)).

```json
{
  "version": "1.0.0",
  "publicKey": "…",
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

## Раздать сборки

```ts
const agents = new Agents({
  enrollToken,
  releasesDir: "/srv/agent-release", // manifest.json, сборки, install.sh
  publicKey: process.env.AGENT_UPDATE_PUBLIC_KEY, // вписывается в install.sh
  baseUrl: "https://api.example.com", // публичный адрес (иначе — из запроса)
});
const manifest = await agents.release(); // ReleaseView | null — итоговый набор сборок
```

`Agents` раздаёт без авторизации — сборки подписаны, секретов в них нет:

| Ссылка                                          | Что отдаёт                                                                                                |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `GET /api/v1/agent-link/releases/manifest.json` | манифест сборок                                                                                           |
| `GET /api/v1/agent-link/releases/<file>`        | сборки агента и воркеров — только файлы из манифеста; из удалённого источника — `302` или поток (`proxy`) |
| `GET /api/v1/agent-link/install.sh`             | установщик, в который уже вписаны адрес сервера и ключи проверки (`DEFAULT_UPDATE_KEYS`)                  |

## Установка одной командой

Бэкенд собирает строку для узла — по флагу `agent install` на каждую заданную опцию. Выполнить
её на машине (SSH, cloud-init, Ansible) — дело бэкенда.

```ts
const cmd = agents.installCommand({
  instance: "example", // необязательно: ещё один агент на узле (--instance), см. ниже
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
  workers: ["report"], // воркеры с сервера (--worker)
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
`agents.listAgents()`. Воркер со сборкой с сервера (`--worker report`) ставится в
`/var/lib/agent/workers/report/current` и прописывается в `agent.yaml` с `release: true`.

**Если на узле уже есть агент другого бэкенда**, ставьте своего отдельным экземпляром:
`instance: "example"` (флаг `--instance`; имя — по правилу имён). У экземпляра свои служба
(`agent-example`), пользователь, настройки (`/etc/agent-example`), данные
(`/var/lib/agent-example`) и программа — агенты разных бэкендов не мешают друг другу и
обновляются по отдельности. Пути и команды экземпляра —
[docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md#несколько-агентов-на-одном-узле).

## Обновление агента

```ts
const candidates = await agents.updateCandidates(); // [{ agentId, name, online, current, target, os, arch }]
for (const c of candidates) await agents.updateAgent(c.agentId); // → { version, previous }
```

Кандидаты — агенты, чья версия отличается от версии в манифесте сборок и для чьих ОС и процессора есть сборка.
Нет манифеста или сборки — `UPDATE_NOT_AVAILABLE`. Аудит — `agent.update`.

**Что уходит по сети.** `action {name: "agent.update", args: {version, url, sha256, signature}}`
(`url` — от корня бэкенда; сборку удалённого источника бэкенд отдаёт перенаправлением или потоком)
→ после запуска новой версии — `action.result {ok, result: {version,
previous}}`. Итог приходит уже в новом соединении; срок ожидания — `updateTimeoutMs` (по
умолчанию 5 мин).

**Агент** скачивает сборку со своим ключом, сверяет sha256 и подпись, сохраняет текущую
программу как `.prev`, заменяет её и перезапускается. Новая версия за три запуска не получила
`welcome` — возвращается `.prev`, итог — `UPDATE_FAILED`. Агент в контейнере себя не обновляет —
`UPDATE_NOT_SUPPORTED` (`update.mode: external`, обновляют образ; так же при `update.mode: disabled`). Воркеры обновление агента не замечают: они работают
дальше, новая версия их подхватывает (`lifecycle.onAgentRestart: keep`), — долгая работа не
прерывается и ждать её окончания не нужно.

## Обновление воркеров с сервера

```ts
const list = await agents.workerUpdateCandidates();
// [{ agentId, agentName, online, worker, current, target, os, arch }]
for (const c of list) await agents.updateWorker(c.agentId, c.worker);
// → { version, previous, deferred: false } — обновлён;
//   { deferred: true, pending: "update", actionId } — воркер занят, итог — событие action
await agents.updateWorker(agentId, "report", { force: true }); // не ждать окончания работы воркера
await agents.updateWorker(agentId, "report", { wait: true }); // ждать итога и отложенной замены
```

Кандидаты — воркеры с `release: true`, чья версия отличается от новейшей сборки этого воркера в
манифесте под ОС и процессор агента. Переустановить ту же версию можно. Воркер, прописанный командой, —
`WORKER_NOT_RELEASED`, нет сборки — `UPDATE_NOT_AVAILABLE`. Аудит — `worker.update`.

**Агент** скачивает и проверяет сборку; если воркер занят (`GET /health` → `busy: true`), сразу
отвечает, что замена отложена, и ждёт окончания его работы (в `status` — `pending: "update"`, не
дольше `lifecycle.busy.timeout`; `force: true` — не ждёт; итог — событие `action` с
`deferred: true`), останавливает прежний процесс, запускает новый и ждёт `ok: true` на
`GET /health` до минуты (`lifecycle.updateHealthyTimeout`). Не дождался — возвращает прежнюю сборку, итог —
`UPDATE_FAILED`. Уже обновляется — `BUSY`. Добавить или удалить воркер бэкенд не может — это
решает `agent.yaml`.

## Удаление агента с узла

Выполнить на узле (как и установку):

```bash
sudo agent uninstall [--purge]
# или тем же установщиком:
curl -fsSL https://api.example.com/api/v1/agent-link/install.sh | sudo sh -s -- --uninstall [--purge]
# экземпляр (instance в installCommand) — с --instance ИМЯ; другие экземпляры не затрагиваются
```

и отозвать агента, чтобы его ключ больше не принимался: `agents.revoke(agentId)`.

Агент запускает воркеры, вызывает у каждого `POST /cleanup` — воркер убирает всё, что создал на
узле ([workers.md](workers.md#что-воркер-обслуживает)), — останавливает их и удаляет себя. С
`--purge` — ещё настройки, данные и пакеты, которые поставила установка. Бэкенд уборку запустить
не может. В бэкенде агент становится `online: false` (проблема `offline`); после `revoke` —
`revoked: true`.
