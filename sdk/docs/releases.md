# Сборки, установка, обновление, удаление

Сборки — подписанные файлы агента и воркеров с `manifest.json`. Бэкенд берёт агента из
релизов GitHub (GitHub Releases) или своего каталога, раздаёт сборки и обновляет агентов и
воркеры; ставит агента на узел установщик из релиза агента; узел проверяет подпись и
возвращает прежнюю сборку, если новая не заработала. Формат —
[sdk/spec §10–§11](../spec/README.md#11-сборки-и-обновление), образцы —
[actions.json](../spec/examples/actions.json). Флаги установщика и настройки агента —
[docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md).

- [Откуда бэкенд берёт агента](#откуда-бэкенд-берёт-агента)
- [Ключи подписи](#ключи-подписи)
- [Собрать сборки](#собрать-сборки)
- [Раздать сборки](#раздать-сборки)
- [Установка одной командой](#установка-одной-командой)
- [Установка из архива проекта](#установка-из-архива-проекта)
- [Обновление агента](#обновление-агента)
- [Агент сам замечает новые версии](#агент-сам-замечает-новые-версии)
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
    range: "^2", // необязательно; по умолчанию — та же мажорная версия, что у SDK
    checkIntervalMs: 3_600_000, // необязательно; по умолчанию 1 ч, первая проверка — при старте
    token: process.env.GITHUB_TOKEN, // необязательно: лимиты API GitHub
    proxy: false, // true — сборки узлам через бэкенд потоком (узлы без доступа к GitHub)
  },
  // Воркеры проекта: manifest.json от agent manifest и их сборки.
  releasesDir: "/srv/agent-release",
});
agents.on("release", ({ version, previous }) => {}); // в источнике появилась новая версия
await agents.release({ refresh: true }); // проверить сейчас, не дожидаясь checkIntervalMs
```

**Удалённый источник** (`agentReleases`):

- `{ github: "owner/repo" }` — SDK запрашивает API GitHub (`GET /repos/{owner}/{repo}/releases`),
  берёт релизы без `prerelease` и не `draft`, выбирает старшую версию в диапазоне `range` (semver;
  по умолчанию `^<мажорная версия SDK>`, например `^2` — новая мажорная версия агента сама не
  подхватывается) и скачивает её `manifest.json`. Нужен публичный репозиторий;
  `token` — только для лимитов запросов к API.
- `{ url: "https://…/releases/download/v1.1.0" }` — адрес, откуда берутся сборки: `<url>/manifest.json`,
  `<url>/<file>`. Так закрепляют одну версию, берут сборки со своего
  зеркала или из каталога `agent-dist` на своём сервере.

Проверка — при старте и раз в `checkIntervalMs` (`release({ refresh: true })` — сейчас). Полученный манифест сборок
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
publicKey? }`). `updateCandidates()`, `updateAgent()` и `updateWorker()` работают по нему. Узлам `…/releases/manifest.json` отдаёт тот же набор сборок без
источников: `file` — имя файла.

**Сборки из удалённого источника.** `GET …/releases/<file>` отвечает перенаправлением `302` на
ссылку источника (GitHub): узлу нужен доступ к GitHub, бэкенд не тратит трафик. Узлам без доступа
к GitHub — `agentReleases.proxy: true`: бэкенд скачивает сборку и отдаёт её потоком. В
`agent.update` и `worker.update` ссылка всегда от корня бэкенда: агент идёт к бэкенду со своим
ключом, а при перенаправлении на другой хост ключ не передаёт.

**Воркеры проекта в `releasesDir`.** Сборки — файлы или архивы
`<name>-<version>-<os>-<arch>[.tar.gz]`, `manifest.json` — от `agent manifest`, без сборок агента
(подпись — ключом проекта):

```bash
AGENT_SIGNING_KEY=<ключ проекта> agent manifest /srv/agent-release 2.0.0 --worker report=2.0.0
```

**Ключи.** У агента может быть несколько ключей проверки; подпись принимается, если сходится с
любым:

- **ключ автора агента** — им подписаны агент и netprobe в релизах GitHub; он вшит в эти сборки, а
  `manifest.json` сборок называет его в поле `publicKey`;
- **ключ проекта** — им проект подписывает свои воркеры; узлу его передаёт команда установки
  (`--update-key`) или архив `agent pack`, в настройках агента — `update.publicKeys`.

## Ключи подписи

```bash
agent keygen   # пара ключей подписи сборок
# AGENT_SIGNING_KEY=…         закрытый: только там, где подписывают сборки (agent pack, секреты CI), не на бэкенде
# AGENT_UPDATE_PUBLIC_KEYS=…  открытый: agent pack кладёт его в архив, установка из архива — к ключам узла;
#                             в команде установки — флагом --update-key
```

Сборки воркеров проекта удобнее всего делать `agent pack` в папке агента: каталог `release/` в
архиве (`manifest.json` и подписанные архивы воркеров) — готовый каталог сборок для опции
`releasesDir`. Папка агента и установка из архива — [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md#из-папки-агента-и-архива).

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
`manifest.json` командой `agent manifest` (с `--worker netprobe=<версия>` и `publicKey` —
открытым ключом подписи) и кладёт установщик `install.sh`. Так же собираются сборки для релиза на GitHub (CI по тегу). Сборки
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
  releasesDir: "/srv/agent-release", // manifest.json и сборки
});
const manifest = await agents.release(); // ReleaseView | null — итоговый набор сборок
```

`Agents` раздаёт без авторизации — сборки подписаны, секретов в них нет:

| Ссылка                                          | Что отдаёт                                                                                                |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `GET /api/v1/agent-link/releases/manifest.json` | манифест сборок                                                                                           |
| `GET /api/v1/agent-link/releases/<file>`        | сборки агента и воркеров — только файлы из манифеста; из удалённого источника — `302` или поток (`proxy`) |

## Установка одной командой

Ставит агента установщик `install.sh` из релиза агента — SDK его не раздаёт и команду не
собирает. Адрес сервера, токен и ключи проверки подписи сборок проекта передаются флагами;
строку для узла собирает бэкенд, выполнить её на машине (SSH, cloud-init, Ansible) — тоже его
дело:

```bash
I=https://github.com/epifanovmd/agent/releases/latest/download/install.sh
curl -fsSL "$I" | sudo sh -s -- --server https://api.example.com --token <токен> \
  --update-key <ключ проекта> --name node-01 --worker report
```

```ts
const sh = (v: string) => `'${v.replaceAll("'", `'\\''`)}'`;
const flags = ["--server", "https://api.example.com", "--token", await issueEnrollToken(), "--name", "node-01"];
const cmd = `curl -fsSL ${sh(installUrl)} | sudo sh -s -- ${flags.map(sh).join(" ")}`;
```

`installUrl` (`$I` выше) — `https://github.com/epifanovmd/agent/releases/latest/download/install.sh`, адрес
той же версии (`…/releases/download/v<версия>/install.sh`) или свой: установщик лежит и в каталоге
сборок `make release` (`dist/<версия>/install.sh`), бэкенд может раздавать его сам — любым
статическим хостингом. Остальные флаги уходят `agent install` как есть: `--worker` (воркеры с
сервера), `--packages`, `--sysctl`, `--privileged`, `--user`, `--rw-path`, `--ca-file`,
`--kill-mode`, `--stop-timeout`, `--token-file` вместо `--token`. `--privileged` нужен, только если
воркер настраивает сам узел: сеть, firewall, системные файлы. Флаги —
[ARCHITECTURE.md](../../docs/ARCHITECTURE.md#установка).

Способ установки — флаги `--rootless` и `--run` (по умолчанию — служба systemd от root):

```bash
curl -fsSL "$I" | sh -s -- --rootless --server … --token …   # без sudo, агент в ~/.agent
curl -fsSL "$I" | sh -s -- --run --server … --token …        # без установки, на переднем плане
```

`--rootless` — агент пользователя без root: команда без `sudo`, служба пользователя systemd (её
нет — ошибка: запускайте агента сами, `--run`); с ним нельзя `--user`, `--sysctl`,
`--privileged`, `--rw-path` и пакеты. `--run` — без установки: агент запускается здесь же на
переднем плане (контейнер, свой менеджер процессов), себя не обновляет; с ним — только `--token`
или `--token-file`, `--name`, `--update-key`, `--ca-file` и `--releases`.

`install.sh` скачивает с бэкенда (`GET /api/v1/agent-link/releases/…`) сборку агента под машину,
сверяет контрольную сумму и запускает `agent install` с этими флагами (с `--run` — `agent run`).
Агент при первом запуске регистрируется по токену
([connection.md](connection.md#регистрация-по-токену)) и появляется в `agents.listAgents()`.
Воркер со сборкой с сервера (`--worker report`) ставится в
`/var/lib/agent/workers/report/current` и прописывается в `agent.yaml` с `release: true`.

**Если на узле уже есть агент другого бэкенда**, ставьте своего отдельным экземпляром:
`--instance example` (имя — по правилу имён). У экземпляра свои служба
(`agent-example`), пользователь, настройки (`/etc/agent-example`), данные
(`/var/lib/agent-example`) и программа — агенты разных бэкендов не мешают друг другу и
обновляются по отдельности. Пути и команды экземпляра —
[docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md#несколько-агентов-на-одном-узле).

## Установка из архива проекта

Бэкенд, у которого настройки и воркеры агента — своя папка агента, собирает её архивы
`agent pack` (`agent-<окружение>-<версия>-linux-<arch>.tar.gz`) и раздаёт их сам — любым
статическим хостингом. Ставит тот же `install.sh` с флагом `--bundle URL`; `{arch}` в ссылке
заменяется на `amd64` или `arm64` машины:

```bash
B='https://files.example.com/agent-prod-linux-{arch}.tar.gz'
curl -fsSL "$I" | sudo sh -s -- --bundle "$B" --token <токен> --name node-01
curl -fsSL "$I" | sh -s -- --bundle "$B" --rootless --token <токен>   # без root
curl -fsSL "$I" | sh -s -- --bundle "$B" --run --token <токен>        # без установки (контейнер)
curl -fsSL "$I" | sudo sh -s -- --bundle "$B" --uninstall [--purge]   # удалить
```

Программа агента, экземпляр, воркеры, пакеты, права и ключ проекта — из архива (`instance`,
`install:` в настройках, ключ — в `bundle.json`); в команде — токен (`--token` или
`--token-file`), имя и способ (`--rootless` или `--run`). `--server` нужен, только если адреса
сервера нет в настройках архива. Установщик скачивает архив под машину и запускает из него
`agent install` (служба или `--rootless`) или `agent run` с настройками архива (`--run`: данные —
`AGENT_DATA_DIR`, по умолчанию `/var/lib/agent`; ключ проекта из `bundle.json` здесь не читается —
передайте его `--update-key`).

## Обновление агента

```ts
const { agents: list } = await agents.updateCandidates();
// [{ agentId, agentName, online, current, target, os, arch, source: "server" | "agent" }]
for (const c of list) await agents.updateAgent(c.agentId, c.source === "agent" ? { version: c.target } : {}); // → { version, previous }
```

Кандидаты — агенты, чья версия отличается от версии в манифесте сборок и для чьих ОС и процессора
есть сборка (`source: "server"` — `updateAgent` без `version`), или те, кто нашёл в своём каталоге
сборок версию новее своей (`source: "agent"` — `updateAgent` с `version: target`,
[ниже](#агент-сам-замечает-новые-версии)). Из двух версий берётся новейшая.
Агенты, которые себя не обновляют (`hello.agent.updateMode` — `external` у агента в контейнере или
`disabled`), в кандидаты не входят: `agent.update` им ответил бы `UPDATE_NOT_SUPPORTED`. Как агент
поставлен на узел — `hello.agent.install` (`systemd`, `user`; нет — запущен иначе,
например в контейнере).
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
дальше, новая версия их подхватывает (`lifecycle.onAgentExit: keep`), — долгая работа не
прерывается и ждать её окончания не нужно.

## Агент сам замечает новые версии

Агент раз в `update.checkInterval` (6 ч) смотрит последнюю версию в своём каталоге сборок (релизы
агента на GitHub или его `update.releases`) и сообщает новую в `hello` и `status`; в SDK это поле
`update` агента:

```ts
const outdated = (await agents.listAgents()).filter((a) => a.update);
// [{ id, name, version: "1.1.0", update: { latest: "1.2.0", checkedAt }, … }]
for (const a of outdated) await agents.updateAgent(a.id, { version: a.update!.latest }); // агент берёт сборку сам
```

`updateAgent(id, { version })` отправляет `agent.update` только с версией: агент сам скачивает
сборку под свою платформу из своего каталога сборок и сверяет подпись из его `manifest.json`;
бэкенду не нужны ни `releasesDir`, ни `agentReleases`. Неверная версия — `MESSAGE_INVALID`. Без
`version` — сборка из набора бэкенда. Такие агенты входят и в `updateCandidates()` с
`source: "agent"`. На узле то же делает `agent upgrade`.

## Обновление воркеров с сервера

```ts
const { workers } = await agents.updateCandidates();
// [{ agentId, agentName, online, worker, current, target, os, arch }]
for (const c of workers) await agents.updateWorker(c.agentId, c.worker);
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
дольше `lifecycle.busyTimeout`; `force: true` — не ждёт; итог — событие `action` с
`deferred: true`), останавливает прежний процесс, запускает новый и ждёт `ok: true` на
`GET /health` до минуты (`lifecycle.startTimeout`). Не дождался — возвращает прежнюю сборку, итог —
`UPDATE_FAILED`. Уже обновляется — `BUSY`. Добавить или удалить воркер бэкенд не может — это
решает `agent.yaml`.

## Удаление агента с узла

Выполнить на узле (как и установку):

```bash
sudo agent uninstall [--purge]
# или тем же установщиком:
curl -fsSL https://github.com/epifanovmd/agent/releases/latest/download/install.sh | sudo sh -s -- --uninstall [--purge]
# экземпляр — с --instance ИМЯ (другие экземпляры не затрагиваются); агент из архива — с --bundle URL
```

и отозвать агента, чтобы его ключ больше не принимался: `agents.revoke(agentId)`.

Агент запускает воркеры, вызывает у каждого `POST /cleanup` — воркер убирает всё, что создал на
узле ([workers.md](workers.md#что-воркер-обслуживает)), — останавливает их и удаляет себя. С
`--purge` — ещё настройки, данные и пакеты, которые поставила установка. Бэкенд уборку запустить
не может. В бэкенде агент становится `online: false` (проблема `offline`); после `revoke` —
`revoked: true`.
