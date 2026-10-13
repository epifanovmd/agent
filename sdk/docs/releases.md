# Сборки, установка, обновление, удаление

Сборки — подписанные файлы агента и воркеров с `manifest.json`. Сборки агента лежат в его каталоге
сборок (релизы агента на GitHub или зеркало), сборки воркеров проекта раздаёт бэкенд; ставит агента
на узел установщик из релиза агента; обновления запускает бэкенд, а узел проверяет подпись и
возвращает прежнюю сборку, если новая не заработала. Формат —
[sdk/spec §10–§11](../spec/README.md#11-сборки-и-обновление), образцы —
[actions.json](../spec/examples/actions.json). Флаги установщика и настройки агента —
[docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md).

- [Откуда берутся сборки](#откуда-берутся-сборки)
- [Ключи подписи](#ключи-подписи)
- [Собрать сборки](#собрать-сборки)
- [Раздать сборки](#раздать-сборки)
- [Установка одной командой](#установка-одной-командой)
- [Установка из архива проекта](#установка-из-архива-проекта)
- [Обновление агента](#обновление-агента)
- [Обновление воркеров с сервера](#обновление-воркеров-с-сервера)
- [Удаление агента с узла](#удаление-агента-с-узла)

## Откуда берутся сборки

| Сборки          | Где лежат                                                                                                                     | Кто ставит                                                        |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| агент           | каталог сборок агента: релизы агента на GitHub или зеркало — `update.releases` в настройках агента (ссылка или папка на узле) | агент сам: по `updateAgent(id, { version })` или `agent upgrade`  |
| воркеры проекта | `releasesDir` бэкенда: `manifest.json` от `agent manifest` и сборки                                                           | агент по ссылке от бэкенда: `updateWorker`, `install.sh --worker` |

Новая версия агента не требует ни пересобирать бэкенд, ни класть её на бэкенд: агент сам находит
её в своём каталоге сборок, сообщает бэкенду ([ниже](#обновление-агента)) и ставит по команде.
Узлу без доступа к GitHub задают зеркало — `update.releases`
([ARCHITECTURE.md](../../docs/ARCHITECTURE.md#настройки)).

**Воркеры проекта в `releasesDir`.** Сборки — файлы или архивы
`<name>-<version>-<os>-<arch>[.tar.gz]`, `manifest.json` — от `agent manifest`, без сборок агента
(подпись — ключом проекта):

```bash
AGENT_SIGNING_KEY=<ключ проекта> agent manifest /srv/agent-release 2.0.0 --worker report=2.0.0
```

**Ключи.** У агента может быть несколько ключей проверки; подпись принимается, если сходится с
любым:

- **ключ автора агента** — им подписаны сборки агента в релизах GitHub; он вшит в эти сборки, а
  `manifest.json` сборок называет его в поле `publicKey`;
- **ключ проекта** — им проект подписывает свои воркеры; узлу его передаёт команда установки
  (`--update-key`) или архив `agent pack`, в настройках агента — `update.publicKeys`.

## Ключи подписи

Пара Ed25519 в base64 (OpenSSL 1.1.1 и новее):

```bash
openssl genpkey -algorithm ed25519 -outform DER -out signing.der
echo "AGENT_SIGNING_KEY=$(tail -c 32 signing.der | base64)"
echo "AGENT_UPDATE_PUBLIC_KEYS=$(openssl pkey -inform DER -in signing.der -pubout -outform DER | tail -c 32 | base64)"
rm signing.der
# AGENT_SIGNING_KEY         закрытый: только там, где подписывают сборки (agent pack, секреты CI), не на бэкенде
# AGENT_UPDATE_PUBLIC_KEYS  открытый: agent pack кладёт его в архив, установка из архива — к ключам узла;
#                           в команде установки — флагом --update-key
```

Сборки воркеров проекта удобнее всего делать `agent pack` в папке агента: каталог `release/` в
архиве (`manifest.json` и подписанные архивы воркеров) — готовый каталог сборок для опции
`releasesDir`. Папка агента и установка из архива — [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md#из-папки-агента-и-архива).

Подпись — Ed25519 над строкой `agent-release/1\n<имя>\n<версия>\n<os>\n<arch>\n<sha256>`: она
не подходит к другой сборке, версии или платформе. Без открытого ключа агент не ставит ни
обновление агента, ни сборку воркера (`UPDATE_NOT_VERIFIED`). Ключей может быть несколько —
[выше](#откуда-берутся-сборки).

## Собрать сборки

```bash
AGENT_SIGNING_KEY=… AGENT_UPDATE_PUBLIC_KEY=… make release   # → dist/<VERSION>/
```

`scripts/release.sh` собирает агента под linux и darwin × amd64 и arm64 (`agent-<os>-<arch>`),
пишет подписанный `manifest.json` командой `agent manifest` (с `publicKey` — открытым ключом
подписи) и кладёт установщик `install.sh`. Так же собираются сборки для релиза на GitHub (CI по тегу). Сборки
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
  releasesDir: "/srv/agent-release", // manifest.json и сборки воркеров проекта
});
const view = await agents.release(); // ReleaseView | null — manifest.json, у каждой сборки — url
```

`release()` возвращает `manifest.json` каталога `releasesDir`: у каждой сборки (`artifacts`,
`workers`) — `url`, ссылка от корня бэкенда (или `file` как есть, если это абсолютная ссылка
`https://…`). Нет `releasesDir` или манифест не читается — `null` (второе — с записью в журнал).

`Agents` раздаёт без авторизации — сборки подписаны, секретов в них нет:

| Ссылка                                          | Что отдаёт                       |
| ----------------------------------------------- | -------------------------------- |
| `GET /api/v1/agent-link/releases/manifest.json` | `manifest.json` из `releasesDir` |
| `GET /api/v1/agent-link/releases/<file>`        | только файлы из манифеста        |

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
сервера), `--packages`, `--sysctl`, `--privileged`, `--ca-file`, `--token-file` вместо `--token`. `--privileged` нужен, только если
воркер настраивает сам узел: сеть, firewall, системные файлы. Флаги —
[ARCHITECTURE.md](../../docs/ARCHITECTURE.md#установка).

Способ установки — флаги `--rootless` и `--run` (по умолчанию — служба systemd от root):

```bash
curl -fsSL "$I" | sh -s -- --rootless --server … --token …   # без sudo, агент в ~/.agent
curl -fsSL "$I" | sh -s -- --run --server … --token …        # без установки, на переднем плане
```

`--rootless` — агент пользователя без root: команда без `sudo`, служба пользователя systemd (её
нет — ошибка: запускайте агента сами, `--run`); с ним нельзя `--sysctl`, `--privileged` и
пакеты. `--run` — без установки: агент запускается здесь же на
переднем плане (контейнер, свой менеджер процессов), себя не обновляет; с ним — только `--token`
или `--token-file`, `--name`, `--update-key`, `--ca-file` и `--releases`.

`install.sh` скачивает сборку агента под машину из каталога сборок `--releases` (по умолчанию —
с бэкенда, `GET /api/v1/agent-link/releases/…`: тогда в `manifest.json` из `releasesDir` нужны и
сборки агента, `artifacts`; или адрес релиза агента —
`--releases https://github.com/epifanovmd/agent/releases/download/v<версия>`), сверяет контрольную сумму и запускает `agent install` с этими флагами (с `--run` — `agent run`).
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

Агент раз в `update.checkInterval` (6 ч) смотрит последнюю версию в своём каталоге сборок и
сообщает новую в `hello` и `status`; в SDK это поле `update` агента — `{ latest, checkedAt }`, нет
поля — новее нет. Кого обновлять, решает бэкенд по `listAgents()`:

```ts
for (const a of await agents.listAgents()) {
  // update.latest — версия новее a.version; обновляет себя только агент с updateMode "self"
  if (!a.online || !a.update || a.hello?.agent.updateMode !== "self") continue;
  await agents.updateAgent(a.id, { version: a.update.latest }); // → { version, previous }
}
```

`updateAgent(id, { version })` отправляет `agent.update { version }`: агент сам берёт сборку этой
версии под свою платформу из своего каталога сборок и сверяет подпись; `releasesDir` для этого не
нужен. `version` обязательна, неверная — `MESSAGE_INVALID`. Агенты, которые себя не обновляют
(`hello.agent.updateMode` — `external` у агента в контейнере или `disabled`), ответят
`UPDATE_NOT_SUPPORTED`. Как агент поставлен на узел — `hello.agent.install` (`systemd`, `user`;
нет — запущен иначе, например в контейнере). На узле то же делает `agent upgrade`.

**Что уходит по сети.** `action {name: "agent.update", args: {version}}` → после запуска новой
версии — `action.result {ok, result: {version, previous}}`. Итог приходит уже в новом соединении;
срок ожидания — `updateTimeoutMs` (по умолчанию 5 мин).

**Агент** берёт `manifest.json` версии и сборку под свою платформу, сверяет подпись и sha256,
сохраняет текущую программу как `.prev`, заменяет её и перезапускается. Новая версия за три
запуска не получила `welcome` — возвращается `.prev`, итог — `UPDATE_FAILED`. Агент в контейнере
себя не обновляет — `UPDATE_NOT_SUPPORTED` (`update.mode: external`, обновляют образ; так же при
`update.mode: disabled`). Воркеры обновление агента не замечают: они работают дальше, новая версия
их подхватывает (`lifecycle.onAgentExit: keep`), — долгая работа не прерывается и ждать её
окончания не нужно.

## Обновление воркеров с сервера

Кандидаты — воркеры с `release: true`, для которых в `release()` есть сборка под ОС и процессор
агента с другой версией:

```ts
import semver from "semver";

const rel = await agents.release();
for (const a of await agents.listAgents()) {
  for (const w of a.workers.filter((w) => w.release)) {
    const [target] = (rel?.workers ?? [])
      .filter((b) => b.name === w.name && b.os === a.host?.os && b.arch === a.host?.arch)
      .map((b) => b.version)
      .sort(semver.rcompare);
    if (target && target !== w.version) await agents.updateWorker(a.id, w.name);
  }
}
// updateWorker → { version, previous, deferred: false } — обновлён;
//   { deferred: true, pending: "update", actionId } — воркер занят, итог — событие action
await agents.updateWorker(agentId, "report", { force: true }); // не ждать окончания работы воркера
await agents.updateWorker(agentId, "report", { wait: true }); // ждать итога и отложенной замены
```

`updateWorker` ставит новейшую сборку воркера под ОС и процессор агента из `releasesDir`;
переустановить ту же версию можно. Воркер, прописанный командой, — `WORKER_NOT_RELEASED`, нет
`releasesDir` или сборки — `UPDATE_NOT_AVAILABLE`.

**Агент** скачивает и проверяет сборку; если воркер занят (`GET /health` → `busy: true`), сразу
отвечает, что замена отложена, и ждёт окончания его работы (в `status` — `pending: "update"`, не
дольше 24 ч; `force: true` — не ждёт; итог — событие `action` с
`deferred: true`), останавливает прежний процесс, запускает новый и ждёт `ok: true` на
`GET /health` до минуты. Не дождался — возвращает прежнюю сборку, итог —
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
не может. В бэкенде агент становится `online: false`; после `revoke` — `revoked: true`.
