# agent

**Агент для серверов: запускайте свои программы на узлах и управляйте ими из своего бэкенда — без
открытых портов, SSH и самописной доставки.**

[![CI](https://github.com/epifanovmd/agent/actions/workflows/ci.yml/badge.svg)](https://github.com/epifanovmd/agent/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/epifanovmd/agent)](https://github.com/epifanovmd/agent/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`agent` — одна программа на Go, которая ставится на каждый сервер (виртуальную машину, контейнер),
сама подключается к вашему бэкенду и запускает рядом с собой **воркеры** — программы вашего проекта
на любом языке. Бэкенд задаёт воркерам настройки (желаемое состояние), отправляет им команды,
получает итоги, события, метрики и журнал, обновляет агента и воркеры.

```
ваш бэкенд ◄──HTTPS: поток SSE вниз, POST вверх── agent ◄──HTTP по unix-сокету агента── воркеры
                                                    │
                соединение открывает агент, а к агенту — воркер: входящих портов на узле нет
```

Библиотек не нужно: бэкенд и воркер реализуют протокол сами, стандартными средствами своего языка.
Протокол — [docs/PROTOCOL.md](docs/PROTOCOL.md).

## Что делает агент

- **связь** — сам подключается к бэкенду (`GET /agent/v1/stream`, запасной путь — длинный опрос) и
  переподключается после обрыва; итоги и события ждут на диске и уходят, когда связь вернётся;
- **надзор** — запускает воркеры, следит за пульсом, перезапускает зависшие; воркеры переживают
  перезапуск и обновление агента; занятый воркер плановая замена ждёт;
- **доставка** — желаемое состояние (настройки воркеров с версиями) и команды вниз; итоги, ход,
  события, запросы воркеров к бэкенду, метрики узла и журнал вверх;
- **обновления** — агента и воркеров по подписанным сборкам (Ed25519) с откатом;
- **без бэкенда** (явно, `server.required: false`) — узел работает дальше: желаемое — из файлов рядом
  с `agent.yaml`, команды — через локальный API (`agent command`).

**Требования.** Агент — Linux или macOS, amd64 или arm64; службой — Linux с systemd, без systemd —
в контейнере или `agent run`. Воркер и бэкенд — на любом языке. Сборка из исходников — Docker и
python3.

## Быстрый старт

### 1. Бэкенд

Бэкенд реализует часть I протокола: регистрацию агента (`POST /agent/v1/enroll`), поток
`GET /agent/v1/stream` с желаемым и командами и приём `POST /agent/v1/{hello,state,results,events,
metrics,logs,requests}`. Каждый запрос агента, кроме регистрации, приходит с заголовком
`Authorization: Agent <agentId>.<secret>`. Минимальная реализация — §21
[протокола](docs/PROTOCOL.md); рабочий пример на Python без библиотек —
[examples/server/server.py](examples/server/server.py).

### 2. Агент

```bash
curl -fLo agent "https://github.com/epifanovmd/agent/releases/latest/download/agent-$(uname -s | tr A-Z a-z)-$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')"
chmod +x agent
```

Рядом — папка агента: программа, настройки и воркеры проекта.

```yaml
# agent.yaml — ./agent run читает его сам
envFiles: [.env] # AGENT_SERVER_URL=https://api.example.com, AGENT_ENROLL_TOKEN=<токен>
server:
  url: ${AGENT_SERVER_URL}
enroll:
  token: ${AGENT_ENROLL_TOKEN}
dataDir: .data
update:
  mode: disabled
workers:
  - path: workers/hello # папка воркера: агент запускает в ней ./run
```

### 3. Воркер

Воркер — программа в папке `workers/hello` с исполняемым `run` (и файлом `VERSION` для
`agent pack`; зависимости ставит необязательный исполняемый `setup` — агент выполняет его перед
первым запуском, у сборки — один раз на версию), HTTP-клиент сокета агента: агент передаёт ему `AGENT_SOCKET` и
`AGENT_WORKER_TOKEN`. Воркер регистрируется (`POST /register` с манифестом), держит поток
`GET /stream` (желаемое, команды, отмена), присылает итоги (`POST /results`), итоги настроек
(`POST /applied`) и пульс раз в 10 с (`POST /health`). На Go — пакет `worker`:

```go
w, err := worker.New(worker.Options{
	Version:  "1.0.0",
	Manifest: protocol.WorkerManifest{Commands: []protocol.ManifestCommand{{Type: "example.hello"}}},
	Commands: map[string]worker.CommandFunc{
		"example.hello": func(ctx context.Context, c *worker.Call) (any, error) {
			return map[string]string{"text": "привет"}, nil
		},
	},
})
if err != nil {
	log.Fatal(err)
}
log.Fatal(w.Run(ctx))
```

На другом языке — те же запросы стандартными средствами, например:

```bash
curl --unix-socket "$AGENT_SOCKET" -H "Authorization: Worker $AGENT_WORKER_TOKEN" \
  http://agent/register -d '{"version":"1.0.0","manifest":{"commands":[{"type":"example.hello"}]}}'
```

Примеры — [echo/main.py](examples/workers/echo/main.py) (Python без библиотек) и
[sysinfo/main.go](examples/workers/sysinfo/main.go) (Go).

### 4. Запуск

```bash
./agent config check   # проверить настройки и показать итоговые значения
./agent run            # Ctrl+C — остановить
./agent status         # в другом терминале: связь, воркеры
./agent command hello example.hello '{}'   # команда воркеру через локальный API
```

Весь стенд на своей машине (бэкенд, агент, воркеры на Python и Go) —
[examples/README.md](examples/README.md).

## Установка на узел

На узле — программа агента, один файл настроек, воркеры и то, что их запускает: служба systemd
(`Type=notify`, сторож `WatchdogSec=60`) или служба пользователя без root (`--rootless`); в
контейнере и под своим менеджером процессов — без установки (`install.sh --run`).
`…/install.sh` — установщик из релиза агента:
`https://github.com/epifanovmd/agent/releases/latest/download/install.sh`.

| Способ                    | Команда                                                                                                                        | Когда                                                    |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------- |
| Из папки агента           | `sudo ./agent install --env prod` в папке проекта                                                                              | агент ставится на ту же машину, где лежит папка          |
| Из архива                 | `agent pack --env prod` → на узле `tar xzf … && cd agent && sudo ./agent install`                                              | перенос вручную; узлу не нужен доступ к бэкенду и GitHub |
| Одной командой            | `curl -fsSL …/install.sh \| sudo sh -s -- --server https://api.example.com --token <токен>`                                    | сборка агента — из релиза на GitHub                      |
| Архив по ссылке           | `curl -fsSL …/install.sh \| sudo sh -s -- --bundle 'https://files.example.com/agent-prod.tar.gz' --token <токен>` | архив `agent pack` с любого хостинга                     |
| Только программа          | `sudo ./agent install --server https://api.example.com --token <токен> [--worker …]`                                           | без папки агента: настройки создаются по шаблону         |
| Без root                  | `./agent install --rootless --server https://api.example.com --token <токен>`                                                  | нет прав root: агент в `~/.agent`, служба пользователя   |
| Без установки (контейнер) | `curl -fsSL …/install.sh \| sh -s -- --run --bundle '…/agent-prod.tar.gz' --token <токен>`                                   | агент на переднем плане: контейнер, свой менеджер        |
| Образ Docker              | `docker run -e AGENT_SERVER_URL=… -e AGENT_ENROLL_TOKEN=… -v agent-data:/var/lib/agent ghcr.io/epifanovmd/agent:<версия>`      | узел — контейнер; `:<версия>-python` — с python3         |

Повторная установка обновляет программу и службу тем же способом и сохраняет ключ агента; новые
`--worker` дописываются в `agent.yaml`. Несколько агентов на одном узле (для разных бэкендов) —
`--instance ИМЯ` или `instance:` в настройках. Удаление — `sudo agent uninstall [--purge]`: агент
останавливает воркеры и выполняет их `./cleanup` — уборку созданного ими на узле. Все флаги — [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#установка).

## Настройки

Настройки — YAML; файл окружения ложится поверх общего (`extends`), секреты — в файлах переменных
(`envFiles`). Почти любой ключ можно задать переменной `AGENT_*`: `AGENT_SERVER_URL`,
`AGENT_ENROLL_TOKEN`, `AGENT_LOG_LEVEL`…

| Ключ                                        | Что                                                                                |
| ------------------------------------------- | ---------------------------------------------------------------------------------- |
| `server.url`                                | адрес бэкенда, только `https://`; обязателен, кроме `server.required: false`       |
| `server.required`                           | `true` — без адреса и, до регистрации, без токена агент не запускается             |
| `server.allowInsecure`                      | разрешить `http://` (стенд, локальная сеть)                                        |
| `server.caFile`                             | свой корневой сертификат TLS                                                       |
| `enroll.token`                              | токен регистрации; нужен только до первой регистрации                              |
| `dataDir`, `name`, `labels`                 | каталог данных агента, имя и метки узла                                            |
| `log.level`, `log.forward`                  | уровень журнала агента; с какого уровня записи уходят бэкенду                      |
| `update.mode`                               | `self` — агент обновляет себя, `external` — контейнер, `disabled`                  |
| `update.releases`, `update.checkInterval`   | каталог сборок агента (по умолчанию релизы на GitHub); как часто проверять (6 ч)   |
| `update.publicKey`, `update.publicKeys`     | ключи проверки подписи сборок, в дополнение к вшитому                              |
| `outbox.maxMessages`                        | сколько итогов и событий очередь на диске держит без связи                         |
| `workers[]`                                 | воркеры: `path` (папка с `./run`), `name` + `command`, `release: true` (сборка)    |
| `workers[].env`, `args`, `dir`, `lifecycle` | переменные, аргументы, каталог; `lifecycle.stopTimeout` — ожидание после `SIGTERM`, `lifecycle.setupTimeout` — срок `./setup` |

`./agent run` без флагов читает `agent.yaml` рядом с программой или в текущем каталоге; `--env prod`
— `agent.prod.yaml`; `-config ФАЙЛ` — любой файл. `SIGHUP` перечитывает настройки и файлы
желаемого. Все ключи и переменные — [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#настройки).

## Команды агента

| Команда                                    | Что делает                                                             |
| ------------------------------------------ | ---------------------------------------------------------------------- |
| `agent run [--env ИМЯ \| -config ФАЙЛ]`    | работать; `SIGHUP` — перечитать настройки                              |
| `agent config check [--env ИМЯ]`           | проверить настройки, показать итоговые значения                        |
| `agent status [--json]`                    | работает ли агент, связь, воркеры, последняя ошибка                    |
| `agent command ВОРКЕР ТИП [JSON]`          | команда воркеру через локальный API, вывод — итог                      |
| `agent restart [--worker ИМЯ] [--force]`   | перезапустить агента (воркеры работают дальше) или воркер              |
| `agent logs [-f] [-n N] [--worker ИМЯ]`    | журнал службы или последние записи журнала воркера                     |
| `agent pack [--env ИМЯ] [--platform …]`    | архив папки агента для узла                                            |
| `agent install [--env ИМЯ] [флаги]`        | поставить службой systemd или без root (`--rootless`)                  |
| `agent upgrade [--check] [--version X]`    | поставить новую версию агента из каталога сборок (подпись проверяется) |
| `agent manifest КАТАЛОГ ВЕРСИЯ [--worker]` | `manifest.json` каталога сборок агента и воркеров                      |
| `agent stop-workers [--cleanup]`           | остановить воркеры, оставшиеся после остановки агента; `--cleanup` — и их уборка |
| `agent uninstall [--purge]`                | удалить службу (с `--purge` — настройки и данные)                      |
| `agent version`, `agent help`              | версия, справка                                                        |

Локальный API, к которому ходят `status`, `command`, `restart --worker` и `logs --worker`, —
сокет `<dataDir>/run/admin.sock`, доступный только владельцу агента.

## Обновления и подписи

- **Агент** раз в `update.checkInterval` смотрит последнюю версию в своём каталоге сборок и
  сообщает её бэкенду в `state`. Ставит её `agent upgrade` на узле или команда бэкенда
  `agent.update {version}`; новая версия не вышла на связь — возвращается прежняя.
- **Воркеры** со сборкой (`release: true`) обновляет бэкенд командой `worker.update` — с проверкой
  подписи и откатом, если новая сборка не заработала.
- **Подписи** — Ed25519. Сборки агента подписаны ключом автора агента, он вшит в программу. Свои
  воркеры проект подписывает своим ключом (пару создаёт OpenSSL —
  [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#обновление), «Ключи подписи»); закрытый ключ
  (`AGENT_SIGNING_KEY`) нужен только `agent pack` и `agent manifest`.

## Документация

| Документ                                     | О чём                                                                 |
| -------------------------------------------- | --------------------------------------------------------------------- |
| [docs/PROTOCOL.md](docs/PROTOCOL.md)         | протокол бэкенд ↔ агент ↔ воркер — источник правды                    |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | устройство агента, папка агента, все настройки, установка, обновление |
| [examples/README.md](examples/README.md)     | стенд: бэкенд на Python, воркеры на Python и Go, сквозные тесты       |
| [CONTRIBUTING.md](CONTRIBUTING.md)           | правила разработки                                                    |
| [SECURITY.md](SECURITY.md)                   | как сообщить об уязвимости, секреты на узле                           |

## Разработка

Go ставить не нужно: цели `make` запускают его в контейнере `golang`. Список целей — `make help`.

```bash
make demo-build && make demo-server   # стенд: бэкенд на Python — http://localhost:8080/api
make demo-agent                       # в другом терминале: агент с воркерами-примерами
make check                            # все проверки перед коммитом
```

## Лицензия

MIT — [LICENSE](LICENSE).
