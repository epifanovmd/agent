# Стенд: сервер, агент, воркеры, сквозные тесты

Рабочий пример целиком:

- **сервер на Node.js** — небольшой сервер на `agent-sdk/server` с HTTP API ([API.md](API.md));
- **агент** — сам подключается к серверу и запускает **воркеры-примеры без SDK** на Python,
  Node.js и Go;
- **сквозные тесты** — тот же сервер в процессе теста и настоящий агент с этими воркерами.

Как работает SDK — [sdk/docs](../sdk/docs/README.md); как написать воркер —
[sdk/docs/workers.md](../sdk/docs/workers.md).

```
examples/
├── server/                сервер на agent-sdk/server (TypeScript через tsx, без сборки)
│   ├── src/app.ts         createApp: маршруты агентов и HTTP API (API.md)
│   ├── src/main.ts        запуск стенда: Agents, переменные окружения
│   ├── src/history.ts     история событий, метрик и действий в памяти
│   └── e2e/               сквозные тесты (node:test): стенд, сценарии, bare-worker.mjs
├── workers/
│   ├── echo/main.py       Python, только стандартная библиотека
│   ├── node-echo/main.mjs Node.js, только node:http — то же, что echo
│   ├── sysinfo/main.go    Go: сведения о процессе, настройка banner, метрики, событие
│   └── netprobe/          Go: проверка сети до целей из настройки targets, итоги — в метриках
├── agent.demo.yaml        настройки агента стенда
└── API.md                 HTTP API сервера
```

## Воркеры

Все четыре — обычные HTTP-сервисы на unix-сокете из `AGENT_WORKER_SOCKET`; события они
отправляют агенту (`POST /events` на `AGENT_SOCKET` с токеном `AGENT_WORKER_TOKEN`).

| Воркер      | Маршруты для запросов сервера                                                                                                                                   | Настройка                                                                                  | `GET /metrics`                           |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ | ---------------------------------------- |
| `echo`      | `POST /echo` — текст в верхнем регистре; `GET /stream?n=5` — ответ по частям; `GET /bytes?n=256` — двоичный ответ; задачи `POST /jobs`, `POST /hang` — см. ниже | `settings`: `{ prefix, upper }`; неверное значение — 400                                   | счётчики запросов и событий              |
| `node-echo` | то же                                                                                                                                                           | то же                                                                                      | то же                                    |
| `sysinfo`   | `GET /info` — процесс, версия Go, баннер                                                                                                                        | `banner`: `{ text }` (до 200 символов)                                                     | горутины, память, время работы           |
| `netprobe`  | `GET /results` — итог последнего круга; задача `netprobe.run` (`POST /jobs`) — проверить сейчас                                                                 | `targets`: `{ targets: [{ id, host, port?, method? }], intervalSec?, count?, timeoutMs? }` | итог последнего круга: `{ at, results }` |

- **Задачи `POST /jobs`** ([sdk/docs/workers.md](../sdk/docs/workers.md#задачи)) у `echo` и
  `node-echo`: `echo.quick { text, lookup? }` — итог сразу (`200 { result: { text } }`;
  `lookup: true` — префикс воркер спрашивает у бэкенда запросом `echo.lookup`, сервер стенда
  отвечает `<имя агента>: `); `echo.long
{ steps, delayMs, text? }` — долгая: сразу `202 { id }`, затем события `job.progress { jobId, id,
progress, message }` и итог `job.done { jobId, id, result: { text } }`. Пока задача идёт,
  `GET /health` отвечает `busy: true` — агент откладывает замену воркера до её окончания (на
  `restart` сервер сразу отвечает `deferred: true`). `GET /jobs/{id}` — состояние, `POST
/jobs/{id}/cancel` — прервать (событие `job.cancelled`). Ход каждой задачи `echo` хранит в
  `ECHO_JOBS_DIR` и, запущенный заново, продолжает с сохранённого шага. Перезапуск агента задачу
  не прерывает: воркер работает дальше. У `netprobe` — быстрая задача `netprobe.run { targets?,
count?, timeoutMs? }`: проверка сейчас, итог — `{ at, results }`.
- **`POST /hang`** — воркер «зависает»: перестаёт отвечать на `GET /health`, и агент перезапускает
  его.
- **`GET /health`** — у `echo` и `node-echo`: `ok`, `busy`, `message` и `info` (версия, pid, версия
  настройки, префикс); `ok: false`, если события не доходят до агента.
- **`POST /cleanup`** — уборка при удалении агента с узла: `echo` и `node-echo` удаляют свой файл
  `ECHO_STATE_FILE` (если задан — туда записывается применённая настройка), сбрасывают настройку и
  счётчики; `sysinfo` — баннер.
- **`GET /manifest`** — все четыре воркера описывают себя: версия, ключ настроек, маршруты, события,
  типы задач и запросы к бэкенду (`echo.lookup`) — со схемами ([API.md](API.md)). Без `GET /health`
  и `GET /manifest` агент воркер не регистрирует (`state: invalid`); маршрута не из манифеста агент
  к воркеру не пропускает (`ROUTE_UNDECLARED`).
- Метрики узла собирает встроенный воркер агента `sysmetrics` — в `agent.demo.yaml` его нет.

## Запуск

Нужны Node ≥ 24, python3 и Docker. Go ставить не нужно — он запускается в контейнере.

```bash
make demo-build          # один раз: агент, sysinfo и netprobe → .dev/bin, сборки → dist/<VERSION>,
                         # agent-sdk, зависимости сервера
make demo-server         # сервер: http://localhost:8080 (токен регистрации demo-token)
make demo-agent          # в другом терминале: агент demo-1 с четырьмя воркерами
scripts/demo.sh agent 2  # ещё один агент (свой каталог данных .dev/demo-agent-2)
scripts/demo.sh status   # состояние агента 1: связь, воркеры, последняя ошибка
```

Другой порт сервера — `PORT=8081` для `server` и `agent`. Работа со стендом — через
[HTTP API](API.md), например:

```bash
curl -s localhost:8080/api/agents                                        # агенты, их id
curl -X PUT localhost:8080/api/agents/<id>/configs/echo/settings -d '{"prefix":"> "}'
curl -X POST localhost:8080/api/agents/<id>/workers/echo/fetch/echo -d '{"text":"привет"}'
curl -N 'localhost:8080/api/agents/<id>/watch?logLevel=info'             # метрики и журнал вживую
```

| Переменная                                     | По умолчанию                            | Что                                                                      |
| ---------------------------------------------- | --------------------------------------- | ------------------------------------------------------------------------ |
| `PORT`                                         | `8080`                                  | порт                                                                     |
| `ENROLL_TOKEN`                                 | `demo-token`                            | токен регистрации агентов                                                |
| `RELEASES_DIR`                                 | `dist/<VERSION>` (в `make demo-server`) | каталог сборок; с `AGENT_RELEASES_*` — воркеры проекта                   |
| `AGENT_RELEASES_GITHUB`                        | —                                       | `owner/repo`: агент и netprobe из релизов GitHub (`agentReleases`)       |
| `AGENT_RELEASES_RANGE`, `AGENT_RELEASES_TOKEN` | `^<мажор SDK>`, —                       | диапазон версий и токен API GitHub                                       |
| `AGENT_RELEASES_URL`                           | —                                       | вместо GitHub: адрес каталога сборок (с `manifest.json`)                 |
| `AGENT_RELEASES_PROXY`                         | —                                       | `1` — сборки из источника узлам через сервер потоком, а не 302           |
| `AGENT_RELEASES_CHECK_INTERVAL_MS`             | `3600000`                               | как часто проверять новую версию                                         |
| `PUBLIC_KEY`, `UPDATE_PUBLIC_KEYS`             | —                                       | ключи проверки подписи (base64; второй — через запятую) для `install.sh` |
| `PUBLIC_URL`                                   | из запроса                              | адрес сервера для команды установки                                      |
| `STATUS_INTERVAL_MS`, `METRICS_INTERVAL_MS`    | `5000`                                  | как часто агенты шлют статус и метрики                                   |
| `METRICS_HISTORY_INTERVAL_MS`                  | `5000`                                  | как часто точка метрик попадает в историю (`0` — каждая)                 |
| `VALIDATE_CONFIGS`                             | —                                       | `1` — проверять настройки по схеме из манифеста воркера                  |

Это переменные сервера (`server/src/main.ts`). `scripts/demo.sh` задаёт `ENROLL_TOKEN` сам — из
`DEMO_TOKEN` (по умолчанию `demo-token`), один и тот же для сервера и агента; адрес сервера для
агента — `AGENT_SERVER_URL` (по умолчанию `http://localhost:$PORT`).

Ограничения примера: всё хранится в памяти (`MemoryStore`: перезапуск сервера — агенты
регистрируются заново), у API нет авторизации. История событий, метрик и действий — кольца в
памяти сервера стенда (`server/src/history.ts`): она пишется из `onEvent` и событий `Agents` —
так же бэкенд пишет её в свою БД. Настоящий бэкенд передаёт `Agents` свой `Store`
([sdk/docs/store.md](../sdk/docs/store.md)) и проверяет, кто делает запрос.

## Сквозные тесты

```bash
make e2e                 # сборки → .dev/e2e, затем тесты (около минуты)
scripts/e2e.sh build     # только сборки: агент, агент и netprobe следующей версии, agent-release, sysinfo, netprobe
scripts/e2e.sh test      # только тесты (сборки и sdk/node уже собраны: make node-sdk)
E2E_KEEP=1 scripts/e2e.sh test   # не удалять временные каталоги стендов (журналы agent.log, server.log)
```

Каждый файл `e2e/*.test.ts` поднимает свой стенд: сервер стенда (`createApp`) в процессе теста на
свободном порту и настоящий агент с отдельным каталогом данных во временном каталоге и воркерами
echo, node-echo, sysinfo и netprobe (у `registration.test.ts` — ещё `bare`). Файлы идут
параллельно, ожидания — опросом со сроком.

| Файл                   | Что проверяет                                                                                                                                                                                                                                                                                                                                                                                                       |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `link.test.ts`         | регистрация по токену, `hello` и `status` со всеми воркерами и `sysmetrics`, `health`; запросы к воркерам: `POST /echo`, потоковый и двоичный ответ, `sysinfo`, срок, отмена, служебный путь, неизвестный воркер; задачи `echo.quick`, `echo.long` (события `job.*`, состояние, отмена) и `netprobe.run`; метрики узла и воркеров, `watch` (раз в секунду), история; журнал с узла и живой журнал; `netprobe`       |
| `configs.test.ts`      | настройка применяется, неверная — `failed` с текстом воркера, следующая версия, удаление; повторная передача после перезапуска воркера и агента (без сервера, воркеры остановлены `agent stop-workers` — с диска агента); события при остановленном сервере доходят после восстановления; уборка `agent cleanup`                                                                                                    |
| `manifest.test.ts`     | манифест echo, node-echo, sysinfo и netprobe в статусе агента, версия воркера из манифеста; `supports` по манифесту; `validateConfigs` — неверная настройка отклонена сервером (`CONFIG_INVALID`) до отправки агенту, верная применяется                                                                                                                                                                            |
| `registration.test.ts` | обязательный минимум воркера: воркер без `GET /manifest` (`e2e/bare-worker.mjs`) — `invalid` с причиной, запрос — `WORKER_INVALID`, проблема `workerInvalid`; манифест появился — воркер зарегистрирован; событие не из манифеста отклонено агентом (`EVENT_UNDECLARED`); ключ настроек не из манифеста — `failed` `CONFIG_KEY_UNKNOWN`                                                                             |
| `longwork.test.ts`     | долгая задача `echo.long` переживает перезапуск агента (SIGTERM и запуск): тот же процесс, все шаги и итог дошли; `worker.restart` во время задачи — сразу `deferred` (`pending: restart`), замена и событие `action` — после её окончания; `force` — сразу, а `echo` продолжает задачу с сохранённого шага                                                                                                         |
| `detect.test.ts`       | обнаружение потери связи с настройками по умолчанию: упавший процесс воркера — новый `state` не позже 2 с; убитый агент (SIGKILL) — `offline` не позже 8 с, `lastSeenAt` — последняя весть                                                                                                                                                                                                                          |
| `actions.test.ts`      | сборки, подписанные тестовым ключом (`agent-release keygen`, `manifest`): кандидаты обновления, чужая подпись — отказ, обновление агента с перезапуском (тест перезапускает процесс, как служба) — работа `echo` при этом не прерывается; `worker.restart`; перезапуск зависшего воркера; смена ключа; отзыв и новая регистрация по токену                                                                          |
| `releases.test.ts`     | сборки агента с другого сервера (`agentReleases.url` — каталог на отдельном HTTP-сервере, как GitHub Releases; подпись ключом автора агента) и воркер проекта netprobe в `releasesDir` (подпись ключом проекта): итоговый список сборок с источниками, `install.sh` с ключами проекта и автора, сборка — `302`; агент с двумя ключами (`update.publicKeys`) обновляет netprobe из `releasesDir` и себя из источника |
| `strict.test.ts`       | строгие возможности: маршрут не из манифеста — `ROUTE_UNDECLARED`, тип задачи — `JOB_UNKNOWN` (от агента), тело не по схеме маршрута — `REQUEST_INVALID` (`validateRequests`); запрос воркера к бэкенду `echo.lookup` в задаче `echo.quick`; `waitEvent` — `echo.started` после перезапуска, `data` по схеме (`validateEvents: reject`); `capabilities`                                                             |
