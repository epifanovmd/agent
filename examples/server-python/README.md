# Сервер стенда на Python

Бэкенд на `agent_sdk.server` ([sdk/docs](../../sdk/docs/README.md)), только стандартная
библиотека: свой небольшой HTTP-сервер на asyncio, один порт. API такое же, как у Node-примера
([examples/server](../server)), поэтому с ним работают тот же веб-интерфейс и та же самопроверка.
Описание API — [examples/API.md](../API.md).

```
examples/server-python/
├── main.py     # Agents и маршруты: агенты, файлы задач, API интерфейса, раздача интерфейса
├── live.py     # WebSocket /api/ws: снимок, изменения; метрики и лог агента для подписанных клиентов
├── http11.py   # небольшой HTTP/1.1-сервер на asyncio и WebSocket
├── fastapi_app.py  # отдельный пример: FastAPI и готовый ASGI-транспорт agent_sdk.server.asgi
└── tests/      # unittest
```

## Запуск

```bash
PORT=18090 ENROLL_TOKEN=demo-token python3 examples/server-python/main.py
```

Агенты подключаются к этому серверу только по HTTP. WebSocket для агентов здесь нет: сервер
отвечает `404`, и агент в режиме `auto` сам переходит на HTTP.

Агент стенда с теми же пятью воркерами — через `scripts/demo.sh`, указав адрес этого сервера:

```bash
make demo-build   # агент и sysinfo в .dev/bin, SDK для Node, интерфейс — один раз
AGENT_SERVER_URL=http://localhost:18090 AGENT_TRANSPORT=http scripts/demo.sh agent
```

С выпуском агента — чтобы работали обновления из интерфейса и `install.sh`:

```bash
make release   # dist/<VERSION>/: manifest.json, agent-<os>-<arch>, install.sh (подпись — AGENT_SIGNING_KEY)
RELEASES_DIR=dist/<VERSION> PUBLIC_KEY=<base64> PORT=18090 python3 examples/server-python/main.py
curl -fsSL http://localhost:18090/api/v1/agent-link/install.sh | sudo sh -s -- --token demo-token
```

Интерфейс — http://localhost:18090, если он собран (`make demo-build` или `cd examples/web && npm ci && npm run build`).
Самопроверка воркеров из терминала:

```bash
cd examples/web && DEMO_SERVER=http://localhost:18090 npm run selfcheck
```

| Переменная                                  | По умолчанию        | Что                                                                                                                                                                   |
| ------------------------------------------- | ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PORT`, `HOST`                              | `8080`, `0.0.0.0`   | адрес                                                                                                                                                                 |
| `ENROLL_TOKEN`                              | `demo-token`        | токен регистрации агентов                                                                                                                                             |
| `WEB_DIR`                                   | `examples/web/dist` | собранный интерфейс                                                                                                                                                   |
| `STATUS_INTERVAL_MS`, `METRICS_INTERVAL_MS` | `5000`              | как часто агенты шлют статус и метрики                                                                                                                                |
| `METRICS_STORE_INTERVAL_MS`                 | `5000`              | как часто точка метрик попадает в историю (`0` — каждая)                                                                                                              |
| `RELEASES_DIR`                              | —                   | каталог выпуска агента (`make release` → `dist/<VERSION>`): обновления и установка                                                                                    |
| `PUBLIC_KEY`                                | —                   | открытый ключ проверки подписи (base64), вписывается в `install.sh`                                                                                                   |
| `TRUST_PROXY`                               | —                   | `1` или `true` — сервер за прокси: адрес агента (`address` в записи агента) берётся из `X-Forwarded-For`, адрес сервера — из `X-Forwarded-Host` и `X-Forwarded-Proto` |

SDK берётся прямо из репозитория (`sdk/python` добавляется в `sys.path`); установленный пакет
`agent-sdk` тоже подойдёт.

Тесты — в `make examples-test` или отдельно:

```bash
python3 -m unittest discover -s examples/server-python/tests -t examples/server-python
```

## Маршруты

Для агентов (их обслуживает `Agents`):

| Маршрут                                                              | Что                                              |
| -------------------------------------------------------------------- | ------------------------------------------------ |
| `POST /api/v1/agent-link/enroll`                                     | регистрация агента                               |
| `POST /api/v1/agent-link/sync`                                       | обмен по HTTP                                    |
| `GET /api/v1/agent-link`                                             | `404` — WebSocket для агентов нет                |
| `GET`/`PUT /files/<jobId>/(in\|out)/<имя>`                           | файлы задач                                      |
| `GET /api/v1/agent-link/releases/manifest.json`, `…/releases/<file>` | выпуск агента                                    |
| `GET /api/v1/agent-link/install.sh`                                  | установщик с вписанными адресом сервера и ключом |

Тело запроса — не больше предела `agents.body_limit(path)`: регистрация — 64 КБ, остальное — 32 МБ.
Больше — ответ `413`, а тело сервер не читает.

Для интерфейса — всё из [API.md](../API.md): в том числе подписки на агента
(`/api/agents/:id/subscriptions`), пауза воркеров (`/api/agents/:id/workers/:name/pause`, `…/resume`)
и поток `/api/ws`. Ещё:

| Маршрут                         | Что                                                                                  |
| ------------------------------- | ------------------------------------------------------------------------------------ |
| `POST /api/commands/:id/cancel` | отменить команду → Command (`cancelled`); агенту — `cmd.cancel`, если она уже у него |
| `DELETE /api/agents/:id`        | удалить отозванного агента (запись и историю метрик) → `{}`; не отозван — `409`      |

Плюс `GET /*` — сам интерфейс (неизвестный путь отдаёт `index.html`).

## FastAPI

`fastapi_app.py` — небольшой бэкенд на FastAPI: маршруты агента (регистрация, HTTP sync, WebSocket,
выпуск, файлы задач) обслуживает готовое ASGI-приложение `agent_sdk.server.asgi.AgentsApp`, остальное —
FastAPI. В стенд и тесты не входит, нужны пакеты `fastapi` и `uvicorn`:

```bash
pip install fastapi uvicorn
cd examples/server-python && uvicorn fastapi_app:app --port 18090 --ws-ping-interval 20
```

## Ограничения

Это пример, а не готовый продукт:

- всё хранится в памяти;
- у API нет авторизации и TLS;
- WebSocket минимальный и нужен только интерфейсу.

Для настоящей работы нужны:

- свой `Store` (БД);
- свои ссылки на файлы, с подписью;
- полноценный веб-сервер с WebSocket — uvicorn с `agent_sdk.server.asgi` (как в `fastapi_app.py`),
  aiohttp или `agent_sdk.server.websockets_adapter`. `Agents` работает с любым из них.
