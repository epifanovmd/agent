# Регистрация и связь

Как агент попадает в бэкенд, как держит связь, как меняется и отзывается его ключ и как
работать с несколькими процессами бэкенда. Справочник API — [sdk/README.md](../README.md),
формат сообщений — [sdk/spec §2–§5](../spec/README.md#2-подключение), образцы —
[connection.json](../spec/examples/connection.json).

Во всех разделах одна схема: **Бэкенд** (вызов на Node, Go, Python) → **Что уходит по сети** →
**Агент** (что делает на узле) → **Воркер** → **Результат** (что видно в бэкенде).

- [Регистрация по токену](#регистрация-по-токену)
- [Ограничение попыток регистрации](#ограничение-попыток-регистрации)
- [Метки агента](#метки-агента)
- [Связь: WebSocket и HTTP](#связь-websocket-и-http)
- [На связи ли агент](#на-связи-ли-агент)
- [Несколько адресов, прокси, TLS](#несколько-адресов-прокси-tls)
- [Смена ключа](#смена-ключа)
- [Отзыв агента](#отзыв-агента)
- [Несколько процессов бэкенда](#несколько-процессов-бэкенда)

## Регистрация по токену

Агент приходит с **токеном регистрации** и получает постоянный ключ `agentId.secret`. Токен
нужен только до первой регистрации. Как выпускать и проверять токены — решает бэкенд.

**Бэкенд.** Один общий токен — `enrollToken`; своя проверка — `enroll`: вернуть `null` —
отказ, вернуть `{ labels }` — принять и выдать агенту метки.

```ts
const agents = new Agents({
  enroll: async (token, { name, labels, host }) => {
    const t = await db.enrollTokens.find(token); // ваша таблица токенов
    if (!t || t.expiresAt < Date.now()) return null; // отказ — 401
    return { labels: { nodeId: t.nodeId } }; // выданные метки
  },
});
```

```go
agents := server.New(server.Options{
	Enroll: func(token string, info server.EnrollInfo) (map[string]string, bool) {
		nodeID, ok := lookupToken(token) // info.Name, info.Labels, info.Host — из запроса
		return map[string]string{"nodeId": nodeID}, ok
	},
})
```

```python
async def check(token: str, info: dict):   # функция или корутина
    node_id = await lookup_token(token)      # info["name"], info["labels"], info["host"]
    return {"labels": {"nodeId": node_id}} if node_id else None

agents = Agents(enroll=check)
```

`enroll` получает токен и то, что агент прислал о себе: имя, метки и сведения об узле
(`host`).

Запрос проверяется **до** токена: тело — не больше 64 КБ (больше — `413`, тело дальше не
читается), `token` и `name` — непустые строки, `name` — до 128 символов, `labels` — объект до 64
меток, ключ — непустая строка до 256 символов, значение — строка до 256 символов; иначе — `400
MESSAGE_INVALID`. Такие отказы считаются неудачными
попытками ([ниже](#ограничение-попыток-регистрации)).

**Что уходит по сети.** `POST /api/v1/agent-link/enroll` `{token, name, labels, host}` → `201
{agentId, secret}`; неверный токен — `401 AGENT_ENROLLMENT_TOKEN_INVALID`
([§5](../spec/README.md#5-регистрация-агента)). Дальше каждое подключение — с заголовком
`Authorization: Agent <agentId>.<secret>`.

**Агент** (`internal/identity`, `internal/app`):

- токен — из `enroll.token` (`AGENT_ENROLL_TOKEN`); имя — `name`, метки — `labels`;
- ключ сохраняется в `<dataDir>/credentials.json` с правами `0600`; есть файл — токен больше не
  нужен;
- сервер недоступен (сеть, `5xx`, `429`) — агент повторяет регистрацию с растущей паузой,
  перебирая адреса из `server.url`/`server.urls`; токен отклонён (`401`/`400`) — ошибка запуска;
- рядом создаётся ключ шифрования `<dataDir>/encryption.key` для секретов в состоянии
  ([state.md](state.md#секреты-в-снимке)).

**Воркер** не участвует. Id агента он видит в контексте: `context.agent.id` (пусто, пока агент
не зарегистрирован) — [workers.md](workers.md#контекст-воркера).

**Результат.** Новая запись `Agent` (`id`, `name`, `labels`, `enrolledAt`), событие `change`
`{kind: "agent"}` (в Node — ещё `agent`). В хранилище — только sha256 секрета (`secretHash`),
наружу он не отдаётся.

## Ограничение попыток регистрации

Чтобы токен нельзя было подбирать, неудачные регистрации с одного адреса ограничены.

**Бэкенд.**

```ts
new Agents({ enrollToken, enrollFailureLimit: 10, enrollFailureWindowMs: 60_000 });
```

```go
server.New(server.Options{EnrollToken: token, EnrollFailureLimit: 10, EnrollFailureWindow: time.Minute})
```

```python
Agents(enroll_token=token, enroll_failure_limit=10, enroll_failure_window_ms=60000)
```

По умолчанию — 10 неудач за минуту. Выключить: в Node и Python — `0`, в Go — отрицательное
значение (ноль в Go — значение по умолчанию).

Адрес клиента — тот же, что попадает в `agent.address`: адрес сокета, а с `trustProxy` —
первый адрес `X-Forwarded-For`. Без `trustProxy` за прокси все агенты приходят с адреса
прокси и делят один лимит. В Python адрес передаёт адаптер (`AgentsApp` — сам) или бэкенд:
`handle_enroll(body, remote=client_ip, forwarded_for=xff)`; без него все клиенты считаются
одним.

**Что уходит по сети.** После лимита — `429 ENROLL_RATE_LIMITED` с заголовком `Retry-After`
(секунды до конца окна).

**Агент** считает `429` временной ошибкой и повторяет регистрацию с растущей паузой.

**Воркер** не участвует.

**Результат.** В Node — `AgentsError` с `code: "ENROLL_RATE_LIMITED"`, `status: 429`,
`retryAfterSec`; в Python — `HttpReply` с `reply.headers["Retry-After"]`.

## Метки агента

Метки — пары «ключ — значение» (`zone: eu`), по ним бэкенд выбирает агентов.

**Бэкенд** ничего не вызывает: метки приходят сами. Выданные при регистрации (`enroll` →
`{labels}`) **главнее**: агент не может их переписать, иначе узел смог бы выдать себя за другой.

**Что уходит по сети.** `hello.labels` при каждом подключении
([§6.1](../spec/README.md#61-подключение)).

**Агент.** `labels` в `agent.yaml` или `AGENT_LABELS=k=v,…`. Правка `agent.yaml` и
`systemctl reload agent` (SIGHUP) — агент переподключается с новым `hello`, очередь важных
сообщений сохраняется.

**Воркер** видит метки в `context.agent.labels`.

**Результат.** `agent.labels` = метки из `hello` + выданные бэкендом поверх; изменение —
событие `change` `{kind: "agent"}`.

## Связь: WebSocket и HTTP

**Бэкенд** подключает маршруты агентов к своему HTTP-серверу — дальше `Agents` всё делает сам:

```ts
const server = createServer(async (req, res) => {
  if (await agents.handle(req, res)) return; // POST enroll, POST sync, /files/…, releases, install.sh
  yourApi(req, res);
});
agents.attach(server); // WebSocket GET /api/v1/agent-link
```

```go
mux := http.NewServeMux()
agents.Mount(mux) // или http.Handler: agents.Handler()
mux.HandleFunc("/api/", yourAPI)
```

```python
# ASGI-приложение маршрутов агентов (без зависимостей); остальное — в fallback (FastAPI и т. п.)
from agent_sdk.server.asgi import AgentsApp
app = AgentsApp(agents, fallback=api)  # или api.mount("/", AgentsApp(agents)) последним маршрутом
```

Подключение к фреймворкам:

- **Express** — маршруты агентов **до** `express.json()` и других разборщиков тела: `Agents`
  читает тело сам, с пределом.

  ```ts
  const app = express();
  app.use(async (req, res, next) => ((await agents.handle(req, res)) ? undefined : next()));
  app.use(express.json());
  const server = app.listen(8080);
  agents.attach(server); // WebSocket
  ```

- **Fastify** — отдать ответ `Agents` через `reply.hijack()`, тело не разбирать:

  ```ts
  fastify.addHook("onRequest", async (req, reply) => {
    if (!req.url.startsWith("/api/v1/agent-link") && !req.url.startsWith("/files/")) return;
    reply.hijack();
    if (!(await agents.handle(req.raw, reply.raw))) reply.raw.writeHead(404).end();
  });
  await fastify.listen({ port: 8080 });
  agents.attach(fastify.server);
  ```

- **NestJS** (Express внутри) — без глобального разбора тела, `Agents` — первым:

  ```ts
  const app = await NestFactory.create(AppModule, { bodyParser: false });
  app.use(async (req, res, next) => ((await agents.handle(req, res)) ? undefined : next()));
  app.use(express.json());
  await app.listen(8080);
  agents.attach(app.getHttpServer());
  ```

- **Python** без ASGI — по методу на маршрут; тело читайте не больше `agents.body_limit(path)`
  (регистрация — 64 КБ, остальное — `max_body`, 32 МБ):

  ```python
  reply = await agents.handle_enroll(body, remote=ip, forwarded_for=xff)  # POST …/enroll
  status, data = await agents.handle_sync(auth, body, request.is_disconnected,
                                          remote=ip, forwarded_for=xff)   # POST …/sync
  await agents.serve_websocket(auth, ws, remote=ip, forwarded_for=xff)   # WS …/agent-link
  ```

Интервалы, которые сервер задаёт агентам: `statusIntervalMs` (5000) и `metricsIntervalMs`
(15000); в Go — `StatusInterval`, `MetricsInterval`.

**Что уходит по сети** ([§2](../spec/README.md#2-подключение),
[§3](../spec/README.md#3-начало-разговора), [§4](../spec/README.md#4-конверт-и-способы-доставки)):

- WebSocket `GET /api/v1/agent-link`, канал `agent.v1`; ключ проверяется до открытия (`401`,
  `403`, `426`);
- первое сообщение агента — `hello` (версии, сведения об узле, метки, возможности, задачи),
  ответ — `welcome` (`agentId`, `sessionId`, `serverTime`, `config`);
- запасной путь — `POST /api/v1/agent-link/sync` пачками; сервер держит запрос до 25 с, пока
  ему нечего отдать;
- сообщения агента — трёх видов: обычные (с `seq`, подтверждает `ack{seq}`), важные (с `id`,
  подтверждает `ack{ids}`), запросы (ответ с `re`).

**Агент** (`internal/link`, `internal/outbox`, `internal/stream`):

- `server.transport: auto` (по умолчанию) — WebSocket; если прокси не пропускает upgrade, —
  HTTP, и раз в 10 минут снова пробует WebSocket; `ws` или `http` — только он;
- ping/pong раз в 20 с, нет ответа 10 с — переподключение; нет `welcome` за 15 с — тоже;
- переподключение с растущей паузой; по коду закрытия: `1012` (сервер перезапускается) — сразу,
  `4410` (подключилась другая копия) — через 30 с, `4409` (нет общей версии) — с наибольшей
  паузой, `4401` — см. [отзыв](#отзыв-агента);
- **важные** сообщения (итоги задач и команд, события, отчёты о состоянии) лежат файлами в
  `<dataDir>/outbox`, пока сервер не подтвердит, и переживают перезапуск агента; отклонённые с
  `retryable: true` агент повторяет через 30 с;
- **обычные** (статус, метрики, прогресс, вывод команд, лог) — в памяти, до 2000 штук, и
  досылаются после переподключения;
- без связи агент работает как обычно: задачи доделываются, итоги копятся на диске.

**Воркер** связи с сервером не видит и не держит. Есть ли связь — `context.online`
([workers.md](workers.md#контекст-воркера)).

**Результат.** `agent.online`, `agent.transport` (`ws` | `http`), `agent.lastSeenAt`,
`agent.hello`, `agent.capabilities`, `agent.status`; событие `change` `{kind: "agent"}`.

## На связи ли агент

**Бэкенд.**

```ts
new Agents({ enrollToken, offlineGraceMs: 20_000, trustProxy: true });
agents.on("alert", (a) => a.type === "offline" && notify(a));
```

```go
server.New(server.Options{EnrollToken: token, OfflineGrace: 20 * time.Second, TrustProxy: true, OnAlert: notify})
```

```python
agents = Agents(enroll_token=token, offline_grace_ms=20000, trust_proxy=True)
agents.on("alert", notify)
```

- `offlineGraceMs` (20 с) — сколько агент после обрыва ещё считается на связи: успел
  переподключиться — никто ничего не заметил;
- `offlineAfterMs` — для нескольких процессов бэкенда: агент «на связи» по записи, но без
  сессии в этом процессе и без вестей дольше этого срока (процесс с его сессией упал) — агент
  без связи. По умолчанию `max(3 × statusIntervalMs, 30 с) + offlineGraceMs`;
- `trustProxy` — бэкенд за доверенным прокси: адрес агента — первый из `X-Forwarded-For`, адрес
  сервера для ссылок — из `X-Forwarded-Host` и `X-Forwarded-Proto` (иначе адрес сокета, `Host` и
  TLS соединения; заголовки подделывает любой клиент).

**Что уходит по сети.** `status` при каждом изменении и не реже `statusIntervalMs` — это и
«я жив».

**Агент** шлёт `status` и `metrics` по своим циклам (`internal/runtime`).

**Воркер** не участвует.

**Результат.** `agent.online`, `agent.lastSeenAt`, `agent.address` (IP последнего
подключения, без порта); уведомление `alert` `offline` — начало при потере связи, конец при
возвращении ([events.md](events.md#уведомления-о-проблемах)).

## Несколько адресов, прокси, TLS

Всё это — настройки агента; бэкенд в них не участвует.

**Бэкенд.** Для своего сертификата и клиентских сертификатов (mTLS) — настройки вашего
HTTPS-сервера или балансировщика. В команду установки можно передать файл корневого
сертификата: `installCommand({ …, caFile: "/etc/ssl/example-ca.pem" })` →
`--ca-file`.

**Что уходит по сети** — то же самое, только к другому адресу или через прокси.

**Агент** (`internal/link`, `internal/app/proxy.go`, `internal/config`):

```yaml
server:
  urls: [https://a.example.com, https://b.example.com] # один бэкенд, несколько адресов
  caFile: /etc/agent/ca.pem # свой корневой сертификат в дополнение к системным
  certFile: /etc/agent/client.pem # клиентский сертификат — вместе с keyFile
  keyFile: /etc/agent/client.key
```

- **несколько адресов** — агент подключается к первому доступному; при отказе (сеть, нет
  ответа, `5xx`) сразу переходит к следующему, после полного круга — пауза. Закрытие кодом или
  ответ `4xx` адрес не меняют: сервер жив. Ключ агента один на все адреса;
- **прокси** — `HTTPS_PROXY` / `HTTP_PROXY` / `NO_PROXY` (и строчные), читаются при запуске;
  localhost и петлевые адреса — всегда напрямую;
- **TLS** — системные корни плюс `caFile`; воркеры получают путь в `AGENT_SERVER_CA_FILE` (Node —
  ещё в `NODE_EXTRA_CA_CERTS`), чтобы скачивать файлы задач;
- адрес `http://` не на эту машину — агент при старте предупреждает: ключ и состояние идут
  открытым текстом.

`server.*` меняются только перезапуском агента. Все ключи — в
[docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md#настройки).

**Воркер.** SDK воркера сам берёт `AGENT_SERVER_CA_FILE` для загрузки файлов задач.

**Результат** — `agent.online`, `agent.address`; адрес, через который агент пришёл, SDK
использует в ссылках на файлы задач и в `install.sh`, если не задан `baseUrl`.

## Смена ключа

Секрет агента меняется без новой регистрации, id остаётся прежним. Новый секрет придумывает
сам агент и присылает только его хеш — секрет не попадает ни в БД, ни в историю команд.

**Бэкенд.**

```ts
const cmd = await agents.rotateKey(agentId); // команда agent.rotateKey, срок 60 с
```

```go
cmd, err := agents.RotateKey(agentID)
```

```python
cmd = await agents.rotate_key(agent_id)
```

Ошибки: `AGENT_NOT_FOUND`, `AGENT_REVOKED`, `COMMAND_NOT_SUPPORTED` (агент не объявил команду —
выключена в `commands.disabled`).

**Что уходит по сети** ([§5](../spec/README.md#5-регистрация-агента)):
`cmd.run {name: "agent.rotateKey"}` → `cmd.done {ok: true, result: {secretHash}}`; сервер
запоминает хеш как ожидающий и закрывает соединение кодом `1012`.

**Агент** (`internal/identity`): создаёт новый секрет, записывает его в `credentials.json` как
ожидающий **до** ответа и возвращает sha256. Следующее подключение — с новым секретом: сервер
его признаёт, агент делает его основным и стирает прежний. Новый не принят (`401`) — агент
подключается с прежним, связь не теряется.

**Воркер** не участвует.

**Результат.** `Command` со `status: "succeeded"`, `result.secretHash`; у записи агента
`pendingSecretHash` → `secretHash` при первом входе с новым секретом. Аудит `agent.rotateKey`.

## Отзыв агента

**Бэкенд.**

```ts
await agents.revoke(agentId); // → Agent с revoked: true
```

```go
err := agents.Revoke(agentID)
```

```python
agent = await agents.revoke(agent_id)
```

`Agents` закрывает сессию кодом `4401`, удаляет подписки и ожидающий ключ, заканчивает все
уведомления о проблемах агента. Дальше WebSocket и HTTP отвечают `401`.

**Что уходит по сети.** Закрытие `4401`; последующие подключения — `401`.

**Агент** (`internal/link`, `internal/app`): получил `4401`/`401` — если есть ожидающий секрет,
пробует прежний; иначе удаляет `credentials.json` и регистрируется заново, если в настройках
есть `enroll.token`. Токена нет — ждёт с наибольшей паузой и пишет ошибку в лог.

**Воркер** не участвует (агент продолжает его держать).

**Результат.** `agent.revoked: true`, `online: false`; аудит `agent.revoke`. Повторная
регистрация — **новый** агент с новым id.

Отозванного агента можно удалить совсем — `deleteAgent(agentId)` (Go `DeleteAgent`, Python
`delete_agent`): из `Store` уходят запись агента и его история метрик, задачи, команды, события
и снимки состояния остаются. Не отозванного удалить нельзя — `AGENT_NOT_REVOKED`; аудит
`agent.delete`.

## Несколько процессов бэкенда

Агент подключён к одному процессу, и только этот процесс может ему что-то отправить. Все
процессы работают с одним `Store` ([store.md](store.md)): записи меняются условно (по `rev`),
поэтому задача не выдаётся двум агентам, а запись одного процесса не затирает отзыв, подписки
или ключ, записанные другим.

**Балансировщик.** WebSocket — одно долгое соединение, с ним ничего настраивать не нужно.
HTTP-канал (`POST …/sync`) — это сессия из многих запросов: они должны приходить в **тот же**
процесс («липкие» сессии по заголовку `Authorization` или по адресу клиента). Без этого каждый
запрос к другому процессу открывает новую сессию, и агент получает сообщения с задержкой и
повторами.

**Бэкенд.** Процесс, изменивший данные (задача, команда, состояние, подписка, отзыв), сообщает
остальным своим способом, и каждый вызывает `refresh`: отправит тот, к кому подключён агент,
двойной отправки не будет.

```ts
agents.on("change", (c) => pg.query("SELECT pg_notify('agents', $1)", [c.id]));
pg.on("notification", () => void agents.refresh()); // или refresh(agentId)
```

```go
agents := server.New(server.Options{Store: pgStore, OnChange: func(c server.Change) { notify(c) }})
onNotify(func() { agents.Refresh("") }) // "" — все агенты этого процесса
```

```python
agents.on("change", lambda c: notify(c))
await agents.refresh()          # или refresh(agent_id)
```

`refresh` делает в этом процессе:

- доставляет ждущие команды и новые снимки состояния агентам этого процесса;
- раздаёт задачи из очереди;
- применяет подписки из записи агента (сводная — агенту, если изменилась);
- закрывает сессию отозванного в другом процессе агента (`4401`);
- будит ждущие `call`: итог команды мог сохранить другой процесс;
- отправляет `cmd.cancel` агентам этого процесса по командам, отменённым в другом процессе, —
  отправленным агенту в этой сессии или выполнявшимся, когда агент подключился (один раз,
  [commands.md](commands.md#отменить-команду)).

Сроки задач и команд каждый процесс проверяет по БД сам, раз в секунду. Номер последнего
принятого сообщения агента (`lastSeq`) и активные уведомления о проблемах тоже лежат в записи
агента: повтор после переподключения к другому процессу не обрабатывается второй раз, а
`alerts()` в любом процессе отдаёт одно и то же.

**Что уходит по сети** — то, что накопилось: `cmd.run`, `state.put`, `job.assign`, `config`.

**Агент** ничего не знает о процессах бэкенда.

**Воркер** не участвует.

**Результат.** `call` в любом процессе возвращает итог команды: он ждёт событие своего
процесса, раз в секунду проверяет `Store` и перечитывает итог по каждому `refresh` — но не
дольше срока команды + 20 с ([commands.md](commands.md#команда-с-ожиданием-итога)). Агент без
сессии где-либо дольше `offlineAfterMs` становится `online: false`.
