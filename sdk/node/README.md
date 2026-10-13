# agent-sdk

Серверная часть связи с агентами для Node.js (≥ 24): регистрация агентов, WebSocket,
запросы к воркерам на узлах (`fetch`), настройки воркеров с версиями, метрики, события,
наблюдение, встроенные действия (перезапуск и обновление воркеров,
обновление агента, смена ключа, журнал), несколько копий бэкенда с общим хранилищем и раздача
сборок воркеров. Пакет — ESM с типами TypeScript.

Зависимости: `ws` — WebSocket, `zod` — проверка входящих данных, `semver` — сравнение версий
сборок, `lru-cache` — учёт неудачных регистраций и повторов, `@cfworker/json-schema` —
проверка значений по схемам из манифеста воркера (`schemaProblems`).

Воркерам SDK не нужен: воркер — обычный HTTP-сервис на unix-сокете на любом языке.

- справочник API — [sdk/README.md](https://github.com/epifanovmd/agent/blob/main/sdk/README.md);
- с чего начать и разбор каждой возможности —
  [sdk/docs](https://github.com/epifanovmd/agent/blob/main/sdk/docs/README.md);
- формат сообщений — [sdk/spec](https://github.com/epifanovmd/agent/blob/main/sdk/spec/README.md).

## Установка

Готовый архив из релизов GitHub (GitHub Releases; TypeScript уже собран; `<версия>` — номер версии):

```bash
npm install https://github.com/epifanovmd/agent/releases/download/v<версия>/agent-sdk-<версия>.tgz
```

## Пример

```ts
import { createServer } from "node:http";
import { Agents } from "agent-sdk/server";

const agents = new Agents({ enrollToken: process.env.AGENT_ENROLL_TOKEN });

const server = createServer(async (req, res) => {
  if (await agents.handle(req, res)) return; // регистрация, сборки
  res.writeHead(404).end();
});
agents.attach(server); // WebSocket агентов
server.listen(8080);

agents.on("agent", a =>
  console.log(a.name, a.online ? "на связи" : "без связи"),
);

// Где-то в API бэкенда:
async function greet(agentId: string) {
  // Настройки воркеру: агент хранит их на диске и передаёт воркеру.
  await agents.setConfig(agentId, "echo", "main", { greeting: "привет" });
  // Запрос к воркеру — как fetch.
  const res = await agents.fetch(agentId, "echo", "/echo", {
    method: "POST",
    body: "{}",
  });
  return res.json();
}
agents.on("event", e => console.log(e.worker, e.type, e.data));
```

## Разработка

```bash
npm ci
npm run lint && npm run format:check && npm run typecheck && npm test && npm run build
```

Исправить замечания линтера и формат — `npm run lint:fix` и `npm run format`.

Лицензия — MIT.
