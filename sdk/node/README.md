# agent-sdk

SDK агента для Node.js (≥ 24): воркер на узле (`agent-sdk/worker`) и серверная часть
(`agent-sdk/server`) — регистрация агентов, задачи, команды, состояние, подписки, метрики,
выпуск. Пакет — ESM с типами TypeScript, единственная зависимость — `ws`.

Здесь — только то, что относится к Node. API обеих частей одинаково во всех языках:

- справочник API (Go, Node, Python) — [sdk/README.md](https://github.com/epifanovmd/agent/blob/main/sdk/README.md);
- с чего начать и разбор каждой возможности с примерами —
  [sdk/docs](https://github.com/epifanovmd/agent/blob/main/sdk/docs/README.md).

## Установка

Готовый архив из GitHub Release (TypeScript уже собран; `<версия>` — номер выпуска, например
`1.1.0`):

```bash
npm install https://github.com/epifanovmd/agent/releases/download/v<версия>/agent-sdk-<версия>.tgz
```

TypeScript без сборки: `node --import tsx main.ts`.

## Воркер (`agent-sdk/worker`)

```ts
import { AUTO_INTERVAL, CommandError, JobError, StateError, Worker } from "agent-sdk/worker";

const worker = new Worker({ name: "report", version: "1.0.0" }); // name по умолчанию — AGENT_WORKER

worker.job("example.render", { concurrency: 2 }, async (job) => {
  job.progress(0.5, "строю отчёт");
  if (job.signal.aborted) return; // отменили — итог не отправится
  if (!job.data?.reportId) throw new JobError("BAD_INPUT", "нет reportId", { retryable: false });
  const source = await job.inputPath("source");
  await job.upload("report", await build(source)); // Uint8Array или путь к файлу
  return { pages: 3 };
});

worker.command("example.report.reload", async (cmd) => {
  cmd.write("перечитываю\n");
  await reload({ signal: cmd.signal }); // срок истёк — signal сработает
  return { templates: 12 };
});

worker.state("example.report", async (version, spec) => {
  const failed = await applyTemplates(spec.templates);
  if (failed.length) throw new StateError("не применились шаблоны", { failed });
  return { applied: version };
});

// показатели с частотой подписки на канал (иначе — частоты метрик агента)
worker.telemetry("example.report", { intervalMs: AUTO_INTERVAL }, () => ({ queue: 0 }));

worker.on("context", (ctx) => console.error("связь:", ctx.online, "метрики раз в", ctx.metricsIntervalMs, "мс"));
worker.setHealth(false, "нет связи с базой"); // status.workers[].health, alert workerDegraded
worker.pause(["example.render"]); // не брать новые задачи очереди; resume — снова брать
worker.requestRestart("память выросла"); // попросить агента заменить воркер
worker.cleanup(async () => removeInterfaces()); // уборка при удалении агента с узла

await worker.run();
```

Особенности Node:

- отмена задачи и истёкший срок команды — `AbortSignal`: `job.signal`, `cmd.signal` (его можно
  передать в `fetch` и другие API Node); ещё — `job.cancelled`, `cmd.cancelled`;
- `job.stopRequested` — просят закончить пораньше;
- `worker.stopping` — `AbortSignal`: воркер останавливается, пора гасить свои фоновые циклы;
  `worker.drain()` — остановиться самому;
- `worker.context` — последний контекст агента (`mode`, `agent`, `online`, `metricsIntervalMs`,
  `channels`, `logLevel`), `worker.on("context", fn)` — при каждом изменении;
- `{ intervalMs: AUTO_INTERVAL }` (`"auto"`) у `telemetry` — частота подписки сервера на канал,
  иначе частота метрик агента, иначе 15 с; число — свой интервал в мс (по умолчанию 15000);
- ошибки — `JobError(code, message, { retryable })`, `CommandError(code, message)`,
  `StateError(message, report)`; `AgentError` — агент отклонил запрос воркера;
- `setHealth`, `pause`, `resume`, `requestRestart` можно вызывать и до `run()` — уйдут сразу
  после регистрации;
- `new Worker({ …, transport })` — свой канал связи (`Duplex`) для тестов;
  `worker.run({ signals: false })` — не ставить обработчики SIGTERM/SIGINT (по умолчанию
  ставит);
- `log: (level, msg) => …` — свой лог SDK; по умолчанию — stderr (агент пишет его в свой журнал).

## Сервер (`agent-sdk/server`)

```ts
import { createServer } from "node:http";
import { Agents } from "agent-sdk/server";

const agents = new Agents({ enrollToken: "demo-token" });

const server = createServer(async (req, res) => {
  if (await agents.handle(req, res)) return; // регистрация, HTTP sync, файлы задач, выпуск, install.sh
  res.writeHead(404).end();
});
agents.attach(server); // WebSocket агентов
server.listen(8080);

agents.on("job", (job) => console.log(job.id, job.status));
agents.on("alert", (a) => a.active && console.warn(a.type, a.agentName, a.message));

const job = await agents.enqueue({ queue: "example.render", data: { reportId: 42 } });
const [agent] = await agents.listAgents();
const cmd = await agents.call({ name: "example.report.reload", agentId: agent.id });
await agents.setState("example.report", { header: "ACME" });

// пока на узел смотрят — чаще и подробнее; продлевать тем же id
const sub = await agents.subscribe(agent.id, {
  ttlMs: 30_000,
  metrics: { intervalMs: 1000, groups: ["diskio", "sockets"] },
  logs: { level: "debug" },
  channels: { "example.report": { intervalMs: 1000 } },
});
await agents.unsubscribe(agent.id, sub.id);

await agents.pauseWorker(agent.id, "report", { queues: ["example.render"] });
await agents.resumeWorker(agent.id, "report");
```

Особенности Node:

- `agents.handle(req, res)` обслуживает маршруты агентов на обычном `node:http` и возвращает
  `true`, если запрос был для него; `agents.attach(server)` — WebSocket на том же сервере;
- `Agents` — типизированный `EventEmitter`: кроме `change`, `metrics`, `stateDeleted`, `audit`,
  `alert`, `log` есть события по видам с готовым объектом — `agent`, `job`, `command`, `state`,
  `stateApplied`, `event`;
- ошибки API — `AgentsError` с полями `code`, `status` (HTTP-статус) и `retryAfterSec` (у `429`);
- `alerts(): Promise<Alert[]>` читает активные проблемы из `Store`; `cancelCommand(id)`,
  `deleteAgent(agentId): Promise<void>` (только отозванного), `prune({jobsOlderThanMs?,
commandsOlderThanMs?, eventsOlderThanMs?}): Promise<number>`, `listJobs` / `listCommands` с
  `limit` и `after` — постранично;
- `MemoryFiles` — `EventEmitter` с событием `upload` (`{ jobId, name }`), содержимое —
  `files.get(jobId, "out", name)`;
- помощники для своего API: `readJSON(req)`, `readBody(req, limit?)` (больше предела —
  `AgentsError` со статусом 413), `sendJSON(res, status, body)`, `baseUrl(req, trustProxy?)`,
  `clientAddress(req, trustProxy?)` — заголовки `X-Forwarded-*` только с `trustProxy`.
