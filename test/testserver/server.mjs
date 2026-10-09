// Сервер для тестов агента (test/integration): Agents из agent-sdk/server в памяти и
// управляющий API для Go-тестов. Своей логики связи с агентами нет.
//
//   node test/testserver/server.mjs '<настройки JSON>'
//
// Настройки — опции Agents (enrollToken, statusIntervalMs, metricsIntervalMs, releasesDir…).
// Сервер слушает 127.0.0.1 на свободном порту и пишет в stdout одну строку {"url": "..."}.
// Он стоит за прокси теста: адрес для install.sh — из X-Forwarded-Host и X-Forwarded-Proto.
//
// Управляющий API: POST /test/<метод>, тело — массив аргументов, ответ — {"result": ...}
// или {"error": {"code", "message"}} с кодом 4xx/5xx. Методы — Agents (getAgent, setConfig,
// configStatus, watch, restartWorker…) и свои:
//   agents()                         — агенты с их событиями воркеров (onEvent, по порядку
//                                      прихода, повтор id не задваивается)
//   logs(agentId)                    — принятые записи журнала агента (сообщения log)
//   agentLogs(agentId, opts)         — Agents.logs: действие agent.logs (журнал на узле)
//   metrics(agentId)                 — все принятые точки метрик (событие metrics), по порядку
//   configEvents(agentId)            — события config (статусы ключей), по порядку
//   alerts(agentId)                  — события alert, по порядку
//   actions(agentId)                 — события action (итоги действий), по порядку
//   workerRequests(agentId)          — запросы воркеров к серверу (onWorkerRequest), по порядку:
//                                      example.ask — ответ {worker, echo: data, agent: имя};
//                                      example.slow — ответа нет, пока не истечёт срок;
//                                      example.deny — отказ EXAMPLE_DENIED
//   fetch(agentId, worker, path, init) — запрос к воркеру; init.body — строка, init.bodyBase64 —
//                                      двоичное тело, init.abortAfterMs — отменить запрос через
//                                      столько мс; ответ — {status, headers, body, bodyBase64,
//                                      chunks, firstChunkAt, endAt} (chunks — сколько кусков
//                                      прочитано, время — мс Unix) или {error: {code, message}} —
//                                      ошибка после заголовка
import http from "node:http";
import { Agents, AgentsError, readJSON, sendJSON } from "../../sdk/node/dist/server/index.js";

const options = JSON.parse(process.argv[2] ?? "{}");

/** Сколько записей каждого вида держит сервер (последние). */
const KEEP = 10_000;
const kept = {
  events: new Map(),
  logs: new Map(),
  metrics: new Map(),
  configEvents: new Map(),
  alerts: new Map(),
  actions: new Map(),
  workerRequests: new Map(),
};
const keep = (kind, agentId, items) => {
  const list = [...(kept[kind].get(agentId) ?? []), ...items];
  kept[kind].set(agentId, list.slice(-KEEP));
};

/** События воркеров: сохранить (повтор с тем же id — пропустить), затем Agents подтвердит. */
const onEvent = (e) => {
  if (!(kept.events.get(e.agentId) ?? []).some((x) => x.id === e.id)) keep("events", e.agentId, [e]);
};

/** Запросы воркеров к серверу: сохранить и ответить по типу. */
const onWorkerRequest = (req) => {
  keep("workerRequests", req.agentId, [
    { worker: req.worker, type: req.type, data: req.data, timeoutMs: req.timeoutMs },
  ]);
  if (req.type === "example.deny") throw new AgentsError("EXAMPLE_DENIED", "запрос отклонён", 403);
  if (req.type === "example.slow")
    return new Promise((resolve) => req.signal.addEventListener("abort", () => resolve(null)));
  return { worker: req.worker, echo: req.data ?? null, agent: req.agent.name };
};

const agents = new Agents({
  ...options,
  onEvent,
  onWorkerRequest,
  trustProxy: true,
  log: (msg, extra) => process.stderr.write(`${msg} ${extra ? JSON.stringify(extra) : ""}\n`),
});
agents.on("log", (e) => keep("logs", e.agentId, e.entries));
agents.on("metrics", (m) => keep("metrics", m.agentId, [m]));
agents.on("config", (c) => keep("configEvents", c.agentId, [c]));
agents.on("alert", (a) => keep("alerts", a.agentId, [a]));
agents.on("action", (a) => keep("actions", a.agentId, [a]));

/** Методы Agents, доступные тестам. */
const methods = new Set([
  "listAgents",
  "getAgent",
  "listAlerts",
  "revoke",
  "deleteAgent",
  "setConfig",
  "deleteConfig",
  "getConfig",
  "listConfigs",
  "configStatus",
  "watch",
  "unwatch",
  "restartWorker",
  "updateWorker",
  "updateAgent",
  "rotateKey",
  "updateCandidates",
  "workerUpdateCandidates",
  "installCommand",
  "runJob",
  "jobStatus",
  "cancelJob",
]);

/** Свои методы теста. */
const own = {
  async agents() {
    return (await agents.listAgents()).map((a) => ({ ...a, events: kept.events.get(a.id) ?? [] }));
  },
  logs: (agentId) => kept.logs.get(agentId) ?? [],
  agentLogs: (agentId, opts) => agents.logs(agentId, opts),
  metrics: (agentId) => kept.metrics.get(agentId) ?? [],
  configEvents: (agentId) => kept.configEvents.get(agentId) ?? [],
  alerts: (agentId) => kept.alerts.get(agentId) ?? [],
  actions: (agentId) => kept.actions.get(agentId) ?? [],
  workerRequests: (agentId) => kept.workerRequests.get(agentId) ?? [],
  async fetch(agentId, worker, path, init = {}) {
    const { bodyBase64, abortAfterMs, ...rest } = init;
    if (bodyBase64 !== undefined) rest.body = Buffer.from(bodyBase64, "base64");
    if (abortAfterMs !== undefined) rest.signal = AbortSignal.timeout(abortAfterMs);
    const res = await agents.fetch(agentId, worker, path, rest);
    const out = { status: res.status, headers: Object.fromEntries(res.headers), chunks: 0 };
    const parts = [];
    try {
      for await (const part of res.body ?? []) {
        if (out.chunks++ === 0) out.firstChunkAt = Date.now();
        parts.push(Buffer.from(part));
      }
    } catch (e) {
      out.error = { code: e?.code ?? "INTERNAL", message: String(e?.message ?? e) };
    }
    out.endAt = Date.now();
    const buf = Buffer.concat(parts);
    out.body = buf.toString("utf8");
    out.bodyBase64 = buf.toString("base64");
    return out;
  },
};

async function control(req, res, method) {
  try {
    const args = await readJSON(req);
    if (!Array.isArray(args)) throw new AgentsError("MESSAGE_INVALID", "тело — массив аргументов");
    let result;
    if (method in own) result = await own[method](...args);
    else if (methods.has(method)) result = await agents[method](...args);
    else throw new AgentsError("NOT_FOUND", `нет метода ${method}`, 404);
    sendJSON(res, 200, { result: result ?? null });
  } catch (e) {
    const code = e instanceof AgentsError ? e.code : "INTERNAL";
    const status = e instanceof AgentsError ? e.status : 500;
    sendJSON(res, status, { error: { code, message: String(e?.message ?? e) } });
  }
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url ?? "/", "http://localhost");
  if (req.method === "POST" && url.pathname.startsWith("/test/")) {
    await control(req, res, url.pathname.slice("/test/".length));
    return;
  }
  if (await agents.handle(req, res)) return;
  sendJSON(res, 404, { error: { code: "NOT_FOUND", message: url.pathname } });
});
agents.attach(server);

server.listen(0, "127.0.0.1", () => {
  const { port } = server.address();
  process.stdout.write(JSON.stringify({ url: `http://127.0.0.1:${port}` }) + "\n");
});

// Тест закрывает stdin при завершении — сервер останавливается сам.
process.stdin.on("end", () => process.exit(0));
process.stdin.resume();
for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, () => process.exit(0));
