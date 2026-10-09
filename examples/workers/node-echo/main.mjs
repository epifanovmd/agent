// Воркер node-echo — пример воркера без SDK на Node.js (только node:http, Node ≥ 18).
// HTTP-сервис на unix-сокете AGENT_WORKER_SOCKET (sdk/spec §12):
//
//   POST /echo             {"text": "…"} → {"text": "<префикс>ТЕКСТ"}
//   GET  /stream?n=5       ответ по частям: n строк с паузой
//   GET  /bytes?n=256      двоичный ответ: n байт 0, 1, …, 255, 0, …
//   POST /hang             «зависнуть»: GET /health больше не отвечает (агент перезапустит воркер)
//   POST /jobs             задачи (§12): echo.quick {text} → 200 {result: {text}};
//                          echo.long {steps, delayMs, text?} → 202 {id}, события job.progress и
//                          job.done; пока задача идёт, GET /health отвечает busy: true
//   GET  /jobs/{id}        состояние задачи; POST /jobs/{id}/cancel — прервать (job.cancelled)
//   PUT  /config/settings  {version, data: {"prefix", "upper"}}; неверное — 400 {message}
//   DELETE /config/settings, GET /metrics, GET /health, POST /cleanup
//   GET  /manifest         что воркер умеет: версия, ключ settings со схемой, маршруты, события
//
// События — POST /events на AGENT_SOCKET с токеном AGENT_WORKER_TOKEN.
// ECHO_STATE_FILE (необязательно) — файл на узле с применённой настройкой: пример того, что воркер
// создаёт на узле и убирает при POST /cleanup.
import { createServer, request } from "node:http";
import { randomUUID } from "node:crypto";
import { rmSync, writeFileSync } from "node:fs";

const VERSION = "1.0.0";
const DEFAULTS = { prefix: "", upper: true };
const MAX_PREFIX = 64;

// Манифест (sdk/spec §12): по нему сервер знает, что умеет воркер, и может проверить настройку
// по схеме до отправки агенту.
const MANIFEST = {
  version: VERSION,
  description: "Эхо на Node.js: текст, потоковый и двоичный ответ, быстрые и долгие задачи",
  configs: [
    {
      key: "settings",
      description: "Как отвечать: префикс и верхний регистр",
      schema: {
        type: "object",
        properties: { prefix: { type: "string", maxLength: MAX_PREFIX }, upper: { type: "boolean" } },
        additionalProperties: false,
      },
    },
  ],
  routes: [
    { method: "POST", path: "/echo", description: "Текст с префиксом" },
    { method: "GET", path: "/stream", description: "Ответ по частям, ?n= строк" },
    { method: "GET", path: "/bytes", description: "Двоичный ответ, ?n= байт" },
    { method: "POST", path: "/hang", description: "Зависнуть: GET /health больше не отвечает" },
  ],
  events: [{ type: "echo.started", description: "Воркер запущен" }],
  jobs: [
    {
      type: "echo.quick",
      description: "Текст с префиксом — итог сразу",
      schema: { type: "object", properties: { text: { type: "string" } } },
    },
    {
      type: "echo.long",
      description: "Долгая задача: шаги с паузой, ход — job.progress, итог — job.done",
      schema: {
        type: "object",
        properties: {
          steps: { type: "integer", minimum: 1, maximum: 100 },
          delayMs: { type: "integer", minimum: 0, maximum: 10_000 },
          text: { type: "string" },
        },
      },
    },
  ],
};
let settings = { ...DEFAULTS };
let settingsVersion = 0;
const counters = { requests: 0, echoed: 0, streamed: 0, jobs: 0, events: 0, eventErrors: 0 };
let lastEventError = "";
let hung = false;
// Долгие задачи: id → {id, jobId, steps, step, state, result?}; пока идёт хоть одна — busy
// (агент не заменяет воркер).
const jobs = new Map();
const running = () => [...jobs.values()].filter((j) => j.state === "running").length;
const jobStatus = (j) => ({
  id: j.id,
  state: j.state,
  progress: j.step / j.steps,
  ...(j.result ? { result: j.result } : {}),
});
const STATE_FILE = process.env.ECHO_STATE_FILE ?? "";

/** Применённая настройка — в ECHO_STATE_FILE (если задан). */
const saveState = () => {
  if (STATE_FILE) writeFileSync(STATE_FILE, JSON.stringify({ version: settingsVersion, settings }));
};

/** Запрос к агенту по unix-сокету AGENT_SOCKET → { status, body }. */
function agent(method, path, body) {
  return new Promise((resolve, reject) => {
    const raw = body === undefined ? undefined : JSON.stringify(body);
    const req = request(
      {
        socketPath: process.env.AGENT_SOCKET,
        method,
        path,
        timeout: 10_000,
        headers: {
          authorization: `Bearer ${process.env.AGENT_WORKER_TOKEN}`,
          ...(raw ? { "content-type": "application/json" } : {}),
        },
      },
      (res) => {
        let data = "";
        res.on("data", (c) => (data += c));
        res.on("end", () => resolve({ status: res.statusCode, body: data ? JSON.parse(data) : null }));
      },
    );
    req.on("timeout", () => req.destroy(new Error("агент не ответил")));
    req.on("error", reject);
    req.end(raw);
  });
}

/** Событие серверу; ошибка видна в /health. */
async function event(type, data) {
  try {
    const { status, body } = await agent("POST", "/events", { type, data });
    if (status !== 202) throw new Error(`HTTP ${status}: ${body?.message ?? ""}`);
    counters.events++;
    lastEventError = "";
  } catch (e) {
    counters.eventErrors++;
    lastEventError = e.message;
    console.error(`событие ${type} не отправлено: ${e.message}`);
  }
}

const transform = (text) => settings.prefix + (settings.upper ? text.toUpperCase() : text);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function validate(data) {
  if (!data || typeof data !== "object" || Array.isArray(data)) return "data: нужен объект {prefix, upper}";
  const unknown = Object.keys(data).filter((k) => !(k in DEFAULTS));
  if (unknown.length) return `неизвестные поля: ${unknown.join(", ")}`;
  if ("prefix" in data && (typeof data.prefix !== "string" || data.prefix.length > MAX_PREFIX))
    return `prefix: строка до ${MAX_PREFIX} символов`;
  if ("upper" in data && typeof data.upper !== "boolean") return "upper: true или false";
  return null;
}

const json = (res, status, body) => {
  if (body === undefined) return res.writeHead(status).end();
  res.writeHead(status, { "content-type": "application/json" }).end(JSON.stringify(body));
};

async function readJSON(req) {
  let data = "";
  for await (const c of req) data += c;
  return data ? JSON.parse(data) : null;
}

async function route(req, res) {
  const url = new URL(req.url, "http://worker");
  const { pathname: path } = url;
  const method = req.method;

  if (method === "GET" && path === "/health") {
    if (hung) return; // «завис»: ответа не будет, пока агент не перезапустит воркер
    const info = { version: VERSION, pid: process.pid, settingsVersion, prefix: settings.prefix };
    return json(
      res,
      200,
      lastEventError
        ? { ok: false, busy: running() > 0, message: `события не доходят: ${lastEventError}`, info }
        : { ok: true, busy: running() > 0, message: running() ? `идёт задач: ${running()}` : "работаю", info },
    );
  }
  if (method === "GET" && path === "/manifest") return json(res, 200, MANIFEST);
  if (method === "GET" && path === "/metrics") return json(res, 200, { ...counters, settingsVersion });
  if (method === "POST" && path === "/cleanup") {
    settings = { ...DEFAULTS };
    settingsVersion = 0;
    for (const k of Object.keys(counters)) counters[k] = 0;
    if (STATE_FILE) rmSync(STATE_FILE, { force: true });
    return json(res, 204);
  }
  if (path.startsWith("/config/")) {
    const key = path.slice("/config/".length);
    if (method === "DELETE") {
      if (key === "settings") {
        [settings, settingsVersion] = [{ ...DEFAULTS }, 0];
        saveState();
      }
      return json(res, 204);
    }
    if (method !== "PUT") return json(res, 405, { message: "PUT или DELETE" });
    if (key !== "settings") return json(res, 400, { message: `ключ ${key} не поддерживается: есть только settings` });
    const body = await readJSON(req);
    const err = validate(body?.data);
    if (err) return json(res, 400, { message: err });
    settings = { ...DEFAULTS, ...body.data };
    settingsVersion = body.version ?? 0;
    saveState();
    console.log(`настройки применены: версия ${settingsVersion}`, settings);
    return json(res, 204);
  }

  if (method === "POST" && path === "/hang") {
    hung = true;
    console.log("завис: GET /health больше не отвечает");
    return json(res, 202, { hung: true });
  }
  counters.requests++;
  if (method === "POST" && path === "/echo") {
    const body = await readJSON(req);
    counters.echoed++;
    return json(res, 200, { text: transform(String(body?.text ?? "")) });
  }
  if (method === "GET" && path === "/stream") {
    const n = Math.max(1, Math.min(Number(url.searchParams.get("n") ?? 5) || 5, 100));
    counters.streamed++;
    res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
    for (let i = 1; i <= n && !res.destroyed; i++) {
      res.write(`${transform(`строка ${i} из ${n}`)}\n`);
      await sleep(300);
    }
    return res.end();
  }
  if (method === "GET" && path === "/bytes") {
    const n = Math.max(0, Math.min(Number(url.searchParams.get("n") ?? 256) || 0, 1 << 20));
    const raw = Buffer.alloc(n);
    for (let i = 0; i < n; i++) raw[i] = i % 256;
    res.writeHead(200, { "content-type": "application/octet-stream", "content-length": n });
    return res.end(raw);
  }
  if (method === "POST" && path === "/jobs") {
    const { type, jobId = "", data = {} } = (await readJSON(req)) ?? {};
    const text = String(data?.text ?? "");
    if (type === "echo.quick") {
      counters.echoed++;
      return json(res, 200, { result: { text: transform(text) } });
    }
    if (type !== "echo.long") return json(res, 400, { message: `задачи ${type} нет: есть echo.quick и echo.long` });
    const steps = Number(data.steps ?? 5);
    const delayMs = Number(data.delayMs ?? 500);
    if (!(steps >= 1 && steps <= 100) || !(delayMs >= 0 && delayMs <= 10_000))
      return json(res, 400, { message: "steps — от 1 до 100, delayMs — до 10000" });
    // Повтор с тем же jobId — та же задача.
    const same = jobId && [...jobs.values()].find((j) => j.jobId === jobId);
    if (same) return json(res, 202, { id: same.id });
    const job = { id: randomUUID().slice(0, 12), jobId, steps, step: 0, state: "running" };
    jobs.set(job.id, job);
    counters.jobs++;
    const ids = { jobId, id: job.id };
    void (async () => {
      while (job.state === "running" && job.step < steps) {
        await sleep(delayMs);
        if (job.state !== "running") return;
        job.step++;
        await event("job.progress", { ...ids, progress: job.step / steps, message: `шаг ${job.step} из ${steps}` });
      }
      if (job.state !== "running") return;
      job.state = "done";
      job.result = { text: transform(text || `готово: ${steps} шагов`) };
      await event("job.done", { ...ids, result: job.result });
    })();
    return json(res, 202, { id: job.id });
  }
  const jobPath = /^\/jobs\/([^/]+)(\/cancel)?$/.exec(path);
  if (jobPath) {
    const job = jobs.get(jobPath[1]);
    if (!job) return json(res, 404, { message: "нет такой задачи" });
    if (method === "GET" && !jobPath[2]) return json(res, 200, jobStatus(job));
    if (method === "POST" && jobPath[2]) {
      if (job.state === "running") {
        job.state = "cancelled";
        await event("job.cancelled", { jobId: job.jobId, id: job.id });
      }
      return json(res, 200, jobStatus(job));
    }
  }
  json(res, 404, { message: "нет маршрута" });
}

const server = createServer((req, res) =>
  route(req, res).catch((e) => {
    if (!res.headersSent) json(res, 400, { message: e.message });
    else res.destroy();
  }),
);

const socketPath = process.env.AGENT_WORKER_SOCKET;
if (!socketPath) {
  console.error("нет AGENT_WORKER_SOCKET — воркер запускает агент");
  process.exit(2);
}
rmSync(socketPath, { force: true });
server.listen(socketPath, () => void event("echo.started", { version: VERSION, pid: process.pid }));
// SIGTERM — остановка: созданное на узле не убирается (это делает только POST /cleanup).
process.on("SIGTERM", () => server.close(() => process.exit(0)));
