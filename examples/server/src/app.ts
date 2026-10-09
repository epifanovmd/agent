// Сервер стенда на agent-sdk/server: связь с агентами (регистрация, WebSocket, запросы к
// воркерам, настройки, метрики, события, действия, выпуск) делает Agents; здесь — небольшой HTTP
// API над ним (examples/API.md) и история в памяти (history.ts). Без авторизации — пример для
// запуска у себя.
import { randomUUID } from "node:crypto";
import {
  createServer,
  type IncomingMessage,
  type Server,
  type ServerResponse,
} from "node:http";
import { Readable } from "node:stream";
import type { ReadableStream } from "node:stream/web";

import {
  type Actor,
  type AgentEvent,
  type Agents,
  AgentsError,
  baseUrl,
  LOG_LEVELS,
  type LogEvent,
  type LogLevel,
  MAX_FETCH_BODY,
  type MetricsEvent,
  readBody,
  readJSON,
  sendJSON,
  supports,
} from "agent-sdk/server";

import { History } from "./history";

export interface AppOptions {
  /** История событий, метрик и действий: её onEvent передан в Agents, follow — вызван. */
  history: History;
  /** Токен регистрации — для команды установки агента в /api/releases. */
  enrollToken?: string;
  /** Публичный адрес сервера для команды установки; по умолчанию — из запроса. */
  publicUrl?: string;
}

/** Сроки потока /watch (тесты не меняют: агент не присылает метрики чаще раза в секунду). */
const WATCH = { metricsIntervalMs: 1000, ttlMs: 60_000, renewMs: 20_000 };

/** Заголовки ответа воркера, которые не передаются дальше как есть. */
const HOP_HEADERS = new Set([
  "connection",
  "content-length",
  "transfer-encoding",
]);

/** Разобранный запрос к API. */
interface Call {
  method: string;
  /** Части пути после /api/. */
  parts: string[];
  query: URLSearchParams;
  req: IncomingMessage;
  res: ServerResponse;
}

const num = (v: string | null): number | undefined => {
  if (v === null || v === "") return undefined;
  const n = Number(v);

  return Number.isFinite(n) ? n : undefined;
};

const isLogLevel = (v: unknown): v is LogLevel =>
  LOG_LEVELS.includes(v as LogLevel);

const notFound = (res: ServerResponse) =>
  sendJSON(res, 404, { code: "NOT_FOUND", message: "Нет такого метода" });

const agentNotFound = (res: ServerResponse) =>
  sendJSON(res, 404, { code: "AGENT_NOT_FOUND", message: "Агент не найден" });

/** Что проверить в манифесте воркера: ?method=&path=, ?config=, ?event=. */
const supportsQuery = (query: URLSearchParams) => {
  const method = query.get("method");
  const path = query.get("path");

  return {
    route: method && path ? { method, path } : undefined,
    config: query.get("config") || undefined,
    event: query.get("event") || undefined,
  };
};

/** HTTP-сервер стенда: маршруты агентов, их WebSocket и API (examples/API.md). */
export const createApp = (agents: Agents, opts: AppOptions): Server => {
  const server = createServer((req, res) => {
    handle(agents, opts, req, res).catch(err => {
      const e =
        err instanceof AgentsError
          ? err
          : new AgentsError("MESSAGE_INVALID", (err as Error).message);

      if (!res.headersSent)
        sendJSON(res, e.status, { code: e.code, message: e.message });
      else res.destroy();
    });
  });

  agents.attach(server);

  return server;
};

const handle = async (
  agents: Agents,
  opts: AppOptions,
  req: IncomingMessage,
  res: ServerResponse,
): Promise<void> => {
  if (await agents.handle(req, res)) return; // регистрация, выпуск, install.sh
  const url = new URL(req.url ?? "/", "http://x");

  if (!url.pathname.startsWith("/api/")) return notFound(res);
  const call: Call = {
    method: req.method ?? "GET",
    parts: url.pathname.split("/").slice(2).map(decodeURIComponent),
    query: url.searchParams,
    req,
    res,
  };

  if (call.parts[0] === "agents" && call.parts[1])
    return agentRoute(agents, opts.history, call);
  const reply = (body: unknown) => sendJSON(res, 200, body);
  const { method, query } = call;
  const path = call.parts.join("/");

  if (method === "GET" && path === "agents")
    return reply(await agents.listAgents());
  if (method === "GET" && path === "alerts")
    return reply(await agents.listAlerts(query.get("agentId") || undefined));
  if (method === "GET" && path === "events")
    return reply(
      opts.history.listEvents({
        agentId: query.get("agentId") || undefined,
        worker: query.get("worker") || undefined,
        type: query.get("type") || undefined,
        before: num(query.get("before")),
        limit: num(query.get("limit")),
      }),
    );
  if (method === "GET" && path === "releases") {
    const [release, candidates, workerCandidates] = await Promise.all([
      agents.release(),
      agents.updateCandidates(),
      agents.workerUpdateCandidates(),
    ]);
    const installCommand = agents.installCommand({
      baseUrl: opts.publicUrl || baseUrl(req),
      token: opts.enrollToken ?? "<ENROLL_TOKEN>",
    });

    return reply({ release, candidates, workerCandidates, installCommand });
  }
  notFound(res);
};

/** /api/agents/{id}/… */
const agentRoute = async (
  agents: Agents,
  history: History,
  call: Call,
): Promise<void> => {
  const { method, query, req, res } = call;
  const [, id, section, worker, ...rest] = call.parts;
  const reply = (body: unknown) => sendJSON(res, 200, body);
  // Кто действует: заголовок X-Actor (в примере без авторизации — как назвался), иначе "api".
  const as = agents.by(String(req.headers["x-actor"] ?? "").trim() || "api");
  const route = `${method} ${section ?? ""}`;

  if (section === "workers" && worker && rest[0] === "fetch")
    return proxy(as, call, id, worker, rest.slice(1));
  if (section === "workers" && worker && rest[0] === "jobs")
    return jobsRoute(as, call, id, worker, rest.slice(1));
  if (
    section === "workers" &&
    worker &&
    rest.length === 1 &&
    method === "POST"
  ) {
    // Тело { force: true } — заменить сразу, не дожидаясь окончания работы воркера;
    // { wait: true } — ждать итога и отложенной замены.
    const body = await readJSON(req);
    const opts = { force: body?.force === true, wait: body?.wait === true };

    if (rest[0] === "restart")
      return reply(await as.restartWorker(id, worker, opts));
    if (rest[0] === "update")
      return reply(await as.updateWorker(id, worker, opts));
  }
  if (section === "configs" && worker && rest.length === 1) {
    if (method === "PUT")
      return reply(
        await as.setConfig(id, worker, rest[0], await readJSON(req)),
      );
    if (method === "DELETE")
      return reply({ deleted: await as.deleteConfig(id, worker, rest[0]) });
  }
  if (
    section === "workers" &&
    worker &&
    rest.length === 1 &&
    rest[0] === "supports" &&
    method === "GET"
  ) {
    const agent = await agents.getAgent(id);

    if (!agent) return agentNotFound(res);

    return reply({ supported: supports(agent, worker, supportsQuery(query)) });
  }
  if (worker !== undefined) return notFound(res);

  switch (route) {
    case "GET ": {
      const agent = await agents.getAgent(id);

      return agent ? reply(agent) : agentNotFound(res);
    }
    case "DELETE ":
      return (await as.deleteAgent(id), reply({}));
    case "POST revoke":
      return reply(await as.revoke(id));
    case "POST rotate-key":
      return (await as.rotateKey(id), reply({}));
    case "POST update":
      return reply(await as.updateAgent(id));
    case "GET actions":
      return reply(history.listActions(id, num(query.get("limit"))));
    case "GET configs": {
      const [configs, status] = await Promise.all([
        agents.listConfigs(id),
        agents.configStatus(id),
      ]);

      return reply({ configs, status });
    }
    case "GET metrics":
      return reply(
        history.listMetrics(id, {
          since: num(query.get("since")),
          limit: num(query.get("limit")),
        }),
      );
    case "GET logs":
      return reply(
        await as.logs(id, {
          worker: query.get("worker") || undefined,
          lines: num(query.get("lines")),
        }),
      );
    case "GET watch":
      return watch(agents, call, id);
    default:
      return notFound(res);
  }
};

/**
 * Задачи воркера: POST …/jobs { type, jobId?, data?, files?, timeoutMs? } — runJob; GET
 * …/jobs/{jobId} — состояние; POST …/jobs/{jobId}/cancel — прервать. Закрыли запрос — ожидание
 * итога прерывается, задаче уходит отмена.
 */
const jobsRoute = async (
  as: Actor,
  call: Call,
  id: string,
  worker: string,
  rest: string[],
): Promise<void> => {
  const { method, req, res } = call;
  const reply = (body: unknown) => sendJSON(res, 200, body);

  if (method === "POST" && rest.length === 0) {
    const abort = new AbortController();

    res.on("close", () => {
      if (!res.writableFinished) abort.abort();
    });

    return reply(
      await as.runJob(id, worker, {
        ...(await readJSON(req)),
        signal: abort.signal,
      }),
    );
  }
  if (method === "GET" && rest.length === 1)
    return reply(await as.jobStatus(id, worker, rest[0]));
  if (method === "POST" && rest.length === 2 && rest[1] === "cancel")
    return reply(await as.cancelJob(id, worker, rest[0]));
  notFound(res);
};

/**
 * Запрос к воркеру как есть: метод, путь после /fetch с параметрами, тело и Content-Type; срок —
 * заголовок X-Timeout-Ms. Ответ воркера передаётся потоком; закрыли запрос — запрос к воркеру
 * отменяется.
 */
const proxy = async (
  as: Actor,
  call: Call,
  id: string,
  worker: string,
  path: string[],
): Promise<void> => {
  const { method, req, res } = call;
  const search = new URL(req.url ?? "/", "http://x").search;
  const body =
    method === "GET" || method === "HEAD"
      ? undefined
      : new Uint8Array(await readBody(req, MAX_FETCH_BODY));
  const abort = new AbortController();
  const headers: Record<string, string> = {};

  if (req.headers["content-type"])
    headers["content-type"] = req.headers["content-type"];
  res.on("close", () => {
    if (!res.writableFinished) abort.abort();
  });
  const reply = await as.fetch(id, worker, `/${path.join("/")}${search}`, {
    method,
    headers,
    body: body?.length ? body : undefined,
    timeoutMs: num(String(req.headers["x-timeout-ms"] ?? "")),
    signal: abort.signal,
  });

  for (const [k, v] of reply.headers)
    if (!HOP_HEADERS.has(k)) res.setHeader(k, v);
  res.writeHead(reply.status);
  if (!reply.body) return void res.end();
  Readable.fromWeb(reply.body as ReadableStream<Uint8Array>)
    .on("error", () => res.destroy())
    .pipe(res);
};

/**
 * Поток Server-Sent Events: пока он открыт, агент присылает метрики раз в секунду и журнал с
 * уровня logLevel (watch); сюда идут события metrics, log и event этого агента.
 */
const watch = async (agents: Agents, call: Call, id: string): Promise<void> => {
  const { query, res } = call;
  const level = isLogLevel(query.get("logLevel"))
    ? (query.get("logLevel") as LogLevel)
    : "info";
  const watchId = `api-${randomUUID()}`;
  const renew = () =>
    agents.watch(id, {
      id: watchId,
      metricsIntervalMs: WATCH.metricsIntervalMs,
      logLevel: level,
      ttlMs: WATCH.ttlMs,
    });

  await renew();
  res.writeHead(200, {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache",
  });
  res.write(": watch\n\n");
  const send = (type: string, data: unknown) =>
    res.write(`event: ${type}\ndata: ${JSON.stringify(data)}\n\n`);
  const min = LOG_LEVELS.indexOf(level);
  const onMetrics = (m: MetricsEvent) => {
    if (m.agentId === id) send("metrics", m);
  };
  const onLog = ({ agentId, entries }: LogEvent) => {
    const mine = entries.filter(e => LOG_LEVELS.indexOf(e.level) >= min);

    if (agentId === id && mine.length) send("log", mine);
  };
  const onEvent = (e: AgentEvent) => {
    if (e.agentId === id) send("event", e);
  };
  const timer = setInterval(() => void renew().catch(() => {}), WATCH.renewMs);

  agents.on("metrics", onMetrics);
  agents.on("log", onLog);
  agents.on("event", onEvent);
  res.on("close", () => {
    clearInterval(timer);
    agents.off("metrics", onMetrics);
    agents.off("log", onLog);
    agents.off("event", onEvent);
    void agents.unwatch(id, watchId).catch(() => {});
  });
};
