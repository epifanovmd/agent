// Сервер стенда на agent-sdk/server: связь с агентами (регистрация, WebSocket, запросы к
// воркерам, настройки, метрики, события, действия, сборки) делает Agents; здесь — небольшой HTTP
// API над ним (examples/API.md), проверка значений по схемам манифестов (validate) и история в
// памяти (history.ts). Без авторизации — пример для запуска у себя.
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import {
  createServer,
  type IncomingMessage,
  type Server,
  type ServerResponse,
} from "node:http";
import { join } from "node:path";
import { Readable } from "node:stream";
import type { ReadableStream } from "node:stream/web";

import {
  type AgentEvent,
  type Agents,
  AgentsError,
  type LogEvent,
  type LogLevel,
  type MetricsEvent,
  schemaProblems,
  type SchemaTarget,
  sendJSON,
  workerManifest,
} from "agent-sdk/server";

import { History } from "./history";
import { baseUrl, LOG_LEVELS, readBody, readJSON } from "./http";

export interface AppOptions {
  /** История событий, метрик и действий: её onEvent передан в Agents, follow — вызван. */
  history: History;
  /** Токен регистрации — для команды установки агента в /api/releases. */
  enrollToken?: string;
  /** Публичный адрес сервера для команды установки; по умолчанию — из запроса. */
  publicUrl?: string;
  /** Каталог сборок: его install.sh стенд отдаёт по GET /install.sh. */
  releasesDir?: string;
  /** Ключи проверки подписи сборок — флагами --update-key в команде установки. */
  updateKeys?: string[];
  /**
   * Проверять значения по схемам манифеста воркера до отправки агенту: настройки (400
   * CONFIG_INVALID) и тела запросов к воркерам (400 REQUEST_INVALID).
   */
  validate?: boolean;
}

/** Тело запроса к воркеру (fetch) — не больше 4 МБ (sdk/spec §16). */
const MAX_FETCH_BODY = 4 * 1024 * 1024;

/** Поток /watch: агент не присылает метрики чаще раза в секунду. */
const WATCH = { metricsIntervalMs: 1000 };

/** Заголовки ответа воркера, которые не передаются дальше как есть. */
const HOP_HEADERS = new Set([
  "connection",
  "content-length",
  "transfer-encoding",
]);

/** Строка в одинарных кавычках sh. */
const shellQuote = (v: string): string => `'${v.replaceAll("'", `'\\''`)}'`;

/** install.sh агента из каталога сборок (scripts/release.sh кладёт его рядом со сборками). */
const installScript = async (
  opts: AppOptions,
  res: ServerResponse,
): Promise<void> => {
  const script =
    opts.releasesDir &&
    (await readFile(join(opts.releasesDir, "install.sh")).catch(() => null));

  if (!script) return notFound(res);
  res.writeHead(200, { "Content-Type": "text/x-shellscript; charset=utf-8" });
  res.end(script);
};

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

/** Значение по схеме из манифеста воркера: не подходит — AgentsError code (400). */
const checkSchema = async (
  agents: Agents,
  id: string,
  worker: string,
  target: SchemaTarget,
  data: unknown,
  code: string,
): Promise<void> => {
  const agent = await agents.getAgent(id);
  const problems = agent ? schemaProblems(agent, worker, target, data) : [];

  if (problems.length)
    throw new AgentsError(code, `${worker}: ${problems.join("; ")}`, 400);
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
  if (await agents.handle(req, res)) return; // регистрация, сборки
  const url = new URL(req.url ?? "/", "http://x");

  if (req.method === "GET" && url.pathname === "/install.sh")
    return installScript(opts, res);

  if (!url.pathname.startsWith("/api/")) return notFound(res);
  const call: Call = {
    method: req.method ?? "GET",
    parts: url.pathname.split("/").slice(2).map(decodeURIComponent),
    query: url.searchParams,
    req,
    res,
  };

  if (call.parts[0] === "agents" && call.parts[1])
    return agentRoute(agents, opts, call);
  const reply = (body: unknown) => sendJSON(res, 200, body);
  const { method, query } = call;
  const path = call.parts.join("/");

  if (method === "GET" && path === "agents")
    return reply(await agents.listAgents());
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
    const release = await agents.release();
    const server = opts.publicUrl || baseUrl(req);
    const flags = [
      "--server",
      server,
      "--token",
      opts.enrollToken ?? "<ENROLL_TOKEN>",
      ...(opts.updateKeys ?? []).flatMap(k => ["--update-key", k]),
    ];
    const installCommand = `curl -fsSL ${shellQuote(`${server}/install.sh`)} | sudo sh -s -- ${flags.map(shellQuote).join(" ")}`;

    return reply({ release, installCommand });
  }
  notFound(res);
};

/** /api/agents/{id}/… */
const agentRoute = async (
  agents: Agents,
  opts: AppOptions,
  call: Call,
): Promise<void> => {
  const { method, query, req, res } = call;
  const { history } = opts;
  const [, id, section, worker, ...rest] = call.parts;
  const reply = (body: unknown) => sendJSON(res, 200, body);
  const route = `${method} ${section ?? ""}`;

  if (section === "workers" && worker && rest[0] === "fetch")
    return proxy(agents, opts, call, id, worker, rest.slice(1));
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
      return reply(await agents.restartWorker(id, worker, opts));
    if (rest[0] === "update")
      return reply(await agents.updateWorker(id, worker, opts));
  }
  if (section === "configs" && worker && rest.length === 1) {
    if (method === "PUT") {
      const data = await readJSON(req);

      if (opts.validate)
        await checkSchema(
          agents,
          id,
          worker,
          { config: rest[0] },
          data,
          "CONFIG_INVALID",
        );

      return reply(await agents.setConfig(id, worker, rest[0], data));
    }
    if (method === "DELETE")
      return reply({
        deleted: await agents.deleteConfig(id, worker, rest[0]),
      });
  }
  if (
    section === "workers" &&
    worker &&
    rest.length === 1 &&
    rest[0] === "manifest" &&
    method === "GET"
  ) {
    const agent = await agents.getAgent(id);

    if (!agent) return agentNotFound(res);
    const manifest = workerManifest(agent, worker);

    return manifest
      ? reply(manifest)
      : sendJSON(res, 404, {
          code: "WORKER_UNKNOWN",
          message: "воркер себя не описал (нет манифеста)",
        });
  }
  if (worker !== undefined) return notFound(res);

  switch (route) {
    case "GET ": {
      const agent = await agents.getAgent(id);

      return agent ? reply(agent) : agentNotFound(res);
    }
    case "DELETE ":
      await agents.deleteAgent(id);
      history.forget(id);

      return reply({});
    case "POST revoke":
      return reply(await agents.revoke(id));
    case "POST rotate-key":
      return (await agents.rotateKey(id), reply({}));
    case "POST update": {
      // Тело { version } — до какой версии; без него — до новой версии, которую агент нашёл сам.
      const { version } = await readJSON(req);
      const target = version ?? (await agents.getAgent(id))?.update?.latest;

      if (!target)
        throw new AgentsError(
          "UPDATE_NOT_AVAILABLE",
          "агент не сообщал о новой версии — укажите version",
          409,
        );

      return reply(await agents.updateAgent(id, { version: target }));
    }
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
        await agents.logs(id, {
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
 * Запрос к воркеру как есть: метод, путь после /fetch с параметрами, тело и Content-Type; срок —
 * заголовок X-Timeout-Ms. Ответ воркера передаётся потоком; закрыли запрос — запрос к воркеру
 * отменяется.
 */
const proxy = async (
  agents: Agents,
  opts: AppOptions,
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
      : await readBody(req, MAX_FETCH_BODY);
  const target = `/${path.join("/")}${search}`;

  if (opts.validate && body?.length) {
    let value: unknown;

    try {
      value = JSON.parse(body.toString());
    } catch {
      throw new AgentsError(
        "REQUEST_INVALID",
        `${worker}: тело — не JSON`,
        400,
      );
    }
    await checkSchema(
      agents,
      id,
      worker,
      { route: { method, path: target } },
      value,
      "REQUEST_INVALID",
    );
  }
  const abort = new AbortController();
  const headers: Record<string, string> = {};

  if (req.headers["content-type"])
    headers["content-type"] = req.headers["content-type"];
  res.on("close", () => {
    if (!res.writableFinished) abort.abort();
  });
  const reply = await agents.fetch(id, worker, target, {
    method,
    headers,
    body: body?.length ? new Uint8Array(body) : undefined,
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
  const watcher = await agents.watch(id, {
    id: `api-${randomUUID()}`,
    metricsIntervalMs: WATCH.metricsIntervalMs,
    logLevel: level,
  });

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

  agents.on("metrics", onMetrics);
  agents.on("log", onLog);
  agents.on("event", onEvent);
  res.on("close", () => {
    agents.off("metrics", onMetrics);
    agents.off("log", onLog);
    agents.off("event", onEvent);
    void watcher.close().catch(() => {});
  });
};
