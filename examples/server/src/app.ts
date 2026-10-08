// Бэкенд примера на agent-sdk/server: механику связи (регистрация агентов,
// WebSocket и HTTP sync, задачи, команды, состояние, файлы) делает Agents; здесь —
// только API веб-интерфейса и раздача его статики. Без авторизации — пример
// для локального запуска.
import { randomUUID } from "node:crypto";
import { createReadStream, existsSync, statSync } from "node:fs";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { extname, join, normalize } from "node:path";
import type { Duplex } from "node:stream";
import { WebSocketServer, type WebSocket } from "ws";
import { LINK_PATH, type LogEntry, type LogEntryLevel } from "agent-sdk";
import {
  baseUrl,
  AgentsError,
  readJSON,
  sendJSON,
  type Agent,
  type AgentEvent,
  type Alert,
  type AuditEntry,
  type Command,
  type DesiredState,
  type Agents,
  type Job,
  type MemoryFiles,
  type MetricsPoint,
} from "agent-sdk/server";

/** Поток для интерфейса (examples/API.md). */
export const WS_PATH = "/api/ws";
/**
 * Группы метрик узла сверх настройки агента, пока карточку агента смотрят: диск чтение/запись,
 * TCP-соединения, процессы, температура (подписка клиента потока, metrics.groups).
 */
export const SUBSCRIPTION_METRICS = ["diskio", "sockets", "processes", "temperatures"];
/** Частота метрик и показателей воркеров, пока карточку агента смотрят, мс. */
export const SUBSCRIPTION_INTERVAL_MS = 1000;

/** Тайминги потока (тесты уменьшают). */
export const wsDefaults = {
  /** Продление подписки клиента потока, мс (её срок — 30 с). */
  subscriptionRenewMs: 20_000,
  /** Ping клиентов: не ответил до следующего — соединение закрывается. */
  pingIntervalMs: 20_000,
};

const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript",
  ".css": "text/css",
  ".svg": "image/svg+xml",
  ".json": "application/json",
  ".ico": "image/x-icon",
};

/**
 * Всё для интерфейса: агенты, задачи (новые первыми, с загруженными выходами), команды,
 * состояние, события, активные уведомления о проблемах.
 */
export async function snapshot(agents: Agents, files: MemoryFiles) {
  const [list, jobs, commands, states, events] = await Promise.all([
    agents.listAgents(),
    agents.listJobs(),
    agents.listCommands(),
    agents.listStates(),
    agents.listEvents(500),
  ]);
  return {
    serverTime: Date.now(),
    agents: list,
    jobs: jobs.map((j) => withFiles(j, files)),
    commands,
    states,
    events,
    alerts: agents.alerts(),
  };
}

/** Задача с именами загруженных выходных файлов. */
function withFiles(j: Job, files: MemoryFiles) {
  return { ...j, files: files.outputs(j.id).filter((n) => j.outputs.includes(n)) };
}

export interface AppOptions {
  /** Токен регистрации — для команды установки агента в /api/releases. */
  enrollToken?: string;
  log?: (msg: string, extra?: Record<string, unknown>) => void;
}

export function createApp(agents: Agents, files: MemoryFiles, webDir: string, opts: AppOptions = {}): Server {
  const server = createServer(async (req, res) => {
    try {
      if (await agents.handle(req, res)) return; // агенты: enroll, sync, файлы задач, релизы, install.sh
      if (!(await api(agents, files, opts, req, res))) staticFile(webDir, req, res);
    } catch (err) {
      const e = err instanceof AgentsError ? err : new AgentsError("MESSAGE_INVALID", (err as Error).message);
      if (!res.headersSent) sendJSON(res, e.status, { code: e.code, message: e.message });
    }
  });
  agents.attach(server); // WebSocket агентов
  serveStream(server, agents, files, opts);
  return server;
}

async function api(
  agents: Agents,
  files: MemoryFiles,
  opts: AppOptions,
  req: IncomingMessage,
  res: ServerResponse,
): Promise<boolean> {
  const url = new URL(req.url ?? "/", "http://x");
  const path = url.pathname;
  const method = req.method ?? "GET";
  const reply = (status: number, body: unknown) => (sendJSON(res, status, body), true);
  const found = (v: unknown, what: string) =>
    v ? reply(200, v) : reply(404, { code: "NOT_FOUND", message: `${what} не найдена` });
  let m: RegExpMatchArray | null;
  // Кто действует: заголовок X-Actor (в примере без авторизации — как назвался), иначе "web".
  const as = agents.by(String(req.headers["x-actor"] ?? "").trim() || "web");

  if (method === "GET" && path === "/api/snapshot") return reply(200, await snapshot(agents, files));
  if (method === "POST" && path === "/api/jobs") return reply(201, await as.enqueue(await readJSON(req)));
  if (method === "GET" && (m = path.match(/^\/api\/jobs\/([^/]+)$/))) return found(await agents.getJob(m[1]), "Задача");
  if (method === "POST" && (m = path.match(/^\/api\/jobs\/([^/]+)\/(cancel|stop)$/))) {
    return reply(200, m[2] === "cancel" ? await as.cancelJob(m[1]) : await as.stopJob(m[1]));
  }
  if (method === "POST" && path === "/api/commands") return reply(201, await as.command(await readJSON(req)));
  if (method === "GET" && (m = path.match(/^\/api\/commands\/([^/]+)$/)))
    return found(await agents.getCommand(m[1]), "Команда");
  if (method === "PUT" && (m = path.match(/^\/api\/state\/([^/]+)$/))) {
    const agentId = url.searchParams.get("agentId") || undefined;
    return reply(200, await as.setState(decodeURIComponent(m[1]), await readJSON(req), { agentId }));
  }
  // Удалить снимок: с agentId — личный (агенту переиздаётся общий), без — общий.
  if (method === "DELETE" && (m = path.match(/^\/api\/state\/([^/]+)$/))) {
    const agentId = url.searchParams.get("agentId") || undefined;
    return reply(200, { state: await as.deleteState(decodeURIComponent(m[1]), { agentId }) });
  }
  // История версий раздела (общая или агента) и откат к версии (новой версией с тем же spec).
  if (method === "GET" && (m = path.match(/^\/api\/state\/([^/]+)\/history$/))) {
    const agentId = url.searchParams.get("agentId") || undefined;
    const limit = Number(url.searchParams.get("limit"));
    return reply(
      200,
      await agents.stateHistory(decodeURIComponent(m[1]), {
        agentId,
        limit: url.searchParams.has("limit") && Number.isFinite(limit) ? limit : undefined,
      }),
    );
  }
  if (method === "POST" && (m = path.match(/^\/api\/state\/([^/]+)\/rollback$/))) {
    const body = await readJSON(req);
    if (typeof body?.version !== "number")
      return reply(400, { code: "MESSAGE_INVALID", message: "Нужна version (число)" });
    return reply(
      200,
      await as.rollbackState(decodeURIComponent(m[1]), body.version, { agentId: body.agentId || undefined }),
    );
  }
  // Подписка на агента (тело — как у agents.subscribe) и её снятие.
  if (method === "POST" && (m = path.match(/^\/api\/agents\/([^/]+)\/subscriptions$/))) {
    const body = (await readJSON(req)) ?? {};
    if (typeof body !== "object" || Array.isArray(body))
      return reply(400, { code: "MESSAGE_INVALID", message: "Тело — объект подписки" });
    return reply(200, await agents.subscribe(decodeURIComponent(m[1]), body));
  }
  if (method === "DELETE" && (m = path.match(/^\/api\/agents\/([^/]+)\/subscriptions\/([^/]+)$/))) {
    await agents.unsubscribe(decodeURIComponent(m[1]), decodeURIComponent(m[2]));
    return reply(200, {});
  }
  // Агенты: история метрик, отзыв, обновление, смена ключа.
  if ((m = path.match(/^\/api\/agents\/([^/]+)\/(metrics|revoke|update|rotate-key)$/))) {
    const id = decodeURIComponent(m[1]);
    if (method === "GET" && m[2] === "metrics") {
      if (!(await agents.getAgent(id))) return reply(404, { code: "AGENT_NOT_FOUND", message: "Агент не найден" });
      const since = Number(url.searchParams.get("since"));
      return reply(
        200,
        await agents.listMetrics(id, {
          since: url.searchParams.has("since") && Number.isFinite(since) ? since : undefined,
        }),
      );
    }
    if (method === "POST" && m[2] === "revoke") return reply(200, await as.revoke(id));
    if (method === "POST" && m[2] === "update") return reply(201, await as.updateAgent(id));
    if (method === "POST" && m[2] === "rotate-key") return reply(201, await as.rotateKey(id));
  }
  // Обновление воркера из выпуска (release: true в agent.yaml узла) — команда worker.update.
  if (method === "POST" && (m = path.match(/^\/api\/agents\/([^/]+)\/workers\/([^/]+)\/update$/)))
    return reply(201, await as.updateWorker(decodeURIComponent(m[1]), decodeURIComponent(m[2])));
  // Пауза воркера с сервера (команда worker.pause / worker.resume); тело — {queues?} (пусто — все очереди).
  if (method === "POST" && (m = path.match(/^\/api\/agents\/([^/]+)\/workers\/([^/]+)\/(pause|resume)$/))) {
    const body = await readJSON(req);
    const queues = Array.isArray(body?.queues) ? body.queues.map(String) : undefined;
    const [id, name] = [decodeURIComponent(m[1]), decodeURIComponent(m[2])];
    return reply(
      201,
      m[3] === "pause" ? await as.pauseWorker(id, name, { queues }) : await as.resumeWorker(id, name, { queues }),
    );
  }
  // Выпуск агента: манифест, кандидаты на обновление агентов и воркеров, команда установки на новый узел.
  if (method === "GET" && path === "/api/releases") {
    const [release, candidates, workerCandidates] = await Promise.all([
      agents.release(),
      agents.updateCandidates(),
      agents.workerUpdateCandidates(),
    ]);
    const installCommand = agents.installCommand({
      baseUrl: agents.publicUrl(baseUrl(req)),
      token: opts.enrollToken ?? "<ENROLL_TOKEN>",
    });
    return reply(200, { release, candidates, workerCandidates, installCommand });
  }
  if (path.startsWith("/api/")) return reply(404, { code: "NOT_FOUND", message: "Нет такого метода" });
  return false;
}

/** Сообщение клиенту потока (сервер → клиент). */
export type StreamMessage =
  | { type: "snapshot"; data: Awaited<ReturnType<typeof snapshot>> }
  | { type: "agent"; data: Agent }
  | { type: "job"; data: Job & { files: string[] } }
  | { type: "command"; data: Command }
  | { type: "state"; data: DesiredState }
  /** Снимок удалён: agentId нет — общий; переизданный общий придёт сообщением state. */
  | { type: "stateDeleted"; domain: string; agentId?: string }
  | { type: "event"; data: AgentEvent }
  /** Проблема началась (active) или закончилась. */
  | { type: "alert"; data: Alert }
  /** Изменяющее действие (журнал аудита). */
  | { type: "audit"; data: AuditEntry }
  | { type: "metrics"; agentId: string; point: MetricsPoint }
  /** Записи лога агента (подписанным, с выбранного клиентом уровня). */
  | { type: "log"; agentId: string; entries: LogEntry[] };

/** Уровни лога от подробного к важному. */
const LOG_LEVELS: LogEntryLevel[] = ["debug", "info", "warn", "error"];
const isLogLevel = (v: unknown): v is LogEntryLevel => LOG_LEVELS.includes(v as LogEntryLevel);
/** Уровень логов подписки по умолчанию. */
const DEFAULT_LOG_LEVEL: LogEntryLevel = "info";

/** Клиент потока: до отправки snapshot дельты копятся (snapshot их не перекроет). */
interface StreamClient {
  /** id соединения: из него — id подписок клиента. */
  id: string;
  ws: WebSocket;
  ready: boolean;
  queued: string[];
  /** Подписки: agentId → уровень логов, с которого клиенту слать записи, и таймер продления. */
  subs: Map<string, { level: LogEntryLevel; timer: NodeJS.Timeout }>;
  alive: boolean;
}

/**
 * WebSocket /api/ws: snapshot при подключении, дальше — изменения по событиям Agents;
 * точки metrics и записи log — только подписанным (subscribe {agentId, logLevel?}, logLevel
 * {agentId, level}, unsubscribe). Пока клиент подписан на агента, сервер держит его подписку
 * agents.subscribe (id — соединение и агент): метрики и показатели всех воркеров агента раз в
 * секунду, группы SUBSCRIPTION_METRICS, лог с уровня клиента; продлевает её, снимает при
 * unsubscribe и закрытии соединения.
 */
function serveStream(server: Server, agents: Agents, files: MemoryFiles, opts: AppOptions): void {
  const wss = new WebSocketServer({ noServer: true, maxPayload: 64 << 10 });
  const clients = new Set<StreamClient>();
  const log = opts.log ?? (() => {});

  const send = (c: StreamClient, raw: string) => {
    if (!c.ready) c.queued.push(raw);
    else if (c.ws.readyState === c.ws.OPEN) c.ws.send(raw);
  };
  const broadcast = (msg: StreamMessage) => {
    const raw = JSON.stringify(msg);
    for (const c of clients) send(c, raw);
  };

  const subscriptionId = (c: StreamClient, agentId: string) => `${c.id}:${agentId}`;
  /** Подписка клиента на агента: создать или продлить (каналы — по текущим воркерам агента). */
  const renew = async (c: StreamClient, agentId: string) => {
    const sub = c.subs.get(agentId);
    if (!sub) return;
    try {
      const agent = await agents.getAgent(agentId);
      const channels = Object.fromEntries(
        (agent?.capabilities?.telemetry?.channels ?? []).map((ch) => [ch, { intervalMs: SUBSCRIPTION_INTERVAL_MS }]),
      );
      await agents.subscribe(agentId, {
        id: subscriptionId(c, agentId),
        metrics: { intervalMs: SUBSCRIPTION_INTERVAL_MS, groups: SUBSCRIPTION_METRICS },
        logs: { level: sub.level },
        channels,
      });
    } catch (e) {
      log("подписка на агента не удалась", { agentId, err: String((e as Error).message) });
    }
  };
  const subscribe = (c: StreamClient, agentId: string, level: LogEntryLevel) => {
    if (c.subs.has(agentId)) return setLogLevel(c, agentId, level);
    const timer = setInterval(() => void renew(c, agentId), wsDefaults.subscriptionRenewMs);
    timer.unref();
    c.subs.set(agentId, { level, timer });
    void renew(c, agentId);
  };
  const setLogLevel = (c: StreamClient, agentId: string, level: LogEntryLevel) => {
    const sub = c.subs.get(agentId);
    if (!sub || sub.level === level) return;
    sub.level = level;
    void renew(c, agentId);
  };
  const unsubscribe = (c: StreamClient, agentId: string) => {
    const sub = c.subs.get(agentId);
    if (!sub) return;
    clearInterval(sub.timer);
    c.subs.delete(agentId);
    agents
      .unsubscribe(agentId, subscriptionId(c, agentId))
      .catch((e) => log("снять подписку на агента не удалось", { agentId, err: String((e as Error).message) }));
  };

  const listeners = {
    agent: (data: Agent) => broadcast({ type: "agent", data }),
    job: (j: Job) => broadcast({ type: "job", data: withFiles(j, files) }),
    command: (data: Command) => broadcast({ type: "command", data }),
    state: (data: DesiredState) => broadcast({ type: "state", data }),
    stateDeleted: (d: { domain: string; agentId?: string }) => broadcast({ type: "stateDeleted", ...d }),
    event: (data: AgentEvent) => broadcast({ type: "event", data }),
    alert: (data: Alert) => broadcast({ type: "alert", data }),
    audit: (data: AuditEntry) => broadcast({ type: "audit", data }),
    metrics: (agentId: string, point: MetricsPoint) => {
      const raw = JSON.stringify({ type: "metrics", agentId, point } satisfies StreamMessage);
      for (const c of clients) if (c.subs.has(agentId)) send(c, raw);
    },
    log: (agentId: string, entries: LogEntry[]) => {
      for (const c of clients) {
        const sub = c.subs.get(agentId);
        if (!sub) continue;
        const min = LOG_LEVELS.indexOf(sub.level);
        const mine = entries.filter((e) => LOG_LEVELS.indexOf(e.level) >= min);
        if (mine.length) send(c, JSON.stringify({ type: "log", agentId, entries: mine } satisfies StreamMessage));
      }
    },
  };
  agents.on("agent", listeners.agent);
  agents.on("job", listeners.job);
  agents.on("command", listeners.command);
  agents.on("state", listeners.state);
  agents.on("stateDeleted", listeners.stateDeleted);
  agents.on("event", listeners.event);
  agents.on("alert", listeners.alert);
  agents.on("audit", listeners.audit);
  agents.on("metrics", listeners.metrics);
  agents.on("log", listeners.log);

  const ping = setInterval(() => {
    for (const c of clients) {
      if (!c.alive) {
        c.ws.terminate();
        continue;
      }
      c.alive = false;
      c.ws.ping();
    }
  }, wsDefaults.pingIntervalMs);
  ping.unref();

  server.on("upgrade", (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    const path = new URL(req.url ?? "/", "http://x").pathname;
    if (path === LINK_PATH) return; // агентов обслуживает Agents
    if (path !== WS_PATH)
      return void socket.end("HTTP/1.1 404 Not Found\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
    wss.handleUpgrade(req, socket, head, (ws) => {
      const c: StreamClient = { id: randomUUID(), ws, ready: false, queued: [], subs: new Map(), alive: true };
      clients.add(c);
      ws.on("pong", () => (c.alive = true));
      ws.on("message", (raw) => {
        c.alive = true;
        let msg: { type?: unknown; agentId?: unknown; logLevel?: unknown; level?: unknown };
        try {
          msg = JSON.parse(raw.toString());
        } catch {
          return;
        }
        if (typeof msg?.agentId !== "string" || !msg.agentId) return;
        if (msg.type === "subscribe")
          subscribe(c, msg.agentId, isLogLevel(msg.logLevel) ? msg.logLevel : DEFAULT_LOG_LEVEL);
        else if (msg.type === "logLevel" && isLogLevel(msg.level)) setLogLevel(c, msg.agentId, msg.level);
        else if (msg.type === "unsubscribe") unsubscribe(c, msg.agentId);
      });
      ws.on("close", () => {
        clients.delete(c);
        for (const id of [...c.subs.keys()]) unsubscribe(c, id);
      });
      ws.on("error", () => ws.terminate());
      snapshot(agents, files).then(
        (data) => {
          if (ws.readyState !== ws.OPEN) return;
          ws.send(JSON.stringify({ type: "snapshot", data } satisfies StreamMessage));
          c.ready = true;
          for (const raw of c.queued.splice(0)) ws.send(raw);
        },
        (e) => {
          log("снимок для потока не удался", { err: String(e) });
          ws.close(1011);
        },
      );
    });
  });

  server.on("close", () => {
    clearInterval(ping);
    for (const c of clients) for (const sub of c.subs.values()) clearInterval(sub.timer);
    agents.off("agent", listeners.agent);
    agents.off("job", listeners.job);
    agents.off("command", listeners.command);
    agents.off("state", listeners.state);
    agents.off("stateDeleted", listeners.stateDeleted);
    agents.off("event", listeners.event);
    agents.off("alert", listeners.alert);
    agents.off("audit", listeners.audit);
    agents.off("metrics", listeners.metrics);
    agents.off("log", listeners.log);
    for (const c of clients) c.ws.terminate();
    wss.close();
  });
}

/** Собранный интерфейс (examples/web/dist); неизвестный путь — index.html (SPA). */
function staticFile(dir: string, req: IncomingMessage, res: ServerResponse): void {
  if (req.method !== "GET") return void res.writeHead(405).end();
  const index = join(dir, "index.html");
  if (!existsSync(index)) {
    res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
    res.end("Интерфейс не собран: cd examples/web && npm install && npm run build (или npm run dev)\n");
    return;
  }
  let file = normalize(join(dir, decodeURIComponent(new URL(req.url ?? "/", "http://x").pathname)));
  if (!file.startsWith(dir) || !existsSync(file) || statSync(file).isDirectory()) file = index;
  res.writeHead(200, { "Content-Type": MIME[extname(file)] ?? "application/octet-stream" });
  createReadStream(file).pipe(res);
}
