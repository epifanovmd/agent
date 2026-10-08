// Транспорты Agents: регистрация агента, WebSocket (основной, §2.1) и HTTP sync
// (запасной, §2.2). Логика сессии — в Agents; здесь только доставка конвертов.
import { createReadStream } from "node:fs";
import { readFile, stat } from "node:fs/promises";
import type { IncomingMessage, Server, ServerResponse } from "node:http";
import { join } from "node:path";
import type { Duplex } from "node:stream";
import { WebSocketServer, type WebSocket } from "ws";
import {
  Close,
  ENROLL_PATH,
  INSTALL_PATH,
  LINK_PATH,
  RELEASES_PATH,
  WS_CHANNEL,
  SYNC_PATH,
  type Envelope,
} from "../index";
import { baseUrl, clientAddress, readBody, readJSON, sendJSON } from "./http";
import type { Agents } from "./agents";
import { AgentsError, ENROLL_MAX_BODY } from "./model";
import { Session } from "./session";

/** Тайминги транспорта (тесты уменьшают). */
export const transportDefaults = {
  helloTimeoutMs: 10_000,
  pingIntervalMs: 20_000,
  syncMaxWaitMs: 25_000,
  /** HTTP-сессия без запросов дольше этого — агент без связи. */
  syncIdleMs: 60_000,
};

export class Transport {
  private readonly agents: Agents;
  private wss?: WebSocketServer;
  private readonly idle = new Map<Session, NodeJS.Timeout>();
  private readonly sockets = new Set<WebSocket>();

  constructor(agents: Agents) {
    this.agents = agents;
  }

  async handle(req: IncomingMessage, res: ServerResponse): Promise<boolean> {
    const path = new URL(req.url ?? "/", "http://x").pathname;
    if (req.method === "POST" && path === ENROLL_PATH) {
      await this.enroll(req, res);
      return true;
    }
    if (req.method === "POST" && path === SYNC_PATH) {
      await this.sync(req, res);
      return true;
    }
    if ((req.method === "GET" || req.method === "HEAD") && this.agents.releaseOptions.dir) {
      if (path === INSTALL_PATH) return this.install(req, res);
      if (path.startsWith(RELEASES_PATH + "/"))
        return this.releaseFile(req, res, decodeURIComponent(path.slice(RELEASES_PATH.length + 1)));
    }
    return (await this.agents.files.handle?.(req, res, path)) ?? false;
  }

  // ── выпуск агента (§7): публично, сборки подписаны ──

  /** manifest.json или сборка агента или воркера, перечисленная в манифесте (других файлов каталога не отдаём). */
  private async releaseFile(req: IncomingMessage, res: ServerResponse, name: string): Promise<boolean> {
    const dir = this.agents.releaseOptions.dir!;
    const manifest = await this.agents.release();
    if (!manifest) return notFound(res);
    if (name === "manifest.json") {
      sendJSON(res, 200, manifest);
      return true;
    }
    // Сборки агента и воркеров — только перечисленные в манифесте.
    const art = [...manifest.artifacts, ...(manifest.workers ?? [])].find((a) => a?.file === name);
    if (!art || name.includes("/") || name.includes("\\")) return notFound(res);
    const file = join(dir, art.file);
    let size: number;
    try {
      size = (await stat(file)).size;
    } catch {
      return notFound(res);
    }
    res.writeHead(200, {
      "Content-Type": "application/octet-stream",
      "Content-Length": size,
      "Content-Disposition": `attachment; filename="${art.file}"`,
    });
    if (req.method === "HEAD") res.end();
    else createReadStream(file).pipe(res);
    return true;
  }

  /** install.sh: DEFAULT_SERVER — baseUrl или адрес из запроса (иначе 400), DEFAULT_PUBLIC_KEY — ключ выпуска. */
  private async install(req: IncomingMessage, res: ServerResponse): Promise<boolean> {
    const { dir, publicKey } = this.agents.releaseOptions;
    let script: string;
    try {
      script = await readFile(join(dir!, "install.sh"), "utf8");
    } catch {
      return notFound(res);
    }
    // Значения попадают в строку sh в двойных кавычках — только безопасные символы.
    const server = this.agents.publicUrl(baseUrl(req, this.agents.trustProxy));
    if (!safeServer(server)) {
      sendJSON(res, 400, { code: "MESSAGE_INVALID", message: "Некорректный адрес сервера" });
      return true;
    }
    let key = publicKey ?? "";
    if (!SAFE_KEY.test(key)) {
      this.agents.warn("publicKey не base64: в install.sh не подставлен");
      key = "";
    }
    script = script
      .replace(/^DEFAULT_SERVER=""$/m, `DEFAULT_SERVER="${server}"`)
      .replace(/^DEFAULT_PUBLIC_KEY=""$/m, `DEFAULT_PUBLIC_KEY="${key}"`);
    res.writeHead(200, {
      "Content-Type": "text/x-shellscript; charset=utf-8",
      "Content-Length": Buffer.byteLength(script),
    });
    res.end(req.method === "HEAD" ? undefined : script);
    return true;
  }

  close(): void {
    for (const t of this.idle.values()) clearTimeout(t);
    this.idle.clear();
    this.wss?.close();
  }

  /**
   * POST enroll — токен регистрации → учётные данные. Тело — не больше ENROLL_MAX_BODY (читается
   * до проверки токена, больше — 413); клиент для ограничения попыток — адрес как у Agent.address.
   */
  private async enroll(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const body = async () => {
      const raw = await readBody(req, ENROLL_MAX_BODY);
      return raw.length ? JSON.parse(raw.toString("utf8")) : {};
    };
    try {
      sendJSON(res, 201, await this.agents.enrollAgent(body, clientAddress(req, this.agents.trustProxy) || "*"));
    } catch (err) {
      const e = err instanceof AgentsError ? err : new AgentsError("MESSAGE_INVALID", (err as Error).message);
      if (e.retryAfterSec !== undefined) res.setHeader("Retry-After", String(e.retryAfterSec));
      sendJSON(res, e.status, { code: e.code, message: e.message });
    }
  }

  // ── WebSocket ──

  /** Авторизация и канал — до upgrade (§2.1). */
  attach(server: Server): void {
    this.wss ??= new WebSocketServer({
      noServer: true,
      maxPayload: 16 << 20,
      handleProtocols: (protocols) => (protocols.has(WS_CHANNEL) ? WS_CHANNEL : false),
    });
    const wss = this.wss;
    server.on("upgrade", (req: IncomingMessage, socket: Duplex, head: Buffer) => {
      const path = new URL(req.url ?? "/", "http://x").pathname;
      if (path !== LINK_PATH) {
        // Чужой upgrade: отвечает другой обработчик, если он есть.
        if (server.listenerCount("upgrade") === 1) reject(socket, 404, "Not Found");
        return;
      }
      const channels = String(req.headers["sec-websocket-protocol"] ?? "")
        .split(",")
        .map((p) => p.trim());
      if (!channels.includes(WS_CHANNEL)) return reject(socket, 426, "Upgrade Required");
      this.agents.authenticate(req.headers.authorization).then(
        (agent) => {
          if (!agent) return reject(socket, 401, "Unauthorized");
          wss.handleUpgrade(req, socket, head, (ws) =>
            this.serve(
              ws,
              new Session(
                agent.id,
                "ws",
                baseUrl(req, this.agents.trustProxy),
                clientAddress(req, this.agents.trustProxy),
              ),
            ),
          );
        },
        () => reject(socket, 500, "Internal Server Error"),
      );
    });
  }

  private serve(ws: WebSocket, ss: Session): void {
    this.sockets.add(ws);
    // Исходящее сессии — в сокет по мере появления; закрытие — с кодом (4409, 4410…).
    const flush = () => {
      for (const env of ss.take()) ws.send(JSON.stringify(env));
      if (ss.closed && ws.readyState === ws.OPEN) ws.close(ss.code || Close.Normal);
    };
    ss.onWake(flush);

    let greeted = false;
    const helloTimer = setTimeout(() => !greeted && ss.close(Close.Invalid), transportDefaults.helloTimeoutMs);
    let alive = true;
    const ping = setInterval(() => {
      if (!alive) return ws.terminate();
      alive = false;
      ws.ping();
    }, transportDefaults.pingIntervalMs);
    ws.on("pong", () => (alive = true));

    ws.on("message", (raw) => {
      alive = true;
      let env: Envelope;
      try {
        env = JSON.parse(raw.toString());
      } catch {
        return ss.close(Close.Invalid);
      }
      if (ss.closed) return;
      if (!greeted) {
        if (env?.type !== "hello") return ss.close(Close.Invalid);
        greeted = true;
        void ss.serial(() => this.agents.open(ss, env));
      } else {
        void ss.serial(() => this.agents.process(ss, env));
      }
    });
    ws.on("close", () => {
      this.sockets.delete(ws);
      clearTimeout(helloTimer);
      clearInterval(ping);
      void ss.serial(() => this.agents.closed(ss));
    });
    ws.on("error", () => ws.terminate());
  }

  // ── HTTP sync ──

  /** Сессия HTTP без запросов syncIdleMs — агент без связи. */
  private touch(ss: Session): void {
    clearTimeout(this.idle.get(ss));
    const timer = setTimeout(() => {
      this.idle.delete(ss);
      void ss.serial(() => this.agents.closed(ss));
    }, transportDefaults.syncIdleMs);
    timer.unref();
    this.idle.set(ss, timer);
  }

  /** POST sync — пачка сообщений агента, ответ — доставки (§2.2). */
  private async sync(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const agent = await this.agents.authenticate(req.headers.authorization);
    if (!agent) return sendJSON(res, 401, { code: "AGENT_CREDENTIALS_INVALID", message: "Неверные учётные данные" });
    let body: { sessionId?: string | null; messages?: Envelope[]; waitSeconds?: number };
    try {
      body = await readJSON(req);
    } catch (err) {
      return sendJSON(res, 400, { code: "MESSAGE_INVALID", message: (err as Error).message });
    }
    let messages = Array.isArray(body?.messages) ? body.messages : [];
    let ss: Session;
    if (body.sessionId == null) {
      if (messages[0]?.type !== "hello") {
        return sendJSON(res, 400, { code: "AGENT_HELLO_REQUIRED", message: "Первое сообщение — hello" });
      }
      ss = new Session(
        agent.id,
        "http",
        baseUrl(req, this.agents.trustProxy),
        clientAddress(req, this.agents.trustProxy),
      );
      const hello = messages[0];
      await ss.serial(() => this.agents.open(ss, hello));
      if (ss.closed) {
        const code = ss.code === Close.Unsupported ? "AGENT_VERSION_UNSUPPORTED" : "MESSAGE_INVALID";
        return sendJSON(res, ss.code === Close.Unsupported ? 409 : 400, { code, message: "hello отклонён" });
      }
      messages = messages.slice(1);
    } else {
      const cur = this.agents.currentSession(agent.id);
      if (!cur || cur.id !== body.sessionId || cur.closed) {
        const code = cur && cur.id !== body.sessionId ? "AGENT_SESSION_REPLACED" : "AGENT_SESSION_EXPIRED";
        return sendJSON(res, 409, { code, message: "Сессия недействительна" });
      }
      ss = cur;
    }
    this.touch(ss);
    for (const env of messages) await ss.serial(() => this.agents.process(ss, env));

    // Доставлять нечего — ждать до waitSeconds: новую задачу, команду, состояние.
    const wait = Math.min(Math.max(0, Number(body.waitSeconds) || 0) * 1000, transportDefaults.syncMaxWaitMs);
    if (ss.outq.length === 0 && !ss.closed && wait > 0) {
      await new Promise<void>((resolve) => {
        const done = () => {
          clearTimeout(timer);
          off();
          res.off("close", done);
          resolve();
        };
        const timer = setTimeout(done, wait);
        const off = ss.onWake(done);
        res.on("close", done);
      });
    }
    // Клиент ушёл — доставки не забирать: ответ до него не дойдёт, их возьмёт следующий запрос.
    if (res.destroyed || req.socket.destroyed) return;
    this.touch(ss);
    if (ss.closed && ss.code === Close.Unauthorized) {
      return sendJSON(res, 401, { code: "AGENT_CREDENTIALS_INVALID", message: "Учётные данные отозваны" });
    }
    if (ss.closed && ss.code === Close.Replaced) {
      return sendJSON(res, 409, { code: "AGENT_SESSION_REPLACED", message: "Сессию вытеснила другая" });
    }
    sendJSON(res, 200, { sessionId: ss.id, messages: ss.take() });
  }
}

function notFound(res: ServerResponse): true {
  sendJSON(res, 404, { code: "NOT_FOUND", message: "Нет такого файла выпуска" });
  return true;
}

const SAFE_SERVER = /^https?:\/\/[A-Za-z0-9.\-:[\]]+(\/.*)?$/;
const SAFE_KEY = /^[A-Za-z0-9+/=]*$/;

/** Адрес для строки sh в двойных кавычках: http(s)://хост[/путь], без "$`\\ и переводов строк. */
export function safeServer(v: string): boolean {
  return SAFE_SERVER.test(v) && !/["$`\\\r\n]/.test(v);
}

function reject(socket: Duplex, status: number, text: string): void {
  socket.end(`HTTP/1.1 ${status} ${text}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
}
