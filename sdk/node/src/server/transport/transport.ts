// Транспорт Agents: HTTP-маршруты (регистрация, выпуск — releases.ts) и WebSocket (§2). Логика
// сессии — у хозяина транспорта; здесь только доставка конвертов.
import type { IncomingMessage, Server, ServerResponse } from "node:http";
import type { Duplex } from "node:stream";

import { type WebSocket, WebSocketServer } from "ws";

import { AgentsError } from "../core/errors";
import { Session } from "../core/session";
import type { AgentRecord } from "../model/types";
import {
  Close,
  ENROLL_MAX_BODY,
  ENROLL_PATH,
  type Envelope,
  LINK_PATH,
  MAX_MESSAGE_BYTES,
  type ReleaseManifest,
  WS_CHANNEL,
} from "../protocol/messages";
import { envelopeSchema } from "../protocol/schemas";
import { baseUrl, clientAddress, readJSON, sendJSON } from "./http";
import { type ReleaseSource, serveRelease } from "./releases";

/** Сроки транспорта (§16); тесты уменьшают. */
export const transportDefaults = {
  helloTimeoutMs: 10_000,
  pingIntervalMs: 20_000,
  pongTimeoutMs: 10_000,
};

/** Что транспорт передаёт дальше. */
export interface TransportHost extends ReleaseSource {
  /** Регистрация: функция чтения тела и адрес клиента → ключ агента. */
  enroll(body: () => Promise<unknown>, remote: string): Promise<unknown>;
  authenticate(header: string | undefined): Promise<AgentRecord | undefined>;
  /** manifest.json каталога выпуска или null — для раздачи выпуска. */
  manifest(): Promise<ReleaseManifest | null>;
  open(ss: Session, env: Envelope): Promise<void>;
  process(ss: Session, env: Envelope): Promise<void>;
  closed(ss: Session): Promise<void>;
}

export class Transport {
  private readonly host: TransportHost;
  private wss?: WebSocketServer;
  private readonly sockets = new Set<WebSocket>();

  constructor(host: TransportHost) {
    this.host = host;
  }

  async handle(req: IncomingMessage, res: ServerResponse): Promise<boolean> {
    const path = pathOf(req);

    if (req.method === "POST" && path === ENROLL_PATH) {
      await this.enroll(req, res);

      return true;
    }

    return serveRelease(this.host, req, res, path);
  }

  close(): void {
    for (const ws of this.sockets) ws.close(Close.Restart);
    this.wss?.close();
  }

  /**
   * POST enroll — токен регистрации → ключ агента. Тело — не больше ENROLL_MAX_BODY (больше —
   * 413); клиент для ограничения неудачных попыток — адрес как у Agent.address.
   */
  private async enroll(
    req: IncomingMessage,
    res: ServerResponse,
  ): Promise<void> {
    try {
      const remote = clientAddress(req, this.host.settings.trustProxy) || "*";

      sendJSON(
        res,
        200,
        await this.host.enroll(() => readJSON(req, ENROLL_MAX_BODY), remote),
      );
    } catch (err) {
      const e =
        err instanceof AgentsError
          ? err
          : new AgentsError("MESSAGE_INVALID", (err as Error).message);

      if (e.retryAfterSec !== undefined)
        res.setHeader("Retry-After", String(e.retryAfterSec));
      sendJSON(res, e.status, { code: e.code, message: e.message });
    }
  }

  // ── WebSocket ──

  /** Авторизация и канал — до upgrade (§2). */
  attach(server: Server): void {
    this.wss ??= new WebSocketServer({
      noServer: true,
      maxPayload: MAX_MESSAGE_BYTES,
      handleProtocols: protocols =>
        protocols.has(WS_CHANNEL) ? WS_CHANNEL : false,
    });
    const wss = this.wss;

    server.on(
      "upgrade",
      (req: IncomingMessage, socket: Duplex, head: Buffer) => {
        if (pathOf(req) !== LINK_PATH) {
          // Чужой upgrade: отвечает другой обработчик, если он есть.
          if (server.listenerCount("upgrade") === 1)
            reject(socket, 404, "Not Found");

          return;
        }
        const channels = String(req.headers["sec-websocket-protocol"] ?? "")
          .split(",")
          .map(p => p.trim());

        if (!channels.includes(WS_CHANNEL))
          return reject(socket, 426, "Upgrade Required");
        this.host.authenticate(req.headers.authorization).then(
          agent => {
            if (!agent) return reject(socket, 401, "Unauthorized");
            wss.handleUpgrade(req, socket, head, ws =>
              this.serve(
                ws,
                new Session(
                  agent.id,
                  baseUrl(req, this.host.settings.trustProxy),
                  clientAddress(req, this.host.settings.trustProxy),
                ),
              ),
            );
          },
          () => reject(socket, 500, "Internal Server Error"),
        );
      },
    );
  }

  private serve(ws: WebSocket, ss: Session): void {
    this.sockets.add(ws);
    // Исходящее сессии — в сокет по мере появления; закрытие — с кодом (§2).
    const flush = () => {
      if (ws.readyState !== ws.OPEN) return;
      for (const env of ss.take()) ws.send(JSON.stringify(env));
      if (ss.closed) ws.close(ss.code || Close.Normal, ss.reason);
    };

    ss.onWake(flush);

    let greeted = false;
    const helloTimer = setTimeout(
      () => !greeted && ss.close(Close.Invalid, "нет hello"),
      transportDefaults.helloTimeoutMs,
    );
    let pongTimer: NodeJS.Timeout | undefined;
    const ping = setInterval(() => {
      if (pongTimer) return;
      ws.ping();
      pongTimer = setTimeout(
        () => ws.terminate(),
        transportDefaults.pongTimeoutMs,
      );
    }, transportDefaults.pingIntervalMs);

    ws.on("pong", () => {
      clearTimeout(pongTimer);
      pongTimer = undefined;
    });

    ws.on("message", (raw, binary) => {
      let parsed: unknown;

      try {
        parsed = binary ? undefined : JSON.parse(raw.toString());
      } catch {
        parsed = undefined;
      }
      const object = envelopeSchema.safeParse(parsed);

      if (!object.success)
        return ss.close(Close.Invalid, "сообщение — не JSON-объект");
      const env = object.data as unknown as Envelope;

      if (ss.closed) return;
      if (!greeted) {
        if (env.type !== "hello")
          return ss.close(Close.Invalid, "первое сообщение — не hello");
        greeted = true;
        void ss.serial(() => this.host.open(ss, env));
      } else {
        void ss.serial(() => this.host.process(ss, env));
      }
    });
    ws.on("close", () => {
      this.sockets.delete(ws);
      clearTimeout(helloTimer);
      clearTimeout(pongTimer);
      clearInterval(ping);
      void ss.serial(() => this.host.closed(ss));
    });
    ws.on("error", () => ws.terminate());
  }
}

/** Путь запроса; адрес не разбирается — пусто. */
const pathOf = (req: IncomingMessage): string => {
  try {
    return new URL(req.url ?? "/", "http://x").pathname;
  } catch {
    return "";
  }
};

const reject = (socket: Duplex, status: number, text: string): void => {
  socket.end(
    `HTTP/1.1 ${status} ${text}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`,
  );
};
