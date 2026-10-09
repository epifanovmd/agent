// Помощники тестов: сервер с Agents на свободном порту и фейковый агент на WebSocket.
import { readFileSync } from "node:fs";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { join } from "node:path";

import WebSocket from "ws";

import { Agents, type AgentsOptions, type Envelope } from "../src/server/index";

export const SPEC = join(import.meta.dirname, "..", "..", "spec", "examples");

export interface Sample {
  from: string;
  to: string;
  message?: Envelope;
  request?: {
    method: string;
    path: string;
    headers?: Record<string, string>;
    body?: unknown;
  };
  response?: {
    status: number;
    headers?: Record<string, string>;
    body?: unknown;
  };
}

/** Все образцы sdk/spec/examples: имя → образец (и файл). */
export const samples = (): Map<string, Sample & { file: string }> => {
  const out = new Map<string, Sample & { file: string }>();

  for (const file of [
    "connection",
    "configs",
    "fetch",
    "observe",
    "actions",
    "events",
    "worker",
  ]) {
    const all = JSON.parse(
      readFileSync(join(SPEC, `${file}.json`), "utf8"),
    ) as Record<string, Sample>;

    for (const [name, s] of Object.entries(all)) out.set(name, { ...s, file });
  }

  return out;
};

export const sample = (name: string): Envelope => {
  const s = samples().get(name);

  if (!s?.message) throw new Error(`нет образца ${name}`);

  return structuredClone(s.message);
};

export interface TestServer {
  agents: Agents;
  url: string;
  ws: string;
  server: Server;
  close(): Promise<void>;
}

export const startServer = async (
  opts: AgentsOptions = {},
): Promise<TestServer> => {
  const agents = new Agents({
    enrollToken: "enroll-token-example",
    log: () => {},
    ...opts,
  });
  const server = createServer(async (req, res) => {
    if (await agents.handle(req, res)) return;
    res.writeHead(404).end();
  });

  agents.attach(server);
  await new Promise<void>(r => server.listen(0, "127.0.0.1", r));
  const { port } = server.address() as AddressInfo;

  return {
    agents,
    server,
    url: `http://127.0.0.1:${port}`,
    ws: `ws://127.0.0.1:${port}/api/v1/agent-link`,
    async close() {
      await agents.close();
      server.closeAllConnections();
      await new Promise(r => server.close(r));
    },
  };
};

export interface Creds {
  agentId: string;
  secret: string;
}

export const enroll = async (
  url: string,
  body: Record<string, unknown> = {},
): Promise<Creds> => {
  const res = await fetch(`${url}/api/v1/agent-link/enroll`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      token: "enroll-token-example",
      name: "node-01",
      host: { os: "linux", arch: "amd64", hostname: "node-01" },
      ...body,
    }),
  });

  if (res.status !== 200)
    throw new Error(`enroll: ${res.status} ${await res.text()}`);

  return (await res.json()) as Creds;
};

/** Агент для тестов: шлёт конверты, ждёт сообщения сервера. */
export class FakeAgent {
  readonly ws: WebSocket;
  readonly inbox: Envelope[] = [];
  readonly closed: Promise<{ code: number; reason: string }>;
  private seq = 0;
  private waiters: (() => void)[] = [];

  private constructor(ws: WebSocket) {
    this.ws = ws;
    ws.on("message", raw => {
      this.inbox.push(JSON.parse(raw.toString()));
      for (const w of this.waiters.splice(0)) w();
    });
    this.closed = new Promise(resolve =>
      ws.on("close", (code, reason) => {
        resolve({ code, reason: reason.toString() });
        for (const w of this.waiters.splice(0)) w();
      }),
    );
  }

  /** Открыть WebSocket (без hello). */
  static open(
    wsUrl: string,
    creds: Creds,
    protocol = "agent.v2",
  ): Promise<FakeAgent> {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(wsUrl, protocol, {
        headers: { authorization: `Agent ${creds.agentId}.${creds.secret}` },
      });
      const fa = new FakeAgent(ws);

      ws.once("open", () => resolve(fa));
      ws.once("unexpected-response", (_req, res) =>
        reject(new Error(`HTTP ${res.statusCode}`)),
      );
      ws.once("error", reject);
    });
  }

  /** Подключиться и поздороваться: hello из образца, поверх — свои поля. */
  static async connect(
    wsUrl: string,
    creds: Creds,
    hello: Record<string, unknown> = {},
  ): Promise<FakeAgent> {
    const fa = await FakeAgent.open(wsUrl, creds);
    const h = sample("hello");

    h.data = { ...h.data, ...hello };
    fa.send(h);
    await fa.next("welcome");

    return fa;
  }

  send(env: Envelope): void {
    this.ws.send(JSON.stringify(env));
  }

  /** Поток: seq по порядку, если не задан. */
  stream(type: string, data: unknown, seq?: number): number {
    this.seq = seq ?? this.seq + 1;
    this.send({ type, seq: this.seq, data });

    return this.seq;
  }

  /** Следующее (непрочитанное) сообщение типа type, подходящее под pred. */
  async next(
    type: string,
    pred: (e: Envelope) => boolean = () => true,
    timeoutMs = 3000,
  ): Promise<Envelope> {
    const deadline = Date.now() + timeoutMs;

    for (;;) {
      const i = this.inbox.findIndex(e => e.type === type && pred(e));

      if (i >= 0) return this.inbox.splice(i, 1)[0];
      if (this.ws.readyState === WebSocket.CLOSED)
        throw new Error(`соединение закрыто, нет ${type}`);
      const left = deadline - Date.now();

      if (left <= 0)
        throw new Error(
          `нет ${type}; пришло: ${JSON.stringify(this.inbox.map(e => e.type))}`,
        );
      await new Promise<void>(r => {
        const t = setTimeout(r, left);

        this.waiters.push(() => {
          clearTimeout(t);
          r();
        });
      });
    }
  }

  /** Сообщения типа type, пришедшие за ms. */
  async collect(type: string, ms: number): Promise<Envelope[]> {
    await sleep(ms);
    const got = this.inbox.filter(e => e.type === type);

    for (const e of got) this.inbox.splice(this.inbox.indexOf(e), 1);

    return got;
  }

  /** ack, подтверждающий id. */
  ackOf(id: string): Promise<Envelope> {
    return this.next("ack", e => (e.data?.ids ?? []).includes(id));
  }

  /** ack с seq не меньше seq. */
  ackSeq(seq: number): Promise<Envelope> {
    return this.next("ack", e => (e.data?.seq ?? 0) >= seq);
  }

  close(): Promise<{ code: number; reason: string }> {
    this.ws.close();

    return this.closed;
  }
}

export const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

/** Ждать, пока cond не станет истинным. */
export const until = async (
  cond: () => boolean | Promise<boolean>,
  timeoutMs = 3000,
): Promise<void> => {
  const deadline = Date.now() + timeoutMs;

  while (!(await cond())) {
    if (Date.now() > deadline) throw new Error("не дождались");
    await sleep(10);
  }
};
