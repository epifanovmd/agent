// Общее для тестов: фейковый агент для воркера (пара потоков) и ожидание сообщений.
import { duplexPair } from "node:stream";
import type { Envelope } from "../src/index";
import { Worker, type WorkerOptions } from "../src/worker/index";

/** Входящие сообщения с ожиданием по типу и условию. */
export class Inbox {
  readonly got: Envelope[] = [];
  private waiters: (() => void)[] = [];

  push(env: Envelope): void {
    this.got.push(env);
    for (const w of this.waiters.splice(0)) w();
  }

  wait(type: string, pred: (e: Envelope) => boolean = () => true, timeoutMs = 3000): Promise<Envelope> {
    return this.waitFor((e) => e.type === type && pred(e), type, timeoutMs);
  }

  async waitFor(pred: (e: Envelope) => boolean, what = "сообщения", timeoutMs = 3000): Promise<Envelope> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const hit = this.got.find(pred);
      if (hit) return hit;
      const left = deadline - Date.now();
      if (left <= 0) throw new Error(`нет ${what}: ${JSON.stringify(this.got.map((e) => e.type))}`);
      await new Promise<void>((r) => {
        const t = setTimeout(r, left);
        this.waiters.push(() => (clearTimeout(t), r()));
      });
    }
  }

  all(type: string): Envelope[] {
    return this.got.filter((e) => e.type === type);
  }
}

/** Агент для воркера: свой конец пары потоков, по строке JSON на конверт. */
export function fakeAgent(opts: Omit<WorkerOptions, "transport"> = {}) {
  const [mine, theirs] = duplexPair();
  const inbox = new Inbox();
  let buf = "";
  theirs.setEncoding("utf8");
  theirs.on("data", (chunk: string) => {
    buf += chunk;
    for (let nl = buf.indexOf("\n"); nl >= 0; nl = buf.indexOf("\n")) {
      inbox.push(JSON.parse(buf.slice(0, nl)));
      buf = buf.slice(nl + 1);
    }
  });
  theirs.on("error", () => {});
  const worker = new Worker({ name: "t", version: "1.0.0", log: () => {}, ...opts, transport: mine });
  return {
    worker,
    inbox,
    send(type: string, data?: unknown, extra: Partial<Envelope> = {}) {
      theirs.write(JSON.stringify({ type, ts: Date.now(), data, ...extra }) + "\n");
    },
    /** Агент ушёл: канал закрыт. */
    close() {
      theirs.destroy();
    },
  };
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
