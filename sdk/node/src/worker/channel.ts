// Канал воркера с агентом (§10): по строке JSON на конверт; агент передаёт
// unix socket дескриптором AGENT_IPC_FD. Свой транспорт — любой Duplex.
import { Socket } from "node:net";
import type { Duplex } from "node:stream";
import { envelope, newId, type Envelope } from "../index";
import { AgentError } from "./errors";

/** Ответ агента на запрос (job.urls) — не дольше (агент сам ждёт сервер до 30 с). */
export const REQUEST_TIMEOUT_MS = 45_000;

type Pending = { resolve: (env: Envelope) => void; reject: (err: Error) => void; timer: NodeJS.Timeout };

export class Channel {
  private buf = "";
  private pending = new Map<string, Pending>();
  private listeners: ((env: Envelope) => void)[] = [];
  private closeListeners: (() => void)[] = [];
  private isClosed = false;
  private readonly stream: Duplex;

  constructor(stream: Duplex) {
    this.stream = stream;
    stream.setEncoding("utf8");
    stream.on("data", (chunk: string) => this.read(chunk));
    stream.on("end", () => this.shutdown());
    stream.on("close", () => this.shutdown());
    stream.on("error", () => this.shutdown());
  }

  /** Канал, унаследованный от агента; без агента — понятная ошибка. */
  static fromEnv(): Channel {
    const fd = Number(process.env.AGENT_IPC_FD);
    if (!process.env.AGENT_IPC_FD || !Number.isInteger(fd)) {
      throw new Error("воркер запускается агентом (AGENT_IPC_FD не задан): опишите его в workers конфигурации агента");
    }
    return new Channel(new Socket({ fd, readable: true, writable: true }));
  }

  get closed(): boolean {
    return this.isClosed;
  }

  /** Отправить конверт; канал закрыт — false (агента нет, слать некому). */
  send(type: string, data?: unknown, opts: { id?: string; re?: string } = {}): boolean {
    if (this.isClosed || this.stream.destroyed || !this.stream.writable) return false;
    this.stream.write(JSON.stringify(envelope(type, data, opts)) + "\n");
    return true;
  }

  /** Запрос с ответом по `re`; ответ `error` — AgentError. */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- ответ задаётся типом запроса; any — умолчание
  request<T = any>(type: string, data: unknown, timeoutMs = REQUEST_TIMEOUT_MS): Promise<T> {
    const id = newId();
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new AgentError("TIMEOUT", `нет ответа агента на ${type}`));
      }, timeoutMs);
      this.pending.set(id, {
        timer,
        reject,
        resolve: (env) => {
          if (env.type === "error") {
            const e = env.data ?? {};
            reject(new AgentError(e.code ?? "ERROR", e.message ?? "", e.retryable ?? true));
          } else resolve(env.data as T);
        },
      });
      if (!this.send(type, data, { id })) {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(new AgentError("CHANNEL_CLOSED", "канал с агентом закрыт"));
      }
    });
  }

  /** Входящие сообщения (кроме ответов на запросы). */
  onMessage(fn: (env: Envelope) => void): void {
    this.listeners.push(fn);
  }

  onClose(fn: () => void): void {
    if (this.isClosed) fn();
    else this.closeListeners.push(fn);
  }

  close(): void {
    this.stream.end();
    this.stream.destroy();
    this.shutdown();
  }

  private read(chunk: string): void {
    this.buf += chunk;
    for (let nl = this.buf.indexOf("\n"); nl >= 0; nl = this.buf.indexOf("\n")) {
      const line = this.buf.slice(0, nl).trim();
      this.buf = this.buf.slice(nl + 1);
      if (!line) continue;
      let env: Envelope;
      try {
        env = JSON.parse(line);
      } catch {
        continue;
      }
      if (!env || typeof env.type !== "string") continue;
      const waiter = env.re ? this.pending.get(env.re) : undefined;
      if (waiter) {
        this.pending.delete(env.re!);
        clearTimeout(waiter.timer);
        waiter.resolve(env);
        continue;
      }
      for (const fn of this.listeners) fn(env);
    }
  }

  private shutdown(): void {
    if (this.isClosed) return;
    this.isClosed = true;
    for (const [id, p] of this.pending) {
      clearTimeout(p.timer);
      p.reject(new AgentError("CHANNEL_CLOSED", "канал с агентом закрыт"));
      this.pending.delete(id);
    }
    for (const fn of this.closeListeners.splice(0)) fn();
  }
}
