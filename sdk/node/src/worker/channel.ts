// Канал воркера с агентом (§10): по строке JSON на конверт; агент передаёт
// unix socket дескриптором AGENT_IPC_FD. Свой транспорт — любой Duplex.
import { Socket } from "node:net";
import type { Duplex } from "node:stream";
import { envelope, newId, type Envelope } from "../index";
import { AgentError, MessageTooLargeError } from "./errors";

/** Ответ агента на запрос (job.urls) — не дольше (агент сам ждёт сервер до 30 с). */
export const REQUEST_TIMEOUT_MS = 45_000;
/** Предел строки канала (§10), байт: длиннее агент не примет. */
export const MAX_LINE_BYTES = 16 << 20;
/**
 * Строк в очереди записи, пока поток не готов (обратное давление), после которых
 * прогресс задач и телеметрия отбрасываются; остальное ждёт в очереди.
 */
export const WRITE_QUEUE_SOFT_LIMIT = 1000;

/** Сообщения, которые можно отбросить при заторе: следующие их заменят. */
const DROPPABLE = new Set(["job.progress", "telemetry"]);

type Pending = { resolve: (env: Envelope) => void; reject: (err: Error) => void; timer: NodeJS.Timeout };
type Logger = (level: "info" | "warn" | "error", msg: string) => void;

export class Channel {
  private buf = "";
  private pending = new Map<string, Pending>();
  private listeners: ((env: Envelope) => void)[] = [];
  private closeListeners: (() => void)[] = [];
  private isClosed = false;
  private readonly stream: Duplex;
  /** Строки, ждущие 'drain' потока. */
  private queue: string[] = [];
  private needDrain = false;
  private dropped = 0;
  private readonly log: Logger;

  constructor(stream: Duplex, opts: { log?: Logger } = {}) {
    this.stream = stream;
    this.log = opts.log ?? (() => {});
    stream.setEncoding("utf8");
    stream.on("data", (chunk: string) => this.read(chunk));
    stream.on("drain", () => this.drainQueue());
    stream.on("end", () => this.shutdown());
    stream.on("close", () => this.shutdown());
    stream.on("error", () => this.shutdown());
  }

  /** Канал, унаследованный от агента; без агента — понятная ошибка. */
  static fromEnv(opts: { log?: Logger } = {}): Channel {
    const fd = Number(process.env.AGENT_IPC_FD);
    if (!process.env.AGENT_IPC_FD || !Number.isInteger(fd)) {
      throw new Error("воркер запускается агентом (AGENT_IPC_FD не задан): опишите его в workers конфигурации агента");
    }
    return new Channel(new Socket({ fd, readable: true, writable: true }), opts);
  }

  get closed(): boolean {
    return this.isClosed;
  }

  /**
   * Отправить конверт; канал закрыт — false (агента нет, слать некому). Данные не
   * превращаются в JSON (BigInt, цикл) — TypeError; строка больше 16 МБ —
   * MessageTooLargeError; в обоих случаях канал цел. Пока поток не готов
   * принимать (обратное давление), строки ждут в очереди.
   */
  send(type: string, data?: unknown, opts: { id?: string; re?: string } = {}): boolean {
    if (this.isClosed || this.stream.destroyed || !this.stream.writable) return false;
    const line = JSON.stringify(envelope(type, data, opts)) + "\n";
    if (Buffer.byteLength(line) > MAX_LINE_BYTES) throw new MessageTooLargeError(type);
    if (this.needDrain) {
      if (DROPPABLE.has(type) && this.queue.length >= WRITE_QUEUE_SOFT_LIMIT) {
        if (this.dropped++ === 0) this.log("warn", "канал с агентом не успевает: прогресс и телеметрия отбрасываются");
        return true;
      }
      this.queue.push(line);
      return true;
    }
    this.write(line);
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
      let sent: boolean;
      try {
        sent = this.send(type, data, { id });
      } catch (err) {
        clearTimeout(timer);
        this.pending.delete(id);
        return reject(err);
      }
      if (!sent) {
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
    // Накопленное при заторе — дописать, если поток ещё принимает.
    for (const line of this.queue.splice(0)) if (this.stream.writable) this.stream.write(line);
    this.stream.end();
    this.stream.destroy();
    this.shutdown();
  }

  private write(line: string): void {
    if (!this.stream.write(line)) this.needDrain = true;
  }

  /** 'drain': дописать очередь, пока поток принимает. */
  private drainQueue(): void {
    this.needDrain = false;
    while (this.queue.length && !this.needDrain && !this.isClosed) this.write(this.queue.shift()!);
    if (!this.queue.length && !this.needDrain && this.dropped) {
      this.log("warn", `канал с агентом снова успевает; отброшено сообщений: ${this.dropped}`);
      this.dropped = 0;
    }
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
    this.queue = [];
    for (const [id, p] of this.pending) {
      clearTimeout(p.timer);
      p.reject(new AgentError("CHANNEL_CLOSED", "канал с агентом закрыт"));
      this.pending.delete(id);
    }
    for (const fn of this.closeListeners.splice(0)) fn();
  }
}
