// Сессия агента — одно соединение WebSocket: исходящее копится в очереди, транспорт пишет его по
// мере появления; входящее обрабатывается строго по порядку.
import { type Envelope, newId } from "../protocol/messages";

/** Запрос fetch, ждущий ответа в этой сессии. */
export interface PendingFetch {
  head(status: number, headers: Record<string, string>): void;
  chunk(data: Uint8Array): void;
  end(error?: { code: string; message: string }): void;
}

export class Session {
  readonly id = newId();
  readonly agentId: string;
  /** Адрес сервера, по которому агент до него дошёл. */
  readonly baseUrl: string;
  /** IP агента; пусто — неизвестен. */
  readonly address: string;
  closed = false;
  code = 0;
  reason = "";
  /** Получен hello и отправлен welcome. */
  welcomed = false;
  /** Запросы fetch этой сессии: id → ожидающий. */
  readonly fetches = new Map<string, PendingFetch>();
  /** Версии настроек, известные агенту: "воркер/ключ" → версия (из hello и отправленных config.put). */
  readonly known = new Map<string, number>();
  /** Последний отправленный watch (JSON сводки). */
  watchSent = "{}";
  private outq: Envelope[] = [];
  private ackIds: string[] = [];
  private ackSeq = 0;
  private ackScheduled = false;
  private waiters = new Set<() => void>();
  private chain: Promise<unknown> = Promise.resolve();

  constructor(agentId: string, baseUrl: string, address = "") {
    this.agentId = agentId;
    this.baseUrl = baseUrl;
    this.address = address;
  }

  send(env: Envelope): void {
    if (this.closed) return;
    this.outq.push(env);
    this.wake();
  }

  /** Подтвердить важное (id) или поток (seq); подтверждения собираются в одно ack. */
  ack(ids: string[], seq = 0): void {
    this.ackIds.push(...ids);
    if (seq > this.ackSeq) this.ackSeq = seq;
    if (this.ackScheduled) return;
    this.ackScheduled = true;
    setImmediate(() => this.flushAck());
  }

  /** Отправить накопленные подтверждения сейчас. */
  flushAck(): void {
    this.ackScheduled = false;
    const data: { ids?: string[]; seq?: number } = {};

    if (this.ackIds.length) data.ids = this.ackIds;
    if (this.ackSeq) data.seq = this.ackSeq;
    this.ackIds = [];
    this.ackSeq = 0;
    if (data.ids || data.seq) this.send({ type: "ack", data });
  }

  take(): Envelope[] {
    const out = this.outq;

    this.outq = [];

    return out;
  }

  close(code: number, reason = ""): void {
    if (this.closed) return;
    this.closed = true;
    this.code = code;
    this.reason = reason;
    for (const f of this.fetches.values())
      f.end({ code: "DISCONNECTED", message: "связь с агентом оборвалась" });
    this.fetches.clear();
    this.wake();
  }

  /** Подписка на новое исходящее или закрытие; возвращает отписку. */
  onWake(fn: () => void): () => void {
    this.waiters.add(fn);

    return () => this.waiters.delete(fn);
  }

  /** Выполнять строго по порядку. */
  serial<T>(fn: () => Promise<T>): Promise<T> {
    const next = this.chain.then(fn, fn);

    this.chain = next.catch(() => {});

    return next;
  }

  private wake(): void {
    for (const fn of [...this.waiters]) fn();
  }
}
