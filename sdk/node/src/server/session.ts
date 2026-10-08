// Сессия агента: исходящее копится в очереди; WebSocket пишет его сразу,
// HTTP sync отдаёт ответом на запрос. Логика сессии от транспорта не зависит.
import { envelope, newId, type Envelope } from "../index";

export class Session {
  readonly id = newId();
  readonly agentId: string;
  readonly mode: "ws" | "http";
  /** Адрес сервера, по которому агент до него дошёл: ссылки на файлы задач. */
  readonly baseUrl: string;
  /** Адрес агента (IP без порта), с которого пришло подключение; пусто — неизвестен. */
  readonly address: string;
  outq: Envelope[] = [];
  closed = false;
  code = 0;
  /** Выданные задачи, ещё не учтённые в status.slots: jobId → очередь (занимают слот). */
  readonly pending = new Map<string, string>();
  /** Команды, отправленные в этой сессии (дедупликация доставки). */
  readonly sent = new Set<string>();
  /** В этой сессии пришёл status: его slots — основание раздачи. */
  statusSeen = false;
  /** Домен → версия, известная агенту. */
  readonly known = new Map<string, number>();
  private waiters = new Set<() => void>();
  private chain: Promise<unknown> = Promise.resolve();

  constructor(agentId: string, mode: "ws" | "http", baseUrl: string, address = "") {
    this.agentId = agentId;
    this.mode = mode;
    this.baseUrl = baseUrl;
    this.address = address;
  }

  send(type: string, data: unknown, re?: string): void {
    if (this.closed) return;
    this.outq.push(envelope(type, data, { re }));
    this.wake();
  }

  take(): Envelope[] {
    const out = this.outq;
    this.outq = [];
    return out;
  }

  close(code: number): void {
    if (this.closed) return;
    this.closed = true;
    this.code = code;
    this.wake();
  }

  /** Подписка на новое исходящее или закрытие; возвращает отписку. */
  onWake(fn: () => void): () => void {
    this.waiters.add(fn);
    return () => this.waiters.delete(fn);
  }

  /** Обработка сообщений сессии строго по порядку (Store асинхронный). */
  serial<T>(fn: () => Promise<T>): Promise<T> {
    const next = this.chain.then(fn, fn);
    this.chain = next.catch(() => {});
    return next;
  }

  private wake(): void {
    for (const fn of [...this.waiters]) fn();
  }
}
