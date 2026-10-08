// Команда в руках обработчика: аргументы, вывод потоком, отмена по сроку.
import type { CommandRun } from "../index";
import type { Channel } from "./channel";

/** Символов в одном cmd.output: UTF-8 — до 4 байт на символ, по спецификации — ≤ 64 КБ. */
const CHUNK_CHARS = 16 * 1024;

/**
 * Команда, переданная воркеру агентом (`cmd.run`). Вывод (`write`) уходит на
 * сервер по мере записи; результат обработчика — итог. Срок истёк
 * (`cmd.cancel`) — `signal` прерывается, итог уже не отправляется.
 */
export class Command {
  readonly id: string;
  readonly name: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- аргументы команды — JSON, схема у воркера; any — удобство публичного API
  readonly args: any;
  readonly timeoutSec: number;
  private readonly ac = new AbortController();
  private readonly channel: Channel;

  /** @internal Создаёт Worker по cmd.run. */
  constructor(channel: Channel, run: CommandRun) {
    this.channel = channel;
    this.id = run.commandId;
    this.name = run.name;
    this.args = run.args ?? {};
    this.timeoutSec = run.timeoutSec ?? 60;
  }

  get signal(): AbortSignal {
    return this.ac.signal;
  }

  get cancelled(): boolean {
    return this.ac.signal.aborted;
  }

  /** Вывод команды (виден на сервере по мере выполнения). */
  write(text: string): void {
    const data = String(text);
    for (let i = 0; i < data.length; i += CHUNK_CHARS) {
      this.channel.send("cmd.output", { commandId: this.id, chunk: data.slice(i, i + CHUNK_CHARS) });
    }
  }

  /** @internal */
  cancel(reason = "срок команды истёк"): void {
    this.ac.abort(new DOMException(reason, "AbortError"));
  }
}
