// Задача в руках обработчика: данные, прогресс, лог, события, файлы, отмена.
import { createWriteStream, openAsBlob } from "node:fs";
import { mkdir, mkdtemp, rename, rm, stat, unlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as WebReadableStream } from "node:stream/web";
import type { JobAssign, JobRef, JobUrls, OutputUrl } from "../index";
import type { Channel } from "./channel";

/** Настройки задач (тесты уменьшают паузы). */
export const jobDefaults = {
  /** Прогресс и лог уходят агенту не чаще этого: частые вызовы схлопываются. */
  progressIntervalMs: 500,
  /** Строк лога в одном job.progress. */
  logBatch: 100,
  /** Попыток загрузки выходного файла (каждая следующая — со свежей ссылкой). */
  uploadAttempts: 4,
  /** Пауза перед повтором загрузки, мс (растёт с номером попытки). */
  uploadRetryMs: 5000,
  /** Ссылку, истекающую раньше этого, обновить до использования. */
  urlRefreshMarginMs: 60_000,
};

type Logger = (level: "info" | "warn" | "error", msg: string) => void;

/**
 * Задача, выданная воркеру агентом. Связь с сервером, повторы и досылка после
 * обрыва — забота агента. Отмена (`job.cancel`) — `signal` прерывается, итог не
 * отправляется; остановка (`job.stop`) — `stopRequested`: довести шаг и
 * вернуть результат как обычно.
 */
export class Job {
  readonly id: string;
  readonly queue: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- данные задачи — JSON, схема у воркера; any — удобство публичного API
  readonly data: any;
  readonly attempt: number;
  readonly leaseSeconds: number;
  private readonly ac = new AbortController();
  private stop = false;
  private inputUrls: Record<string, string>;
  private outputUrls: Record<string, OutputUrl>;
  private expiresAt: number;
  private pendingProgress?: number;
  private pendingText?: string;
  private pendingLog: string[] = [];
  private lastSent = 0;
  private timer?: NodeJS.Timeout;
  private eventSeq = 0;
  private tmp?: Promise<string>;
  private readonly channel: Channel;
  private readonly logger: Logger;

  /** @internal Создаёт Worker по job.assign. */
  constructor(channel: Channel, assign: JobAssign, logger: Logger) {
    this.channel = channel;
    this.logger = logger;
    this.id = assign.jobId;
    this.queue = assign.queue;
    this.data = assign.data ?? {};
    this.attempt = assign.attempt ?? 0;
    this.leaseSeconds = assign.leaseSeconds ?? 60;
    this.inputUrls = { ...(assign.inputs ?? {}) };
    this.outputUrls = { ...(assign.outputs ?? {}) };
    this.expiresAt = assign.urlsExpireAt ?? 0;
  }

  get ref(): JobRef {
    return { jobId: this.id, attempt: this.attempt };
  }

  /** Прерывается при отмене задачи (`job.cancel`) и при закрытии канала. */
  get signal(): AbortSignal {
    return this.ac.signal;
  }

  get cancelled(): boolean {
    return this.ac.signal.aborted;
  }

  /** Попросили завершиться досрочно: довести шаг и вернуть результат. */
  get stopRequested(): boolean {
    return this.stop;
  }

  /** Прогресс 0..1 и что делается сейчас; частые вызовы схлопываются. */
  progress(value: number, text?: string): void {
    this.pendingProgress = Math.max(0, Math.min(1, Number(value) || 0));
    if (text !== undefined) this.pendingText = String(text).slice(0, 200);
    this.schedule();
  }

  /** Строка лога задачи (хвост виден на сервере). */
  log(line: string): void {
    this.pendingLog.push(String(line).slice(0, 1000));
    if (this.pendingLog.length >= jobDefaults.logBatch) this.flush();
    else this.schedule();
  }

  /** Доменное событие задачи (итог этапа и т. п.): надёжно, по порядку (`seq`). */
  event(type: string, data?: unknown): void {
    this.flush();
    this.eventSeq++;
    this.channel.send("job.event", { ...this.ref, seq: this.eventSeq, type: String(type).slice(0, 50), data });
  }

  get inputs(): string[] {
    return Object.keys(this.inputUrls);
  }

  get outputs(): string[] {
    return Object.keys(this.outputUrls);
  }

  /** Скачать входной файл во временный каталог задачи (один раз); путь к нему. */
  async inputPath(name: string): Promise<string> {
    const target = join(await this.tmpDir(), "inputs", name);
    const exists = await stat(target).then(
      () => true,
      () => false,
    );
    return exists ? target : this.download(name, target);
  }

  /** Скачать входной файл в указанное место атомарно; путь к нему. */
  async download(name: string, target: string): Promise<string> {
    if (!(name in this.inputUrls)) throw new Error(`нет входного файла ${name}`);
    if (this.expiring()) await this.refreshUrls([name], undefined);
    try {
      return await download(this.inputUrls[name], target, this.signal);
    } catch (err) {
      if (this.cancelled) throw err;
      // Ссылка могла истечь (долгая задача) — свежая и ещё раз.
      await this.refreshUrls([name], undefined);
      return download(this.inputUrls[name], target, this.signal);
    }
  }

  /**
   * Загрузить выходной файл по подписанной ссылке (PUT): `source` — путь к файлу
   * (string) или содержимое (Uint8Array). Сбой — повтор со свежей ссылкой.
   */
  async upload(name: string, source: string | Uint8Array): Promise<void> {
    if (!(name in this.outputUrls)) throw new Error(`нет выходного файла ${name}`);
    if (this.expiring()) await this.refreshUrls(undefined, [name]);
    for (let attempt = 1; ; attempt++) {
      try {
        await upload(this.outputUrls[name], source, this.signal);
        return;
      } catch (err) {
        if (this.cancelled || attempt >= jobDefaults.uploadAttempts) throw err;
        this.logger("warn", `загрузка ${name} задачи ${this.id}: ${(err as Error).message} — повтор`);
        await sleep(jobDefaults.uploadRetryMs * attempt, this.signal);
        try {
          await this.refreshUrls(undefined, [name]);
        } catch (e) {
          this.logger("warn", `свежая ссылка ${name} не получена: ${(e as Error).message}`);
        }
      }
    }
  }

  /** Свежие подписанные ссылки (все или перечисленные). */
  async refreshUrls(inputs?: string[], outputs?: string[]): Promise<void> {
    const req: Record<string, unknown> = { ...this.ref };
    if (inputs) req.inputs = inputs;
    if (outputs) req.outputs = outputs;
    const urls = await this.channel.request<Partial<JobUrls>>("job.urls", req);
    Object.assign(this.inputUrls, urls?.inputs ?? {});
    Object.assign(this.outputUrls, urls?.outputs ?? {});
    if (urls?.expiresAt) this.expiresAt = urls.expiresAt;
  }

  // ── жизненный цикл (вызывает Worker) ──

  /** @internal Отправить накопленный прогресс и лог сейчас. */
  flush(): void {
    clearTimeout(this.timer);
    this.timer = undefined;
    if (this.pendingProgress === undefined && this.pendingText === undefined && this.pendingLog.length === 0) return;
    const update: Record<string, unknown> = { ...this.ref };
    if (this.pendingProgress !== undefined) update.progress = this.pendingProgress;
    if (this.pendingText !== undefined) update.text = this.pendingText;
    if (this.pendingLog.length) update.log = this.pendingLog.splice(0, jobDefaults.logBatch);
    this.pendingProgress = undefined;
    this.pendingText = undefined;
    this.lastSent = Date.now();
    this.channel.send("job.progress", update);
    if (this.pendingLog.length) this.schedule();
  }

  /** @internal */
  cancel(reason = "задача отменена"): void {
    if (!this.cancelled) this.logger("info", `задача ${this.id}: ${reason}`);
    this.ac.abort(new DOMException(reason, "AbortError"));
  }

  /** @internal */
  requestStop(): void {
    if (!this.stop) this.logger("info", `задачу ${this.id} просят завершить досрочно`);
    this.stop = true;
  }

  /** @internal */
  async close(): Promise<void> {
    clearTimeout(this.timer);
    this.timer = undefined;
    if (this.tmp) await rm(await this.tmp, { recursive: true, force: true }).catch(() => {});
  }

  private schedule(): void {
    if (this.timer) return;
    const wait = this.lastSent + jobDefaults.progressIntervalMs - Date.now();
    if (wait <= 0) return this.flush();
    this.timer = setTimeout(() => this.flush(), wait);
    this.timer.unref();
  }

  private expiring(): boolean {
    return this.expiresAt > 0 && this.expiresAt - Date.now() < jobDefaults.urlRefreshMarginMs;
  }

  private tmpDir(): Promise<string> {
    this.tmp ??= mkdtemp(join(tmpdir(), `job-${this.id.slice(0, 8)}-`));
    return this.tmp;
  }
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(signal.reason);
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

/** Скачать атомарно: файл появляется целиком или не появляется. */
async function download(url: string, target: string, signal: AbortSignal): Promise<string> {
  await mkdir(dirname(target), { recursive: true });
  const partial = `${target}.${process.pid}.part`;
  try {
    const res = await fetch(url, { signal });
    if (!res.ok || !res.body) throw new Error(`GET ${res.status}`);
    await pipeline(Readable.fromWeb(res.body as WebReadableStream<Uint8Array>), createWriteStream(partial), { signal });
    await rename(partial, target);
  } finally {
    await unlink(partial).catch(() => {});
  }
  return target;
}

/** PUT по подписанной ссылке; Content-Type — ровно подписанный. */
async function upload(target: OutputUrl, source: string | Uint8Array, signal: AbortSignal): Promise<void> {
  const headers: Record<string, string> = {};
  if (target.contentType) headers["Content-Type"] = target.contentType;
  const body = typeof source === "string" ? await openAsBlob(source) : source;
  const res = await fetch(target.url, { method: "PUT", body: body as BodyInit, headers, signal });
  await res.arrayBuffer().catch(() => undefined);
  if (!res.ok) throw new Error(`PUT ${res.status}`);
}
