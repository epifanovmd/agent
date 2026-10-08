// Воркер агента (§10): объявляет очереди, команды, домены состояния и каналы
// телеметрии; выполняет задачи, команды и применения состояния параллельно.
import type { Duplex } from "node:stream";
import {
  NAME_PATTERN,
  SDK_VERSION,
  validName,
  type CommandDone,
  type CommandRun,
  type Envelope,
  type JobAssign,
  type JobRef,
  type StateApplied,
  type StatePut,
  type WorkerCleaned,
  type WorkerContext,
  type WorkerReady,
  type WorkerRegister,
} from "../index";
import { Channel } from "./channel";
import { Command } from "./command";
import { CommandError, JobError, StateError } from "./errors";
import { Job } from "./job";

export type JobHandler = (job: Job) => unknown | Promise<unknown>;
export type CommandHandler = (cmd: Command) => unknown | Promise<unknown>;
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- spec — JSON домена, схема у воркера; any — удобство публичного API
export type StateHandler = (version: number, spec: any) => unknown | Promise<unknown>;
export type CleanupHandler = () => unknown | Promise<unknown>;
export type TelemetrySource = () => unknown | Promise<unknown>;
export type LogLevel = "info" | "warn" | "error";
export type ContextHandler = (ctx: WorkerContext) => void;

/**
 * «Авто» частота источника телеметрии: интервал подписки сервера на этот канал
 * (worker.context.channels), если она есть; иначе частота метрик агента
 * (worker.context.metricsIntervalMs); неизвестна — 15 с.
 */
export const AUTO_INTERVAL = "auto";
const DEFAULT_INTERVAL_MS = 15_000;

export interface TelemetryOptions {
  /** Интервал опроса, мс, или "auto" (AUTO_INTERVAL) — по подписке и частоте метрик агента (по умолчанию 15000). */
  intervalMs?: number | typeof AUTO_INTERVAL;
}

/** worker.context.channels: только каналы с интервалом > 0; пусто — undefined. */
function contextChannels(v: unknown): Record<string, number> | undefined {
  if (!v || typeof v !== "object" || Array.isArray(v)) return undefined;
  const out: Record<string, number> = {};
  for (const [k, ms] of Object.entries(v)) if (typeof ms === "number" && ms > 0) out[k] = ms;
  return Object.keys(out).length ? out : undefined;
}

/** Контекст до первого worker.context. */
function defaultContext(): WorkerContext {
  return {
    mode: "run",
    agent: { id: "", name: "", version: "", labels: {} },
    online: false,
    metricsIntervalMs: 0,
    logLevel: "",
  };
}

export interface WorkerOptions {
  /** Имя воркера; по умолчанию — AGENT_WORKER (агент передаёт имя из конфигурации). */
  name?: string;
  version?: string;
  /** Свой транспорт (тесты, unix-сокет); по умолчанию — канал агента AGENT_IPC_FD. */
  transport?: Duplex;
  /** Лог SDK; по умолчанию — stderr (агент пишет его в свой журнал). */
  log?: (level: LogLevel, msg: string) => void;
}

export interface RunOptions {
  /** SIGTERM/SIGINT → drain (по умолчанию true). */
  signals?: boolean;
}

interface Queue {
  concurrency: number;
  fn: JobHandler;
  running: number;
  waiting: Job[];
}

/**
 * Воркер: обработчики и цикл приёма сообщений агента. Сеть, повторы, досылка
 * итогов, учётные данные и обновление — забота агента.
 *
 *     const w = new Worker({ name: "report", version: "1.0.0" });
 *     w.job("example.convert", { concurrency: 2 }, async (job) => ({ ok: true }));
 *     w.command("example.app.reload", async (cmd) => ({ reloaded: true }));
 *     w.state("example.app", async (version, spec) => ({ listeners: 1 }));
 *     w.telemetry("example.app", { intervalMs: 15000 }, () => ({ connections: 3 }));
 *     w.telemetry("example.app.load", { intervalMs: "auto" }, () => ({ queue: 0 }));
 *     w.on("context", (ctx) => console.log(ctx.online));
 *     w.event("app.started", { port: 8080 });
 *     await w.run();
 *
 * Остановка — SIGTERM или `worker.drain`: новые задачи не берутся, текущие
 * задачи и команды дорабатываются, run() завершается. Канал закрыт (агента
 * нет) — всё отменяется.
 */
export class Worker {
  readonly name: string;
  readonly version: string;
  /** Имена, которые агент отклонил (зарезервированы или заняты другим воркером). */
  rejected: string[] = [];
  private readonly transport?: Duplex;
  private readonly logFn: (level: LogLevel, msg: string) => void;
  private readonly queues = new Map<string, Queue>();
  private readonly commands = new Map<string, CommandHandler>();
  private readonly domains = new Map<string, StateHandler>();
  private readonly channels: string[] = [];
  private cleanupFn?: CleanupHandler;
  private readonly sources = new Map<string, { intervalMs: number | typeof AUTO_INTERVAL; fn: TelemetrySource }>();
  private readonly jobs = new Map<string, Job>();
  private readonly running = new Map<string, Command>();
  /** Таймеры источников телеметрии: канал → таймер и его интервал. */
  private readonly timers = new Map<string, { timer: NodeJS.Timeout; ms: number }>();
  private started = false;
  private ctx: WorkerContext = defaultContext();
  private readonly contextHandlers: ContextHandler[] = [];
  private readonly early: [string, unknown][] = [];
  private busy = 0;
  private draining = false;
  private chan?: Channel;
  private finish?: () => void;

  constructor(opts: WorkerOptions = {}) {
    this.name = opts.name ?? process.env.AGENT_WORKER ?? "worker";
    this.version = opts.version ?? "0.0.0";
    this.transport = opts.transport;
    this.logFn =
      opts.log ?? ((level, msg) => process.stderr.write(`${level.toUpperCase()} agent-sdk ${this.name}: ${msg}\n`));
  }

  // ── объявления ──

  /** Обработчик очереди: результат — итог задачи; JobError — провал с кодом. */
  job(queue: string, handler: JobHandler): this;
  job(queue: string, opts: { concurrency?: number }, handler: JobHandler): this;
  job(queue: string, a: { concurrency?: number } | JobHandler, b?: JobHandler): this {
    checkName("очередь", queue);
    const [opts, fn] = typeof a === "function" ? [{}, a] : [a, b!];
    const concurrency = opts.concurrency ?? 1;
    if (!Number.isInteger(concurrency) || concurrency < 1) throw new Error("concurrency — целое не меньше 1");
    this.queues.set(queue, { concurrency, fn, running: 0, waiting: [] });
    return this;
  }

  /** Команда: результат — итог; CommandError — ошибка с кодом. Имена agent.* и worker.* — агента. */
  command(name: string, handler: CommandHandler): this {
    checkName("команда", name);
    this.commands.set(name, handler);
    return this;
  }

  /**
   * Домен желаемого состояния: `handler(version, spec)` → отчёт. Применение
   * идемпотентно: агент присылает последний снимок после каждой регистрации и
   * при новой версии; исключение — снимок не применён, агент повторит.
   */
  state(domain: string, handler: StateHandler): this {
    checkName("раздел состояния", domain);
    this.domains.set(domain, handler);
    return this;
  }

  /** Объявить канал телеметрии: данные — report(name, data). */
  channel(name: string): this {
    checkName("канал показателей", name);
    if (!this.channels.includes(name)) this.channels.push(name);
    return this;
  }

  /**
   * Источник канала телеметрии: опрашивается раз в intervalMs после worker.ready.
   * intervalMs: "auto" (AUTO_INTERVAL) — с частотой подписки сервера на канал
   * (worker.context.channels), без неё — с частотой метрик агента
   * (worker.context.metricsIntervalMs); меняется вместе с ними, неизвестна — 15 с.
   */
  telemetry(name: string, opts: TelemetryOptions, fn: TelemetrySource): this {
    const iv = opts.intervalMs ?? DEFAULT_INTERVAL_MS;
    if (iv !== AUTO_INTERVAL && (typeof iv !== "number" || !(iv > 0)))
      throw new Error(`intervalMs — число больше 0 или "${AUTO_INTERVAL}"`);
    this.channel(name);
    this.sources.set(name, { intervalMs: iv, fn });
    return this;
  }

  /**
   * Уборка при удалении агента с узла (`agent cleanup`): снять то, что воркер
   * оставил в системе. Связи с сервером в этот момент нет — события не уйдут.
   * Нет обработчика — агенту сразу `{ok: true}`; исключение — `{ok: false, error}`.
   */
  cleanup(handler: CleanupHandler): this {
    this.cleanupFn = handler;
    return this;
  }

  /**
   * Подписка на worker.context: агент шлёт его после worker.ready и при каждом изменении
   * (связь, частота метрик, подписки на каналы, уровень логов). Исключение обработчика — в лог.
   */
  on(event: "context", handler: ContextHandler): this {
    if (event !== "context") throw new Error(`неизвестное событие ${String(event)}`);
    this.contextHandlers.push(handler);
    return this;
  }

  /** Последний worker.context; до первого — mode run, online false, metricsIntervalMs 0. */
  get context(): WorkerContext {
    return this.ctx;
  }

  // ── из обработчиков ──

  /** Сообщить агенту, в порядке ли воркер: ok false — degraded с причиной message (status, alert workerDegraded). */
  setHealth(ok: boolean, message?: string): void {
    const data: { ok: boolean; message?: string } = { ok: !!ok };
    if (message) data.message = String(message).slice(0, 2000);
    this.send("worker.health", data);
  }

  /** Не брать новые задачи очередей queues (без списка — всех своих); выданные доделываются. */
  pause(queues?: string[]): void {
    this.send("worker.pause", this.queueList(queues));
  }

  /** Снова брать задачи очередей queues (без списка — всех своих); пауза от сервера остаётся. */
  resume(queues?: string[]): void {
    this.send("worker.resume", this.queueList(queues));
  }

  /** Попросить агента заменить воркер штатно (его способом rolling/stop-first), без статуса сбоя. */
  requestRestart(reason?: string): void {
    this.send("worker.restart", reason ? { reason: String(reason).slice(0, 2000) } : {});
  }

  /** Последние данные канала: уйдут в ближайший metrics агента. */
  report(channel: string, data: unknown): void {
    if (!this.channels.includes(channel)) throw new Error(`канал ${channel} не объявлен: worker.channel("${channel}")`);
    this.send("telemetry", { channel, data });
  }

  /** Событие воркера серверу; доставка надёжная (агент хранит до подтверждения). */
  event(type: string, data?: unknown): void {
    this.send("event", { type: String(type).slice(0, 50), data });
  }

  // ── работа ──

  /** Зарегистрироваться у агента и работать до остановки. */
  run(opts: RunOptions = {}): Promise<void> {
    if (this.chan) throw new Error("воркер уже запущен");
    if (!this.queues.size && !this.commands.size && !this.domains.size && !this.channels.length && !this.cleanupFn) {
      throw new Error("нечего объявить: нужны очередь, команда, домен, канал телеметрии или уборка");
    }
    const chan = this.transport ? new Channel(this.transport) : Channel.fromEnv();
    this.chan = chan;
    const register: WorkerRegister = {
      name: this.name,
      version: this.version,
      sdk: `node/${SDK_VERSION}`,
      queues: [...this.queues].map(([name, q]) => ({ name, concurrency: q.concurrency })),
    };
    if (this.commands.size) register.commands = [...this.commands.keys()];
    if (this.domains.size) register.domains = [...this.domains.keys()];
    if (this.channels.length) register.channels = [...this.channels];
    chan.send("worker.register", register);
    for (const [type, data] of this.early.splice(0)) chan.send(type, data);

    const onSignal = () => this.drain();
    if (opts.signals ?? true) {
      process.on("SIGTERM", onSignal);
      process.on("SIGINT", onSignal);
    }
    return new Promise<void>((resolve) => {
      this.finish = () => {
        this.finish = undefined;
        this.stopTimers();
        process.off("SIGTERM", onSignal);
        process.off("SIGINT", onSignal);
        chan.close();
        this.log("info", `воркер ${this.name} остановлен`);
        resolve();
      };
      chan.onMessage((env) => this.dispatch(env));
      chan.onClose(() => {
        // Агента нет: итоги некому отдать — всё прервать.
        for (const job of this.jobs.values()) job.cancel("канал с агентом закрыт");
        for (const cmd of this.running.values()) cmd.cancel("канал с агентом закрыт");
        this.draining = true;
        this.checkIdle();
      });
    });
  }

  private readonly stopController = new AbortController();

  /** Срабатывает, когда воркер уходит (worker.drain, SIGTERM, drain()): свои фоновые циклы по нему останавливаются. */
  get stopping(): AbortSignal {
    return this.stopController.signal;
  }

  /** Не брать новых задач; завершиться после текущих задач и команд. */
  drain(): void {
    if (!this.draining) this.log("info", `воркер ${this.name} дорабатывает задачи и завершается`);
    this.draining = true;
    this.stopController.abort();
    this.stopTimers();
    this.checkIdle();
  }

  // ── внутреннее ──

  private log(level: LogLevel, msg: string): void {
    try {
      this.logFn(level, msg);
    } catch {
      // лог не должен ронять воркер
    }
  }

  private send(type: string, data: unknown, opts?: { re?: string }): void {
    if (!this.chan) {
      this.early.push([type, data]);
      return;
    }
    if (!this.chan.send(type, data, opts)) this.log("warn", `${type} не отправлено: канал с агентом закрыт`);
  }

  private dispatch(env: Envelope): void {
    const d = env.data ?? {};
    switch (env.type) {
      case "worker.ready":
        return this.ready(d as WorkerReady);
      case "worker.drain":
        return this.drain();
      case "worker.context":
        return this.setContext(d as Partial<WorkerContext>);
      case "job.assign":
        return this.assign(d as JobAssign);
      case "job.cancel":
      case "job.stop": {
        const ref = d as JobRef;
        const job = this.jobs.get(ref.jobId);
        if (!job || job.attempt !== ref.attempt) return;
        if (env.type === "job.cancel") {
          job.cancel();
          this.dequeue(job);
        } else job.requestStop();
        return;
      }
      case "cmd.run":
        return void this.track(this.runCommand(d as CommandRun));
      case "cmd.cancel":
        return this.running.get(d.commandId)?.cancel();
      case "state.put":
        return void this.track(this.applyState(d as StatePut, env.id));
      case "worker.cleanup":
        return void this.track(this.runCleanup(env.id));
      default:
        this.log("warn", `неизвестное сообщение агента: ${env.type}`);
    }
  }

  private ready(d: WorkerReady): void {
    this.rejected = d.rejected ?? [];
    if (this.rejected.length)
      this.log("warn", `агент отклонил имена (зарезервированы или заняты): ${this.rejected.join(", ")}`);
    this.log("info", `воркер ${this.name} зарегистрирован у агента ${d.agentVersion ?? "?"}`);
    if (this.draining || this.started) return;
    this.started = true;
    for (const name of this.sources.keys()) this.schedule(name, true);
  }

  /** Таймер источника по его интервалу (now — сразу опросить); интервал тот же — не трогать. */
  private schedule(name: string, now = false): void {
    const src = this.sources.get(name)!;
    const ms =
      src.intervalMs === AUTO_INTERVAL
        ? this.ctx.channels?.[name] || this.ctx.metricsIntervalMs || DEFAULT_INTERVAL_MS
        : src.intervalMs;
    const cur = this.timers.get(name);
    if (cur?.ms === ms) return;
    if (cur) clearInterval(cur.timer);
    const poll = async () => {
      try {
        this.report(name, await src.fn());
      } catch (err) {
        this.log("error", `телеметрия ${name} не собрана: ${(err as Error).message}`);
      }
    };
    if (now) void poll();
    const timer = setInterval(poll, ms);
    timer.unref();
    this.timers.set(name, { timer, ms });
  }

  private stopTimers(): void {
    for (const t of this.timers.values()) clearInterval(t.timer);
    this.timers.clear();
  }

  /** worker.context: запомнить, перестроить источники "auto", известить подписчиков. */
  private setContext(d: Partial<WorkerContext>): void {
    const def = defaultContext();
    const agent = (d.agent ?? {}) as Partial<WorkerContext["agent"]>;
    this.ctx = {
      ...d,
      mode: typeof d.mode === "string" && d.mode ? d.mode : def.mode,
      agent: {
        ...agent,
        id: agent.id ?? "",
        name: agent.name ?? "",
        version: agent.version ?? "",
        labels: agent.labels ?? {},
      },
      online: d.online === true,
      metricsIntervalMs: typeof d.metricsIntervalMs === "number" && d.metricsIntervalMs > 0 ? d.metricsIntervalMs : 0,
      logLevel: typeof d.logLevel === "string" ? d.logLevel : "",
    };
    const channels = contextChannels(d.channels);
    if (channels) this.ctx.channels = channels;
    else delete this.ctx.channels;
    if (this.started && !this.draining) {
      for (const [name, src] of this.sources) if (src.intervalMs === AUTO_INTERVAL) this.schedule(name);
    }
    for (const fn of this.contextHandlers) {
      try {
        fn(this.ctx);
      } catch (err) {
        this.log("error", `обработчик context упал: ${(err as Error)?.stack ?? err}`);
      }
    }
  }

  /** Список очередей pause/resume: пусто — {} (все очереди воркера). */
  private queueList(queues: string[] | undefined): { queues?: string[] } {
    if (!queues?.length) return {};
    for (const q of queues) checkName("очередь", q);
    return { queues: [...new Set(queues)] };
  }

  private assign(d: JobAssign): void {
    if (this.jobs.has(d.jobId)) return; // повторная доставка
    const q = this.queues.get(d.queue);
    if (this.draining || !q) {
      this.send("job.fail", {
        jobId: d.jobId,
        attempt: d.attempt,
        code: "WORKER_STOPPING",
        retryable: true,
        message: `Воркер ${this.name} не берёт задачу очереди ${d.queue}`,
      });
      return;
    }
    const job = new Job(this.chan!, d, (level, msg) => this.log(level, msg));
    this.jobs.set(job.id, job);
    if (q.running < q.concurrency) void this.execute(q, job);
    else q.waiting.push(job);
  }

  /** Отменённая задача из ожидания — сразу прочь. */
  private dequeue(job: Job): void {
    const q = this.queues.get(job.queue);
    const i = q?.waiting.indexOf(job) ?? -1;
    if (q && i >= 0) {
      q.waiting.splice(i, 1);
      this.jobs.delete(job.id);
      void job.close();
      this.checkIdle();
    }
  }

  private async execute(q: Queue, job: Job): Promise<void> {
    q.running++;
    try {
      const result = await q.fn(job);
      if (job.cancelled) return;
      job.flush();
      this.send("job.complete", { ...job.ref, result });
    } catch (err) {
      if (job.cancelled) return;
      job.flush();
      if (err instanceof JobError) {
        this.send("job.fail", {
          ...job.ref,
          code: err.code,
          message: err.message.slice(0, 2000),
          retryable: err.retryable,
        });
      } else {
        this.log("error", `задача ${job.id} упала: ${(err as Error)?.stack ?? err}`);
        this.send("job.fail", { ...job.ref, code: "WORKER_ERROR", message: describe(err), retryable: true });
      }
    } finally {
      q.running--;
      this.jobs.delete(job.id);
      await job.close();
      const next = q.waiting.shift();
      if (next) void this.execute(q, next);
      this.checkIdle();
    }
  }

  private async runCommand(d: CommandRun): Promise<void> {
    const cmd = new Command(this.chan!, d);
    const handler = this.commands.get(cmd.name);
    const done: CommandDone = { commandId: cmd.id, ok: false };
    this.running.set(cmd.id, cmd);
    try {
      if (!handler) {
        done.error = { code: "COMMAND_UNKNOWN", message: `Команда ${cmd.name} не поддерживается` };
      } else {
        const result = await handler(cmd);
        done.ok = true;
        if (result !== undefined) done.result = result;
      }
    } catch (err) {
      if (err instanceof CommandError) done.error = { code: err.code, message: err.message.slice(0, 2000) };
      else {
        if (!cmd.cancelled) this.log("error", `команда ${cmd.name} упала: ${(err as Error)?.stack ?? err}`);
        done.error = { code: "COMMAND_FAILED", message: describe(err) };
      }
    } finally {
      this.running.delete(cmd.id);
    }
    // Срок истёк: итог уже не нужен (агент ответил серверу TIMEOUT).
    if (!cmd.cancelled) this.send("cmd.done", done);
  }

  private async applyState(d: StatePut, re: string | undefined): Promise<void> {
    const applied: StateApplied = { domain: d.domain, version: d.version, ok: false };
    const handler = this.domains.get(d.domain);
    try {
      if (!handler) applied.error = `домен ${d.domain} не обслуживается воркером ${this.name}`;
      else {
        const report = await handler(d.version, d.spec);
        applied.ok = true;
        if (report !== undefined) applied.report = report;
      }
    } catch (err) {
      this.log("error", `домен ${d.domain} версии ${d.version} не применён: ${(err as Error)?.message ?? err}`);
      if (err instanceof StateError) {
        applied.error = err.message.slice(0, 2000);
        if (err.report !== undefined) applied.report = err.report;
      } else applied.error = describe(err);
    }
    this.send("state.applied", applied, { re });
  }

  private async runCleanup(re: string | undefined): Promise<void> {
    const cleaned: WorkerCleaned = { ok: false };
    try {
      if (this.cleanupFn) await this.cleanupFn();
      cleaned.ok = true;
    } catch (err) {
      this.log("error", `уборка воркера ${this.name} не удалась: ${(err as Error)?.message ?? err}`);
      cleaned.error = (err instanceof Error ? err.message : String(err)).slice(0, 2000);
    }
    this.send("worker.cleaned", cleaned, { re });
  }

  /** Команды и применения состояния в работе — их дожидается drain. */
  private async track(p: Promise<void>): Promise<void> {
    this.busy++;
    try {
      await p;
    } finally {
      this.busy--;
      this.checkIdle();
    }
  }

  private checkIdle(): void {
    if (this.draining && this.jobs.size === 0 && this.busy === 0) this.finish?.();
  }
}

function describe(err: unknown): string {
  const e = err as Error;
  return (e instanceof Error ? `${e.name}: ${e.message}` : String(err)).slice(0, 2000);
}

/** Неверное имя объявления — ошибка сразу, при регистрации обработчика. */
function checkName(kind: string, name: string): void {
  if (!validName(name)) throw new Error(`${kind} ${JSON.stringify(name)}: неверное имя — нужно ${NAME_PATTERN}`);
}
