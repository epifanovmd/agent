// Agents — движок серверной части: регистрация агентов, сессии, классы
// доставки, задачи по слотам с арендой, команды со сроком, желаемое состояние
// (общее и для агента), события, история метрик, подписки, отзыв, выпуск агента.
// Данные — в Store; транспорты — transport.ts.
import { createHash, randomBytes, timingSafeEqual } from "node:crypto";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { EventEmitter } from "node:events";
import type { IncomingMessage, Server, ServerResponse } from "node:http";
import {
  Close,
  mergeCapabilities,
  newId,
  MESSAGE_VERSION,
  NAME_PATTERN,
  validName,
  RELEASES_PATH,
  RELIABLE,
  STREAM,
  type Capabilities,
  type Envelope,
  type Hello,
  type Inventory,
  type LogEntry,
  type LogEntryLevel,
  type JobRef,
  type Metrics,
  type MessageError,
  type StateApplied,
  type Status,
  type Subscription,
  type Welcome,
} from "../index";
import { MemoryFiles, type Files } from "./files";
import { installCommand, type InstallOptions } from "./install";
import {
  AgentsError,
  commandFinished,
  ENROLL_MAX_LABEL,
  ENROLL_MAX_LABELS,
  ENROLL_MAX_NAME,
  jobFinished,
  publicAgent,
  helloLabels,
  publicCommand,
  publicJob,
  type Agent,
  type Alert,
  type AlertType,
  type AuditAction,
  type AuditEntry,
  type AgentEvent,
  type AgentRecord,
  type AgentSubscription,
  type Change,
  type ChangeKind,
  type Command,
  type CommandFilter,
  type CommandRecord,
  type CommandRequest,
  type DesiredState,
  type Job,
  type JobFilter,
  type JobRecord,
  type JobRequest,
  type MetricsPoint,
  type PruneOptions,
  type ReleaseManifest,
  type UpdateCandidate,
  type WorkerArtifact,
  type WorkerUpdateCandidate,
} from "./model";
import { seal, type Sealed } from "./seal";
import { Session } from "./session";
import { MemoryStore, type Store } from "./store";
import { Transport } from "./transport";

export interface AgentsOptions {
  /** Токен регистрации агентов (или своя проверка — enroll). */
  enrollToken?: string;
  /**
   * Своя проверка токена регистрации: метки агента или null — отказ. Получает токен и
   * name, labels, host из запроса (уже проверенные на пределы).
   */
  enroll?: (
    token: string,
    req: { name: string; labels: Record<string, string>; host?: unknown },
  ) => { labels?: Record<string, string> } | null | Promise<{ labels?: Record<string, string> } | null>;
  /** Хранилище (по умолчанию MemoryStore). */
  store?: Store;
  /** Провайдер файлов задач (по умолчанию MemoryFiles). */
  files?: Files;
  /** welcome.config.statusIntervalMs (по умолчанию 5000). */
  statusIntervalMs?: number;
  /** welcome.config.metricsIntervalMs (по умолчанию 15000). */
  metricsIntervalMs?: number;
  /**
   * Точку метрик в Store — не чаще, мс (по умолчанию 15000; 0 — каждую). Событие
   * metrics и agent.metrics — каждая точка (частые метрики по подписке — для живых графиков).
   */
  metricsStoreIntervalMs?: number;
  /** Сколько хранить историю метрик, мс (по умолчанию 7 суток; 0 — всегда). */
  metricsRetentionMs?: number;
  /** Агент после обрыва сессии остаётся online столько, мс (по умолчанию 20000). */
  offlineGraceMs?: number;
  /**
   * Бэкенд из нескольких процессов: агент online, но без сессии в этом процессе и без вестей
   * (lastSeenAt) дольше — offline и alert offline (процесс с его сессией упал). По умолчанию
   * max(3 × statusIntervalMs, 30000) + offlineGraceMs.
   */
  offlineAfterMs?: number;
  /** Каталог выпуска агента (`make release`: manifest.json, agent-<os>-<arch>, install.sh). */
  releasesDir?: string;
  /** Ключ проверки релизов (base64): подставляется в install.sh. */
  publicKey?: string;
  /** Публичный адрес сервера для ссылок на файлы и install.sh (по умолчанию — из запроса). */
  baseUrl?: string;
  /**
   * Неудачных регистраций с одного адреса за enrollFailureWindowMs, после которых — 429
   * ENROLL_RATE_LIMITED (по умолчанию 10; 0 — без ограничения).
   */
  enrollFailureLimit?: number;
  /** Окно подсчёта неудачных регистраций, мс (по умолчанию 60000). */
  enrollFailureWindowMs?: number;
  /**
   * Сервер за доверенным прокси: адрес агента (Agent.address) и адрес для ограничения неудачных
   * регистраций — первый из X-Forwarded-For, адрес сервера для ссылок — из X-Forwarded-Host и
   * X-Forwarded-Proto. По умолчанию false — адрес сокета и заголовок Host (заголовки X-Forwarded-*
   * подделываются клиентом).
   */
  trustProxy?: boolean;
  log?: (msg: string, extra?: Record<string, unknown>) => void;
}

/** Типизированные события Agents: "change" — что изменилось, остальные — с объектом. */
export interface AgentsEvents {
  change: [Change];
  agent: [Agent];
  job: [Job];
  command: [Command];
  state: [DesiredState];
  /** Снимок удалён (agentId пусто — общий); переизданный общий придёт событием state. */
  stateDeleted: [{ domain: string; agentId?: string }];
  stateApplied: [StateApplied & { agentId: string }];
  event: [AgentEvent];
  /** Каждая точка метрик агента (в т. ч. backfill и не сохранённые прореживанием) — после обработки. */
  metrics: [agentId: string, point: MetricsPoint];
  /** Изменяющее действие API (с by и без): журнал аудита пишет бэкенд. */
  audit: [AuditEntry];
  /** Проблема началась (active: true) или закончилась (false); по одному событию на переход. */
  alert: [Alert];
  /** Пачка записей лога агента и воркеров (сообщение log); SDK логи не хранит. */
  log: [agentId: string, entries: LogEntry[]];
}

export interface StateHistoryOptions {
  /** Личные снимки агента; пусто — общие. */
  agentId?: string;
  /** Сколько версий (по умолчанию 20). */
  limit?: number;
}

const KEEP_LOG = 500;
const KEEP_JOB_EVENTS = 1000;
const KEEP_OUTPUT = 256 * 1024;
const KEEP_EVENT_IDS = 1000;
/** Тайминги Agents (тесты уменьшают). */
export const agentsDefaults = {
  /** Запас к сроку команды: итог TIMEOUT агент присылает сам, сервер — если агент пропал. */
  commandGraceMs: 15_000,
  /** Как часто проверять аренды задач и сроки команд. */
  sweepIntervalMs: 1000,
  /** Как часто удалять историю метрик старше metricsRetentionMs (и при старте). */
  pruneIntervalMs: 3600_000,
  /** call: как часто проверять итог в Store (итог мог сохранить другой процесс бэкенда). */
  callPollIntervalMs: 1000,
  /** call: запас к сроку команды и commandGraceMs — дальше call возвращает команду как есть. */
  callSlackMs: 5000,
};
/** Срок команд agent.update и worker.update, с. */
const UPDATE_TIMEOUT_SEC = 300;

/**
 * Подписка (Agents.subscribe): «присылай это, так часто, столько времени». Интервалы — от
 * 200 мс; агент только ужесточает подпиской свои настройки.
 */
export interface SubscribeOptions {
  /** id подписки; нет — создаётся. Тот же id — продлить и заменить содержимое. */
  id?: string;
  /** Сколько действует без повторного вызова, мс (по умолчанию 30000). */
  ttlMs?: number;
  /** Частота статуса. */
  status?: { intervalMs: number };
  /**
   * Частота метрик и группы метрик узла сверх настройки агента: cpu, load, memory, swap, disk,
   * diskio, network, interfaces, conntrack, sockets, processes, fds, uptime, temperatures (METRIC_GROUPS).
   */
  metrics?: { intervalMs?: number; groups?: string[] };
  /** С какого уровня слать лог (событие log). */
  logs?: { level: LogEntryLevel };
  /** Каналы показателей воркеров: канал → частота. */
  channels?: Record<string, { intervalMs: number }>;
}

/** Итог subscribe: id подписки и до какого момента она действует, мс. */
export interface SubscriptionRef {
  id: string;
  until: number;
}

export interface PauseWorkerOptions {
  /** Очереди воркера; пусто — все его очереди. */
  queues?: string[];
}

/** Уровни лога от подробного к краткому. */
const LOG_LEVELS: readonly LogEntryLevel[] = ["debug", "info", "warn", "error"];
/** Наименьший интервал подписки, мс. */
const MIN_SUBSCRIPTION_INTERVAL_MS = 200;
/** Имя группы метрик узла. */
const METRIC_GROUP_PATTERN = /^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$/;
const NOT_DISPATCHING = new Set(["draining", "updating", "starting"]);
/** Состояния воркера в status.workers без сбоя; остальные (backoff и т. п.) — workerDown. */
/** Состояния воркера в status.workers, при которых он в сбое (workerDown). */
const WORKER_DOWN = new Set(["backoff", "crashed", "failed", "error"]);
/** Команда смены ключа агента и её срок, с. */
const ROTATE_KEY = "agent.rotateKey";
const ROTATE_TIMEOUT_SEC = 60;
/** Срок команд worker.pause и worker.resume, с. */
const WORKER_CONTROL_TIMEOUT_SEC = 30;
/** Сколько версий истории просматривает rollbackState. */
const ROLLBACK_LOOKUP = 1000;
/** Попыток условной записи при конфликте rev (запись одновременно меняют другие процессы). */
const MUTATE_ATTEMPTS = 8;
/** Поля записи агента, которые меняют сообщения потока (кроме lastSeenAt, lastSeq, stateApplied, alerts). */
const MESSAGE_FIELDS = ["status", "metrics", "metricsAt", "inventory", "capabilities", "pendingSecretHash"] as const;

/**
 * Движок связи с агентами. Бэкенд подключает транспорты (`attach`, `handle`) и
 * пользуется API приложения: задачи, команды, состояние, чтение, события.
 * В процессе изменения выполняются по одному; записи в Store — условные (по rev) с
 * повтором, поэтому несколько процессов с общим Store не затирают изменения друг друга.
 */
export class Agents extends EventEmitter<AgentsEvents> {
  readonly store: Store;
  readonly files: Files;
  private readonly opts: AgentsOptions & {
    statusIntervalMs: number;
    metricsIntervalMs: number;
    metricsStoreIntervalMs: number;
    metricsRetentionMs: number;
    offlineGraceMs: number;
    offlineAfterMs: number;
    enrollFailureLimit: number;
    enrollFailureWindowMs: number;
    log: NonNullable<AgentsOptions["log"]>;
  };
  private readonly sessions = new Map<string, Session>(); // agentId → текущая сессия
  private readonly eventIds = new Map<string, Set<string>>(); // agentId → id недавних event
  private readonly grace = new Map<string, NodeJS.Timeout>(); // agentId → отсрочка offline
  /** Сводная подписка (JSON), о которой сессия этого процесса уже знает (welcome или config). */
  private readonly sentSubscription = new WeakMap<Session, string>();
  /** agentId → at последней сохранённой точки (прореживание; только в памяти процесса). */
  private readonly lastStored = new Map<string, number>();
  /** Ждущие call: перечитать итог команды из Store. */
  private readonly callWaiters = new Set<() => void>();
  /** Адрес клиента → моменты неудачных регистраций в окне. */
  private readonly enrollFailures = new Map<string, number[]>();
  /** Сессии, которые закрыть кодом 1012 после ответа на cmd.done agent.rotateKey. */
  private readonly restartAfterReply = new WeakSet<Session>();
  private readonly transport: Transport;
  private readonly timer: NodeJS.Timeout;
  private readonly pruneTimer: NodeJS.Timeout;
  private lock: Promise<unknown> = Promise.resolve();
  private stopped = false;

  constructor(opts: AgentsOptions = {}) {
    super();
    if (!opts.enrollToken && !opts.enroll) throw new Error("нужен enrollToken или enroll");
    this.opts = {
      statusIntervalMs: 5000,
      metricsIntervalMs: 15_000,
      metricsStoreIntervalMs: 15_000,
      metricsRetentionMs: 7 * 24 * 3600_000,
      offlineGraceMs: 20_000,
      enrollFailureLimit: 10,
      enrollFailureWindowMs: 60_000,
      log: () => {},
      ...opts,
      offlineAfterMs: 0,
    };
    this.opts.offlineAfterMs =
      opts.offlineAfterMs && opts.offlineAfterMs > 0
        ? opts.offlineAfterMs
        : Math.max(3 * this.opts.statusIntervalMs, 30_000) + this.opts.offlineGraceMs;
    this.store = opts.store ?? new MemoryStore();
    this.files = opts.files ?? new MemoryFiles();
    // Загружен выходной файл — задача изменилась (у бэкенда — список файлов).
    if (this.files instanceof MemoryFiles) {
      this.files.on(
        "upload",
        ({ jobId }) =>
          void this.store.getJob(jobId).then(
            (j) => j && this.jobChanged(j),
            () => {},
          ),
      );
    }
    this.transport = new Transport(this);
    this.timer = setInterval(
      () => void this.sweep().catch((e) => this.opts.log("сверка сроков не удалась", { err: String(e) })),
      agentsDefaults.sweepIntervalMs,
    );
    this.timer.unref();
    this.pruneTimer = setInterval(() => void this.pruneMetricsHistory(), agentsDefaults.pruneIntervalMs);
    this.pruneTimer.unref();
    void this.pruneMetricsHistory();
  }

  // ── транспорт ──

  /** WebSocket /api/v1/agent-link на этом HTTP-сервере (upgrade). */
  attach(server: Server): void {
    this.transport.attach(server);
  }

  /** Регистрация, HTTP sync, релизы и install.sh (releasesDir), файлы (MemoryFiles); false — запрос не к агентам. */
  handle(req: IncomingMessage, res: ServerResponse): Promise<boolean> {
    return this.transport.handle(req, res);
  }

  /** @internal Адрес агента — из X-Forwarded-For (опция trustProxy). */
  get trustProxy(): boolean {
    return this.opts.trustProxy === true;
  }

  /** Остановка: сессиям — 1012 (агенты переподключатся сразу). */
  close(): void {
    this.stopped = true;
    clearInterval(this.timer);
    clearInterval(this.pruneTimer);
    for (const t of this.grace.values()) clearTimeout(t);
    this.grace.clear();
    for (const ss of this.sessions.values()) ss.close(Close.Restart);
    this.transport.close();
  }

  // ── API приложения: задачи ──

  enqueue(req: JobRequest): Promise<Job> {
    return this.enqueueAs("", req);
  }

  /** Отменить: бросить работу, итог не нужен. */
  cancelJob(id: string): Promise<Job> {
    return this.signalJob("", id, "cancel");
  }

  /** Досрочно остановить: агент доводит шаг и сдаёт итог (ждущая — отменяется). */
  stopJob(id: string): Promise<Job> {
    return this.signalJob("", id, "stop");
  }

  /**
   * Изменяющие методы от имени actor: поле actor у задачи, команды, снимка состояния
   * и в событии audit. Без by — actor пустой.
   */
  by(actor: string): Actor {
    return new Actor(this, actor);
  }

  private async enqueueAs(actor: string, req: JobRequest): Promise<Job> {
    if (!req?.queue) throw new AgentsError("MESSAGE_INVALID", "Нужна queue");
    checkName("queue", req.queue);
    const created = await this.exclusive(async () => {
      if (req.agentId && !(await this.store.getAgent(req.agentId)))
        throw new AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404);
      const job: JobRecord = {
        id: newId(),
        queue: req.queue,
        data: req.data ?? {},
        status: "queued",
        attempt: 0,
        maxAttempts: Math.max(1, Math.floor(req.maxAttempts ?? 1)),
        leaseSeconds: Math.max(5, Math.floor(req.leaseSeconds ?? 60)),
        accepted: false,
        progress: 0,
        log: [],
        events: [],
        stopRequested: false,
        inputs: Object.keys(req.inputs ?? {}).sort(),
        outputs: [...(req.outputs ?? [])],
        createdAt: Date.now(),
        leaseUntil: 0,
        eventSeq: 0,
        rev: 0,
      };
      if (req.agentId) job.pinnedAgentId = req.agentId;
      if (actor) job.actor = actor;
      if (req.inputs && job.inputs.length) await this.files.saveInputs(job.id, req.inputs);
      await this.store.createJob(job);
      this.jobChanged(job);
      await this.fillAll();
      return publicJob((await this.store.getJob(job.id)) ?? job);
    });
    this.audit(actor, "job.enqueue", created.id, created.pinnedAgentId, { queue: created.queue });
    return created;
  }

  async getJob(id: string): Promise<Job | undefined> {
    const j = await this.store.getJob(id);
    return j && publicJob(j);
  }

  /** Задачи, новые первыми; limit и after — постраничное чтение. */
  async listJobs(filter?: JobFilter): Promise<Job[]> {
    return (await this.store.listJobs(filter)).map(publicJob);
  }

  // ── команды ──

  /** Команда агенту; доставка — сразу или при подключении. */
  command(req: CommandRequest): Promise<Command> {
    return this.commandAs("", req);
  }

  /** Команда и её итог (succeeded, failed или cancelled; срок — timeoutSec + 15 с). */
  call(req: CommandRequest): Promise<Command> {
    return this.callAs("", req);
  }

  /**
   * Отменить команду: ждущая или выполняющаяся становится cancelled (error CANCELLED). Уже
   * отправленной агенту или выполняющейся — агенту cmd.cancel; ждущая и не отправленная —
   * без сообщения. Итог агента после отмены не учитывается. Сессия агента в другом процессе —
   * cmd.cancel шлёт тот процесс при refresh. Нет — COMMAND_NOT_FOUND; завершена — COMMAND_NOT_ACTIVE.
   */
  cancelCommand(id: string): Promise<Command> {
    return this.cancelCommandAs("", id);
  }

  private async cancelCommandAs(actor: string, id: string): Promise<Command> {
    const res = await this.exclusive(async () => {
      let wasRunning = false;
      const cmd = await this.mutateCommand(id, (c) => {
        if (commandFinished(c)) throw new AgentsError("COMMAND_NOT_ACTIVE", "Команда уже завершена", 409);
        wasRunning = c.status === "running";
        Object.assign(c, {
          status: "cancelled",
          error: { code: "CANCELLED", message: "Команду отменили" },
          finishedAt: Date.now(),
        });
        return true;
      });
      if (!cmd) throw new AgentsError("COMMAND_NOT_FOUND", "Команда не найдена", 404);
      const ss = this.sessions.get(cmd.agentId);
      if (ss && (wasRunning || ss.sent.has(cmd.id))) {
        ss.sent.delete(cmd.id);
        ss.send("cmd.cancel", { commandId: cmd.id });
      }
      this.commandChanged(cmd);
      return publicCommand(cmd);
    });
    this.audit(actor, "command.cancel", id, res.agentId);
    return res;
  }

  private async commandAs(actor: string, req: CommandRequest): Promise<Command> {
    const cmd = await this.newCommand(actor, req);
    this.audit(actor, "command", cmd.id, cmd.agentId, { name: cmd.name });
    return cmd;
  }

  /** Команда без записи аудита (её пишет вызывающий: command, agent.update, agent.rotateKey). */
  private async newCommand(actor: string, req: CommandRequest): Promise<Command> {
    if (!req?.name) throw new AgentsError("MESSAGE_INVALID", "Нужно name");
    checkName("name", req.name);
    return this.exclusive(async () => {
      let agentId = req.agentId;
      if (agentId) {
        if (!(await this.store.getAgent(agentId))) throw new AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404);
      } else {
        // Агент на связи, объявивший команду; нет такого — любой объявивший (команда
        // дождётся его подключения или срока).
        const capable = (await this.store.listAgents()).filter(
          (a) => !a.revoked && a.capabilities?.commands?.names?.includes(req.name),
        );
        agentId = (capable.find((a) => a.online) ?? capable[0])?.id;
        if (!agentId)
          throw new AgentsError("COMMAND_NOT_SUPPORTED", `Ни один агент не объявил команду ${req.name}`, 404);
      }
      const cmd: CommandRecord = {
        id: newId(),
        agentId,
        name: req.name,
        args: req.args,
        timeoutSec: Math.max(1, req.timeoutSec ?? 60),
        status: "pending",
        output: "",
        createdAt: Date.now(),
        rev: 0,
      };
      if (actor) cmd.actor = actor;
      await this.store.createCommand(cmd);
      const ss = this.sessions.get(agentId);
      const agent = ss && (await this.store.getAgent(agentId));
      if (ss && agent) await this.deliver(ss, agent);
      this.commandChanged(cmd);
      return publicCommand(cmd);
    });
  }

  /**
   * Итог ловится тремя путями: событие своего процесса (агент подключён сюда), проверка
   * Store раз в callPollIntervalMs и по refresh (агент подключён к другому процессу —
   * итог приходит туда). Предел — срок команды + commandGraceMs + callSlackMs: дальше
   * возвращается команда как есть, ожидание не вечное.
   */
  private async callAs(actor: string, req: CommandRequest): Promise<Command> {
    const cmd = await this.commandAs(actor, req);
    const finished = commandFinished;
    return new Promise<Command>((resolve) => {
      let settled = false;
      const finish = (c: Command) => {
        if (settled) return;
        settled = true;
        this.off("command", onCommand);
        this.callWaiters.delete(recheck);
        clearInterval(poll);
        clearTimeout(deadline);
        resolve(c);
      };
      const onCommand = (c: Command) => {
        if (c.id === cmd.id && finished(c)) finish(c);
      };
      const recheck = () => {
        void this.store.getCommand(cmd.id).then((c) => c && finished(c) && finish(publicCommand(c)));
      };
      this.on("command", onCommand);
      this.callWaiters.add(recheck);
      const poll = setInterval(recheck, agentsDefaults.callPollIntervalMs);
      const deadline = setTimeout(
        () => void this.store.getCommand(cmd.id).then((c) => finish(c ? publicCommand(c) : cmd)),
        cmd.timeoutSec * 1000 + agentsDefaults.commandGraceMs + agentsDefaults.callSlackMs,
      );
      recheck(); // итог мог прийти до подписки
    });
  }

  async getCommand(id: string): Promise<Command | undefined> {
    const c = await this.store.getCommand(id);
    return c && publicCommand(c);
  }

  /** Команды, новые первыми; limit и after — постраничное чтение. */
  async listCommands(filter?: CommandFilter): Promise<Command[]> {
    return (await this.store.listCommands(filter)).map(publicCommand);
  }

  // ── желаемое состояние ──

  /**
   * Новый снимок домена: общий или для агента (`agentId`, важнее общего).
   * Доставка — агентам на связи, объявившим домен, если версия новее известной.
   */
  setState(domain: string, spec: unknown, opts: { agentId?: string } = {}): Promise<DesiredState> {
    return this.setStateAs("", domain, spec, opts);
  }

  private async setStateAs(
    actor: string,
    domain: string,
    spec: unknown,
    opts: { agentId?: string } = {},
  ): Promise<DesiredState> {
    checkDomain(domain);
    const agentId = opts.agentId || undefined;
    const st = await this.exclusive(() => this.putState(actor, domain, agentId, spec));
    this.audit(actor, "state.set", domain, agentId, { version: st.version });
    return st;
  }

  /** Новый снимок (под блокировкой): проверка агента, сохранение, доставка. */
  private async putState(
    actor: string,
    domain: string,
    agentId: string | undefined,
    spec: unknown,
  ): Promise<DesiredState> {
    if (agentId && !(await this.store.getAgent(agentId)))
      throw new AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404);
    const st = await this.store.setState(domain, agentId, spec, actor || undefined);
    await this.warnUndeclared(domain, agentId);
    await this.stateChanged(st);
    return st;
  }

  /** История снимков раздела (общих или агента), новые первыми. */
  stateHistory(domain: string, opts: StateHistoryOptions = {}): Promise<DesiredState[]> {
    checkDomain(domain);
    return this.store.listStateHistory(domain, opts.agentId || undefined, Math.max(1, Math.floor(opts.limit ?? 20)));
  }

  /**
   * Откат раздела: spec версии version из истории (общей или агента) задаётся снова —
   * новой версией, как обычный setState. Нет такой версии — STATE_VERSION_NOT_FOUND.
   */
  rollbackState(domain: string, version: number, opts: { agentId?: string } = {}): Promise<DesiredState> {
    return this.rollbackStateAs("", domain, version, opts);
  }

  private async rollbackStateAs(
    actor: string,
    domain: string,
    version: number,
    opts: { agentId?: string } = {},
  ): Promise<DesiredState> {
    checkDomain(domain);
    const agentId = opts.agentId || undefined;
    const st = await this.exclusive(async () => {
      const hist = await this.store.listStateHistory(domain, agentId, ROLLBACK_LOOKUP);
      const from = hist.find((h) => h.version === version);
      if (!from) throw new AgentsError("STATE_VERSION_NOT_FOUND", `Нет версии ${version} раздела ${domain}`, 404);
      return this.putState(actor, domain, agentId, from.spec);
    });
    this.audit(actor, "state.rollback", domain, agentId, { fromVersion: version, version: st.version });
    return st;
  }

  /**
   * Удалить снимок домена. Снимка не было — не ошибка; удалённый снимок — событие
   * `stateDeleted` и change `state` (только если что-то удалено).
   * С `agentId` — личный снимок агента. Если он был удалён и есть общий снимок домена,
   * общий сохраняется заново с новой версией и доставляется: агент, применивший личный
   * снимок с большей версией, иначе общий не примет. Личного не было — возвращает
   * текущий общий без новой версии и ничего не отправляет; общего нет — null.
   * Без `agentId` — удалить общий: агентам ничего не отправляется, личные снимки
   * остаются; возвращает null.
   */
  deleteState(domain: string, opts: { agentId?: string } = {}): Promise<DesiredState | null> {
    return this.deleteStateAs("", domain, opts);
  }

  private async deleteStateAs(
    actor: string,
    domain: string,
    opts: { agentId?: string } = {},
  ): Promise<DesiredState | null> {
    checkDomain(domain);
    const agentId = opts.agentId || undefined;
    const res = await this.exclusive(async () => {
      const deleted = await this.store.deleteState(domain, agentId);
      if (deleted) {
        this.emit("stateDeleted", agentId ? { domain, agentId } : { domain });
        this.emit("change", { kind: "state", id: domain });
      }
      if (!agentId) return null;
      const common = await this.store.getState(domain);
      if (!common) return null;
      if (!deleted) return common;
      // Переиздание общего — тот же spec и тот же автор.
      const st = await this.store.setState(domain, undefined, common.spec, common.actor);
      await this.stateChanged(st);
      return st;
    });
    this.audit(actor, "state.delete", domain, agentId);
    return res;
  }

  /** Снимок сохранён: доставка (личный — только своему агенту) и уведомления. */
  private async stateChanged(st: DesiredState): Promise<void> {
    for (const ss of this.sessions.values()) {
      if (st.agentId && ss.agentId !== st.agentId) continue;
      const agent = await this.store.getAgent(ss.agentId);
      if (agent) await this.deliver(ss, agent);
    }
    this.emit("state", st);
    this.emit("change", { kind: "state", id: st.domain });
  }

  listStates(): Promise<DesiredState[]> {
    return this.store.listStates();
  }

  // ── агенты и события ──

  async listAgents(): Promise<Agent[]> {
    return (await this.store.listAgents()).map(publicAgent);
  }

  async getAgent(id: string): Promise<Agent | undefined> {
    const a = await this.store.getAgent(id);
    return a && publicAgent(a);
  }

  /** Последние limit событий агентов и воркеров, новые первыми; limit ≤ 0 — все. */
  listEvents(limit = 100): Promise<AgentEvent[]> {
    return this.store.listEvents(limit);
  }

  /**
   * Отозвать агента: revoked, сессия закрывается кодом 4401, учётные данные
   * больше не принимаются (WebSocket и HTTP sync — 401).
   */
  revoke(agentId: string): Promise<Agent> {
    return this.revokeAs("", agentId);
  }

  private async revokeAs(actor: string, agentId: string): Promise<Agent> {
    const res = await this.exclusive(async () => {
      let ended: Alert[] = [];
      const agent = await this.mutateAgent(agentId, (a) => {
        a.revoked = true;
        a.online = false;
        delete a.subscriptions;
        delete a.pendingSecretHash;
        // Отозванный — не «проблема»: его уведомления закончились.
        ended = applyAlerts(a, [{ settle: () => true }], Date.now()).ended;
        return true;
      });
      if (!agent) throw new AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404);
      this.dropSession(agentId);
      this.opts.log("агент отозван", { agent: agent.name, id: agent.id });
      this.agentChanged(agent);
      this.emitAlerts(ended);
      return publicAgent(agent);
    });
    this.audit(actor, "agent.revoke", agentId, agentId);
    return res;
  }

  /**
   * Удалить отозванного агента (сначала revoke): его запись и история метрик. Задачи, команды,
   * события и снимки состояния остаются. Нет — AGENT_NOT_FOUND; не отозван — AGENT_NOT_REVOKED.
   */
  deleteAgent(agentId: string): Promise<void> {
    return this.deleteAgentAs("", agentId);
  }

  private async deleteAgentAs(actor: string, agentId: string): Promise<void> {
    await this.exclusive(async () => {
      const agent = await this.store.getAgent(agentId);
      if (!agent) throw new AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404);
      if (!agent.revoked) throw new AgentsError("AGENT_NOT_REVOKED", "Удалить можно только отозванного агента", 409);
      if (!(await this.store.deleteAgent(agentId))) throw new AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404);
      this.dropSession(agentId);
      this.eventIds.delete(agentId);
      this.lastStored.delete(agentId);
      this.lastStored.delete(`${agentId}\0backfill`);
      this.opts.log("агент удалён", { agent: agent.name, id: agentId });
      this.notify("agent", agentId);
    });
    this.audit(actor, "agent.delete", agentId, agentId);
  }

  /**
   * Уборка Store: завершённые задачи и команды, завершённые раньше, чем столько мс назад, и
   * события старше. Нет или 0 — этот вид записей не трогать. Результат — сколько записей
   * удалено. Сами Agents уборку не запускают: её вызывает бэкенд (например, раз в час).
   */
  async prune(opts: PruneOptions = {}): Promise<number> {
    const now = Date.now();
    const before = (ms: number | undefined) => (ms && ms > 0 ? now - ms : undefined);
    const removed = await this.store.prune({
      jobsBefore: before(opts.jobsOlderThanMs),
      commandsBefore: before(opts.commandsOlderThanMs),
      eventsBefore: before(opts.eventsOlderThanMs),
    });
    if (removed > 0) this.opts.log("старые записи удалены", { records: removed });
    return removed;
  }

  /**
   * Сменить ключ агента без новой регистрации: команда agent.rotateKey (срок 60 с).
   * Агент присылает sha256 нового секрета, сервер запоминает его как ожидающий и
   * закрывает сессию кодом 1012; вход с новым секретом делает его основным.
   */
  rotateKey(agentId: string): Promise<Command> {
    return this.rotateKeyAs("", agentId);
  }

  private async rotateKeyAs(actor: string, agentId: string): Promise<Command> {
    const agent = await this.store.getAgent(agentId);
    if (!agent) throw new AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404);
    if (agent.revoked) throw new AgentsError("AGENT_REVOKED", "Агент отозван", 409);
    if (!agent.capabilities?.commands?.names?.includes(ROTATE_KEY))
      throw new AgentsError("COMMAND_NOT_SUPPORTED", `Агент не объявил команду ${ROTATE_KEY}`, 404);
    const cmd = await this.newCommand(actor, { agentId, name: ROTATE_KEY, timeoutSec: ROTATE_TIMEOUT_SEC });
    this.audit(actor, "agent.rotateKey", agentId, agentId, { commandId: cmd.id });
    return cmd;
  }

  /**
   * Активные проблемы всех агентов (из записей агентов в Store — видны любому процессу),
   * по времени начала.
   */
  async alerts(): Promise<Alert[]> {
    const out: Alert[] = [];
    for (const a of await this.store.listAgents()) if (!a.revoked) for (const x of a.alerts ?? []) out.push({ ...x });
    return out.sort((x, y) => x.at - y.at);
  }

  // ── метрики ──

  /** История метрик агента по возрастанию at; since — строго позже. */
  listMetrics(agentId: string, opts: { since?: number } = {}): Promise<MetricsPoint[]> {
    return this.store.listMetrics(agentId, opts.since);
  }

  // ── подписки ──

  /**
   * Подписка на агента: статус, метрики, лог и каналы показателей чаще и подробнее обычного,
   * пока не истечёт ttlMs. Тот же id — продлить и заменить содержимое. Подписок у агента
   * несколько; агенту уходит сводная (config.subscription) — только когда она изменилась.
   * Подписки — в записи агента (Store): их видят и применяют все процессы бэкенда.
   * Интервал меньше 200 мс, группа, уровень или канал не по правилу — MESSAGE_INVALID;
   * агента нет — AGENT_NOT_FOUND; отозван — AGENT_REVOKED.
   */
  async subscribe(agentId: string, opts: SubscribeOptions = {}): Promise<SubscriptionRef> {
    const sub = subscriptionFrom(opts);
    const ttlMs = Math.max(1, Math.floor(opts.ttlMs ?? 30_000));
    return this.exclusive(async () => {
      const now = Date.now();
      sub.until = now + ttlMs;
      const agent = await this.mutateAgent(agentId, (a) => {
        if (a.revoked) throw new AgentsError("AGENT_REVOKED", "Агент отозван", 409);
        const subs = (a.subscriptions ?? []).filter((s) => s.until > now);
        const at = subs.findIndex((s) => s.id === sub.id);
        if (at >= 0) subs[at] = sub;
        else subs.push(sub);
        a.subscriptions = subs;
        return true;
      });
      if (!agent) throw new AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404);
      this.applySubscription(agent, now);
      return { id: sub.id, until: sub.until };
    });
  }

  /** Снять подписку id; её нет — не ошибка. Агента нет — AGENT_NOT_FOUND. */
  async unsubscribe(agentId: string, id: string): Promise<void> {
    return this.exclusive(async () => {
      const now = Date.now();
      let found = false;
      const agent = await this.mutateAgent(agentId, (a) => {
        found = true;
        const subs = a.subscriptions ?? [];
        const left = subs.filter((s) => s.id !== id && s.until > now);
        if (left.length === subs.length) return false;
        if (left.length) a.subscriptions = left;
        else delete a.subscriptions;
        return true;
      });
      if (!found) throw new AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404);
      if (agent) this.applySubscription(agent, now);
    });
  }

  // ── управление воркером ──

  /**
   * Пауза воркера name: агент не выдаёт ему новые задачи очередей queues (пусто — всех его
   * очередей), выданные доделываются. Пауза от сервера и от самого воркера независимы.
   * Команда worker.pause (срок 30 с); агента нет — AGENT_NOT_FOUND, не объявил команду — COMMAND_NOT_SUPPORTED.
   */
  pauseWorker(agentId: string, name: string, opts: PauseWorkerOptions = {}): Promise<Command> {
    return this.workerControlAs("", "worker.pause", agentId, name, opts);
  }

  /** Снять паузу, выставленную сервером (pauseWorker); пауза воркера остаётся. Команда worker.resume. */
  resumeWorker(agentId: string, name: string, opts: PauseWorkerOptions = {}): Promise<Command> {
    return this.workerControlAs("", "worker.resume", agentId, name, opts);
  }

  private async workerControlAs(
    actor: string,
    command: "worker.pause" | "worker.resume",
    agentId: string,
    name: string,
    opts: PauseWorkerOptions,
  ): Promise<Command> {
    if (!name) throw new AgentsError("MESSAGE_INVALID", "Нужно имя воркера");
    checkName("worker", name);
    const queues = [...new Set(opts.queues ?? [])];
    for (const q of queues) checkName("queue", q);
    const agent = await this.store.getAgent(agentId);
    if (!agent) throw new AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404);
    if (agent.revoked) throw new AgentsError("AGENT_REVOKED", "Агент отозван", 409);
    if (!agent.capabilities?.commands?.names?.includes(command))
      throw new AgentsError("COMMAND_NOT_SUPPORTED", `Агент не объявил команду ${command}`, 404);
    const args: { name: string; queues?: string[] } = { name };
    if (queues.length) args.queues = queues;
    const cmd = await this.newCommand(actor, { agentId, name: command, args, timeoutSec: WORKER_CONTROL_TIMEOUT_SEC });
    this.audit(actor, command, agentId, agentId, {
      worker: name,
      ...(queues.length ? { queues } : {}),
      commandId: cmd.id,
    });
    return cmd;
  }

  // ── секреты ──

  /**
   * Запечатать value (любой JSON) ключом агента (hello.agent.encryptionKey): {"$sealed": "…"} —
   * подставляется в снимок состояния в любое место; раскрывает только агент, перед передачей
   * воркеру. Агента нет — AGENT_NOT_FOUND; агент не сообщил ключ — SEAL_NOT_AVAILABLE.
   */
  async seal(agentId: string, value: unknown): Promise<Sealed> {
    const agent = await this.store.getAgent(agentId);
    if (!agent) throw new AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404);
    const key = agent.hello?.agent?.encryptionKey;
    if (typeof key !== "string" || !key)
      throw new AgentsError("SEAL_NOT_AVAILABLE", "Агент не сообщил ключ шифрования (encryptionKey)", 409);
    try {
      return seal(key, value);
    } catch (e) {
      throw new AgentsError("SEAL_NOT_AVAILABLE", `Ключ шифрования агента некорректен: ${(e as Error).message}`, 409);
    }
  }

  // ── выпуск агента ──

  /** Манифест каталога выпуска (releasesDir) или null. */
  async release(): Promise<ReleaseManifest | null> {
    if (!this.opts.releasesDir) return null;
    try {
      const m = JSON.parse(await readFile(join(this.opts.releasesDir, "manifest.json"), "utf8"));
      if (typeof m?.version !== "string" || !Array.isArray(m.artifacts)) return null;
      if (m.workers !== undefined && !Array.isArray(m.workers)) delete m.workers;
      return m;
    } catch {
      return null;
    }
  }

  /** Агенты update.mode = self, чья версия не как в манифесте и для чьих os/arch есть сборка. */
  async updateCandidates(): Promise<UpdateCandidate[]> {
    const m = await this.release();
    if (!m) return [];
    const out: UpdateCandidate[] = [];
    for (const a of await this.store.listAgents()) {
      if (a.revoked || !selfUpdating(a) || a.hello!.agent.version === m.version) continue;
      const { os, arch } = a.hello!.host;
      if (!artifactFor(m, os, arch)) continue;
      out.push({
        agentId: a.id,
        name: a.name,
        online: a.online,
        current: a.hello!.agent.version,
        target: m.version,
        os,
        arch,
      });
    }
    return out;
  }

  /**
   * Воркеры из выпуска (status.workers[].release) на неотозванных агентах, объявивших worker.update,
   * чья версия не как у старшей сборки воркера в манифесте под os/arch агента. update.mode не учитывается:
   * он про обновление самого агента.
   */
  async workerUpdateCandidates(): Promise<WorkerUpdateCandidate[]> {
    const m = await this.release();
    if (!m?.workers?.length) return [];
    const out: WorkerUpdateCandidate[] = [];
    for (const a of await this.store.listAgents()) {
      if (a.revoked || !updatesWorkers(a)) continue;
      const { os, arch } = a.hello!.host;
      for (const w of a.status?.workers ?? []) {
        if (!w?.release) continue;
        const art = workerArtifactFor(m, w.name, os, arch);
        if (!art || w.version === art.version) continue;
        out.push({
          agentId: a.id,
          agentName: a.name,
          online: a.online,
          worker: w.name,
          current: w.version ?? "",
          target: art.version,
          os,
          arch,
        });
      }
    }
    return out;
  }

  /**
   * Команда установки агента одной строкой: curl '<адрес>/api/v1/agent-link/install.sh' | sudo sh -s -- --token '…' [флаги].
   * Значения — в одинарных кавычках POSIX. Адрес — opts.baseUrl, иначе опция baseUrl Agents. Не ровно один из token и
   * tokenFile, нет адреса, адрес не http(s)://хост[:порт][/путь], недопустимое имя пакета или менеджера, killMode, ключ
   * sysctl или имя воркера, перевод строки — MESSAGE_INVALID.
   */
  installCommand(opts: InstallOptions): string {
    return installCommand(opts, this.opts.baseUrl);
  }

  /** Команда agent.update со сборкой под os/arch агента (срок 300 с); иначе UPDATE_NOT_AVAILABLE. */
  updateAgent(agentId: string): Promise<Command> {
    return this.updateAgentAs("", agentId);
  }

  private async updateAgentAs(actor: string, agentId: string): Promise<Command> {
    const agent = await this.store.getAgent(agentId);
    if (!agent) throw new AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404);
    const m = await this.release();
    if (!m) throw new AgentsError("UPDATE_NOT_AVAILABLE", "Нет выпуска агента (releasesDir)", 409);
    if (!selfUpdating(agent))
      throw new AgentsError("UPDATE_NOT_AVAILABLE", "Агент не обновляется сам (update.mode ≠ self)", 409);
    const art = artifactFor(m, agent.hello!.host.os, agent.hello!.host.arch);
    if (!art)
      throw new AgentsError(
        "UPDATE_NOT_AVAILABLE",
        `Нет сборки ${agent.hello!.host.os}/${agent.hello!.host.arch} в выпуске ${m.version}`,
        409,
      );
    const cmd = await this.newCommand(actor, {
      agentId,
      name: "agent.update",
      timeoutSec: UPDATE_TIMEOUT_SEC,
      args: {
        version: m.version,
        url: `${RELEASES_PATH}/${encodeURIComponent(art.file)}`,
        sha256: art.sha256,
        signature: art.signature ?? "",
      },
    });
    this.audit(actor, "agent.update", agentId, agentId, { commandId: cmd.id, version: m.version });
    return cmd;
  }

  /**
   * Команда worker.update со сборкой воркера name под os/arch агента (срок 300 с); переустановка той же
   * версии разрешена. Агента нет — AGENT_NOT_FOUND; имя не по правилу — MESSAGE_INVALID; нет выпуска или
   * сборки, агент не объявил worker.update, воркер не из выпуска (status.workers[].release) — UPDATE_NOT_AVAILABLE.
   */
  updateWorker(agentId: string, name: string): Promise<Command> {
    return this.updateWorkerAs("", agentId, name);
  }

  private async updateWorkerAs(actor: string, agentId: string, name: string): Promise<Command> {
    const agent = await this.store.getAgent(agentId);
    if (!agent) throw new AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404);
    if (!name) throw new AgentsError("MESSAGE_INVALID", "Нужно имя воркера");
    checkName("worker", name);
    const na = (msg: string) => new AgentsError("UPDATE_NOT_AVAILABLE", msg, 409);
    const m = await this.release();
    if (!m) throw na("Нет выпуска агента (releasesDir)");
    if (!updatesWorkers(agent)) throw na("Агент не объявил команду worker.update");
    if (!agent.status?.workers?.some((w) => w?.name === name && w.release))
      throw na(`Воркер ${name} не из выпуска (release: true)`);
    const os = agent.hello?.host?.os ?? "";
    const arch = agent.hello?.host?.arch ?? "";
    const art = workerArtifactFor(m, name, os, arch);
    if (!art) throw na(`Нет сборки воркера ${name} ${os}/${arch} в выпуске ${m.version}`);
    const cmd = await this.newCommand(actor, {
      agentId,
      name: "worker.update",
      timeoutSec: UPDATE_TIMEOUT_SEC,
      args: {
        name,
        version: art.version,
        url: `${RELEASES_PATH}/${encodeURIComponent(art.file)}`,
        sha256: art.sha256,
        signature: art.signature ?? "",
      },
    });
    this.audit(actor, "worker.update", agentId, agentId, { worker: name, version: art.version, commandId: cmd.id });
    return cmd;
  }

  /** @internal Каталог выпуска и ключ — для транспорта. */
  get releaseOptions(): { dir?: string; publicKey?: string } {
    return { dir: this.opts.releasesDir, publicKey: this.opts.publicKey };
  }

  // ── для транспортов (внутреннее) ──

  /**
   * @internal POST enroll: токен регистрации → учётные данные. body — тело запроса или функция,
   * читающая его (ошибка чтения, например 413, идёт в счёт неудач). remote — адрес клиента
   * (ограничение неудачных попыток); неизвестен — "*".
   */
  async enrollAgent(body: unknown, remote = "*"): Promise<{ agentId: string; secret: string }> {
    const client = remote || "*";
    const limit = this.opts.enrollFailureLimit;
    const window = this.opts.enrollFailureWindowMs;
    if (limit > 0) {
      const now = Date.now();
      const recent = (this.enrollFailures.get(client) ?? []).filter((t) => t > now - window);
      if (recent.length) this.enrollFailures.set(client, recent);
      else this.enrollFailures.delete(client);
      if (recent.length >= limit) {
        const retry = Math.max(1, Math.ceil((recent[recent.length - limit] + window - now) / 1000));
        throw new AgentsError(
          "ENROLL_RATE_LIMITED",
          "Слишком много неудачных регистраций, повторите позже",
          429,
          retry,
        );
      }
    }
    try {
      return await this.enrollChecked(typeof body === "function" ? await (body as () => Promise<unknown>)() : body);
    } catch (e) {
      if (limit > 0) this.enrollFailed(client, window);
      throw e;
    }
  }

  /** Неудачная регистрация клиента — в счёт; заодно — забыть клиентов с прошедшим окном. */
  private enrollFailed(client: string, window: number): void {
    const now = Date.now();
    const list = this.enrollFailures.get(client) ?? [];
    list.push(now);
    this.enrollFailures.set(client, list);
    if (this.enrollFailures.size > 10_000) {
      for (const [k, v] of this.enrollFailures) if (v[v.length - 1] <= now - window) this.enrollFailures.delete(k);
    }
  }

  private async enrollChecked(body: unknown): Promise<{ agentId: string; secret: string }> {
    if (body instanceof SyntaxError) throw new AgentsError("MESSAGE_INVALID", body.message);
    const b = (body && typeof body === "object" && !Array.isArray(body) ? body : {}) as Record<string, unknown>;
    const { token, name } = b;
    if (typeof token !== "string" || !token || typeof name !== "string" || !name)
      throw new AgentsError("MESSAGE_INVALID", "Нужны token и name — непустые строки");
    if ([...name].length > ENROLL_MAX_NAME)
      throw new AgentsError("MESSAGE_INVALID", `name длиннее ${ENROLL_MAX_NAME} символов`);
    const labels = enrollLabels(b.labels);
    const verdict = this.opts.enroll
      ? await this.opts.enroll(token, { name, labels: { ...labels }, host: b.host })
      : safeEqual(token, this.opts.enrollToken!)
        ? {}
        : null;
    if (!verdict) throw new AgentsError("AGENT_ENROLLMENT_TOKEN_INVALID", "Токен регистрации неверен", 401);
    const secret = randomBytes(24).toString("hex");
    const agent: AgentRecord = {
      id: newId(),
      name,
      labels: { ...labels, ...(verdict.labels ?? {}) },
      grantedLabels: { ...(verdict.labels ?? {}) },
      online: false,
      revoked: false,
      enrolledAt: Date.now(),
      stateApplied: {},
      secretHash: sha256(secret),
      lastSeq: 0,
      rev: 0,
    };
    await this.store.createAgent(agent);
    this.opts.log("агент зарегистрирован", { agent: name, id: agent.id });
    this.agentChanged(agent);
    return { agentId: agent.id, secret };
  }

  /** @internal Агент по заголовку `Authorization: Agent <id>.<secret>`. */
  async authenticate(header: string | undefined): Promise<AgentRecord | undefined> {
    const raw = header?.startsWith("Agent ") ? header.slice(6).trim() : undefined;
    const dot = raw?.indexOf(".") ?? -1;
    if (!raw || dot <= 0) return undefined;
    const id = raw.slice(0, dot);
    const hash = sha256(raw.slice(dot + 1));
    const agent = await this.store.getAgent(id);
    if (!agent || agent.revoked) return undefined;
    if (safeEqual(hash, agent.secretHash)) return agent;
    if (!agent.pendingSecretHash || !safeEqual(hash, agent.pendingSecretHash)) return undefined;
    // Вход с новым секретом после agent.rotateKey: он — основной, старый больше не принимается.
    return this.exclusive(async () => {
      let current = false;
      const cur = await this.mutateAgent(id, (a) => {
        current = !a.revoked && safeEqual(hash, a.secretHash);
        if (a.revoked || current || !a.pendingSecretHash || !safeEqual(hash, a.pendingSecretHash)) return false;
        a.secretHash = a.pendingSecretHash;
        delete a.pendingSecretHash;
        return true;
      });
      if (cur) {
        this.opts.log("ключ агента сменён", { agent: cur.name, id: cur.id });
        return cur;
      }
      return current ? this.store.getAgent(id) : undefined;
    });
  }

  /** @internal */
  currentSession(agentId: string): Session | undefined {
    return this.sessions.get(agentId);
  }

  /** @internal Публичный адрес для ссылок (опция или адрес из запроса). */
  publicUrl(fromRequest: string): string {
    return this.opts.baseUrl?.replace(/\/$/, "") ?? fromRequest;
  }

  /**
   * Раздел состояния ещё никто не объявлял (опечатка или воркер ещё не подключался):
   * снимок сохранён, но пока никуда не уйдёт — предупреждение в журнал, не ошибка.
   * Для личного снимка проверяется только этот агент.
   */
  private async warnUndeclared(domain: string, agentId: string | undefined): Promise<void> {
    const agents = agentId ? [await this.store.getAgent(agentId)] : await this.store.listAgents();
    if (agents.some((a) => a && domain in (a.capabilities?.state?.domains ?? {}))) return;
    this.opts.log(
      agentId
        ? "раздел состояния агент ещё не объявлял — снимок сохранён и уйдёт агенту, когда воркер его объявит"
        : "раздел состояния ещё никто не объявлял — снимок сохранён и уйдёт агентам, когда воркер его объявит",
      { domain, ...(agentId ? { agentId } : {}) },
    );
  }

  /** @internal Предупреждение в журнал (для транспорта). */
  warn(msg: string, extra?: Record<string, unknown>): void {
    this.opts.log(msg, extra);
  }

  /** welcome.config: интервалы сервера и сводная подписка (нет подписок — поля нет). */
  private config(agent: AgentRecord): Welcome["config"] {
    const cfg: Welcome["config"] = {
      statusIntervalMs: this.opts.statusIntervalMs,
      metricsIntervalMs: this.opts.metricsIntervalMs,
    };
    const sub = summarize(agent);
    if (Object.keys(sub).length) cfg.subscription = sub;
    return cfg;
  }

  /** Сессии этого процесса — сводную подписку по записи агента, если у неё другая ({} — подписок нет). */
  private applySubscription(agent: AgentRecord, now = Date.now()): void {
    const ss = this.sessions.get(agent.id);
    if (!ss) return;
    const sub = summarize(agent, now);
    const key = JSON.stringify(sub);
    if ((this.sentSubscription.get(ss) ?? "{}") === key) return;
    this.sentSubscription.set(ss, key);
    ss.send("config", { subscription: sub });
  }

  /** Сессию агента — закрыть кодом 4401 (отозван), без отсрочки offline. */
  private dropSession(agentId: string): void {
    clearTimeout(this.grace.get(agentId));
    this.grace.delete(agentId);
    const ss = this.sessions.get(agentId);
    this.sessions.delete(agentId);
    ss?.close(Close.Unauthorized);
  }

  /** История метрик старше metricsRetentionMs — прочь (раз в pruneIntervalMs и при старте). */
  private async pruneMetricsHistory(): Promise<void> {
    if (this.opts.metricsRetentionMs <= 0 || this.stopped) return;
    try {
      const n = await this.store.pruneMetrics(Date.now() - this.opts.metricsRetentionMs);
      if (n > 0) this.opts.log("старая история метрик удалена", { points: n });
    } catch (e) {
      this.opts.log("удаление старой истории метрик не удалось", { err: String(e) });
    }
  }

  /** @internal hello: новая сессия вытесняет прежнюю, welcome, сверка задач, доставка. */
  open(ss: Session, env: Envelope): Promise<void> {
    return this.exclusive(async () => {
      const hello = env.data as Hello | undefined;
      if (env.type !== "hello" || !hello?.agent || !Array.isArray(hello.versions)) return ss.close(Close.Invalid);
      if (!hello.versions.includes(MESSAGE_VERSION)) return ss.close(Close.Unsupported);
      if (this.stopped) {
        const a = await this.store.getAgent(ss.agentId);
        return ss.close(a && !a.revoked ? Close.Restart : Close.Unauthorized);
      }
      const now = Date.now();
      let ended: Alert[] = [];
      const agent = await this.mutateAgent(ss.agentId, (a) => {
        if (a.revoked) return false;
        if (a.bootId !== hello.agent.bootId) {
          a.bootId = hello.agent.bootId;
          a.lastSeq = 0;
        }
        Object.assign(a, {
          hello,
          online: true,
          transport: ss.mode,
          lastSeenAt: now,
          capabilities: hello.capabilities ?? {},
          labels: helloLabels(a, hello.labels),
        });
        if (ss.address) a.address = ss.address;
        ended = applyAlerts(a, [settleKey("offline", "")], now).ended;
        return true;
      });
      if (!agent) return ss.close(Close.Unauthorized);
      clearTimeout(this.grace.get(agent.id));
      this.grace.delete(agent.id);
      const prev = this.sessions.get(agent.id);
      if (prev && prev !== ss) prev.close(Close.Replaced);
      this.sessions.set(agent.id, ss);
      learnDomains(ss, hello.capabilities);
      const config = this.config(agent);
      this.sentSubscription.set(ss, JSON.stringify(config.subscription ?? {}));
      ss.send("welcome", {
        version: MESSAGE_VERSION,
        agentId: agent.id,
        sessionId: ss.id,
        serverTime: Date.now(),
        config,
      });
      this.opts.log("агент на связи", { agent: agent.name, transport: ss.mode });
      this.emitAlerts(ended);
      await this.reconcile(ss, hello.jobs ?? []);
      // Выполняющиеся команды (приняты в прошлой сессии, может быть, другим процессом) —
      // как отправленные в этой: их отмену в другом процессе refresh доставит агенту.
      for (const c of await this.store.listCommands({ status: "running", agentId: agent.id })) ss.sent.add(c.id);
      await this.deliver(ss, agent);
      this.agentChanged(agent);
    });
  }

  /**
   * @internal Сессия закрыта транспортом: если она текущая — агент без связи
   * через offlineGraceMs (переподключился раньше — изменений нет).
   */
  closed(ss: Session): Promise<void> {
    ss.close(Close.Normal);
    return this.exclusive(async () => {
      if (this.sessions.get(ss.agentId) !== ss) return;
      this.sessions.delete(ss.agentId);
      if (this.opts.offlineGraceMs <= 0 || this.stopped) return this.goOffline(ss.agentId);
      clearTimeout(this.grace.get(ss.agentId));
      const timer = setTimeout(() => {
        this.grace.delete(ss.agentId);
        void this.exclusive(() => this.goOffline(ss.agentId)).catch((e) =>
          this.opts.log("отметка offline не удалась", { err: String(e) }),
        );
      }, this.opts.offlineGraceMs);
      timer.unref();
      this.grace.set(ss.agentId, timer);
    });
  }

  private async goOffline(agentId: string): Promise<void> {
    if (this.sessions.has(agentId)) return;
    const agent = await this.markOffline(agentId, (a) => a.online);
    if (agent) this.opts.log("агент без связи", { agent: agent.name });
  }

  /** Агент online → offline и начало проблемы offline (условие when — по свежей записи). */
  private async markOffline(agentId: string, when: (a: AgentRecord) => boolean): Promise<AgentRecord | undefined> {
    const now = Date.now();
    let started: Alert[] = [];
    const agent = await this.mutateAgent(agentId, (a) => {
      if (!a.online || a.revoked || !when(a)) return false;
      a.online = false;
      started = applyAlerts(a, [{ raise: newAlert(a, "offline", "Агент без связи", now) }], now).started;
      return true;
    });
    if (!agent) return undefined;
    this.agentChanged(agent);
    this.emitAlerts(started);
    return agent;
  }

  /**
   * @internal Сообщение открытой сессии; ack или error по классу доставки (§4). Изменения записи
   * агента — одной условной записью на сообщение: поля, изменённые сообщением, поверх свежей записи.
   */
  process(ss: Session, env: Envelope): Promise<void> {
    return this.exclusive(async () => {
      if (ss.closed || this.sessions.get(ss.agentId) !== ss) return;
      const agent = await this.store.getAgent(ss.agentId);
      if (!agent || agent.revoked) return ss.close(Close.Unauthorized);
      if (typeof env?.type !== "string") {
        ss.send("error", { code: "MESSAGE_INVALID", message: "Нет type", retryable: false });
        return;
      }
      // Учёт seq — в записи агента: повтор после переподключения к другому процессу не обрабатывается дважды.
      const seq = STREAM.has(env.type) && env.seq ? env.seq : 0;
      if (seq && seq <= (agent.lastSeq ?? 0)) {
        ss.send("ack", { seq: agent.lastSeq });
        return;
      }
      const before = Object.fromEntries(MESSAGE_FIELDS.map((k) => [k, JSON.stringify(agent[k] ?? null)]));
      const appliedBefore = JSON.stringify(agent.stateApplied ?? {});
      const alerts: AlertOp[] = [];
      let err: MessageError | undefined;
      try {
        err = await this.dispatch(ss, agent, env, alerts);
      } catch (e) {
        // Запись всё время меняют другие процессы — агент повторит сообщение.
        err =
          e instanceof AgentsError && e.code === "STORE_CONFLICT"
            ? { code: e.code, message: e.message, retryable: true }
            : {
                code: "MESSAGE_INVALID",
                message: `Некорректное ${env.type}: ${(e as Error).message}`,
                retryable: false,
              };
      }
      const changed = MESSAGE_FIELDS.filter((k) => JSON.stringify(agent[k] ?? null) !== before[k]);
      const metricsChanged = changed.includes("metrics") || changed.includes("metricsAt");
      const prevApplied = JSON.parse(appliedBefore) as Record<string, unknown>;
      const applied = Object.entries(agent.stateApplied ?? {}).filter(
        ([d, v]) => JSON.stringify(v) !== JSON.stringify(prevApplied[d]),
      );
      const now = Date.now();
      let started: Alert[] = [];
      let ended: Alert[] = [];
      const written = await this.mutateAgent(agent.id, (a) => {
        if (a.revoked) return false;
        a.lastSeenAt = now;
        if (seq) a.lastSeq = Math.max(a.lastSeq ?? 0, seq);
        for (const k of changed) {
          if (k === "metrics" || k === "metricsAt") continue;
          setField(a, k, agent[k]);
        }
        // Текущие метрики — последняя по времени точка (другой процесс мог записать точку новее).
        if (metricsChanged && (a.metrics === undefined || (agent.metricsAt ?? 0) >= (a.metricsAt ?? 0))) {
          setField(a, "metrics", agent.metrics);
          setField(a, "metricsAt", agent.metricsAt);
        }
        if (applied.length) a.stateApplied = { ...a.stateApplied, ...Object.fromEntries(applied) };
        ({ started, ended } = applyAlerts(a, alerts, now));
        return true;
      });
      if (!written) return ss.close(Close.Unauthorized);
      this.emitAlerts(started);
      this.emitAlerts(ended);
      if (err) ss.send("error", err, env.id);
      else if (RELIABLE.has(env.type) && env.id) ss.send("ack", { ids: [env.id] });
      else if (seq) ss.send("ack", { seq });
      // Ожидающий ключ записан, ответ на cmd.done отправлен — переподключение с новым ключом.
      if (this.restartAfterReply.has(ss)) {
        this.restartAfterReply.delete(ss);
        ss.close(Close.Restart);
      }
      // «Агент изменился» — только если есть что обновить: тот же status (пульс), досланная
      // или устаревшая точка метрик текущее состояние агента не меняют.
      const quiet =
        (env.type === "status" && !changed.includes("status")) || (env.type === "metrics" && !metricsChanged);
      if (!quiet && ["status", "metrics", "inventory", "capabilities", "state.applied"].includes(env.type))
        this.agentChanged(written);
    });
  }

  // ── механика ──

  /**
   * Сообщение агента: agent — рабочая копия записи (process запишет изменённые поля), alerts —
   * начала и концы проблем (process применит их к свежей записи).
   */
  private async dispatch(
    ss: Session,
    agent: AgentRecord,
    env: Envelope,
    alerts: AlertOp[],
  ): Promise<MessageError | undefined> {
    const d = env.data ?? {};
    switch (env.type) {
      case "status": {
        agent.status = d as Status;
        ss.statusSeen = true;
        alerts.push(...statusAlerts(agent, agent.status, Date.now()));
        for (const r of agent.status.jobs ?? []) {
          if (typeof r?.jobId !== "string") continue;
          ss.pending.delete(r.jobId); // учтена в slots
          await this.mutateJob(r.jobId, (j) => {
            if (!isHeld(j, agent.id, r)) return false;
            j.leaseUntil = Date.now() + j.leaseSeconds * 1000;
            return true;
          });
        }
        await this.fill(ss, agent);
        return;
      }
      case "metrics": {
        const m = d as Metrics;
        const point: MetricsPoint = { at: pointTime(m, Date.now()), backfill: m.backfill === true, metrics: m };
        // Прореживание: в Store — не чаще metricsStoreIntervalMs; первая точка после старта — всегда.
        // Досланные — отдельно от живых: их время раньше уже сохранённых живых, и общее правило
        // отбросило бы всю историю без связи.
        const key = point.backfill ? `${agent.id}\0backfill` : agent.id;
        const last = this.lastStored.get(key);
        const every = this.opts.metricsStoreIntervalMs;
        if (every <= 0 || last === undefined || point.at >= last + every) {
          await this.store.addMetrics(agent.id, point);
          this.lastStored.set(key, point.at);
        }
        this.emit("metrics", agent.id, point);
        // Текущие метрики — последняя по времени точка без backfill.
        if (!point.backfill && (agent.metrics === undefined || point.at >= (agent.metricsAt ?? 0))) {
          agent.metrics = m;
          agent.metricsAt = point.at;
        }
        return;
      }
      case "inventory":
        agent.inventory = d as Inventory;
        return;
      case "log": {
        if (d.entries !== undefined && d.entries !== null && !Array.isArray(d.entries)) return invalid(env.type);
        const entries = ((d.entries ?? []) as unknown[]).filter(
          (e): e is LogEntry => !!e && typeof e === "object" && typeof (e as LogEntry).msg === "string",
        );
        if (entries.length) this.safeEmit("log", agent.id, entries);
        return;
      }
      case "capabilities":
        agent.capabilities = mergeCapabilities(agent.capabilities, d as Capabilities);
        learnDomains(ss, d as Capabilities);
        await this.deliver(ss, agent);
        return;
      case "event": {
        if (!d.type || typeof d.type !== "string") return invalid(env.type);
        if (env.id && this.seen(agent.id, env.id)) return;
        const e: AgentEvent = {
          agentId: agent.id,
          agentName: agent.name,
          source: d.source || "agent",
          type: d.type,
          data: d.data,
          at: env.ts || Date.now(),
        };
        await this.store.addEvent(e);
        this.emit("event", e);
        this.emit("change", { kind: "event", id: env.id || newId() });
        return;
      }
      case "job.accept": {
        const job = await this.mutateJob(jobIdOf(d), (j) => {
          if (!isHeld(j, agent.id, d) || j.accepted) return false;
          j.accepted = true;
          return true;
        });
        if (job) this.jobChanged(job);
        return;
      }
      case "job.progress": {
        const job = await this.mutateJob(jobIdOf(d), (j) => {
          if (!isHeld(j, agent.id, d)) return false;
          if (typeof d.progress === "number") j.progress = d.progress;
          if (typeof d.text === "string") j.text = d.text;
          if (Array.isArray(d.log)) j.log = [...j.log, ...d.log.map(String)].slice(-KEEP_LOG);
          return true;
        });
        if (job) this.jobChanged(job);
        return;
      }
      case "job.event": {
        if (!(await this.held(agent.id, d))) return leaseLost();
        if (typeof d.type !== "string" || !(d.seq >= 1)) return invalid(env.type);
        let lost = true;
        const job = await this.mutateJob(jobIdOf(d), (j) => {
          lost = !isHeld(j, agent.id, d);
          if (lost || d.seq <= j.eventSeq) return false;
          j.eventSeq = d.seq;
          j.events = [...j.events, { seq: d.seq, type: d.type, data: d.data, at: env.ts ?? Date.now() }].slice(
            -KEEP_JOB_EVENTS,
          );
          return true;
        });
        if (lost) return leaseLost();
        if (job) this.jobChanged(job);
        return;
      }
      case "job.urls": {
        const job = await this.held(agent.id, d);
        if (!job) return leaseLost();
        const urls = await this.files.urls(publicJob(job), this.publicUrl(ss.baseUrl), {
          inputs: d.inputs,
          outputs: d.outputs,
        });
        ss.send("job.urls", urls, env.id);
        return;
      }
      case "job.complete": {
        const job = await this.mutateJob(jobIdOf(d), (j) => {
          if (!isHeld(j, agent.id, d)) return false;
          Object.assign(j, { status: "completed", result: d.result, progress: 1, finishedAt: Date.now() });
          delete j.error;
          return true;
        });
        if (!job) return (await this.finishedHere(agent.id, d, "completed")) ? undefined : leaseLost();
        ss.pending.delete(job.id);
        this.opts.log("задача выполнена", { job: job.id, queue: job.queue });
        this.jobChanged(job);
        await this.fill(ss, agent);
        return;
      }
      case "job.fail": {
        const job = await this.failAttempt(
          jobIdOf(d),
          agent.id,
          d.attempt,
          String(d.code ?? "WORKER_ERROR"),
          String(d.message ?? ""),
          d.retryable === true,
        );
        if (!job) return (await this.finishedHere(agent.id, d)) ? undefined : leaseLost();
        return;
      }
      case "job.reject": {
        ss.pending.delete(d.jobId);
        const job = await this.mutateJob(jobIdOf(d), (j) => {
          if (!isHeld(j, agent.id, d)) return false;
          Object.assign(j, { status: "queued", accepted: false, leaseUntil: 0 });
          delete j.agentId;
          return true;
        });
        if (job) {
          this.jobChanged(job);
          await this.fillAll(ss.agentId);
        }
        return;
      }
      case "cmd.accept": {
        const cmd = await this.mutateCommand(commandIdOf(d), (c) => {
          if (c.agentId !== agent.id || c.status !== "pending") return false;
          c.status = "running";
          return true;
        });
        if (cmd) this.commandChanged(cmd);
        return;
      }
      case "cmd.output": {
        if (typeof d.chunk !== "string") return;
        const cmd = await this.mutateCommand(commandIdOf(d), (c) => {
          if (c.agentId !== agent.id || commandFinished(c)) return false;
          c.output = (c.output + d.chunk).slice(-KEEP_OUTPUT);
          return true;
        });
        if (cmd) this.commandChanged(cmd);
        return;
      }
      case "cmd.done": {
        const id = commandIdOf(d);
        ss.sent.delete(id);
        // Итог — только незавершённой: поздний итог (после отмены или срока) не учитывается.
        const cmd = await this.mutateCommand(id, (c) => {
          if (c.agentId !== agent.id || commandFinished(c)) return false;
          Object.assign(c, {
            status: d.ok ? "succeeded" : "failed",
            result: d.result,
            error: d.error,
            finishedAt: Date.now(),
          });
          if (Number.isInteger(d.exitCode)) c.exitCode = d.exitCode;
          return true;
        });
        if (cmd) this.commandChanged(cmd);
        // Принятый итог смены ключа: хеш нового секрета — ожидающий.
        const hash = (d.result as { secretHash?: unknown } | undefined)?.secretHash;
        if (cmd && d.ok && cmd.name === ROTATE_KEY && typeof hash === "string" && SHA256_HEX.test(hash)) {
          agent.pendingSecretHash = hash;
          this.restartAfterReply.add(ss);
        }
        return;
      }
      case "state.applied": {
        const applied = d as StateApplied;
        if (typeof applied.domain !== "string" || typeof applied.version !== "number") return invalid(env.type);
        agent.stateApplied = { ...agent.stateApplied, [applied.domain]: applied };
        if (applied.ok && applied.version > (ss.known.get(applied.domain) ?? 0))
          ss.known.set(applied.domain, applied.version);
        this.emit("stateApplied", { ...applied, agentId: agent.id });
        this.emit("change", { kind: "state", id: applied.domain });
        if (applied.ok) alerts.push(settleKey("stateFailed", applied.domain));
        else
          alerts.push({
            raise: newAlert(agent, "stateFailed", applied.error ?? "", Date.now(), { domain: applied.domain }),
          });
        return;
      }
      default:
        return { code: "UNKNOWN_TYPE", message: `Неизвестный тип: ${env.type}`, retryable: false };
    }
  }

  /** Задача за агентом в этой попытке и выполняется. */
  private async held(agentId: string, ref: JobRef): Promise<JobRecord | undefined> {
    if (typeof ref?.jobId !== "string") return undefined;
    const job = await this.store.getJob(ref.jobId);
    return job && isHeld(job, agentId, ref) ? job : undefined;
  }

  /** Повтор итога уже завершённой этим агентом попытки — подтвердить (идемпотентно). */
  private async finishedHere(agentId: string, ref: JobRef, status?: Job["status"]): Promise<boolean> {
    if (typeof ref?.jobId !== "string") return false;
    const job = await this.store.getJob(ref.jobId);
    return (
      !!job &&
      job.agentId === agentId &&
      job.attempt === ref.attempt &&
      (status ? job.status === status : job.status === "completed" || job.status === "failed")
    );
  }

  /** Сверка задач при hello (§6.3). */
  private async reconcile(ss: Session, reported: JobRef[]): Promise<void> {
    const listed = new Set(reported.map((r) => `${r.jobId}#${r.attempt}`));
    for (const job of await this.store.listJobs({ status: "running", agentId: ss.agentId })) {
      if (listed.has(`${job.id}#${job.attempt}`)) {
        if (job.stopRequested) ss.send("job.stop", ref(job));
      } else if (!job.accepted) {
        const again = await this.mutateJob(job.id, (j) => {
          if (!isHeld(j, ss.agentId, job) || j.accepted) return false;
          j.leaseUntil = Date.now() + j.leaseSeconds * 1000;
          return true;
        });
        if (!again) continue;
        ss.pending.set(again.id, again.queue);
        ss.send("job.assign", await this.assignment(ss, again));
      } else {
        await this.failAttempt(
          job.id,
          ss.agentId,
          job.attempt,
          "AGENT_LOST",
          "Агент перезапустился и потерял задачу",
          true,
          {
            when: (j) => j.accepted,
          },
        );
      }
    }
    for (const r of reported) {
      if (!(await this.held(ss.agentId, r))) ss.send("job.cancel", { jobId: r.jobId, attempt: r.attempt });
    }
  }

  /**
   * Ожидающие команды и новые версии объявленных доменов. Команды, отправленные в этой сессии и
   * отменённые (в том числе другим процессом), — cmd.cancel по одному разу; завершённые — забываются.
   */
  private async deliver(ss: Session, agent: AgentRecord): Promise<void> {
    if (ss.closed) return;
    for (const id of [...ss.sent]) {
      const c = await this.store.getCommand(id);
      if (c && !commandFinished(c)) continue;
      ss.sent.delete(id);
      if (c?.status === "cancelled") ss.send("cmd.cancel", { commandId: id });
    }
    // Старые первыми.
    const declared = agent.capabilities?.commands?.names ?? [];
    for (const cmd of (await this.store.listCommands({ status: "pending", agentId: agent.id })).reverse()) {
      // Только команды, которые агент объявил (§6.4): остальные ждут его capabilities.
      if (ss.sent.has(cmd.id) || !declared.includes(cmd.name)) continue;
      ss.sent.add(cmd.id);
      ss.send("cmd.run", { commandId: cmd.id, name: cmd.name, args: cmd.args, timeoutSec: cmd.timeoutSec });
    }
    for (const domain of Object.keys(agent.capabilities?.state?.domains ?? {})) {
      const st = (await this.store.getState(domain, agent.id)) ?? (await this.store.getState(domain));
      if (st && st.version > (ss.known.get(domain) ?? 0)) {
        ss.known.set(domain, st.version);
        ss.send("state.put", { domain, version: st.version, spec: st.spec });
      }
    }
  }

  /**
   * Раздать ждущие задачи по свободным слотам сессии (status.slots). Задачу берёт условная запись
   * queued → running: взял другой процесс — пропустить.
   */
  private async fill(ss: Session, agent: AgentRecord): Promise<void> {
    const st = agent.status;
    if (ss.closed || !ss.statusSeen || !st?.slots || NOT_DISPATCHING.has(st.state)) return;
    const queues = Object.keys(st.slots)
      .filter((q) => st.slots[q] > 0)
      .sort();
    if (!queues.length) return;
    const queued = (await this.store.listJobs({ status: "queued" })).reverse(); // старые первыми
    for (const queue of queues) {
      let free = st.slots[queue] - [...ss.pending.values()].filter((q) => q === queue).length;
      for (const job of queued) {
        if (free <= 0) break;
        if (job.queue !== queue) continue;
        if (job.pinnedAgentId && job.pinnedAgentId !== agent.id) continue;
        const taken = await this.mutateJob(job.id, (j) => {
          if (j.status !== "queued" || j.attempt !== job.attempt || j.queue !== queue) return false;
          if (j.pinnedAgentId && j.pinnedAgentId !== agent.id) return false;
          Object.assign(j, {
            status: "running",
            agentId: agent.id,
            accepted: false,
            leaseUntil: Date.now() + j.leaseSeconds * 1000,
          });
          return true;
        });
        if (!taken) continue;
        ss.pending.set(taken.id, queue);
        ss.send("job.assign", await this.assignment(ss, taken));
        this.jobChanged(taken);
        free--;
      }
    }
  }

  /** Раздача по всем агентам: сначала менее загруженные (задач в работе и выданных). */
  private async fillAll(skipAgentId?: string): Promise<void> {
    const loaded: { ss: Session; agent: AgentRecord; load: number }[] = [];
    for (const ss of this.sessions.values()) {
      if (ss.agentId === skipAgentId) continue;
      const agent = await this.store.getAgent(ss.agentId);
      if (agent) loaded.push({ ss, agent, load: (agent.status?.jobs?.length ?? 0) + ss.pending.size });
    }
    loaded.sort((a, b) => a.load - b.load);
    for (const { ss, agent } of loaded) await this.fill(ss, agent);
  }

  private async assignment(ss: Session, job: JobRecord) {
    const urls = await this.files.urls(publicJob(job), this.publicUrl(ss.baseUrl));
    return {
      jobId: job.id,
      attempt: job.attempt,
      queue: job.queue,
      data: job.data,
      leaseSeconds: job.leaseSeconds,
      inputs: urls.inputs,
      outputs: urls.outputs,
      urlsExpireAt: urls.expiresAt,
    };
  }

  /**
   * Провал попытки (задача running у agentId в попытке attempt и opts.when по свежей записи):
   * повтор, если попытки остались. opts.cancel — агенту на связи job.cancel этой попытки.
   * Результат — записанная задача; условие не выполнено — undefined.
   */
  private async failAttempt(
    jobId: string,
    agentId: string | undefined,
    attempt: number,
    code: string,
    message: string,
    retryable: boolean,
    opts: { when?: (j: JobRecord) => boolean; cancel?: boolean } = {},
  ): Promise<JobRecord | undefined> {
    let retry = false;
    const job = await this.mutateJob(jobId, (j) => {
      if (j.status !== "running" || j.agentId !== agentId || j.attempt !== attempt) return false;
      if (opts.when && !opts.when(j)) return false;
      j.error = { code, message };
      retry = retryable && j.attempt + 1 < j.maxAttempts;
      if (retry) {
        Object.assign(j, {
          status: "queued",
          attempt: j.attempt + 1,
          accepted: false,
          eventSeq: 0,
          leaseUntil: 0,
          progress: 0,
          stopRequested: false,
        });
        delete j.agentId;
      } else Object.assign(j, { status: "failed", finishedAt: Date.now() });
      return true;
    });
    if (!job) return undefined;
    const ss = agentId ? this.sessions.get(agentId) : undefined;
    ss?.pending.delete(jobId);
    if (opts.cancel) ss?.send("job.cancel", { jobId, attempt });
    this.jobChanged(job);
    if (retry) await this.fillAll();
    else this.opts.log("задача провалена", { job: job.id, code });
    return job;
  }

  private async signalJob(actor: string, id: string, kind: "cancel" | "stop"): Promise<Job> {
    const res = await this.exclusive(async () => {
      let wasRunning = false;
      const job = await this.mutateJob(id, (j) => {
        if (jobFinished(j)) throw new AgentsError("JOB_NOT_ACTIVE", "Задача уже завершена", 409);
        wasRunning = j.status === "running";
        if (kind === "stop" && wasRunning) j.stopRequested = true;
        else Object.assign(j, { status: "cancelled", finishedAt: Date.now() });
        return true;
      });
      if (!job) throw new AgentsError("JOB_NOT_FOUND", "Задача не найдена", 404);
      const ss = job.agentId ? this.sessions.get(job.agentId) : undefined;
      if (ss && wasRunning) {
        if (kind === "stop") ss.send("job.stop", ref(job));
        else {
          ss.pending.delete(job.id);
          ss.send("job.cancel", ref(job));
        }
      }
      this.jobChanged(job);
      return publicJob(job);
    });
    this.audit(actor, kind === "cancel" ? "job.cancel" : "job.stop", id, res.agentId ?? res.pinnedAgentId);
    return res;
  }

  /**
   * Раз в секунду: истёкшие аренды задач, сроки команд, истёкшие подписки (агентов на связи
   * с этим процессом; агентов без связи — любым процессом), агенты без вестей (offlineAfterMs).
   * Условия проверяются по свежей записи: аренду, продлённую другим процессом, не трогаем.
   */
  private sweep(): Promise<void> {
    return this.exclusive(async () => {
      const now = Date.now();
      for (const id of [...this.sessions.keys()]) {
        const agent = await this.mutateAgent(id, (a) => dropExpired(a, now));
        if (agent) this.applySubscription(agent, now);
      }
      await this.sweepOffline(now);
      for (const job of await this.store.listJobs({ status: "running" })) {
        if (now <= job.leaseUntil) continue;
        // Агент на связи, но задачу не перечисляет: её заберут — прервать.
        await this.failAttempt(
          job.id,
          job.agentId,
          job.attempt,
          "LEASE_EXPIRED",
          "Агент перестал отвечать: аренда истекла",
          true,
          { when: (j) => now > j.leaseUntil, cancel: true },
        );
      }
      for (const listed of await this.store.listCommands({ status: ["pending", "running"] })) {
        if (now <= listed.createdAt + listed.timeoutSec * 1000 + agentsDefaults.commandGraceMs) continue;
        const cmd = await this.mutateCommand(listed.id, (c) => {
          if (commandFinished(c)) return false;
          Object.assign(c, {
            status: "failed",
            error: { code: "TIMEOUT", message: "Нет итога от агента" },
            finishedAt: now,
          });
          return true;
        });
        if (cmd) this.commandChanged(cmd);
      }
    });
  }

  /**
   * Агент online без сессии в этом процессе и без вестей дольше offlineAfterMs (процесс с его
   * сессией упал) — offline и alert offline. Сессия здесь или отсрочка offlineGraceMs — не трогаем.
   */
  private async sweepOffline(now: number): Promise<void> {
    const stale = (a: AgentRecord) =>
      a.online &&
      !a.revoked &&
      !this.sessions.has(a.id) &&
      !this.grace.has(a.id) &&
      now - (a.lastSeenAt ?? 0) > this.opts.offlineAfterMs;
    for (const listed of await this.store.listAgents()) {
      if (!stale(listed)) continue;
      // Условие — по свежей записи: другой процесс мог обновить её после listAgents.
      const agent = await this.markOffline(listed.id, stale);
      if (agent) this.opts.log("агент без вестей — без связи", { agent: agent.name, lastSeenAt: agent.lastSeenAt });
    }
    // Истёкшие подписки агентов без связи: сверять их некому, кроме любого процесса.
    for (const listed of await this.store.listAgents()) {
      if (listed.online || this.sessions.has(listed.id) || !hasExpired(listed, now)) continue;
      await this.mutateAgent(listed.id, (a) => !a.online && dropExpired(a, now));
    }
  }

  /**
   * Перечитать хранилище и доставить агенту (без agentId — всем агентам на связи с
   * этим объектом Agents) то, что появилось в нём мимо него: ожидающие команды, новые снимки
   * состояния, задачи. Для бэкенда из нескольких процессов с общим Store: процесс,
   * изменивший данные, уведомляет остальные (например, Postgres NOTIFY), каждый
   * вызывает refresh — доставит тот, у кого сессия агента. Заодно применяет
   * сделанное другим процессом с записью агента: отзыв (сессия закрывается кодом
   * 4401) и подписки (сводная — агенту, если изменилась).
   */
  refresh(agentId?: string): Promise<void> {
    // Ждущие call — перечитать итог: его мог сохранить другой процесс.
    for (const recheck of this.callWaiters) recheck();
    return this.exclusive(async () => {
      for (const [id, ss] of [...this.sessions]) {
        if (agentId && id !== agentId) continue;
        const agent = await this.store.getAgent(id);
        if (!agent) continue;
        if (agent.revoked) {
          this.dropSession(id);
          this.opts.log("агент отозван другим процессом — сессия закрыта", { agent: agent.name, id });
          continue;
        }
        this.applySubscription(agent);
        await this.deliver(ss, agent);
      }
      await this.fillAll();
    });
  }

  private exclusive<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.lock.then(fn, fn);
    this.lock = run.catch(() => {});
    return run;
  }

  private seen(agentId: string, id: string): boolean {
    let ids = this.eventIds.get(agentId);
    if (!ids) this.eventIds.set(agentId, (ids = new Set()));
    if (ids.has(id)) return true;
    ids.add(id);
    if (ids.size > KEEP_EVENT_IDS) ids.delete(ids.values().next().value!);
    return false;
  }

  /** Событие audit; ошибка подписчика не отменяет сделанного. */
  private audit(
    actor: string,
    action: AuditAction,
    target: string,
    agentId?: string,
    details?: Record<string, unknown>,
  ): void {
    const e: AuditEntry = { at: Date.now(), actor, action, target };
    if (agentId) e.agentId = agentId;
    if (details) e.details = details;
    this.safeEmit("audit", e);
  }

  private safeEmit<K extends "audit" | "alert" | "log">(event: K, ...args: AgentsEvents[K]): void {
    try {
      (this.emit as (e: string, ...a: unknown[]) => boolean)(event, ...args);
    } catch (e) {
      this.opts.log(`подписчик ${event} упал`, { err: String(e) });
    }
  }

  /** События alert (начала или концы проблем) — после записи. */
  private emitAlerts(list: Alert[]): void {
    for (const a of list) this.safeEmit("alert", { ...a });
  }

  /**
   * Чтение-изменение-запись с повтором: fn меняет свежую запись и говорит, писать ли (false —
   * условие не выполнено, ничего не делать). Конфликт rev — перечитать и повторить fn, до
   * MUTATE_ATTEMPTS раз; дальше — ошибка STORE_CONFLICT. Результат — записанная запись; записи
   * нет или fn вернула false — undefined. fn может выполниться несколько раз: побочные действия —
   * у вызывающего, после записи, по итогу последнего прогона fn.
   */
  private async mutate<T extends { rev: number }>(
    what: string,
    id: string,
    get: (id: string) => Promise<T | undefined>,
    put: (rec: T) => Promise<boolean>,
    fn: (rec: T) => boolean,
  ): Promise<T | undefined> {
    for (let i = 0; i < MUTATE_ATTEMPTS; i++) {
      const rec = await get(id);
      if (!rec || !fn(rec)) return undefined;
      if (await put(rec)) return rec;
    }
    this.opts.log("запись не сохранена: её одновременно меняют другие процессы", { record: what, id });
    throw new AgentsError("STORE_CONFLICT", `Запись ${what} ${id} одновременно меняют другие процессы`, 409);
  }

  private mutateAgent(id: string, fn: (a: AgentRecord) => boolean): Promise<AgentRecord | undefined> {
    return this.mutate(
      "агента",
      id,
      (x) => this.store.getAgent(x),
      (r) => this.store.updateAgent(r),
      fn,
    );
  }

  private mutateJob(id: string, fn: (j: JobRecord) => boolean): Promise<JobRecord | undefined> {
    return this.mutate(
      "задачи",
      id,
      (x) => this.store.getJob(x),
      (r) => this.store.updateJob(r),
      fn,
    );
  }

  private mutateCommand(id: string, fn: (c: CommandRecord) => boolean): Promise<CommandRecord | undefined> {
    return this.mutate(
      "команды",
      id,
      (x) => this.store.getCommand(x),
      (r) => this.store.updateCommand(r),
      fn,
    );
  }

  private notify(kind: ChangeKind, id: string): void {
    this.emit("change", { kind, id });
  }

  private agentChanged(a: AgentRecord): void {
    this.emit("agent", publicAgent(a));
    this.notify("agent", a.id);
  }

  private jobChanged(j: JobRecord): void {
    this.emit("job", publicJob(j));
    this.notify("job", j.id);
  }

  private commandChanged(c: CommandRecord): void {
    this.emit("command", publicCommand(c));
    this.notify("command", c.id);
  }
}

function learnDomains(ss: Session, caps: Capabilities | undefined): void {
  for (const [domain, version] of Object.entries(caps?.state?.domains ?? {})) {
    if (version != null && version > (ss.known.get(domain) ?? 0)) ss.known.set(domain, version);
  }
}

/**
 * Время точки по часам сервера (§6.2): collectedAt + clockOffsetMs; без смещения —
 * время получения.
 */
export function pointTime(m: Metrics, receivedAt: number): number {
  const finite = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v);
  if (finite(m.collectedAt) && finite(m.clockOffsetMs)) return m.collectedAt + m.clockOffsetMs;
  return receivedAt;
}

/** Агент обновляет воркеры: объявил worker.update и прислал hello (update.mode не учитывается). */
function updatesWorkers(a: AgentRecord): boolean {
  return !!a.capabilities?.commands?.names?.includes("worker.update") && !!a.hello?.host?.os;
}

function selfUpdating(a: AgentRecord): boolean {
  return a.capabilities?.update?.mode === "self" && !!a.hello?.agent?.version && !!a.hello?.host?.os;
}

function artifactFor(m: ReleaseManifest, os: string, arch: string) {
  return m.artifacts.find((x) => x.os === os && x.arch === arch && typeof x.file === "string");
}

/** Сборка воркера под os/arch; записей несколько — новейшая версия. */
function workerArtifactFor(m: ReleaseManifest, name: string, os: string, arch: string): WorkerArtifact | undefined {
  let best: WorkerArtifact | undefined;
  for (const w of m.workers ?? []) {
    if (w?.name !== name || w.os !== os || w.arch !== arch || typeof w.file !== "string") continue;
    if (typeof w.version !== "string") continue;
    if (!best || compareVersions(w.version, best.version) > 0) best = w;
  }
  return best;
}

/** Сравнение версий по числовым частям (1.10.0 > 1.9.0); нечисловые части — как строки. */
function compareVersions(a: string, b: string): number {
  const pa = a.replace(/^v/, "").split(/[.-]/);
  const pb = b.replace(/^v/, "").split(/[.-]/);
  for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
    const x = pa[i] ?? "";
    const y = pb[i] ?? "";
    if (x === y) continue;
    const nx = /^\d+$/.test(x) ? Number(x) : NaN;
    const ny = /^\d+$/.test(y) ? Number(y) : NaN;
    if (!isNaN(nx) && !isNaN(ny)) return nx - ny;
    return x < y ? -1 : 1;
  }
  return 0;
}

function ref(job: Job): JobRef {
  return { jobId: job.id, attempt: job.attempt };
}

function invalid(type: string): MessageError {
  return { code: "MESSAGE_INVALID", message: `Некорректное ${type}`, retryable: false };
}

function leaseLost(): MessageError {
  return { code: "JOB_LEASE_LOST", message: "Задача больше не за этим агентом", retryable: false };
}

const sha256 = (s: string) => createHash("sha256").update(s).digest("hex");
const SHA256_HEX = /^[0-9a-f]{64}$/;
/** Подписка из опций subscribe с проверками (until — заполнит вызывающий). */
function subscriptionFrom(opts: SubscribeOptions): AgentSubscription {
  const id = opts.id ?? newId();
  if (typeof id !== "string" || !id || id.length > 256)
    throw new AgentsError("MESSAGE_INVALID", "id подписки — непустая строка до 256 символов");
  const sub: AgentSubscription = { id, until: 0 };
  if (opts.status) sub.status = { intervalMs: checkInterval("status.intervalMs", opts.status.intervalMs) };
  if (opts.metrics) {
    const m: NonNullable<AgentSubscription["metrics"]> = {};
    if (opts.metrics.intervalMs !== undefined)
      m.intervalMs = checkInterval("metrics.intervalMs", opts.metrics.intervalMs);
    const groups = [...new Set(opts.metrics.groups ?? [])];
    for (const g of groups) {
      if (typeof g !== "string" || !METRIC_GROUP_PATTERN.test(g))
        throw new AgentsError(
          "MESSAGE_INVALID",
          `Группа метрик ${JSON.stringify(g)} не по правилу ${METRIC_GROUP_PATTERN}`,
        );
    }
    if (groups.length) m.groups = groups;
    sub.metrics = m;
  }
  if (opts.logs) {
    const level = opts.logs.level;
    if (!LOG_LEVELS.includes(level))
      throw new AgentsError("MESSAGE_INVALID", `Уровень логов ${JSON.stringify(level)}: нужно ${LOG_LEVELS.join("|")}`);
    sub.logs = { level };
  }
  if (opts.channels) {
    const channels: Record<string, { intervalMs: number }> = {};
    for (const [name, spec] of Object.entries(opts.channels)) {
      checkName("канал", name);
      channels[name] = { intervalMs: checkInterval(`channels.${name}.intervalMs`, spec?.intervalMs) };
    }
    if (Object.keys(channels).length) sub.channels = channels;
  }
  return sub;
}

/** Интервал подписки: число мс не меньше 200, иначе MESSAGE_INVALID. */
function checkInterval(field: string, v: unknown): number {
  if (typeof v !== "number" || !Number.isFinite(v) || v < MIN_SUBSCRIPTION_INTERVAL_MS)
    throw new AgentsError("MESSAGE_INVALID", `${field}: нужно число мс от ${MIN_SUBSCRIPTION_INTERVAL_MS}`);
  return Math.floor(v);
}

/**
 * Сводная подписка по действующим подпискам агента: интервалы — минимум, группы — объединение
 * в порядке появления, уровень лога — самый подробный, каналы — минимум по каждому. Поле, которого
 * нет ни в одной подписке, не указывается; подписок нет — {}.
 */
function summarize(a: Pick<AgentRecord, "subscriptions">, now = Date.now()): Subscription {
  const min = (cur: number | undefined, v: number) => (cur === undefined ? v : Math.min(cur, v));
  let status: number | undefined;
  let metrics: number | undefined;
  let level: LogEntryLevel | undefined;
  const groups: string[] = [];
  const channels: Record<string, number> = {};
  for (const s of a.subscriptions ?? []) {
    if (s.until <= now) continue;
    if (s.status) status = min(status, s.status.intervalMs);
    if (s.metrics?.intervalMs !== undefined) metrics = min(metrics, s.metrics.intervalMs);
    for (const g of s.metrics?.groups ?? []) if (!groups.includes(g)) groups.push(g);
    if (s.logs && (!level || LOG_LEVELS.indexOf(s.logs.level) < LOG_LEVELS.indexOf(level))) level = s.logs.level;
    for (const [name, c] of Object.entries(s.channels ?? {})) channels[name] = min(channels[name], c.intervalMs);
  }
  // Порядок полей постоянный: сводные сравниваются по JSON.
  const out: Subscription = {};
  if (status !== undefined) out.statusIntervalMs = status;
  if (metrics !== undefined) out.metricsIntervalMs = metrics;
  if (groups.length) out.metrics = groups;
  if (level) out.logLevel = level;
  const names = Object.keys(channels).sort();
  if (names.length) out.channels = Object.fromEntries(names.map((n) => [n, channels[n]]));
  return out;
}

/** Есть ли у агента истёкшие подписки. */
function hasExpired(a: AgentRecord, now: number): boolean {
  return (a.subscriptions ?? []).some((s) => s.until <= now);
}

/** Убрать истёкшие подписки из записи; true — запись изменилась. */
function dropExpired(a: AgentRecord, now: number): boolean {
  if (!hasExpired(a, now)) return false;
  const left = (a.subscriptions ?? []).filter((s) => s.until > now);
  if (left.length) a.subscriptions = left;
  else delete a.subscriptions;
  return true;
}

/** Начало или конец проблем агента: raise — начать, если такой ещё нет; settle — закончить подходящие. */
type AlertOp = { raise: Alert } | { settle: (a: Alert) => boolean };

const alertKey = (a: Alert) => `${a.type}\0${a.agentId}\0${a.domain ?? a.worker ?? ""}`;

function newAlert(
  agent: AgentRecord,
  type: AlertType,
  message: string,
  at: number,
  extra: { domain?: string; worker?: string } = {},
): Alert {
  return { type, agentId: agent.id, agentName: agent.name, active: true, message, at, ...extra };
}

/** Закончить проблему type (sub — раздел или воркер; пусто — проблема агента). */
function settleKey(type: AlertType, sub: string): AlertOp {
  return { settle: (a) => a.type === type && (a.domain ?? a.worker ?? "") === sub };
}

/**
 * Применить к записи агента начала и концы проблем. Результат — начавшиеся (их не было в записи)
 * и закончившиеся (были; active: false, текст начала, время — now).
 */
function applyAlerts(a: AgentRecord, ops: AlertOp[], now: number): { started: Alert[]; ended: Alert[] } {
  let list = [...(a.alerts ?? [])];
  const started: Alert[] = [];
  const ended: Alert[] = [];
  for (const op of ops) {
    if ("raise" in op) {
      const key = alertKey(op.raise);
      if (list.some((x) => alertKey(x) === key)) continue;
      list.push({ ...op.raise });
      started.push({ ...op.raise });
    } else {
      const keep: Alert[] = [];
      for (const x of list) {
        if (op.settle(x)) ended.push({ ...x, active: false, at: now });
        else keep.push(x);
      }
      list = keep;
    }
  }
  if (list.length) a.alerts = list;
  else delete a.alerts;
  return { started, ended };
}

/** status: degraded, воркеры в сбое и воркеры degraded (worker.health) — начала и концы проблем. */
function statusAlerts(agent: AgentRecord, st: Status, now: number): AlertOp[] {
  const ops: AlertOp[] = [];
  if (st.state === "degraded")
    ops.push({ raise: newAlert(agent, "degraded", st.message || "Агент не в порядке", now) });
  else ops.push(settleKey("degraded", ""));
  const down = new Set<string>();
  const degraded = new Set<string>();
  for (const w of Array.isArray(st.workers) ? st.workers : []) {
    if (typeof w?.name !== "string") continue;
    if (w.health === "degraded") {
      degraded.add(w.name);
      ops.push({
        raise: newAlert(agent, "workerDegraded", w.message || `Воркер ${w.name} не в порядке`, now, { worker: w.name }),
      });
    }
    if (!WORKER_DOWN.has(w.state)) continue;
    down.add(w.name);
    ops.push({ raise: newAlert(agent, "workerDown", `Воркер ${w.name}: ${w.state}`, now, { worker: w.name }) });
  }
  ops.push({
    settle: (a) =>
      (a.type === "workerDown" && !down.has(a.worker ?? "")) ||
      (a.type === "workerDegraded" && !degraded.has(a.worker ?? "")),
  });
  return ops;
}

/** Задача у агента в попытке ref.attempt и выполняется. */
function isHeld(j: JobRecord, agentId: string, ref: { attempt?: unknown }): boolean {
  return j.status === "running" && j.agentId === agentId && j.attempt === ref?.attempt;
}

const jobIdOf = (d: { jobId?: unknown }) => (typeof d?.jobId === "string" ? d.jobId : "");
const commandIdOf = (d: { commandId?: unknown }) => (typeof d?.commandId === "string" ? d.commandId : "");

/** Поле записи агента: undefined — убрать. */
function setField<K extends keyof AgentRecord>(a: AgentRecord, k: K, v: AgentRecord[K]): void {
  if (v === undefined) delete (a as unknown as Record<string, unknown>)[k];
  else a[k] = v;
}

/** Метки из запроса регистрации: объект строк в пределах ENROLL_MAX_LABELS и ENROLL_MAX_LABEL. */
function enrollLabels(v: unknown): Record<string, string> {
  if (v === undefined || v === null) return {};
  if (typeof v !== "object" || Array.isArray(v)) throw new AgentsError("MESSAGE_INVALID", "labels — объект строк");
  const entries = Object.entries(v);
  if (entries.length > ENROLL_MAX_LABELS) throw new AgentsError("MESSAGE_INVALID", `Меток больше ${ENROLL_MAX_LABELS}`);
  for (const [k, val] of entries) {
    if (!k || [...k].length > ENROLL_MAX_LABEL)
      throw new AgentsError("MESSAGE_INVALID", `Ключ метки — непустая строка до ${ENROLL_MAX_LABEL} символов`);
    if (typeof val !== "string" || [...val].length > ENROLL_MAX_LABEL)
      throw new AgentsError("MESSAGE_INVALID", `Значение метки ${k} — строка до ${ENROLL_MAX_LABEL} символов`);
  }
  return Object.fromEntries(entries) as Record<string, string>;
}

function safeEqual(a: string, b: string): boolean {
  const x = Buffer.from(a);
  const y = Buffer.from(b);
  return x.length === y.length && timingSafeEqual(x, y);
}

function checkDomain(domain: string): void {
  if (!domain) throw new AgentsError("MESSAGE_INVALID", "Нужен domain");
  checkName("domain", domain);
}

/** Неверное имя очереди, команды или раздела — MESSAGE_INVALID. */
function checkName(field: string, name: string): void {
  if (!validName(name))
    throw new AgentsError("MESSAGE_INVALID", `${field} ${JSON.stringify(name)} не по правилу ${NAME_PATTERN}`);
}

/**
 * Изменяющие методы Agents от имени actor (agents.by(actor)): actor попадает в задачу,
 * команду, снимок состояния и в событие audit.
 */
export class Actor {
  readonly actor: string;
  private readonly agents: Agents;

  constructor(agents: Agents, actor: string) {
    this.agents = agents;
    this.actor = actor;
  }

  enqueue(req: JobRequest): Promise<Job> {
    return this.agents["enqueueAs"](this.actor, req);
  }
  cancelJob(id: string): Promise<Job> {
    return this.agents["signalJob"](this.actor, id, "cancel");
  }
  stopJob(id: string): Promise<Job> {
    return this.agents["signalJob"](this.actor, id, "stop");
  }
  command(req: CommandRequest): Promise<Command> {
    return this.agents["commandAs"](this.actor, req);
  }
  call(req: CommandRequest): Promise<Command> {
    return this.agents["callAs"](this.actor, req);
  }
  setState(domain: string, spec: unknown, opts: { agentId?: string } = {}): Promise<DesiredState> {
    return this.agents["setStateAs"](this.actor, domain, spec, opts);
  }
  deleteState(domain: string, opts: { agentId?: string } = {}): Promise<DesiredState | null> {
    return this.agents["deleteStateAs"](this.actor, domain, opts);
  }
  rollbackState(domain: string, version: number, opts: { agentId?: string } = {}): Promise<DesiredState> {
    return this.agents["rollbackStateAs"](this.actor, domain, version, opts);
  }
  revoke(agentId: string): Promise<Agent> {
    return this.agents["revokeAs"](this.actor, agentId);
  }
  deleteAgent(agentId: string): Promise<void> {
    return this.agents["deleteAgentAs"](this.actor, agentId);
  }
  cancelCommand(id: string): Promise<Command> {
    return this.agents["cancelCommandAs"](this.actor, id);
  }
  updateAgent(agentId: string): Promise<Command> {
    return this.agents["updateAgentAs"](this.actor, agentId);
  }
  rotateKey(agentId: string): Promise<Command> {
    return this.agents["rotateKeyAs"](this.actor, agentId);
  }
  updateWorker(agentId: string, name: string): Promise<Command> {
    return this.agents["updateWorkerAs"](this.actor, agentId, name);
  }
  pauseWorker(agentId: string, name: string, opts: PauseWorkerOptions = {}): Promise<Command> {
    return this.agents["workerControlAs"](this.actor, "worker.pause", agentId, name, opts);
  }
  resumeWorker(agentId: string, name: string, opts: PauseWorkerOptions = {}): Promise<Command> {
    return this.agents["workerControlAs"](this.actor, "worker.resume", agentId, name, opts);
  }
}
