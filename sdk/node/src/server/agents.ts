// Agents — серверная часть связи с агентами. Фасад: собирает части из features/ (регистрация и
// ключи, сессии, приём сообщений, настройки, fetch, задачи, наблюдение и подписки на события,
// запросы воркеров, действия, выпуск, пересылка между процессами) и передаёт им вызовы. Общее для частей — core/, данные — store/, HTTP и WebSocket — transport/.
import { EventEmitter } from "node:events";
import type { IncomingMessage, Server, ServerResponse } from "node:http";

import { type AgentsEvents, createContext } from "./core/context";
import { codeError } from "./core/errors";
import {
  agentsDefaults,
  type AgentsOptions,
  resolveOptions,
  type Settings,
} from "./core/options";
import {
  type ActionOptions,
  Actions,
  type LogsOptions,
  type RestartResult,
  type UpdateResult,
  type WorkerActionOptions,
  type WorkerUpdateResult,
} from "./features/actions";
import { Configs } from "./features/configs";
import { Connections } from "./features/connections";
import { Enrollment } from "./features/enrollment";
import { Inbound } from "./features/inbound";
import { installCommand, type InstallOptions } from "./features/install";
import { type JobOptions, type JobResult, Jobs } from "./features/jobs";
import {
  type EventFilter,
  type EventHandler,
  Observe,
  type WaitEventOptions,
  type WatchOptions,
  type WatchRef,
} from "./features/observe";
import { type Fetcher, Relay } from "./features/relay";
import { Release } from "./features/release";
import { Requests } from "./features/requests";
import { type FetchInit, fetchReply, Tunnel } from "./features/tunnel";
import { capabilities } from "./model/manifest";
import { publicAgent } from "./model/public-agent";
import type {
  Agent,
  AgentEvent,
  Alert,
  ConfigRecord,
  ConfigStatus,
  ReleaseView,
  UpdateCandidate,
  WorkerCapabilities,
  WorkerUpdateCandidate,
} from "./model/types";
import { type JobStatus, type LogEntry, newId } from "./protocol/messages";
import { MemoryStore } from "./store/memory";
import type { Store } from "./store/store";
import { Transport } from "./transport/transport";

/**
 * Вызовы от имени actor (общие для Agents и Actor). Вызовы, которым нужна сессия агента, при
 * опции relay уходят в процесс с сессией (features/relay.ts).
 */
interface Operations {
  enrollment: Enrollment;
  configs: Configs;
  fetch(
    actor: string,
    agentId: string,
    worker: string,
    path: string,
    init: FetchInit,
  ): Promise<Response>;
  restartWorker(
    actor: string,
    agentId: string,
    name: string,
    opts: WorkerActionOptions,
  ): Promise<RestartResult>;
  updateWorker(
    actor: string,
    agentId: string,
    name: string,
    opts: WorkerActionOptions,
  ): Promise<WorkerUpdateResult>;
  updateAgent(
    actor: string,
    agentId: string,
    opts: ActionOptions,
  ): Promise<UpdateResult>;
  rotateKey(actor: string, agentId: string, opts: ActionOptions): Promise<void>;
  logs(actor: string, agentId: string, opts: LogsOptions): Promise<LogEntry[]>;
  runJob(
    actor: string,
    agentId: string,
    worker: string,
    opts: JobOptions,
  ): Promise<JobResult>;
  jobStatus(
    actor: string,
    agentId: string,
    worker: string,
    id: string,
  ): Promise<JobStatus>;
  cancelJob(
    actor: string,
    agentId: string,
    worker: string,
    id: string,
  ): Promise<JobStatus>;
}

const operations = new WeakMap<Agents, Operations>();

/** Параметры вызова из пересылки (JSON): нет — пустые. */
const opt = <T>(o: unknown): T => (o ?? {}) as T;

export class Agents extends EventEmitter<AgentsEvents> {
  readonly store: Store;
  readonly instanceId: string;
  private readonly settings: Settings;
  private readonly ops: Operations;
  private readonly links: Connections;
  private readonly actions: Actions;
  private readonly watchers: {
    watch(agentId: string, opts: WatchOptions): Promise<WatchRef>;
    unwatch(agentId: string, id: string): Promise<void>;
  };
  private readonly relay: Relay;
  private readonly observe: Observe;
  private readonly releases: Release;
  private readonly transport: Transport;
  private readonly timer: NodeJS.Timeout;

  constructor(opts: AgentsOptions = {}) {
    super();
    this.settings = resolveOptions(opts);
    this.store = opts.store ?? new MemoryStore();
    this.instanceId = opts.instanceId ?? newId().slice(0, 12);
    const ctx = createContext(this, this.store, this.settings, this.instanceId);

    const configs = new Configs(ctx);
    const jobs = new Jobs(ctx);
    const observe = new Observe(ctx, e => jobs.event(e));
    const releases = new Release(ctx);
    const links = new Connections(ctx, {
      known: (ss, c) => configs.known(ss, c),
      syncConfigs: ss => configs.sync(ss),
      applyWatch: ss => observe.apply(ss),
      sweepWatchers: now => observe.sweep(now),
    });
    const enrollment = new Enrollment(ctx, (agentId, reason) => {
      observe.forget(agentId);
      links.drop(agentId, reason);
    });
    const actions = new Actions(ctx, {
      agentUpdate: a => releases.agentUpdate(a),
      workerUpdate: (a, name) => releases.workerUpdate(a, name),
      acceptKey: (agentId, hash) => enrollment.acceptKey(agentId, hash),
    });
    const requests = new Requests(ctx);
    const inbound = new Inbound(ctx, {
      reliable: {
        event: (ss, env) => observe.event(ss, env),
        "config.applied": (ss, env) => configs.applied(ss, env),
        "action.result": (ss, env) => actions.result(ss, env),
        "action.done": (ss, env) => actions.done(ss, env),
      },
      fetchReply,
      workerRequest: (ss, env) => requests.handle(ss, env),
      configsChanged: (agent, keys) => configs.emitStatuses(agent, keys),
      dropForeign: (ss, a) => links.dropForeign(ss, a),
      statusSeen: (agentId, workers) => actions.pendingSeen(agentId, workers),
    });
    const tunnel = new Tunnel(ctx);
    const localFetch: Fetcher = (actor, agentId, worker, path, init) =>
      tunnel.fetch(actor, agentId, worker, path, init);
    // Вызовы в этом процессе (и пересланные сюда из других) — без пересылки дальше.
    const relay = new Relay(ctx, localFetch, {
      restartWorker: (actor, agentId, [name, o]) =>
        actions.restartWorker(actor, agentId, String(name), opt(o)),
      updateWorker: (actor, agentId, [name, o]) =>
        actions.updateWorker(actor, agentId, String(name), opt(o)),
      updateAgent: (actor, agentId, [o]) =>
        actions.updateAgent(actor, agentId, opt(o)),
      rotateKey: (actor, agentId, [o]) =>
        actions.rotateKey(actor, agentId, opt(o)),
      logs: (actor, agentId, [o]) => actions.logs(actor, agentId, opt(o)),
      watch: (_actor, agentId, [o]) => observe.watch(agentId, opt(o)),
      unwatch: async (_actor, agentId, [id]) =>
        observe.unwatch(agentId, String(id)),
      runJob: (actor, agentId, [worker, o], signal) =>
        jobs.run(
          actor,
          agentId,
          String(worker),
          { ...opt<JobOptions>(o), signal },
          localFetch,
        ),
    });
    const fetch = relay.fetch;

    this.transport = new Transport({
      settings: this.settings,
      pingIntervalMs: this.settings.pingIntervalMs,
      releaseEnabled: () => releases.enabled(),
      servedManifest: () => releases.served(),
      releaseFile: name => releases.file(name),
      downloadRelease: (url, signal) => releases.download(url, signal),
      installScript: () => releases.installScript(),
      enroll: (body, remote) => enrollment.enroll(body, remote),
      authenticate: header => enrollment.authenticate(header),
      open: (ss, env) => links.open(ss, env),
      process: (ss, env) => inbound.process(ss, env),
      closed: ss => links.closed(ss),
    });
    this.ops = {
      enrollment,
      configs,
      fetch,
      restartWorker: (actor, agentId, name, o) =>
        relay.run("restartWorker", actor, agentId, [name, o]),
      updateWorker: (actor, agentId, name, o) =>
        relay.run("updateWorker", actor, agentId, [name, o]),
      updateAgent: (actor, agentId, o) =>
        relay.run("updateAgent", actor, agentId, [o]),
      rotateKey: (actor, agentId, o) =>
        relay.run("rotateKey", actor, agentId, [o]),
      logs: (actor, agentId, o) => relay.run("logs", actor, agentId, [o]),
      runJob: (actor, agentId, worker, { signal, ...o }) =>
        relay.run("runJob", actor, agentId, [worker, o], signal),
      jobStatus: (actor, agentId, worker, id) =>
        jobs.status(actor, agentId, worker, id, fetch),
      cancelJob: (actor, agentId, worker, id) =>
        jobs.cancel(actor, agentId, worker, id, fetch),
    };
    operations.set(this, this.ops);
    this.links = links;
    this.watchers = {
      watch: (agentId, o) => relay.run("watch", "", agentId, [o]),
      unwatch: (agentId, id) => relay.run("unwatch", "", agentId, [id]),
    };
    this.actions = actions;
    this.relay = relay;
    this.observe = observe;
    this.releases = releases;

    if (!opts.enrollToken && !opts.enroll)
      ctx.log("нет enrollToken и enroll: регистрация новых агентов закрыта");
    const sweep = setInterval(
      () =>
        void links
          .sweep()
          .catch(e => ctx.log("обход не удался", { err: String(e) })),
      agentsDefaults.sweepIntervalMs,
    );

    sweep.unref();
    this.timer = sweep;
  }

  // ── подключение к HTTP-серверу ──

  /** WebSocket агентов на этом сервере (`GET /api/v1/agent-link`). */
  attach(server: Server): void {
    this.transport.attach(server);
  }

  /** HTTP-маршруты агентов (регистрация, выпуск, install.sh); обработан — true. */
  handle(req: IncomingMessage, res: ServerResponse): Promise<boolean> {
    return this.transport.handle(req, res);
  }

  /**
   * Принять вызов, пересланный другим процессом (опция relay): выполнить его здесь и ответить
   * (fetch — потоком). Вызывающему маршрут доверяет: закройте его сетью или relaySecret.
   */
  handleRelay(req: IncomingMessage, res: ServerResponse): Promise<void> {
    return this.relay.handle(req, res);
  }

  /** Остановить: соединения закрываются кодом 1012 (агенты сразу подключатся снова). */
  async close(): Promise<void> {
    clearInterval(this.timer);
    this.releases.close();
    this.actions.close();
    const offline = this.links.close();

    this.transport.close();
    await offline;
  }

  /** Действия от имени actor: он попадает в аудит (событие audit), записи настроек и итоги действий. */
  by(actor: string): Actor {
    return new Actor(this, actor);
  }

  // ── агенты ──

  async listAgents(): Promise<Agent[]> {
    return (await this.store.listAgents()).map(publicAgent);
  }

  async getAgent(id: string): Promise<Agent | undefined> {
    const a = await this.store.getAgent(id);

    return a && publicAgent(a);
  }

  /** Текущие проблемы (всех агентов или одного) — из записей агентов. */
  async listAlerts(agentId?: string): Promise<Alert[]> {
    const list = agentId
      ? [await this.store.getAgent(agentId)]
      : await this.store.listAgents();

    return list.flatMap(a => a?.alerts ?? []);
  }

  revoke(agentId: string): Promise<Agent> {
    return this.ops.enrollment.revoke("", agentId);
  }

  /**
   * Что умеет воркер агента — из его манифеста (последний status): маршруты, события, задачи,
   * ключи настроек и запросы к серверу со схемами. Нет агента — AGENT_NOT_FOUND; воркер себя не
   * описал (нет в status или нет манифеста) — undefined.
   */
  async capabilities(
    agentId: string,
    worker: string,
  ): Promise<WorkerCapabilities | undefined> {
    const a = await this.store.getAgent(agentId);

    if (!a) throw codeError("AGENT_NOT_FOUND", "агент не найден");

    return capabilities(publicAgent(a), worker);
  }

  deleteAgent(agentId: string): Promise<void> {
    return this.ops.enrollment.remove("", agentId);
  }

  // ── запрос к воркеру ──

  /**
   * HTTP-запрос к воркеру через агента (§7). Ответ — Response: status, headers, body (поток),
   * text(), json(), arrayBuffer(). Ошибка до ответа — AgentsError с кодом (AGENT_OFFLINE,
   * AGENT_ELSEWHERE, WORKER_UNKNOWN, WORKER_UNAVAILABLE, WORKER_INVALID, TIMEOUT, CANCELLED,
   * PATH_FORBIDDEN, ROUTE_UNDECLARED, JOB_UNKNOWN, REQUEST_INVALID, BODY_TOO_LARGE, BUSY, DISCONNECTED);
   * после — ошибка чтения потока body с тем же AgentsError.
   * Сессия агента в другом процессе — запрос уходит туда (опция relay), без relay — AGENT_ELSEWHERE.
   */
  fetch(
    agentId: string,
    worker: string,
    path: string,
    init: FetchInit = {},
  ): Promise<Response> {
    return this.ops.fetch("", agentId, worker, path, init);
  }

  // ── задачи воркера (§12) ──

  /**
   * Задача воркеру: POST /jobs. Быстрая (200) — итог сразу; долгая (202) — ждать события
   * job.done, job.failed или job.cancelled не дольше timeoutMs (по умолчанию 30 с), не дождались —
   * state: running. Тип нет в manifest.jobs — JOB_UNKNOWN; data не по схеме (validateJobs) —
   * JOB_INVALID; отказ воркера — JOB_REJECTED. События задач доходят и до onEvent.
   */
  runJob(
    agentId: string,
    worker: string,
    opts: JobOptions,
  ): Promise<JobResult> {
    return this.ops.runJob("", agentId, worker, opts);
  }

  /** Состояние задачи воркера (GET /jobs/{id}); нет задачи — JOB_NOT_FOUND. */
  jobStatus(agentId: string, worker: string, id: string): Promise<JobStatus> {
    return this.ops.jobStatus("", agentId, worker, id);
  }

  /** Прервать долгую задачу (POST /jobs/{id}/cancel); нет задачи — JOB_NOT_FOUND. */
  cancelJob(agentId: string, worker: string, id: string): Promise<JobStatus> {
    return this.ops.cancelJob("", agentId, worker, id);
  }

  // ── настройки ──

  /**
   * Задать значение ключа настроек воркера на агенте; версия растёт с каждым вызовом. Агент на
   * связи с этим процессом получает config.put сразу, остальные — при подключении (или после
   * refresh в процессе с сессией).
   */
  setConfig(
    agentId: string,
    worker: string,
    key: string,
    data: unknown,
  ): Promise<ConfigRecord> {
    return this.ops.configs.set("", agentId, worker, key, data);
  }

  /** Удалить ключ настроек: агент удалит его у себя и у воркера. false — ключа не было. */
  deleteConfig(agentId: string, worker: string, key: string): Promise<boolean> {
    return this.ops.configs.delete("", agentId, worker, key);
  }

  async getConfig(
    agentId: string,
    worker: string,
    key: string,
  ): Promise<ConfigRecord | undefined> {
    return (await this.store.listConfigs(agentId)).find(
      c => c.worker === worker && c.key === key,
    );
  }

  listConfigs(agentId: string): Promise<ConfigRecord[]> {
    return this.store.listConfigs(agentId);
  }

  /** Статус ключей (желаемая, доставленная, применённая версия, ошибка); worker — только его ключи. */
  configStatus(agentId: string, worker?: string): Promise<ConfigStatus[]> {
    return this.ops.configs.status(agentId, worker);
  }

  // ── наблюдение ──

  /**
   * Наблюдатель: пока он есть, агент присылает метрики чаще и журнал подробнее (§9). Повтор с тем
   * же id продлевает и заменяет параметры. Наблюдатели сводятся в один watch. Живут в памяти
   * процесса с сессией агента (опция relay — туда и уходят) и действуют, пока агент на связи с
   * ним.
   */
  watch(agentId: string, opts: WatchOptions = {}): Promise<WatchRef> {
    return this.watchers.watch(agentId, opts);
  }

  /** Снять наблюдателя. */
  unwatch(agentId: string, id: string): Promise<void> {
    return this.watchers.unwatch(agentId, id);
  }

  /**
   * Подписка на события типа type воркера worker (агента agentId или любого): handler — после
   * onEvent и подтверждения, его ошибка только пишется в журнал. Итог — отписка. Агент известен и
   * манифест есть — тип сверяется сразу (нет в events — EVENT_UNDECLARED, 409), иначе — по
   * первому событию воркера (предупреждение в журнал). Видны события, принятые этим процессом.
   */
  subscribeEvents(
    filter: EventFilter,
    handler: EventHandler,
  ): Promise<() => void> {
    return this.observe.subscribe(filter, handler);
  }

  /**
   * Первое событие type воркера worker агента agentId после вызова (match — своё условие, по
   * data): не дождались за timeoutMs (по умолчанию 30 000) — TIMEOUT, отмена signal — CANCELLED.
   * Событие, пришедшее до вызова, не попадает: начните ждать до действия, которое его вызовет.
   */
  waitEvent(
    agentId: string,
    worker: string,
    type: string,
    opts: WaitEventOptions = {},
  ): Promise<AgentEvent> {
    return this.observe.waitEvent(agentId, worker, type, opts);
  }

  // ── действия (§10) ──

  /**
   * Перезапустить воркер. Свободен — итог после запуска ({ deferred: false }); занят
   * (health.busy) — замена отложена, сразу { deferred: true, pending, actionId }, итог — событие
   * action. force — сразу, wait — ждать итога и отложенной замены.
   */
  restartWorker(
    agentId: string,
    name: string,
    opts: WorkerActionOptions = {},
  ): Promise<RestartResult> {
    return this.ops.restartWorker("", agentId, name, opts);
  }

  /**
   * Обновить воркер из выпуска до сборки в manifest.json; итог — { version, previous }. Занят
   * (health.busy) — как у restartWorker: { deferred: true, … } и событие action.
   */
  updateWorker(
    agentId: string,
    name: string,
    opts: WorkerActionOptions = {},
  ): Promise<WorkerUpdateResult> {
    return this.ops.updateWorker("", agentId, name, opts);
  }

  /** Обновить агента до версии выпуска; итог — после запуска новой версии. */
  updateAgent(
    agentId: string,
    opts: ActionOptions = {},
  ): Promise<UpdateResult> {
    return this.ops.updateAgent("", agentId, opts);
  }

  /** Сменить ключ агента: он переподключится с новым секретом. */
  rotateKey(agentId: string, opts: ActionOptions = {}): Promise<void> {
    return this.ops.rotateKey("", agentId, opts);
  }

  /** Последние строки журнала агента или воркера. */
  logs(agentId: string, opts: LogsOptions = {}): Promise<LogEntry[]> {
    return this.ops.logs("", agentId, opts);
  }

  // ── выпуск ──

  /**
   * Итоговый выпуск или null: агент и его воркеры — из удалённого источника (agentReleases), воркеры
   * проекта — из releasesDir; у каждой сборки — источник (source) и ссылка (url).
   */
  release(): Promise<ReleaseView | null> {
    return this.releases.manifest();
  }

  /** Проверить удалённый источник выпуска сейчас (не дожидаясь checkIntervalMs) и вернуть выпуск. */
  checkRelease(): Promise<ReleaseView | null> {
    return this.releases.check();
  }

  /** Агенты, чья версия не как в выпуске и для чьих os/arch есть сборка. */
  updateCandidates(): Promise<UpdateCandidate[]> {
    return this.releases.updateCandidates();
  }

  /** Воркеры из выпуска, чья версия не как у новейшей сборки в manifest.json. */
  workerUpdateCandidates(): Promise<WorkerUpdateCandidate[]> {
    return this.releases.workerUpdateCandidates();
  }

  /**
   * Команда установки агента одной строкой: `curl -fsSL '<адрес>/api/v1/agent-link/install.sh' |
   * sudo sh -s -- --token '…' [флаги]`. Неверные значения — MESSAGE_INVALID.
   */
  installCommand(opts: InstallOptions): string {
    return installCommand(opts, this.settings.baseUrl);
  }

  // ── несколько процессов ──

  /**
   * Перечитать Store и применить то, что изменили другие процессы (без agentId — для всех
   * сессий этого процесса): отзыв и удаление (соединение закрывается 4401), сессию в другом
   * процессе (4409), настройки. Процесс, изменивший данные, сообщает событием change; бэкенд
   * передаёт его остальным (например, Postgres NOTIFY).
   */
  refresh(agentId?: string): Promise<void> {
    return this.links.refresh(agentId);
  }
}

/** Изменяющие методы от имени actor (agents.by(actor)). */
export class Actor {
  readonly actor: string;
  private readonly ops: Operations;

  constructor(agents: Agents, actor: string) {
    this.ops = operations.get(agents)!;
    this.actor = actor;
  }

  revoke(agentId: string): Promise<Agent> {
    return this.ops.enrollment.revoke(this.actor, agentId);
  }
  deleteAgent(agentId: string): Promise<void> {
    return this.ops.enrollment.remove(this.actor, agentId);
  }
  fetch(
    agentId: string,
    worker: string,
    path: string,
    init: FetchInit = {},
  ): Promise<Response> {
    return this.ops.fetch(this.actor, agentId, worker, path, init);
  }
  runJob(
    agentId: string,
    worker: string,
    opts: JobOptions,
  ): Promise<JobResult> {
    return this.ops.runJob(this.actor, agentId, worker, opts);
  }
  jobStatus(agentId: string, worker: string, id: string): Promise<JobStatus> {
    return this.ops.jobStatus(this.actor, agentId, worker, id);
  }
  cancelJob(agentId: string, worker: string, id: string): Promise<JobStatus> {
    return this.ops.cancelJob(this.actor, agentId, worker, id);
  }
  setConfig(
    agentId: string,
    worker: string,
    key: string,
    data: unknown,
  ): Promise<ConfigRecord> {
    return this.ops.configs.set(this.actor, agentId, worker, key, data);
  }
  deleteConfig(agentId: string, worker: string, key: string): Promise<boolean> {
    return this.ops.configs.delete(this.actor, agentId, worker, key);
  }
  restartWorker(
    agentId: string,
    name: string,
    opts: WorkerActionOptions = {},
  ): Promise<RestartResult> {
    return this.ops.restartWorker(this.actor, agentId, name, opts);
  }
  updateWorker(
    agentId: string,
    name: string,
    opts: WorkerActionOptions = {},
  ): Promise<WorkerUpdateResult> {
    return this.ops.updateWorker(this.actor, agentId, name, opts);
  }
  updateAgent(
    agentId: string,
    opts: ActionOptions = {},
  ): Promise<UpdateResult> {
    return this.ops.updateAgent(this.actor, agentId, opts);
  }
  rotateKey(agentId: string, opts: ActionOptions = {}): Promise<void> {
    return this.ops.rotateKey(this.actor, agentId, opts);
  }
  logs(agentId: string, opts: LogsOptions = {}): Promise<LogEntry[]> {
    return this.ops.logs(this.actor, agentId, opts);
  }
}
