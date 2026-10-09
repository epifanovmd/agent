// Agents — серверная часть связи с агентами. Фасад: собирает части из features/ (регистрация и
// ключи, сессии, приём сообщений, настройки, fetch, наблюдение, действия, выпуск) и передаёт им
// вызовы. Общее для частей — core/, данные — store/, HTTP и WebSocket — transport/.
import { EventEmitter } from "node:events";
import type { IncomingMessage, Server, ServerResponse } from "node:http";

import { type AgentsEvents, createContext } from "./core/context";
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
  type UpdateResult,
  type WorkerActionOptions,
} from "./features/actions";
import { Configs } from "./features/configs";
import { Connections } from "./features/connections";
import { Enrollment } from "./features/enrollment";
import { Inbound } from "./features/inbound";
import { installCommand, type InstallOptions } from "./features/install";
import { Observe, type WatchOptions, type WatchRef } from "./features/observe";
import { Release } from "./features/release";
import { type FetchInit, fetchReply, Tunnel } from "./features/tunnel";
import { publicAgent } from "./model/public-agent";
import type {
  Agent,
  Alert,
  ConfigRecord,
  ConfigStatus,
  UpdateCandidate,
  WorkerUpdateCandidate,
} from "./model/types";
import {
  type LogEntry,
  newId,
  type ReleaseManifest,
} from "./protocol/messages";
import { MemoryStore } from "./store/memory";
import type { Store } from "./store/store";
import { Transport } from "./transport/transport";

/** Части, которые изменяют что-то от имени actor (общие для Agents и Actor). */
interface Operations {
  enrollment: Enrollment;
  tunnel: Tunnel;
  configs: Configs;
  actions: Actions;
}

const operations = new WeakMap<Agents, Operations>();

export class Agents extends EventEmitter<AgentsEvents> {
  readonly store: Store;
  readonly instanceId: string;
  private readonly settings: Settings;
  private readonly ops: Operations;
  private readonly links: Connections;
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
    const observe = new Observe(ctx);
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
    const inbound = new Inbound(ctx, {
      reliable: {
        event: (ss, env) => observe.event(ss, env),
        "config.applied": (ss, env) => configs.applied(ss, env),
        "action.result": (ss, env) => actions.result(ss, env),
      },
      fetchReply,
      configsChanged: (agent, keys) => configs.emitStatuses(agent, keys),
      dropForeign: (ss, a) => links.dropForeign(ss, a),
      statusSeen: (agentId, workers) => actions.pendingSeen(agentId, workers),
    });

    this.transport = new Transport({
      settings: this.settings,
      manifest: () => releases.manifest(),
      enroll: (body, remote) => enrollment.enroll(body, remote),
      authenticate: header => enrollment.authenticate(header),
      open: (ss, env) => links.open(ss, env),
      process: (ss, env) => inbound.process(ss, env),
      closed: ss => links.closed(ss),
    });
    this.ops = { enrollment, tunnel: new Tunnel(ctx), configs, actions };
    operations.set(this, this.ops);
    this.links = links;
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

  /** Остановить: соединения закрываются кодом 1012 (агенты сразу подключатся снова). */
  async close(): Promise<void> {
    clearInterval(this.timer);
    this.ops.actions.close();
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

  deleteAgent(agentId: string): Promise<void> {
    return this.ops.enrollment.remove("", agentId);
  }

  // ── запрос к воркеру ──

  /**
   * HTTP-запрос к воркеру через агента (§7). Ответ — Response: status, headers, body (поток),
   * text(), json(), arrayBuffer(). Ошибка до ответа — AgentsError с кодом (AGENT_OFFLINE,
   * AGENT_ELSEWHERE, WORKER_UNKNOWN, WORKER_UNAVAILABLE, WORKER_INVALID, TIMEOUT, CANCELLED,
   * PATH_FORBIDDEN, BODY_TOO_LARGE, BUSY, DISCONNECTED); после — ошибка чтения потока body с тем же AgentsError.
   * Работает в процессе, у которого сессия агента.
   */
  fetch(
    agentId: string,
    worker: string,
    path: string,
    init: FetchInit = {},
  ): Promise<Response> {
    return this.ops.tunnel.fetch("", agentId, worker, path, init);
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
   * этого процесса и действуют, пока агент на связи с ним.
   */
  watch(agentId: string, opts: WatchOptions = {}): Promise<WatchRef> {
    return this.observe.watch(agentId, opts);
  }

  /** Снять наблюдателя. */
  unwatch(agentId: string, id: string): void {
    this.observe.unwatch(agentId, id);
  }

  // ── действия (§10) ──

  /** Перезапустить воркер; пока он занят (health.busy), замена ждёт — или force: сразу. */
  restartWorker(
    agentId: string,
    name: string,
    opts: WorkerActionOptions = {},
  ): Promise<void> {
    return this.ops.actions.restartWorker("", agentId, name, opts);
  }

  /**
   * Обновить воркер из выпуска до сборки в manifest.json; итог — { version, previous }. Пока
   * воркер занят (health.busy), замена ждёт — или force: сразу.
   */
  updateWorker(
    agentId: string,
    name: string,
    opts: WorkerActionOptions = {},
  ): Promise<UpdateResult> {
    return this.ops.actions.updateWorker("", agentId, name, opts);
  }

  /** Обновить агента до версии выпуска; итог — после запуска новой версии. */
  updateAgent(
    agentId: string,
    opts: ActionOptions = {},
  ): Promise<UpdateResult> {
    return this.ops.actions.updateAgent("", agentId, opts);
  }

  /** Сменить ключ агента: он переподключится с новым секретом. */
  rotateKey(agentId: string, opts: ActionOptions = {}): Promise<void> {
    return this.ops.actions.rotateKey("", agentId, opts);
  }

  /** Последние строки журнала агента или воркера. */
  logs(agentId: string, opts: LogsOptions = {}): Promise<LogEntry[]> {
    return this.ops.actions.logs("", agentId, opts);
  }

  // ── выпуск ──

  /** manifest.json каталога выпуска или null. */
  release(): Promise<ReleaseManifest | null> {
    return this.releases.manifest();
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
    return this.ops.tunnel.fetch(this.actor, agentId, worker, path, init);
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
  ): Promise<void> {
    return this.ops.actions.restartWorker(this.actor, agentId, name, opts);
  }
  updateWorker(
    agentId: string,
    name: string,
    opts: WorkerActionOptions = {},
  ): Promise<UpdateResult> {
    return this.ops.actions.updateWorker(this.actor, agentId, name, opts);
  }
  updateAgent(
    agentId: string,
    opts: ActionOptions = {},
  ): Promise<UpdateResult> {
    return this.ops.actions.updateAgent(this.actor, agentId, opts);
  }
  rotateKey(agentId: string, opts: ActionOptions = {}): Promise<void> {
    return this.ops.actions.rotateKey(this.actor, agentId, opts);
  }
  logs(agentId: string, opts: LogsOptions = {}): Promise<LogEntry[]> {
    return this.ops.actions.logs(this.actor, agentId, opts);
  }
}
