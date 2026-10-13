// Agents — серверная часть связи с агентами. Фасад: собирает части из features/ (регистрация и
// ключи, сессии, приём сообщений, настройки, fetch, наблюдение, запросы воркеров, действия,
// раздача сборок) и передаёт им вызовы. Общее для частей — core/, данные — store/, HTTP и
// WebSocket — transport/.
import { EventEmitter } from "node:events";
import type { IncomingMessage, Server, ServerResponse } from "node:http";

import { type AgentsEvents, createContext } from "./core/context";
import { AgentsError } from "./core/errors";
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
  type UpdateAgentOptions,
  type UpdateResult,
  type WorkerActionOptions,
  type WorkerUpdateResult,
} from "./features/actions";
import { Configs } from "./features/configs";
import { Connections } from "./features/connections";
import { Enrollment } from "./features/enrollment";
import { Inbound } from "./features/inbound";
import { Observe, type Watch, type WatchOptions } from "./features/observe";
import { Release } from "./features/release";
import { Requests } from "./features/requests";
import { type FetchInit, fetchReply, Tunnel } from "./features/tunnel";
import { publicAgent } from "./model/public-agent";
import type {
  Agent,
  ConfigRecord,
  ConfigStatus,
  ReleaseView,
} from "./model/types";
import { type LogEntry, newId } from "./protocol/messages";
import { MemoryStore } from "./store/memory";
import type { Store } from "./store/store";
import { Transport } from "./transport/transport";

export class Agents extends EventEmitter<AgentsEvents> {
  readonly store: Store;
  readonly instanceId: string;
  private readonly settings: Settings;
  private readonly enrollment: Enrollment;
  private readonly configs: Configs;
  private readonly links: Connections;
  private readonly actions: Actions;
  private readonly observe: Observe;
  private readonly tunnel: Tunnel;
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

    this.transport = new Transport({
      settings: this.settings,
      pingIntervalMs: this.settings.pingIntervalMs,
      releaseEnabled: () => releases.enabled(),
      servedManifest: () => releases.served(),
      releaseFile: name => releases.file(name),
      enroll: (body, remote) => enrollment.enroll(body, remote),
      authenticate: header => enrollment.authenticate(header),
      open: (ss, env) => links.open(ss, env),
      process: (ss, env) => inbound.process(ss, env),
      closed: ss => links.closed(ss),
    });
    this.enrollment = enrollment;
    this.configs = configs;
    this.links = links;
    this.actions = actions;
    this.observe = observe;
    this.tunnel = new Tunnel(ctx);
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

  /**
   * HTTP-маршруты агентов (регистрация, сборки); обработан — true, чужой путь — false.
   * Ошибку SDK отвечает сам ({ code, message }), наружу не бросает.
   */
  handle(req: IncomingMessage, res: ServerResponse): Promise<boolean> {
    return this.transport.handle(req, res);
  }

  /** Остановить: соединения закрываются кодом 1012 (агенты сразу подключатся снова). */
  async close(): Promise<void> {
    clearInterval(this.timer);
    this.actions.close();
    const offline = this.links.close();

    this.transport.close();
    await offline;
  }

  // ── агенты ──

  async listAgents(): Promise<Agent[]> {
    return (await this.store.listAgents()).map(publicAgent);
  }

  async getAgent(id: string): Promise<Agent | undefined> {
    const a = await this.store.getAgent(id);

    return a && publicAgent(a);
  }

  /** Отозвать агента: его соединение закрывается (4401), ключ больше не принимается. */
  revoke(agentId: string): Promise<Agent> {
    return this.enrollment.revoke(agentId);
  }

  deleteAgent(agentId: string): Promise<void> {
    return this.enrollment.remove(agentId);
  }

  // ── запрос к воркеру ──

  /**
   * HTTP-запрос к воркеру через агента (§7). Ответ — Response: status, headers, body (поток),
   * text(), json(), arrayBuffer(). Ошибка до ответа — AgentsError с кодом (AGENT_OFFLINE,
   * AGENT_ELSEWHERE, WORKER_UNKNOWN, WORKER_UNAVAILABLE, WORKER_INVALID, TIMEOUT, CANCELLED,
   * PATH_FORBIDDEN, ROUTE_UNDECLARED, JOB_UNKNOWN, BODY_TOO_LARGE, BUSY, DISCONNECTED);
   * после — ошибка чтения потока body с тем же AgentsError. Сессия агента в другом процессе
   * бэкенда — AGENT_ELSEWHERE.
   */
  fetch(
    agentId: string,
    worker: string,
    path: string,
    init: FetchInit = {},
  ): Promise<Response> {
    return this.tunnel.fetch(agentId, worker, path, init);
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
    return this.configs.set(agentId, worker, key, data);
  }

  /** Удалить ключ настроек: агент удалит его у себя и у воркера. false — ключа не было. */
  deleteConfig(agentId: string, worker: string, key: string): Promise<boolean> {
    return this.configs.delete(agentId, worker, key);
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
    return this.configs.status(agentId, worker);
  }

  // ── наблюдение ──

  /**
   * Наблюдатель: пока он есть, агент присылает метрики чаще и журнал подробнее (§9). Наблюдатель
   * продлевается сам, пока его не закроют (close) или не закончится процесс: тогда он истекает
   * через ttlMs. Повтор с тем же id заменяет параметры. Наблюдатели сводятся в один watch;
   * живут в памяти этого процесса и действуют, пока агент на связи с ним.
   */
  async watch(agentId: string, opts: WatchOptions = {}): Promise<Watch> {
    const ref = await this.observe.watch(agentId, opts);
    const renew = setInterval(
      () =>
        void this.observe
          .watch(agentId, { ...opts, id: ref.id })
          .then(r => (w.until = r.until))
          .catch(e => {
            this.settings.log("наблюдатель не продлён", {
              agentId,
              id: ref.id,
              err: String(e),
            });
            if (e instanceof AgentsError && e.code === "AGENT_NOT_FOUND")
              clearInterval(renew);
          }),
      Math.max((ref.until - Date.now()) / 2, 500),
    );

    renew.unref();
    const w: Watch = {
      ...ref,
      close: () => {
        clearInterval(renew);

        return this.unwatch(agentId, ref.id);
      },
    };

    return w;
  }

  /** Снять наблюдателя по id (например, созданного другим процессом). */
  async unwatch(agentId: string, id: string): Promise<void> {
    this.observe.unwatch(agentId, id);
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
    return this.actions.restartWorker(agentId, name, opts);
  }

  /**
   * Обновить воркер со сборкой с сервера до сборки в manifest.json; итог — { version, previous }. Занят
   * (health.busy) — как у restartWorker: { deferred: true, … } и событие action.
   */
  updateWorker(
    agentId: string,
    name: string,
    opts: WorkerActionOptions = {},
  ): Promise<WorkerUpdateResult> {
    return this.actions.updateWorker(agentId, name, opts);
  }

  /**
   * Обновить агента до версии version из его каталога сборок (§11): сборку под свою платформу
   * агент берёт и проверяет сам; итог — после запуска новой версии.
   */
  updateAgent(
    agentId: string,
    opts: UpdateAgentOptions,
  ): Promise<UpdateResult> {
    return this.actions.updateAgent(agentId, opts);
  }

  /** Сменить ключ агента: он переподключится с новым секретом. */
  rotateKey(agentId: string, opts: ActionOptions = {}): Promise<void> {
    return this.actions.rotateKey(agentId, opts);
  }

  /** Последние строки журнала агента или воркера. */
  logs(agentId: string, opts: LogsOptions = {}): Promise<LogEntry[]> {
    return this.actions.logs(agentId, opts);
  }

  // ── сборки ──

  /** Сборки воркеров проекта из releasesDir (у каждой — ссылка url) или null. */
  release(): Promise<ReleaseView | null> {
    return this.releases.manifest();
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
