// Общее для частей Agents: Store, настройки, сессии этого процесса, события, аудит, условная
// запись агента. Части получают Context и не знают друг о друге.
import type { EventEmitter } from "node:events";

import { publicAgent } from "../model/public-agent";
import type {
  ActionRecord,
  Agent,
  AgentEvent,
  AgentRecord,
  Alert,
  AuditAction,
  AuditEntry,
  ConfigStatus,
  InvalidEvent,
  MetricsPoint,
} from "../model/types";
import type { LogEntry } from "../protocol/messages";
import type { Store } from "../store/store";
import { codeError, notFound, revoked } from "./errors";
import type { Settings } from "./options";
import type { Session } from "./session";

/** Точка метрик агента (событие metrics). */
export interface MetricsEvent extends MetricsPoint {
  agentId: string;
}

/** Пачка журнала агента (событие log). */
export interface LogEvent {
  agentId: string;
  entries: LogEntry[];
}

/** Проблема началась (active) или закончилась. */
export interface AlertEvent extends Alert {
  active: boolean;
}

/** Что-то изменилось в Store для агента: другим процессам — refresh(agentId). */
export interface ChangeEvent {
  agentId: string;
  reason: "session" | "config" | "revoke" | "delete";
}

/** Удалённый источник сборок агента дал новую версию (опция agentReleases). */
export interface ReleaseEvent {
  /** Новая версия агента. */
  version: string;
  /** Прежняя версия; нет — сборки получены впервые. */
  previous?: string;
  /** Источник: `github:owner/repo` или база сборок (url). */
  from: string;
}

export interface AgentsEvents {
  agent: [Agent];
  event: [AgentEvent];
  invalidEvent: [InvalidEvent];
  metrics: [MetricsEvent];
  log: [LogEvent];
  config: [ConfigStatus];
  alert: [AlertEvent];
  action: [ActionRecord];
  audit: [AuditEntry];
  change: [ChangeEvent];
  release: [ReleaseEvent];
}

/** Начавшиеся и закончившиеся проблемы. */
export interface AlertChanges {
  started: Alert[];
  ended: Alert[];
}

/** Итог условной записи: свежая запись (нет — агента нет) и записано ли изменение. */
interface Mutation {
  agent: AgentRecord | undefined;
  changed: boolean;
}

export interface Context {
  readonly store: Store;
  readonly settings: Settings;
  readonly instanceId: string;
  /** Сессии этого процесса: id агента → сессия. */
  readonly sessions: Map<string, Session>;
  log(msg: string, extra?: Record<string, unknown>): void;
  /** Событие; ошибка подписчика не мешает работе. */
  emit<K extends keyof AgentsEvents>(event: K, ...args: AgentsEvents[K]): void;
  listens(event: keyof AgentsEvents): boolean;
  /** Событие agent с видом записи для бэкенда. */
  emitAgent(a: AgentRecord): void;
  emitAlerts(changes: AlertChanges): void;
  audit(
    actor: string,
    action: AuditAction,
    agentId: string,
    details?: Record<string, unknown>,
  ): void;
  change(agentId: string, reason: ChangeEvent["reason"]): void;
  /**
   * Условная запись агента: fn меняет свежую запись (false — ничего не менять); запись изменилась
   * в Store — перечитать и повторить.
   */
  mutate(id: string, fn: (a: AgentRecord) => boolean): Promise<Mutation>;
  /** По одному на агента: открытие и закрытие сессий. */
  lock<T>(agentId: string, fn: () => Promise<T>): Promise<T>;
  /** Запись агента; нет — AGENT_NOT_FOUND. */
  agent(id: string): Promise<AgentRecord>;
  /** Сессия агента в этом процессе после welcome. */
  live(agentId: string): Session | undefined;
  /** Открытая сессия агента в этом процессе; иначе — ошибка с причиной. */
  localSession(agentId: string): Promise<Session>;
}

const MAX_MUTATE = 20;

export const createContext = (
  emitter: EventEmitter<AgentsEvents>,
  store: Store,
  settings: Settings,
  instanceId: string,
): Context => {
  const sessions = new Map<string, Session>();
  const locks = new Map<string, Promise<unknown>>();
  const log = settings.log;

  const ctx: Context = {
    store,
    settings,
    instanceId,
    sessions,
    log,

    emit(event, ...args) {
      try {
        (emitter.emit as (e: string, ...a: unknown[]) => boolean).call(
          emitter,
          event,
          ...args,
        );
      } catch (e) {
        log(`подписчик ${event} упал`, { err: String(e) });
      }
    },

    listens: event => emitter.listenerCount(event) > 0,

    emitAgent: a => ctx.emit("agent", publicAgent(a)),

    emitAlerts({ started, ended }) {
      for (const al of started) ctx.emit("alert", { ...al, active: true });
      for (const al of ended) ctx.emit("alert", { ...al, active: false });
    },

    audit(actor, action, agentId, details) {
      const e: AuditEntry = { at: Date.now(), actor, action, agentId };

      if (details) e.details = details;
      ctx.emit("audit", e);
    },

    change: (agentId, reason) => ctx.emit("change", { agentId, reason }),

    async mutate(id, fn) {
      for (let i = 0; i < MAX_MUTATE; i += 1) {
        const a = await store.getAgent(id);

        if (!a) return { agent: undefined, changed: false };
        a.configs ??= {};
        a.alerts ??= [];
        if (!fn(a)) return { agent: a, changed: false };
        if (await store.updateAgent(a)) return { agent: a, changed: true };
      }
      throw codeError("CONFLICT", "запись агента всё время меняется", 409);
    },

    lock(agentId, fn) {
      const run = (locks.get(agentId) ?? Promise.resolve()).then(fn, fn);
      const tail = run.catch(() => {});

      locks.set(agentId, tail);
      void tail.then(() => {
        if (locks.get(agentId) === tail) locks.delete(agentId);
      });

      return run;
    },

    async agent(id) {
      const a = await store.getAgent(id);

      if (!a) throw notFound();

      return a;
    },

    live(agentId) {
      const ss = sessions.get(agentId);

      return ss?.welcomed ? ss : undefined;
    },

    async localSession(agentId) {
      const ss = ctx.live(agentId);

      if (ss && !ss.closed) return ss;
      const a = await ctx.agent(agentId);

      if (a.revoked) throw revoked();
      if (a.online && a.session && a.session.instance !== instanceId)
        throw codeError(
          "AGENT_ELSEWHERE",
          `агент на связи с процессом ${a.session.instance}`,
        );
      throw codeError("AGENT_OFFLINE", "агент без связи");
    },
  };

  return ctx;
};
