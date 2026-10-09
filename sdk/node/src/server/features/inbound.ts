// Приём сообщений агента после hello (§3, §6): ответы fetch, поток (status, metrics, log) с учётом
// seq и важное (event, config.applied, action.result) с подтверждением по id.
import type { Context } from "../core/context";
import type { Session } from "../core/session";
import { recheckAlerts } from "../model/alerts";
import { changedKeys, reportedFromStatus } from "../model/config-report";
import type { AgentRecord, MetricsPoint } from "../model/types";
import {
  parseLog,
  parseMessageId,
  parseMetrics,
  parseStatus,
} from "../protocol/checks";
import {
  type Envelope,
  RELIABLE,
  STREAM,
  type WorkerStatus,
} from "../protocol/messages";
import {
  messageIdSchema,
  messageTypeSchema,
  seqSchema,
} from "../protocol/schemas";

/** Обработчик важного сообщения; непустой результат — причина отказа (MESSAGE_INVALID). */
type ReliableHandler = (ss: Session, env: Envelope) => Promise<string>;

/** Что нужно приёму от других частей. */
export interface InboundDeps {
  reliable: Record<string, ReliableHandler>;
  fetchReply(ss: Session, env: Envelope): void;
  /** События config по изменившимся ключам. */
  configsChanged(agent: AgentRecord, keys: string[]): Promise<void>;
  /** Запись агента не за этой сессией — закрыть её. */
  dropForeign(ss: Session, a: AgentRecord): void;
  /** Пришёл status: отложенные замены воркеров ждут дальше. */
  statusSeen(agentId: string, workers: WorkerStatus[]): void;
}

/**
 * Сообщение потока, применённое к записи агента: invalid — причина отказа (запись всё равно
 * учитывает seq), then — что сделать после сохранения.
 */
type Applied =
  { invalid: string } | { then: (agent: AgentRecord) => void | Promise<void> };

export class Inbound {
  private readonly ctx: Context;
  private readonly deps: InboundDeps;
  private readonly stream: Record<
    string,
    (a: AgentRecord, data: unknown, now: number) => Applied
  >;

  constructor(ctx: Context, deps: InboundDeps) {
    this.ctx = ctx;
    this.deps = deps;
    this.stream = {
      status: (a, data, now) => this.status(a, data, now),
      metrics: (a, data) => this.metrics(a, data),
      log: (_a, data) => this.log(data),
    };
  }

  async process(ss: Session, env: Envelope): Promise<void> {
    if (ss.closed || this.ctx.sessions.get(ss.agentId) !== ss) return;
    const { data: type } = messageTypeSchema.safeParse(env.type);

    if (type === undefined)
      return sendError(ss, undefined, "MESSAGE_INVALID", "нет type");
    if (type === "fetch.head" || type === "fetch.chunk" || type === "fetch.end")
      return this.deps.fetchReply(ss, env);
    if (STREAM.has(type)) return this.streamed(ss, env);
    if (RELIABLE.has(type)) return this.reliable(ss, env);
    sendError(
      ss,
      messageIdSchema.safeParse(env.id).data,
      "UNKNOWN_TYPE",
      `незнакомый тип ${type}`,
    );
  }

  /** Поток: учёт seq в записи агента (повтор после переподключения к другому процессу не задваивается). */
  private async streamed(ss: Session, env: Envelope): Promise<void> {
    const seq = seqSchema.parse(env.seq);
    const now = Date.now();
    let dup = false;
    let foreign = false;
    let applied: Applied | undefined;
    const { agent } = await this.ctx
      .mutate(ss.agentId, a => {
        dup = false;
        applied = undefined;
        foreign = a.revoked || a.session?.id !== ss.id;
        if (foreign) return false;
        if (seq && seq <= a.lastSeq) {
          dup = true;

          return false;
        }
        if (seq) a.lastSeq = seq;
        a.lastSeenAt = now;
        applied = this.stream[env.type](a, env.data, now);

        return true;
      })
      .catch(e => {
        this.ctx.log("сообщение агента не сохранено", {
          type: env.type,
          err: String(e),
        });

        return { agent: undefined };
      });

    if (!agent) return;
    if (foreign) return this.deps.dropForeign(ss, agent);
    if (dup) return ss.ack([], agent.lastSeq);
    if (seq) ss.ack([], seq);
    if (!applied) return;
    if ("invalid" in applied) {
      this.ctx.log("неверное сообщение агента", {
        agent: agent.name,
        type: env.type,
        reason: applied.invalid,
      });

      return sendError(ss, undefined, "MESSAGE_INVALID", applied.invalid);
    }
    await applied.then(agent);
  }

  private status(a: AgentRecord, data: unknown, now: number): Applied {
    const st = parseStatus(data);

    if (!st.ok) return { invalid: st.error };
    const before = a.configs;

    a.status = st.value;
    a.statusAt = now;
    a.configs = reportedFromStatus(before, st.value, now);
    const keys = changedKeys(before, a.configs);
    const alerts = recheckAlerts(a, now);

    this.deps.statusSeen(a.id, st.value.workers);

    return {
      then: async agent => {
        this.ctx.emitAgent(agent);
        await this.deps.configsChanged(agent, keys);
        this.ctx.emitAlerts(alerts);
      },
    };
  }

  private metrics(a: AgentRecord, data: unknown): Applied {
    const m = parseMetrics(data);

    if (!m.ok) return { invalid: m.error };
    const point: MetricsPoint = m.value;

    a.metrics = point;

    return {
      then: agent => this.ctx.emit("metrics", { agentId: agent.id, ...point }),
    };
  }

  private log(data: unknown): Applied {
    const entries = parseLog(data);

    if (!entries.ok) return { invalid: entries.error };

    return {
      then: agent =>
        this.ctx.emit("log", { agentId: agent.id, entries: entries.value }),
    };
  }

  /** Важное: обработать так, чтобы повтор с тем же id ничего не задвоил, и подтвердить (§3). */
  private async reliable(ss: Session, env: Envelope): Promise<void> {
    const parsed = parseMessageId(env.type, env.id);

    if (!parsed.ok)
      return sendError(ss, undefined, "MESSAGE_INVALID", parsed.error);
    const id = parsed.value;

    try {
      const err = await this.deps.reliable[env.type](ss, env);

      if (err) {
        this.ctx.log("неверное сообщение агента", {
          agentId: ss.agentId,
          type: env.type,
          reason: err,
        });

        return sendError(ss, id, "MESSAGE_INVALID", err);
      }
      ss.ack([id]);
      if (env.type === "action.result") ss.flushAck();
    } catch (e) {
      this.ctx.log("сообщение агента не обработано", {
        agentId: ss.agentId,
        type: env.type,
        err: String(e),
      });
      sendError(ss, id, "INTERNAL", "сервер не обработал сообщение", true);
    }
  }
}

/** Сообщение error агенту (re — на какое сообщение). */
const sendError = (
  ss: Session,
  re: string | undefined,
  code: string,
  message: string,
  retryable = false,
): void => {
  ss.send({
    type: "error",
    ...(re !== undefined ? { re } : {}),
    data: { code, message, retryable },
  });
};
