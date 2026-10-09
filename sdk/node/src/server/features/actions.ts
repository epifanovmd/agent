// Встроенные действия (§10): отправка агенту, ожидание итога со сроком в памяти процесса, итог
// action.result (в том числе новый ключ после agent.rotateKey), отложенная замена занятого
// воркера (action.result с deferred, итог — action.done) и событие action.
import { LRUCache } from "lru-cache";
import { z } from "zod";

import type { Context } from "../core/context";
import { codeError, valid } from "../core/errors";
import type { Session } from "../core/session";
import { bounded } from "../lib/util";
import type { ActionName, ActionRecord, AgentRecord } from "../model/types";
import {
  type ActionDone,
  type ActionResult,
  parse,
  parseActionDone,
  parseActionResult,
  parseDeferred,
  parseLog,
  parseName,
  parseSecretHash,
} from "../protocol/checks";
import {
  Close,
  type Envelope,
  type LogEntry,
  newId,
  type WorkerStatus,
} from "../protocol/messages";
import { nameSchema } from "../protocol/schemas";

/** Параметры agent.logs. */
const logsOptionsSchema = z.object({
  /** Журнал воркера; нет — журнал агента. */
  worker: nameSchema.optional(),
  /** По умолчанию 200, не больше 5000. */
  lines: z
    .number()
    .transform(v => bounded(Math.floor(v), 200, 1, 5000))
    .optional(),
});

export type LogsOptions = z.input<typeof logsOptionsSchema>;

export interface ActionOptions {
  /** Срок ответа, мс. */
  timeoutMs?: number;
}

/**
 * Параметры замены воркера (restartWorker, updateWorker). Пока воркер занят, агент откладывает
 * замену (status.workers[].pending) и сразу отвечает: вызов возвращает { deferred: true }, а
 * фактический итог приходит событием action.
 */
export interface WorkerActionOptions extends ActionOptions {
  /** Заменить сразу, не дожидаясь окончания работы воркера. */
  force?: boolean;
  /**
   * Ждать фактического итога и отложенной замены: срок timeoutMs отсчитывается заново с каждым
   * status, где замена ещё ждёт.
   */
  wait?: boolean;
}

/** Итог обновления. */
export interface UpdateResult {
  version: string;
  previous?: string;
}

/** Замена отложена до окончания работы воркера; итог — событие action с тем же actionId. */
export interface Deferred {
  deferred: true;
  pending: "restart" | "update";
  actionId: string;
}

/** Итог restartWorker: воркер заменён или замена отложена. */
export type RestartResult = { deferred: false } | Deferred;

/** Итог updateWorker: воркер обновлён или замена отложена. */
export type WorkerUpdateResult =
  (UpdateResult & { deferred: false }) | Deferred;

/** Что нужно действиям от других частей: аргументы обновлений и приём нового ключа. */
export interface ActionDeps {
  /** Аргументы agent.update и worker.update (версия, адрес сборки, хеш, подпись). */
  agentUpdate(agent: AgentRecord): Promise<Record<string, unknown>>;
  workerUpdate(
    agent: AgentRecord,
    name: string,
  ): Promise<Record<string, unknown>>;
  acceptKey(agentId: string, hash: string): Promise<boolean>;
}

/** Что отправлено: итог допишется к этому. */
type Sent = Omit<ActionRecord, "status" | "finishedAt">;

/** Действие этого процесса, ждущее итога. */
interface Pending {
  action: Sent;
  resolve: (a: ActionRecord) => void;
  reject: (e: Error) => void;
  timer: NodeJS.Timeout;
  /** Замена воркера: срок заново, пока она ждёт; имя воркера — в action.args. */
  rearm?: () => void;
  /** Замена воркера отложена, а вызвавший ждёт action.done (wait). */
  wait?: boolean;
}

/** Сколько отложенных замен и итогов action.done помнить. */
const KEEP_DEFERRED = 10_000;

export class Actions {
  private readonly ctx: Context;
  private readonly deps: ActionDeps;
  /** Действия этого процесса, ждущие итога: id → ожидающий. */
  private readonly pending = new Map<string, Pending>();
  /** Отложенные замены этого процесса: id → что отправлено (для события action по action.done). */
  private readonly deferred = new LRUCache<string, Sent>({
    max: KEEP_DEFERRED,
  });
  /** Принятые action.done ("агент\nid"): повтор доставки не задваивает событие action. */
  private readonly finishedDone = new LRUCache<string, true>({
    max: KEEP_DEFERRED,
  });

  constructor(ctx: Context, deps: ActionDeps) {
    this.ctx = ctx;
    this.deps = deps;
  }

  async restartWorker(
    actor: string,
    agentId: string,
    name: string,
    opts: WorkerActionOptions,
  ): Promise<RestartResult> {
    valid(parseName("name", name));
    const r = await this.run(
      actor,
      agentId,
      "worker.restart",
      withForce({ name }, opts),
      opts.timeoutMs ?? this.ctx.settings.actionTimeoutMs,
      opts.wait,
    );

    return deferredOf(r) ?? { deferred: false };
  }

  async updateWorker(
    actor: string,
    agentId: string,
    name: string,
    opts: WorkerActionOptions,
  ): Promise<WorkerUpdateResult> {
    valid(parseName("name", name));
    const args = await this.deps.workerUpdate(
      await this.ctx.agent(agentId),
      name,
    );
    const r = await this.run(
      actor,
      agentId,
      "worker.update",
      withForce(args, opts),
      this.updateTimeout(opts),
      opts.wait,
    );

    return deferredOf(r) ?? { ...(r.result as UpdateResult), deferred: false };
  }

  async updateAgent(
    actor: string,
    agentId: string,
    opts: ActionOptions,
  ): Promise<UpdateResult> {
    const args = await this.deps.agentUpdate(await this.ctx.agent(agentId));

    return (
      await this.run(
        actor,
        agentId,
        "agent.update",
        args,
        this.updateTimeout(opts),
      )
    ).result as UpdateResult;
  }

  async rotateKey(
    actor: string,
    agentId: string,
    opts: ActionOptions,
  ): Promise<void> {
    await this.run(
      actor,
      agentId,
      "agent.rotateKey",
      undefined,
      opts.timeoutMs ?? this.ctx.settings.actionTimeoutMs,
    );
  }

  async logs(
    actor: string,
    agentId: string,
    opts: LogsOptions,
  ): Promise<LogEntry[]> {
    const args: Record<string, unknown> = {
      ...valid(parse(logsOptionsSchema, opts, "logs")),
    };
    const r = parseLog(
      (
        await this.run(
          actor,
          agentId,
          "agent.logs",
          args,
          this.ctx.settings.actionTimeoutMs,
        )
      ).result,
    );

    if (!r.ok) {
      this.ctx.log("ответ agent.logs не принят", { agentId, reason: r.error });
      throw codeError(
        "MESSAGE_INVALID",
        `ответ агента на agent.logs не принят: ${r.error}`,
        502,
      );
    }

    return r.value;
  }

  /**
   * Важное сообщение action.result: итог действия этого процесса; ошибка — причина отказа. Итог
   * незнакомого действия (его отправил другой процесс или процесс перезапущен) подтверждается и
   * пропускается: вызвавший получит TIMEOUT.
   */
  async result(ss: Session, env: Envelope): Promise<string> {
    const p = parseActionResult(env);

    if (!p.ok) return p.error;
    const d = p.value;
    const w = this.pending.get(d.re);

    if (!w || w.action.agentId !== ss.agentId) {
      this.ctx.log("итог незнакомого действия", {
        agentId: ss.agentId,
        re: d.re,
      });

      return "";
    }
    const fin = finished(w.action, d);
    let rotated = false;

    if (fin.status === "done" && w.rearm && parseDeferred(d.result)) {
      // Воркер занят: замена отложена, итог придёт в action.done.
      this.deferred.set(fin.id, w.action);
      if (w.wait) return (w.rearm(), "");

      return (this.settle(fin), "");
    }
    if (fin.status === "done" && fin.name === "agent.rotateKey") {
      const hash = parseSecretHash(d.result);

      if (hash) rotated = await this.deps.acceptKey(ss.agentId, hash);
      else fail(fin, "нет secretHash в итоге");
    }
    if (!this.settle(fin)) return "";
    this.ctx.emit("action", fin);
    if (rotated) {
      this.ctx.log("новый ключ агента принят — переподключение", {
        agentId: ss.agentId,
      });
      setImmediate(() => ss.close(Close.Restart, "ключ сменён"));
    }

    return "";
  }

  /**
   * Важное сообщение action.done: итог отложенной замены воркера — событие action (и итог
   * ожидающему с wait). Замену мог отправить другой процесс или этот до перезапуска: тогда запись
   * собирается из action.done (args — { name: воркер }, createdAt — время приёма).
   */
  done(ss: Session, env: Envelope): Promise<string> {
    const p = parseActionDone(env);

    if (!p.ok) return Promise.resolve(p.error);
    const d = p.value;
    const key = `${ss.agentId}\n${d.re}`;

    if (this.finishedDone.has(key)) return Promise.resolve("");
    this.finishedDone.set(key, true);
    const w = this.pending.get(d.re);
    const sent =
      (w?.action.agentId === ss.agentId ? w.action : undefined) ??
      this.deferred.get(d.re) ??
      sentFromDone(ss.agentId, d);
    const fin: ActionRecord = { ...finished(sent, d), deferred: true };

    this.deferred.delete(d.re);
    if (w?.action.agentId === ss.agentId) this.settle(fin);
    this.ctx.emit("action", fin);

    return Promise.resolve("");
  }

  /**
   * status агента: замены воркеров, которые агент отложил (pending — воркер занят), ждут итога
   * дальше — их срок отсчитывается заново.
   */
  pendingSeen(agentId: string, workers: WorkerStatus[]): void {
    for (const w of this.pending.values()) {
      if (w.action.agentId !== agentId || !w.rearm) continue;
      const name = w.action.args?.name;

      if (workers.some(x => x.name === name && x.pending)) w.rearm();
    }
  }

  /** Остановка: ожидающие — CANCELLED. */
  close(): void {
    for (const w of this.pending.values()) {
      clearTimeout(w.timer);
      w.reject(codeError("CANCELLED", "сервер остановлен"));
    }
    this.pending.clear();
  }

  private updateTimeout(opts: ActionOptions): number {
    return opts.timeoutMs ?? this.ctx.settings.updateTimeoutMs;
  }

  /**
   * Отправить действие и дождаться итога. Итог — важное сообщение: может прийти после
   * переподключения агента к этому же процессу. wait — у отложенной замены воркера ждать
   * action.done.
   */
  private async run(
    actor: string,
    agentId: string,
    name: ActionName,
    args: Record<string, unknown> | undefined,
    timeoutMs: number,
    wait = false,
  ): Promise<ActionRecord> {
    const ss = await this.ctx.localSession(agentId);
    const action: Pending["action"] = {
      id: newId(),
      agentId,
      name,
      createdAt: Date.now(),
    };

    if (args) action.args = args;
    if (actor) action.actor = actor;
    this.ctx.audit(actor, name, agentId, {
      actionId: action.id,
      ...(args ?? {}),
    });
    const done = new Promise<ActionRecord>((resolve, reject) => {
      const expire = () => {
        this.pending.delete(action.id);
        reject(codeError("TIMEOUT", `нет итога ${name} за ${timeoutMs} мс`));
      };
      const w: Pending = {
        action,
        resolve,
        reject,
        timer: setTimeout(expire, timeoutMs),
        wait,
      };

      if (name === "worker.restart" || name === "worker.update")
        w.rearm = () => {
          clearTimeout(w.timer);
          w.timer = setTimeout(expire, timeoutMs);
        };
      this.pending.set(action.id, w);
    });

    ss.send({
      type: "action",
      id: action.id,
      data: args ? { name, args } : { name },
    });
    const r = await done;

    if (r.status === "failed")
      throw codeError(
        r.error?.code ?? "ACTION_FAILED",
        r.error?.message ?? "действие не выполнено",
        409,
      );

    return r;
  }

  /** Отдать итог ожидающему; уже отдан — false (повтор доставки). */
  private settle(a: ActionRecord): boolean {
    const w = this.pending.get(a.id);

    if (!w) return false;
    clearTimeout(w.timer);
    this.pending.delete(a.id);
    w.resolve(a);

    return true;
  }
}

/** args действия с force, если он задан. */
const withForce = (
  args: Record<string, unknown>,
  opts: WorkerActionOptions,
): Record<string, unknown> => (opts.force ? { ...args, force: true } : args);

/** Замена отложена (итог action.result с deferred) — что вернуть вызвавшему. */
const deferredOf = (r: ActionRecord): Deferred | undefined => {
  const d = r.deferred ? undefined : parseDeferred(r.result);

  return d && { deferred: true, pending: d.pending, actionId: r.id };
};

/** Запись действия, которое отправил не этот процесс, — по action.done. */
const sentFromDone = (agentId: string, d: ActionDone): Sent => {
  const s: Sent = {
    id: d.re,
    agentId,
    name: d.name as ActionName,
    createdAt: Date.now(),
  };

  if (d.worker) s.args = { name: d.worker };

  return s;
};

/** Завершённая запись действия по итогу. */
const finished = (
  rec: Sent,
  d: Pick<ActionResult, "ok" | "result" | "error">,
): ActionRecord => {
  const fin: ActionRecord = {
    ...rec,
    status: d.ok ? "done" : "failed",
    finishedAt: Date.now(),
  };

  if (d.ok && d.result !== undefined) fin.result = d.result;
  if (!d.ok) fin.error = d.error;

  return fin;
};

const fail = (fin: ActionRecord, message: string): void => {
  fin.status = "failed";
  delete fin.result;
  fin.error = { code: "ACTION_FAILED", message };
};
