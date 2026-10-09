// Встроенные действия (§10): отправка агенту, ожидание итога со сроком в памяти процесса, итог
// action.result (в том числе новый ключ после agent.rotateKey) и событие action.
import { z } from "zod";

import type { Context } from "../core/context";
import { codeError, valid } from "../core/errors";
import type { Session } from "../core/session";
import { bounded } from "../lib/util";
import type { ActionName, ActionRecord, AgentRecord } from "../model/types";
import {
  type ActionResult,
  parse,
  parseActionResult,
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
 * замену (status.workers[].pending) — итог ждёт её, а срок timeoutMs отсчитывается заново с
 * каждым status, где замена ещё ждёт.
 */
export interface WorkerActionOptions extends ActionOptions {
  /** Заменить сразу, не дожидаясь окончания работы воркера. */
  force?: boolean;
}

/** Итог обновления. */
export interface UpdateResult {
  version: string;
  previous?: string;
}

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

/** Действие этого процесса, ждущее итога. */
interface Pending {
  /** Что отправлено: итог допишется к этому. */
  action: Omit<ActionRecord, "status" | "finishedAt">;
  resolve: (a: ActionRecord) => void;
  reject: (e: Error) => void;
  timer: NodeJS.Timeout;
  /** Срок заново (замена воркера ждёт, пока он занят); имя воркера — в action.args. */
  rearm?: () => void;
}

export class Actions {
  private readonly ctx: Context;
  private readonly deps: ActionDeps;
  /** Действия этого процесса, ждущие итога: id → ожидающий. */
  private readonly pending = new Map<string, Pending>();

  constructor(ctx: Context, deps: ActionDeps) {
    this.ctx = ctx;
    this.deps = deps;
  }

  async restartWorker(
    actor: string,
    agentId: string,
    name: string,
    opts: WorkerActionOptions,
  ): Promise<void> {
    valid(parseName("name", name));
    await this.run(
      actor,
      agentId,
      "worker.restart",
      withForce({ name }, opts),
      opts.timeoutMs ?? this.ctx.settings.actionTimeoutMs,
    );
  }

  async updateWorker(
    actor: string,
    agentId: string,
    name: string,
    opts: WorkerActionOptions,
  ): Promise<UpdateResult> {
    valid(parseName("name", name));
    const args = await this.deps.workerUpdate(
      await this.ctx.agent(agentId),
      name,
    );

    return (await this.run(
      actor,
      agentId,
      "worker.update",
      withForce(args, opts),
      this.updateTimeout(opts),
    )) as UpdateResult;
  }

  async updateAgent(
    actor: string,
    agentId: string,
    opts: ActionOptions,
  ): Promise<UpdateResult> {
    const args = await this.deps.agentUpdate(await this.ctx.agent(agentId));

    return (await this.run(
      actor,
      agentId,
      "agent.update",
      args,
      this.updateTimeout(opts),
    )) as UpdateResult;
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
      await this.run(
        actor,
        agentId,
        "agent.logs",
        args,
        this.ctx.settings.actionTimeoutMs,
      ),
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
   * переподключения агента к этому же процессу.
   */
  private async run(
    actor: string,
    agentId: string,
    name: ActionName,
    args: Record<string, unknown> | undefined,
    timeoutMs: number,
  ): Promise<unknown> {
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

    return r.result;
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

/** Завершённая запись действия по итогу. */
const finished = (rec: Pending["action"], d: ActionResult): ActionRecord => {
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
