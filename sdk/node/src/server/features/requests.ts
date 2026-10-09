// Запросы воркеров к серверу (§12): сообщение агента request → обработчик бэкенда
// onWorkerRequest → request.result. Запрос приходит в процесс, с которым агент на связи, и
// обрабатывается там; не важное сообщение — без подтверждения, ответ — только в это соединение.
import type { Context } from "../core/context";
import { AgentsError } from "../core/errors";
import type { Session } from "../core/session";
import { jsonSchemaProblems } from "../lib/json-schema";
import { bounded } from "../lib/util";
import { workerManifest } from "../model/manifest";
import { publicAgent } from "../model/public-agent";
import type { Agent } from "../model/types";
import { parseWorkerRequest } from "../protocol/checks";
import {
  type Envelope,
  type ErrorInfo,
  MAX_REQUEST_RESULT,
  MAX_REQUEST_TIMEOUT_MS,
  REQUEST_RESULT,
  REQUEST_TIMEOUT_MS,
} from "../protocol/messages";
import { messageIdSchema } from "../protocol/schemas";

/** Ответ воркеру: data или ошибка. */
type Outcome = { ok: true; data?: unknown } | { ok: false; error: ErrorInfo };

const refuse = (code: string, message: string): Outcome => ({
  ok: false,
  error: { code, message },
});

export class Requests {
  private readonly ctx: Context;

  constructor(ctx: Context) {
    this.ctx = ctx;
  }

  /**
   * Сообщение request: обработать (не задерживая остальные сообщения сессии) и ответить
   * request.result в эту же сессию.
   */
  handle(ss: Session, env: Envelope): void {
    const id = messageIdSchema.safeParse(env.id).data;

    if (id === undefined) {
      this.ctx.log("запрос воркера без id — пропущен", { agentId: ss.agentId });

      return;
    }
    void this.answer(ss, env)
      .catch((e: unknown): Outcome =>
        refuse("REQUEST_FAILED", String((e as Error)?.message ?? e)),
      )
      .then(outcome => {
        if (!ss.closed)
          ss.send({ type: REQUEST_RESULT, re: id, data: outcome });
      });
  }

  private async answer(ss: Session, env: Envelope): Promise<Outcome> {
    const p = parseWorkerRequest(env.data);

    if (!p.ok) return refuse("MESSAGE_INVALID", p.error);
    const { worker, type, data } = p.value;
    const handler = this.ctx.settings.onWorkerRequest;

    if (!handler)
      return refuse(
        "REQUEST_UNHANDLED",
        "на сервере нет обработчика запросов воркеров",
      );
    const agent = publicAgent(await this.ctx.agent(ss.agentId));
    const problems = this.problems(agent, worker, type, data);

    if (problems.length)
      return refuse("REQUEST_INVALID", `${type}: ${problems.join("; ")}`);
    const timeoutMs = bounded(
      p.value.timeoutMs,
      REQUEST_TIMEOUT_MS,
      1,
      MAX_REQUEST_TIMEOUT_MS,
    );
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), timeoutMs);
    const stop = ss.onWake(() => {
      if (ss.closed) ctl.abort();
    });

    try {
      const result = await handler({
        id: env.id!,
        agentId: ss.agentId,
        worker,
        type,
        ...(data !== undefined ? { data } : {}),
        agent,
        timeoutMs,
        signal: ctl.signal,
      });

      return this.result(result);
    } catch (e) {
      if (e instanceof AgentsError) return refuse(e.code, e.message);
      this.ctx.log("обработчик запроса воркера упал", {
        agentId: ss.agentId,
        worker,
        type,
        err: String(e),
      });

      return refuse("REQUEST_FAILED", String((e as Error)?.message ?? e));
    } finally {
      clearTimeout(timer);
      stop();
    }
  }

  /** data не по requests[].schema (validateRequests); схема не применяется — без проверки. */
  private problems(
    agent: Agent,
    worker: string,
    type: string,
    data: unknown,
  ): string[] {
    if (!this.ctx.settings.validateRequests) return [];
    const schema = workerManifest(agent, worker)?.requests?.find(
      r => r.type === type,
    )?.schema;

    if (!schema) return [];
    try {
      return jsonSchemaProblems(schema, data ?? null);
    } catch (e) {
      this.ctx.log("схема запроса из манифеста воркера не применяется", {
        agentId: agent.id,
        worker,
        type,
        err: String(e),
      });

      return [];
    }
  }

  /** Ответ обработчика: undefined — без data; больше предела — BODY_TOO_LARGE. */
  private result(result: unknown): Outcome {
    if (result === undefined) return { ok: true };
    const size = Buffer.byteLength(JSON.stringify(result) ?? "");

    if (size > MAX_REQUEST_RESULT)
      return refuse("BODY_TOO_LARGE", "ответ на запрос воркера больше 4 МБ");

    return { ok: true, data: result };
  }
}
