// Задачи воркера (§12): POST /jobs через fetch, проверка типа и data по манифесту, ожидание
// итога долгой задачи по событиям job.* в этом процессе, состояние и отмена задачи.
import { z } from "zod";

import type { Context } from "../core/context";
import { AgentsError, codeError, valid } from "../core/errors";
import { jsonSchemaProblems } from "../lib/json-schema";
import { bounded, DAY_MS } from "../lib/util";
import { workerManifest } from "../model/manifest";
import { publicAgent } from "../model/public-agent";
import type { AgentEvent } from "../model/types";
import {
  parse,
  parseJobEvent,
  parseJobReply,
  parseJobStatus,
} from "../protocol/checks";
import {
  type ErrorInfo,
  JOB_EVENTS,
  JOBS_PATH,
  type JobState,
  type JobStatus,
  newId,
} from "../protocol/messages";
import {
  eventTypeSchema,
  messageIdSchema,
  nameSchema,
} from "../protocol/schemas";
import type { FetchInit } from "./tunnel";

/** Файлы задачи по ссылкам: воркер сам скачивает входные и загружает выходные. */
const jobFilesSchema = z.object({
  inputs: z.record(z.string(), z.string().min(1)).optional(),
  outputs: z.record(z.string(), z.string().min(1)).optional(),
});

/** Параметры runJob. */
const runOptionsSchema = z.object({
  /** Тип задачи из manifest.jobs. */
  type: eventTypeSchema,
  /** id задачи от сервера (повтор с тем же jobId не начинает вторую задачу); нет — новый. */
  jobId: messageIdSchema.optional(),
  data: z.unknown().optional(),
  files: jobFilesSchema.optional(),
  /**
   * Сколько ждать итога долгой задачи, мс (по умолчанию 30 000, не больше суток); ответ на
   * POST /jobs ждётся не меньше 30 с.
   */
  timeoutMs: z.number().optional().catch(undefined),
});

export type JobFiles = z.input<typeof jobFilesSchema>;

export type JobOptions = z.input<typeof runOptionsSchema> & {
  /** Отмена: ожидание прерывается, долгой задаче уходит POST /jobs/{id}/cancel. */
  signal?: AbortSignal;
};

/**
 * Итог runJob. state: done — итог в result; failed — ошибка воркера в error; cancelled —
 * задачу прервали; running — итога за timeoutMs не дождались (задача идёт, её id — в id,
 * итог придёт событием job.done или job.failed).
 */
export interface JobResult {
  jobId: string;
  /** id долгой задачи у воркера (ответ 202); у быстрой — нет. */
  id?: string;
  state: JobState;
  /** Последний ход долгой задачи: от 0 до 1. */
  progress?: number;
  result?: unknown;
  error?: ErrorInfo;
}

/** Запрос к воркеру (fetch этого процесса или с пересылкой). */
type Fetcher = (
  actor: string,
  agentId: string,
  worker: string,
  path: string,
  init: FetchInit,
) => Promise<Response>;

/** Ожидающий итога долгой задачи в этом процессе. */
interface Waiter {
  progress?: number;
  final?: Omit<JobResult, "jobId">;
  wake: () => void;
}

/** Срок ответа на POST /jobs: не меньше 30 с (быстрая задача) и не больше 10 мин (как у fetch). */
const MIN_POST_MS = 30_000;
const MAX_POST_MS = 600_000;

export class Jobs {
  private readonly ctx: Context;
  /** Ожидающие итога: "агент\njobId" → ожидающие. */
  private readonly waiters = new Map<string, Set<Waiter>>();

  constructor(ctx: Context) {
    this.ctx = ctx;
  }

  /**
   * Задача воркеру в этом процессе (fetch — этого процесса): 200 — итог сразу, 202 — ждать
   * событий job.* не дольше timeoutMs. Тип нет в manifest.jobs — JOB_UNKNOWN, data не по
   * схеме (validateJobs) — JOB_INVALID, отказ воркера (4xx) — JOB_REJECTED.
   */
  async run(
    actor: string,
    agentId: string,
    worker: string,
    opts: JobOptions,
    fetch: Fetcher,
  ): Promise<JobResult> {
    valid(parse(nameSchema, worker, "worker"));
    const {
      type,
      jobId = newId(),
      data,
      files,
      timeoutMs,
    } = valid(parse(runOptionsSchema, opts, "runJob"));

    await this.check(agentId, worker, type, data);
    const { signal } = opts;
    const deadline = Date.now() + bounded(timeoutMs, 30_000, 1, DAY_MS);
    const key = `${agentId}\n${jobId}`;
    const w: Waiter = { wake: () => {} };
    let mine = this.waiters.get(key);

    if (!mine) this.waiters.set(key, (mine = new Set()));
    mine.add(w);
    try {
      const body: Record<string, unknown> = { type, jobId };

      if (data !== undefined) body.data = data;
      if (files) body.files = files;
      const res = await fetch(actor, agentId, worker, JOBS_PATH, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
        timeoutMs: Math.min(
          Math.max(deadline - Date.now(), MIN_POST_MS),
          MAX_POST_MS,
        ),
        signal,
      });
      const reply = await replyOf(res);

      if (res.status !== 202)
        return { jobId, state: "done", ...pick(reply, "result") };
      if (!reply.id)
        throw codeError("JOB_REJECTED", "воркер ответил 202 без id задачи");

      return await this.wait(w, jobId, reply.id, deadline, signal, () =>
        fetch(actor, agentId, worker, `${JOBS_PATH}/${reply.id}/cancel`, {
          method: "POST",
        }).then(r => r.body?.cancel()),
      );
    } finally {
      mine.delete(w);
      if (!mine.size) this.waiters.delete(key);
    }
  }

  /** GET /jobs/{id}: состояние задачи; нет задачи — JOB_NOT_FOUND. */
  async status(
    actor: string,
    agentId: string,
    worker: string,
    id: string,
    fetch: Fetcher,
  ): Promise<JobStatus> {
    return statusOf(
      await fetch(actor, agentId, worker, jobPath(id), { method: "GET" }),
    );
  }

  /** POST /jobs/{id}/cancel: прервать долгую задачу; нет задачи — JOB_NOT_FOUND. */
  async cancel(
    actor: string,
    agentId: string,
    worker: string,
    id: string,
    fetch: Fetcher,
  ): Promise<JobStatus> {
    return statusOf(
      await fetch(actor, agentId, worker, `${jobPath(id)}/cancel`, {
        method: "POST",
      }),
    );
  }

  /**
   * Событие воркера после обработчика бэкенда (onEvent): события задач — ожидающим runJob этого
   * процесса.
   */
  event(e: AgentEvent): void {
    if (!(JOB_EVENTS as readonly string[]).includes(e.type)) return;
    const d = parseJobEvent(e.data);
    const mine = d && this.waiters.get(`${e.agentId}\n${d.jobId}`);

    if (!d || !mine) return;
    for (const w of mine) {
      if (w.final) continue;
      if (e.type === "job.progress") w.progress = d.progress ?? w.progress;
      else if (e.type === "job.done")
        w.final = { id: d.id, state: "done", ...pick(d, "result") };
      else if (e.type === "job.failed")
        w.final = {
          id: d.id,
          state: "failed",
          error: d.error ?? { code: "JOB_FAILED", message: "" },
        };
      else w.final = { id: d.id, state: "cancelled" };
      w.wake();
    }
  }

  /** Тип задачи в manifest.jobs воркера и data по схеме типа (validateJobs). */
  private async check(
    agentId: string,
    worker: string,
    type: string,
    data: unknown,
  ): Promise<void> {
    const a = await this.ctx.agent(agentId);
    const job = workerManifest(publicAgent(a), worker)?.jobs?.find(
      j => j.type === type,
    );

    if (!job)
      throw codeError(
        "JOB_UNKNOWN",
        `задачи ${type} нет в манифесте воркера ${worker} (jobs)`,
        409,
      );
    if (!this.ctx.settings.validateJobs || !job.schema) return;
    let problems: string[];

    try {
      problems = jsonSchemaProblems(job.schema, data ?? null);
    } catch (e) {
      this.ctx.log("схема задачи из манифеста воркера не применяется", {
        agentId,
        worker,
        type,
        err: String(e),
      });

      return;
    }
    if (problems.length)
      throw codeError(
        "JOB_INVALID",
        `${worker}/${type}: ${problems.join("; ")}`,
        400,
      );
  }

  /** Ждать итога долгой задачи до deadline; отмена — POST /jobs/{id}/cancel и CANCELLED. */
  private wait(
    w: Waiter,
    jobId: string,
    id: string,
    deadline: number,
    signal: AbortSignal | undefined,
    cancel: () => Promise<unknown>,
  ): Promise<JobResult> {
    return new Promise<JobResult>((resolve, reject) => {
      const done = () => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
      };
      const onAbort = () => {
        done();
        void cancel().catch(e =>
          this.ctx.log("отмена задачи не удалась", { jobId, err: String(e) }),
        );
        reject(codeError("CANCELLED", "ожидание задачи отменено"));
      };
      const timer = setTimeout(
        () => {
          done();
          resolve({ jobId, id, state: "running", ...pick(w, "progress") });
        },
        Math.max(deadline - Date.now(), 0),
      );

      w.wake = () => {
        if (!w.final) return;
        done();
        resolve({ jobId, ...w.final, id: w.final.id ?? id });
      };
      if (signal?.aborted) return onAbort();
      signal?.addEventListener("abort", onAbort, { once: true });
      w.wake();
    });
  }
}

/** Путь задачи id: сегмент пути, без / и переводов строк. */
const jobPath = (id: string): string => {
  if (!id || /[/\r\n?#]/.test(id))
    throw codeError("MESSAGE_INVALID", "id задачи — один сегмент пути", 400);

  return `${JOBS_PATH}/${encodeURIComponent(id)}`;
};

/** Тело ответа воркера как JSON; не JSON — undefined (текст — в message). */
const bodyOf = async (
  res: Response,
): Promise<{ json?: unknown; text: string }> => {
  const text = await res.text();

  try {
    return { json: text ? JSON.parse(text) : undefined, text };
  } catch {
    return { text };
  }
};

/** Отказ воркера: 404 — JOB_NOT_FOUND, другой 4xx — JOB_REJECTED, остальное — 502. */
const rejection = async (res: Response, notFound = false) => {
  const { json, text } = await bodyOf(res);
  const msg =
    (json as { message?: unknown } | undefined)?.message ?? (text || "");
  const message = `воркер ответил ${res.status}: ${String(msg)}`;

  if (notFound && res.status === 404)
    return codeError("JOB_NOT_FOUND", message, 404);

  return codeError(
    "JOB_REJECTED",
    message,
    res.status >= 400 && res.status < 500 ? res.status : 502,
  );
};

/** Ответ на POST /jobs. */
const replyOf = async (res: Response) => {
  if (res.status < 200 || res.status > 299) throw await rejection(res);
  const p = parseJobReply((await bodyOf(res)).json ?? {});

  if (!p.ok) throw new AgentsError("JOB_REJECTED", p.error, 502);

  return p.value;
};

/** Ответ на GET /jobs/{id} и POST /jobs/{id}/cancel. */
const statusOf = async (res: Response): Promise<JobStatus> => {
  if (res.status < 200 || res.status > 299) throw await rejection(res, true);
  const p = parseJobStatus((await bodyOf(res)).json);

  if (!p.ok) throw new AgentsError("JOB_REJECTED", p.error, 502);

  return p.value;
};

/** Поле объекта, если оно задано. */
const pick = <T extends object, K extends keyof T>(
  o: T,
  k: K,
): Partial<Pick<T, K>> =>
  o[k] === undefined ? {} : ({ [k]: o[k] } as Partial<Pick<T, K>>);
