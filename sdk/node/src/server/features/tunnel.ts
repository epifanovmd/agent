// Запросы к воркерам через агента (§7): fetch → fetch.head, fetch.chunk, fetch.end; отмена и
// срок — fetch.cancel.
import { z } from "zod";

import type { Context } from "../core/context";
import { AgentsError, codeError, valid } from "../core/errors";
import { agentsDefaults } from "../core/options";
import type { PendingFetch, Session } from "../core/session";
import { bounded } from "../lib/util";
import { parse, parseFetchReply } from "../protocol/checks";
import { type Envelope, MAX_FETCH_BODY, newId } from "../protocol/messages";
import { messageIdSchema, nameSchema } from "../protocol/schemas";

/** Параметры fetch. */
export interface FetchInit {
  /** По умолчанию GET. */
  method?: string;
  headers?: Record<string, string> | Headers;
  body?: string | Uint8Array | ArrayBuffer;
  /** Срок, мс (по умолчанию 30 000, не больше 600 000). */
  timeoutMs?: number;
  signal?: AbortSignal;
}

/** Куда запрос: воркер, путь от корня без переводов строк, метод (по умолчанию GET). */
const targetSchema = z.object({
  worker: nameSchema,
  path: z
    .string()
    .startsWith("/", "путь от корня (/…)")
    .refine(v => !/[\r\n]/.test(v), "перевод строки в пути"),
  method: z
    .string()
    .default("GET")
    .transform(v => v.toUpperCase())
    .pipe(z.string().regex(/^[A-Z]+$/)),
});

/** Служебные пути воркера: их вызывает только агент. */
const FORBIDDEN = /^\/(config(\/.*)?|metrics|health|cleanup)$/;
/** Статусы без тела. */
const NULL_BODY = new Set([101, 103, 204, 205, 304]);

export class Tunnel {
  private readonly ctx: Context;

  constructor(ctx: Context) {
    this.ctx = ctx;
  }

  async fetch(
    actor: string,
    agentId: string,
    worker: string,
    path: string,
    init: FetchInit,
  ): Promise<Response> {
    const { data, timeoutMs } = request(worker, path, init);

    if (init.signal?.aborted) throw codeError("CANCELLED", "запрос отменён");
    const ss = await this.ctx.localSession(agentId);

    if (actor)
      this.ctx.audit(actor, "fetch", agentId, {
        worker,
        method: data.method,
        path,
      });

    return open(ss, data, timeoutMs, init.signal);
  }
}

/** Ответ агента на fetch: передать ожидающему запросу сессии. */
export const fetchReply = (ss: Session, env: Envelope): void => {
  const re = messageIdSchema.safeParse(env.re).data;
  const f = re === undefined ? undefined : ss.fetches.get(re);

  if (!f) return;
  const r = parseFetchReply(env);

  if (r?.kind === "head") f.head(r.status, r.headers);
  else if (r?.kind === "chunk") f.chunk(r.data);
  else if (r?.kind === "end") f.end(r.error);
};

/** Проверить параметры и собрать data сообщения fetch. */
const request = (worker: string, path: string, init: FetchInit) => {
  const { method } = valid(
    parse(targetSchema, { worker, path, method: init.method }, "fetch"),
  );

  if (FORBIDDEN.test(path.split("?")[0]))
    throw codeError("PATH_FORBIDDEN", `служебный путь ${path}`);
  const data: Record<string, unknown> = { worker, method, path };
  const headers =
    init.headers instanceof Headers
      ? Object.fromEntries(init.headers)
      : (init.headers ?? {});

  if (Object.keys(headers).length) data.headers = headers;
  if (init.body !== undefined) Object.assign(data, encodeBody(init.body));
  const timeoutMs = bounded(init.timeoutMs, 30_000, 1, 600_000);

  if (init.timeoutMs !== undefined) data.timeoutMs = timeoutMs;

  return { data, timeoutMs };
};

/** Тело запроса: строка — как есть, байты — base64; больше MAX_FETCH_BODY — BODY_TOO_LARGE. */
const encodeBody = (
  body: string | Uint8Array | ArrayBuffer,
): { body: string; encoding?: "base64" } => {
  const tooLarge = () =>
    codeError("BODY_TOO_LARGE", "тело запроса больше 4 МБ");

  if (typeof body === "string") {
    if (Buffer.byteLength(body) > MAX_FETCH_BODY) throw tooLarge();

    return { body };
  }
  const buf = Buffer.from(
    body instanceof ArrayBuffer ? new Uint8Array(body) : body,
  );

  if (buf.length > MAX_FETCH_BODY) throw tooLarge();

  return { body: buf.toString("base64"), encoding: "base64" };
};

/**
 * Отправить fetch и вернуть Response по fetch.head; тело — поток из fetch.chunk. Ошибка до
 * заголовка — отказ промиса, после — ошибка потока тела.
 */
const open = (
  ss: Session,
  data: Record<string, unknown>,
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<Response> => {
  const id = newId();

  return new Promise<Response>((resolve, reject) => {
    let controller!: ReadableStreamDefaultController<Uint8Array>;
    let headed = false;
    let done = false;
    const cancel = () => ss.send({ type: "fetch.cancel", re: id });
    const stream = new ReadableStream<Uint8Array>({
      start: c => void (controller = c),
      cancel: () => {
        if (done) return;
        finish();
        cancel();
      },
    });
    const abortWith = (err: AgentsError) => {
      if (done) return;
      cancel();
      fail(err);
    };
    const onAbort = () => abortWith(codeError("CANCELLED", "запрос отменён"));
    const timer = setTimeout(
      () => abortWith(codeError("TIMEOUT", `нет ответа за ${timeoutMs} мс`)),
      timeoutMs + agentsDefaults.fetchSlackMs,
    );
    const finish = () => {
      done = true;
      ss.fetches.delete(id);
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    };
    const fail = (err: AgentsError) => {
      if (done) return;
      finish();
      if (!headed) reject(err);
      else controller.error(err);
    };
    const pending: PendingFetch = {
      head: (status, h) => {
        if (headed || done) return;
        if (!Number.isInteger(status) || status < 200 || status > 599)
          return fail(
            codeError(
              "WORKER_UNAVAILABLE",
              `воркер ответил статусом ${status}`,
            ),
          );
        headed = true;
        resolve(
          new Response(NULL_BODY.has(status) ? null : stream, {
            status,
            headers: toHeaders(h),
          }),
        );
      },
      chunk: b => {
        if (headed && !done) controller.enqueue(b);
      },
      end: error => {
        if (done) return;
        if (error) return fail(codeError(error.code, error.message));
        if (!headed)
          return fail(
            codeError("WORKER_UNAVAILABLE", "агент завершил запрос без ответа"),
          );
        finish();
        controller.close();
      },
    };

    signal?.addEventListener("abort", onAbort, { once: true });
    ss.fetches.set(id, pending);
    ss.send({ type: "fetch", id, data });
    if (ss.closed)
      pending.end({
        code: "DISCONNECTED",
        message: "связь с агентом оборвалась",
      });
  });
};

/** Заголовки ответа воркера; недопустимые пропускаются. */
const toHeaders = (h: Record<string, string> | undefined): Headers => {
  const hs = new Headers();

  for (const [k, v] of Object.entries(h ?? {})) {
    try {
      hs.append(k, String(v));
    } catch {
      // Недопустимый заголовок пропускается.
    }
  }

  return hs;
};
