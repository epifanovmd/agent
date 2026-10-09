// Пересылка вызовов между процессами бэкенда: вызов, которому нужна сессия агента (fetch,
// действия, logs, watch, runJob), из процесса без неё уходит в процесс с сессией (опция relay),
// там его выполняет handleRelay. Обычные вызовы — JSON { result } или { error }; ответ fetch —
// поток кадров: заголовок ответа воркера, куски тела, конец (с ошибкой, если она была).
import { createHash, timingSafeEqual } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";

import { z } from "zod";

import type { Context } from "../core/context";
import { AgentsError, codeError } from "../core/errors";
import {
  agentsDefaults,
  type RelayCall,
  type RelayMethod,
} from "../core/options";
import { bounded } from "../lib/util";
import { parse } from "../protocol/checks";
import { MAX_MESSAGE_BYTES } from "../protocol/messages";
import { messageIdSchema } from "../protocol/schemas";
import { readJSON, sendJSON } from "../transport/http";
import type { FetchInit } from "./tunnel";

/** Заголовок с общим секретом пересылки (relaySecret). */
export const RELAY_SECRET_HEADER = "x-agents-relay-secret";

/** Тип тела ответа fetch: поток кадров. */
const FRAMES_TYPE = "application/x-agents-relay-frames";

/** Кадр ответа fetch: 1 байт вида, 4 байта длины, данные. */
const Frame = { Head: 1, Data: 2, End: 3 } as const;

/** Вызов в теле запроса пересылки. */
const relayCallSchema = z.object({
  method: z.enum([
    "fetch",
    "restartWorker",
    "updateWorker",
    "updateAgent",
    "rotateKey",
    "logs",
    "watch",
    "unwatch",
    "runJob",
  ]),
  agentId: messageIdSchema,
  actor: z.string().optional(),
  args: z.array(z.unknown()),
});

/** Параметры fetch в пересылке: тело — строка или base64. */
interface RelayFetchInit {
  method?: string;
  headers?: Record<string, string>;
  body?: string;
  bodyBase64?: string;
  timeoutMs?: number;
}

/** fetch к воркеру в этом процессе. */
export type Fetcher = (
  actor: string,
  agentId: string,
  worker: string,
  path: string,
  init: FetchInit,
) => Promise<Response>;

/** Вызов в этом процессе: от имени actor, аргументы — как в RelayCall. */
export type LocalCall = (
  actor: string,
  agentId: string,
  args: unknown[],
  signal?: AbortSignal,
) => Promise<unknown>;

/** Что выполняет процесс с сессией (без пересылки дальше), кроме fetch. */
export type LocalCalls = Record<Exclude<RelayMethod, "fetch">, LocalCall>;

export class Relay {
  private readonly ctx: Context;
  private readonly localFetch: Fetcher;
  private readonly local: LocalCalls;

  constructor(ctx: Context, localFetch: Fetcher, local: LocalCalls) {
    this.ctx = ctx;
    this.localFetch = localFetch;
    this.local = local;
  }

  /** Вызов здесь или в процессе с сессией агента (опция relay). */
  async run<T>(
    method: Exclude<RelayMethod, "fetch">,
    actor: string,
    agentId: string,
    args: unknown[],
    signal?: AbortSignal,
  ): Promise<T> {
    const to = await this.target(agentId);

    if (to === undefined)
      return (await this.local[method](actor, agentId, args, signal)) as T;

    return this.remoteCall<T>(to, callOf(method, actor, agentId, args), signal);
  }

  /** fetch здесь или в процессе с сессией агента (опция relay). */
  fetch: Fetcher = async (actor, agentId, worker, path, init) => {
    const to = await this.target(agentId);

    if (to === undefined)
      return this.localFetch(actor, agentId, worker, path, init);

    return this.remoteFetch(
      to,
      callOf("fetch", actor, agentId, [worker, path, encodeInit(init)]),
      bounded(init.timeoutMs, 30_000, 1, 600_000),
      init.signal,
    );
  };

  /**
   * Куда направить вызов: id процесса с сессией агента или undefined — выполнить здесь (сессия
   * здесь, пересылки нет, агент без связи).
   */
  private async target(agentId: string): Promise<string | undefined> {
    if (!this.ctx.settings.relay || this.ctx.live(agentId)) return undefined;
    const a = await this.ctx.store.getAgent(agentId);
    const instance = a?.online && !a.revoked ? a.session?.instance : undefined;

    return instance && instance !== this.ctx.instanceId ? instance : undefined;
  }

  /** Вызов в процессе instance; итог — result его handleRelay, ошибка — AgentsError. */
  private async remoteCall<T>(
    instance: string,
    call: RelayCall,
    signal?: AbortSignal,
  ): Promise<T> {
    const ctl = new AbortController();
    const onAbort = () =>
      ctl.abort(codeError("CANCELLED", "вызов отменён", 499));

    if (signal?.aborted) onAbort();
    signal?.addEventListener("abort", onAbort, { once: true });
    try {
      const res = await this.send(instance, call, ctl.signal);
      const body = (await res.json().catch(() => undefined)) as
        { result?: unknown; error?: unknown } | undefined;

      if (body?.error !== undefined) throw remoteError(body.error, res.status);
      if (!res.ok || !body)
        throw codeError(
          "RELAY_FAILED",
          `процесс ${instance} ответил ${res.status}`,
          502,
        );

      return body.result as T;
    } catch (e) {
      throw ctl.signal.aborted ? (ctl.signal.reason as AgentsError) : e;
    } finally {
      signal?.removeEventListener("abort", onAbort);
    }
  }

  /**
   * fetch в процессе instance: Response по заголовку ответа воркера, тело — потоком; ошибка до
   * заголовка — отказ промиса, после — ошибка потока тела (как у fetch в этом процессе).
   */
  private async remoteFetch(
    instance: string,
    call: RelayCall,
    timeoutMs: number,
    signal?: AbortSignal,
  ): Promise<Response> {
    const ctl = new AbortController();
    const onAbort = () =>
      ctl.abort(codeError("CANCELLED", "запрос отменён", 499));
    const timer = setTimeout(
      () =>
        ctl.abort(codeError("TIMEOUT", `нет ответа за ${timeoutMs} мс`, 504)),
      timeoutMs + 2 * agentsDefaults.fetchSlackMs,
    );
    const cleanup = () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    };
    const reason = (e: unknown) =>
      ctl.signal.aborted
        ? (ctl.signal.reason as AgentsError)
        : e instanceof AgentsError
          ? e
          : codeError(
              "DISCONNECTED",
              `связь с процессом ${instance} оборвалась`,
              502,
            );

    if (signal?.aborted) onAbort();
    signal?.addEventListener("abort", onAbort, { once: true });
    try {
      const res = await this.send(instance, call, ctl.signal);

      if (
        !res.headers.get("content-type")?.startsWith(FRAMES_TYPE) ||
        !res.body
      )
        throw await errorOf(res, instance);
      const frames = new FrameReader(res.body.getReader());
      const first = await frames.next();

      if (first?.kind !== Frame.Head) {
        throw first?.kind === Frame.End
          ? endError(first.data)
          : codeError("RELAY_FAILED", "нет заголовка ответа", 502);
      }
      const head = JSON.parse(first.data.toString("utf8")) as {
        status: number;
        headers: Record<string, string>;
      };
      const body = new ReadableStream<Uint8Array>({
        pull: async c => {
          try {
            const f = await frames.next();

            if (f?.kind === Frame.Data) return c.enqueue(f.data);
            cleanup();
            const err = f?.kind === Frame.End ? endError(f.data) : undefined;

            if (!f) c.error(reason(undefined));
            else if (err) c.error(err);
            else c.close();
          } catch (e) {
            cleanup();
            c.error(reason(e));
          }
        },
        cancel: () => {
          cleanup();
          ctl.abort(codeError("CANCELLED", "запрос отменён", 499));
        },
      });

      return new Response(NULL_BODY.has(head.status) ? null : body, {
        status: head.status,
        headers: head.headers,
      });
    } catch (e) {
      cleanup();
      throw reason(e);
    }
  }

  /** Принять пересланный вызов: секрет, вызов, ответ (fetch — потоком кадров). */
  async handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const secret = this.ctx.settings.relaySecret;

    if (secret && !sameSecret(req.headers[RELAY_SECRET_HEADER], secret))
      return sendError(
        res,
        codeError("UNAUTHORIZED", "неверный секрет пересылки", 401),
      );
    let call: RelayCall;

    try {
      const p = parse(
        relayCallSchema,
        await readJSON(req, MAX_MESSAGE_BYTES),
        "вызов",
      );

      if (!p.ok) throw codeError("MESSAGE_INVALID", p.error, 400);
      call = p.value;
    } catch (e) {
      return sendError(res, asAgentsError(e));
    }
    const ctl = new AbortController();

    res.on("close", () => {
      if (!res.writableFinished) ctl.abort();
    });
    if (call.method === "fetch") return this.serveFetch(call, ctl.signal, res);
    try {
      const result = await this.local[call.method as keyof LocalCalls](
        call.actor ?? "",
        call.agentId,
        call.args,
        ctl.signal,
      );

      sendJSON(res, 200, { result: result ?? null });
    } catch (e) {
      sendError(res, asAgentsError(e));
    }
  }

  /** Тело HTTP-запроса пересылки и его отправка функцией relay. */
  private async send(
    instance: string,
    call: RelayCall,
    signal: AbortSignal,
  ): Promise<Response> {
    const headers: Record<string, string> = {
      "content-type": "application/json",
    };
    const { relay, relaySecret } = this.ctx.settings;

    if (relaySecret) headers[RELAY_SECRET_HEADER] = relaySecret;
    try {
      return await relay!(instance, {
        call,
        body: JSON.stringify(call),
        headers,
        signal,
      });
    } catch (e) {
      if (signal.aborted) throw signal.reason;
      throw codeError(
        "RELAY_FAILED",
        `вызов не доставлен процессу ${instance}: ${String(e)}`,
        502,
      );
    }
  }

  /** fetch для другого процесса: ответ воркера кадрами (заголовок, тело, конец). */
  private async serveFetch(
    call: RelayCall,
    signal: AbortSignal,
    res: ServerResponse,
  ): Promise<void> {
    const [worker, path, init] = call.args as [string, string, RelayFetchInit];

    res.writeHead(200, { "content-type": FRAMES_TYPE });
    try {
      const r = await this.localFetch(
        call.actor ?? "",
        call.agentId,
        String(worker),
        String(path),
        { ...decodeInit(init ?? {}), signal },
      );

      await write(
        res,
        frame(Frame.Head, {
          status: r.status,
          headers: Object.fromEntries(r.headers),
        }),
      );
      if (r.body) {
        const reader = r.body.getReader();

        for (;;) {
          const { done, value } = await reader.read();

          if (done) break;
          await write(res, frame(Frame.Data, value));
        }
      }
      await write(res, frame(Frame.End, {}));
    } catch (e) {
      const err = asAgentsError(e);

      await write(
        res,
        frame(Frame.End, {
          error: { code: err.code, message: err.message, status: err.status },
        }),
      );
    }
    res.end();
  }
}

/** Пересылаемый вызов. */
const callOf = (
  method: RelayMethod,
  actor: string,
  agentId: string,
  args: unknown[],
): RelayCall =>
  actor ? { method, agentId, actor, args } : { method, agentId, args };

/** Параметры fetch для пересылки: тело — строкой или base64, signal не пересылается. */
const encodeInit = (init: FetchInit): RelayFetchInit => {
  const out: RelayFetchInit = {};

  if (init.method !== undefined) out.method = init.method;
  if (init.headers !== undefined)
    out.headers =
      init.headers instanceof Headers
        ? Object.fromEntries(init.headers)
        : init.headers;
  if (typeof init.body === "string") out.body = init.body;
  else if (init.body !== undefined)
    out.bodyBase64 = Buffer.from(
      init.body instanceof ArrayBuffer ? new Uint8Array(init.body) : init.body,
    ).toString("base64");
  if (init.timeoutMs !== undefined) out.timeoutMs = init.timeoutMs;

  return out;
};

const decodeInit = (init: RelayFetchInit): FetchInit => {
  const out: FetchInit = {};

  if (init.method !== undefined) out.method = init.method;
  if (init.headers !== undefined) out.headers = init.headers;
  if (init.body !== undefined) out.body = init.body;
  else if (init.bodyBase64 !== undefined)
    out.body = new Uint8Array(Buffer.from(init.bodyBase64, "base64"));
  if (init.timeoutMs !== undefined) out.timeoutMs = init.timeoutMs;

  return out;
};

/** Статусы без тела. */
const NULL_BODY = new Set([101, 103, 204, 205, 304]);

/** Секрет из заголовка совпадает (сравнение постоянного времени по хешам). */
const sameSecret = (
  got: string | string[] | undefined,
  want: string,
): boolean => {
  const digest = (v: string) => createHash("sha256").update(v).digest();

  return typeof got === "string" && timingSafeEqual(digest(got), digest(want));
};

const asAgentsError = (e: unknown): AgentsError =>
  e instanceof AgentsError
    ? e
    : new AgentsError("INTERNAL", String((e as Error)?.message ?? e), 500);

const sendError = (res: ServerResponse, e: AgentsError): void => {
  if (res.headersSent) return void res.destroy();
  sendJSON(res, e.status, {
    error: { code: e.code, message: e.message, status: e.status },
  });
};

/** Ошибка из ответа другого процесса. */
const remoteError = (raw: unknown, status: number): AgentsError => {
  const e = raw as { code?: unknown; message?: unknown; status?: unknown };

  return new AgentsError(
    typeof e?.code === "string" ? e.code : "RELAY_FAILED",
    typeof e?.message === "string" ? e.message : "",
    typeof e?.status === "number" ? e.status : status,
  );
};

/** Ответ без кадров: ошибка JSON ({ error }) или RELAY_FAILED. */
const errorOf = async (
  res: Response,
  instance: string,
): Promise<AgentsError> => {
  const body = (await res.json().catch(() => undefined)) as
    { error?: unknown } | undefined;

  return body?.error !== undefined
    ? remoteError(body.error, res.status)
    : codeError(
        "RELAY_FAILED",
        `процесс ${instance} ответил ${res.status}`,
        502,
      );
};

/** Ошибка из кадра конца; её нет — undefined. */
const endError = (data: Buffer): AgentsError | undefined => {
  const end = JSON.parse(data.toString("utf8")) as { error?: unknown };

  return end.error === undefined ? undefined : remoteError(end.error, 502);
};

/** Кадр: вид, длина, данные (объект — JSON). */
const frame = (kind: number, data: Uint8Array | object): Buffer => {
  const payload =
    data instanceof Uint8Array
      ? Buffer.from(data.buffer, data.byteOffset, data.byteLength)
      : Buffer.from(JSON.stringify(data));
  const head = Buffer.alloc(5);

  head.writeUInt8(kind, 0);
  head.writeUInt32BE(payload.length, 1);

  return Buffer.concat([head, payload]);
};

/** Записать с учётом заполненного буфера; соединение закрыто — сразу. */
const write = (res: ServerResponse, chunk: Buffer): Promise<void> =>
  new Promise(resolve => {
    if (res.destroyed || res.writableEnded) return resolve();
    if (res.write(chunk)) return resolve();
    const done = () => {
      res.off("drain", done);
      res.off("close", done);
      resolve();
    };

    res.on("drain", done);
    res.on("close", done);
  });

/** Чтение кадров из потока ответа. */
class FrameReader {
  private buf = Buffer.alloc(0);
  private readonly reader: ReadableStreamDefaultReader<Uint8Array>;

  constructor(reader: ReadableStreamDefaultReader<Uint8Array>) {
    this.reader = reader;
  }

  /** Следующий кадр; поток кончился — undefined. */
  async next(): Promise<{ kind: number; data: Buffer } | undefined> {
    for (;;) {
      if (this.buf.length >= 5) {
        const len = this.buf.readUInt32BE(1);

        if (this.buf.length >= 5 + len) {
          const f = {
            kind: this.buf.readUInt8(0),
            data: this.buf.subarray(5, 5 + len),
          };

          this.buf = this.buf.subarray(5 + len);

          return f;
        }
      }
      const { done, value } = await this.reader.read();

      if (done) return undefined;
      this.buf = Buffer.concat([this.buf, value]);
    }
  }
}
