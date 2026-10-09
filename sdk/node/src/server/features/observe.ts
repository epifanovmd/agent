// Наблюдение: наблюдатели watch в памяти процесса (сводятся в один watch агенту) и события
// воркеров (проверка data по схеме, обработчик onEvent, подтверждение после него, отсечение
// повторов, подписки subscribeEvents и waitEvent этого процесса).
import { LRUCache } from "lru-cache";
import { z } from "zod";

import type { Context } from "../core/context";
import { codeError, valid } from "../core/errors";
import type { Session } from "../core/session";
import { jsonSchemaProblems } from "../lib/json-schema";
import { bounded, DAY_MS } from "../lib/util";
import { declaresEvent, workerManifest } from "../model/manifest";
import { publicAgent } from "../model/public-agent";
import type { AgentEvent, Watcher } from "../model/types";
import { summarize } from "../model/watchers";
import { parse, parseEvent } from "../protocol/checks";
import {
  type Envelope,
  JOB_EVENT_PREFIX,
  newId,
  type WorkerManifest,
} from "../protocol/messages";
import {
  eventTypeSchema,
  logLevelSchema,
  messageIdSchema,
  nameSchema,
} from "../protocol/schemas";

/** Параметры наблюдателя watch (§9). */
const watchOptionsSchema = z.object({
  /** Свой id наблюдателя (повтор с тем же id продлевает); по умолчанию — новый. */
  id: messageIdSchema.optional(),
  /** Частота метрик, мс: не чаще раза в секунду. */
  metricsIntervalMs: z
    .number()
    .positive()
    .transform(v => Math.max(1000, v))
    .optional(),
  logLevel: logLevelSchema.optional(),
  /** Сколько действует, мс (по умолчанию 60 000, не больше 24 часов). */
  ttlMs: z.number().optional().catch(undefined),
});

export type WatchOptions = z.input<typeof watchOptionsSchema>;

export interface WatchRef {
  id: string;
  until: number;
}

/** Какие события получает подписка: агент (нет — любой), воркер, тип. */
const eventFilterSchema = z.object({
  agentId: messageIdSchema.optional(),
  worker: nameSchema,
  type: eventTypeSchema,
});

export type EventFilter = z.input<typeof eventFilterSchema>;

/** Обработчик подписки; его ошибка пишется в журнал и не мешает подтверждению события. */
export type EventHandler = (e: AgentEvent) => void | Promise<void>;

/** Параметры waitEvent. */
export interface WaitEventOptions {
  /** Подходит ли событие (по data); нет — первое событие этого типа. */
  match?: (e: AgentEvent) => boolean;
  /** Сколько ждать, мс (по умолчанию 30 000, не больше суток); не дождались — TIMEOUT. */
  timeoutMs?: number;
  /** Отмена ожидания — CANCELLED. */
  signal?: AbortSignal;
}

interface Subscription {
  filter: z.output<typeof eventFilterSchema>;
  handler: EventHandler;
  /** Тип сверен с манифестом воркера при подписке. */
  checked: boolean;
}

/** Сколько последних id событий помнить для отсечения повторов. */
const SEEN_EVENTS = 10_000;

export class Observe {
  private readonly ctx: Context;
  /** Наблюдатели этого процесса: id агента → id наблюдателя → параметры. */
  private readonly watchers = new Map<string, Map<string, Watcher>>();
  /** Обработанные события ("агент\nid"): повтор доставки подтверждается без обработки. */
  private readonly seen = new LRUCache<string, true>({ max: SEEN_EVENTS });
  /** После обработчика бэкенда: события задач — ожидающим runJob. */
  private readonly handled: (e: AgentEvent) => void;
  /** Подписки этого процесса (subscribeEvents, waitEvent). */
  private readonly subs = new Set<Subscription>();
  /** Несверенные подписки, уже сверенные с манифестом агента: "агент\nворкер\nтип". */
  private readonly vetted = new LRUCache<string, true>({ max: SEEN_EVENTS });

  constructor(ctx: Context, handled: (e: AgentEvent) => void = () => {}) {
    this.ctx = ctx;
    this.handled = handled;
  }

  /**
   * Наблюдатель: пока он есть, агент присылает метрики чаще и журнал подробнее (§9). Повтор с тем
   * же id продлевает и заменяет параметры.
   */
  async watch(agentId: string, opts: WatchOptions): Promise<WatchRef> {
    const {
      id = newId(),
      ttlMs,
      ...params
    } = valid(parse(watchOptionsSchema, opts, "watch"));

    await this.ctx.agent(agentId);
    const until = Date.now() + bounded(ttlMs, 60_000, 1000, DAY_MS);
    let mine = this.watchers.get(agentId);

    if (!mine) this.watchers.set(agentId, (mine = new Map()));
    mine.set(id, { until, ...params });
    this.applyLive(agentId);

    return { id, until };
  }

  unwatch(agentId: string, id: string): void {
    valid(parse(messageIdSchema, id, "id"));
    const mine = this.watchers.get(agentId);

    if (!mine?.delete(id)) return;
    if (!mine.size) this.watchers.delete(agentId);
    this.applyLive(agentId);
  }

  /** Отправить сессии сводку наблюдателей, если она изменилась. */
  apply(ss: Session): void {
    const w = summarize(
      this.watchers.get(ss.agentId)?.values() ?? [],
      Date.now(),
    );
    const key = JSON.stringify(w);

    if (key === ss.watchSent) return;
    ss.watchSent = key;
    ss.send({ type: "watch", data: w });
  }

  /** Убрать истёкших наблюдателей; сводку — сессиям этого процесса. */
  sweep(now: number): void {
    for (const [agentId, mine] of this.watchers) {
      for (const [id, w] of mine) if (w.until <= now) mine.delete(id);
      if (!mine.size) this.watchers.delete(agentId);
      this.applyLive(agentId);
    }
  }

  /** Забыть наблюдателей агента (удалён). */
  forget(agentId: string): void {
    this.watchers.delete(agentId);
  }

  /**
   * Подписка на события типа type воркера worker (агента agentId или любого) в этом процессе;
   * итог — отписка. Агент известен и манифест воркера есть — тип сверяется сразу (нет в манифесте
   * — EVENT_UNDECLARED); иначе — при первом событии этого воркера (предупреждение в журнал).
   */
  async subscribe(
    filter: EventFilter,
    handler: EventHandler,
  ): Promise<() => void> {
    const f = valid(parse(eventFilterSchema, filter, "subscribeEvents"));
    const a =
      f.agentId === undefined
        ? undefined
        : await this.ctx.store.getAgent(f.agentId);
    const m = a && workerManifest(publicAgent(a), f.worker);

    if (m && !declaresEvent(m, f.type))
      throw codeError(
        "EVENT_UNDECLARED",
        `события ${f.type} нет в манифесте воркера ${f.worker} (events)`,
      );
    const sub: Subscription = { filter: f, handler, checked: Boolean(m) };

    this.subs.add(sub);

    return () => void this.subs.delete(sub);
  }

  /** Первое подходящее событие после вызова; не дождались — TIMEOUT, отмена — CANCELLED. */
  async waitEvent(
    agentId: string,
    worker: string,
    type: string,
    opts: WaitEventOptions = {},
  ): Promise<AgentEvent> {
    const { match, signal } = opts;
    const timeoutMs = bounded(opts.timeoutMs, 30_000, 1, DAY_MS);
    const cancelled = () => codeError("CANCELLED", "ожидание отменено");

    if (signal?.aborted) throw cancelled();
    let resolve!: (e: AgentEvent) => void;
    let reject!: (err: unknown) => void;
    const waiting = new Promise<AgentEvent>((ok, fail) => {
      resolve = ok;
      reject = fail;
    });
    const stop = await this.subscribe({ agentId, worker, type }, e => {
      try {
        if (!match || match(e)) resolve(e);
      } catch (err) {
        reject(err);
      }
    });
    const onAbort = () => reject(cancelled());
    const timer = setTimeout(
      () =>
        reject(
          codeError(
            "TIMEOUT",
            `нет события ${type} от воркера ${worker} за ${timeoutMs} мс`,
          ),
        ),
      timeoutMs,
    );

    signal?.addEventListener("abort", onAbort, { once: true });
    if (signal?.aborted) onAbort();
    try {
      return await waiting;
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      stop();
    }
  }

  /**
   * Важное сообщение event: проверка data по схеме (validateEvents), обработчик onEvent, затем
   * ожидающие задач (runJob), событие event и подписки; ошибка обработчика — без подтверждения
   * (агент пришлёт снова). Непустой результат — причина отказа.
   */
  async event(ss: Session, env: Envelope): Promise<string> {
    const p = parseEvent(env.data);

    if (!p.ok) return p.error;
    const seenKey = `${ss.agentId}\n${env.id}`;

    if (this.seen.has(seenKey)) return "";
    const { at, ...d } = p.value;
    const now = Date.now();
    const e: AgentEvent = {
      id: env.id!,
      agentId: ss.agentId,
      worker: d.worker,
      type: d.type,
      at: at ?? now,
      receivedAt: now,
    };

    if (d.data !== undefined) e.data = d.data;
    const manifest = await this.manifestFor(e);

    if (this.rejected(e, manifest)) {
      this.seen.set(seenKey, true);

      return "";
    }
    await this.ctx.settings.onEvent?.(e);
    this.seen.set(seenKey, true);
    this.handled(e);
    this.ctx.emit("event", e);
    this.notify(e, manifest);

    return "";
  }

  /**
   * Манифест воркера события — если он нужен: проверка data (validateEvents) или сверка
   * подписок, тип которых при подписке не сверен.
   */
  private async manifestFor(
    e: AgentEvent,
  ): Promise<WorkerManifest | undefined> {
    const validate =
      this.ctx.settings.validateEvents !== "off" &&
      !e.type.startsWith(JOB_EVENT_PREFIX);
    const unvetted = [...this.subs].some(
      s =>
        !s.checked &&
        this.applies(s, e) &&
        !this.vetted.has(vetKey(e, s.filter.type)),
    );

    if (!validate && !unvetted) return undefined;
    const a = await this.ctx.store.getAgent(e.agentId);

    return a && workerManifest(publicAgent(a), e.worker);
  }

  /** data не по events[].schema (validateEvents): журнал, invalidEvent; reject — не передавать. */
  private rejected(e: AgentEvent, m: WorkerManifest | undefined): boolean {
    const mode = this.ctx.settings.validateEvents;
    const schema =
      mode === "off" ? undefined : m?.events?.find(x => x.type === e.type);

    if (!schema?.schema) return false;
    let problems: string[];

    try {
      problems = jsonSchemaProblems(schema.schema, e.data ?? null);
    } catch (err) {
      this.ctx.log("схема события из манифеста воркера не применяется", {
        agentId: e.agentId,
        worker: e.worker,
        type: e.type,
        err: String(err),
      });

      return false;
    }
    if (!problems.length) return false;
    const rejected = mode === "reject";

    this.ctx.log("data события не по схеме манифеста", {
      agentId: e.agentId,
      worker: e.worker,
      type: e.type,
      problems,
      rejected,
    });
    this.ctx.emit("invalidEvent", { event: e, problems, rejected });

    return rejected;
  }

  /** Подписки этого процесса; несверенные — сверить с манифестом (предупреждение один раз). */
  private notify(e: AgentEvent, m: WorkerManifest | undefined): void {
    for (const s of [...this.subs]) {
      if (!this.applies(s, e)) continue;
      if (!s.checked) this.vet(e, s.filter.type, m);
      if (s.filter.type !== e.type) continue;
      try {
        void Promise.resolve(s.handler(e)).catch(err => this.failed(e, err));
      } catch (err) {
        this.failed(e, err);
      }
    }
  }

  /** Подписка относится к агенту и воркеру события (тип — любой). */
  private applies(s: Subscription, e: AgentEvent): boolean {
    const { agentId, worker } = s.filter;

    return (
      worker === e.worker && (agentId === undefined || agentId === e.agentId)
    );
  }

  /** Тип несверенной подписки — в манифесте агента события; нет — предупреждение. */
  private vet(
    e: AgentEvent,
    type: string,
    m: WorkerManifest | undefined,
  ): void {
    const key = vetKey(e, type);

    if (this.vetted.has(key) || !m) return;
    this.vetted.set(key, true);
    if (!declaresEvent(m, type))
      this.ctx.log("подписка на событие, которого нет в манифесте воркера", {
        agentId: e.agentId,
        worker: e.worker,
        type,
      });
  }

  private failed(e: AgentEvent, err: unknown): void {
    this.ctx.log("подписчик события упал", {
      agentId: e.agentId,
      worker: e.worker,
      type: e.type,
      err: String(err),
    });
  }

  private applyLive(agentId: string): void {
    const ss = this.ctx.live(agentId);

    if (ss) this.apply(ss);
  }
}

const vetKey = (e: AgentEvent, type: string): string =>
  `${e.agentId}\n${e.worker}\n${type}`;
