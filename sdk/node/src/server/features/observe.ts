// Наблюдение: наблюдатели watch в памяти процесса (сводятся в один watch агенту) и события
// воркеров (обработчик onEvent, подтверждение после него, отсечение повторов).
import { LRUCache } from "lru-cache";
import { z } from "zod";

import type { Context } from "../core/context";
import { valid } from "../core/errors";
import type { Session } from "../core/session";
import { bounded, DAY_MS } from "../lib/util";
import type { AgentEvent, Watcher } from "../model/types";
import { summarize } from "../model/watchers";
import { parse, parseEvent } from "../protocol/checks";
import { type Envelope, newId } from "../protocol/messages";
import { logLevelSchema, messageIdSchema } from "../protocol/schemas";

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
   * Важное сообщение event: обработчик onEvent, затем ожидающие задач (runJob) и событие event;
   * ошибка обработчика — без подтверждения (агент пришлёт снова). Непустой результат — причина
   * отказа.
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
    await this.ctx.settings.onEvent?.(e);
    this.seen.set(seenKey, true);
    this.handled(e);
    this.ctx.emit("event", e);

    return "";
  }

  private applyLive(agentId: string): void {
    const ss = this.ctx.live(agentId);

    if (ss) this.apply(ss);
  }
}
