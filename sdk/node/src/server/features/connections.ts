// Сессии агентов в этом процессе (§4): hello → welcome, online и offline с отсрочкой, вытеснение
// второй сессии, перечитывание Store (refresh), обход раз в секунду.
import type { Context } from "../core/context";
import type { Session } from "../core/session";
import { clearOffline, raiseOffline } from "../model/alerts";
import { reportedFromHello } from "../model/config-report";
import type { AgentRecord, Alert } from "../model/types";
import { parseHello } from "../protocol/checks";
import {
  Close,
  type Envelope,
  type Hello,
  type Welcome,
} from "../protocol/messages";

/** Что нужно сессиям от настроек и наблюдения. */
export interface SessionSync {
  /** Версии настроек из hello — известны сессии. */
  known(ss: Session, configs: Hello["configs"]): void;
  /** Сверить настройки сессии с желаемыми. */
  syncConfigs(ss: Session): Promise<void>;
  /** Отправить сводку наблюдателей, если изменилась. */
  applyWatch(ss: Session): void;
  /** Убрать истёкших наблюдателей. */
  sweepWatchers(now: number): void;
}

export class Connections {
  private readonly ctx: Context;
  private readonly sync: SessionSync;
  /** Отсрочка offline после обрыва: id агента → таймер. */
  private readonly grace = new Map<string, NodeJS.Timeout>();
  private stopped = false;

  constructor(ctx: Context, sync: SessionSync) {
    this.ctx = ctx;
    this.sync = sync;
  }

  /** hello → welcome, сверка настроек, watch (§4). */
  open(ss: Session, env: Envelope): Promise<void> {
    return this.ctx.lock(ss.agentId, async () => {
      const parsed = parseHello(env.data);

      if (!parsed.ok) {
        this.ctx.log("неверный hello", {
          agentId: ss.agentId,
          reason: parsed.error,
        });

        return ss.close(Close.Invalid, "неверный hello");
      }
      const hello = parsed.value;

      if (this.stopped)
        return ss.close(Close.Restart, "сервер перезапускается");
      const now = Date.now();
      let ended: Alert[] = [];
      const { agent } = await this.ctx.mutate(ss.agentId, a => {
        if (a.revoked) return false;
        if (a.bootId !== hello.agent.bootId) {
          a.bootId = hello.agent.bootId;
          a.lastSeq = 0;
        }
        a.hello = hello;
        a.online = true;
        a.connectedAt = now;
        a.lastSeenAt = now;
        a.session = { id: ss.id, instance: this.ctx.instanceId, since: now };
        if (ss.address) a.address = ss.address;
        a.labels = { ...hello.labels, ...a.grantedLabels };
        a.configs = reportedFromHello(a.configs, hello.configs, now);
        ended = clearOffline(a);

        return true;
      });

      if (!agent || agent.revoked)
        return ss.close(Close.Unauthorized, "ключ отозван");
      this.cancelGrace(agent.id);
      const prev = this.ctx.sessions.get(agent.id);

      if (prev && prev !== ss)
        prev.close(Close.Duplicate, "подключился другой экземпляр агента");
      this.ctx.sessions.set(agent.id, ss);
      const { metricsIntervalMs, statusIntervalMs } = this.ctx.settings;
      const welcome: Welcome = {
        serverTime: Date.now(),
        metricsIntervalMs,
        statusIntervalMs,
      };

      ss.send({ type: "welcome", data: welcome });
      this.sync.known(ss, hello.configs);
      await this.sync.syncConfigs(ss);
      this.sync.applyWatch(ss);
      ss.welcomed = true;
      this.ctx.log("агент на связи", { agent: agent.name, id: agent.id });
      this.ctx.change(agent.id, "session");
      this.ctx.emitAgent(agent);
      this.ctx.emitAlerts({ started: [], ended });
    });
  }

  /** Соединение закрыто: если оно текущее — offline через offlineGraceMs. */
  closed(ss: Session): Promise<void> {
    ss.close(Close.Normal);

    return this.ctx.lock(ss.agentId, async () => {
      if (this.ctx.sessions.get(ss.agentId) !== ss) return;
      this.ctx.sessions.delete(ss.agentId);
      const graceMs = this.ctx.settings.offlineGraceMs;

      if (graceMs <= 0 || this.stopped) return this.goOffline(ss);
      clearTimeout(this.grace.get(ss.agentId));
      const timer = setTimeout(() => {
        this.grace.delete(ss.agentId);
        void this.ctx
          .lock(ss.agentId, () => this.goOffline(ss))
          .catch(e =>
            this.ctx.log("отметка offline не удалась", { err: String(e) }),
          );
      }, graceMs);

      timer.unref();
      this.grace.set(ss.agentId, timer);
    });
  }

  /** Закрыть сессию агента в этом процессе (4401): отзыв, удаление. */
  drop(agentId: string, reason: string): void {
    this.cancelGrace(agentId);
    const ss = this.ctx.sessions.get(agentId);

    this.ctx.sessions.delete(agentId);
    ss?.close(Close.Unauthorized, reason);
  }

  /** Запись агента уже не за этой сессией: отозван (4401) или подключился к другому процессу (4409). */
  dropForeign(ss: Session, a: AgentRecord): void {
    if (this.ctx.sessions.get(ss.agentId) === ss)
      this.ctx.sessions.delete(ss.agentId);
    if (a.revoked) ss.close(Close.Unauthorized, "ключ отозван");
    else ss.close(Close.Duplicate, "агент подключился к другому процессу");
  }

  /** Перечитать Store для сессий этого процесса (без agentId — всех). */
  async refresh(agentId?: string): Promise<void> {
    for (const [id, ss] of [...this.ctx.sessions]) {
      if (agentId && id !== agentId) continue;
      await this.ctx.lock(id, async () => {
        if (this.ctx.sessions.get(id) !== ss) return;
        const a = await this.ctx.store.getAgent(id);

        if (!a || a.revoked)
          return this.drop(id, a ? "ключ отозван" : "агент удалён");
        if (a.session?.id !== ss.id) {
          this.ctx.sessions.delete(id);

          return ss.close(
            Close.Duplicate,
            "агент подключился к другому процессу",
          );
        }
        if (!ss.welcomed) return;
        await this.sync.syncConfigs(ss);
      });
    }
  }

  /** Раз в секунду: истёкшие наблюдатели, агенты без вестей. */
  async sweep(): Promise<void> {
    if (this.stopped) return;
    const now = Date.now();

    this.sync.sweepWatchers(now);
    // Online без сессии здесь и без вестей: процесс с его сессией упал.
    const stale = (a: AgentRecord) =>
      a.online &&
      !a.revoked &&
      !this.ctx.sessions.has(a.id) &&
      !this.grace.has(a.id) &&
      now - (a.lastSeenAt ?? 0) > this.ctx.settings.offlineAfterMs;

    for (const a of await this.ctx.store.listAgents())
      if (stale(a))
        await this.ctx.lock(a.id, () => this.markOffline(a.id, stale));
  }

  /** Остановить: соединения закрываются кодом 1012, агенты — offline. */
  async close(): Promise<void> {
    this.stopped = true;
    for (const t of this.grace.values()) clearTimeout(t);
    this.grace.clear();
    const open = [...this.ctx.sessions.values()];

    this.ctx.sessions.clear();
    for (const ss of open) ss.close(Close.Restart, "сервер перезапускается");
    await Promise.all(open.map(ss => this.goOffline(ss).catch(() => {})));
  }

  private cancelGrace(agentId: string): void {
    clearTimeout(this.grace.get(agentId));
    this.grace.delete(agentId);
  }

  /**
   * Offline, если запись всё ещё за этой сессией (агент не подключился к другому процессу);
   * lastSeenAt — последняя весть сессии (сообщение или pong).
   */
  private async goOffline(ss: Session): Promise<void> {
    if (this.ctx.sessions.has(ss.agentId)) return;
    await this.markOffline(ss.agentId, a => a.session?.id === ss.id, ss.seenAt);
  }

  private async markOffline(
    agentId: string,
    when: (a: AgentRecord) => boolean,
    seenAt = 0,
  ): Promise<void> {
    const now = Date.now();
    let started: Alert | undefined;
    const { agent, changed } = await this.ctx.mutate(agentId, a => {
      started = undefined;
      if (!a.online || a.revoked || !when(a)) return false;
      a.online = false;
      a.lastSeenAt = Math.max(a.lastSeenAt ?? 0, seenAt);
      delete a.session;
      started = raiseOffline(a, now);

      return true;
    });

    if (!agent || !changed) return;
    this.ctx.log("агент без связи", { agent: agent.name, id: agent.id });
    this.ctx.emitAgent(agent);
    if (started) this.ctx.emitAlerts({ started: [started], ended: [] });
  }
}
