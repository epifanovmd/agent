// Поток сервера примера — WebSocket /api/ws (examples/API.md): snapshot при
// подключении, дальше изменения; точки метрик и записи лога — по подписке на агента.
// Одно соединение на страницу; обрыв — переподключение с растущей паузой,
// новый snapshot и подписки заново.
import { useEffect, useState, useSyncExternalStore } from "react";
import {
  emptySnapshot,
  type Agent,
  type AgentEvent,
  type Alert,
  type AuditEntry,
  type Command,
  type DesiredState,
  type Job,
  type LogEntry,
  type LogLevel,
  type MetricsPoint,
  type Snapshot,
} from "./types";

/** Сообщение сервера → клиент. */
type ServerMessage =
  | { type: "snapshot"; data: Snapshot }
  | { type: "agent"; data: Agent }
  | { type: "job"; data: Job }
  | { type: "command"; data: Command }
  | { type: "state"; data: DesiredState }
  | { type: "stateDeleted"; domain: string; agentId?: string }
  | { type: "event"; data: AgentEvent }
  | { type: "alert"; data: Alert }
  | { type: "audit"; data: AuditEntry }
  | { type: "metrics"; agentId: string; point: MetricsPoint }
  | { type: "log"; agentId: string; entries: LogEntry[] };

type PointListener = (point: MetricsPoint) => void;
type LogListener = (entries: LogEntry[]) => void;

/** Подписка на агента: слушатели точек метрик и записей лога, уровень логов. */
interface AgentSub {
  points: Set<PointListener>;
  logs: Set<LogListener>;
  logLevel: LogLevel;
}

const RETRY_MIN_MS = 500;
const RETRY_MAX_MS = 15_000;
/** Уровень логов подписки по умолчанию. */
const DEFAULT_LOG_LEVEL: LogLevel = "info";
/** Событий в памяти интерфейса (как в снимке сервера). */
const KEEP_EVENTS = 500;
/** Записей аудита в памяти интерфейса. */
const KEEP_AUDIT = 200;

class Stream {
  private ws?: WebSocket;
  private snap: Snapshot = emptySnapshot;
  private online = false;
  private retryMs = RETRY_MIN_MS;
  private readonly watchers = new Set<() => void>();
  private readonly subs = new Map<string, AgentSub>(); // agentId → подписка

  /** Соединение открывается при первом подписчике. */
  private ensure(): void {
    if (this.ws) return;
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${proto}//${location.host}/api/ws`);
    this.ws = ws;
    ws.onopen = () => {
      for (const [agentId, sub] of this.subs) this.send({ type: "subscribe", agentId, logLevel: sub.logLevel });
    };
    ws.onmessage = (e) => {
      let msg: ServerMessage;
      try {
        msg = JSON.parse(String(e.data));
      } catch {
        return;
      }
      this.apply(msg);
    };
    ws.onclose = () => {
      this.ws = undefined;
      this.setOnline(false);
      const wait = this.retryMs;
      this.retryMs = Math.min(this.retryMs * 2, RETRY_MAX_MS);
      setTimeout(() => this.ensure(), wait * (0.8 + Math.random() * 0.4));
    };
  }

  private send(msg: unknown): void {
    if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(msg));
  }

  private setOnline(v: boolean): void {
    if (this.online === v) return;
    this.online = v;
    this.changed();
  }

  private changed(): void {
    for (const w of this.watchers) w();
  }

  private apply(msg: ServerMessage): void {
    const s = this.snap;
    switch (msg.type) {
      case "snapshot":
        // Журнал аудита — только из потока: переподключение его не сбрасывает.
        this.snap = { ...emptySnapshot, ...msg.data, audit: s.audit };
        this.retryMs = RETRY_MIN_MS;
        this.online = true;
        break;
      case "agent":
        this.snap = { ...s, agents: upsert(s.agents, msg.data, (a) => a.id === msg.data.id, false) };
        break;
      case "job":
        this.snap = { ...s, jobs: upsert(s.jobs, msg.data, (j) => j.id === msg.data.id, true) };
        break;
      case "command":
        this.snap = { ...s, commands: upsert(s.commands, msg.data, (c) => c.id === msg.data.id, true) };
        break;
      case "state":
        this.snap = {
          ...s,
          states: upsert(
            s.states,
            msg.data,
            (x) => x.domain === msg.data.domain && (x.agentId ?? "") === (msg.data.agentId ?? ""),
            false,
          ),
        };
        break;
      case "stateDeleted":
        this.snap = {
          ...s,
          states: s.states.filter((x) => !(x.domain === msg.domain && (x.agentId ?? "") === (msg.agentId ?? ""))),
        };
        break;
      case "event":
        this.snap = { ...s, events: [msg.data, ...s.events].slice(0, KEEP_EVENTS) };
        break;
      case "alert": {
        const a = msg.data;
        const same = (x: Alert) =>
          x.type === a.type && x.agentId === a.agentId && (x.domain ?? x.worker ?? "") === (a.domain ?? a.worker ?? "");
        const rest = s.alerts.filter((x) => !same(x));
        this.snap = { ...s, alerts: a.active ? [a, ...rest] : rest };
        break;
      }
      case "audit":
        this.snap = { ...s, audit: [msg.data, ...s.audit].slice(0, KEEP_AUDIT) };
        break;
      case "metrics":
        for (const fn of this.subs.get(msg.agentId)?.points ?? []) fn(msg.point);
        return;
      case "log":
        for (const fn of this.subs.get(msg.agentId)?.logs ?? []) fn(msg.entries);
        return;
      default:
        return;
    }
    this.changed();
  }

  /** Для useSyncExternalStore. */
  watch = (fn: () => void): (() => void) => {
    this.watchers.add(fn);
    this.ensure();
    return () => this.watchers.delete(fn);
  };

  snapshot = (): Snapshot => this.snap;
  connected = (): boolean => this.online;

  /** Точки метрик агента из потока; отписка — когда слушателей агента не осталось. */
  subscribe(agentId: string, fn: PointListener): () => void {
    return this.listen(agentId, "points", fn);
  }

  /** Записи лога агента из потока (с уровня setLogLevel); отписка — как у subscribe. */
  subscribeLogs(agentId: string, fn: LogListener): () => void {
    return this.listen(agentId, "logs", fn);
  }

  /** С какого уровня слать записи лога агента (сервер меняет уровень лога в подписке клиента). */
  setLogLevel(agentId: string, level: LogLevel): void {
    const sub = this.subs.get(agentId);
    if (!sub || sub.logLevel === level) return;
    sub.logLevel = level;
    this.send({ type: "logLevel", agentId, level });
  }

  private listen<K extends "points" | "logs">(
    agentId: string,
    kind: K,
    fn: K extends "points" ? PointListener : LogListener,
  ): () => void {
    this.ensure();
    let sub = this.subs.get(agentId);
    if (!sub) {
      sub = { points: new Set(), logs: new Set(), logLevel: DEFAULT_LOG_LEVEL };
      this.subs.set(agentId, sub);
      this.send({ type: "subscribe", agentId, logLevel: sub.logLevel });
    }
    const set = sub[kind] as Set<typeof fn>;
    set.add(fn);
    const own = sub;
    return () => {
      set.delete(fn);
      if (own.points.size === 0 && own.logs.size === 0 && this.subs.get(agentId) === own) {
        this.subs.delete(agentId);
        this.send({ type: "unsubscribe", agentId });
      }
    };
  }
}

/** Заменить элемент по условию или добавить (в начало — для «новые первыми»). */
function upsert<T>(list: T[], item: T, same: (x: T) => boolean, newFirst: boolean): T[] {
  const i = list.findIndex(same);
  if (i >= 0) return list.map((x, k) => (k === i ? item : x));
  return newFirst ? [item, ...list] : [...list, item];
}

export const stream = new Stream();

/** Снимок сервера (snapshot + изменения); connected — поток открыт. */
export function useSnapshot(): { snapshot: Snapshot; connected: boolean } {
  const snapshot = useSyncExternalStore(stream.watch, stream.snapshot);
  const connected = useSyncExternalStore(stream.watch, stream.connected);
  return { snapshot, connected };
}

/** Точки метрик агента из потока, пока компонент смонтирован. */
export function useMetricsStream(agentId: string, fn: PointListener): void {
  const [ref] = useState(() => ({ fn }));
  ref.fn = fn;
  useEffect(() => stream.subscribe(agentId, (p) => ref.fn(p)), [agentId, ref]);
}

/** Записи лога агента из потока с уровня level, пока компонент смонтирован. */
export function useLogStream(agentId: string, level: LogLevel, fn: LogListener): void {
  const [ref] = useState(() => ({ fn }));
  ref.fn = fn;
  useEffect(() => stream.subscribeLogs(agentId, (e) => ref.fn(e)), [agentId, ref]);
  useEffect(() => stream.setLogLevel(agentId, level), [agentId, level]);
}
