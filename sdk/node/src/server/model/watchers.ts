// Наблюдатели watch (§9) без побочных эффектов: сводка в одно сообщение watch.
import { LOG_LEVELS, type Watch } from "../protocol/messages";
import type { Watcher } from "./types";

/** Сводка наблюдателей (§9): частота — наименьшая, уровень — самый подробный, срок — самый поздний. */
export const summarize = (watchers: Iterable<Watcher>, now: number): Watch => {
  const live = [...watchers].filter(w => w.until > now);

  if (!live.length) return {};
  const out: Watch = { untilMs: Math.max(...live.map(w => w.until)) };
  const intervals = live
    .map(w => w.metricsIntervalMs)
    .filter(v => v !== undefined);

  if (intervals.length) out.metricsIntervalMs = Math.min(...intervals);
  const levels = live.map(w => w.logLevel).filter(v => v !== undefined);

  if (levels.length) out.logLevel = LOG_LEVELS.find(l => levels.includes(l));

  return out;
};
