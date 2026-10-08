// Образцы сообщений sdk/spec/examples для тестов. Каждый файл каталога (кроме
// sealed.json) — объект «имя → {from, to, message}»; имена уникальны во всём каталоге.
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import type { Envelope } from "../src/index";

export const EXAMPLES = join(import.meta.dirname, "../../spec/examples");

export type Side = "agent" | "server" | "worker";

interface Example {
  from: Side;
  to: Side;
  message: Envelope;
}

let cache: Map<string, Example> | undefined;

function all(): Map<string, Example> {
  if (cache) return cache;
  const found = new Map<string, Example>();
  for (const file of readdirSync(EXAMPLES).sort()) {
    if (!file.endsWith(".json") || file === "sealed.json") continue;
    const part = JSON.parse(readFileSync(join(EXAMPLES, file), "utf8")) as Record<string, Example>;
    for (const [name, ex] of Object.entries(part)) {
      if (found.has(name)) throw new Error(`образец ${name} повторяется`);
      found.set(name, ex);
    }
  }
  return (cache = found);
}

/** Конверт образца по имени (копия — можно менять). */
export function example(name: string): Envelope {
  const ex = all().get(name);
  if (!ex) throw new Error(`нет образца ${name}`);
  return structuredClone(ex.message);
}

/** Все образцы направления from → to: [имя, конверт], по имени. */
export function between(from: Side, to: Side): [string, Envelope][] {
  const found = [...all()]
    .filter(([, ex]) => ex.from === from && ex.to === to)
    .map(([name, ex]): [string, Envelope] => [name, structuredClone(ex.message)])
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  if (!found.length) throw new Error(`нет образцов ${from} → ${to}`);
  return found;
}

/** sealed.json: тестовая пара ключей агента и запечатанные значения. */
export function sealedExample() {
  return JSON.parse(readFileSync(join(EXAMPLES, "sealed.json"), "utf8"));
}
