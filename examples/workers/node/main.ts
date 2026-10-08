// Воркер на Node.js (agent-sdk/worker) — все примитивы агента:
//   задачи   example.hash        — sha256 входного файла или текста: прогресс, лог, событие, выходной файл
//   команды  example.node.info   — версия Node, память, аптайм и текущая конфигурация;
//            example.node.fail   — ошибка с кодом (CommandError)
//            example.node.health — {ok, message?}: объявить себя не в порядке (или снова в порядке)
//   домен    example.node.config — приветствие и лимит в памяти (отчёт — что применено)
//   канал    example.node        — память кучи и задержка цикла событий, с частотой метрик агента
//   здоровье setHealth           — degraded, если цикл событий подолгу занят (или по команде)
//   событие  node.started        — при старте; node.config.applied — после применения
//   уборка   cleanup             — `agent cleanup` при удалении агента: сбросить конфигурацию
// Запускает агент (workers в examples/agent.demo.yaml): node --import tsx main.ts
// в каталоге examples/workers/node (tsx — из его node_modules: npm install).
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import { monitorEventLoopDelay } from "node:perf_hooks";
import { AUTO_INTERVAL, CommandError, JobError, Worker } from "agent-sdk/worker";

interface Config {
  greeting: string;
  /** Лимит размера входа задачи example.hash, байт. */
  maxBytes: number;
}

let config: Config = { greeting: "привет от node", maxBytes: 64 << 20 };
let configVersion = 0;
let hashed = 0;

const worker = new Worker({ name: process.env.AGENT_WORKER ?? "node", version: "1.0.0" });

/** Хеш по кускам: прогресс по мере чтения, отмена — через job.signal. */
worker.job("example.hash", { concurrency: 2 }, async (job) => {
  const algorithm = String(job.data.algorithm ?? "sha256");
  const delayMs = Number(job.data.chunkDelayMs ?? 0);
  const hash = createHash(algorithm);
  let bytes = 0;
  let total = 0;
  let source: AsyncIterable<Buffer>;
  if (job.inputs.includes("source")) {
    const path = await job.inputPath("source");
    total = (await stat(path)).size;
    source = createReadStream(path, { highWaterMark: 64 * 1024, signal: job.signal });
  } else if (typeof job.data.text === "string") {
    const buf = Buffer.from(job.data.text);
    total = buf.length;
    source = (async function* () {
      for (let i = 0; i < buf.length; i += 16) yield buf.subarray(i, i + 16);
    })();
  } else {
    throw new JobError("BAD_INPUT", "нужен входной файл source или data.text", { retryable: false });
  }
  if (total > config.maxBytes)
    throw new JobError("TOO_LARGE", `вход ${total} Б больше лимита ${config.maxBytes} Б`, { retryable: false });

  job.log(`${algorithm}: ${total} Б`);
  for await (const chunk of source) {
    job.signal.throwIfAborted();
    hash.update(chunk);
    bytes += chunk.length;
    job.progress(total ? bytes / total : 1, `${bytes} из ${total} Б`);
    if (delayMs) await new Promise((r) => setTimeout(r, delayMs));
  }
  const hex = hash.digest("hex");
  job.event("hashed", { algorithm, bytes });
  if (job.outputs.includes("digest")) await job.upload("digest", Buffer.from(`${hex}  ${job.inputs[0] ?? "text"}\n`));
  hashed++;
  return { algorithm, hex, bytes };
});

worker.command("example.node.info", async (cmd) => {
  cmd.write(`node ${process.version} pid ${process.pid}\n`);
  const mem = process.memoryUsage();
  return {
    node: process.version,
    platform: process.platform,
    arch: process.arch,
    pid: process.pid,
    uptimeSec: Math.round(process.uptime()),
    rssBytes: mem.rss,
    heapUsedBytes: mem.heapUsed,
    hashed,
    config,
    configVersion,
  };
});

/** Снимок целиком и идемпотентно: повтор той же версии ничего не меняет. */
worker.state("example.node.config", async (version, spec) => {
  if (spec?.greeting !== undefined && typeof spec.greeting !== "string") throw new Error("greeting — строка");
  if (spec?.maxBytes !== undefined && !(Number(spec.maxBytes) > 0)) throw new Error("maxBytes — положительное число");
  config = { greeting: spec?.greeting ?? "привет от node", maxBytes: Number(spec?.maxBytes ?? 64 << 20) };
  configVersion = version;
  worker.event("node.config.applied", { version, greeting: config.greeting });
  return { greeting: config.greeting, maxBytes: config.maxBytes };
});

/** Задержка цикла событий, после которой воркер сообщает агенту, что он не в порядке, мс. */
const LAG_LIMIT_MS = Number(process.env.NODE_LAG_LIMIT_MS ?? 500);
/** Здоровье, заданное командой example.node.health (null — по задержке цикла событий). */
let forced: { ok: boolean; message?: string } | null = null;
let health = { ok: true, message: "" };

/** Сообщить агенту о здоровье, только если оно изменилось (агент держит последнее). */
function reportHealth(ok: boolean, message = ""): void {
  if (health.ok === ok && health.message === message) return;
  health = { ok, message };
  worker.setHealth(ok, message || undefined);
}

const lag = monitorEventLoopDelay({ resolution: 20 });
lag.enable();
// Частота опроса — "auto": по подписке сервера на канал (чаще, пока карточку агента смотрят),
// без неё — как у метрик агента; NODE_TELEMETRY_INTERVAL_MS — свой интервал.
const telemetryInterval = process.env.NODE_TELEMETRY_INTERVAL_MS
  ? Number(process.env.NODE_TELEMETRY_INTERVAL_MS)
  : AUTO_INTERVAL;
worker.telemetry("example.node", { intervalMs: telemetryInterval }, () => {
  const mem = process.memoryUsage();
  const lagMs = Math.round((lag.mean / 1e6) * 10) / 10;
  const data = { heapUsedBytes: mem.heapUsed, rssBytes: mem.rss, eventLoopLagMs: lagMs, hashed };
  lag.reset();
  if (forced) reportHealth(forced.ok, forced.message);
  else if (lagMs > LAG_LIMIT_MS) reportHealth(false, `цикл событий занят: ${lagMs} мс`);
  else reportHealth(true);
  return data;
});

// Здоровье вручную (для стенда): {ok: false, message} — degraded; {ok: true} — снова по задержке.
worker.command("example.node.health", async (cmd) => {
  const args = (cmd.args ?? {}) as { ok?: unknown; message?: unknown };
  const ok = args.ok !== false;
  forced = ok ? null : { ok, message: String(args.message ?? "объявлено командой") };
  if (forced) reportHealth(false, forced.message);
  else reportHealth(true);
  return { ok, message: forced?.message ?? "" };
});

// Ошибка команды с кодом (самопроверка и пример CommandError).
worker.command("example.node.fail", async () => {
  throw new CommandError("NODE_FAIL", "ошибка по просьбе");
});

// Удаление агента с узла (`agent cleanup`): снять то, что воркер оставил. Связи с сервером
// нет — событие не нужно; исключение — агент сообщит, что уборка не удалась.
worker.cleanup(async () => {
  config = { greeting: "привет от node", maxBytes: 64 << 20 };
  configVersion = 0;
});

worker.event("node.started", { node: process.version, pid: process.pid });
await worker.run();
lag.disable();
