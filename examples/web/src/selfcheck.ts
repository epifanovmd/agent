// Самопроверка воркеров: сценарий через API сервера — то же, что делает
// приложение: ставит задачи, шлёт команды, меняет желаемое состояние и ждёт
// итогов. Шаг без нужного воркера на агентах — пропускается.
import { api, declared } from "./api";
import type { Command, Job, Snapshot } from "./types";

export type StepState = "waiting" | "running" | "passed" | "failed" | "skipped";

export interface Step {
  id: string;
  title: string;
  worker: string;
  state: StepState;
  detail?: string;
  ms?: number;
}

interface Ctx {
  snap: () => Promise<Snapshot>;
  need: (kind: "queue" | "command" | "domain", name: string) => Promise<boolean>;
  waitJob: (id: string, pred: (j: Job) => boolean, timeoutMs?: number) => Promise<Job>;
  waitCommand: (id: string, timeoutMs?: number) => Promise<Command>;
  until: <T>(what: string, probe: (s: Snapshot) => T | undefined | false, timeoutMs?: number) => Promise<T>;
}

class Skip extends Error {}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function check(cond: unknown, message: string): asserts cond {
  if (!cond) throw new Error(message);
}

const ctx: Ctx = {
  snap: api.snapshot,
  async need(kind, name) {
    const d = declared(await api.snapshot());
    const list = kind === "queue" ? d.queues : kind === "command" ? d.commands : d.domains;
    if (!list.includes(name)) throw new Skip(`ни один агент не объявил ${name}`);
    return true;
  },
  async until(what, probe, timeoutMs = 20_000) {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const hit = probe(await api.snapshot());
      if (hit) return hit;
      if (Date.now() > deadline) throw new Error(`не дождались: ${what}`);
      await sleep(150);
    }
  },
  waitJob(id, pred, timeoutMs) {
    return ctx.until(
      `задача ${id.slice(0, 8)}`,
      (s) => {
        const job = s.jobs.find((j) => j.id === id);
        if (job && (job.status === "failed" || job.status === "cancelled") && !pred(job)) {
          throw new Error(`задача ${job.status}: ${job.error?.code ?? ""} ${job.error?.message ?? ""}`);
        }
        return job && pred(job) ? job : undefined;
      },
      timeoutMs,
    );
  },
  waitCommand(id, timeoutMs) {
    return ctx.until(
      `команда ${id.slice(0, 8)}`,
      (s) => {
        const cmd = s.commands.find((c) => c.id === id);
        return cmd && (cmd.status === "succeeded" || cmd.status === "failed") ? cmd : undefined;
      },
      timeoutMs,
    );
  },
};

interface Scenario {
  id: string;
  title: string;
  worker: string;
  run: (c: Ctx) => Promise<string>;
}

/** Версия домена применена на всех агентах на связи, объявивших домен (команда может уйти любому). */
function appliedEverywhere(s: Snapshot, domain: string, version: number): boolean {
  const agents = s.agents.filter(
    (a) => a.online && a.capabilities?.state?.domains && domain in a.capabilities.state.domains,
  );
  return (
    agents.length > 0 && agents.every((a) => a.stateApplied[domain]?.ok && a.stateApplied[domain].version >= version)
  );
}

/** sha256 в hex (браузер и Node — Web Crypto). */
async function sha256(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

const marker = () => `проверка-${Date.now().toString(36)}`;

export const scenarios: Scenario[] = [
  {
    id: "agents",
    title: "Агент на связи, воркеры запущены",
    worker: "агент",
    async run(c) {
      const s = await c.snap();
      const online = s.agents.filter((a) => a.online);
      check(online.length > 0, "нет агентов на связи");
      const workers = online.flatMap((a) => a.status?.workers ?? []);
      const down = workers.filter((w) => w.state !== "running");
      check(down.length === 0, `не запущены: ${down.map((w) => `${w.name} (${w.state})`).join(", ")}`);
      return `агентов: ${online.length}; воркеры: ${workers.map((w) => w.name).join(", ") || "—"}`;
    },
  },
  {
    id: "echo",
    title: "Задача: текст, входной и выходной файлы",
    worker: "echo",
    async run(c) {
      await c.need("queue", "example.echo");
      const text = marker();
      const job = await api.enqueue({
        queue: "example.echo",
        data: { text },
        inputs: { source: "вход" },
        outputs: ["echo"],
      });
      const done = await c.waitJob(job.id, (j) => j.status === "completed");
      check(done.result?.echo === `${text} | вход`, `результат: ${JSON.stringify(done.result)}`);
      check(done.files.includes("echo"), "выходной файл не загружен");
      const file = await (await fetch(api.fileUrl(job.id, "echo"))).text();
      check(file === `${text} | вход`, `файл: ${file}`);
      return `результат и файл совпали, событий: ${done.events.length}`;
    },
  },
  {
    id: "echo-retry",
    title: "Повтор после ошибки (вторая попытка)",
    worker: "echo",
    async run(c) {
      await c.need("queue", "example.echo");
      const job = await api.enqueue({ queue: "example.echo", data: { text: "повтор", fail: "retry" }, maxAttempts: 2 });
      const done = await c.waitJob(job.id, (j) => j.status === "completed");
      check(done.attempt === 1, `попытка ${done.attempt}`);
      return "первая попытка FLAKY, вторая выполнена";
    },
  },
  {
    id: "echo-fatal",
    title: "Ошибка без повтора",
    worker: "echo",
    async run(c) {
      await c.need("queue", "example.echo");
      const job = await api.enqueue({ queue: "example.echo", data: { text: "x", fail: "fatal" }, maxAttempts: 3 });
      const done = await c.waitJob(job.id, (j) => j.status === "failed");
      check(done.error?.code === "BAD_INPUT" && done.attempt === 0, `ошибка: ${JSON.stringify(done.error)}`);
      return "BAD_INPUT, повторов не было";
    },
  },
  {
    id: "batch",
    title: "Пакетная обработка: этапы, события, файл-результат",
    worker: "batch",
    async run(c) {
      await c.need("queue", "example.batch");
      const job = await api.enqueue({
        queue: "example.batch",
        data: { stages: 3, stepsPerStage: 5, stepSeconds: 0.02 },
        outputs: ["result"],
      });
      const done = await c.waitJob(job.id, (j) => j.status === "completed", 30_000);
      const stages = done.events.filter((e) => e.type === "stage");
      check(stages.length === 3, `событий stage: ${stages.length}`);
      check(done.files.includes("result"), "файл-результат не загружен");
      return `score ${done.result?.score}, лучший этап ${done.result?.bestStage}`;
    },
  },
  {
    id: "batch-stop",
    title: "Досрочная остановка обработки",
    worker: "batch",
    async run(c) {
      await c.need("queue", "example.batch");
      const job = await api.enqueue({
        queue: "example.batch",
        data: { stages: 200, stepsPerStage: 5, stepSeconds: 0.02 },
      });
      await c.waitJob(job.id, (j) => j.status === "running" && j.events.length > 0, 30_000);
      await api.stopJob(job.id);
      const done = await c.waitJob(job.id, (j) => j.status === "completed", 30_000);
      check(done.result?.stopped === true, `результат: ${JSON.stringify(done.result)}`);
      return `остановлено после ${done.result?.stages} этапов`;
    },
  },
  {
    id: "batch-cancel",
    title: "Отмена задачи",
    worker: "batch",
    async run(c) {
      await c.need("queue", "example.batch");
      const job = await api.enqueue({
        queue: "example.batch",
        data: { stages: 200, stepsPerStage: 5, stepSeconds: 0.02 },
      });
      await c.waitJob(job.id, (j) => j.status === "running" && j.progress > 0, 30_000);
      await api.cancelJob(job.id);
      // Слот освобождается: следующая задача очереди снова выполняется.
      const next = await api.enqueue({
        queue: "example.batch",
        data: { stages: 1, stepsPerStage: 2, stepSeconds: 0.01 },
      });
      await c.waitJob(next.id, (j) => j.status === "completed", 30_000);
      return "отменена, слот освободился";
    },
  },
  {
    id: "kv",
    title: "Желаемое состояние → команда читает его",
    worker: "kv",
    async run(c) {
      await c.need("domain", "example.kv");
      await c.need("command", "example.kv.get");
      const value = marker();
      const st = await api.setState("example.kv", { selfcheck: value });
      await c.until("example.kv применено на всех агентах", (s) => appliedEverywhere(s, "example.kv", st.version));
      const cmd = await api.command({ name: "example.kv.get", args: { key: "selfcheck" } });
      const done = await c.waitCommand(cmd.id);
      check(
        done.status === "succeeded" && done.result?.value === value,
        `итог: ${JSON.stringify(done.result ?? done.error)}`,
      );
      return `версия ${st.version} применена, значение прочитано`;
    },
  },
  {
    id: "kv-error",
    title: "Ошибка команды с кодом",
    worker: "kv",
    async run(c) {
      await c.need("command", "example.kv.get");
      const cmd = await api.command({ name: "example.kv.get", args: { key: "нет-такого" } });
      const done = await c.waitCommand(cmd.id);
      check(done.status === "failed" && done.error?.code === "KEY_NOT_FOUND", `итог: ${JSON.stringify(done.error)}`);
      return "KEY_NOT_FOUND";
    },
  },
  {
    id: "sys",
    title: "Go-воркер: состояние и команда",
    worker: "sysinfo",
    async run(c) {
      await c.need("domain", "example.sys.banner");
      await c.need("command", "example.sys.info");
      const text = marker();
      const st = await api.setState("example.sys.banner", { text });
      await c.until("баннер применён на всех агентах", (s) => appliedEverywhere(s, "example.sys.banner", st.version));
      const cmd = await api.command({ name: "example.sys.info" });
      const done = await c.waitCommand(cmd.id);
      check(
        done.status === "succeeded" && done.result?.banner === text,
        `итог: ${JSON.stringify(done.result ?? done.error)}`,
      );
      return `${done.result.go}, ${done.result.os}/${done.result.arch}`;
    },
  },
  {
    id: "sys-stream",
    title: "Вывод команды потоком",
    worker: "sysinfo",
    async run(c) {
      await c.need("command", "example.sys.count");
      const cmd = await api.command({ name: "example.sys.count", args: { to: 3, delaySeconds: 0.2 } });
      const done = await c.waitCommand(cmd.id);
      check(done.status === "succeeded" && done.output === "1\n2\n3\n", `вывод: ${JSON.stringify(done.output)}`);
      return "1, 2, 3";
    },
  },
  {
    id: "sys-timeout",
    title: "Срок команды истёк → TIMEOUT",
    worker: "sysinfo",
    async run(c) {
      await c.need("command", "example.sys.count");
      const cmd = await api.command({ name: "example.sys.count", args: { to: 100, delaySeconds: 1 }, timeoutSec: 2 });
      const done = await c.waitCommand(cmd.id, 20_000);
      check(done.status === "failed" && done.error?.code === "TIMEOUT", `итог: ${JSON.stringify(done.error)}`);
      return `успело: ${done.output.trim().split("\n").filter(Boolean).length}`;
    },
  },
  {
    id: "node-hash",
    title: "Node-воркер: хеш входного файла, выходной файл",
    worker: "node",
    async run(c) {
      await c.need("queue", "example.hash");
      const text = `${marker()}\n`.repeat(50);
      const hex = await sha256(text);
      const job = await api.enqueue({
        queue: "example.hash",
        data: { algorithm: "sha256" },
        inputs: { source: text },
        outputs: ["digest"],
      });
      const done = await c.waitJob(job.id, (j) => j.status === "completed");
      check(done.result?.hex === hex, `хеш: ${done.result?.hex} ≠ ${hex}`);
      check(
        done.events.some((e) => e.type === "hashed"),
        "нет события hashed",
      );
      check(done.files.includes("digest"), "выходной файл не загружен");
      const file = await (await fetch(api.fileUrl(job.id, "digest"))).text();
      check(file.startsWith(hex), `файл: ${file}`);
      return `sha256 совпал, ${done.result.bytes} Б`;
    },
  },
  {
    id: "node-hash-bad",
    title: "Node-воркер: ошибка задачи без повтора",
    worker: "node",
    async run(c) {
      await c.need("queue", "example.hash");
      const job = await api.enqueue({ queue: "example.hash", data: {}, maxAttempts: 3 });
      const done = await c.waitJob(job.id, (j) => j.status === "failed");
      check(done.error?.code === "BAD_INPUT" && done.attempt === 0, `ошибка: ${JSON.stringify(done.error)}`);
      return "BAD_INPUT, повторов не было";
    },
  },
  {
    id: "node-state",
    title: "Node-воркер: состояние → команда читает его",
    worker: "node",
    async run(c) {
      await c.need("domain", "example.node.config");
      await c.need("command", "example.node.info");
      const greeting = marker();
      const st = await api.setState("example.node.config", { greeting });
      await c.until("example.node.config применено на всех агентах", (s) =>
        appliedEverywhere(s, "example.node.config", st.version),
      );
      const cmd = await api.command({ name: "example.node.info" });
      const done = await c.waitCommand(cmd.id);
      check(
        done.status === "succeeded" && done.result?.config?.greeting === greeting,
        `итог: ${JSON.stringify(done.result ?? done.error)}`,
      );
      check(done.output.startsWith("node "), `вывод: ${JSON.stringify(done.output)}`);
      return `${done.result.node}, версия ${st.version} применена`;
    },
  },
  {
    id: "node-cmd-error",
    title: "Node-воркер: ошибка команды с кодом",
    worker: "node",
    async run(c) {
      await c.need("command", "example.node.fail");
      const cmd = await api.command({ name: "example.node.fail" });
      const done = await c.waitCommand(cmd.id);
      check(done.status === "failed" && done.error?.code === "NODE_FAIL", `итог: ${JSON.stringify(done.error)}`);
      return "NODE_FAIL";
    },
  },
  {
    id: "telemetry",
    title: "Телеметрия воркеров в metrics",
    worker: "все",
    async run(c) {
      const d = declared(await c.snap());
      const expected = ["example.kv", "example.sys", "example.batch", "example.node"].filter(
        (ch) =>
          (ch === "example.kv" && d.domains.includes("example.kv")) ||
          (ch === "example.node" && d.commands.includes("example.node.info")) ||
          (ch === "example.sys" && d.commands.includes("example.sys.info")) ||
          (ch === "example.batch" && d.queues.includes("example.batch")),
      );
      if (expected.length === 0) throw new Skip("нет воркеров с каналами");
      await c.until(
        `каналы ${expected.join(", ")}`,
        (s) => s.agents.some((a) => expected.every((ch) => a.metrics?.channels?.[ch] !== undefined)),
        30_000,
      );
      return expected.join(", ");
    },
  },
  {
    id: "events",
    title: "События воркеров",
    worker: "все",
    async run(c) {
      const s = await c.snap();
      const types = new Set(s.events.map((e) => `${e.source}:${e.type}`));
      check(types.size > 0, "событий нет");
      return [...types].slice(0, 6).join(", ");
    },
  },
];

/** Прогнать сценарии по порядку; onStep — изменения шага. */
export async function runSelfCheck(onStep: (step: Step) => void, only?: string[]): Promise<void> {
  for (const sc of scenarios) {
    if (only && !only.includes(sc.id)) continue;
    const step: Step = { id: sc.id, title: sc.title, worker: sc.worker, state: "running" };
    onStep({ ...step });
    const started = performance.now();
    try {
      step.detail = await sc.run(ctx);
      step.state = "passed";
    } catch (err) {
      step.state = err instanceof Skip ? "skipped" : "failed";
      step.detail = (err as Error).message;
    }
    step.ms = Math.round(performance.now() - started);
    onStep({ ...step });
  }
}
