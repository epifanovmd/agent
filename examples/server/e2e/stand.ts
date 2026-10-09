// Стенд сквозного теста: сервер стенда (createApp над Agents) в процессе теста на свободном
// порту и настоящий агент с воркерами-примерами echo (Python), node-echo (Node.js), sysinfo и
// netprobe (Go). У каждого стенда — свой временный каталог: настройки и данные агента, журналы
// агента (agent.log) и сервера (server.log). E2E_KEEP=1 — не удалять его после теста.
//
// Сборки агента и Go-воркеров — в E2E_BIN (по умолчанию .dev/e2e; их собирает scripts/e2e.sh).
import { type ChildProcess, execFile, spawn } from "node:child_process";
import { createWriteStream, existsSync, readFileSync } from "node:fs";
import {
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import type { Server } from "node:http";
import type { AddressInfo } from "node:net";
import { arch, platform, tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import {
  type Agent,
  type AgentEvent,
  Agents,
  type AgentsOptions,
  type JobResult,
  MemoryStore,
} from "agent-sdk/server";

import { createApp } from "../src/app";
import { History } from "../src/history";
import { onWorkerRequest } from "../src/requests";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");

export const BIN = resolve(process.env.E2E_BIN ?? join(ROOT, ".dev/e2e"));
const PLATFORM = `${platform()}-${arch() === "x64" ? "amd64" : arch()}`;

/** Сборка под эту машину: <dir>/<name>-<os>-<arch>. */
export const bin = (name: string, dir = BIN): string =>
  join(dir, `${name}-${PLATFORM}`);

/** Версия агента из VERSION и версия сборки для проверки обновления. */
export const VERSION = readFileSync(join(ROOT, "VERSION"), "utf8").trim();
export const NEXT_VERSION = existsSync(join(BIN, "next/VERSION"))
  ? readFileSync(join(BIN, "next/VERSION"), "utf8").trim()
  : "";

const TOKEN = "e2e-token";

export const WORKERS = ["echo", "node-echo", "sysinfo", "netprobe"];

export const run = promisify(execFile);

type Check<T> = () => T | undefined | false | Promise<T | undefined | false>;

/** Ждать, пока check не вернёт значение (не undefined и не false), не дольше timeoutMs. */
export const waitFor = async <T>(
  what: string,
  check: Check<T>,
  timeoutMs = 20_000,
): Promise<T> => {
  const deadline = Date.now() + timeoutMs;
  let last: unknown;

  for (;;) {
    try {
      const v = await check();

      if (v !== undefined && v !== false) return v;
    } catch (e) {
      last = e;
    }
    if (Date.now() > deadline)
      throw new Error(
        `не дождались: ${what}${last ? ` (${String(last)})` : ""}`,
      );
    await sleep(200);
  }
};

/** Ошибка API стенда: HTTP-статус и код. */
export class ApiError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(`${status} ${code}: ${message}`);
    this.status = status;
    this.code = code;
  }
}

export interface StandOptions {
  /** Имя агента. */
  name: string;
  /** Каталог сборок для Agents. */
  releasesDir?: string;
  /** Ключ проверки подписи сборок для агента (update.publicKey); без него и updateKeys — update.mode: disabled. */
  updateKey?: string;
  /** Ключи проверки подписи сборок для агента (update.publicKeys): подпись принимается, если сходится с любым. */
  updateKeys?: string[];
  /** netprobe — воркер со сборкой с сервера (release: true): сборка в <dataDir>/workers/netprobe/current. */
  releaseNetprobe?: boolean;
  /** Свои настройки Agents сервера стенда. */
  server?: Partial<AgentsOptions>;
  /** Ещё воркеры в agent.yaml (кроме воркеров-примеров). */
  workers?: Record<string, unknown>[];
}

export class Stand {
  readonly dir: string;
  readonly opts: StandOptions;
  readonly store = new MemoryStore();
  /** История стенда переживает перезапуск сервера, как и Store. */
  readonly history = new History();
  agents!: Agents;
  server!: Server;
  port = 0;
  /** id агента на сервере (после регистрации). */
  agentId = "";
  /** Сколько раз запущен процесс агента. */
  agentStarts = 0;
  private proc?: ChildProcess;
  private stopping = false;

  private constructor(dir: string, opts: StandOptions) {
    this.dir = dir;
    this.opts = opts;
  }

  /** Сервер, агент на связи, все воркеры работают. */
  static start = async (opts: StandOptions): Promise<Stand> => {
    if (!existsSync(bin("agent")))
      throw new Error(`нет сборок в ${BIN}: scripts/e2e.sh build`);
    const s = new Stand(await mkdtemp(join(tmpdir(), "agent-e2e-")), opts);

    if (process.env.E2E_KEEP) process.stderr.write(`стенд: ${s.dir}\n`);
    await copyFile(bin("agent"), s.agentBin);
    await s.startServer();
    await s.writeConfig();
    s.startAgent();
    const agent = await s.waitAgent(
      "агент зарегистрировался",
      a => a.name === opts.name && a.online,
    );

    s.agentId = agent.id;
    await s.waitWorkers();

    return s;
  };

  get url(): string {
    return `http://127.0.0.1:${this.port}`;
  }

  get agentBin(): string {
    return join(this.dir, "agent");
  }

  get configPath(): string {
    return join(this.dir, "agent.yaml");
  }

  get dataDir(): string {
    return join(this.dir, "data");
  }

  /** Каталог, где echo хранит ход долгих задач. */
  get jobsDir(): string {
    return join(this.dir, "echo-jobs");
  }

  /** Файл, куда echo (или node-echo) записывает применённую настройку. */
  stateFile = (worker: string): string =>
    join(this.dir, `${worker}-state.json`);

  // ── сервер ──

  startServer = async (extra: Partial<AgentsOptions> = {}): Promise<void> => {
    const log = createWriteStream(join(this.dir, "server.log"), {
      flags: "a",
    });

    this.agents = new Agents({
      store: this.store,
      enrollToken: TOKEN,
      statusIntervalMs: 1000,
      metricsIntervalMs: 3000,
      onEvent: this.history.addEvent,
      onWorkerRequest,
      offlineGraceMs: 1000,
      releasesDir: this.opts.releasesDir,
      log: (msg, more) =>
        log.write(
          `${new Date().toISOString()} ${msg} ${JSON.stringify(more ?? {})}\n`,
        ),
      ...this.opts.server,
      ...extra,
    });
    this.history.follow(this.agents);
    this.server = createApp(this.agents, {
      history: this.history,
      enrollToken: TOKEN,
    });
    await new Promise<void>((ok, fail) => {
      this.server.once("error", fail);
      this.server.listen(this.port, "127.0.0.1", ok);
    });
    this.port = (this.server.address() as AddressInfo).port;
  };

  /** Остановить сервер: агенты теряют связь (их важные сообщения ждут в outbox). */
  stopServer = async (): Promise<void> => {
    const server = this.server;

    await this.agents.close();
    server.closeAllConnections();
    await new Promise(ok => server.close(ok));
  };

  /** JSON API стенда; ответ не 2xx — ApiError. */
  api = async <T = unknown>(
    method: string,
    path: string,
    body?: unknown,
  ): Promise<T> => {
    const res = await fetch(this.url + path, {
      method,
      headers:
        body === undefined ? undefined : { "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const data = await res.json().catch(() => null);

    if (!res.ok)
      throw new ApiError(res.status, data?.code ?? "", data?.message ?? "");

    return data as T;
  };

  /** Запрос к воркеру через API стенда (/api/agents/{id}/workers/{worker}/fetch/…). */
  fetchWorker = (
    worker: string,
    path: string,
    init: RequestInit & { timeoutMs?: number } = {},
  ): Promise<Response> => {
    const { timeoutMs, ...rest } = init;
    const headers = new Headers(rest.headers);

    if (timeoutMs !== undefined) headers.set("x-timeout-ms", String(timeoutMs));

    return fetch(
      `${this.url}/api/agents/${this.agentId}/workers/${worker}/fetch${path}`,
      { ...rest, headers },
    );
  };

  /** Задача воркеру через API стенда (runJob): { type, jobId?, data?, timeoutMs? }. */
  job = (worker: string, body: Record<string, unknown>): Promise<JobResult> =>
    this.api<JobResult>(
      "POST",
      `/api/agents/${this.agentId}/workers/${worker}/jobs`,
      body,
    );

  /** События задачи jobId воркера (job.*), по порядку прихода. */
  jobEvents = async (worker: string, jobId: string): Promise<AgentEvent[]> =>
    (
      await this.api<AgentEvent[]>(
        "GET",
        `/api/events?agentId=${this.agentId}&worker=${worker}&limit=10000`,
      )
    )
      .filter(e => (e.data as { jobId?: string })?.jobId === jobId)
      .reverse();

  /** POST /echo воркера → текст ответа. */
  echo = async (worker: string, text: string): Promise<string> => {
    const res = await this.fetchWorker(worker, "/echo", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ text }),
    });

    if (!res.ok) throw new Error(`POST /echo: ${res.status}`);

    return ((await res.json()) as { text: string }).text;
  };

  agent = (): Promise<Agent> =>
    this.api<Agent>("GET", `/api/agents/${this.agentId}`);

  waitAgent = (
    what: string,
    check: (a: Agent) => boolean,
    timeoutMs?: number,
  ): Promise<Agent> =>
    waitFor(
      what,
      async () => {
        const list = await this.api<Agent[]>("GET", "/api/agents");

        return list.find(
          a => (!this.agentId || a.id === this.agentId) && check(a),
        );
      },
      timeoutMs,
    );

  /** Все воркеры (и встроенный sysmetrics) работают и в порядке. */
  waitWorkers = (timeoutMs = 30_000): Promise<Agent> =>
    this.waitAgent(
      "все воркеры работают",
      a =>
        a.online &&
        ["sysmetrics", ...WORKERS].every(name =>
          a.status?.workers.some(
            w =>
              w.name === name &&
              w.state === "running" &&
              (w.health?.ok ?? name === "sysmetrics"),
          ),
        ),
      timeoutMs,
    );

  /** Воркер из последнего status. */
  worker = async (name: string) => {
    const a = await this.agent();

    return a.status?.workers.find(w => w.name === name);
  };

  // ── агент ──

  /** Ключ агента из каталога данных. */
  credentials = async (): Promise<{ agentId: string; secret: string }> =>
    JSON.parse(await readFile(join(this.dataDir, "credentials.json"), "utf8"));

  writeConfig = async (): Promise<void> => {
    const { name, updateKey, updateKeys, releaseNetprobe } = this.opts;
    const echoEnv = (worker: string) => ({
      ECHO_STATE_FILE: this.stateFile(worker),
    });
    const lifecycle = { stopTimeout: "5s" };
    // JSON — тоже YAML: агент читает его как agent.yaml.
    const config = {
      server: { url: this.url },
      dataDir: this.dataDir,
      name,
      enroll: { token: TOKEN },
      update:
        updateKey || updateKeys
          ? { mode: "self", publicKey: updateKey, publicKeys: updateKeys }
          : { mode: "disabled" },
      log: { level: "debug", forward: "warn" },
      telemetry: { metrics: ["cpu", "load", "memory", "network", "uptime"] },
      workers: [
        {
          name: "echo",
          command: ["python3", join(ROOT, "examples/workers/echo/main.py")],
          env: {
            PYTHONUNBUFFERED: "1",
            ECHO_JOBS_DIR: this.jobsDir,
            ...echoEnv("echo"),
          },
          // GET /health чаще обычного: busy и зависание видны быстрее.
          lifecycle: {
            ...lifecycle,
            health: { interval: "2s", timeout: "1s", failures: 3 },
          },
        },
        {
          name: "node-echo",
          command: [
            process.execPath,
            join(ROOT, "examples/workers/node-echo/main.mjs"),
          ],
          env: echoEnv("node-echo"),
          lifecycle,
        },
        { name: "sysinfo", command: [bin("sysinfo")], lifecycle },
        releaseNetprobe
          ? { name: "netprobe", release: true, lifecycle }
          : { name: "netprobe", command: [bin("netprobe")], lifecycle },
        ...(this.opts.workers ?? []),
      ],
    };

    await writeFile(this.configPath, JSON.stringify(config, null, 2));
    if (releaseNetprobe) {
      // Воркер со сборкой с сервера, как после agent install --worker netprobe.
      const dir = join(this.dataDir, "workers", "netprobe");

      await mkdir(dir, { recursive: true });
      await copyFile(bin("netprobe"), join(dir, "current"));
      await writeFile(join(dir, "version"), "1.0.0\n");
    }
  };

  /**
   * Запустить процесс агента. Как служба (Restart=always): процесс, завершившийся не по
   * stopAgent (например, для перезапуска после обновления), запускается снова.
   */
  startAgent = (): void => {
    const log = createWriteStream(join(this.dir, "agent.log"), { flags: "a" });
    const env = Object.fromEntries(
      Object.entries(process.env).filter(([k]) => !k.startsWith("AGENT_")),
    );
    const proc = spawn(this.agentBin, ["run", "-config", this.configPath], {
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    this.agentStarts += 1;
    this.stopping = false;
    this.proc = proc;
    // Оба потока — в один файл: закрывает его только выход процесса (close — когда оба
    // потока дочитаны), иначе первый закончившийся поток закрыл бы файл для второго.
    proc.stdout.pipe(log, { end: false });
    proc.stderr.pipe(log, { end: false });
    proc.on("close", (code, signal) => {
      log.end(`--- агент завершился: code=${code} signal=${signal}\n`);
    });
    proc.on("exit", () => {
      if (this.proc !== proc || this.stopping) return;
      this.proc = undefined;
      setTimeout(() => {
        if (!this.proc && !this.stopping) this.startAgent();
      }, 200);
    });
  };

  /**
   * Перезапустить процесс агента: SIGTERM (воркеры работают дальше — lifecycle.onAgentStop:
   * keep), затем запуск — новый агент подхватывает воркеры.
   */
  restartAgent = async (): Promise<void> => {
    await this.stopAgent();
    this.startAgent();
  };

  /** Остановить агента (SIGTERM) и дождаться выхода; воркеры работают дальше. */
  stopAgent = async (): Promise<void> => {
    const proc = this.proc;

    this.stopping = true;
    if (!proc || proc.exitCode !== null) return;
    const exited = new Promise(ok => proc.once("exit", ok));

    proc.kill("SIGTERM");
    const timer = setTimeout(() => proc.kill("SIGKILL"), 20_000);

    await exited;
    clearTimeout(timer);
    this.proc = undefined;
  };

  /** Убить процесс агента (SIGKILL) без перезапуска: связь обрывается без закрытия. */
  killAgent = async (): Promise<void> => {
    const proc = this.proc;

    this.stopping = true;
    if (!proc || proc.exitCode !== null) return;
    const exited = new Promise(ok => proc.once("exit", ok));

    proc.kill("SIGKILL");
    await exited;
    this.proc = undefined;
  };

  /** pid процесса агента. */
  get agentPid(): number | undefined {
    return this.proc?.pid;
  }

  /** Остановить воркеры, пережившие агента (agent stop-workers; агент остановлен). */
  stopWorkers = async (): Promise<void> => {
    await run(this.agentBin, ["stop-workers", "-config", this.configPath], {
      env: Object.fromEntries(
        Object.entries(process.env).filter(([k]) => !k.startsWith("AGENT_")),
      ),
    });
  };

  close = async (): Promise<void> => {
    await this.stopAgent();
    await this.stopWorkers().catch(() => {});
    await this.stopServer().catch(() => {});
    if (!process.env.E2E_KEEP)
      await rm(this.dir, { recursive: true, force: true });
  };
}

/** Событие потока Server-Sent Events. */
export interface StreamEvent {
  type: string;
  data: unknown;
  /** Когда пришло, мс. */
  at: number;
}

/** Поток Server-Sent Events (GET /api/agents/{id}/watch): все события копятся в events. */
export class EventStream {
  readonly events: StreamEvent[] = [];
  private readonly abort = new AbortController();

  private constructor() {}

  static open = async (url: string): Promise<EventStream> => {
    const es = new EventStream();
    const res = await fetch(url, { signal: es.abort.signal });

    if (!res.ok || !res.body) throw new Error(`поток ${url}: ${res.status}`);
    void es.read(res.body);

    return es;
  };

  private read = async (body: ReadableStream<Uint8Array>): Promise<void> => {
    const decoder = new TextDecoder();
    let buf = "";

    try {
      for await (const chunk of body) {
        buf += decoder.decode(chunk, { stream: true });
        let end = buf.indexOf("\n\n");

        while (end >= 0) {
          this.parse(buf.slice(0, end));
          buf = buf.slice(end + 2);
          end = buf.indexOf("\n\n");
        }
      }
    } catch {
      // Поток закрыт (close) или оборвался.
    }
  };

  private parse = (block: string): void => {
    let type = "message";
    let data = "";

    for (const line of block.split("\n")) {
      if (line.startsWith("event: ")) type = line.slice(7);
      else if (line.startsWith("data: ")) data += line.slice(6);
    }
    if (data)
      this.events.push({ type, data: JSON.parse(data), at: Date.now() });
  };

  /** События типа type. */
  of = <T>(type: string): T[] =>
    this.events.filter(e => e.type === type).map(e => e.data as T);

  close = (): void => this.abort.abort();
}
