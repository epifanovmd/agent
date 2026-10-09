// Связь и наблюдение: регистрация, hello и status, запросы к воркерам, события, метрики,
// журнал, проверка сети netprobe.
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";

import type {
  AgentEvent,
  AgentsError,
  ConfigStatus,
  LogEntry,
  MetricsEvent,
  MetricsPoint,
} from "agent-sdk/server";

import { EventStream, Stand, VERSION, waitFor, WORKERS } from "./stand";

const ECHOES = ["echo", "node-echo"];

/** Ошибка промиса как AgentsError (или провал теста, если промис выполнился). */
const failure = async (p: Promise<unknown>): Promise<AgentsError> => {
  try {
    await p;
  } catch (e) {
    return e as AgentsError;
  }
  assert.fail("ждали ошибку");
};

/** Ответ API стенда с ошибкой: статус и код. */
const apiError = async (res: Response) => ({
  status: res.status,
  code: ((await res.json()) as { code: string }).code,
});

describe("связь и наблюдение", () => {
  let s: Stand;

  before(async () => {
    s = await Stand.start({ name: "e2e-link" });
  });
  after(() => s?.close());

  it("регистрация по токену: на связи, hello, status со всеми воркерами", async () => {
    const a = await s.agent();

    assert.equal(a.online, true);
    assert.equal(a.revoked, false);
    assert.equal(a.version, VERSION);
    assert.ok(a.host?.hostname, "hello.host.hostname");
    assert.ok(["linux", "darwin"].includes(a.host?.os ?? ""), a.host?.os);
    assert.deepEqual(
      a.hello?.workers?.map(w => w.name).sort(),
      [...WORKERS].sort(),
    );
    const workers = a.status?.workers ?? [];
    const builtin = workers.find(w => w.name === "sysmetrics");

    assert.equal(builtin?.builtin, true, "встроенный sysmetrics в status");
    for (const name of WORKERS) {
      const w = workers.find(x => x.name === name);

      assert.equal(w?.state, "running", name);
      assert.equal(w?.health?.ok, true, `${name}: health`);
    }
    assert.equal(
      workers.find(w => w.name === "echo")?.health?.info?.version,
      "1.0.0",
    );
    assert.deepEqual(a.alerts, []);
  });

  describe("запросы к воркерам", () => {
    it("POST /echo у echo и node-echo", async () => {
      for (const w of ECHOES) assert.equal(await s.echo(w, "привет"), "ПРИВЕТ");
    });

    it("потоковый ответ приходит по частям", async () => {
      for (const w of ECHOES) {
        const started = Date.now();
        const res = await s.fetchWorker(w, "/stream?n=4");

        assert.equal(res.status, 200);
        const times: number[] = [];
        let text = "";
        const decoder = new TextDecoder();

        for await (const chunk of res.body!) {
          times.push(Date.now() - started);
          text += decoder.decode(chunk, { stream: true });
        }
        assert.deepEqual(
          text.trim().split("\n"),
          [1, 2, 3, 4].map(i => `СТРОКА ${i} ИЗ 4`),
          w,
        );
        // Куски с паузой 300 мс: первый — задолго до последнего.
        assert.ok(times.length >= 3, `${w}: кусков ${times.length}`);
        assert.ok(times.at(-1)! - times[0] >= 600, `${w}: ${times.join(", ")}`);
      }
    });

    it("двоичный ответ", async () => {
      for (const w of ECHOES) {
        const res = await s.fetchWorker(w, "/bytes?n=1000");

        assert.equal(
          res.headers.get("content-type"),
          "application/octet-stream",
        );
        const bytes = new Uint8Array(await res.arrayBuffer());

        assert.equal(bytes.length, 1000, w);
        assert.ok(
          bytes.every((b, i) => b === i % 256),
          w,
        );
      }
    });

    it("GET /info у sysinfo", async () => {
      const res = await s.fetchWorker("sysinfo", "/info");
      const info = (await res.json()) as { pid: number; go: string };

      assert.equal(res.status, 200);
      assert.ok(info.pid > 0);
      assert.match(info.go, /^go/);
    });

    it("срок запроса: поток обрывается ошибкой TIMEOUT", async () => {
      const res = await s.agents.fetch(s.agentId, "echo", "/stream?n=20", {
        timeoutMs: 500,
      });
      const err = await failure(res.text());

      assert.equal(err.code, "TIMEOUT");
    });

    it("отмена: поток обрывается ошибкой CANCELLED, воркер дальше отвечает", async () => {
      const abort = new AbortController();
      const res = await s.agents.fetch(s.agentId, "node-echo", "/stream?n=20", {
        signal: abort.signal,
      });
      const reader = res.body!.getReader();

      assert.ok((await reader.read()).value?.length);
      abort.abort();
      const err = await failure(reader.read());

      assert.equal(err.code, "CANCELLED");
      assert.equal(await s.echo("node-echo", "ещё"), "ЕЩЁ");
    });

    it("служебный путь — PATH_FORBIDDEN, неизвестный воркер — WORKER_UNKNOWN", async () => {
      assert.deepEqual(await apiError(await s.fetchWorker("echo", "/health")), {
        status: 403,
        code: "PATH_FORBIDDEN",
      });
      assert.deepEqual(
        await apiError(
          await s.fetchWorker("echo", "/config/settings", {
            method: "PUT",
            body: "{}",
          }),
        ),
        { status: 403, code: "PATH_FORBIDDEN" },
      );
      assert.deepEqual(await apiError(await s.fetchWorker("nope", "/echo")), {
        status: 404,
        code: "WORKER_UNKNOWN",
      });
    });
  });

  it("события: POST /work → echo.progress и echo.done", async () => {
    for (const w of ECHOES) {
      const res = await s.fetchWorker(w, "/work", {
        method: "POST",
        body: JSON.stringify({ steps: 3, delayMs: 100 }),
      });

      assert.equal(res.status, 202);
      const { id } = (await res.json()) as { id: string };
      const mine = (e: AgentEvent) => (e.data as { id?: string })?.id === id;
      const events = await waitFor(`${w}: echo.done`, async () => {
        const list = (
          await s.api<AgentEvent[]>(
            "GET",
            `/api/events?agentId=${s.agentId}&worker=${w}`,
          )
        ).filter(mine);

        return list.some(e => e.type === "echo.done") && list;
      });
      const steps = events
        .filter(e => e.type === "echo.progress")
        .map(e => (e.data as { step: number }).step)
        .sort();

      assert.deepEqual(steps, [1, 2, 3], w);
    }
  });

  describe("метрики и журнал", () => {
    it("metrics.host (группы cpu, load, memory, network, uptime) и metrics.workers", async () => {
      // Поля групп из telemetry.metrics стенда; скорости — со второй точки.
      const fields = [
        "cpuPercent",
        "load1",
        "memUsedBytes",
        "memTotalBytes",
        "netRxBps",
        "uptimeSec",
      ];
      const a = await s.waitAgent("метрики узла и воркеров", a => {
        const host = a.metrics?.host ?? {};

        return (
          fields.every(f => f in host) &&
          ["echo", "node-echo", "netprobe", "sysinfo"].every(
            w => a.metrics?.workers?.[w] !== undefined,
          )
        );
      });
      const host = a.metrics!.host as Record<string, number>;

      assert.ok(host.memTotalBytes > 0 && host.uptimeSec > 0);
      const echo = a.metrics!.workers!.echo as { requests: number };

      assert.equal(typeof echo.requests, "number", "счётчик запросов echo");
    });

    it("watch: метрики раз в секунду и живой журнал с уровня info", async () => {
      const stream = await EventStream.open(
        `${s.url}/api/agents/${s.agentId}/watch?logLevel=info`,
      );

      try {
        await waitFor(
          "4 точки метрик",
          () => stream.of<MetricsEvent>("metrics").length >= 4,
          15_000,
        );
        const at = stream.of<MetricsEvent>("metrics").map(m => m.collectedAt);
        const gaps = at.slice(1).map((t, i) => t - at[i]);

        assert.ok(
          gaps.slice(1).every(g => g < 1800),
          `промежутки ${gaps.join(", ")}`,
        );
        // Запись echo уровня info: без watch на сервер уходят только warn и выше.
        await s.echo("echo", "в журнал");
        const entry = await waitFor("запись журнала echo", () =>
          stream
            .of<LogEntry[]>("log")
            .flat()
            .find(e => e.source === "echo" && e.msg.includes("/echo")),
        );

        assert.equal(entry.level, "info");
      } finally {
        stream.close();
      }
    });

    it("история метрик", async () => {
      const points = await s.api<MetricsPoint[]>(
        "GET",
        `/api/agents/${s.agentId}/metrics`,
      );

      assert.ok(points.length >= 3, `точек ${points.length}`);
      assert.ok(points.every((p, i) => i === 0 || p.at >= points[i - 1].at));
    });

    it("журнал агента и воркера с узла", async () => {
      const agentLog = await s.api<LogEntry[]>(
        "GET",
        `/api/agents/${s.agentId}/logs?lines=500`,
      );

      assert.ok(agentLog.some(e => e.source === "agent"));
      const echoLog = await s.api<LogEntry[]>(
        "GET",
        `/api/agents/${s.agentId}/logs?worker=echo`,
      );

      assert.ok(
        echoLog.some(e => e.msg.includes("echo 1.0.0")),
        echoLog.map(e => e.msg).join("\n"),
      );
    });
  });

  it("netprobe: цель TCP — итоги в его метриках", async () => {
    await s.api("PUT", `/api/agents/${s.agentId}/configs/netprobe/targets`, {
      targets: [
        { id: "server", host: "127.0.0.1", port: s.port, method: "tcp" },
      ],
      intervalSec: 1,
      count: 2,
    });
    await waitFor("настройка targets применена", async () => {
      const { status } = await s.api<{ status: ConfigStatus[] }>(
        "GET",
        `/api/agents/${s.agentId}/configs`,
      );

      return status.find(c => c.worker === "netprobe")?.state === "applied";
    });
    interface Probe {
      results?: { id: string; received: number; lossPct: number }[];
    }
    const a = await s.waitAgent("итог проверки в метриках", a =>
      Boolean(
        (a.metrics?.workers?.netprobe as Probe | undefined)?.results?.some(
          r => r.id === "server",
        ),
      ),
    );
    const r = (a.metrics!.workers!.netprobe as Probe).results!.find(
      x => x.id === "server",
    )!;

    assert.equal(r.received, 2);
    assert.equal(r.lossPct, 0);
  });
});
