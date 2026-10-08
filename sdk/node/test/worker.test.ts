// Поведение воркера против фейкового агента: задачи, команды, состояние,
// телеметрия, события, файлы, остановка (контракт sdk/README.md §1).
import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";
import {
  AUTO_INTERVAL,
  CommandError,
  JobError,
  StateError,
  jobDefaults,
  Worker as SdkWorker,
} from "../src/worker/index";
import { fakeAgent, sleep } from "./helpers";

const assign = (jobId: string, queue = "q", extra: Record<string, unknown> = {}) => ({
  jobId,
  attempt: 0,
  queue,
  data: { n: 1 },
  leaseSeconds: 60,
  ...extra,
});

test("register — первым сообщением, пустые списки не отправляются", async () => {
  const a = fakeAgent();
  a.worker.job("q", { concurrency: 2 }, async () => 1);
  const done = a.worker.run({ signals: false });
  const reg = await a.inbox.wait("worker.register");
  assert.equal(a.inbox.got[0].type, "worker.register");
  assert.deepEqual(reg.data, {
    name: "t",
    version: "1.0.0",
    sdk: reg.data.sdk,
    queues: [{ name: "q", concurrency: 2 }],
  });
  assert.match(reg.data.sdk, /^node\//);
  a.close();
  await done;
});

test("задача: complete с результатом, прогресс схлопывается, лог и события по порядку", async () => {
  const a = fakeAgent();
  a.worker.job("q", async (job) => {
    for (let i = 1; i <= 20; i++) job.progress(i / 20, `шаг ${i}`);
    job.log("строка");
    job.event("epoch", { loss: 0.5 });
    job.event("epoch", { loss: 0.4 });
    return { sum: job.data.n + 1 };
  });
  const done = a.worker.run({ signals: false });
  a.send("job.assign", assign("j1"));
  const complete = await a.inbox.wait("job.complete");
  assert.deepEqual(complete.data, { jobId: "j1", attempt: 0, result: { sum: 2 } });
  const progress = a.inbox.all("job.progress");
  assert.ok(progress.length <= 3, `прогресс не схлопнулся: ${progress.length}`);
  assert.equal(progress.at(-1)!.data.progress, 1);
  assert.deepEqual(
    progress.flatMap((p) => p.data.log ?? []),
    ["строка"],
  );
  assert.deepEqual(
    a.inbox.all("job.event").map((e) => e.data.seq),
    [1, 2],
  );
  a.close();
  await done;
});

test("задача: JobError — код и retryable, другая ошибка — WORKER_ERROR с повтором", async () => {
  const a = fakeAgent();
  a.worker.job("q", async (job) => {
    if (job.id === "bad") throw new JobError("BAD_INPUT", "плохо", { retryable: false });
    throw new TypeError("упало");
  });
  const done = a.worker.run({ signals: false });
  a.send("job.assign", assign("bad"));
  a.send("job.assign", assign("boom"));
  const bad = await a.inbox.wait("job.fail", (e) => e.data.jobId === "bad");
  assert.deepEqual(bad.data, { jobId: "bad", attempt: 0, code: "BAD_INPUT", message: "плохо", retryable: false });
  const boom = await a.inbox.wait("job.fail", (e) => e.data.jobId === "boom");
  assert.equal(boom.data.code, "WORKER_ERROR");
  assert.equal(boom.data.retryable, true);
  assert.match(boom.data.message, /TypeError: упало/);
  a.close();
  await done;
});

test("задача: job.cancel прерывает signal, итог не отправляется; job.stop — stopRequested", async () => {
  const a = fakeAgent();
  let aborted = false;
  a.worker.job("q", { concurrency: 2 }, async (job) => {
    if (job.id === "c") {
      await new Promise((_, reject) =>
        job.signal.addEventListener("abort", () => ((aborted = true), reject(job.signal.reason))),
      );
    }
    let steps = 0;
    while (!job.stopRequested) {
      steps++;
      await sleep(5);
    }
    return { stopped: true, steps };
  });
  const done = a.worker.run({ signals: false });
  a.send("job.assign", assign("c"));
  a.send("job.assign", assign("s"));
  await sleep(30);
  a.send("job.cancel", { jobId: "c", attempt: 0 });
  a.send("job.stop", { jobId: "s", attempt: 0 });
  const stopped = await a.inbox.wait("job.complete", (e) => e.data.jobId === "s");
  assert.equal(stopped.data.result.stopped, true);
  await sleep(30);
  assert.ok(aborted);
  assert.equal(
    a.inbox.got.some((e) => e.data?.jobId === "c" && (e.type === "job.complete" || e.type === "job.fail")),
    false,
  );
  a.close();
  await done;
});

test("задачи очереди — не больше concurrency одновременно", async () => {
  const a = fakeAgent();
  let now = 0;
  let peak = 0;
  a.worker.job("q", { concurrency: 2 }, async () => {
    peak = Math.max(peak, ++now);
    await sleep(20);
    now--;
  });
  const done = a.worker.run({ signals: false });
  for (let i = 0; i < 5; i++) a.send("job.assign", assign(`j${i}`));
  for (let i = 0; i < 5; i++) await a.inbox.wait("job.complete", (e) => e.data.jobId === `j${i}`);
  assert.equal(peak, 2);
  a.close();
  await done;
});

test("команда: вывод потоком и итог; CommandError; неизвестная; cmd.cancel — без итога", async () => {
  const a = fakeAgent();
  a.worker.command("x.echo", async (cmd) => {
    cmd.write("раз\n");
    cmd.write("два\n");
    return { args: cmd.args };
  });
  a.worker.command("x.fail", async () => {
    throw new CommandError("NOPE", "нельзя");
  });
  a.worker.command(
    "x.slow",
    (cmd) => new Promise((_, reject) => cmd.signal.addEventListener("abort", () => reject(cmd.signal.reason))),
  );
  const done = a.worker.run({ signals: false });
  const reg = await a.inbox.wait("worker.register");
  assert.deepEqual(reg.data.commands, ["x.echo", "x.fail", "x.slow"]);

  a.send("cmd.run", { commandId: "c1", name: "x.echo", args: { k: 1 }, timeoutSec: 5 });
  const ok = await a.inbox.wait("cmd.done", (e) => e.data.commandId === "c1");
  assert.deepEqual(ok.data, { commandId: "c1", ok: true, result: { args: { k: 1 } } });
  assert.equal(
    a.inbox
      .all("cmd.output")
      .map((e) => e.data.chunk)
      .join(""),
    "раз\nдва\n",
  );

  a.send("cmd.run", { commandId: "c2", name: "x.fail", timeoutSec: 5 });
  const fail = await a.inbox.wait("cmd.done", (e) => e.data.commandId === "c2");
  assert.deepEqual(fail.data, { commandId: "c2", ok: false, error: { code: "NOPE", message: "нельзя" } });

  a.send("cmd.run", { commandId: "c3", name: "x.none", timeoutSec: 5 });
  assert.equal((await a.inbox.wait("cmd.done", (e) => e.data.commandId === "c3")).data.error.code, "COMMAND_UNKNOWN");

  a.send("cmd.run", { commandId: "c4", name: "x.slow", timeoutSec: 1 });
  await sleep(20);
  a.send("cmd.cancel", { commandId: "c4" });
  await sleep(50);
  assert.equal(
    a.inbox.got.some((e) => e.type === "cmd.done" && e.data.commandId === "c4"),
    false,
  );
  a.close();
  await done;
});

test("состояние: state.put → state.applied с re; ошибка — ok:false", async () => {
  const a = fakeAgent();
  let applied: unknown;
  a.worker.state("x.kv", async (version, spec) => {
    if (spec.bad) throw new Error("не применить");
    applied = spec;
    return { version };
  });
  const done = a.worker.run({ signals: false });
  a.send("state.put", { domain: "x.kv", version: 7, spec: { a: 1 } }, { id: "p1" });
  const ok = await a.inbox.wait("state.applied", (e) => e.re === "p1");
  assert.deepEqual(ok.data, { domain: "x.kv", version: 7, ok: true, report: { version: 7 } });
  assert.deepEqual(applied, { a: 1 });
  a.send("state.put", { domain: "x.kv", version: 8, spec: { bad: true } }, { id: "p2" });
  const bad = await a.inbox.wait("state.applied", (e) => e.re === "p2");
  assert.equal(bad.data.ok, false);
  assert.match(bad.data.error, /не применить/);
  assert.equal("report" in bad.data, false, "обычное исключение — без report");
  a.close();
  await done;
});

test("состояние: StateError → ok:false, error и report", async () => {
  const a = fakeAgent();
  a.worker.state("x.kv", async () => {
    throw new StateError("порт занят", { listeners: 0, port: 8080 });
  });
  const done = a.worker.run({ signals: false });
  a.send("state.put", { domain: "x.kv", version: 3, spec: {} }, { id: "p1" });
  const got = await a.inbox.wait("state.applied", (e) => e.re === "p1");
  assert.deepEqual(got.data, {
    domain: "x.kv",
    version: 3,
    ok: false,
    error: "порт занят",
    report: { listeners: 0, port: 8080 },
  });
  a.close();
  await done;
});

test("телеметрия после worker.ready, сбой источника не роняет; report и event", async () => {
  const a = fakeAgent();
  let calls = 0;
  a.worker.telemetry("x.tm", { intervalMs: 20 }, () => {
    if (++calls === 2) throw new Error("сбой");
    return { calls };
  });
  a.worker.channel("x.push");
  a.worker.event("x.started", { at: 1 }); // до run — уйдёт после register
  const done = a.worker.run({ signals: false });
  const reg = await a.inbox.wait("worker.register");
  assert.deepEqual(reg.data.channels, ["x.tm", "x.push"]);
  await sleep(50);
  assert.equal(a.inbox.all("telemetry").length, 0, "до worker.ready телеметрии нет");
  a.send("worker.ready", { agentVersion: "1.1.0", rejected: ["x.push"] });
  await a.inbox.wait("telemetry", (e) => e.data.data?.calls === 3);
  assert.deepEqual(a.worker.rejected, ["x.push"]);
  a.worker.report("x.push", { v: 1 });
  assert.deepEqual((await a.inbox.wait("telemetry", (e) => e.data.channel === "x.push")).data, {
    channel: "x.push",
    data: { v: 1 },
  });
  assert.throws(() => a.worker.report("x.unknown", {}));
  assert.deepEqual((await a.inbox.wait("event")).data, { type: "x.started", data: { at: 1 } });
  a.close();
  await done;
});

test("файлы: download входа, upload с повтором и свежей ссылкой (job.urls)", async () => {
  jobDefaults.uploadRetryMs = 10;
  const stored = new Map<string, string>();
  let puts = 0;
  const http = createServer(async (req, res) => {
    if (req.method === "GET" && req.url === "/in/src") return res.end("вход");
    const chunks: Buffer[] = [];
    for await (const c of req) chunks.push(c);
    puts++;
    if (req.url === "/out/stale") return res.writeHead(403).end();
    stored.set(req.url!, Buffer.concat(chunks).toString());
    res.end();
  });
  await new Promise<void>((r) => http.listen(0, "127.0.0.1", r));
  const base = `http://127.0.0.1:${(http.address() as AddressInfo).port}`;

  const a = fakeAgent();
  a.worker.job("q", async (job) => {
    const text = await (await import("node:fs/promises")).readFile(await job.inputPath("src"), "utf8");
    await job.upload("res", Buffer.from(`${text}!`));
    return { inputs: job.inputs, outputs: job.outputs };
  });
  const done = a.worker.run({ signals: false });
  a.send(
    "job.assign",
    assign("f1", "q", {
      inputs: { src: `${base}/in/src` },
      outputs: { res: { url: `${base}/out/stale`, contentType: "text/plain" } },
    }),
  );
  const urls = await a.inbox.wait("job.urls");
  assert.deepEqual(urls.data, { jobId: "f1", attempt: 0, outputs: ["res"] });
  a.send(
    "job.urls",
    { inputs: {}, outputs: { res: { url: `${base}/out/fresh` } }, expiresAt: Date.now() + 3600_000 },
    { re: urls.id },
  );
  const complete = await a.inbox.wait("job.complete");
  assert.deepEqual(complete.data.result, { inputs: ["src"], outputs: ["res"] });
  assert.equal(stored.get("/out/fresh"), "вход!");
  assert.equal(puts, 2);
  a.close();
  await done;
  http.close();
});

test("drain: новые задачи — WORKER_STOPPING, текущие и команды дорабатываются, run завершается", async () => {
  const a = fakeAgent();
  a.worker.job("q", async () => {
    await sleep(60);
    return "ok";
  });
  a.worker.command("x.wait", async () => {
    await sleep(80);
    return "done";
  });
  const done = a.worker.run({ signals: false });
  let finished = false;
  void done.then(() => (finished = true));
  a.send("job.assign", assign("long"));
  a.send("cmd.run", { commandId: "w", name: "x.wait", timeoutSec: 5 });
  await sleep(10);
  a.send("worker.drain", {});
  a.send("job.assign", assign("late"));
  const late = await a.inbox.wait("job.fail", (e) => e.data.jobId === "late");
  assert.equal(late.data.code, "WORKER_STOPPING");
  assert.equal(late.data.retryable, true);
  assert.equal(finished, false);
  await a.inbox.wait("job.complete", (e) => e.data.jobId === "long");
  await a.inbox.wait("cmd.done", (e) => e.data.commandId === "w");
  await done;
});

test("SIGTERM — то же, что drain", async () => {
  const a = fakeAgent();
  a.worker.job("q", async () => {
    await sleep(30);
    return 1;
  });
  const done = a.worker.run();
  a.send("job.assign", assign("j"));
  await sleep(5);
  process.emit("SIGTERM");
  await a.inbox.wait("job.complete");
  await done;
  assert.equal(process.listenerCount("SIGTERM"), 0);
});

test("канал закрыт (агента нет): задачи и команды отменяются, run завершается", async () => {
  const a = fakeAgent();
  let jobAborted = false;
  let cmdAborted = false;
  a.worker.job(
    "q",
    (job) =>
      new Promise((_, reject) =>
        job.signal.addEventListener("abort", () => ((jobAborted = true), reject(job.signal.reason))),
      ),
  );
  a.worker.command(
    "x.hang",
    (cmd) =>
      new Promise((_, reject) =>
        cmd.signal.addEventListener("abort", () => ((cmdAborted = true), reject(cmd.signal.reason))),
      ),
  );
  const done = a.worker.run({ signals: false });
  a.send("job.assign", assign("j"));
  a.send("cmd.run", { commandId: "h", name: "x.hang", timeoutSec: 60 });
  await sleep(20);
  a.close();
  await done;
  assert.ok(jobAborted && cmdAborted);
});

test("stopping: сигнал «уходим» для своих фоновых циклов — после drain()", () => {
  const w = new SdkWorker({ name: "s", version: "1" });
  assert.equal(w.stopping.aborted, false);
  let fired = 0;
  w.stopping.addEventListener("abort", () => fired++);
  w.drain();
  w.drain();
  assert.equal(w.stopping.aborted, true);
  assert.equal(fired, 1);
});

test("имена: неверное имя очереди, команды, раздела, канала — ошибка сразу при регистрации", () => {
  const w = new SdkWorker({ name: "t", version: "1.0.0", transport: undefined });
  for (const bad of ["bad name", "-x", "a/b", "x".repeat(65), ""]) {
    assert.throws(() => w.job(bad, async () => 1), /неверное имя/);
    assert.throws(() => w.command(bad, async () => 1), /неверное имя/);
    assert.throws(() => w.state(bad, async () => ({})), /неверное имя/);
    assert.throws(() => w.channel(bad), /неверное имя/);
    assert.throws(() => w.telemetry(bad, {}, () => ({})), /неверное имя/);
  }
  assert.doesNotThrow(() => w.state("example.kv", async () => ({})).command("example.kv-get_1", async () => 1));
});

test("воркер только с уборкой запускается; без объявлений — ошибка", async () => {
  const empty = fakeAgent();
  assert.throws(() => empty.worker.run({ signals: false }), /нечего объявить/);
  empty.close();

  const a = fakeAgent();
  let cleaned = 0;
  a.worker.cleanup(() => {
    cleaned++;
  });
  const done = a.worker.run({ signals: false });
  await a.inbox.wait("worker.register");
  a.send("worker.cleanup", {}, { id: "only" });
  assert.deepEqual((await a.inbox.wait("worker.cleaned", (e) => e.re === "only")).data, { ok: true });
  assert.equal(cleaned, 1);
  a.close();
  await done;
});

test("уборка: worker.cleanup → worker.cleaned с re; без обработчика — ok сразу; исключение — ok:false", async () => {
  const a = fakeAgent();
  a.worker.command("x.noop", async () => null);
  const done = a.worker.run({ signals: false });
  a.send("worker.cleanup", {}, { id: "c0" });
  assert.deepEqual((await a.inbox.wait("worker.cleaned", (e) => e.re === "c0")).data, { ok: true });
  a.close();
  await done;

  const b = fakeAgent();
  let cleaned = 0;
  let fail = false;
  b.worker.command("x.noop", async () => null);
  b.worker.cleanup(async () => {
    await sleep(5);
    if (fail) throw new Error("не убрать");
    cleaned++;
  });
  const done2 = b.worker.run({ signals: false });
  b.send("worker.cleanup", {}, { id: "c1" });
  assert.deepEqual((await b.inbox.wait("worker.cleaned", (e) => e.re === "c1")).data, { ok: true });
  assert.equal(cleaned, 1);
  fail = true;
  b.send("worker.cleanup", {}, { id: "c2" });
  const bad = await b.inbox.wait("worker.cleaned", (e) => e.re === "c2");
  assert.equal(bad.data.ok, false);
  assert.match(bad.data.error, /не убрать/);
  b.close();
  await done2;
});

test("контекст: по умолчанию до worker.context; событие context, worker.context — последний; сбой обработчика не роняет", async () => {
  const a = fakeAgent();
  a.worker.command("x.noop", async () => null);
  assert.deepEqual(a.worker.context, {
    mode: "run",
    agent: { id: "", name: "", version: "", labels: {} },
    online: false,
    metricsIntervalMs: 0,
    logLevel: "",
  });
  const got: unknown[] = [];
  a.worker.on("context", () => {
    throw new Error("обработчик упал");
  });
  a.worker.on("context", (ctx) => got.push(ctx));
  const done = a.worker.run({ signals: false });
  a.send("worker.ready", { agentVersion: "1.1.0" });
  const ctx = {
    mode: "run",
    agent: { id: "a1", name: "node-01", version: "1.1.0", labels: { zone: "eu" } },
    online: true,
    metricsIntervalMs: 15000,
    channels: { "x.load": 1000 },
    logLevel: "warn",
  };
  a.send("worker.context", ctx);
  await sleep(20);
  assert.deepEqual(got, [ctx]);
  assert.deepEqual(a.worker.context, ctx);
  // Неполный контекст — недостающее по умолчанию.
  a.send("worker.context", { mode: "cleanup", online: true });
  await sleep(20);
  assert.equal(a.worker.context.mode, "cleanup");
  assert.equal(a.worker.context.agent.id, "");
  assert.equal(a.worker.context.metricsIntervalMs, 0);
  assert.equal(got.length, 2);
  assert.throws(() => a.worker.on("other" as "context", () => {}), /неизвестное событие/);
  a.close();
  await done;
});

test('телеметрия intervalMs: "auto" — с частотой подписки на канал, без неё — с частотой метрик агента', async () => {
  const a = fakeAgent();
  let calls = 0;
  a.worker.telemetry("x.load", { intervalMs: AUTO_INTERVAL }, () => ({ calls: ++calls }));
  assert.throws(() => a.worker.telemetry("x.bad", { intervalMs: 0 }, () => null), /intervalMs/);
  assert.throws(() => a.worker.telemetry("x.bad", { intervalMs: "metrics" as "auto" }, () => null), /intervalMs/);
  const done = a.worker.run({ signals: false });
  const ctx = (metricsIntervalMs: number, channels?: Record<string, number>) => ({
    mode: "run",
    agent: {},
    online: true,
    metricsIntervalMs,
    logLevel: "",
    ...(channels ? { channels } : {}),
  });
  a.send("worker.context", ctx(30));
  a.send("worker.ready", { agentVersion: "t" });
  await a.inbox.wait("telemetry", (e) => e.data.data.calls >= 4, 1000);
  // Частота метрик реже — опросы реже.
  a.send("worker.context", ctx(60_000));
  await sleep(20);
  let before = calls;
  await sleep(150);
  assert.equal(calls, before, "после смены интервала — не чаще нового");
  // Подписка на чужой канал не влияет; на свой — её частота.
  a.send("worker.context", ctx(60_000, { "x.other": 20 }));
  await sleep(20);
  assert.deepEqual(a.worker.context.channels, { "x.other": 20 });
  before = calls;
  await sleep(150);
  assert.equal(calls, before, "подписка на другой канал — интервал прежний");
  a.send("worker.context", ctx(60_000, { "x.load": 30 }));
  const from = calls;
  await a.inbox.wait("telemetry", (e) => e.data.data.calls >= from + 3, 1000);
  // Подписка кончилась — снова частота метрик.
  a.send("worker.context", ctx(60_000));
  await sleep(20);
  assert.equal(a.worker.context.channels, undefined);
  before = calls;
  await sleep(150);
  assert.equal(calls, before, "без подписки — частота метрик");
  a.close();
  await done;
});

test("самоуправление: setHealth, pause, resume, requestRestart — сообщения агенту; до run — после register", async () => {
  const a = fakeAgent();
  a.worker.job("q1", async () => null).job("q2", async () => null);
  a.worker.setHealth(false, "нет связи с базой");
  const done = a.worker.run({ signals: false });
  await a.inbox.wait("worker.health");
  assert.equal(a.inbox.got[0].type, "worker.register");
  a.worker.setHealth(true);
  a.worker.pause(["q1", "q1"]);
  a.worker.pause();
  a.worker.resume(["q2"]);
  a.worker.resume([]);
  a.worker.requestRestart("утечка памяти");
  a.worker.requestRestart();
  await a.inbox.wait("worker.restart", () => a.inbox.all("worker.restart").length === 2);
  assert.deepEqual(
    a.inbox.all("worker.health").map((e) => e.data),
    [{ ok: false, message: "нет связи с базой" }, { ok: true }],
  );
  assert.deepEqual(
    a.inbox.all("worker.pause").map((e) => e.data),
    [{ queues: ["q1"] }, {}],
  );
  assert.deepEqual(
    a.inbox.all("worker.resume").map((e) => e.data),
    [{ queues: ["q2"] }, {}],
  );
  assert.deepEqual(
    a.inbox.all("worker.restart").map((e) => e.data),
    [{ reason: "утечка памяти" }, {}],
  );
  assert.throws(() => a.worker.pause(["плохое имя"]), /неверное имя/);
  a.close();
  await done;
});
