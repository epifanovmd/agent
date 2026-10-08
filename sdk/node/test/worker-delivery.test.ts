// Доставка итогов и устойчивость воркера: итоги не в JSON и больше 16 МБ, коды
// ошибок, повторная доставка задачи, вызовы до worker.ready, обратное давление
// канала, закрытие канала с ждущими задачами, отклонённые каналы телеметрии.
import assert from "node:assert/strict";
import { Duplex } from "node:stream";
import { test } from "node:test";
import { Channel, CommandError, EARLY_LIMIT, JobError, MAX_LINE_BYTES } from "../src/worker/index";
import { baseName } from "../src/worker/job";
import { fakeAgent, sleep } from "./helpers";

const assign = (jobId: string, queue = "q", extra: Record<string, unknown> = {}) => ({
  jobId,
  attempt: 0,
  queue,
  data: {},
  leaseSeconds: 60,
  ...extra,
});

const BIG = "x".repeat(MAX_LINE_BYTES);

// Необработанный rejection в любом тесте файла — провал.
const rejections: unknown[] = [];
process.on("unhandledRejection", (err) => rejections.push(err));

test("итог задачи: BigInt, цикл, больше 16 МБ — job.fail, процесс жив; коды по схеме", async () => {
  const a = fakeAgent();
  const cyclic: Record<string, unknown> = {};
  cyclic.self = cyclic;
  a.worker.job("q", { concurrency: 4 }, async (job) => {
    switch (job.id) {
      case "bigint":
        return { n: 1n };
      case "cycle":
        return cyclic;
      case "big":
        return BIG;
      default:
        throw new JobError("bad code", "плохо", { retryable: false });
    }
  });
  const done = a.worker.run({ signals: false });
  for (const id of ["bigint", "cycle", "big", "code"]) a.send("job.assign", assign(id));
  await a.inbox.wait("job.fail", () => a.inbox.all("job.fail").length === 4);
  const fails = Object.fromEntries(a.inbox.all("job.fail").map((e) => [e.data.jobId, e.data]));
  assert.equal(fails.bigint.code, "WORKER_ERROR");
  assert.equal(fails.cycle.code, "WORKER_ERROR");
  assert.deepEqual([fails.big.code, fails.big.retryable], ["RESULT_TOO_LARGE", false]);
  assert.equal(fails.code.code, "WORKER_ERROR");
  assert.match(fails.code.message, /bad code/);
  assert.equal(a.inbox.all("job.complete").length, 0);
  a.close();
  await done;
  assert.deepEqual(rejections, []);
});

test("итог команды, отчёт состояния, уборка: не JSON или больше 16 МБ — ответ с ошибкой", async () => {
  const a = fakeAgent();
  a.worker.command("example.cmd", async (cmd) => {
    if (cmd.args.kind === "big") return BIG;
    if (cmd.args.kind === "code") throw new CommandError("not-a-code", "нет");
    return { n: 1n };
  });
  a.worker.state("example.app", async (_v, spec) => (spec.big ? BIG : { n: 1n }));
  a.worker.cleanup(async () => {});
  const done = a.worker.run({ signals: false });
  a.send("cmd.run", { commandId: "c-big", name: "example.cmd", args: { kind: "big" } });
  a.send("cmd.run", { commandId: "c-bigint", name: "example.cmd", args: {} });
  a.send("cmd.run", { commandId: "c-code", name: "example.cmd", args: { kind: "code" } });
  a.send("state.put", { domain: "example.app", version: 1, spec: { big: true } }, { id: "s1" });
  a.send("state.put", { domain: "example.app", version: 2, spec: {} }, { id: "s2" });
  a.send("worker.cleanup", {}, { id: "c1" });
  await a.inbox.wait("cmd.done", () => a.inbox.all("cmd.done").length === 3);
  await a.inbox.wait("state.applied", () => a.inbox.all("state.applied").length === 2);
  const cmds = Object.fromEntries(a.inbox.all("cmd.done").map((e) => [e.data.commandId, e.data]));
  assert.equal(cmds["c-big"].error.code, "RESULT_TOO_LARGE");
  assert.equal(cmds["c-bigint"].error.code, "COMMAND_FAILED");
  assert.equal(cmds["c-code"].error.code, "COMMAND_FAILED");
  assert.match(cmds["c-code"].error.message, /not-a-code/);
  for (const e of a.inbox.all("cmd.done")) assert.equal(e.data.ok, false);
  const states = Object.fromEntries(a.inbox.all("state.applied").map((e) => [e.re, e.data]));
  assert.equal(states.s1.ok, false);
  assert.match(states.s1.error, /^RESULT_TOO_LARGE/);
  assert.equal(states.s2.ok, false);
  assert.deepEqual((await a.inbox.wait("worker.cleaned")).data, { ok: true });
  a.close();
  await done;
  assert.deepEqual(rejections, []);
});

test("событие и телеметрия больше 16 МБ отбрасываются с записью в лог, не JSON — TypeError; канал цел", async () => {
  const logs: string[] = [];
  const a = fakeAgent({ log: (_l, m) => logs.push(m) });
  a.worker.channel("example.app");
  a.worker.telemetry("example.bad", { intervalMs: 20 }, () => ({ n: 1n }));
  const done = a.worker.run({ signals: false });
  a.send("worker.ready", { agentVersion: "1" });
  await a.inbox.wait("worker.register");
  await sleep(10);
  a.worker.event("example.big", BIG);
  a.worker.report("example.app", BIG);
  assert.throws(() => a.worker.event("example.bad", { n: 1n }), TypeError);
  a.worker.event("example.small", 1);
  const ev = await a.inbox.wait("event");
  assert.equal(ev.data.type, "example.small");
  assert.equal(a.inbox.all("telemetry").length, 0);
  assert.ok(logs.some((m) => /больше 16 МБ/.test(m)));
  assert.ok(logs.some((m) => /телеметрия example.bad не собрана/.test(m)));
  a.close();
  await done;
  assert.deepEqual(rejections, []);
});

test("повторная доставка job.assign: та же попытка — без последствий, новая — прежняя отменяется (CANCELLED)", async () => {
  const a = fakeAgent();
  const runs: number[] = [];
  a.worker.job("q", { concurrency: 2 }, async (job) => {
    runs.push(job.attempt);
    if (job.attempt === 0) await new Promise((r) => job.signal.addEventListener("abort", r));
    return { attempt: job.attempt };
  });
  const done = a.worker.run({ signals: false });
  a.send("job.assign", assign("j"));
  await sleep(20);
  a.send("job.assign", assign("j"));
  await sleep(30);
  assert.deepEqual(runs, [0]);
  a.send("job.assign", assign("j", "q", { attempt: 1 }));
  const c = await a.inbox.wait("job.complete");
  assert.deepEqual(c.data, { jobId: "j", attempt: 1, result: { attempt: 1 } });
  await sleep(30);
  assert.equal(a.inbox.all("job.complete").length, 1);
  assert.deepEqual(
    a.inbox.all("job.fail").map((e) => e.data),
    [{ jobId: "j", attempt: 0, code: "CANCELLED", message: "задача отменена", retryable: false }],
  );
  a.close();
  await done;
});

test("вызовы до worker.ready копятся и уходят после него по порядку; предел — старые вытесняются", async () => {
  const a = fakeAgent();
  a.worker.job("example.echo", async () => null).channel("example.app");
  for (let i = 0; i < EARLY_LIMIT + 5; i++) a.worker.event("example.n", i);
  a.worker.setHealth(false, "example.db недоступна");
  a.worker.report("example.app", { n: 1 });
  assert.throws(() => a.worker.event("example.bad", { n: 1n }), TypeError);
  const done = a.worker.run({ signals: false });
  await a.inbox.wait("worker.register");
  await sleep(20);
  assert.deepEqual(
    a.inbox.got.map((e) => e.type),
    ["worker.register"],
  );
  a.send("worker.ready", { agentVersion: "1" });
  await a.inbox.wait("telemetry");
  const sent = a.inbox.got.slice(1);
  assert.equal(sent.length, EARLY_LIMIT);
  assert.equal(sent[0].data.data, 7, "старые вытеснены");
  assert.deepEqual(
    sent.slice(-2).map((e) => e.type),
    ["worker.health", "telemetry"],
  );
  a.close();
  await done;
});

test("закрытие канала: ждущие задачи убираются, их обработчики не запускаются", async () => {
  const a = fakeAgent();
  const started: string[] = [];
  a.worker.job("q", { concurrency: 1 }, async (job) => {
    started.push(job.id);
    await new Promise((r) => job.signal.addEventListener("abort", r));
    await sleep(20);
  });
  const done = a.worker.run({ signals: false });
  a.send("job.assign", assign("a1"));
  a.send("job.assign", assign("a2"));
  a.send("job.assign", assign("a3"));
  await sleep(30);
  assert.deepEqual(started, ["a1"]);
  a.close();
  await done;
  await sleep(50);
  assert.deepEqual(started, ["a1"]);
});

test("отклонённый канал телеметрии не опрашивается; undefined и null — без точки", async () => {
  const a = fakeAgent();
  let taken = 0;
  let empty = 0;
  a.worker.telemetry("example.taken", { intervalMs: 10 }, () => (taken++, 1));
  a.worker.telemetry("example.empty", { intervalMs: 10 }, () => (empty++ % 2 ? undefined : null));
  const done = a.worker.run({ signals: false });
  a.send("worker.ready", { agentVersion: "1", rejected: ["example.taken"] });
  await sleep(80);
  assert.equal(taken, 0);
  assert.ok(empty >= 2);
  assert.equal(a.inbox.all("telemetry").length, 0);
  a.close();
  await done;
});

test("worker.ping → worker.pong; error от агента — в лог с кодом", async () => {
  const logs: string[] = [];
  const a = fakeAgent({ log: (_l, m) => logs.push(m) });
  a.worker.channel("example.app");
  const done = a.worker.run({ signals: false });
  a.send("worker.ping", {}, { id: "p1" });
  assert.equal((await a.inbox.wait("worker.pong")).re, "p1");
  a.send("error", { code: "BAD_MESSAGE", message: "непонятно" });
  await sleep(20);
  assert.ok(logs.some((m) => m.includes("BAD_MESSAGE") && m.includes("непонятно")));
  a.close();
  await done;
});

test("имя входного файла — только последняя часть", () => {
  assert.equal(baseName("../../etc/passwd"), "passwd");
  assert.equal(baseName("a\\b\\c.txt"), "c.txt");
  for (const bad of ["", ".", "..", "a/..", "dir/"]) assert.throws(() => baseName(bad));
});

test("обратное давление: строки ждут 'drain', прогресс и телеметрия при заторе отбрасываются", async () => {
  const written: string[] = [];
  const held: (() => void)[] = [];
  let open = false;
  const stream = new Duplex({
    writableHighWaterMark: 1,
    read() {},
    write(chunk, _enc, cb) {
      written.push(String(chunk));
      if (open) cb();
      else held.push(cb);
    },
  });
  const logs: string[] = [];
  const ch = new Channel(stream, { log: (_l, m) => logs.push(m) });
  for (let i = 0; i < 1500; i++) ch.send("telemetry", { channel: "example.app", data: i });
  ch.send("event", { type: "example.last" });
  assert.equal(written.length, 1, "поток не готов — остальное ждёт");
  open = true;
  for (const cb of held.splice(0)) cb();
  await sleep(20);
  const types = written.map((l) => JSON.parse(l).type);
  assert.equal(types.at(-1), "event", "важное не теряется и идёт по порядку");
  assert.ok(types.length < 1501, `часть телеметрии отброшена: ${types.length}`);
  assert.ok(logs.some((m) => /не успевает/.test(m)));
  ch.close();
});
