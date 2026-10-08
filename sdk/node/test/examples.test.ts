// Образцы sdk/spec/examples (§9): сервер принимает каждый агент → сервер; воркер
// формирует сообщения воркер → агент и понимает агент → воркер.
import assert from "node:assert/strict";
import { test } from "node:test";
import type { Subscription } from "../src/index";
import { Agents, Session, type SubscribeOptions } from "../src/server/index";
import { publicKeyOf, unseal } from "../src/server/seal";
import { between, example, sealedExample } from "./examples";
import { fakeAgent, sleep } from "./helpers";

test("server: образцы агент → сервер принимаются (ни один не отклонён как неизвестный или некорректный)", async () => {
  const agents = new Agents({ enrollToken: "t", metricsStoreIntervalMs: 0 });
  const { agentId, secret } = await agents.enrollAgent({ token: "t", name: "examples" });
  const agent = (await agents.authenticate(`Agent ${agentId}.${secret}`))!;
  const ss = new Session(agent.id, "ws", "http://localhost");
  const logs: unknown[] = [];
  agents.on("log", (_id, entries) => logs.push(...entries));
  const found = between("agent", "server");
  assert.ok(found.length > 10);
  for (const [name, hello] of found.filter(([n]) => n.startsWith("hello"))) {
    await agents.open(ss, hello);
    assert.equal(ss.take()[0].type, "welcome", name);
  }
  for (const [i, [name, env]] of found.filter(([n]) => !n.startsWith("hello")).entries()) {
    // Новый запуск агента перед каждым: seq образцов не обязаны расти по порядку имён.
    const hello = example("hello");
    hello.data.agent.bootId = `boot-${i}`;
    await agents.open(ss, hello);
    ss.take();
    await agents.process(ss, env);
    for (const out of ss.take()) {
      if (out.type !== "error") continue;
      // Итог задачи, которой у агента нет, — семантический отказ, а не ошибка схемы.
      assert.equal(out.data.code, "JOB_LEASE_LOST", `${name}: ${JSON.stringify(out.data)}`);
    }
  }
  const a = (await agents.getAgent(agentId))!;
  assert.ok(a.status && a.metrics && a.capabilities, "status, metrics, capabilities сохранены");
  assert.deepEqual(a.inventory, example("inventory").data, "inventory сохранено");
  assert.notEqual(a.metrics?.backfill, true, "agent.metrics — не backfill");
  const points = await agents.listMetrics(agentId);
  assert.deepEqual(points.map((p) => p.backfill).sort(), [false, true], "обе точки metrics — в истории");
  assert.ok((await agents.listEvents()).length > 0, "событие сохранено");
  assert.deepEqual(logs, example("log").data.entries, "событие log");
  agents.close();
});

test("worker: worker.register совпадает со схемой образца", async () => {
  const want = example("worker.register").data;
  const a = fakeAgent({ name: want.name, version: want.version });
  a.worker
    .job(want.queues[0].name, { concurrency: want.queues[0].concurrency }, async () => null)
    .command(want.commands[0], async () => null)
    .state(want.domains[0], async () => null)
    .channel(want.channels[0]);
  const done = a.worker.run({ signals: false });
  const reg = await a.inbox.wait("worker.register");
  assert.match(reg.data.sdk, /^node\/\d+\.\d+\.\d+$/);
  assert.deepEqual({ ...reg.data, sdk: want.sdk }, want);
  a.close();
  await done;
});

test("worker: понимает агент → воркер (ready, state.put, cmd.cancel, drain) и шлёт telemetry/event/state.applied по образцам", async () => {
  const applied = example("state.applied@worker");
  const telemetry = example("telemetry");
  const event = example("event@worker");
  const put = example("state.put@agent");
  const a = fakeAgent();
  a.worker.state(put.data.domain, async () => applied.data.report);
  a.worker.channel(telemetry.data.channel);
  a.worker.command(
    "example.app.slow",
    (cmd) => new Promise((resolve) => cmd.signal.addEventListener("abort", () => resolve(null))),
  );
  const done = a.worker.run({ signals: false });

  const ready = example("worker.ready");
  a.send(ready.type, ready.data);
  await sleep(10);
  assert.deepEqual(a.worker.rejected, ready.data.rejected);

  a.send(put.type, put.data, { id: put.id });
  const got = await a.inbox.wait("state.applied");
  assert.equal(got.re, applied.re);
  assert.deepEqual(got.data, applied.data);

  a.worker.report(telemetry.data.channel, telemetry.data.data);
  assert.deepEqual((await a.inbox.wait("telemetry")).data, telemetry.data);
  a.worker.event(event.data.type, event.data.data);
  assert.deepEqual((await a.inbox.wait("event")).data, event.data);

  const cancel = example("cmd.cancel");
  a.send("cmd.run", { commandId: cancel.data.commandId, name: "example.app.slow", timeoutSec: 1 });
  await sleep(10);
  a.send(cancel.type, cancel.data);

  const drain = example("worker.drain");
  a.send(drain.type, drain.data);
  await done; // команда отменена, задач нет — воркер завершился
  assert.equal(a.inbox.all("cmd.done").length, 0);
});

test("worker: worker.cleanup → worker.cleaned / worker.cleaned.failed по образцам", async () => {
  const req = example("worker.cleanup");
  const ok = example("worker.cleaned");
  const a = fakeAgent();
  let fail = "";
  a.worker.command("example.noop", async () => null);
  a.worker.cleanup(async () => {
    if (fail) throw new Error(fail);
  });
  const done = a.worker.run({ signals: false });
  a.send(req.type, req.data, { id: req.id });
  const got = await a.inbox.wait("worker.cleaned");
  assert.equal(got.re, ok.re ?? req.id);
  assert.deepEqual(got.data, ok.data);
  const failed = example("worker.cleaned.failed");
  fail = failed.data.error;
  a.send(req.type, req.data, { id: "again" });
  const bad = await a.inbox.wait("worker.cleaned", (e) => e.re === "again");
  assert.deepEqual(bad.data, failed.data, "ошибка — текст исключения");
  a.close();
  await done;
});

/** Раскрыть все {"$sealed"} в значении (как агент перед передачей воркеру). */
function unsealDeep(privateKey: string, v: unknown): unknown {
  if (Array.isArray(v)) return v.map((x) => unsealDeep(privateKey, x));
  if (v && typeof v === "object") {
    const o = v as Record<string, unknown>;
    if (Object.keys(o).length === 1 && typeof o.$sealed === "string") return unseal(privateKey, o.$sealed);
    return Object.fromEntries(Object.entries(o).map(([k, x]) => [k, unsealDeep(privateKey, x)]));
  }
  return v;
}

test("seal: образец sealed.json раскрывается, seal SDK раскрывается тем же ключом", async () => {
  const fx = sealedExample();
  const { privateKey, publicKey } = fx.agentKey;
  assert.equal(publicKeyOf(privateKey), publicKey, "открытый ключ — от закрытого");
  for (const s of fx.samples) assert.deepEqual(unseal(privateKey, s.sealed.$sealed), s.value);
  assert.deepEqual(unsealDeep(privateKey, fx.snapshot.spec), fx.snapshot.unsealed);
  assert.throws(() => unseal(privateKey, fx.wrongKey.$sealed), "чужой ключ — не раскрывается");
  const put = example("state.put.sealed");
  assert.deepEqual(unsealDeep(privateKey, put.data.spec), fx.snapshot.unsealed);

  // seal SDK: ключ из hello.agent.encryptionKey образца.
  const agents = new Agents({ enrollToken: "t" });
  try {
    const { agentId, secret } = await agents.enrollAgent({ token: "t", name: "sealed" });
    const agent = (await agents.authenticate(`Agent ${agentId}.${secret}`))!;
    const hello = example("hello");
    assert.equal(hello.data.agent.encryptionKey, publicKey, "hello образца — с ключом тестовой пары");
    await agents.open(new Session(agent.id, "ws", "http://localhost"), hello);
    for (const s of fx.samples) {
      const sealed = await agents.seal(agentId, s.value);
      assert.deepEqual(unseal(privateKey, sealed.$sealed), s.value);
    }
  } finally {
    agents.close();
  }
});

test("worker: worker.context — worker.context и событие context как в образце", async () => {
  const a = fakeAgent();
  a.worker.command("example.noop", async () => null);
  const got: unknown[] = [];
  a.worker.on("context", (ctx) => got.push(ctx));
  const done = a.worker.run({ signals: false });
  for (const f of ["worker.context", "worker.context.cleanup"]) {
    const env = example(f);
    a.send(env.type, env.data);
    await sleep(20);
    // Нет меток в сообщении — {} (значение по умолчанию).
    const want = { ...env.data, agent: { labels: {}, ...env.data.agent } };
    assert.deepEqual(a.worker.context, want, f);
    assert.deepEqual(got.at(-1), want, f);
  }
  a.close();
  await done;
});

test("worker: worker.health, worker.pause, worker.resume, worker.restart — по образцам", async () => {
  const cases: [string, (w: import("../src/worker/index").Worker, d: Record<string, never>) => void][] = [
    ["worker.health", (w, d) => w.setHealth(d.ok, d.message)],
    ["worker.health.degraded", (w, d) => w.setHealth(d.ok, d.message)],
    ["worker.pause", (w, d) => w.pause(d.queues)],
    ["worker.resume", (w, d) => w.resume(d.queues)],
    ["worker.restart", (w, d) => w.requestRestart(d.reason)],
  ];
  const a = fakeAgent();
  a.worker.job("example.convert", async () => null);
  const done = a.worker.run({ signals: false });
  for (const [f, call] of cases) {
    const env = example(f);
    const before = a.inbox.all(env.type).length;
    call(a.worker, env.data);
    const sent = await a.inbox.wait(env.type, () => a.inbox.all(env.type).length > before);
    assert.ok(sent);
    assert.deepEqual(a.inbox.all(env.type).at(-1)!.data, env.data ?? {}, f);
  }
  a.close();
  await done;
});

test("server: pauseWorker — cmd.run как образец cmd.run.workerPause", async () => {
  const fx = example("cmd.run.workerPause").data;
  const agents = new Agents({ enrollToken: "t" });
  try {
    const { agentId, secret } = await agents.enrollAgent({ token: "t", name: "pause" });
    const agent = (await agents.authenticate(`Agent ${agentId}.${secret}`))!;
    const ss = new Session(agent.id, "ws", "http://localhost");
    const hello = example("hello");
    hello.data.capabilities = { ...hello.data.capabilities, commands: { names: ["worker.pause", "worker.resume"] } };
    await agents.open(ss, hello);
    ss.take();
    const cmd = await agents.pauseWorker(agentId, fx.args.name, { queues: fx.args.queues });
    const run = ss.take().find((e) => e.type === "cmd.run");
    assert.equal(run?.data.name, fx.name);
    assert.deepEqual(run?.data.args, fx.args);
    assert.equal(run?.data.timeoutSec, fx.timeoutSec);
    assert.equal(cmd.name, fx.name);
  } finally {
    agents.close();
  }
});

/** Подписка, сводная которой — sub (образец config.subscription). */
function subscribeOptions(sub: Subscription): SubscribeOptions {
  const opts: SubscribeOptions = {};
  if (sub.statusIntervalMs) opts.status = { intervalMs: sub.statusIntervalMs };
  if (sub.metricsIntervalMs || sub.metrics) opts.metrics = { intervalMs: sub.metricsIntervalMs, groups: sub.metrics };
  if (sub.logLevel) opts.logs = { level: sub.logLevel };
  if (sub.channels)
    opts.channels = Object.fromEntries(Object.entries(sub.channels).map(([ch, intervalMs]) => [ch, { intervalMs }]));
  return opts;
}

test("server: subscribe и unsubscribe — config как образцы config.subscription*", async () => {
  const fx = example("config.subscription").data;
  const agents = new Agents({ enrollToken: "t" });
  try {
    const { agentId, secret } = await agents.enrollAgent({ token: "t", name: "subscription" });
    const agent = (await agents.authenticate(`Agent ${agentId}.${secret}`))!;
    const ss = new Session(agent.id, "ws", "http://localhost");
    await agents.open(ss, example("hello"));
    ss.take();
    const { id } = await agents.subscribe(agentId, subscribeOptions(fx.subscription));
    assert.deepEqual(ss.take().find((e) => e.type === "config")?.data, fx);
    await agents.unsubscribe(agentId, id);
    assert.deepEqual(ss.take().find((e) => e.type === "config")?.data, example("config.subscription.empty").data);
  } finally {
    agents.close();
  }
});

test("server: welcome с подпиской — как образец welcome.subscription", async () => {
  const fx = example("welcome.subscription").data;
  const agents = new Agents({
    enrollToken: "t",
    statusIntervalMs: fx.config.statusIntervalMs,
    metricsIntervalMs: fx.config.metricsIntervalMs,
  });
  try {
    const { agentId, secret } = await agents.enrollAgent({ token: "t", name: "welcome" });
    const agent = (await agents.authenticate(`Agent ${agentId}.${secret}`))!;
    await agents.subscribe(agentId, subscribeOptions(fx.config.subscription));
    const ss = new Session(agent.id, "ws", "http://localhost");
    await agents.open(ss, example("hello"));
    assert.deepEqual(ss.take().find((e) => e.type === "welcome")?.data.config, fx.config);
  } finally {
    agents.close();
  }
});
