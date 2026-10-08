// Поведение Agents сквозь настоящий HTTP/WebSocket: фейковые агенты регистрируются,
// здороваются, получают задачи, команды и состояние (контракт sdk/README.md §2).
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AddressInfo } from "node:net";
import { createHash, generateKeyPairSync } from "node:crypto";
import { after, before, test } from "node:test";
import { WebSocket } from "ws";
import {
  Close,
  ENROLL_PATH,
  INSTALL_PATH,
  LINK_PATH,
  RELEASES_PATH,
  WS_CHANNEL,
  SYNC_PATH,
  type Envelope,
} from "../src/index";
import {
  Agents,
  agentsDefaults,
  MemoryFiles,
  MemoryStore,
  Session,
  type AgentsOptions,
  type Change,
  type SubscribeOptions,
} from "../src/server/index";
import { helloLabels } from "../src/server/model";
import { publicKeyOf, unseal } from "../src/server/seal";
import { Inbox, sleep } from "./helpers";

agentsDefaults.commandGraceMs = 300;
agentsDefaults.sweepIntervalMs = 50;

const files = new MemoryFiles();
// Каталог выпуска: манифест, одна сборка, установщик (как make release).
const releasesDir = mkdtempSync(join(tmpdir(), "agent-sdk-rel-"));
const manifest = {
  version: "2.0.0",
  artifacts: [{ os: "linux", arch: "amd64", file: "agent-linux-amd64", sha256: "ab".repeat(32), signature: "c2ln" }],
  workers: [
    {
      name: "report",
      version: "1.10.0",
      os: "linux",
      arch: "amd64",
      file: "report-1.10.0-linux-amd64",
      sha256: "ef".repeat(32),
      signature: "d2c=",
      restart: "stop-first",
      stopTimeout: "30s",
    },
    // Старшая — 1.10.0 (по числам), хотя 1.2.0 в манифесте позже и «больше» как строка.
    {
      name: "report",
      version: "1.2.0",
      os: "linux",
      arch: "amd64",
      file: "report-1.2.0-linux-amd64",
      sha256: "cd".repeat(32),
    },
  ],
};
writeFileSync(join(releasesDir, "manifest.json"), JSON.stringify(manifest));
writeFileSync(join(releasesDir, "agent-linux-amd64"), "BINARY");
writeFileSync(join(releasesDir, "report-1.10.0-linux-amd64"), "WORKER");
writeFileSync(join(releasesDir, "secret.txt"), "не отдавать");
writeFileSync(
  join(releasesDir, "install.sh"),
  '#!/bin/sh\nDEFAULT_SERVER=""\nDEFAULT_PUBLIC_KEY=""\necho "$DEFAULT_SERVER"\n',
);
const agents = new Agents({
  enroll: (token) => (token === "it" ? {} : token === "gpu" ? { labels: { gpu: "1" } } : null),
  files,
  offlineGraceMs: 150,
  metricsStoreIntervalMs: 0, // общие тесты метрик — каждая точка; прореживание — отдельным тестом
  releasesDir,
  publicKey: "UFVCS0VZ",
});
const server: Server = createServer(async (req, res) => {
  if (!(await agents.handle(req, res))) res.writeHead(404).end();
});
agents.attach(server);
let base = "";

before(async () => {
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});
after(() => {
  agents.close();
  server.closeAllConnections();
  server.close();
});

async function enroll(name: string, token = "it"): Promise<{ auth: string; agentId: string }> {
  const res = await fetch(base + ENROLL_PATH, { method: "POST", body: JSON.stringify({ token, name }) });
  assert.equal(res.status, 201);
  const { agentId, secret } = await res.json();
  return { auth: `Agent ${agentId}.${secret}`, agentId };
}

const hello = (name: string, extra: Record<string, unknown> = {}): Envelope => ({
  type: "hello",
  data: {
    versions: [1],
    agent: { name, version: "t", bootId: "b1", startedAt: Date.now() },
    host: { hostname: name, os: "linux", arch: "amd64" },
    capabilities: {},
    jobs: [],
    ...extra,
  },
});

const status = (slots: Record<string, number>, jobs: unknown[] = []) => ({
  state: "idle",
  slots,
  jobs,
  workers: [],
  outbox: 0,
});

/** Агент по WebSocket: входящие копятся в inbox. */
async function connect(auth: string, name: string, helloExtra: Record<string, unknown> = {}) {
  const ws = new WebSocket(base.replace("http", "ws") + LINK_PATH, WS_CHANNEL, { headers: { Authorization: auth } });
  const inbox = new Inbox();
  let closeCode = 0;
  ws.on("message", (raw) => inbox.push(JSON.parse(raw.toString())));
  ws.on("close", (code) => (closeCode = code));
  await new Promise((r, j) => ws.once("open", r).once("error", j));
  let seq = 0;
  let rid = 0;
  const a = {
    ws,
    inbox,
    get closeCode() {
      return closeCode;
    },
    send: (env: Envelope) => ws.send(JSON.stringify(env)),
    stream: (type: string, data: unknown) => ws.send(JSON.stringify({ type, seq: ++seq, data })),
    /** Надёжное: ждёт ack с id. */
    async reliable(type: string, data: unknown) {
      const id = `${name}-r${++rid}`;
      ws.send(JSON.stringify({ type, id, data }));
      return inbox.waitFor(
        (e) => (e.type === "ack" && e.data.ids?.includes(id)) || (e.type === "error" && e.re === id),
        type,
      );
    },
  };
  a.send(hello(name, helloExtra));
  await inbox.wait("welcome");
  return a;
}

/** Отдельный HTTP-сервер со своим Agents на время fn. */
async function withAgents(opts: AgentsOptions, fn: (base: string, a: Agents) => Promise<void>): Promise<void> {
  const own = new Agents(opts);
  const srv = createServer(async (req, res) => {
    if (!(await own.handle(req, res))) res.writeHead(404).end();
  });
  own.attach(srv);
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r));
  try {
    await fn(`http://127.0.0.1:${(srv.address() as AddressInfo).port}`, own);
  } finally {
    own.close();
    srv.closeAllConnections();
    srv.close();
  }
}

async function eventually<T>(what: string, probe: () => Promise<T | undefined | false>, timeoutMs = 3000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const v = await probe();
    if (v) return v;
    if (Date.now() > deadline) throw new Error(`не дождались: ${what}`);
    await sleep(20);
  }
}

test("регистрация: свой enroll даёт метки, неверный токен — 401; отказ до upgrade: 401, 426", async () => {
  const { agentId } = await enroll("gpu-agent", "gpu");
  assert.deepEqual((await agents.getAgent(agentId))?.labels, { gpu: "1" });
  assert.equal("secretHash" in (await agents.getAgent(agentId))!, false, "секрет наружу не отдаётся");
  const bad = await fetch(base + ENROLL_PATH, { method: "POST", body: JSON.stringify({ token: "no", name: "x" }) });
  assert.equal(bad.status, 401);
  assert.equal((await bad.json()).code, "AGENT_ENROLLMENT_TOKEN_INVALID");

  const code = (ws: WebSocket) =>
    new Promise<number>((r) => ws.on("unexpected-response", (_req, res) => r(res.statusCode ?? 0)));
  assert.equal(
    await code(new WebSocket(base.replace("http", "ws") + LINK_PATH, { headers: { Authorization: "Agent x.y" } })),
    426,
  );
  assert.equal(
    await code(
      new WebSocket(base.replace("http", "ws") + LINK_PATH, WS_CHANNEL, { headers: { Authorization: "Agent x.y" } }),
    ),
    401,
  );
});

test("метки следуют за hello, выданные при регистрации — главнее", async () => {
  const { auth, agentId } = await enroll("labels-agent", "gpu");
  const changes: Change[] = [];
  const onChange = (c: Change) => changes.push(c);
  agents.on("change", onChange);
  const a = await connect(auth, "labels-agent", { labels: { zone: "eu", gpu: "0" } });
  assert.deepEqual((await agents.getAgent(agentId))?.labels, { zone: "eu", gpu: "1" }, "выданную не переписать");
  assert.ok(changes.some((c) => c.kind === "agent" && c.id === agentId));
  assert.equal("grantedLabels" in (await agents.getAgent(agentId))!, false, "служебное наружу не отдаётся");
  a.ws.close();
  const b = await connect(auth, "labels-agent", { labels: { rack: "r1" } });
  assert.deepEqual((await agents.getAgent(agentId))?.labels, { rack: "r1", gpu: "1" });
  b.ws.close();
  agents.off("change", onChange);

  // Без выданных меток — метки из hello.
  assert.deepEqual(helloLabels({}, { nodeId: "n2", rack: "r1" }), { nodeId: "n2", rack: "r1" });
});

test("WebSocket: задача по слоту, capabilities → state.put, команда, событие, ack потока и надёжных", async () => {
  const { auth, agentId } = await enroll("ws-agent");
  const a = await connect(auth, "ws-agent");
  assert.equal((await agents.getAgent(agentId))?.online, true);
  const changes: Change[] = [];
  const onChange = (c: Change) => changes.push(c);
  agents.on("change", onChange);

  const job = await agents.enqueue({ queue: "ws.echo", data: { text: "hi" }, agentId });
  a.stream("status", status({ "ws.echo": 1 }));
  const assign = await a.inbox.wait("job.assign");
  assert.equal(assign.data.jobId, job.id);
  assert.ok(await a.inbox.wait("ack", (e) => e.data.seq === 1));
  a.stream("job.accept", { jobId: job.id, attempt: 0 });
  await a.reliable("job.event", { jobId: job.id, attempt: 0, seq: 1, type: "epoch", data: { loss: 1 } });
  await a.reliable("job.event", { jobId: job.id, attempt: 0, seq: 1, type: "epoch", data: { loss: 1 } }); // повтор
  await a.reliable("job.complete", { jobId: job.id, attempt: 0, result: { echo: "hi" } });
  const done = (await agents.getJob(job.id))!;
  assert.equal(done.status, "completed");
  assert.equal(done.accepted, true);
  assert.equal(done.events.length, 1, "повтор события отброшен по seq");
  const again = await a.reliable("job.complete", { jobId: job.id, attempt: 0, result: { echo: "hi" } });
  assert.equal(again.type, "ack", "повтор итога подтверждается");

  const st = await agents.setState("ws.kv", { k: "v" });
  a.stream("capabilities", { state: { domains: { "ws.kv": null } }, commands: { names: ["ws.kv.get"] } });
  const put = await a.inbox.wait("state.put");
  assert.deepEqual(put.data, { domain: "ws.kv", version: st.version, spec: { k: "v" } });
  const beforeApplied = changes.length;
  await a.reliable("state.applied", { domain: "ws.kv", version: st.version, ok: true, report: { keys: 1 } });
  assert.equal((await agents.getAgent(agentId))?.stateApplied["ws.kv"]?.version, st.version);
  await eventually("state.applied — change agent и state (id — раздел)", async () => {
    const after = new Set(changes.slice(beforeApplied).map((c) => `${c.kind}:${c.id}`));
    return after.has(`agent:${agentId}`) && after.has("state:ws.kv");
  });

  const call = agents.call({ name: "ws.kv.get", args: { key: "k" } });
  const run = await a.inbox.wait("cmd.run");
  a.stream("cmd.accept", { commandId: run.data.commandId });
  a.stream("cmd.output", { commandId: run.data.commandId, chunk: "читаю\n" });
  await a.reliable("cmd.done", { commandId: run.data.commandId, ok: true, result: { value: "v" }, exitCode: 0 });
  const cmd = await call;
  assert.equal(cmd.status, "succeeded");
  assert.equal(cmd.exitCode, 0, "exitCode из cmd.done");
  assert.equal(cmd.output, "читаю\n");
  assert.deepEqual(cmd.result, { value: "v" });

  await a.reliable("event", { source: "kv", type: "kv.applied" });
  const dup = { type: "event", id: "same-id", data: { source: "kv", type: "kv.dup" } };
  a.send(dup);
  a.send(dup);
  await a.inbox.wait("ack", (e) => e.data.ids?.includes("same-id"));
  await sleep(50);
  const events = await agents.listEvents();
  assert.equal(events.filter((e) => e.type === "kv.dup").length, 1, "повтор события по id отброшен");
  assert.deepEqual(
    events.slice(0, 2).map((e) => e.type),
    ["kv.dup", "kv.applied"],
    "новые первыми",
  );
  assert.deepEqual(
    (await agents.listEvents(1)).map((e) => e.type),
    ["kv.dup"],
  );
  assert.ok(
    changes.some((c) => c.kind === "event" && c.id === "same-id"),
    "change event — id сообщения",
  );

  const unknown = { type: "x.unknown", id: "u1", data: {} };
  a.send(unknown);
  assert.equal((await a.inbox.wait("error", (e) => e.re === "u1")).data.code, "UNKNOWN_TYPE");
  assert.ok(
    changes.some((c) => c.kind === "job") &&
      changes.some((c) => c.kind === "command") &&
      changes.some((c) => c.kind === "event"),
  );
  agents.off("change", onChange);
  a.ws.close();
  await eventually("агент без связи", async () => !(await agents.getAgent(agentId))?.online);
});

test("HTTP sync: hello → welcome, long-poll будится доставкой, ушедший клиент не забирает доставки, 409", async () => {
  const { auth, agentId } = await enroll("http-agent");
  const sync = async (body: unknown, signal?: AbortSignal) => {
    const res = await fetch(base + SYNC_PATH, {
      method: "POST",
      headers: { Authorization: auth },
      body: JSON.stringify(body),
      signal,
    });
    return { status: res.status, body: await res.json() };
  };
  const first = await sync({ sessionId: null, messages: [hello("http-agent")], waitSeconds: 0 });
  assert.equal(first.status, 200);
  assert.equal(first.body.messages[0].type, "welcome");
  const sessionId = first.body.sessionId;
  assert.equal((await agents.getAgent(agentId))?.transport, "http");

  const caps = await sync({
    sessionId,
    messages: [{ type: "capabilities", seq: 1, data: { state: { domains: { "http.dom": null } } } }],
    waitSeconds: 0,
  });
  assert.deepEqual(
    caps.body.messages.map((m: Envelope) => m.type),
    ["ack"],
  );
  const poll = sync({ sessionId, messages: [], waitSeconds: 5 });
  setTimeout(() => void agents.setState("http.dom", { on: true }), 100);
  const woke = await poll;
  assert.deepEqual(
    woke.body.messages.map((m: Envelope) => m.type),
    ["state.put"],
  );

  // Клиент ушёл посреди long-poll — доставка остаётся для следующего запроса.
  const ac = new AbortController();
  const gone = sync({ sessionId, messages: [], waitSeconds: 5 }, ac.signal).catch(() => "aborted");
  await sleep(50);
  ac.abort();
  assert.equal(await gone, "aborted");
  await sleep(20);
  const st = await agents.setState("http.dom", { on: false });
  await sleep(20);
  const next = await sync({ sessionId, messages: [], waitSeconds: 0 });
  assert.deepEqual(
    next.body.messages.map((m: Envelope) => m.data?.version),
    [st.version],
  );

  assert.equal((await sync({ sessionId: "nope", messages: [], waitSeconds: 0 })).body.code, "AGENT_SESSION_REPLACED");
  assert.equal((await sync({ sessionId: null, messages: [], waitSeconds: 0 })).status, 400);
});

test("раздача: сначала менее загруженным; закреплённая — только своему агенту", async () => {
  const one = await enroll("load-1");
  const two = await enroll("load-2");
  const a = await connect(one.auth, "load-1");
  const b = await connect(two.auth, "load-2");
  // a занят одной задачей, у обоих по два свободных слота.
  a.stream("status", status({ "load.q": 2 }, [{ jobId: "other", attempt: 0, queue: "load.q" }]));
  b.stream("status", status({ "load.q": 2 }));
  await a.inbox.wait("ack");
  await b.inbox.wait("ack");
  const job = await agents.enqueue({ queue: "load.q" });
  assert.equal((await b.inbox.wait("job.assign")).data.jobId, job.id, "менее загруженному");

  const pinned = await agents.enqueue({ queue: "load.q", agentId: one.agentId });
  assert.equal((await a.inbox.wait("job.assign", (e) => e.data.jobId === pinned.id)).data.jobId, pinned.id);
  assert.equal((await agents.getJob(pinned.id))?.pinnedAgentId, one.agentId);
  assert.equal(
    b.inbox.all("job.assign").some((e) => e.data.jobId === pinned.id),
    false,
  );

  await assert.rejects(agents.enqueue({ queue: "load.q", agentId: "nope" }), /не найден/);
  a.ws.close();
  b.ws.close();
});

test("аренда истекла — LEASE_EXPIRED и повтор; job.fail с повтором; отмена и остановка", async () => {
  const { auth, agentId } = await enroll("lease-agent");
  const a = await connect(auth, "lease-agent");
  a.stream("status", status({ "lease.q": 1 }));
  await a.inbox.wait("ack");
  const job = await agents.enqueue({ queue: "lease.q", maxAttempts: 3, agentId });
  await a.inbox.wait("job.assign", (e) => e.data.jobId === job.id);
  // Аренда истекает (агент «пропал»): сдвинуть срок в прошлое.
  const rec = (await agents.store.getJob(job.id))!;
  await agents.store.updateJob({ ...rec, leaseUntil: Date.now() - 1 });
  await eventually("повтор после LEASE_EXPIRED", async () => {
    const j = await agents.getJob(job.id);
    return j?.attempt === 1 && j.error?.code === "LEASE_EXPIRED";
  });
  a.stream("status", status({ "lease.q": 1 }));
  const second = await a.inbox.wait("job.assign", (e) => e.data.jobId === job.id && e.data.attempt === 1);
  const lost = await a.reliable("job.complete", { jobId: job.id, attempt: 0, result: {} });
  assert.equal(lost.data.code, "JOB_LEASE_LOST", "итог старой попытки отклонён");
  await a.reliable("job.fail", {
    jobId: job.id,
    attempt: second.data.attempt,
    code: "FLAKY",
    message: "ещё раз",
    retryable: true,
  });
  assert.equal((await agents.getJob(job.id))?.attempt, 2);

  a.stream("status", status({ "lease.q": 1 }));
  await a.inbox.wait("job.assign", (e) => e.data.jobId === job.id && e.data.attempt === 2);
  await agents.stopJob(job.id);
  assert.ok(await a.inbox.wait("job.stop", (e) => e.data.jobId === job.id));
  assert.equal((await agents.getJob(job.id))?.stopRequested, true);
  await agents.cancelJob(job.id);
  assert.ok(await a.inbox.wait("job.cancel", (e) => e.data.jobId === job.id));
  assert.equal((await agents.getJob(job.id))?.status, "cancelled");
  await assert.rejects(agents.cancelJob(job.id), /завершена/);
  a.ws.close();
});

test("сверка при hello: невыданная — повтор assign, принятая и не названная — AGENT_LOST, чужая — job.cancel", async () => {
  const { auth, agentId } = await enroll("rec-agent");
  let a = await connect(auth, "rec-agent");
  a.stream("status", status({ "rec.q": 2 }));
  const j1 = await agents.enqueue({ queue: "rec.q", agentId, maxAttempts: 2 });
  const j2 = await agents.enqueue({ queue: "rec.q", agentId });
  await a.inbox.wait("job.assign", (e) => e.data.jobId === j2.id);
  a.stream("job.accept", { jobId: j1.id, attempt: 0 });
  await a.inbox.wait("ack", (e) => e.data.seq === 2);
  a.ws.close();
  await eventually("без связи", async () => !(await agents.getAgent(agentId))?.online);

  a = await connect(auth, "rec-agent", { jobs: [{ jobId: "ghost", attempt: 0 }] });
  const reassign = await a.inbox.wait("job.assign");
  assert.equal(reassign.data.jobId, j2.id, "не подтверждённая — выдаётся заново");
  assert.equal((await a.inbox.wait("job.cancel")).data.jobId, "ghost");
  const lost = (await agents.getJob(j1.id))!;
  assert.equal(lost.error?.code, "AGENT_LOST");
  assert.equal(lost.attempt, 1);
  a.ws.close();
});

test("команды: ожидающая доставляется при подключении, нет агента — COMMAND_NOT_SUPPORTED, срок на сервере — TIMEOUT", async () => {
  await assert.rejects(agents.command({ name: "nobody.has" }), { code: "COMMAND_NOT_SUPPORTED" });
  const { auth, agentId } = await enroll("cmd-agent");
  const pending = await agents.command({ agentId, name: "cmd.x", timeoutSec: 1 });
  assert.equal(pending.status, "pending");
  const a = await connect(auth, "cmd-agent", { capabilities: { commands: { names: ["cmd.x"] } } });
  assert.equal((await a.inbox.wait("cmd.run")).data.commandId, pending.id);
  const timedOut = await eventually("TIMEOUT", async () => {
    const c = await agents.getCommand(pending.id);
    return c?.status === "failed" && c;
  });
  assert.equal(timedOut.error?.code, "TIMEOUT");
  a.ws.close();
});

test("состояние: общее и для агента (важнее), версия монотонна", async () => {
  const one = await enroll("st-1");
  const two = await enroll("st-2");
  const caps = { capabilities: { state: { domains: { "st.dom": null } } } };
  const a = await connect(one.auth, "st-1", caps);
  const b = await connect(two.auth, "st-2", caps);
  const common = await agents.setState("st.dom", { who: "all" });
  assert.equal((await a.inbox.wait("state.put")).data.spec.who, "all");
  assert.equal((await b.inbox.wait("state.put")).data.spec.who, "all");
  const own = await agents.setState("st.dom", { who: "one" }, { agentId: one.agentId });
  assert.ok(own.version > common.version);
  assert.equal(own.agentId, one.agentId);
  assert.equal((await a.inbox.wait("state.put", (e) => e.data.spec.who === "one")).data.version, own.version);
  const common2 = await agents.setState("st.dom", { who: "all-2" });
  assert.ok(common2.version > own.version);
  await b.inbox.wait("state.put", (e) => e.data.spec.who === "all-2");
  await sleep(50);
  assert.equal(
    a.inbox.all("state.put").some((e) => e.data.spec.who === "all-2"),
    false,
    "у агента свой снимок",
  );
  assert.equal((await agents.listStates()).filter((s) => s.domain === "st.dom").length, 2);
  a.ws.close();
  b.ws.close();
});

test("deleteState: личный → общий с новой версией; повтор и без общего — ничего; общий удаляется, личные остаются", async () => {
  const one = await enroll("del-1");
  const two = await enroll("del-2");
  const caps = { capabilities: { state: { domains: { "del.dom": null, "del.solo": null } } } };
  const a = await connect(one.auth, "del-1", caps);
  const b = await connect(two.auth, "del-2", caps);
  await assert.rejects(agents.deleteState(""), { code: "MESSAGE_INVALID" });
  const deleted: unknown[] = [];
  const onDeleted = (d: unknown) => deleted.push(d);
  agents.on("stateDeleted", onDeleted);
  const stateChanges: unknown[] = [];
  const onChange = (c: Change) => c.kind === "state" && stateChanges.push(c.id);
  agents.on("change", onChange);

  // Личный снимок новее общего; удаление личного — агент получает общий с версией больше личной.
  await agents.setState("del.dom", { who: "all" });
  await a.inbox.wait("state.put", (e) => e.data.domain === "del.dom" && e.data.spec.who === "all");
  const own = await agents.setState("del.dom", { who: "one" }, { agentId: one.agentId });
  await a.inbox.wait("state.put", (e) => e.data.spec.who === "one");
  const back = await agents.deleteState("del.dom", { agentId: one.agentId });
  assert.ok(back && back.version > own.version && back.agentId === undefined);
  assert.deepEqual(back.spec, { who: "all" });
  const got = await a.inbox.wait("state.put", (e) => e.data.domain === "del.dom" && e.data.version === back.version);
  assert.equal(got.data.spec.who, "all");
  await b.inbox.wait("state.put", (e) => e.data.domain === "del.dom" && e.data.version === back.version);
  assert.equal(await agents.store.getState("del.dom", one.agentId), undefined);
  assert.deepEqual(deleted, [{ domain: "del.dom", agentId: one.agentId }]);

  // Повторное удаление личного (его уже нет) — текущий общий без новой версии, ничего не шлётся.
  await sleep(50);
  const beforeRepeat = [a.inbox.all("state.put").length, b.inbox.all("state.put").length];
  const changesBefore = stateChanges.length;
  const again = await agents.deleteState("del.dom", { agentId: one.agentId });
  assert.equal(again?.version, back.version, "общий не переиздаётся");
  assert.deepEqual(again?.spec, { who: "all" });
  await sleep(50);
  assert.deepEqual([a.inbox.all("state.put").length, b.inbox.all("state.put").length], beforeRepeat);
  assert.equal(stateChanges.length, changesBefore, "нет change state");
  assert.equal(deleted.length, 1, "нет stateDeleted");

  // Общего нет — агенту ничего не отправляется.
  await agents.setState("del.solo", { n: 1 }, { agentId: one.agentId });
  await a.inbox.wait("state.put", (e) => e.data.domain === "del.solo");
  const before = a.inbox.all("state.put").length;
  assert.equal(await agents.deleteState("del.solo", { agentId: one.agentId }), null);
  assert.equal(await agents.deleteState("del.solo", { agentId: one.agentId }), null, "снимка не было — не ошибка");
  assert.equal(deleted.length, 2, "повтор без снимка — нет stateDeleted");

  // Без agentId — удаляется общий, личные остаются, агентам ничего.
  await agents.setState("del.dom", { who: "two" }, { agentId: two.agentId });
  await b.inbox.wait("state.put", (e) => e.data.spec.who === "two");
  const beforeB = b.inbox.all("state.put").length;
  assert.equal(await agents.deleteState("del.dom"), null);
  assert.equal(await agents.store.getState("del.dom"), undefined);
  assert.deepEqual((await agents.store.getState("del.dom", two.agentId))?.spec, { who: "two" });
  await sleep(50);
  assert.equal(a.inbox.all("state.put").length, before);
  assert.equal(b.inbox.all("state.put").length, beforeB);
  assert.deepStrictEqual(deleted.slice(2), [{ domain: "del.dom" }], "у общего — без agentId");
  const changesShared = stateChanges.length;
  assert.equal(await agents.deleteState("del.dom"), null, "общего уже нет — не ошибка");
  assert.equal(deleted.length, 3);
  assert.equal(stateChanges.length, changesShared);
  agents.off("stateDeleted", onDeleted);
  agents.off("change", onChange);
  a.ws.close();
  b.ws.close();
});

test("версия после «перезапуска» новее применённой агентом (не меньше now_ms)", async () => {
  const appliedBefore = Date.now() - 60_000;
  const store = new MemoryStore();
  const st = await store.setState("x", undefined, {});
  assert.ok(st.version > appliedBefore);
  assert.ok((await store.setState("x", undefined, {})).version > st.version);
  assert.ok((await store.setState("x", "agent", {})).version > st.version, "монотонна в пределах домена");
  assert.equal(await store.deleteState("x", "agent"), true);
  assert.equal(await store.deleteState("x", "agent"), false);
  assert.ok((await store.setState("x", "agent", {})).version > st.version + 2, "удаление не сбрасывает версию");
});

test("вытеснение: новая сессия того же агента закрывает прежнюю кодом 4410", async () => {
  const { auth, agentId } = await enroll("dup-agent");
  const a = await connect(auth, "dup-agent");
  const b = await connect(auth, "dup-agent");
  await eventually("4410", async () => a.closeCode === Close.Replaced);
  await sleep(50);
  assert.equal((await agents.getAgent(agentId))?.online, true, "новая сессия на связи");
  b.ws.close();
});

test("файлы: job.urls по запросу агента, PUT/GET через agents.handle (MemoryFiles)", async () => {
  const { auth, agentId } = await enroll("files-agent");
  const a = await connect(auth, "files-agent");
  a.stream("status", status({ "files.q": 1 }));
  const job = await agents.enqueue({ queue: "files.q", agentId, inputs: { src: "вход" }, outputs: ["out"] });
  const assign = await a.inbox.wait("job.assign", (e) => e.data.jobId === job.id);
  assert.equal(await (await fetch(assign.data.inputs.src)).text(), "вход");
  a.send({ type: "job.urls", id: "q1", data: { jobId: job.id, attempt: 0, outputs: ["out"] } });
  const urls = await a.inbox.wait("job.urls", (e) => e.re === "q1");
  assert.deepEqual(Object.keys(urls.data.inputs), []);
  assert.ok(urls.data.expiresAt > Date.now());
  const put = await fetch(urls.data.outputs.out.url, { method: "PUT", body: "выход" });
  assert.equal(put.status, 200);
  assert.deepEqual(files.outputs(job.id), ["out"]);
  assert.equal(files.get(job.id, "out", "out")?.toString(), "выход");
  a.ws.close();
});

test("асинхронный Store: сообщения сессии обрабатываются строго по порядку", async () => {
  // Store с задержками: без сериализации чтение-изменение-запись теряло бы обновления.
  class SlowStore extends MemoryStore {
    override async getCommand(id: string) {
      await sleep(Math.random() * 5);
      return super.getCommand(id);
    }
    override async updateCommand(cmd: Parameters<MemoryStore["updateCommand"]>[0]) {
      await sleep(Math.random() * 5);
      return super.updateCommand(cmd);
    }
  }
  const slowAgents = new Agents({ enrollToken: "s", store: new SlowStore() });
  const srv = createServer(async (req, res) => {
    if (!(await slowAgents.handle(req, res))) res.writeHead(404).end();
  });
  slowAgents.attach(srv);
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r));
  const url = `http://127.0.0.1:${(srv.address() as AddressInfo).port}`;
  const { agentId, secret } = await (
    await fetch(url + ENROLL_PATH, { method: "POST", body: JSON.stringify({ token: "s", name: "slow" }) })
  ).json();
  const ws = new WebSocket(url.replace("http", "ws") + LINK_PATH, WS_CHANNEL, {
    headers: { Authorization: `Agent ${agentId}.${secret}` },
  });
  const inbox = new Inbox();
  ws.on("message", (raw) => inbox.push(JSON.parse(raw.toString())));
  await new Promise((r) => ws.once("open", r));
  ws.send(JSON.stringify(hello("slow")));
  await inbox.wait("welcome");
  const cmd = await slowAgents.command({ agentId, name: "slow.cmd" });
  let seq = 0;
  for (let i = 0; i < 30; i++)
    ws.send(JSON.stringify({ type: "cmd.output", seq: ++seq, data: { commandId: cmd.id, chunk: `${i},` } }));
  ws.send(JSON.stringify({ type: "cmd.done", id: "d", data: { commandId: cmd.id, ok: true } }));
  await inbox.wait("ack", (e) => e.data.ids?.includes("d"));
  const got = (await slowAgents.getCommand(cmd.id))!;
  assert.equal(got.output, Array.from({ length: 30 }, (_, i) => `${i},`).join(""));
  assert.equal(got.status, "succeeded");
  ws.close();
  slowAgents.close();
  srv.closeAllConnections();
  srv.close();
});

test("метрики: история с поправкой времени, backfill — в истории, но не в agent.metrics; inventory", async () => {
  const { auth, agentId } = await enroll("metrics-agent");
  const a = await connect(auth, "metrics-agent");
  const host = { cpuPercent: 10, interfaces: [{ name: "eth0", rxBps: 1, txBps: 2 }] };
  // Часы агента на час впереди: время точки — collectedAt + clockOffsetMs.
  const skew = 3600_000;
  const t0 = Date.now();
  a.stream("metrics", { collectedAt: t0 + skew - 50, clockOffsetMs: -skew, host });
  await a.inbox.wait("ack", (e) => e.data.seq === 1);
  a.stream("metrics", {
    collectedAt: t0 + skew - 60_000,
    clockOffsetMs: -skew,
    backfill: true,
    host: { cpuPercent: 99 },
  });
  await a.inbox.wait("ack", (e) => e.data.seq === 2);
  const inv = { collectedAt: t0, os: { hostname: "h", virtualization: "kvm" }, ports: { tcp: [22] } };
  a.stream("inventory", inv);
  await a.inbox.wait("ack", (e) => e.data.seq === 3);

  const points = await agents.listMetrics(agentId);
  assert.equal(points.length, 2);
  const [back, live] = points;
  assert.equal(back.backfill, true, "backfill старше — первым");
  assert.equal(live.backfill, false);
  assert.ok(Math.abs(live.at - (t0 - 50)) < 1000, `at живой точки: ${live.at - t0}`);
  assert.ok(Math.abs(back.at - (t0 - 60_000)) < 1000, `at backfill: ${back.at - t0}`);
  assert.deepEqual(live.metrics.host?.interfaces, host.interfaces);
  const agent = (await agents.getAgent(agentId))!;
  assert.equal(agent.metrics?.host?.cpuPercent, 10, "agent.metrics — последняя не-backfill");
  assert.deepEqual(agent.inventory, inv);
  assert.deepEqual(
    (await agents.listMetrics(agentId, { since: back.at })).map((p) => p.at),
    [live.at],
    "since — строго позже",
  );
  a.ws.close();
});

test("метрики: время точки — collectedAt + clockOffsetMs (и для переотправленной); событие metrics", async () => {
  const { auth, agentId } = await enroll("offset-agent");
  const a = await connect(auth, "offset-agent");
  const seen: { agentId: string; at: number; backfill: boolean; stored: boolean }[] = [];
  const onMetrics = (id: string, p: { at: number; backfill: boolean }) => {
    if (id !== agentId) return;
    // Событие — после сохранения: точка уже в истории.
    void agents
      .listMetrics(agentId)
      .then((all) =>
        seen.push({ agentId: id, at: p.at, backfill: p.backfill, stored: all.some((x) => x.at === p.at) }),
      );
  };
  agents.on("metrics", onMetrics);
  // Часы агента на час впереди, смещение известно по welcome.
  const skew = 3600_000;
  const t0 = Date.now();
  a.stream("metrics", { collectedAt: t0 + skew - 1000, clockOffsetMs: -skew });
  // Переотправлена после обрыва (собрана 10 мин назад) — время точки по смещению.
  a.stream("metrics", {
    collectedAt: t0 + skew - 600_000,
    clockOffsetMs: -skew,
    backfill: true,
  });
  // Без смещения — момент получения.
  a.stream("metrics", { collectedAt: 1 });
  await a.inbox.wait("ack", (e) => e.data.seq === 3);
  await eventually("события metrics", async () => seen.length === 3);
  agents.off("metrics", onMetrics);
  const pts = await agents.listMetrics(agentId);
  assert.deepEqual(
    pts.slice(0, 2).map((p) => p.at),
    [t0 - 600_000, t0 - 1000],
  );
  assert.ok(Math.abs(pts[2].at - Date.now()) < 1000, `без смещения — получено: ${pts[2].at - Date.now()}`);
  assert.deepEqual(
    seen.map((x) => [x.backfill, x.stored]),
    [
      [false, true],
      [true, true],
      [false, true],
    ],
  );
  assert.equal(seen[1].at, t0 - 600_000, "backfill — тоже событием");
  a.ws.close();
});

test("MemoryFiles: загрузка выходного файла — событие job с задачей", async () => {
  const job = await agents.enqueue({ queue: "upload.q", outputs: ["out"] });
  const got = new Promise<string>((r) => {
    const on = (j: { id: string }) => j.id === job.id && (agents.off("job", on), r(j.id));
    agents.on("job", on);
  });
  files.put(job.id, "out", "out", Buffer.from("x"));
  assert.equal(await got, job.id);
  await agents.cancelJob(job.id);
});

test("listJobs, listCommands — новые первыми; раздача задач и доставка команд — старые первыми", async () => {
  const { auth, agentId } = await enroll("order-agent");
  const jobIds: string[] = [];
  const cmdIds: string[] = [];
  for (let i = 0; i < 3; i++) {
    jobIds.push((await agents.enqueue({ queue: "order.q" })).id);
    cmdIds.push((await agents.command({ agentId, name: "order.x" })).id);
  }
  assert.deepEqual(
    (await agents.listJobs({ queue: "order.q" })).map((j) => j.id),
    [...jobIds].reverse(),
  );
  assert.deepEqual(
    (await agents.listCommands({ agentId })).map((c) => c.id),
    [...cmdIds].reverse(),
  );
  const a = await connect(auth, "order-agent", { capabilities: { commands: { names: ["order.x"] } } });
  await eventually("cmd.run", async () => a.inbox.all("cmd.run").length >= 3);
  assert.deepEqual(
    a.inbox.all("cmd.run").map((e) => e.data.commandId),
    cmdIds,
  );
  a.stream("status", status({ "order.q": 1 }));
  assert.equal((await a.inbox.wait("job.assign")).data.jobId, jobIds[0]);
  a.ws.close();
});

test("MemoryStore: пределы по умолчанию одинаковые во всех SDK", () => {
  const s = new MemoryStore() as unknown as Record<string, number>;
  assert.deepEqual(
    [s.keepJobs, s.keepCommands, s.keepEvents, s.keepMetrics, s.keepStateHistory],
    [1000, 500, 1000, 4320, 50],
  );
});

test("MemoryStore: последние keepMetrics точек на агента, по возрастанию at", async () => {
  const store = new MemoryStore({ keepMetrics: 3 });
  for (const at of [5, 1, 4, 2, 3]) await store.addMetrics("a", { at, backfill: false, metrics: { collectedAt: at } });
  assert.deepEqual(
    (await store.listMetrics("a")).map((p) => p.at),
    [3, 4, 5],
  );
  assert.deepEqual(await store.listMetrics("b"), []);
  assert.equal(new MemoryStore().constructor.name, "MemoryStore");
});

test("subscribe: config.subscription, продление без повтора, welcome с подпиской, истечение — {}", async () => {
  const { auth, agentId } = await enroll("sub-agent");
  let a = await connect(auth, "sub-agent");
  assert.equal(a.inbox.got[0].data.config.subscription, undefined, "без подписок — нет поля");
  assert.equal(a.inbox.got[0].data.config.metricsIntervalMs, 15_000);
  const opts = {
    ttlMs: 400,
    metrics: { intervalMs: 1000, groups: ["sockets", "diskio", "sockets"] },
    logs: { level: "info" as const },
  };
  const { id, until } = await agents.subscribe(agentId, opts);
  assert.ok(id && until > Date.now());
  assert.deepEqual(
    (await agents.getAgent(agentId))?.subscriptions,
    [{ id, until, metrics: { intervalMs: 1000, groups: ["sockets", "diskio"] }, logs: { level: "info" } }],
    "в записи агента",
  );
  const want = { metricsIntervalMs: 1000, metrics: ["sockets", "diskio"], logLevel: "info" };
  assert.deepEqual((await a.inbox.wait("config")).data, { subscription: want });
  await sleep(150);
  const again = await agents.subscribe(agentId, { ...opts, id }); // продление
  assert.equal(again.id, id);
  assert.ok(again.until > until);
  await sleep(100);
  assert.equal(a.inbox.all("config").length, 1, "продление без повторного config");

  a.ws.close();
  await sleep(30);
  a = await connect(auth, "sub-agent");
  assert.deepEqual(a.inbox.got[0].data.config.subscription, want, "welcome — со сводной");
  const back = await a.inbox.wait("config", () => true, 2000);
  assert.deepEqual(back.data, { subscription: {} }, "срок истёк — подписок нет");
  assert.equal((await agents.getAgent(agentId))?.subscriptions, undefined, "истёкшая из записи убрана");
  a.ws.close();
});

test("subscribe: сводная — минимум, объединение, самый подробный уровень; замена по id; unsubscribe", async () => {
  const { auth, agentId } = await enroll("sub-many");
  const a = await connect(auth, "sub-many");
  const configs = () => a.inbox.all("config").map((e) => e.data.subscription);
  const s1 = await agents.subscribe(agentId, {
    status: { intervalMs: 2000 },
    metrics: { intervalMs: 1000, groups: ["sockets"] },
    logs: { level: "warn" },
    channels: { "example.app": { intervalMs: 1000 } },
  });
  await agents.subscribe(agentId, {
    id: "ui",
    status: { intervalMs: 500 },
    metrics: { groups: ["diskio", "sockets"] },
    logs: { level: "debug" },
    channels: { "example.app": { intervalMs: 300 }, "example.db": { intervalMs: 2000 } },
  });
  await eventually("две сводные", async () => configs().length === 2);
  assert.deepEqual(configs()[1], {
    statusIntervalMs: 500,
    metricsIntervalMs: 1000,
    metrics: ["sockets", "diskio"],
    logLevel: "debug",
    channels: { "example.app": 300, "example.db": 2000 },
  });
  // Тот же id — содержимое заменяется.
  await agents.subscribe(agentId, { id: "ui", logs: { level: "error" } });
  await eventually("замена", async () => configs().length === 3);
  assert.deepEqual(configs()[2], {
    statusIntervalMs: 2000,
    metricsIntervalMs: 1000,
    metrics: ["sockets"],
    logLevel: "warn",
    channels: { "example.app": 1000 },
  });
  assert.equal((await agents.getAgent(agentId))?.subscriptions?.length, 2);
  await agents.unsubscribe(agentId, s1.id);
  await eventually("без s1", async () => configs().length === 4);
  assert.deepEqual(configs()[3], { logLevel: "error" });
  await agents.unsubscribe(agentId, "nope"); // нет такой — не ошибка
  await agents.unsubscribe(agentId, "ui");
  await eventually("пусто", async () => configs().length === 5);
  assert.deepEqual(configs()[4], {});
  assert.equal((await agents.getAgent(agentId))?.subscriptions, undefined);

  // Проверки.
  const bad: SubscribeOptions[] = [
    { status: { intervalMs: 100 } },
    { metrics: { intervalMs: 199 } },
    { metrics: { groups: ["Sockets"] } },
    { metrics: { groups: ["disk-io"] } },
    { logs: { level: "trace" as "info" } },
    { channels: { плохо: { intervalMs: 1000 } } },
    { channels: { "example.app": { intervalMs: 50 } } },
    { id: "" },
  ];
  for (const opts of bad) await assert.rejects(agents.subscribe(agentId, opts), { code: "MESSAGE_INVALID" });
  await agents.subscribe(agentId, { ttlMs: 50, metrics: { groups: ["gpu.nvidia"] }, status: { intervalMs: 200 } });
  await assert.rejects(agents.subscribe("nope", {}), { code: "AGENT_NOT_FOUND" });
  await assert.rejects(agents.unsubscribe("nope", "x"), { code: "AGENT_NOT_FOUND" });
  a.ws.close();
});

test("subscribe: revoke удаляет подписки, отозванному — AGENT_REVOKED", async () => {
  const { agentId } = await enroll("sub-revoke");
  await agents.subscribe(agentId, { logs: { level: "debug" } });
  await agents.revoke(agentId);
  assert.equal((await agents.getAgent(agentId))?.subscriptions, undefined);
  await assert.rejects(agents.subscribe(agentId, { logs: { level: "debug" } }), { code: "AGENT_REVOKED" });
});

test("offline с отсрочкой: переподключился в срок — online не мигает, нет — online: false", async () => {
  const { auth, agentId } = await enroll("grace-agent");
  let a = await connect(auth, "grace-agent");
  const seen: boolean[] = [];
  const onAgent = (ag: { id: string; online: boolean }) => ag.id === agentId && seen.push(ag.online);
  agents.on("agent", onAgent);
  a.ws.close();
  await sleep(50);
  assert.equal((await agents.getAgent(agentId))?.online, true, "в отсрочке — ещё online");
  a = await connect(auth, "grace-agent");
  await sleep(250);
  assert.equal((await agents.getAgent(agentId))?.online, true);
  assert.equal(seen.includes(false), false, "offline не публиковался");
  a.ws.close();
  await eventually("offline после отсрочки", async () => (await agents.getAgent(agentId))?.online === false);
  agents.off("agent", onAgent);
});

test("revoke: 4401, revoked, далее 401 на WebSocket и HTTP sync", async () => {
  const { auth, agentId } = await enroll("revoke-agent");
  const a = await connect(auth, "revoke-agent");
  const agent = await agents.revoke(agentId);
  assert.equal(agent.revoked, true);
  assert.equal(agent.online, false);
  await eventually("4401", async () => a.closeCode === Close.Unauthorized);
  const code = await new Promise<number>((r) => {
    const ws = new WebSocket(base.replace("http", "ws") + LINK_PATH, WS_CHANNEL, { headers: { Authorization: auth } });
    ws.on("unexpected-response", (_req, res) => r(res.statusCode ?? 0));
    ws.on("open", () => r(101));
  });
  assert.equal(code, 401);
  const sync = await fetch(base + SYNC_PATH, {
    method: "POST",
    headers: { Authorization: auth },
    body: JSON.stringify({ sessionId: null, messages: [hello("revoke-agent")] }),
  });
  assert.equal(sync.status, 401);
  assert.equal((await agents.getAgent(agentId))?.revoked, true);
  await assert.rejects(agents.revoke("nope"), { code: "AGENT_NOT_FOUND" });
});

test("выпуск: manifest.json, сборка только из манифеста, install.sh с адресом и ключом", async () => {
  assert.deepEqual(await agents.release(), manifest);
  const m = await fetch(base + RELEASES_PATH + "/manifest.json");
  assert.equal(m.status, 200);
  assert.deepEqual(await m.json(), manifest);
  const bin = await fetch(base + RELEASES_PATH + "/agent-linux-amd64");
  assert.equal(bin.status, 200);
  assert.equal(await bin.text(), "BINARY");
  const wbin = await fetch(base + RELEASES_PATH + "/report-1.10.0-linux-amd64");
  assert.equal(wbin.status, 200, "сборка воркера из манифеста");
  assert.equal(await wbin.text(), "WORKER");
  assert.equal((await fetch(base + RELEASES_PATH + "/secret.txt")).status, 404, "не из манифеста");
  assert.equal((await fetch(base + RELEASES_PATH + "/..%2Fsecret.txt")).status, 404);
  const sh = await (await fetch(base + INSTALL_PATH)).text();
  assert.match(sh, new RegExp(`^DEFAULT_SERVER="${base}"$`, "m"));
  assert.match(sh, /^DEFAULT_PUBLIC_KEY="UFVCS0VZ"$/m);
  const forwardedHeaders = { "X-Forwarded-Proto": "https", "X-Forwarded-Host": "agents.example.com" };
  const direct = await (await fetch(base + INSTALL_PATH, { headers: forwardedHeaders })).text();
  assert.match(direct, new RegExp(`^DEFAULT_SERVER="${base}"$`, "m"), "без trustProxy X-Forwarded-* не учитываются");

  await withAgents({ enrollToken: "x", releasesDir, trustProxy: true }, async (pbase) => {
    const forwarded = await (await fetch(pbase + INSTALL_PATH, { headers: forwardedHeaders })).text();
    assert.match(forwarded, /^DEFAULT_SERVER="https:\/\/agents\.example\.com"$/m);
    // Опасный адрес — 400, а не вырезание символов.
    const bads: Record<string, string>[] = [
      { "X-Forwarded-Proto": 'https"; rm -rf /; "' },
      { "X-Forwarded-Host": "a.example.com$(id)" },
      { "X-Forwarded-Proto": "ftp" },
    ];
    for (const headers of bads) {
      const r = await fetch(pbase + INSTALL_PATH, { headers });
      assert.equal(r.status, 400, JSON.stringify(headers));
      assert.equal((await r.json()).code, "MESSAGE_INVALID");
    }
  });

  // Без releasesDir — пути выпуска не обслуживаются.
  const bare = new Agents({ enrollToken: "x" });
  assert.equal(await bare.release(), null);
  assert.deepEqual(await bare.updateCandidates(), []);
  assert.deepEqual(await bare.workerUpdateCandidates(), []);
  bare.close();
});

test("install.sh: адрес — baseUrl (с путём), publicKey не из base64 — не подставляется, предупреждение", async () => {
  const logs: string[] = [];
  const h = new Agents({
    enrollToken: "x",
    releasesDir,
    publicKey: 'K"; rm -rf / #',
    baseUrl: "https://agents.example.com/agents/",
    log: (m) => logs.push(m),
  });
  const srv = createServer(async (req, res) => {
    if (!(await h.handle(req, res))) res.writeHead(404).end();
  });
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r));
  const url = `http://127.0.0.1:${(srv.address() as AddressInfo).port}`;
  const sh = await (await fetch(url + INSTALL_PATH, { headers: { "X-Forwarded-Host": "ignored.example.com" } })).text();
  assert.match(sh, /^DEFAULT_SERVER="https:\/\/agents\.example\.com\/agents"$/m);
  assert.match(sh, /^DEFAULT_PUBLIC_KEY=""$/m);
  assert.ok(
    logs.some((m) => m.includes("publicKey")),
    logs.join("; "),
  );
  h.close();
  srv.closeAllConnections();
  srv.close();

  const bad = new Agents({ enrollToken: "x", releasesDir, baseUrl: "https://a.example.com/`id`" });
  const srv2 = createServer(async (req, res) => {
    if (!(await bad.handle(req, res))) res.writeHead(404).end();
  });
  await new Promise<void>((r) => srv2.listen(0, "127.0.0.1", r));
  assert.equal((await fetch(`http://127.0.0.1:${(srv2.address() as AddressInfo).port}${INSTALL_PATH}`)).status, 400);
  bad.close();
  srv2.closeAllConnections();
  srv2.close();
});

test("обновление: кандидаты (self, другая версия, есть сборка) и updateAgent → agent.update; иначе UPDATE_NOT_AVAILABLE", async () => {
  const selfCaps = { capabilities: { update: { mode: "self" }, commands: { names: ["agent.update"] } } };
  const old = await enroll("upd-old");
  const same = await enroll("upd-same");
  const ext = await enroll("upd-ext");
  const arm = await enroll("upd-arm");
  const a = await connect(old.auth, "upd-old", selfCaps);
  const b = await connect(same.auth, "upd-same", selfCaps);
  b.ws.close();
  const c = await connect(ext.auth, "upd-ext", { capabilities: { update: { mode: "external" } } });
  const d = await connect(arm.auth, "upd-arm", { ...selfCaps, host: { hostname: "arm", os: "linux", arch: "arm64" } });
  // upd-same уже на версии манифеста.
  const rec = (await agents.store.getAgent(same.agentId))!;
  rec.hello!.agent.version = "2.0.0";
  await agents.store.updateAgent(rec);

  const cands = await agents.updateCandidates();
  assert.deepEqual(
    cands.filter((x) => x.name.startsWith("upd-")),
    [
      {
        agentId: old.agentId,
        name: "upd-old",
        online: true,
        current: "t",
        target: "2.0.0",
        os: "linux",
        arch: "amd64",
      },
    ],
  );
  const cmd = await agents.updateAgent(old.agentId);
  assert.equal(cmd.timeoutSec, 300);
  const run = await a.inbox.wait("cmd.run", (e) => e.data.name === "agent.update");
  assert.deepEqual(run.data.args, {
    version: "2.0.0",
    url: `${RELEASES_PATH}/agent-linux-amd64`,
    sha256: manifest.artifacts[0].sha256,
    signature: "c2ln",
  });
  assert.equal(run.data.timeoutSec, 300);
  await assert.rejects(agents.updateAgent(ext.agentId), { code: "UPDATE_NOT_AVAILABLE" });
  await assert.rejects(agents.updateAgent(arm.agentId), { code: "UPDATE_NOT_AVAILABLE" });
  await assert.rejects(agents.updateAgent("nope"), { code: "AGENT_NOT_FOUND" });
  a.ws.close();
  c.ws.close();
  d.ws.close();
});

test("обновление воркеров: кандидаты (release, другая версия, есть сборка) и updateWorker → worker.update; иначе UPDATE_NOT_AVAILABLE", async () => {
  const caps = { capabilities: { update: { mode: "self" }, commands: { names: ["worker.update"] } } };
  const workers = (version: string, release = true) => [
    { name: "report", state: "running", instances: 1, version, release },
    { name: "plain", state: "running", instances: 1, version: "0.1.0" },
  ];
  const old = await enroll("wupd-old");
  const same = await enroll("wupd-same");
  const arm = await enroll("wupd-arm");
  const nocmd = await enroll("wupd-nocmd");
  const ext = await enroll("wupd-ext");
  const a = await connect(old.auth, "wupd-old", caps);
  const b = await connect(same.auth, "wupd-same", caps);
  const c = await connect(arm.auth, "wupd-arm", { ...caps, host: { hostname: "arm", os: "linux", arch: "arm64" } });
  const d = await connect(nocmd.auth, "wupd-nocmd", { capabilities: { update: { mode: "self" } } });
  // Агент в контейнере (update.mode = external) воркеры обновляет: условие — объявленный worker.update.
  const e = await connect(ext.auth, "wupd-ext", {
    capabilities: { update: { mode: "external" }, commands: { names: ["worker.update"] } },
  });
  a.stream("status", { ...status({}), workers: workers("1.2.0") });
  b.stream("status", { ...status({}), workers: workers("1.10.0") });
  c.stream("status", { ...status({}), workers: workers("1.2.0") });
  d.stream("status", { ...status({}), workers: workers("1.2.0") });
  e.stream("status", { ...status({}), workers: workers("1.2.0") });
  await eventually("status всех", async () => {
    for (const id of [old.agentId, same.agentId, arm.agentId, nocmd.agentId, ext.agentId])
      if (!(await agents.getAgent(id))?.status?.workers?.length) return false;
    return true;
  });

  const cands = (await agents.workerUpdateCandidates()).filter((x) => x.agentName.startsWith("wupd-"));
  // Новейшая сборка (1.10.0 > 1.2.0); wupd-same уже на ней, у wupd-arm нет сборки, wupd-nocmd не объявил
  // worker.update, plain — не из выпуска; wupd-ext (update.mode = external) — кандидат.
  assert.deepEqual(cands, [
    {
      agentId: old.agentId,
      agentName: "wupd-old",
      online: true,
      worker: "report",
      current: "1.2.0",
      target: "1.10.0",
      os: "linux",
      arch: "amd64",
    },
    {
      agentId: ext.agentId,
      agentName: "wupd-ext",
      online: true,
      worker: "report",
      current: "1.2.0",
      target: "1.10.0",
      os: "linux",
      arch: "amd64",
    },
  ]);

  const got: import("../src/server/index").AuditEntry[] = [];
  const onAudit = (e: (typeof got)[number]) => got.push(e);
  agents.on("audit", onAudit);
  const cmd = await agents.by("ivan").updateWorker(old.agentId, "report");
  agents.off("audit", onAudit);
  assert.equal(cmd.name, "worker.update");
  assert.equal(cmd.timeoutSec, 300);
  assert.equal(cmd.actor, "ivan");
  const run = await a.inbox.wait("cmd.run", (e) => e.data.name === "worker.update");
  assert.deepEqual(run.data.args, {
    name: "report",
    version: "1.10.0",
    url: `${RELEASES_PATH}/report-1.10.0-linux-amd64`,
    sha256: "ef".repeat(32),
    signature: "d2c=",
  });
  assert.deepEqual(
    got.map((e) => [e.actor, e.action, e.target, e.agentId, e.details]),
    [["ivan", "worker.update", old.agentId, old.agentId, { worker: "report", version: "1.10.0", commandId: cmd.id }]],
  );

  // update.mode = external — обновляется; та же версия — переустановка разрешена.
  await agents.updateWorker(ext.agentId, "report");
  await e.inbox.wait("cmd.run", (x) => x.data.name === "worker.update");
  await agents.updateWorker(same.agentId, "report");
  await b.inbox.wait("cmd.run", (x) => x.data.name === "worker.update" && x.data.args.version === "1.10.0");

  await assert.rejects(agents.updateWorker(old.agentId, "plain"), { code: "UPDATE_NOT_AVAILABLE" });
  await assert.rejects(agents.updateWorker(old.agentId, "nope"), { code: "UPDATE_NOT_AVAILABLE" });
  await assert.rejects(agents.updateWorker(arm.agentId, "report"), { code: "UPDATE_NOT_AVAILABLE" });
  await assert.rejects(agents.updateWorker(nocmd.agentId, "report"), { code: "UPDATE_NOT_AVAILABLE" });
  await assert.rejects(agents.updateWorker("nope", "report"), { code: "AGENT_NOT_FOUND" });
  await assert.rejects(agents.updateWorker(old.agentId, "a b"), { code: "MESSAGE_INVALID" });
  const bare = new Agents({ enrollToken: "x" });
  await assert.rejects(bare.updateWorker("nope", "report"), { code: "AGENT_NOT_FOUND" });
  bare.close();
  for (const x of [a, b, c, d, e]) x.ws.close();
});

test("«агент изменился» — только когда есть что обновить: повторный status и backfill-точка без события", async () => {
  const { auth, agentId } = await enroll("quiet-agent");
  const a = await connect(auth, "quiet-agent");
  const changes: number[] = [];
  const onAgent = (x: { id: string }) => x.id === agentId && changes.push(Date.now());
  agents.on("agent", onAgent);
  try {
    const idle = { state: "idle", slots: { q: 1 }, jobs: [], workers: [], outbox: 0 };
    a.stream("status", idle);
    await a.inbox.wait("ack", (e) => e.data.seq === 1);
    a.stream("status", idle);
    await a.inbox.wait("ack", (e) => e.data.seq === 2);
    assert.equal(changes.length, 1, "два одинаковых status — одно событие");
    a.stream("metrics", { collectedAt: Date.now(), clockOffsetMs: 0, backfill: true });
    await a.inbox.wait("ack", (e) => e.data.seq === 3);
    assert.equal(changes.length, 1, "backfill не меняет агента");
    a.stream("status", { ...idle, state: "busy" });
    await a.inbox.wait("ack", (e) => e.data.seq === 4);
    a.stream("metrics", { collectedAt: Date.now(), clockOffsetMs: 0 });
    await a.inbox.wait("ack", (e) => e.data.seq === 5);
    assert.equal(changes.length, 3, "изменившийся status и обычная точка — по событию");
    assert.ok((await agents.getAgent(agentId))!.lastSeenAt, "lastSeenAt сохраняется");
  } finally {
    agents.off("agent", onAgent);
    a.ws.close();
  }
});

test("refresh: команда и снимок, созданные другим объектом Agents с общим Store, доходят до агента", async () => {
  const other = new Agents({ enrollToken: "it", store: agents.store });
  try {
    const { auth, agentId } = await enroll("refresh-agent");
    const a = await connect(auth, "refresh-agent", {
      capabilities: { commands: { names: ["x.do"] }, state: { domains: { "x.refresh": null } } },
    });
    await other.command({ name: "x.do", agentId });
    await other.setState("x.refresh", { n: 1 });
    await new Promise((r) => setTimeout(r, 100));
    assert.equal(
      a.inbox.got.filter((e) => e.type === "cmd.run" || e.type === "state.put").length,
      0,
      "без refresh не знает",
    );
    await agents.refresh(agentId);
    await a.inbox.wait("cmd.run");
    await a.inbox.wait("state.put", (e) => e.data.domain === "x.refresh");
    a.ws.close();
  } finally {
    other.close();
  }
});

test("имена: неверное имя очереди, команды, раздела — MESSAGE_INVALID; необъявленный раздел — предупреждение", async () => {
  const logs: Array<[string, Record<string, unknown> | undefined]> = [];
  const store = new MemoryStore();
  const local = new Agents({ enrollToken: "x", store, log: (msg, extra) => logs.push([msg, extra]) });
  try {
    for (const bad of ["bad name", ".dot", "a/b", "x".repeat(65), "имя"]) {
      await assert.rejects(local.enqueue({ queue: bad }), { code: "MESSAGE_INVALID" });
      await assert.rejects(local.command({ name: bad }), { code: "MESSAGE_INVALID" });
      await assert.rejects(local.setState(bad, {}), { code: "MESSAGE_INVALID" });
      await assert.rejects(local.deleteState(bad), { code: "MESSAGE_INVALID" });
    }
    assert.deepEqual(await local.listStates(), []);

    await store.createAgent({
      id: "a1",
      name: "a1",
      labels: {},
      online: false,
      enrolledAt: Date.now(),
      stateApplied: {},
      capabilities: { state: { domains: { "example.kv": null } } },
      secretHash: "",
      lastSeq: 0,
      rev: 0,
    });
    await store.createAgent({
      id: "a2",
      name: "a2",
      labels: {},
      online: false,
      enrolledAt: Date.now(),
      stateApplied: {},
      secretHash: "",
      lastSeq: 0,
      rev: 0,
    });
    const warned = () => logs.filter(([msg]) => msg.startsWith("раздел состояния")).map(([, extra]) => extra);

    await local.setState("example.kv", { v: 1 });
    await local.setState("example.kv", { v: 2 }, { agentId: "a1" });
    assert.deepEqual(warned(), []);

    await local.setState("example.kvv", { v: 1 });
    await local.setState("example.kv", { v: 3 }, { agentId: "a2" });
    assert.deepEqual(warned(), [{ domain: "example.kvv" }, { domain: "example.kv", agentId: "a2" }]);
  } finally {
    local.close();
  }
});

test("несколько процессов: revoke другим процессом — refresh закрывает сессию кодом 4401", async () => {
  const other = new Agents({ enrollToken: "x", store: agents.store });
  const { auth, agentId } = await enroll("revoke-other");
  const a = await connect(auth, "revoke-other");
  await other.revoke(agentId);
  await sleep(50);
  assert.equal(a.closeCode, 0, "у другого процесса сессии нет — до refresh агент на связи");
  await agents.refresh(); // без agentId — все сессии процесса
  await eventually("4401", async () => a.closeCode === Close.Unauthorized);
  assert.equal((await agents.getAgent(agentId))?.revoked, true);
  other.close();
});

test("несколько процессов: подписка в записи агента — refresh применяет, истечение сверяет процесс сессии", async () => {
  const other = new Agents({ enrollToken: "x", store: agents.store });
  const { auth, agentId } = await enroll("sub-other");
  let a = await connect(auth, "sub-other");
  const { id, until } = await other.subscribe(agentId, { ttlMs: 400, metrics: { intervalMs: 500 } });
  assert.deepEqual(
    (await agents.getAgent(agentId))?.subscriptions,
    [{ id, until, metrics: { intervalMs: 500 } }],
    "подписка — в записи агента",
  );
  await sleep(50);
  assert.equal(a.inbox.all("config").length, 0, "до refresh config не уходит");
  await agents.refresh(agentId);
  assert.deepEqual((await a.inbox.wait("config")).data, { subscription: { metricsIntervalMs: 500 } });
  await agents.refresh(agentId);
  await sleep(30);
  assert.equal(a.inbox.all("config").length, 1, "та же сводная — config не повторяется");

  // Переподключение во время подписки — сводная по записи в welcome.
  a.ws.close();
  await sleep(30);
  a = await connect(auth, "sub-other");
  assert.deepEqual(a.inbox.got[0].data.config.subscription, { metricsIntervalMs: 500 });
  const back = await a.inbox.wait("config", () => true, 2000);
  assert.deepEqual(back.data, { subscription: {} }, "срок истёк — подписок нет");
  assert.equal((await agents.getAgent(agentId))?.subscriptions, undefined, "истёкшая из записи убрана");
  a.ws.close();
  other.close();
});

test("подписки агента без связи: истёкшие убирает любой процесс", async () => {
  const other = new Agents({ enrollToken: "x", store: agents.store });
  const { agentId } = await enroll("sub-offline");
  await other.subscribe(agentId, { ttlMs: 50, logs: { level: "debug" } });
  await eventually("убрана", async () => (await agents.getAgent(agentId))?.subscriptions === undefined);
  other.close();
});

test("метрики: прореживание истории — не чаще metricsStoreIntervalMs, событие metrics — каждая точка", async () => {
  const thin = new Agents({ enrollToken: "x", metricsStoreIntervalMs: 15_000 });
  const { agentId, secret } = await thin.enrollAgent({ token: "x", name: "thin" });
  const agent = (await thin.authenticate(`Agent ${agentId}.${secret}`))!;
  const ss = new Session(agent.id, "ws", "http://localhost");
  await thin.open(ss, hello("thin"));
  let events = 0;
  thin.on("metrics", () => events++);
  const t0 = Date.now() - 60_000;
  let seq = 0;
  for (const dt of [0, 5_000, 14_999, 15_000, 16_000, 29_999, 30_000]) {
    await thin.process(ss, {
      type: "metrics",
      seq: ++seq,
      data: { collectedAt: t0 + dt, clockOffsetMs: 0, host: { cpuPercent: dt } },
    });
  }
  // Досланные (время без связи — раньше уже сохранённых живых) прореживаются отдельно:
  // иначе история без связи пропала бы целиком.
  for (const dt of [-40_000, -30_000, -25_000, -10_000]) {
    await thin.process(ss, {
      type: "metrics",
      seq: ++seq,
      data: { collectedAt: t0 + dt, clockOffsetMs: 0, backfill: true, host: { cpuPercent: dt } },
    });
  }
  assert.deepEqual(
    (await thin.listMetrics(agentId)).map((p) => p.at - t0),
    [-40_000, -25_000, -10_000, 0, 15_000, 30_000],
  );
  assert.equal(events, 11);
  assert.equal((await thin.getAgent(agentId))?.metrics?.host?.cpuPercent, 30_000, "agent.metrics — последняя точка");
  thin.close();
});

test("метрики: срок хранения — pruneMetrics при старте; MemoryStore.pruneMetrics", async () => {
  const store = new MemoryStore();
  const now = Date.now();
  for (const at of [now - 10_000, now - 5_000, now - 100])
    await store.addMetrics("a", { at, backfill: false, metrics: { collectedAt: at } });
  await store.addMetrics("b", { at: now - 9_000, backfill: false, metrics: { collectedAt: now - 9_000 } });
  assert.equal(await store.pruneMetrics(now - 6_000), 2);
  assert.deepEqual(
    (await store.listMetrics("a")).map((p) => now - p.at),
    [5_000, 100],
  );
  assert.deepEqual(await store.listMetrics("b"), []);
  assert.equal(await store.pruneMetrics(now - 6_000), 0);

  const keep = new Agents({ enrollToken: "x", store, metricsRetentionMs: 0 });
  await sleep(20);
  assert.equal((await store.listMetrics("a")).length, 2, "0 — хранить всегда");
  keep.close();
  const pruning = new Agents({ enrollToken: "x", store, metricsRetentionMs: 1_000 });
  await eventually("старые точки удалены при старте", async () => (await store.listMetrics("a")).length === 1);
  pruning.close();
});

const sha = (s: string) => createHash("sha256").update(s).digest("hex");

test("rotateKey: agent.rotateKey → ожидающий хеш, 1012 после ack; вход с новым ключом делает его основным", async () => {
  const { auth, agentId } = await enroll("rotate-agent");
  const caps = { capabilities: { commands: { names: ["agent.rotateKey"] } } };
  await assert.rejects(agents.rotateKey("nope"), { code: "AGENT_NOT_FOUND" });
  const plain = await enroll("rotate-plain");
  await connect(plain.auth, "rotate-plain");
  await assert.rejects(agents.rotateKey(plain.agentId), { code: "COMMAND_NOT_SUPPORTED" });

  const a = await connect(auth, "rotate-agent", caps);
  const cmd = await agents.rotateKey(agentId);
  assert.equal(cmd.name, "agent.rotateKey");
  assert.equal(cmd.timeoutSec, 60);
  const run = await a.inbox.wait("cmd.run", (e) => e.data.commandId === cmd.id);
  assert.equal(run.data.name, "agent.rotateKey");
  const secret = "n".repeat(48);
  const ack = await a.reliable("cmd.done", { commandId: cmd.id, ok: true, result: { secretHash: sha(secret) } });
  assert.equal(ack.type, "ack", "ack на cmd.done — до закрытия");
  await eventually("1012", async () => a.closeCode === Close.Restart);
  assert.equal((await agents.store.getAgent(agentId))?.pendingSecretHash, sha(secret));
  assert.equal("pendingSecretHash" in (await agents.getAgent(agentId))!, false, "служебное поле наружу не отдаётся");
  assert.equal((await agents.getCommand(cmd.id))?.status, "succeeded");

  // Старый ключ пока принимается; новый — принимается и становится основным.
  const old = await connect(auth, "rotate-agent", caps);
  old.ws.close();
  const fresh = `Agent ${agentId}.${secret}`;
  const b = await connect(fresh, "rotate-agent", caps);
  const rec = await agents.store.getAgent(agentId);
  assert.equal(rec?.secretHash, sha(secret));
  assert.equal(rec?.pendingSecretHash, undefined);
  const code = await new Promise<number>((r) => {
    const ws = new WebSocket(base.replace("http", "ws") + LINK_PATH, WS_CHANNEL, { headers: { Authorization: auth } });
    ws.on("unexpected-response", (_req, res) => r(res.statusCode ?? 0));
    ws.on("open", () => r(101));
  });
  assert.equal(code, 401, "старый ключ больше не принимается");

  // Неверный хеш — не запоминается и сессия не закрывается.
  const cmd2 = await agents.rotateKey(agentId);
  await b.inbox.wait("cmd.run", (e) => e.data.commandId === cmd2.id);
  await b.reliable("cmd.done", { commandId: cmd2.id, ok: true, result: { secretHash: "zz" } });
  await sleep(50);
  assert.equal(b.closeCode, 0);
  assert.equal((await agents.store.getAgent(agentId))?.pendingSecretHash, undefined);

  // revoke очищает ожидающий; отозванному — AGENT_REVOKED.
  const cmd3 = await agents.rotateKey(agentId);
  await b.inbox.wait("cmd.run", (e) => e.data.commandId === cmd3.id);
  await b.reliable("cmd.done", { commandId: cmd3.id, ok: true, result: { secretHash: sha("x") } });
  assert.equal((await agents.store.getAgent(agentId))?.pendingSecretHash, sha("x"));
  await agents.revoke(agentId);
  assert.equal((await agents.store.getAgent(agentId))?.pendingSecretHash, undefined);
  await assert.rejects(agents.rotateKey(agentId), { code: "AGENT_REVOKED" });
});

test("by(actor) и audit: actor в задаче, команде, снимке; событие на каждое изменение", async () => {
  const { auth, agentId } = await enroll("audit-agent");
  await connect(auth, "audit-agent", { capabilities: { commands: { names: ["audit.cmd"] } } });
  const got: import("../src/server/index").AuditEntry[] = [];
  const onAudit = (e: (typeof got)[number]) => got.push(e);
  agents.on("audit", onAudit);
  const ivan = agents.by("ivan");
  assert.equal(ivan.actor, "ivan");

  const job = await ivan.enqueue({ queue: "audit.q", agentId });
  assert.equal(job.actor, "ivan");
  await agents.cancelJob(job.id);
  const job2 = await agents.enqueue({ queue: "audit.q" });
  assert.equal(job2.actor, undefined);
  await ivan.stopJob(job2.id);
  const cmd = await ivan.command({ name: "audit.cmd", agentId });
  assert.equal(cmd.actor, "ivan");
  const st = await ivan.setState("audit.dom", { a: 1 });
  assert.equal(st.actor, "ivan");
  const rolled = await agents.by("petr").rollbackState("audit.dom", st.version);
  assert.equal(rolled.actor, "petr");
  await ivan.deleteState("audit.dom");
  await ivan.revoke(agentId);

  agents.off("audit", onAudit);
  const brief = got.map((e) => [e.actor, e.action, e.target]);
  assert.deepEqual(brief, [
    ["ivan", "job.enqueue", job.id],
    ["", "job.cancel", job.id],
    ["", "job.enqueue", job2.id],
    ["ivan", "job.stop", job2.id],
    ["ivan", "command", cmd.id],
    ["ivan", "state.set", "audit.dom"],
    ["petr", "state.rollback", "audit.dom"],
    ["ivan", "state.delete", "audit.dom"],
    ["ivan", "agent.revoke", agentId],
  ]);
  assert.equal(got[0].agentId, agentId);
  assert.deepEqual(got[0].details, { queue: "audit.q" });
  assert.deepEqual(got[4].details, { name: "audit.cmd" });
  assert.equal(got[6].details?.fromVersion, st.version);
  assert.ok(got.every((e) => typeof e.at === "number"));
  // Ошибка — без записи аудита.
  const before = got.length;
  agents.on("audit", onAudit);
  await assert.rejects(agents.by("ivan").cancelJob("nope"), { code: "JOB_NOT_FOUND" });
  agents.off("audit", onAudit);
  assert.equal(got.length, before);
});

test("история состояния: от новых к старым, limit, личная отдельно, не стирается удалением; откат — новая версия", async () => {
  const { agentId } = await enroll("hist-agent");
  const v1 = await agents.setState("hist.dom", { n: 1 });
  const v2 = await agents.setState("hist.dom", { n: 2 });
  const v3 = await agents.setState("hist.dom", { n: 3 });
  const own = await agents.setState("hist.dom", { n: 9 }, { agentId });
  assert.deepEqual(
    (await agents.stateHistory("hist.dom")).map((s) => s.version),
    [v3.version, v2.version, v1.version],
  );
  assert.deepEqual(
    (await agents.stateHistory("hist.dom", { limit: 2 })).map((s) => s.spec),
    [{ n: 3 }, { n: 2 }],
  );
  assert.deepEqual(
    (await agents.stateHistory("hist.dom", { agentId })).map((s) => s.version),
    [own.version],
  );

  const back = await agents.rollbackState("hist.dom", v1.version);
  assert.deepEqual(back.spec, { n: 1 });
  assert.ok(back.version > v3.version, "откат — новая версия");
  assert.equal(back.agentId, undefined);
  assert.equal((await agents.stateHistory("hist.dom"))[0].version, back.version);
  // Версия общей истории не ищется в личной.
  await assert.rejects(agents.rollbackState("hist.dom", v1.version, { agentId }), {
    code: "STATE_VERSION_NOT_FOUND",
    status: 404,
  });
  await agents.deleteState("hist.dom");
  assert.equal((await agents.stateHistory("hist.dom")).length, 4, "удаление историю не стирает");
  const again = await agents.rollbackState("hist.dom", v2.version);
  assert.deepEqual((await agents.store.getState("hist.dom"))?.spec, { n: 2 });
  assert.equal(again.version, (await agents.store.getState("hist.dom"))?.version);

  const store = new MemoryStore({ keepStateHistory: 3 });
  for (let i = 0; i < 5; i++) await store.setState("d", undefined, { i });
  assert.deepEqual(
    (await store.listStateHistory("d", undefined, 10)).map((s) => s.spec),
    [{ i: 4 }, { i: 3 }, { i: 2 }],
  );
  assert.deepEqual(
    (await store.listStateHistory("d", undefined, 0)).map((s) => s.spec),
    [{ i: 4 }, { i: 3 }, { i: 2 }],
    "limit ≤ 0 — все",
  );
  assert.deepEqual(await store.listStateHistory("d", "other", 10), []);
});

test("alert: degraded, workerDown, stateFailed, offline — одно событие на начало и конец; alerts()", async () => {
  const { auth, agentId } = await enroll("alert-agent");
  const got: import("../src/server/index").Alert[] = [];
  const onAlert = (al: (typeof got)[number]) => al.agentId === agentId && got.push(al);
  agents.on("alert", onAlert);
  let a = await connect(auth, "alert-agent");
  const bad = {
    ...status({}),
    state: "degraded",
    message: "воркеры перезапускаются",
    workers: [
      { name: "w1", state: "backoff", instances: 0 },
      { name: "w2", state: "running", instances: 1 },
    ],
  };
  a.stream("status", bad);
  a.stream("status", { ...bad, outbox: 1 }); // повтор — без новых событий
  await eventually("degraded и workerDown", async () => got.length >= 2);
  await sleep(50);
  assert.deepEqual(
    got.map((x) => [x.type, x.active, x.worker ?? ""]),
    [
      ["degraded", true, ""],
      ["workerDown", true, "w1"],
    ],
  );
  assert.equal(got[0].message, "воркеры перезапускаются");
  assert.equal(got[1].message, "Воркер w1: backoff");
  assert.equal(got[0].agentName, "alert-agent");
  assert.equal((await agents.alerts()).filter((x) => x.agentId === agentId).length, 2);

  a.stream("status", { ...status({}), workers: [{ name: "w2", state: "running", instances: 1 }] });
  await eventually("конец", async () => got.length >= 4);
  // Конец — с текстом начала.
  assert.deepEqual(
    got.slice(2).map((x) => [x.type, x.active, x.message]),
    [
      ["degraded", false, "воркеры перезапускаются"],
      ["workerDown", false, "Воркер w1: backoff"],
    ],
  );

  await a.reliable("state.applied", { domain: "al.dom", version: 1, ok: false, error: "нет файла" });
  await a.reliable("state.applied", { domain: "al.dom", version: 1, ok: false, error: "нет файла" });
  await a.reliable("state.applied", { domain: "al.dom", version: 2, ok: true });
  assert.deepEqual(
    got.slice(4).map((x) => [x.type, x.active, x.domain, x.message]),
    [
      ["stateFailed", true, "al.dom", "нет файла"],
      ["stateFailed", false, "al.dom", "нет файла"],
    ],
  );

  // degraded без status.message — текст по умолчанию, и он же в конце.
  a.stream("status", { ...status({}), state: "degraded" });
  a.stream("status", status({}));
  await eventually("degraded без message", async () => got.length >= 8);
  assert.deepEqual(
    got.slice(6).map((x) => [x.type, x.active, x.message]),
    [
      ["degraded", true, "Агент не в порядке"],
      ["degraded", false, "Агент не в порядке"],
    ],
  );

  a.ws.close();
  await eventually("offline", async () => got.length >= 9, 2000);
  assert.deepEqual([got[8].type, got[8].active, got[8].message], ["offline", true, "Агент без связи"]);
  assert.ok((await agents.alerts()).some((x) => x.agentId === agentId && x.type === "offline"));
  a = await connect(auth, "alert-agent");
  await eventually("снова online", async () => got.length >= 10);
  assert.deepEqual([got[9].type, got[9].active, got[9].message], ["offline", false, "Агент без связи"]);
  assert.equal((await agents.alerts()).filter((x) => x.agentId === agentId).length, 0);
  agents.off("alert", onAlert);
  a.ws.close();
});

test("регистрация: после enrollFailureLimit неудач за окно — 429 ENROLL_RATE_LIMITED с Retry-After, даже верный токен", async () => {
  const limited = new Agents({ enrollToken: "ok", enrollFailureLimit: 2, enrollFailureWindowMs: 300 });
  const unlimited = new Agents({ enrollToken: "ok", enrollFailureLimit: 0 });
  const make = (ag: Agents) => {
    const srv = createServer(async (req, res) => {
      if (!(await ag.handle(req, res))) res.writeHead(404).end();
    });
    return new Promise<{ srv: Server; url: string }>((r) =>
      srv.listen(0, "127.0.0.1", () => r({ srv, url: `http://127.0.0.1:${(srv.address() as AddressInfo).port}` })),
    );
  };
  const post = (url: string, token: string) =>
    fetch(url + ENROLL_PATH, { method: "POST", body: JSON.stringify({ token, name: "rl" }) });
  const l = await make(limited);
  const u = await make(unlimited);
  try {
    assert.equal((await post(l.url, "ok")).status, 201, "удачная — не в счёт");
    assert.equal((await post(l.url, "bad")).status, 401);
    assert.equal((await post(l.url, "bad")).status, 401);
    const res = await post(l.url, "ok");
    assert.equal(res.status, 429);
    assert.equal((await res.json()).code, "ENROLL_RATE_LIMITED");
    assert.equal(res.headers.get("retry-after"), "1");
    await sleep(350);
    assert.equal((await post(l.url, "ok")).status, 201, "окно прошло");
    for (let i = 0; i < 12; i++) assert.equal((await post(u.url, "bad")).status, 401);
    // Без адреса — общий клиент "*".
    await assert.rejects(limited.enrollAgent({ token: "bad", name: "x" }), { code: "AGENT_ENROLLMENT_TOKEN_INVALID" });
    await assert.rejects(limited.enrollAgent({ token: "bad", name: "x" }), { code: "AGENT_ENROLLMENT_TOKEN_INVALID" });
    await assert.rejects(limited.enrollAgent({ token: "ok", name: "x" }), { code: "ENROLL_RATE_LIMITED", status: 429 });
  } finally {
    limited.close();
    unlimited.close();
    l.srv.close();
    u.srv.close();
  }
});

test("несколько процессов: call в процессе без сессии агента находит итог в Store", async () => {
  const other = new Agents({ enrollToken: "x", store: agents.store });
  const { auth, agentId } = await enroll("call-other");
  const a = await connect(auth, "call-other", { capabilities: { commands: { names: ["x.do"] } } });
  const call = other.call({ name: "x.do", agentId, timeoutSec: 30 });
  await sleep(30);
  await agents.refresh(agentId); // NOTIFY → refresh в процессе, где сессия агента
  const run = await a.inbox.wait("cmd.run");
  await a.reliable("cmd.done", { commandId: run.data.commandId, ok: true, result: { n: 1 } });
  const started = Date.now();
  const cmd = await call;
  assert.equal(cmd.status, "succeeded", "итог сохранил другой процесс");
  assert.ok(Date.now() - started < 2500, "найден проверкой Store, а не по сроку");
  a.ws.close();
  other.close();
});

test("несколько процессов: агент без вестей дольше offlineAfterMs в процессе без его сессии — offline и alert", async () => {
  const store = new MemoryStore();
  const live = new Agents({ enrollToken: "x", store });
  const other = new Agents({ enrollToken: "x", store, offlineAfterMs: 250 });
  try {
    const { agentId } = await live.enrollAgent({ token: "x", name: "stale" });
    const ss = new Session(agentId, "ws", "http://localhost");
    await live.open(ss, hello("stale"));
    const alerts: import("../src/server/index").Alert[] = [];
    const changes: string[] = [];
    other.on("alert", (al) => al.agentId === agentId && alerts.push(al));
    other.on("change", (c) => c.kind === "agent" && c.id === agentId && changes.push(c.id));
    // Пульс (status) обновляет lastSeenAt — другой процесс агента не трогает.
    let seq = 0;
    for (let i = 0; i < 8; i++) {
      await live.process(ss, { type: "status", seq: ++seq, data: status({}) });
      await sleep(50);
    }
    assert.equal((await other.getAgent(agentId))?.online, true, "есть вести — online");
    assert.equal(alerts.length, 0);
    // Процесс с сессией «упал»: вестей нет — offline по сверке другого процесса.
    await eventually("offline", async () => (await other.getAgent(agentId))?.online === false, 2000);
    await eventually("alert offline", async () => alerts.length >= 1);
    await sleep(200);
    assert.deepEqual(
      alerts.map((x) => [x.type, x.active]),
      [["offline", true]],
      "одно уведомление, повторно не поднимается",
    );
    assert.ok(changes.length >= 1, "change agent");
    assert.ok((await other.alerts()).some((x) => x.agentId === agentId && x.type === "offline"));
    // Свою сессию процесс не трогает.
    const own = new Agents({ enrollToken: "x", store: new MemoryStore(), offlineAfterMs: 100 });
    try {
      const reg = await own.enrollAgent({ token: "x", name: "own" });
      const s2 = new Session(reg.agentId, "ws", "http://localhost");
      await own.open(s2, hello("own"));
      await sleep(300);
      assert.equal((await own.getAgent(reg.agentId))?.online, true, "сессия в этом процессе — online");
    } finally {
      own.close();
    }
  } finally {
    live.close();
    other.close();
  }
});

test("log: поток — ack{seq}, событие log [agentId, entries]; entries не массив — MESSAGE_INVALID", async () => {
  const { auth, agentId } = await enroll("log-agent");
  const a = await connect(auth, "log-agent");
  const got: [string, unknown[]][] = [];
  const onLog = (id: string, entries: unknown[]) => id === agentId && got.push([id, entries]);
  agents.on("log", onLog);
  const entries = [
    { at: 1, level: "warn", source: "agent", msg: "связь потеряна", attrs: { err: "EOF" } },
    { at: 2, level: "error", source: "report", msg: "порт занят" },
  ];
  a.send({ type: "log", seq: 1, data: { entries } });
  await eventually("log", async () => got.length >= 1);
  assert.deepEqual(got[0], [agentId, entries]);
  assert.equal((await a.inbox.wait("ack", (e) => e.data.seq === 1)).data.seq, 1);
  a.send({ type: "log", seq: 1, data: { entries } }); // повтор seq — не второй раз
  a.send({ type: "log", id: "bad", seq: 2, data: { entries: "x" } });
  const err = await a.inbox.wait("error", (e) => e.re === "bad");
  assert.equal(err.data.code, "MESSAGE_INVALID");
  assert.equal(got.length, 1, "повтор seq не доставляется");
  agents.off("log", onLog);
  a.ws.close();
});

test("seal: запечатанное ключом агента раскрывается его закрытым; нет ключа — SEAL_NOT_AVAILABLE", async () => {
  const kp = generateKeyPairSync("x25519");
  const priv = Buffer.from(kp.privateKey.export({ format: "jwk" }).d!, "base64url").toString("base64");
  const pub = Buffer.from(kp.publicKey.export({ format: "jwk" }).x!, "base64url").toString("base64");
  assert.equal(publicKeyOf(priv), pub);
  const { auth, agentId } = await enroll("seal-agent");
  let a = await connect(auth, "seal-agent");
  await assert.rejects(agents.seal(agentId, "x"), { code: "SEAL_NOT_AVAILABLE", status: 409 });
  await assert.rejects(agents.seal("nope", "x"), { code: "AGENT_NOT_FOUND" });
  a.ws.close();
  await sleep(30);
  const h = hello("seal-agent").data;
  a = await connect(auth, "seal-agent", { agent: { ...h.agent, encryptionKey: pub } });
  const value = { privateKey: "секрет", peers: [1, 2], nested: { ok: true } };
  const sealed = await agents.seal(agentId, value);
  assert.match(sealed.$sealed, /^v1\.[A-Za-z0-9_-]{43}\.[A-Za-z0-9_-]{16}\.[A-Za-z0-9_-]+$/);
  assert.deepEqual(unseal(priv, sealed.$sealed), value);
  const again = await agents.seal(agentId, value);
  assert.notEqual(again.$sealed, sealed.$sealed, "одноразовый ключ и nonce — каждый раз другое");
  // Повреждённое не раскрывается.
  const broken = sealed.$sealed.slice(0, -2) + (sealed.$sealed.endsWith("A") ? "BB" : "AA");
  assert.throws(() => unseal(priv, broken));
  a.ws.close();
});

test("pauseWorker/resumeWorker: команды worker.pause/resume, аудит, by(actor); ошибки", async () => {
  const { auth, agentId } = await enroll("pause-agent");
  const a = await connect(auth, "pause-agent", {
    capabilities: { commands: { names: ["worker.pause", "worker.resume"] } },
  });
  const got: import("../src/server/index").AuditEntry[] = [];
  const onAudit = (e: (typeof got)[number]) => e.agentId === agentId && got.push(e);
  agents.on("audit", onAudit);
  const p = await agents.by("ivan").pauseWorker(agentId, "report", { queues: ["example.convert"] });
  assert.equal(p.name, "worker.pause");
  assert.equal(p.actor, "ivan");
  assert.deepEqual(p.args, { name: "report", queues: ["example.convert"] });
  const run = await a.inbox.wait("cmd.run", (e) => e.data.commandId === p.id);
  assert.deepEqual(run.data.args, { name: "report", queues: ["example.convert"] });
  const r = await agents.resumeWorker(agentId, "report");
  assert.equal(r.name, "worker.resume");
  assert.deepEqual(r.args, { name: "report" });
  agents.off("audit", onAudit);
  assert.deepEqual(
    got.map((e) => [e.actor, e.action, e.target, e.details]),
    [
      ["ivan", "worker.pause", agentId, { worker: "report", queues: ["example.convert"], commandId: p.id }],
      ["", "worker.resume", agentId, { worker: "report", commandId: r.id }],
    ],
  );
  await assert.rejects(agents.pauseWorker("nope", "report"), { code: "AGENT_NOT_FOUND" });
  await assert.rejects(agents.pauseWorker(agentId, "плохо"), { code: "MESSAGE_INVALID" });
  await assert.rejects(agents.pauseWorker(agentId, "report", { queues: ["плохо"] }), { code: "MESSAGE_INVALID" });
  const other = await enroll("pause-old");
  const b = await connect(other.auth, "pause-old");
  await assert.rejects(agents.resumeWorker(other.agentId, "report"), { code: "COMMAND_NOT_SUPPORTED" });
  a.ws.close();
  b.ws.close();
});

test("alert workerDegraded: начало по health degraded, конец по ok и при исчезновении воркера", async () => {
  const { auth, agentId } = await enroll("wdeg-agent");
  const got: import("../src/server/index").Alert[] = [];
  const onAlert = (al: (typeof got)[number]) => al.agentId === agentId && al.type === "workerDegraded" && got.push(al);
  agents.on("alert", onAlert);
  const a = await connect(auth, "wdeg-agent");
  const w = (name: string, extra: Record<string, unknown> = {}) => ({ name, state: "running", instances: 1, ...extra });
  a.stream("status", {
    ...status({}),
    workers: [w("w1", { health: "degraded", message: "нет базы", paused: true }), w("w2", { health: "degraded" })],
  });
  await eventually("начало", async () => got.length >= 2);
  assert.deepEqual(
    got.map((x) => [x.active, x.worker, x.message]),
    [
      [true, "w1", "нет базы"],
      [true, "w2", "Воркер w2 не в порядке"],
    ],
  );
  assert.equal((await agents.getAgent(agentId))?.status?.workers[0].paused, true);
  a.stream("status", { ...status({}), workers: [w("w1", { health: "ok" })] });
  await eventually("конец", async () => got.length >= 4);
  assert.deepEqual(
    got
      .slice(2)
      .map((x) => [x.active, x.worker, x.message])
      .sort(),
    [
      [false, "w1", "нет базы"],
      [false, "w2", "Воркер w2 не в порядке"],
    ],
  );
  agents.off("alert", onAlert);
  a.ws.close();
});

test("address: адрес подключения в записи агента; X-Forwarded-For — только с trustProxy", async () => {
  const { auth, agentId } = await enroll("addr-agent");
  // Без trustProxy заголовок не учитывается.
  const ws = new WebSocket(base.replace("http", "ws") + LINK_PATH, WS_CHANNEL, {
    headers: { Authorization: auth, "X-Forwarded-For": "203.0.113.7" },
  });
  await new Promise((r, j) => ws.once("open", r).once("error", j));
  ws.send(JSON.stringify(hello("addr-agent")));
  await eventually("address", async () => (await agents.getAgent(agentId))?.address);
  assert.equal((await agents.getAgent(agentId))?.address, "127.0.0.1");
  ws.close();

  // trustProxy: первый адрес списка, без порта; HTTP sync — тоже.
  const proxied = new Agents({ enrollToken: "p", trustProxy: true });
  const srv = createServer(async (req, res) => {
    if (!(await proxied.handle(req, res))) res.writeHead(404).end();
  });
  proxied.attach(srv);
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r));
  const pbase = `http://127.0.0.1:${(srv.address() as AddressInfo).port}`;
  try {
    const res = await fetch(pbase + ENROLL_PATH, { method: "POST", body: JSON.stringify({ token: "p", name: "px" }) });
    const { agentId: id, secret } = await res.json();
    const sync = await fetch(pbase + SYNC_PATH, {
      method: "POST",
      headers: { Authorization: `Agent ${id}.${secret}`, "X-Forwarded-For": "198.51.100.4:5555, 10.0.0.1" },
      body: JSON.stringify({ sessionId: null, messages: [hello("px")] }),
    });
    assert.equal(sync.status, 200);
    assert.equal((await proxied.getAgent(id))?.address, "198.51.100.4");
  } finally {
    proxied.close();
    srv.closeAllConnections();
    srv.close();
  }
});

test("clientAddress: сокет без ::ffff:, X-Forwarded-For только с trustProxy — первый адрес без порта", async () => {
  const { clientAddress } = await import("../src/server/index");
  const req = (remote: string, xff?: string) =>
    ({ socket: { remoteAddress: remote }, headers: xff ? { "x-forwarded-for": xff } : {} }) as never;
  assert.equal(clientAddress(req("::ffff:10.0.0.5")), "10.0.0.5");
  assert.equal(clientAddress(req("::1")), "::1");
  assert.equal(clientAddress(req("10.0.0.5", "203.0.113.7")), "10.0.0.5", "без trustProxy — заголовок не учитывается");
  assert.equal(clientAddress(req("10.0.0.5", " 203.0.113.7 , 10.0.0.1"), true), "203.0.113.7");
  assert.equal(clientAddress(req("10.0.0.5", "203.0.113.7:4000"), true), "203.0.113.7");
  assert.equal(clientAddress(req("10.0.0.5", "[2001:db8::1]:443"), true), "2001:db8::1");
  assert.equal(clientAddress(req("10.0.0.5", "2001:db8::1"), true), "2001:db8::1");
  assert.equal(clientAddress(req("10.0.0.5"), true), "10.0.0.5", "нет заголовка — адрес сокета");
});

test("команды: ожидающая команда уходит только агенту, который её объявил", async () => {
  const { auth, agentId } = await enroll("cmd-declared");
  let a = await connect(auth, "cmd-declared", { capabilities: { commands: { names: ["x.only"] } } });
  a.ws.close();
  await sleep(50);
  const cmd = await agents.command({ name: "x.only", agentId });
  a = await connect(auth, "cmd-declared", { capabilities: {} });
  await sleep(100);
  assert.equal(a.inbox.all("cmd.run").length, 0, "агент не объявил команду — не отправляется");
  a.stream("capabilities", { commands: { names: ["x.only"] } });
  const run = await a.inbox.wait("cmd.run");
  assert.equal(run.data.commandId, cmd.id);
  a.ws.close();
});

test("MemoryStore: история состояния с limit ≤ 0 — все снимки, новые первыми", async () => {
  const store = new MemoryStore();
  for (const v of [1, 2, 3]) await store.setState("example.app", undefined, { v });
  const all = await store.listStateHistory("example.app", undefined, 0);
  assert.deepEqual(
    all.map((s) => (s.spec as { v: number }).v),
    [3, 2, 1],
  );
  assert.equal((await store.listStateHistory("example.app", undefined, 2)).length, 2);
});
