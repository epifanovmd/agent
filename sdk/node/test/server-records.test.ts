// Записи Store и несколько процессов бэкенда: условная запись по rev, два Agents на одном
// хранилище, отмена команды, удаление агента, уборка, постраничное чтение, пределы регистрации,
// заголовки прокси только с trustProxy.
import assert from "node:assert/strict";
import { createServer, type IncomingMessage } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";
import { WebSocket } from "ws";
import { ENROLL_PATH, LINK_PATH, WS_CHANNEL, type Envelope } from "../src/index";
import {
  Agents,
  agentsDefaults,
  baseUrl,
  MemoryStore,
  type AgentRecord,
  type AgentsOptions,
  type Alert,
  type AuditEntry,
  type Change,
  type CommandRecord,
  type JobRecord,
} from "../src/server/index";
import { Inbox, sleep } from "./helpers";

agentsDefaults.commandGraceMs = 300;
agentsDefaults.sweepIntervalMs = 50;

/** Сервер со своим Agents; закрывается в конце теста. */
async function serve(t: { after: (fn: () => void) => void }, opts: AgentsOptions) {
  const agents = new Agents({ log: () => {}, ...opts });
  const srv = createServer(async (req, res) => {
    if (!(await agents.handle(req, res))) res.writeHead(404).end();
  });
  agents.attach(srv);
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r));
  t.after(() => {
    agents.close();
    srv.closeAllConnections();
    srv.close();
  });
  return { agents, base: `http://127.0.0.1:${(srv.address() as AddressInfo).port}` };
}

async function enroll(base: string, name: string, token = "t", headers: Record<string, string> = {}) {
  const res = await fetch(base + ENROLL_PATH, { method: "POST", headers, body: JSON.stringify({ token, name }) });
  assert.equal(res.status, 201);
  const { agentId, secret } = await res.json();
  return { agentId, auth: `Agent ${agentId}.${secret}` };
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

const status = (slots: Record<string, number>, extra: Record<string, unknown> = {}) => ({
  state: "idle",
  slots,
  jobs: [],
  workers: [],
  outbox: 0,
  ...extra,
});

/** Агент по WebSocket к серверу base; seq потока — свой счётчик (можно задать с какого). */
async function connect(base: string, auth: string, name: string, helloExtra: Record<string, unknown> = {}) {
  const ws = new WebSocket(base.replace("http", "ws") + LINK_PATH, WS_CHANNEL, { headers: { Authorization: auth } });
  const inbox = new Inbox();
  let closeCode = 0;
  ws.on("message", (raw) => inbox.push(JSON.parse(raw.toString())));
  ws.on("close", (code) => (closeCode = code));
  await new Promise((r, j) => ws.once("open", r).once("error", j));
  let rid = 0;
  let last = 0;
  const a = {
    ws,
    inbox,
    get closeCode() {
      return closeCode;
    },
    send: (env: Envelope) => ws.send(JSON.stringify(env)),
    stream(type: string, data: unknown, seq = last + 1) {
      last = seq;
      ws.send(JSON.stringify({ type, seq, data }));
    },
    async reliable(type: string, data: unknown) {
      const id = `${name}-r${++rid}`;
      ws.send(JSON.stringify({ type, id, data }));
      return inbox.waitFor(
        (e) => (e.type === "ack" && e.data.ids?.includes(id)) || (e.type === "error" && e.re === id),
        type,
      );
    },
    close: () =>
      new Promise<void>((r) => (ws.readyState === ws.CLOSED ? r() : (ws.once("close", () => r()), ws.close()))),
  };
  a.send(hello(name, helloExtra));
  await inbox.wait("welcome");
  return a;
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

const agentRecord = (id: string): AgentRecord => ({
  id,
  name: id,
  labels: {},
  online: false,
  enrolledAt: Date.now(),
  stateApplied: {},
  secretHash: "",
  lastSeq: 0,
  rev: 0,
});

const jobRecord = (id: string, extra: Partial<JobRecord> = {}): JobRecord => ({
  id,
  queue: "example.q",
  data: {},
  status: "queued",
  attempt: 0,
  maxAttempts: 1,
  leaseSeconds: 60,
  accepted: false,
  progress: 0,
  log: [],
  events: [],
  stopRequested: false,
  inputs: [],
  outputs: [],
  createdAt: Date.now(),
  leaseUntil: 0,
  eventSeq: 0,
  rev: 0,
  ...extra,
});

const commandRecord = (id: string, extra: Partial<CommandRecord> = {}): CommandRecord => ({
  id,
  agentId: "a",
  name: "example.cmd",
  timeoutSec: 60,
  status: "pending",
  output: "",
  createdAt: Date.now(),
  rev: 0,
  ...extra,
});

test("MemoryStore: условная запись по rev — вторая копия того же чтения не пишется, записи нет — false", async () => {
  const store = new MemoryStore();
  await store.createAgent(agentRecord("a"));
  const one = (await store.getAgent("a"))!;
  const two = (await store.getAgent("a"))!;
  one.revoked = true;
  assert.equal(await store.updateAgent(one), true);
  assert.equal(one.rev, 1, "у записанной rev + 1");
  two.online = true;
  assert.equal(await store.updateAgent(two), false, "запись изменилась с чтения");
  assert.equal(two.rev, 0);
  const cur = (await store.getAgent("a"))!;
  assert.equal(cur.revoked, true, "чужая запись не затёрла отзыв");
  assert.equal(cur.rev, 1);
  assert.equal(await store.updateAgent(agentRecord("nope")), false);

  await store.createJob(jobRecord("j"));
  const j1 = (await store.getJob("j"))!;
  const j2 = (await store.getJob("j"))!;
  assert.equal(await store.updateJob({ ...j1, status: "running" }), true);
  assert.equal(await store.updateJob({ ...j2, status: "cancelled" }), false);
  assert.equal((await store.getJob("j"))?.status, "running");
  assert.equal(await store.updateJob(jobRecord("nope")), false);

  await store.createCommand(commandRecord("c"));
  const c1 = (await store.getCommand("c"))!;
  const c2 = (await store.getCommand("c"))!;
  assert.equal(await store.updateCommand({ ...c1, status: "running" }), true);
  assert.equal(await store.updateCommand({ ...c2, status: "failed" }), false);
  assert.equal((await store.getCommand("c"))?.status, "running");
  assert.equal(await store.updateCommand(commandRecord("nope")), false);

  await store.addMetrics("a", { at: 1, backfill: false, metrics: { collectedAt: 1 } });
  assert.equal(await store.deleteAgent("a"), true);
  assert.equal(await store.getAgent("a"), undefined);
  assert.deepEqual(await store.listMetrics("a"), [], "история метрик удалена с агентом");
  assert.equal(await store.deleteAgent("a"), false);
});

test("MemoryStore: limit и after — страницы в порядке «новые первыми»; after неизвестен — пусто", async () => {
  const store = new MemoryStore();
  for (const id of ["j1", "j2", "j3", "j4", "j5"])
    await store.createJob(jobRecord(id, { status: id === "j3" ? "running" : "queued" }));
  const ids = (list: { id: string }[]) => list.map((j) => j.id);
  assert.deepEqual(ids(await store.listJobs({ limit: 2 })), ["j5", "j4"]);
  assert.deepEqual(ids(await store.listJobs({ limit: 2, after: "j4" })), ["j3", "j2"]);
  assert.deepEqual(ids(await store.listJobs({ limit: 2, after: "j2" })), ["j1"]);
  assert.deepEqual(ids(await store.listJobs({ limit: 0, after: "j4" })), ["j3", "j2", "j1"], "limit 0 — все");
  assert.deepEqual(ids(await store.listJobs({ status: "queued", limit: 2, after: "j4" })), ["j2", "j1"]);
  assert.deepEqual(ids(await store.listJobs({ after: "j3", status: "queued" })), ["j2", "j1"], "after не под фильтром");
  assert.deepEqual(await store.listJobs({ after: "nope" }), []);

  for (const id of ["c1", "c2", "c3"]) await store.createCommand(commandRecord(id));
  assert.deepEqual(ids(await store.listCommands({ limit: 1 })), ["c3"]);
  assert.deepEqual(ids(await store.listCommands({ limit: 1, after: "c3" })), ["c2"]);

  const agents = new Agents({ enrollToken: "x", store });
  try {
    assert.deepEqual(ids(await agents.listJobs({ limit: 2, after: "j5" })), ["j4", "j3"]);
    assert.deepEqual(ids(await agents.listCommands({ limit: 5, after: "c2" })), ["c1"]);
    assert.equal("rev" in (await agents.listCommands())[0], false, "служебное наружу не отдаётся");
    assert.equal("rev" in (await agents.listJobs())[0], false);
  } finally {
    agents.close();
  }
});

test("prune: завершённые задачи и команды, события — по возрасту; активные не трогаются", async () => {
  const store = new MemoryStore();
  const now = Date.now();
  await store.createJob(jobRecord("old-done", { status: "completed", finishedAt: now - 10_000 }));
  await store.createJob(jobRecord("new-done", { status: "failed", finishedAt: now - 100 }));
  await store.createJob(jobRecord("old-queued", { createdAt: now - 10_000 }));
  await store.createCommand(commandRecord("old-cmd", { status: "cancelled", finishedAt: now - 10_000 }));
  await store.createCommand(commandRecord("old-pending", { createdAt: now - 10_000 }));
  for (const at of [now - 10_000, now - 9_000, now - 100])
    await store.addEvent({ agentId: "a", agentName: "a", source: "agent", type: "example.e", at });

  assert.equal(await store.prune({}), 0, "без границ — ничего");
  const agents = new Agents({ enrollToken: "x", store });
  try {
    assert.equal(await agents.prune({ jobsOlderThanMs: 5_000 }), 1);
    assert.deepEqual(
      (await store.listJobs()).map((j) => j.id),
      ["old-queued", "new-done"],
    );
    assert.equal(await agents.prune({ commandsOlderThanMs: 5_000, eventsOlderThanMs: 5_000 }), 3);
    assert.deepEqual(
      (await store.listCommands()).map((c) => c.id),
      ["old-pending"],
    );
    assert.equal((await store.listEvents(0)).length, 1);
    assert.equal(await agents.prune({ jobsOlderThanMs: 0, commandsOlderThanMs: 0, eventsOlderThanMs: 0 }), 0);
  } finally {
    agents.close();
  }
});

test("два Agents на одном Store: ждущая задача выдаётся одному агенту", async (t) => {
  // Чтение задачи с задержкой: оба процесса успевают прочитать её ждущей до записи.
  class SlowRead extends MemoryStore {
    override async getJob(id: string) {
      const j = await super.getJob(id);
      await sleep(30);
      return j;
    }
  }
  const store = new SlowRead();
  const one = await serve(t, { enrollToken: "t", store });
  const two = await serve(t, { enrollToken: "t", store });
  const ra = await enroll(one.base, "race-a");
  const rb = await enroll(two.base, "race-b");
  const a = await connect(one.base, ra.auth, "race-a");
  const b = await connect(two.base, rb.auth, "race-b");
  await store.createJob(jobRecord("race-job"));
  a.stream("status", status({ "example.q": 1 }));
  b.stream("status", status({ "example.q": 1 }));
  await eventually("задача выдана", async () => (await store.getJob("race-job"))?.status === "running");
  await sleep(200);
  const assigned = [...a.inbox.all("job.assign"), ...b.inbox.all("job.assign")];
  assert.equal(assigned.length, 1, "задачу получил один агент");
  const job = (await store.getJob("race-job"))!;
  assert.ok([ra.agentId, rb.agentId].includes(job.agentId!));
  await a.close();
  await b.close();
});

test("два Agents на одном Store: отзыв в одном не затирается сообщением, которое обрабатывает другой", async (t) => {
  // Перед записью агента — один раз выполнить hook (отзыв другим процессом между чтением и записью).
  class HookStore extends MemoryStore {
    hook?: () => Promise<unknown>;
    override async updateAgent(a: AgentRecord) {
      const h = this.hook;
      this.hook = undefined;
      if (h) await h();
      return super.updateAgent(a);
    }
  }
  const store = new HookStore();
  const one = await serve(t, { enrollToken: "t", store });
  const two = await serve(t, { enrollToken: "t", store });
  const { agentId, auth } = await enroll(two.base, "revoked-race");
  const a = await connect(two.base, auth, "revoked-race");
  store.hook = () => one.agents.revoke(agentId);
  a.stream("status", status({}, { state: "degraded", message: "диск" }));
  await eventually("сессия закрыта", async () => a.closeCode === 4401);
  const rec = (await store.getAgent(agentId))!;
  assert.equal(rec.revoked, true, "отзыв не затёрт");
  assert.equal(rec.online, false);
  assert.equal(rec.status, undefined, "сообщение отозванного не записано");
  assert.deepEqual(await one.agents.alerts(), []);
});

test("два Agents на одном Store: повтор seq после переподключения к другому процессу не обрабатывается дважды", async (t) => {
  const store = new MemoryStore();
  const opts = { enrollToken: "t", store, metricsStoreIntervalMs: 0 };
  const one = await serve(t, opts);
  const two = await serve(t, opts);
  const { agentId, auth } = await enroll(one.base, "seq-agent");
  const a = await connect(one.base, auth, "seq-agent");
  a.stream("metrics", { collectedAt: 1000 }, 1);
  await a.inbox.wait("ack", (e) => e.data.seq === 1);
  assert.equal((await store.getAgent(agentId))?.lastSeq, 1);
  await a.close();

  // Тот же запуск агента (bootId) — к другому процессу; пачка из outbox с seq 1 приходит снова.
  const b = await connect(two.base, auth, "seq-agent");
  b.stream("metrics", { collectedAt: 1000 }, 1);
  await b.inbox.wait("ack", (e) => e.data.seq === 1);
  b.stream("metrics", { collectedAt: 2000 }, 2);
  await b.inbox.wait("ack", (e) => e.data.seq === 2);
  assert.equal((await store.listMetrics(agentId)).length, 2, "повтор seq 1 не сохранён второй раз");
  assert.equal((await store.getAgent(agentId))?.lastSeq, 2);
  await b.close();

  // Новый запуск (другой bootId) — нумерация с начала.
  const c = await connect(two.base, auth, "seq-agent", {
    agent: { name: "seq-agent", version: "t", bootId: "b2", startedAt: Date.now() },
  });
  assert.equal((await store.getAgent(agentId))?.lastSeq, 0);
  c.stream("metrics", { collectedAt: 3000 }, 1);
  await c.inbox.wait("ack", (e) => e.data.seq === 1);
  assert.equal((await store.listMetrics(agentId)).length, 3);
  await c.close();
});

test("два Agents на одном Store: сверка не трогает аренду, продлённую другим процессом", async (t) => {
  // listJobs отдаёт устаревшую копию (аренда истекла), запись в Store — продлённая.
  class StaleList extends MemoryStore {
    override async listJobs(f?: Parameters<MemoryStore["listJobs"]>[0]) {
      return (await super.listJobs(f)).map((j) => (j.id === "extended" ? { ...j, leaseUntil: 0 } : j));
    }
  }
  const store = new StaleList();
  await serve(t, { enrollToken: "t", store });
  await serve(t, { enrollToken: "t", store });
  const until = Date.now() + 60_000;
  await store.createJob(jobRecord("extended", { status: "running", agentId: "x", leaseUntil: until }));
  await store.createJob(jobRecord("expired", { status: "running", agentId: "x", leaseUntil: Date.now() - 1 }));
  await eventually("истёкшая аренда сверена", async () => (await store.getJob("expired"))?.status === "failed");
  assert.equal((await store.getJob("expired"))?.error?.code, "LEASE_EXPIRED");
  await sleep(200);
  const ext = (await store.getJob("extended"))!;
  assert.equal(ext.status, "running");
  assert.equal(ext.leaseUntil, until);
});

test("два Agents на одном Store: alerts() видны второму, событие alert — у записавшего", async (t) => {
  const store = new MemoryStore();
  const one = await serve(t, { enrollToken: "t", store });
  const two = await serve(t, { enrollToken: "t", store });
  const seen1: Alert[] = [];
  const seen2: Alert[] = [];
  one.agents.on("alert", (x) => seen1.push(x));
  two.agents.on("alert", (x) => seen2.push(x));
  const { agentId, auth } = await enroll(one.base, "alert-agent");
  const a = await connect(one.base, auth, "alert-agent");
  a.stream("status", status({}, { state: "degraded", message: "диск" }));
  await eventually("alert", async () => seen1.length === 1);
  const list = await two.agents.alerts();
  assert.deepEqual(
    list.map((x) => [x.type, x.agentId, x.message, x.active]),
    [["degraded", agentId, "диск", true]],
  );
  assert.equal("alerts" in (await two.agents.getAgent(agentId))!, false, "служебное наружу не отдаётся");
  a.stream("status", status({}, { state: "degraded", message: "диск" }));
  a.stream("status", status({}));
  await eventually("конец", async () => seen1.length === 2);
  assert.deepEqual(
    seen1.map((x) => [x.type, x.active, x.message]),
    [
      ["degraded", true, "диск"],
      ["degraded", false, "диск"],
    ],
  );
  assert.deepEqual(seen2, [], "второй процесс запись не делал — событий нет");
  assert.deepEqual(await two.agents.alerts(), []);
  await a.close();
});

test("cancelCommand: ждущая без отправки — без сообщения; отправленная и выполняющаяся — cmd.cancel; поздний итог не учитывается", async (t) => {
  const { agents, base } = await serve(t, { enrollToken: "t" });
  const audit: AuditEntry[] = [];
  agents.on("audit", (e) => audit.push(e));
  const { agentId, auth } = await enroll(base, "cancel-agent");

  // Ждущая и не отправленная (агент без связи): отмена без сообщения, при подключении не уходит.
  const waiting = await agents.command({ agentId, name: "example.cmd" });
  const call = agents.call({ agentId, name: "example.cmd" });
  await eventually("команда call создана", async () => (await agents.listCommands({ status: "pending" })).length === 2);
  const callId = (await agents.listCommands({ status: "pending", limit: 1 }))[0].id;
  const cancelled = await agents.by("ivan").cancelCommand(waiting.id);
  assert.equal(cancelled.status, "cancelled");
  assert.deepEqual(cancelled.error, { code: "CANCELLED", message: "Команду отменили" });
  assert.ok(cancelled.finishedAt);
  assert.equal("rev" in cancelled, false);
  await agents.cancelCommand(callId);
  assert.equal((await call).status, "cancelled", "call дожидается отмены");

  const a = await connect(base, auth, "cancel-agent", { capabilities: { commands: { names: ["example.cmd"] } } });
  // Отправленная: cmd.run ушёл — отмена шлёт cmd.cancel; поздний cmd.done ok:false CANCELLED — ack без ошибки.
  const sent = await agents.command({ agentId, name: "example.cmd" });
  await a.inbox.wait("cmd.run", (e) => e.data.commandId === sent.id);
  await agents.cancelCommand(sent.id);
  await a.inbox.wait("cmd.cancel", (e) => e.data.commandId === sent.id);
  const before = (await agents.getCommand(sent.id))!;
  const late = await a.reliable("cmd.done", {
    commandId: sent.id,
    ok: false,
    error: { code: "CANCELLED", message: "прервана" },
  });
  assert.equal(late.type, "ack");
  assert.deepEqual(await agents.getCommand(sent.id), before, "итог не изменился");

  // Выполняющаяся: cmd.accept → running; отмена — cmd.cancel; поздний успех не учитывается.
  const running = await agents.command({ agentId, name: "example.cmd" });
  await a.inbox.wait("cmd.run", (e) => e.data.commandId === running.id);
  a.stream("cmd.accept", { commandId: running.id });
  await eventually("running", async () => (await agents.getCommand(running.id))?.status === "running");
  await agents.cancelCommand(running.id);
  await a.inbox.wait("cmd.cancel", (e) => e.data.commandId === running.id);
  assert.equal((await a.reliable("cmd.done", { commandId: running.id, ok: true, result: 1 })).type, "ack");
  assert.equal((await agents.getCommand(running.id))?.status, "cancelled");
  assert.equal(a.inbox.all("cmd.cancel").length, 2, "ждущим без отправки cmd.cancel не уходил");
  assert.ok(!a.inbox.all("cmd.run").some((e) => e.data.commandId === waiting.id));

  await assert.rejects(agents.cancelCommand("nope"), { code: "COMMAND_NOT_FOUND", status: 404 });
  await assert.rejects(agents.cancelCommand(running.id), { code: "COMMAND_NOT_ACTIVE", status: 409 });
  const cancels = audit.filter((e) => e.action === "command.cancel");
  assert.deepEqual(cancels[0], { ...cancels[0], actor: "ivan", target: waiting.id, agentId });
  assert.equal(cancels.length, 4);
  assert.equal(cancels[1].actor, "");
  await a.close();
});

test("cancelCommand в процессе без сессии агента: cmd.cancel шлёт процесс сессии при refresh, один раз", async (t) => {
  const store = new MemoryStore();
  const one = await serve(t, { enrollToken: "t", store });
  const two = await serve(t, { enrollToken: "t", store });
  const { agentId, auth } = await enroll(one.base, "cancel-remote");
  const a = await connect(one.base, auth, "cancel-remote", { capabilities: { commands: { names: ["example.cmd"] } } });
  const cmd = await one.agents.command({ agentId, name: "example.cmd" });
  await a.inbox.wait("cmd.run", (e) => e.data.commandId === cmd.id);
  await two.agents.cancelCommand(cmd.id);
  await one.agents.refresh(agentId);
  await a.inbox.wait("cmd.cancel", (e) => e.data.commandId === cmd.id);
  await one.agents.refresh(agentId);
  await sleep(100);
  assert.equal(a.inbox.all("cmd.cancel").length, 1);
  await a.close();
});

test("cancelCommand: выполняющаяся с прошлой сессии в другом процессе — cmd.cancel при refresh, один раз", async (t) => {
  const store = new MemoryStore();
  const one = await serve(t, { enrollToken: "t", store });
  const two = await serve(t, { enrollToken: "t", store });
  const { agentId, auth } = await enroll(one.base, "cancel-prev");
  const caps = { capabilities: { commands: { names: ["example.cmd"] } } };
  const a = await connect(one.base, auth, "cancel-prev", caps);
  const cmd = await one.agents.command({ agentId, name: "example.cmd" });
  await a.inbox.wait("cmd.run", (e) => e.data.commandId === cmd.id);
  a.stream("cmd.accept", { commandId: cmd.id });
  await eventually("running", async () => (await store.getCommand(cmd.id))?.status === "running");
  await a.close();
  // Агент переподключился к другому процессу; команда выполняется с прошлой сессии.
  const b = await connect(two.base, auth, "cancel-prev", caps);
  await one.agents.cancelCommand(cmd.id);
  await two.agents.refresh(agentId);
  await b.inbox.wait("cmd.cancel", (e) => e.data.commandId === cmd.id);
  await two.agents.refresh(agentId);
  await sleep(100);
  assert.equal(b.inbox.all("cmd.cancel").length, 1);
  await b.close();
});

test("повторы условной записи исчерпаны — STORE_CONFLICT (409)", async () => {
  class ConflictStore extends MemoryStore {
    override async updateCommand(): Promise<boolean> {
      return false;
    }
  }
  const store = new ConflictStore();
  await store.createAgent(agentRecord("a"));
  const agents = new Agents({ enrollToken: "t", store, log: () => {} });
  try {
    const cmd = await agents.command({ agentId: "a", name: "example.cmd" });
    await assert.rejects(agents.cancelCommand(cmd.id), { code: "STORE_CONFLICT", status: 409 });
  } finally {
    agents.close();
  }
});

test("deleteAgent: только отозванного; запись и метрики удаляются, задачи и события остаются", async (t) => {
  const { agents, base } = await serve(t, { enrollToken: "t", metricsStoreIntervalMs: 0 });
  const audit: AuditEntry[] = [];
  const changes: Change[] = [];
  agents.on("audit", (e) => audit.push(e));
  agents.on("change", (c) => changes.push(c));
  const { agentId, auth } = await enroll(base, "delete-agent");
  const a = await connect(base, auth, "delete-agent");
  a.stream("metrics", { collectedAt: 1 });
  await a.reliable("event", { type: "example.e" });
  const job = await agents.enqueue({ queue: "example.q", agentId });
  await assert.rejects(agents.deleteAgent(agentId), { code: "AGENT_NOT_REVOKED", status: 409 });
  await assert.rejects(agents.deleteAgent("nope"), { code: "AGENT_NOT_FOUND", status: 404 });
  await agents.revoke(agentId);
  await agents.by("ivan").deleteAgent(agentId);
  assert.equal(await agents.getAgent(agentId), undefined);
  assert.deepEqual(await agents.listMetrics(agentId), []);
  assert.ok(await agents.getJob(job.id), "задачи остаются");
  assert.ok(
    (await agents.listEvents(0)).some((e) => e.agentId === agentId),
    "события остаются",
  );
  assert.deepEqual(
    audit.filter((e) => e.action === "agent.delete").map((e) => [e.actor, e.target, e.agentId]),
    [["ivan", agentId, agentId]],
  );
  assert.ok(changes.some((c) => c.kind === "agent" && c.id === agentId));
  await assert.rejects(agents.deleteAgent(agentId), { code: "AGENT_NOT_FOUND" });
});

test("регистрация: тело больше 64 КБ — 413, name и labels в пределах, хук получает name/labels/host", async (t) => {
  const got: unknown[] = [];
  const { base } = await serve(t, {
    enroll: (token, req) => (got.push(req), token === "t" ? { labels: { granted: "1" } } : null),
    enrollFailureLimit: 0,
  });
  const post = (body: unknown) =>
    fetch(base + ENROLL_PATH, { method: "POST", body: typeof body === "string" ? body : JSON.stringify(body) });

  const big = await post({ token: "t", name: "big", pad: "x".repeat(64 << 10) });
  assert.equal(big.status, 413);
  assert.equal((await big.json()).code, "MESSAGE_INVALID");

  const bad: unknown[] = [
    { token: "t", name: "n".repeat(129) },
    { token: "t", name: "" },
    { token: 1, name: "x" },
    { token: "t", name: "x", labels: Object.fromEntries(Array.from({ length: 65 }, (_, i) => [`k${i}`, "v"])) },
    { token: "t", name: "x", labels: { k: "v".repeat(257) } },
    { token: "t", name: "x", labels: { ["k".repeat(257)]: "v" } },
    { token: "t", name: "x", labels: { k: 1 } },
    { token: "t", name: "x", labels: ["a"] },
    "{не json",
  ];
  for (const body of bad) {
    const r = await post(body);
    assert.equal(r.status, 400, JSON.stringify(body));
    assert.equal((await r.json()).code, "MESSAGE_INVALID");
  }
  assert.deepEqual(got, [], "до хука отклонено всё некорректное");

  const labels = Object.fromEntries(Array.from({ length: 64 }, (_, i) => [`k${i}`, "v".repeat(256)]));
  const ok = await post({ token: "t", name: "н".repeat(128), labels, host: { os: "linux" } });
  assert.equal(ok.status, 201);
  assert.deepEqual(got, [{ name: "н".repeat(128), labels, host: { os: "linux" } }]);
});

test("регистрация: неудачи считаются по X-Forwarded-For только с trustProxy", async (t) => {
  const fail = (base: string, xff: string) =>
    fetch(base + ENROLL_PATH, {
      method: "POST",
      headers: { "X-Forwarded-For": xff },
      body: JSON.stringify({ token: "bad", name: "x" }),
    });
  const opts = { enrollToken: "t", enrollFailureLimit: 2, enrollFailureWindowMs: 60_000 };
  const direct = await serve(t, opts);
  assert.equal((await fail(direct.base, "203.0.113.1")).status, 401);
  assert.equal((await fail(direct.base, "203.0.113.2")).status, 401);
  assert.equal((await fail(direct.base, "203.0.113.3")).status, 429, "без trustProxy — один адрес сокета");

  const proxied = await serve(t, { ...opts, trustProxy: true });
  assert.equal((await fail(proxied.base, "203.0.113.1")).status, 401);
  assert.equal((await fail(proxied.base, "203.0.113.1, 10.0.0.1")).status, 401);
  assert.equal((await fail(proxied.base, "203.0.113.2")).status, 401, "другой клиент — свой счёт");
  assert.equal((await fail(proxied.base, "203.0.113.1:4000")).status, 429);
  await enroll(proxied.base, "ok-agent", "t", { "X-Forwarded-For": "203.0.113.9" });
});

test("baseUrl: X-Forwarded-Host и X-Forwarded-Proto — только с trustProxy", () => {
  const req = (headers: Record<string, string>, encrypted = false) =>
    ({ headers, socket: { encrypted } }) as unknown as IncomingMessage;
  const fwd = { host: "10.0.0.5:8080", "x-forwarded-host": "agents.example.com", "x-forwarded-proto": "https" };
  assert.equal(baseUrl(req(fwd)), "http://10.0.0.5:8080");
  assert.equal(baseUrl(req(fwd), true), "https://agents.example.com");
  assert.equal(baseUrl(req({ host: "a.example.com" }, true)), "https://a.example.com", "TLS сокета");
  assert.equal(baseUrl(req({ host: "a.example.com" }), true), "http://a.example.com", "нет заголовков — Host");
  assert.equal(
    baseUrl(
      req({ host: "h", "x-forwarded-host": "a.example.com, b.example.com", "x-forwarded-proto": "https, http" }),
      true,
    ),
    "https://a.example.com",
  );
});
