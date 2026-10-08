// API интерфейса поверх Agents: фейковый агент по WebSocket выполняет задачу с
// выходным файлом, снимок показывает её с files; состояние для агента; ошибки.
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { WebSocket } from "ws";
import { ENROLL_PATH, INSTALL_PATH, LINK_PATH, WS_CHANNEL, type Envelope } from "agent-sdk";
import { Agents, MemoryFiles } from "agent-sdk/server";
import { createApp, SUBSCRIPTION_INTERVAL_MS, SUBSCRIPTION_METRICS, WS_PATH, wsDefaults } from "../src/app";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const releasesDir = mkdtempSync(join(tmpdir(), "example-rel-"));
const manifest = {
  version: "9.9.9",
  artifacts: [{ os: "linux", arch: "amd64", file: "agent-linux-amd64", sha256: "00".repeat(32), signature: "c2ln" }],
  workers: [
    {
      name: "report",
      version: "1.1.0",
      os: "linux",
      arch: "amd64",
      file: "report-1.1.0-linux-amd64",
      sha256: "11".repeat(32),
    },
  ],
};
writeFileSync(join(releasesDir, "manifest.json"), JSON.stringify(manifest));
writeFileSync(join(releasesDir, "agent-linux-amd64"), "BIN");
writeFileSync(join(releasesDir, "install.sh"), '#!/bin/sh\nDEFAULT_SERVER=""\nDEFAULT_PUBLIC_KEY=""\n');

const files = new MemoryFiles();
// metricsStoreIntervalMs: 0 — в истории каждая точка (тесты шлют точки чаще прореживания).
const agents = new Agents({
  enrollToken: "it",
  files,
  releasesDir,
  publicKey: "S0VZ",
  offlineGraceMs: 100,
  metricsStoreIntervalMs: 0,
});
const server = createApp(agents, files, "/nonexistent", { enrollToken: "it" });
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

async function call(method: string, path: string, body?: unknown, headers: Record<string, string> = {}) {
  const res = await fetch(base + path, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: res.status, body: await res.json() };
}

/** Агент: регистрация, hello, входящие с ожиданием. */
async function agent(
  name: string,
  capabilities: Record<string, unknown> = { state: { domains: { "app.dom": null } } },
) {
  const { agentId, secret } = await (
    await fetch(base + ENROLL_PATH, { method: "POST", body: JSON.stringify({ token: "it", name }) })
  ).json();
  const ws = new WebSocket(base.replace("http", "ws") + LINK_PATH, WS_CHANNEL, {
    headers: { Authorization: `Agent ${agentId}.${secret}` },
  });
  const got: Envelope[] = [];
  ws.on("message", (raw) => got.push(JSON.parse(raw.toString())));
  await new Promise((r, j) => ws.once("open", r).once("error", j));
  const wait = async (type: string) => {
    for (let i = 0; i < 150; i++) {
      const hit = got.find((e) => e.type === type);
      if (hit) return hit;
      await new Promise((r) => setTimeout(r, 20));
    }
    throw new Error(`нет ${type}`);
  };
  const send = (env: Envelope) => ws.send(JSON.stringify(env));
  send({
    type: "hello",
    data: {
      versions: [1],
      agent: { name, version: "t", bootId: "b", startedAt: 0 },
      host: { hostname: name, os: "linux", arch: "amd64" },
      capabilities,
      jobs: [],
    },
  });
  await wait("welcome");
  /** Все config агенту, по порядку. */
  const configs = () => got.filter((e) => e.type === "config").map((e) => e.data);
  return { agentId, ws, wait, send, configs };
}

test("задача с выходным файлом: снимок показывает files, файл скачивается", async () => {
  const a = await agent("app-1");
  a.send({ type: "status", seq: 1, data: { state: "idle", slots: { "app.q": 1 }, jobs: [], workers: [], outbox: 0 } });
  const job = await call("POST", "/api/jobs", {
    queue: "app.q",
    data: { x: 1 },
    inputs: { src: "вход" },
    outputs: ["out"],
  });
  assert.equal(job.status, 201);
  const assign = await a.wait("job.assign");
  assert.equal(assign.data.jobId, job.body.id);
  await fetch(assign.data.outputs.out.url, { method: "PUT", body: "выход" });
  a.send({ type: "job.complete", id: "c1", data: { jobId: job.body.id, attempt: 0, result: { ok: true } } });
  await a.wait("ack");

  const snap = (await call("GET", "/api/snapshot")).body;
  assert.deepEqual(Object.keys(snap).sort(), [
    "agents",
    "alerts",
    "commands",
    "events",
    "jobs",
    "serverTime",
    "states",
  ]);
  const j = snap.jobs.find((x: any) => x.id === job.body.id);
  assert.equal(j.status, "completed");
  assert.deepEqual(j.files, ["out"]);
  assert.equal(await (await fetch(`${base}/files/${job.body.id}/out/out`)).text(), "выход");
  assert.equal((await call("GET", `/api/jobs/${job.body.id}`)).body.status, "completed");
  assert.equal((await call("POST", `/api/jobs/${job.body.id}/cancel`)).status, 409);
  a.ws.close();
});

test("состояние: общее и для агента (?agentId=); ошибки API — код и статус", async () => {
  const a = await agent("app-2");
  const common = await call("PUT", "/api/state/app.dom", { v: 1 });
  assert.equal(common.status, 200);
  const own = await call("PUT", `/api/state/app.dom?agentId=${a.agentId}`, { v: 2 });
  assert.equal(own.body.agentId, a.agentId);
  assert.ok(own.body.version > common.body.version);
  const states = (await call("GET", "/api/snapshot")).body.states.filter((s: any) => s.domain === "app.dom");
  assert.equal(states.length, 2);
  assert.equal((await call("PUT", "/api/state/app.dom?agentId=nope", {})).status, 404);

  const cmd = await call("POST", "/api/commands", { name: "nobody.has" });
  assert.deepEqual([cmd.status, cmd.body.code], [404, "COMMAND_NOT_SUPPORTED"]);
  assert.equal((await call("GET", "/api/commands/nope")).status, 404);
  assert.equal((await call("GET", "/api/nope")).status, 404);
  a.ws.close();
});

test("DELETE /api/state: личный → общий заново (агенту и в поток), без agentId — общий удалён (stateDeleted)", async () => {
  const c = await streamClient();
  await c.wait((m) => m.type === "snapshot", "snapshot");
  const a = await agent("app-del", { state: { domains: { "app.del": null } } });
  await call("PUT", "/api/state/app.del", { v: 1 });
  const own = await call("PUT", `/api/state/app.del?agentId=${a.agentId}`, { v: 2 });
  await a.wait("state.put");

  const back = await call("DELETE", `/api/state/app.del?agentId=${a.agentId}`);
  assert.equal(back.status, 200);
  assert.ok(back.body.state.version > own.body.version);
  assert.equal(back.body.state.agentId, undefined);
  await c.wait(
    (m) => m.type === "stateDeleted" && m.domain === "app.del" && m.agentId === a.agentId,
    "stateDeleted личного",
  );
  await c.wait((m) => m.type === "state" && m.data.version === back.body.state.version, "state переизданного общего");
  // Личного уже нет — текущий общий без новой версии, в поток ничего.
  const again = await call("DELETE", `/api/state/app.del?agentId=${a.agentId}`);
  assert.equal(again.body.state.version, back.body.state.version);

  const gone = await call("DELETE", "/api/state/app.del");
  assert.deepEqual([gone.status, gone.body], [200, { state: null }]);
  await c.wait((m) => m.type === "stateDeleted" && m.domain === "app.del" && !("agentId" in m), "stateDeleted общего");
  assert.equal(
    (await call("GET", "/api/snapshot")).body.states.some((s: any) => s.domain === "app.del"),
    false,
  );
  assert.deepEqual((await call("DELETE", "/api/state/app.del")).body, { state: null }, "снимка не было — не ошибка");
  await sleep(50);
  assert.equal(c.got.filter((m) => m.type === "stateDeleted").length, 2, "stateDeleted — только когда что-то удалено");
  assert.equal(
    c.got.filter((m) => m.type === "state" && m.data.domain === "app.del" && !m.data.agentId).length,
    2,
    "повтор не переиздаёт общий",
  );
  c.ws.close();
  a.ws.close();
});

/** Клиент потока /api/ws: входящие копятся, ожидание по условию. */
async function streamClient() {
  const ws = new WebSocket(base.replace("http", "ws") + WS_PATH);
  const got: any[] = [];
  ws.on("message", (raw) => got.push(JSON.parse(raw.toString())));
  await new Promise((r, j) => ws.once("open", r).once("error", j));
  const wait = async (pred: (m: any) => boolean, what: string) => {
    for (let i = 0; i < 150; i++) {
      const hit = got.find(pred);
      if (hit) return hit;
      await sleep(20);
    }
    throw new Error(`нет ${what}: ${JSON.stringify(got.map((m) => m.type))}`);
  };
  const send = (msg: unknown) => ws.send(JSON.stringify(msg));
  return { ws, got, wait, send };
}

test("поток /api/ws: snapshot сразу, затем дельты agent/job (с files)/command/state/event", async () => {
  const c = await streamClient();
  const snap = await c.wait((m) => m.type === "snapshot", "snapshot");
  assert.deepEqual(Object.keys(snap.data).sort(), [
    "agents",
    "alerts",
    "commands",
    "events",
    "jobs",
    "serverTime",
    "states",
  ]);
  assert.equal(c.got[0].type, "snapshot", "snapshot — первым");

  const a = await agent("ws-delta", { state: { domains: { "app.dom": null } }, commands: { names: ["ws.cmd"] } });
  await c.wait((m) => m.type === "agent" && m.data.id === a.agentId && m.data.online, "agent online");
  a.send({ type: "status", seq: 1, data: { state: "idle", slots: { "ws.q": 1 }, jobs: [], workers: [], outbox: 0 } });
  const job = await call("POST", "/api/jobs", { queue: "ws.q", outputs: ["out"] });
  const assign = await a.wait("job.assign");
  await fetch(assign.data.outputs.out.url, { method: "PUT", body: "x" });
  await c.wait((m) => m.type === "job" && m.data.id === job.body.id && m.data.files?.includes("out"), "job с files");
  const cmd = await call("POST", "/api/commands", { name: "ws.cmd", agentId: a.agentId });
  await c.wait((m) => m.type === "command" && m.data.id === cmd.body.id, "command");
  await call("PUT", "/api/state/app.ws", { v: 1 });
  await c.wait((m) => m.type === "state" && m.data.domain === "app.ws", "state");
  a.send({ type: "event", id: "e1", data: { source: "w", type: "ws.happened" } });
  await c.wait((m) => m.type === "event" && m.data.type === "ws.happened", "event");
  assert.equal(c.got.filter((m) => m.type === "metrics").length, 0, "метрики — только подписанным");
  c.ws.close();
  a.ws.close();
});

test("поток /api/ws: subscribe — подписка клиента и точки metrics (в т. ч. backfill), продление, снятие", async () => {
  wsDefaults.subscriptionRenewMs = 100;
  const renewals: string[] = [];
  const orig = agents.subscribe.bind(agents);
  agents.subscribe = (id, o) => (renewals.push(`${id}/${o?.id}`), orig(id, o));
  try {
    const a = await agent("ws-metrics", { telemetry: { channels: ["example.app"] } });
    const subs = async () => (await agents.getAgent(a.agentId))?.subscriptions ?? [];
    const c = await streamClient();
    const other = await streamClient();
    await c.wait((m) => m.type === "snapshot", "snapshot");
    c.send({ type: "subscribe", agentId: a.agentId });
    other.send({ type: "subscribe", agentId: a.agentId });
    await waitFor(async () => (await subs()).length === 2, "подписка у каждого клиента");
    assert.deepEqual(
      (await a.wait("config")).data.subscription,
      {
        metricsIntervalMs: SUBSCRIPTION_INTERVAL_MS,
        metrics: SUBSCRIPTION_METRICS,
        logLevel: "info",
        channels: { "example.app": SUBSCRIPTION_INTERVAL_MS },
      },
      "subscribe → сводная агенту сразу: метрики, группы, каналы воркеров, лог",
    );
    const now = Date.now();
    a.send({ type: "metrics", seq: 1, data: { collectedAt: now - 500, clockOffsetMs: 0, host: { cpuPercent: 3 } } });
    a.send({ type: "metrics", seq: 2, data: { collectedAt: now - 60_000, clockOffsetMs: 0, backfill: true } });
    const live = await c.wait((m) => m.type === "metrics" && !m.point.backfill, "metrics");
    assert.equal(live.agentId, a.agentId);
    assert.equal(live.point.at, now - 500);
    assert.equal(live.point.metrics.host.cpuPercent, 3);
    assert.equal((await c.wait((m) => m.type === "metrics" && m.point.backfill, "backfill")).point.at, now - 60_000);
    await sleep(250);
    const ids = new Set(renewals.filter((r) => r.startsWith(`${a.agentId}/`)));
    assert.equal(ids.size, 2, "у каждого клиента своя подписка");
    for (const id of ids) assert.ok(renewals.filter((r) => r === id).length >= 2, "подписка продлевается");
    assert.equal(a.configs().length, 1, "продление без повторного config");

    // Один клиент отписался — его подписка снята, точки — только второму.
    c.send({ type: "unsubscribe", agentId: a.agentId });
    await waitFor(async () => (await subs()).length === 1, "подписка снята");
    const before = c.got.length;
    const t3 = Date.now();
    a.send({ type: "metrics", seq: 3, data: { collectedAt: t3, clockOffsetMs: 0 } });
    await other.wait((m) => m.type === "metrics" && m.point.at === t3, "точка второму");
    await sleep(50);
    assert.equal(c.got.slice(before).filter((m) => m.type === "metrics").length, 0, "отписанному точки не идут");

    // Закрытие соединения снимает подписки: сводная пуста, продления нет.
    other.ws.close();
    await waitFor(async () => (await subs()).length === 0, "подписок нет");
    await waitFor(async () => JSON.stringify(a.configs().at(-1)) === '{"subscription":{}}', "config без подписок");
    const last = renewals.length;
    await sleep(300);
    assert.equal(renewals.length, last, "подписок нет — продления нет");
    c.ws.close();
    a.ws.close();
  } finally {
    agents.subscribe = orig;
    wsDefaults.subscriptionRenewMs = 20_000;
  }
});

test("поток /api/ws: subscribe — лог с уровня клиента, logLevel меняет подписку клиента", async () => {
  const a = await agent("ws-logs", {});
  const levels = async () => ((await agents.getAgent(a.agentId))?.subscriptions ?? []).map((s) => s.logs?.level).sort();
  const c = await streamClient();
  const other = await streamClient();
  await c.wait((m) => m.type === "snapshot", "snapshot");
  await other.wait((m) => m.type === "snapshot", "snapshot");
  c.send({ type: "subscribe", agentId: a.agentId, logLevel: "warn" });
  await waitFor(async () => JSON.stringify(await levels()) === '["warn"]', "подписка warn");
  other.send({ type: "subscribe", agentId: a.agentId }); // по умолчанию info
  await waitFor(async () => JSON.stringify(await levels()) === '["info","warn"]', "подписка info");
  await waitFor(async () => a.configs().at(-1)?.subscription?.logLevel === "info", "сводная — самый подробный");
  const entries = [
    { at: 1, level: "info", source: "agent", msg: "подключён" },
    { at: 2, level: "error", source: "report", msg: "порт занят" },
  ];
  a.send({ type: "log", seq: 1, data: { entries } });
  const mine = await c.wait((m) => m.type === "log", "log клиенту warn");
  assert.deepEqual(mine, { type: "log", agentId: a.agentId, entries: [entries[1]] }, "только с уровня клиента");
  assert.deepEqual((await other.wait((m) => m.type === "log", "log клиенту info")).entries, entries);
  c.send({ type: "logLevel", agentId: a.agentId, level: "debug" });
  await waitFor(async () => JSON.stringify(await levels()) === '["debug","info"]', "подписка debug");
  c.send({ type: "unsubscribe", agentId: a.agentId });
  await waitFor(async () => JSON.stringify(await levels()) === '["info"]', "осталась подписка второго");
  const before = c.got.length;
  a.send({ type: "log", seq: 2, data: { entries: [{ at: 3, level: "error", source: "agent", msg: "ещё" }] } });
  await other.wait((m) => m.type === "log" && m.entries[0].msg === "ещё", "log второму");
  await sleep(50);
  assert.equal(c.got.slice(before).filter((m) => m.type === "log").length, 0, "отписанному log не идёт");
  c.ws.close();
  other.ws.close();
  a.ws.close();
});

async function waitFor(probe: () => Promise<boolean>, what: string) {
  for (let i = 0; i < 150; i++) {
    if (await probe()) return;
    await sleep(20);
  }
  throw new Error(`нет ${what}`);
}

test("поток: чужой upgrade и неизвестный путь — 404", async () => {
  const code = await new Promise<number>((r) => {
    const ws = new WebSocket(base.replace("http", "ws") + "/api/nope");
    ws.on("unexpected-response", (_q, res) => r(res.statusCode ?? 0));
    ws.on("error", () => r(-1));
  });
  assert.equal(code, 404);
  assert.equal((await call("GET", "/api/stream")).status, 404);
});

test("агент: история метрик (since), подписка → config и её снятие, отзыв", async () => {
  const a = await agent("app-metrics");
  const now = Date.now();
  a.send({
    type: "metrics",
    seq: 1,
    data: {
      collectedAt: now,
      clockOffsetMs: 0,
      host: { cpuPercent: 5, interfaces: [{ name: "eth0", rxBps: 1, txBps: 2 }] },
    },
  });
  a.send({
    type: "metrics",
    seq: 2,
    data: { collectedAt: now - 30_000, clockOffsetMs: 0, backfill: true, host: { cpuPercent: 7 } },
  });
  await a.wait("ack");
  await new Promise((r) => setTimeout(r, 50));
  const all = await call("GET", `/api/agents/${a.agentId}/metrics`);
  assert.equal(all.status, 200);
  assert.deepEqual(
    all.body.map((p: any) => p.backfill),
    [true, false],
  );
  assert.equal(all.body[1].metrics.host.interfaces[0].name, "eth0");
  const later = await call("GET", `/api/agents/${a.agentId}/metrics?since=${all.body[0].at}`);
  assert.equal(later.body.length, 1);
  assert.equal((await call("GET", "/api/agents/nope/metrics")).status, 404);

  const sub = await call("POST", `/api/agents/${a.agentId}/subscriptions`, {
    metrics: { intervalMs: 1000, groups: ["sockets"] },
  });
  assert.equal(sub.status, 200);
  assert.ok(sub.body.id && sub.body.until > Date.now());
  assert.deepEqual((await a.wait("config")).data, { subscription: { metricsIntervalMs: 1000, metrics: ["sockets"] } });
  const bad = await call("POST", `/api/agents/${a.agentId}/subscriptions`, { status: { intervalMs: 10 } });
  assert.equal(bad.status, 400);
  assert.equal(bad.body.code, "MESSAGE_INVALID");
  assert.equal((await call("POST", "/api/agents/nope/subscriptions", {})).status, 404);
  const del = await call("DELETE", `/api/agents/${a.agentId}/subscriptions/${encodeURIComponent(sub.body.id)}`);
  assert.equal(del.status, 200);
  await waitFor(async () => JSON.stringify(a.configs().at(-1)) === '{"subscription":{}}', "config без подписок");
  assert.equal((await call("DELETE", "/api/agents/nope/subscriptions/x")).status, 404);

  const r = await call("POST", `/api/agents/${a.agentId}/revoke`);
  assert.equal(r.status, 200);
  assert.equal(r.body.revoked, true);
  assert.equal((await call("POST", "/api/agents/nope/revoke")).status, 404);
});

test("выпуск: /api/releases (манифест, кандидаты, команда установки), обновление агента, install.sh", async () => {
  const a = await agent("app-upd", { update: { mode: "self" }, commands: { names: ["agent.update"] } });
  const rel = await call("GET", "/api/releases");
  assert.equal(rel.status, 200);
  assert.deepEqual(rel.body.release, manifest);
  assert.deepEqual(
    rel.body.candidates.map((c: any) => [c.name, c.current, c.target]),
    [["app-upd", "t", "9.9.9"]],
  );
  assert.deepEqual(rel.body.workerCandidates, []);
  assert.equal(rel.body.installCommand, `curl -fsSL '${base}${INSTALL_PATH}' | sudo sh -s -- --token 'it'`);
  const upd = await call("POST", `/api/agents/${a.agentId}/update`);
  assert.equal(upd.status, 201);
  assert.equal(upd.body.name, "agent.update");
  const run = await a.wait("cmd.run");
  assert.equal(run.data.args.url, "/api/v1/agent-link/releases/agent-linux-amd64");
  const plain = await agent("app-plain", {});
  const no = await call("POST", `/api/agents/${plain.agentId}/update`);
  assert.deepEqual([no.status, no.body.code], [409, "UPDATE_NOT_AVAILABLE"]);
  const sh = await (await fetch(base + INSTALL_PATH)).text();
  assert.match(sh, new RegExp(`DEFAULT_SERVER="${base}"`));
  assert.match(sh, /DEFAULT_PUBLIC_KEY="S0VZ"/);
  a.ws.close();
  plain.ws.close();
});

test("воркеры из выпуска: workerCandidates в /api/releases, POST /api/agents/:id/workers/:name/update", async () => {
  const a = await agent("app-wupd", { update: { mode: "self" }, commands: { names: ["worker.update"] } });
  a.send({
    type: "status",
    seq: 1,
    data: {
      state: "idle",
      slots: {},
      jobs: [],
      outbox: 0,
      workers: [
        { name: "report", state: "running", instances: 1, version: "1.0.0", release: true },
        { name: "plain", state: "running", instances: 1, version: "1.0.0" },
      ],
    },
  });
  for (let i = 0; i < 100 && !(await agents.getAgent(a.agentId))?.status; i++) await sleep(20);
  const rel = await call("GET", "/api/releases");
  assert.deepEqual(
    rel.body.workerCandidates.map((c: any) => [c.agentName, c.worker, c.current, c.target]),
    [["app-wupd", "report", "1.0.0", "1.1.0"]],
  );
  const upd = await call("POST", `/api/agents/${a.agentId}/workers/report/update`, undefined, { "X-Actor": "ivan" });
  assert.equal(upd.status, 201);
  assert.equal(upd.body.name, "worker.update");
  assert.equal(upd.body.actor, "ivan");
  const run = await a.wait("cmd.run");
  assert.deepEqual(run.data.args, {
    name: "report",
    version: "1.1.0",
    url: "/api/v1/agent-link/releases/report-1.1.0-linux-amd64",
    sha256: "11".repeat(32),
    signature: "",
  });
  const no = await call("POST", `/api/agents/${a.agentId}/workers/plain/update`);
  assert.deepEqual([no.status, no.body.code], [409, "UPDATE_NOT_AVAILABLE"]);
  a.ws.close();
});

test("X-Actor → by(actor): поле actor и audit в потоке; без заголовка — web", async () => {
  const c = await streamClient();
  await c.wait((m) => m.type === "snapshot", "snapshot");
  const st = await call("PUT", "/api/state/app.actor", { v: 1 }, { "X-Actor": "ivan" });
  assert.equal(st.body.actor, "ivan");
  const audit = await c.wait((m) => m.type === "audit" && m.data.target === "app.actor", "audit");
  assert.deepEqual([audit.data.actor, audit.data.action], ["ivan", "state.set"]);
  const job = await call("POST", "/api/jobs", { queue: "app.actor" });
  assert.equal(job.body.actor, "web");
  await call("POST", `/api/jobs/${job.body.id}/cancel`, undefined, { "X-Actor": "petr" });
  const cancel = await c.wait((m) => m.type === "audit" && m.data.action === "job.cancel", "audit cancel");
  assert.deepEqual([cancel.data.actor, cancel.data.target], ["petr", job.body.id]);
  c.ws.close();
});

test("история состояния и откат: /history (agentId, limit), /rollback → новая версия; нет версии — 404", async () => {
  const a = await agent("app-hist");
  const v1 = (await call("PUT", "/api/state/app.hist", { n: 1 })).body;
  const v2 = (await call("PUT", "/api/state/app.hist", { n: 2 })).body;
  const own = (await call("PUT", `/api/state/app.hist?agentId=${a.agentId}`, { n: 9 })).body;
  const hist = await call("GET", "/api/state/app.hist/history");
  assert.deepEqual(
    hist.body.map((s: any) => s.version),
    [v2.version, v1.version],
  );
  assert.equal((await call("GET", "/api/state/app.hist/history?limit=1")).body.length, 1);
  assert.deepEqual(
    (await call("GET", `/api/state/app.hist/history?agentId=${a.agentId}`)).body.map((s: any) => s.version),
    [own.version],
  );
  const back = await call("POST", "/api/state/app.hist/rollback", { version: v1.version }, { "X-Actor": "ivan" });
  assert.equal(back.status, 200);
  assert.deepEqual(back.body.spec, { n: 1 });
  assert.equal(back.body.actor, "ivan");
  assert.ok(back.body.version > v2.version);
  const ownBack = await call("POST", "/api/state/app.hist/rollback", { version: own.version, agentId: a.agentId });
  assert.equal(ownBack.body.agentId, a.agentId);
  const miss = await call("POST", "/api/state/app.hist/rollback", { version: 1 });
  assert.deepEqual([miss.status, miss.body.code], [404, "STATE_VERSION_NOT_FOUND"]);
  assert.equal((await call("POST", "/api/state/app.hist/rollback", {})).status, 400);
  a.ws.close();
});

test("rotate-key: команда agent.rotateKey агенту; не объявил — COMMAND_NOT_SUPPORTED", async () => {
  const a = await agent("app-rot", { commands: { names: ["agent.rotateKey"] } });
  const cmd = await call("POST", `/api/agents/${a.agentId}/rotate-key`);
  assert.equal(cmd.status, 201);
  assert.deepEqual([cmd.body.name, cmd.body.actor], ["agent.rotateKey", "web"]);
  assert.equal((await a.wait("cmd.run")).data.commandId, cmd.body.id);
  const b = await agent("app-norot", {});
  const no = await call("POST", `/api/agents/${b.agentId}/rotate-key`);
  assert.equal(no.body.code, "COMMAND_NOT_SUPPORTED");
  assert.equal((await call("POST", "/api/agents/nope/rotate-key")).status, 404);
  a.ws.close();
  b.ws.close();
});

test("уведомления: alert в потоке, активные — в snapshot.alerts", async () => {
  const c = await streamClient();
  await c.wait((m) => m.type === "snapshot", "snapshot");
  const a = await agent("app-alert", {});
  a.send({
    type: "status",
    seq: 1,
    data: { state: "degraded", message: "плохо", slots: {}, jobs: [], workers: [], outbox: 0 },
  });
  const on = await c.wait((m) => m.type === "alert" && m.data.agentId === a.agentId, "alert");
  assert.deepEqual([on.data.type, on.data.active, on.data.message], ["degraded", true, "плохо"]);
  const snap = (await call("GET", "/api/snapshot")).body;
  assert.ok(snap.alerts.some((x: any) => x.agentId === a.agentId && x.type === "degraded"));
  a.send({ type: "status", seq: 2, data: { state: "idle", slots: {}, jobs: [], workers: [], outbox: 0 } });
  await c.wait((m) => m.type === "alert" && m.data.agentId === a.agentId && !m.data.active, "alert конец");
  assert.equal(
    (await call("GET", "/api/snapshot")).body.alerts.some((x: any) => x.agentId === a.agentId),
    false,
  );
  c.ws.close();
  a.ws.close();
});

test("пауза воркера: POST /api/agents/:id/workers/:name/pause|resume → worker.pause/resume, actor", async () => {
  const a = await agent("app-pause", { commands: { names: ["worker.pause", "worker.resume"] } });
  const p = await call("POST", `/api/agents/${a.agentId}/workers/report/pause`, undefined, { "X-Actor": "ivan" });
  assert.equal(p.status, 201);
  assert.deepEqual([p.body.name, p.body.actor, p.body.args], ["worker.pause", "ivan", { name: "report" }]);
  assert.deepEqual((await a.wait("cmd.run")).data.args, { name: "report" });
  const r = await call("POST", `/api/agents/${a.agentId}/workers/report/resume`, { queues: ["example.echo"] });
  assert.equal(r.status, 201);
  assert.deepEqual(
    [r.body.name, r.body.actor, r.body.args],
    ["worker.resume", "web", { name: "report", queues: ["example.echo"] }],
  );
  const bad = await call("POST", `/api/agents/nope/workers/report/pause`);
  assert.deepEqual([bad.status, bad.body.code], [404, "AGENT_NOT_FOUND"]);
  a.ws.close();
});
