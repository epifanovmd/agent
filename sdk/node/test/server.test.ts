// Сценарии серверной части с фейковым агентом: регистрация, связь, поток и важное, fetch,
// настройки, watch, метрики, проблемы, смена ключа, выпуск.
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { connect } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, afterEach, before, describe, it } from "node:test";

import WebSocket from "ws";

import {
  type ActionRecord,
  agentsDefaults,
  type AlertEvent,
  type ConfigStatus,
  MAX_CONFIG_BYTES,
  MAX_FETCH_BODY,
  type MetricsEvent,
  transportDefaults,
} from "../src/server/index";
import {
  enroll,
  FakeAgent,
  sample,
  sleep,
  startServer,
  type TestServer,
  until,
} from "./helpers";

agentsDefaults.sweepIntervalMs = 50;
agentsDefaults.fetchSlackMs = 0;
transportDefaults.helloTimeoutMs = 200;

const servers: TestServer[] = [];
const server = async (
  opts: Parameters<typeof startServer>[0] = {},
): Promise<TestServer> => {
  const s = await startServer(opts);

  servers.push(s);

  return s;
};

afterEach(async () => {
  for (const s of servers.splice(0)) await s.close();
});

const post = (
  url: string,
  body: string,
  headers: Record<string, string> = {},
) =>
  fetch(`${url}/api/v1/agent-link/enroll`, {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body,
  });
const good = (extra: Record<string, unknown> = {}) =>
  JSON.stringify({
    token: "enroll-token-example",
    name: "n",
    host: { os: "linux", arch: "amd64", hostname: "n" },
    ...extra,
  });

describe("регистрация", () => {
  it("ошибки тела и токена", async () => {
    const s = await server();
    let r = await post(s.url, "{");

    assert.equal(r.status, 400);
    assert.equal((await r.json()).code, "MESSAGE_INVALID");
    r = await post(s.url, JSON.stringify({ token: "enroll-token-example" }));
    assert.equal(r.status, 400);
    r = await post(s.url, good({ name: "x".repeat(129) }));
    assert.equal(r.status, 400);
    r = await post(
      s.url,
      good({
        labels: Object.fromEntries(
          Array.from({ length: 65 }, (_, i) => [`k${i}`, "v"]),
        ),
      }),
    );
    assert.equal(r.status, 400);
    r = await post(s.url, good({ pad: "x".repeat(70 * 1024) }));
    assert.equal(r.status, 413);
    assert.equal((await r.json()).code, "BODY_TOO_LARGE");
    r = await post(s.url, good({ token: "wrong" }));
    assert.equal(r.status, 401);
    assert.equal((await r.json()).code, "ENROLL_DENIED");
    r = await post(s.url, good());
    assert.equal(r.status, 200);
  });

  it("предел неудачных попыток с адреса; trustProxy — адрес из X-Forwarded-For", async () => {
    const s = await server({ enrollFailureLimit: 2, trustProxy: true });
    const xff = (ip: string) => ({ "x-forwarded-for": ip });

    assert.equal(
      (await post(s.url, good({ token: "bad" }), xff("10.0.0.1"))).status,
      401,
    );
    assert.equal(
      (await post(s.url, good({ token: "bad" }), xff("10.0.0.1"))).status,
      401,
    );
    const r = await post(s.url, good(), xff("10.0.0.1"));

    assert.equal(r.status, 429);
    assert.equal((await r.json()).code, "RATE_LIMITED");
    assert.ok(Number(r.headers.get("retry-after")) >= 1);
    // Другой адрес — свой счёт.
    assert.equal((await post(s.url, good(), xff("10.0.0.2"))).status, 200);
  });

  it("предел неудач: после окна адрес снова принимается", async () => {
    const s = await server({
      enrollFailureLimit: 1,
      enrollFailureWindowMs: 100,
      trustProxy: true,
    });
    const xff = { "x-forwarded-for": "10.0.0.3" };

    assert.equal((await post(s.url, good({ token: "bad" }), xff)).status, 401);
    assert.equal((await post(s.url, good(), xff)).status, 429);
    await sleep(150);
    assert.equal((await post(s.url, good(), xff)).status, 200);
  });

  it("хук enroll: решение, метки и имя от сервера", async () => {
    const seen: unknown[] = [];
    const s = await server({
      enrollToken: undefined,
      enroll: (token, info) => {
        seen.push(info);

        return token === "t-1"
          ? { labels: { team: "example" }, name: "example-node" }
          : null;
      },
    });

    assert.equal((await post(s.url, good({ token: "t-2" }))).status, 401);
    const r = await post(
      s.url,
      good({ token: "t-1", labels: { zone: "eu", team: "x" } }),
    );
    const { agentId } = await r.json();
    const a = await s.agents.getAgent(agentId);

    assert.equal(a?.name, "example-node");
    assert.deepEqual(a?.labels, { zone: "eu", team: "example" });
    assert.equal((seen[1] as { address: string }).address, "127.0.0.1");
  });
});

describe("связь", () => {
  it("401 без ключа, 426 без подпротокола", async () => {
    const s = await server();
    const c = await enroll(s.url);

    await assert.rejects(
      FakeAgent.open(s.ws, { ...c, secret: "x" }),
      /HTTP 401/,
    );
    await assert.rejects(FakeAgent.open(s.ws, c, "agent.v1"), /HTTP 426/);
  });

  it("4400: нет hello, первое — не hello, не JSON", async () => {
    const s = await server();
    const c = await enroll(s.url);
    let fa = await FakeAgent.open(s.ws, c);

    assert.equal((await fa.closed).code, 4400);
    fa = await FakeAgent.open(s.ws, c);
    fa.send({ type: "status", seq: 1, data: { workers: [] } });
    assert.equal((await fa.closed).code, 4400);
    fa = await FakeAgent.open(s.ws, c);
    fa.ws.send("не json");
    assert.equal((await fa.closed).code, 4400);
  });

  it("второе подключение — прежнее закрывается 4409; отзыв — 4401 и 401", async () => {
    const s = await server();
    const c = await enroll(s.url);
    const a1 = await FakeAgent.connect(s.ws, c);
    const a2 = await FakeAgent.connect(s.ws, c);

    assert.equal((await a1.closed).code, 4409);
    const ag = await s.agents.getAgent(c.agentId);

    assert.equal(ag?.online, true);
    assert.equal(ag?.address, "127.0.0.1");
    assert.equal(ag?.version, "1.0.0");
    assert.equal(ag?.workers.find(w => w.name === "report")?.release, true);
    await s.agents.by("admin").revoke(c.agentId);
    assert.equal((await a2.closed).code, 4401);
    await assert.rejects(FakeAgent.open(s.ws, c), /HTTP 401/);
  });

  it("остановка сервера — 1012", async () => {
    const s = await startServer();
    const c = await enroll(s.url);
    const fa = await FakeAgent.connect(s.ws, c);

    await s.close();
    assert.equal((await fa.closed).code, 1012);
    assert.equal((await s.agents.getAgent(c.agentId))?.online, false);
  });

  it("offline после обрыва и проблема offline; переподключение её заканчивает", async () => {
    const s = await server({ offlineGraceMs: 0 });
    const c = await enroll(s.url);
    const alerts: AlertEvent[] = [];

    s.agents.on("alert", a => alerts.push(a));
    const fa = await FakeAgent.connect(s.ws, c);

    await fa.close();
    await until(
      async () => (await s.agents.getAgent(c.agentId))?.online === false,
    );
    assert.deepEqual(
      (await s.agents.listAlerts()).map(a => a.type),
      ["offline"],
    );
    await FakeAgent.connect(s.ws, c);
    await until(() => alerts.length === 2);
    assert.deepEqual(
      alerts.map(a => [a.type, a.active]),
      [
        ["offline", true],
        ["offline", false],
      ],
    );
  });
});

describe("поток и важное", () => {
  it("повтор seq не обрабатывается; новый bootId — нумерация заново", async () => {
    const s = await server();
    const c = await enroll(s.url);
    let n = 0;

    s.agents.on("log", () => (n += 1));
    const fa = await FakeAgent.connect(s.ws, c);
    const log = {
      entries: [{ at: 1, level: "warn", source: "agent", msg: "x" }],
    };

    fa.stream("log", log, 5);
    await fa.ackSeq(5);
    fa.stream("log", log, 5);
    fa.stream("log", log, 3);
    assert.equal((await fa.ackSeq(5)).data.seq, 5);
    assert.equal(n, 1);
    await fa.close();
    const fb = await FakeAgent.connect(s.ws, c, {
      agent: { version: "1.0.0", bootId: "new-boot", startedAt: 2 },
    });

    fb.stream("log", log, 1);
    await fb.ackSeq(1);
    assert.equal(n, 2);
  });

  it("событие с тем же id — одно; неверное — error без повтора", async () => {
    const s = await server();
    const c = await enroll(s.url);
    const got: string[] = [];

    s.agents.on("event", e => got.push(`${e.agentId}/${e.worker}/${e.id}`));
    const fa = await FakeAgent.connect(s.ws, c);
    const ev = sample("event");

    fa.send(ev);
    await fa.ackOf(ev.id!);
    fa.send(ev);
    await fa.ackOf(ev.id!);
    assert.deepEqual(got, [`${c.agentId}/report/${ev.id}`]);
    fa.send({
      type: "event",
      id: "bad",
      data: { worker: "Report", type: "x" },
    });
    const e = await fa.next("error", x => x.re === "bad");

    assert.equal(e.data.retryable, false);
  });

  it("onEvent: подтверждение после обработчика; ошибка — error retryable без ack", async () => {
    let fail = true;
    const gates: (() => void)[] = [];
    const release = async () => {
      await until(() => gates.length > 0);
      gates.shift()!();
    };
    const handled: string[] = [];
    const s = await server({
      onEvent: async e => {
        await new Promise<void>(r => gates.push(r));
        if (fail) throw new Error("нет базы");
        handled.push(e.id);
      },
    });
    const c = await enroll(s.url);
    const emitted: string[] = [];

    s.agents.on("event", e => emitted.push(e.id));
    const fa = await FakeAgent.connect(s.ws, c);
    const ev = sample("event");

    fa.send(ev);
    // Пока обработчик не завершился, подтверждения нет.
    assert.deepEqual(await fa.collect("ack", 100), []);
    await release();
    const e = await fa.next("error", x => x.re === ev.id);

    assert.equal(e.data.retryable, true);
    assert.deepEqual(emitted, []);
    fail = false;
    fa.send(ev);
    await release();
    await fa.ackOf(ev.id!);
    assert.deepEqual([handled, emitted], [[ev.id], [ev.id]]);
    // Повтор после успеха — подтверждается без обработчика.
    fa.send(ev);
    await fa.ackOf(ev.id!);
    assert.deepEqual(handled, [ev.id]);
  });
});

describe("fetch", () => {
  let s: TestServer;
  let fa: FakeAgent;
  let agentId: string;

  before(async () => {
    s = await startServer();
    const c = await enroll(s.url);

    agentId = c.agentId;
    fa = await FakeAgent.connect(s.ws, c);
  });
  after(() => s.close());

  it("потоковый ответ читается до конца по кускам", async () => {
    const res = s.agents.fetch(agentId, "echo", "/stream");
    const req = await fa.next("fetch");

    assert.deepEqual(req.data, {
      worker: "echo",
      method: "GET",
      path: "/stream",
    });
    fa.send({
      type: "fetch.head",
      re: req.id,
      data: { status: 200, headers: { "content-type": "text/plain" } },
    });
    const r = await res;
    const reader = r.body!.getReader();

    fa.send({
      type: "fetch.chunk",
      re: req.id,
      data: { data: "раз,", encoding: "utf8" },
    });
    assert.equal(Buffer.from((await reader.read()).value!).toString(), "раз,");
    fa.send({
      type: "fetch.chunk",
      re: req.id,
      data: { data: Buffer.from("два").toString("base64"), encoding: "base64" },
    });
    assert.equal(Buffer.from((await reader.read()).value!).toString(), "два");
    fa.send({ type: "fetch.end", re: req.id, data: {} });
    assert.equal((await reader.read()).done, true);
  });

  it("отмена чтения тела — fetch.cancel", async () => {
    const p = s.agents.fetch(agentId, "echo", "/stream");
    const req = await fa.next("fetch");

    fa.send({
      type: "fetch.head",
      re: req.id,
      data: { status: 200, headers: {} },
    });
    const r = await p;

    await r.body!.cancel();
    assert.equal((await fa.next("fetch.cancel")).re, req.id);
  });

  it("ошибка после заголовка — ошибка чтения тела", async () => {
    const p = s.agents.fetch(agentId, "echo", "/big");
    const req = await fa.next("fetch");

    fa.send({
      type: "fetch.head",
      re: req.id,
      data: { status: 200, headers: {} },
    });
    const r = await p;

    fa.send({
      type: "fetch.end",
      re: req.id,
      data: { error: { code: "BODY_TOO_LARGE", message: "больше 32 МБ" } },
    });
    await assert.rejects(r.text(), { code: "BODY_TOO_LARGE" });
  });

  it("204 без тела, json()", async () => {
    let p = s.agents.fetch(agentId, "echo", "/none", { method: "DELETE" });
    let req = await fa.next("fetch");

    fa.send({
      type: "fetch.head",
      re: req.id,
      data: { status: 204, headers: {} },
    });
    fa.send({ type: "fetch.end", re: req.id, data: {} });
    assert.equal((await p).status, 204);
    p = s.agents.fetch(agentId, "echo", "/json");
    req = await fa.next("fetch");
    fa.send({
      type: "fetch.head",
      re: req.id,
      data: { status: 201, headers: { "content-type": "application/json" } },
    });
    fa.send({
      type: "fetch.chunk",
      re: req.id,
      data: { data: '{"a":1}', encoding: "utf8" },
    });
    fa.send({ type: "fetch.end", re: req.id, data: {} });
    assert.deepEqual(await (await p).json(), { a: 1 });
  });

  it("срок — TIMEOUT и fetch.cancel", async () => {
    const p = s.agents.fetch(agentId, "echo", "/slow", { timeoutMs: 50 });
    const req = await fa.next("fetch");

    assert.equal(req.data.timeoutMs, 50);
    await assert.rejects(p, { code: "TIMEOUT" });
    assert.equal((await fa.next("fetch.cancel")).re, req.id);
  });

  it("timeoutMs не число — срок по умолчанию", async () => {
    const p = s.agents.fetch(agentId, "echo", "/nan", { timeoutMs: NaN });
    const req = await fa.next("fetch");

    assert.equal(req.data.timeoutMs, 30_000);
    await sleep(50);
    fa.send({
      type: "fetch.head",
      re: req.id,
      data: { status: 204, headers: {} },
    });
    fa.send({ type: "fetch.end", re: req.id, data: {} });
    assert.equal((await p).status, 204);
  });

  it("проверки до отправки", async () => {
    await assert.rejects(s.agents.fetch(agentId, "echo", "/health"), {
      code: "PATH_FORBIDDEN",
    });
    await assert.rejects(s.agents.fetch(agentId, "echo", "/config/main"), {
      code: "PATH_FORBIDDEN",
    });
    await assert.rejects(s.agents.fetch(agentId, "Echo", "/x"), {
      code: "MESSAGE_INVALID",
    });
    await assert.rejects(s.agents.fetch(agentId, "echo", "x"), {
      code: "MESSAGE_INVALID",
    });
    await assert.rejects(
      s.agents.fetch(agentId, "echo", "/x", {
        body: "x".repeat(MAX_FETCH_BODY + 1),
      }),
      {
        code: "BODY_TOO_LARGE",
      },
    );
    await assert.rejects(s.agents.fetch("nope", "echo", "/x"), {
      code: "AGENT_NOT_FOUND",
    });
  });

  it("обрыв связи — DISCONNECTED; агент без связи — AGENT_OFFLINE", async () => {
    const p = s.agents.fetch(agentId, "echo", "/slow");

    await fa.next("fetch");
    fa.ws.terminate();
    await assert.rejects(p, { code: "DISCONNECTED" });
    await until(async () => {
      try {
        await s.agents.fetch(agentId, "echo", "/x");

        return false;
      } catch (e) {
        return (e as { code: string }).code === "AGENT_OFFLINE";
      }
    });
  });
});

describe("настройки", () => {
  it("доставка при подключении, статус применения, событие config, удаление", async () => {
    const s = await server();
    const c = await enroll(s.url);
    const events: ConfigStatus[] = [];

    s.agents.on("config", e => events.push(e));
    const r1 = await s.agents
      .by("admin")
      .setConfig(c.agentId, "echo", "main", { a: 1 });

    assert.equal(r1.version, 1);
    assert.equal(r1.actor, "admin");
    assert.equal((await s.agents.configStatus(c.agentId))[0].state, "pending");

    const fa = await FakeAgent.connect(s.ws, c, { configs: {} });
    const put = await fa.next("config.put");

    assert.deepEqual(put.data, {
      worker: "echo",
      key: "main",
      version: 1,
      data: { a: 1 },
    });
    const status = (configs: Record<string, unknown>) => ({
      workers: [{ name: "echo", state: "running", restarts: 0, configs }],
      outbox: 0,
    });

    fa.stream("status", status({ main: { version: 1 } }));
    await fa.ackSeq(1);
    assert.equal(
      (await s.agents.configStatus(c.agentId, "echo"))[0].state,
      "applying",
    );
    fa.stream("status", status({ main: { version: 1, ok: true } }));
    await fa.ackSeq(2);
    const st = (await s.agents.configStatus(c.agentId, "echo"))[0];

    assert.equal(st.state, "applied");
    assert.equal(st.delivered, 1);
    assert.equal(st.applied, 1);
    assert.deepEqual(
      events.map(e => e.state),
      ["pending", "applying", "applied"],
    );

    // Новая версия — сразу агенту на связи.
    const r2 = await s.agents.setConfig(c.agentId, "echo", "main", { a: 2 });

    assert.equal((await fa.next("config.put")).data.version, r2.version);

    assert.equal(await s.agents.deleteConfig(c.agentId, "echo", "main"), true);
    assert.deepEqual((await fa.next("config.delete")).data, {
      worker: "echo",
      key: "main",
    });
    assert.equal((await s.agents.configStatus(c.agentId))[0].state, "deleting");
    fa.stream("status", status({}));
    await fa.ackSeq(3);
    assert.deepEqual(await s.agents.configStatus(c.agentId), []);
    assert.equal(await s.agents.deleteConfig(c.agentId, "echo", "main"), false);
    // После удаления версия всё равно растёт.
    assert.equal(
      (await s.agents.setConfig(c.agentId, "echo", "main", {})).version,
      3,
    );
  });

  it("при подключении: новее у сервера — config.put, у агента такая же — ничего", async () => {
    const s = await server();
    const c = await enroll(s.url);

    await s.agents.setConfig(c.agentId, "report", "main", { x: 1 });
    await s.agents.setConfig(c.agentId, "report", "main", { x: 2 });
    await s.agents.setConfig(c.agentId, "report", "limits", { y: 1 });
    const fa = await FakeAgent.connect(s.ws, c, {
      configs: { report: { main: 2 } },
    });
    const puts = await fa.collect("config.put", 100);

    assert.deepEqual(
      puts.map(p => [p.data.key, p.data.version]),
      [["limits", 1]],
    );
  });

  it("отказ воркера — проблема configFailed; успешная версия её заканчивает", async () => {
    const s = await server();
    const c = await enroll(s.url);
    const fa = await FakeAgent.connect(s.ws, c, { configs: {} });

    await s.agents.setConfig(c.agentId, "report", "main", { intervalSec: 0 });
    await fa.next("config.put");
    const bad = sample("config.applied.error");

    bad.data.version = 1;
    fa.send(bad);
    await fa.ackOf(bad.id!);
    let alerts = await s.agents.listAlerts(c.agentId);

    assert.deepEqual(
      alerts.map(a => [a.type, a.worker, a.configKey]),
      [["configFailed", "report", "main"]],
    );
    await s.agents.setConfig(c.agentId, "report", "main", { intervalSec: 60 });
    await fa.next("config.put");
    fa.send({
      type: "config.applied",
      id: "ok-2",
      data: { worker: "report", key: "main", version: 2, ok: true },
    });
    await fa.ackOf("ok-2");
    alerts = await s.agents.listAlerts(c.agentId);
    assert.deepEqual(alerts, []);
    const st = (await s.agents.configStatus(c.agentId))[0];

    assert.equal(st.state, "applied");
    // Повтор доставки старого итога ничего не меняет.
    fa.send(bad);
    await fa.ackOf(bad.id!);
    assert.equal((await s.agents.configStatus(c.agentId))[0].state, "applied");
  });

  it("проверки", async () => {
    const s = await server();
    const c = await enroll(s.url);

    await assert.rejects(s.agents.setConfig(c.agentId, "Echo", "main", {}), {
      code: "MESSAGE_INVALID",
    });
    await assert.rejects(
      s.agents.setConfig(
        c.agentId,
        "echo",
        "main",
        "x".repeat(MAX_CONFIG_BYTES),
      ),
      {
        code: "BODY_TOO_LARGE",
      },
    );
    await assert.rejects(s.agents.setConfig("nope", "echo", "main", {}), {
      code: "AGENT_NOT_FOUND",
    });
  });
});

describe("наблюдение", () => {
  it("watch: сводка зрителей, уход зрителя, истечение", async () => {
    const s = await server();
    const c = await enroll(s.url);
    const fa = await FakeAgent.connect(s.ws, c);
    const w1 = await s.agents.watch(c.agentId, {
      metricsIntervalMs: 5000,
      logLevel: "info",
      ttlMs: 60_000,
    });
    let w = await fa.next("watch");

    assert.deepEqual(w.data, {
      untilMs: w1.until,
      metricsIntervalMs: 5000,
      logLevel: "info",
    });
    const w2 = await s.agents.watch(c.agentId, {
      metricsIntervalMs: 1000,
      logLevel: "debug",
      ttlMs: 1000,
    });

    w = await fa.next("watch");
    assert.deepEqual(w.data, {
      untilMs: w1.until,
      metricsIntervalMs: 1000,
      logLevel: "debug",
    });
    // Второй истёк — сводка снова по первому.
    w = await fa.next("watch", () => true, 3000);
    assert.deepEqual(w.data, {
      untilMs: w1.until,
      metricsIntervalMs: 5000,
      logLevel: "info",
    });
    assert.ok(Date.now() >= w2.until);
    await s.agents.unwatch(c.agentId, w1.id);
    assert.deepEqual((await fa.next("watch")).data, {});
    // Наблюдатель есть до подключения — watch сразу после welcome.
    await fa.close();
    await s.agents.watch(c.agentId, { id: "ui", metricsIntervalMs: 2000 });
    const fb = await FakeAgent.connect(s.ws, c);

    assert.equal((await fb.next("watch")).data.metricsIntervalMs, 2000);
  });

  it("watch: ttlMs не число — срок по умолчанию", async () => {
    const s = await server();
    const c = await enroll(s.url);
    const before = Date.now();
    const w = await s.agents.watch(c.agentId, { ttlMs: NaN });

    assert.ok(w.until >= before + 60_000 && w.until <= Date.now() + 60_000);
  });

  it("метрики: событие на каждую точку, в записи агента — последняя", async () => {
    const s = await server();
    const c = await enroll(s.url);
    const got: MetricsEvent[] = [];

    s.agents.on("metrics", m => got.push(m));
    const fa = await FakeAgent.connect(s.ws, c);

    for (const at of [10_000, 10_500, 11_000, 11_200])
      fa.stream("metrics", { collectedAt: at, workers: { echo: { n: at } } });
    await fa.ackSeq(4);
    await until(() => got.length === 4);
    assert.deepEqual(
      got.map(p => [p.agentId, p.at]),
      [10_000, 10_500, 11_000, 11_200].map(at => [c.agentId, at]),
    );
    assert.deepEqual(got[3].workers, { echo: { n: 11_200 } });
    assert.equal((await s.agents.getAgent(c.agentId))?.metrics?.at, 11_200);
  });

  it("проблемы воркеров по status", async () => {
    const s = await server();
    const c = await enroll(s.url);
    const fa = await FakeAgent.connect(s.ws, c);

    fa.stream("status", sample("status").data);
    await fa.ackSeq(1);
    const first = await s.agents.listAlerts(c.agentId);

    assert.deepEqual(first.map(a => a.key).sort(), [
      "configFailed:report/limits",
      "workerDown:echo",
      "workerInvalid:legacy",
    ]);
    // Причина invalid — из status.workers[].message.
    assert.match(
      first.find(a => a.type === "workerInvalid")?.message ?? "",
      /^воркер legacy не зарегистрирован: GET \/manifest: HTTP 404/,
    );
    fa.stream("status", {
      workers: [
        {
          name: "report",
          state: "running",
          health: { ok: false, message: "нет связи с example.com" },
        },
        { name: "echo", state: "running", health: { ok: true } },
      ],
    });
    await fa.ackSeq(2);
    const list = await s.agents.listAlerts(c.agentId);

    assert.deepEqual(
      list.map(a => a.key),
      ["workerUnhealthy:report"],
    );
    assert.match(list[0].message, /example\.com/);
  });
});

describe("смена ключа", () => {
  it("новый секрет принимается и становится основным; до входа с ним работает и старый", async () => {
    const s = await server();
    const c = await enroll(s.url);
    const fa = await FakeAgent.connect(s.ws, c);
    const secret = "new-secret-example";
    const { createHash } = await import("node:crypto");
    const acts: ActionRecord[] = [];

    s.agents.on("action", a => acts.push(a));
    const p = s.agents.by("admin").rotateKey(c.agentId);
    const act = await fa.next("action");

    assert.equal(act.data.name, "agent.rotateKey");
    fa.send({
      type: "action.result",
      id: "r1",
      re: act.id,
      data: {
        ok: true,
        result: {
          secretHash: createHash("sha256").update(secret).digest("hex"),
        },
      },
    });
    await p;
    assert.deepEqual(
      acts.map(a => [a.name, a.actor, a.status, a.agentId]),
      [["agent.rotateKey", "admin", "done", c.agentId]],
    );
    assert.equal((await fa.closed).code, 1012);
    // Старый ещё принимается (агент мог не сохранить новый).
    const old = await FakeAgent.connect(s.ws, c);

    await old.close();
    const neu = await FakeAgent.connect(s.ws, { agentId: c.agentId, secret });

    await neu.close();
    await assert.rejects(FakeAgent.open(s.ws, c), /HTTP 401/);
  });

  it("замена занятого воркера: force в args; пока status показывает pending — срок заново", async () => {
    const s = await server();
    const c = await enroll(s.url);
    const fa = await FakeAgent.connect(s.ws, c);
    const forced = s.agents.restartWorker(c.agentId, "echo", { force: true });
    const act = await fa.next("action");

    assert.deepEqual(act.data, {
      name: "worker.restart",
      args: { name: "echo", force: true },
    });
    fa.send({
      type: "action.result",
      id: "f1",
      re: act.id,
      data: { ok: true },
    });
    await forced;

    const p = s.agents.restartWorker(c.agentId, "echo", { timeoutMs: 300 });
    const waiting = await fa.next("action");

    assert.deepEqual(waiting.data.args, { name: "echo" });
    const pending = {
      name: "echo",
      state: "running",
      health: { ok: true, busy: true },
      pending: "restart",
    };

    for (let seq = 1; seq <= 6; seq += 1) {
      fa.send({ type: "status", seq, data: { workers: [pending] } });
      await sleep(150);
    }
    fa.send({
      type: "action.result",
      id: "f2",
      re: waiting.id,
      data: { ok: true },
    });
    await p;
    const a = await s.agents.getAgent(c.agentId);

    assert.equal(a?.status?.workers[0]?.pending, "restart");
    assert.equal(a?.status?.workers[0]?.health?.busy, true);
    await fa.close();
  });

  it("действие без связи — AGENT_OFFLINE; без итога — TIMEOUT", async () => {
    const s = await server();
    const c = await enroll(s.url);

    await assert.rejects(s.agents.restartWorker(c.agentId, "echo"), {
      code: "AGENT_OFFLINE",
    });
    const fa = await FakeAgent.connect(s.ws, c);

    await assert.rejects(
      s.agents.restartWorker(c.agentId, "echo", { timeoutMs: 50 }),
      { code: "TIMEOUT" },
    );
    await fa.next("action");
  });
});

describe("выпуск", () => {
  it("manifest.json, сборки из манифеста, install.sh с адресом и ключом", async () => {
    const dir = mkdtempSync(join(tmpdir(), "agent-release-"));
    const manifest = {
      version: "1.1.0",
      artifacts: [
        {
          os: "linux",
          arch: "amd64",
          file: "agent-linux-amd64",
          sha256: "aa",
          signature: "sig",
        },
      ],
      workers: [
        {
          name: "report",
          version: "1.4.0",
          os: "linux",
          arch: "amd64",
          file: "report-1.4.0.tar.gz",
          sha256: "bb",
        },
        {
          name: "report",
          version: "1.10.0",
          os: "linux",
          arch: "amd64",
          file: "report-1.10.0.tar.gz",
          sha256: "cc",
        },
        // Предварительная версия старше выпуска 1.10.0.
        {
          name: "report",
          version: "1.10.0-rc.1",
          os: "linux",
          arch: "amd64",
          file: "report-1.10.0-rc.1.tar.gz",
          sha256: "dd",
        },
      ],
    };

    writeFileSync(join(dir, "manifest.json"), JSON.stringify(manifest));
    writeFileSync(join(dir, "agent-linux-amd64"), "BIN");
    writeFileSync(join(dir, "secret.txt"), "нельзя");
    writeFileSync(
      join(dir, "install.sh"),
      '#!/bin/sh\nDEFAULT_SERVER=""\nDEFAULT_PUBLIC_KEY=""\n',
    );
    const s = await server({ releasesDir: dir, publicKey: "a2V5" });
    const base = `${s.url}/api/v1/agent-link`;

    assert.deepEqual(
      await (await fetch(`${base}/releases/manifest.json`)).json(),
      manifest,
    );
    assert.equal(
      await (await fetch(`${base}/releases/agent-linux-amd64`)).text(),
      "BIN",
    );
    assert.equal((await fetch(`${base}/releases/secret.txt`)).status, 404);
    const sh = await (await fetch(`${base}/install.sh`)).text();

    assert.match(sh, new RegExp(`DEFAULT_SERVER="${s.url}"`));
    assert.match(sh, /DEFAULT_PUBLIC_KEY="a2V5"/);

    const c = await enroll(s.url);
    const fa = await FakeAgent.connect(s.ws, c);

    fa.stream("status", {
      workers: [
        { name: "report", state: "running", version: "1.4.0", release: true },
      ],
    });
    await fa.ackSeq(1);
    assert.deepEqual(
      (await s.agents.updateCandidates()).map(x => [x.current, x.target]),
      [["1.0.0", "1.1.0"]],
    );
    assert.deepEqual(
      (await s.agents.workerUpdateCandidates()).map(x => [
        x.worker,
        x.current,
        x.target,
      ]),
      [["report", "1.4.0", "1.10.0"]],
    );
    const p = s.agents.updateWorker(c.agentId, "report");
    const act = await fa.next("action");

    assert.equal(act.data.args.version, "1.10.0");
    assert.equal(
      act.data.args.url,
      "/api/v1/agent-link/releases/report-1.10.0.tar.gz",
    );
    fa.send({
      type: "action.result",
      id: "u1",
      re: act.id,
      data: { ok: true, result: { version: "1.10.0" } },
    });
    assert.deepEqual(await p, { version: "1.10.0" });
    await assert.rejects(s.agents.updateWorker(c.agentId, "echo"), {
      code: "WORKER_NOT_RELEASED",
      status: 409,
    });
  });

  it("неверный путь к файлу выпуска — 404; сборка без file — не кандидат", async () => {
    const dir = mkdtempSync(join(tmpdir(), "agent-release-"));
    const manifest = {
      version: "1.1.0",
      artifacts: [{ os: "linux", arch: "amd64", sha256: "aa" }],
    };

    writeFileSync(join(dir, "manifest.json"), JSON.stringify(manifest));
    const s = await server({ releasesDir: dir });

    assert.equal(
      (await fetch(`${s.url}/api/v1/agent-link/releases/%E0%A4%A`)).status,
      404,
    );
    const c = await enroll(s.url);

    await FakeAgent.connect(s.ws, c);
    assert.deepEqual(await s.agents.updateCandidates(), []);
    await assert.rejects(s.agents.updateAgent(c.agentId), {
      code: "UPDATE_NOT_AVAILABLE",
    });
  });

  it("без каталога выпуска — UPDATE_NOT_AVAILABLE", async () => {
    const s = await server();
    const c = await enroll(s.url);

    await FakeAgent.connect(s.ws, c);
    await assert.rejects(s.agents.updateAgent(c.agentId), {
      code: "UPDATE_NOT_AVAILABLE",
      status: 409,
    });
    assert.equal(
      (await fetch(`${s.url}/api/v1/agent-link/releases/manifest.json`)).status,
      404,
    );
  });
});

describe("подключение к фреймворкам", () => {
  it("чужой upgrade — 404, если других обработчиков нет", async () => {
    const s = await server();
    const ws = new WebSocket(
      s.url.replace("http", "ws") + "/other",
      "agent.v2",
    );
    const status = await new Promise<number>(r =>
      ws.once("unexpected-response", (_q, res) => r(res.statusCode!)),
    );

    assert.equal(status, 404);
  });

  it("адрес запроса, который не разбирается, — 404, сервер работает дальше", async () => {
    const s = await server();
    const port = new URL(s.url).port;
    const raw = (req: string) =>
      new Promise<string>((resolve, reject) => {
        const sock = connect(Number(port), "127.0.0.1", () => sock.write(req));
        let out = "";

        sock.on("data", d => {
          out += d.toString();
          sock.end();
        });
        sock.on("close", () => resolve(out));
        sock.on("error", reject);
      });
    const upgrade = await raw(
      "GET // HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n" +
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Protocol: agent.v2\r\n\r\n",
    );

    assert.match(upgrade, /^HTTP\/1\.1 404/);
    const plain = await raw(
      "POST // HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );

    assert.match(plain, /^HTTP\/1\.1 404/);
    await enroll(s.url);
  });

  it("handle — false для чужих маршрутов", async () => {
    const s = await server();

    assert.equal((await fetch(`${s.url}/api/other`)).status, 404);
    await sleep(1);
  });
});
