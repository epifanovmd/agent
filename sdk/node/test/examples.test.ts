// Все образцы sdk/spec/examples между сервером и агентом проходят через SDK: сообщения сервера
// SDK строит сам (и они совпадают с образцами), сообщения агента SDK принимает и подтверждает.
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, it } from "node:test";

import type {
  ActionRecord,
  AgentEvent,
  ConfigStatus,
  Envelope,
} from "../src/server/index";
import {
  type Creds,
  enroll,
  FakeAgent,
  sample,
  samples,
  startServer,
  type TestServer,
} from "./helpers";

const covered = new Set<string>();
const use = (name: string) => {
  covered.add(name);

  return sample(name);
};

/** Сообщение сервера совпадает с образцом (id, которые порождает сервер, не сравниваются). */
const same = (got: Envelope, name: string, ignore: string[] = []) => {
  const want = use(name);
  const strip = (e: Envelope) => {
    const c = structuredClone(e) as Envelope & Record<string, unknown>;

    if (want.id) delete c.id;
    if (want.re) delete c.re;
    for (const path of ignore) {
      const [a, b] = path.split(".");

      if (b) delete (c[a] as Record<string, unknown>)?.[b];
      else delete c[a];
    }

    return c;
  };

  assert.deepEqual(strip(got), strip(want));
};

const releaseDir = (): string => {
  const dir = mkdtempSync(join(tmpdir(), "agent-release-"));
  const upd = sample("action.agent.update").data.args;
  const wupd = sample("action.worker.update").data.args;

  writeFileSync(
    join(dir, "manifest.json"),
    JSON.stringify({
      version: upd.version,
      artifacts: [
        {
          os: "linux",
          arch: "amd64",
          file: "agent-linux-amd64",
          sha256: upd.sha256,
          signature: upd.signature,
        },
      ],
      workers: [
        {
          name: "report",
          version: wupd.version,
          os: "linux",
          arch: "amd64",
          file: "report-1.5.0-linux-amd64",
          sha256: wupd.sha256,
          signature: wupd.signature,
        },
      ],
    }),
  );

  return dir;
};

describe("образцы", () => {
  let s: TestServer;
  let creds: Creds;
  let fa: FakeAgent;
  const logged: string[] = [];

  before(async () => {
    s = await startServer({
      releasesDir: releaseDir(),
      log: msg => logged.push(msg),
      onWorkerRequest: () => sample("request.result").data.data,
    });
  });
  after(async () => {
    await s.close();
    const all = [...samples()]
      .filter(([, v]) => v.file !== "worker")
      .map(([k]) => k);

    assert.deepEqual(
      all.filter(n => !covered.has(n)),
      [],
      "все образцы сервер ↔ агент прогнаны",
    );
  });

  it("enroll и enroll.denied", async () => {
    for (const name of ["enroll", "enroll.denied"]) {
      covered.add(name);
      const ex = samples().get(name)!;
      const res = await fetch(s.url + ex.request!.path, {
        method: ex.request!.method,
        headers: { "content-type": "application/json" },
        body: JSON.stringify(ex.request!.body),
      });

      assert.equal(res.status, ex.response!.status);
      const body = await res.json();

      if (res.status === 200) {
        assert.equal(typeof body.agentId, "string");
        assert.equal(typeof body.secret, "string");
        creds = body;
      } else assert.deepEqual(body, ex.response!.body);
    }
  });

  it("hello → welcome, config.delete лишнего ключа; config.put по версии hello", async () => {
    // У сервера есть report/main; у агента в hello — main 41 и limits 7.
    await s.agents.setConfig(
      creds.agentId,
      "report",
      "main",
      use("config.put").data.data,
    );
    fa = await FakeAgent.open(s.ws, creds);
    fa.send(use("hello"));
    const welcome = await fa.next("welcome");

    same(welcome, "welcome", [
      "data.serverTime",
      "data.metricsIntervalMs",
      "data.statusIntervalMs",
    ]);
    assert.equal(typeof welcome.data.serverTime, "number");
    // Версия сервера (1) старше, чем у агента (41): setConfig после hello поднимет её выше.
    same(await fa.next("config.delete"), "config.delete");
    const rec = await s.agents.setConfig(
      creds.agentId,
      "report",
      "main",
      sample("config.put").data.data,
    );

    assert.equal(rec.version, 42);
    same(await fa.next("config.put"), "config.put");
  });

  it("status, metrics, log — ack seq", async () => {
    for (const name of ["status", "metrics", "log"]) {
      const m = use(name);

      fa.send(m);
      await fa.ackSeq(m.seq!);
    }
    const a = await s.agents.getAgent(creds.agentId);

    assert.equal(a?.status?.workers.length, 5);
    assert.equal(a?.status?.workers[3]?.state, "invalid");
    assert.equal(
      a?.status?.workers[3]?.message,
      sample("status").data.workers[3].message,
    );
    assert.equal(a?.status?.workers[1]?.pending, "restart");
    assert.equal(a?.status?.workers[1]?.health?.busy, true);
    assert.equal(a?.metrics?.at, sample("metrics").data.collectedAt);
  });

  it("ack ids на event; error на неверное событие", async () => {
    const ev = use("event");
    const events: AgentEvent[] = [];

    s.agents.on("event", e => events.push(e));
    fa.send(ev);
    const ack = await fa.ackOf(ev.id!);

    assert.deepEqual(Object.keys(ack).sort(), Object.keys(use("ack")).sort());
    assert.deepEqual([events[0].id, events[0].type], [ev.id, ev.data.type]);

    const err = sample("error");

    fa.send({ type: "event", id: err.re, data: { worker: "report", at: 1 } });
    const got = await fa.next("error");

    assert.equal(got.re, err.re);
    assert.equal(got.data.code, err.data.code);
    assert.equal(got.data.retryable, err.data.retryable);
    covered.add("error");
  });

  it("config.applied и config.applied.error", async () => {
    const ok = use("config.applied");

    fa.send(ok);
    await fa.ackOf(ok.id!);
    let st = (await s.agents.configStatus(creds.agentId, "report")).find(
      c => c.key === "main",
    )!;

    assert.equal(st.state, "applied");
    assert.equal(st.applied, 42);
    await s.agents.setConfig(creds.agentId, "report", "main", {
      intervalSec: 0,
    });
    await fa.next("config.put", e => e.data.version === 43);
    const bad = use("config.applied.error");

    fa.send(bad);
    await fa.ackOf(bad.id!);
    st = (await s.agents.configStatus(creds.agentId, "report")).find(
      c => c.key === "main",
    )!;
    assert.equal(st.state, "failed");
    assert.equal(st.applied, 42);
    assert.deepEqual(st.error, bad.data.error);
  });

  it("config.applied.result — подробный итог в ConfigStatus.result и событии config", async () => {
    const m = use("config.applied.result");
    const events: unknown[] = [];
    const onConfig = (c: ConfigStatus) => {
      if (c.key === "main" && c.state === "applied") events.push(c.result);
    };

    s.agents.on("config", onConfig);

    await s.agents.setConfig(creds.agentId, "report", "main", {
      target: "https://example.com/report",
      intervalSec: 60,
    });
    await fa.next("config.put", e => e.data.version === m.data.version);
    fa.send(m);
    await fa.ackOf(m.id!);
    const find = async () =>
      (await s.agents.configStatus(creds.agentId, "report")).find(
        c => c.key === "main",
      )!;
    let st = await find();

    assert.equal(st.state, "applied");
    assert.deepEqual(st.result, m.data.result);
    assert.deepEqual(events, [m.data.result]);
    // status той же версии (без result) итог не стирает.
    const seq = fa.stream("status", {
      workers: [
        {
          name: "report",
          state: "running",
          restarts: 0,
          configs: { main: { version: m.data.version, ok: true } },
        },
      ],
      outbox: 0,
    });

    await fa.ackSeq(seq);
    st = await find();
    assert.deepEqual(st.result, m.data.result);
    s.agents.off("config", onConfig);
  });

  it("config.applied.key-unknown — отказ с кодом CONFIG_KEY_UNKNOWN", async () => {
    const m = use("config.applied.key-unknown");

    await s.agents.setConfig(creds.agentId, "report", m.data.key, {
      days: 30,
    });
    await fa.next("config.put", e => e.data.key === m.data.key);
    fa.send(m);
    await fa.ackOf(m.id!);
    const st = (await s.agents.configStatus(creds.agentId, "report")).find(
      c => c.key === m.data.key,
    )!;

    assert.equal(st.state, "failed");
    assert.deepEqual(st.error, m.data.error);
    await s.agents.deleteConfig(creds.agentId, "report", m.data.key);
    await fa.next("config.delete", e => e.data.key === m.data.key);
  });

  it("fetch, fetch.head, fetch.chunk, fetch.end", async () => {
    const want = sample("fetch").data;
    const resP = s.agents.fetch(creds.agentId, want.worker, want.path, {
      method: want.method,
      headers: want.headers,
      body: want.body,
      timeoutMs: want.timeoutMs,
    });
    const req = await fa.next("fetch");

    same(req, "fetch");
    const reply = (name: string) => {
      const m = use(name);

      m.re = req.id;
      fa.send(m);
    };

    reply("fetch.head");
    reply("fetch.chunk");
    const res = await resP;

    assert.equal(res.status, 200);
    assert.equal(res.headers.get("content-type"), "application/json");
    reply("fetch.chunk.base64");
    reply("fetch.end");
    const body = Buffer.from(await res.arrayBuffer());
    const text = Buffer.from(sample("fetch.chunk").data.data, "utf8");
    const bin = Buffer.from(sample("fetch.chunk.base64").data.data, "base64");

    assert.deepEqual(body, Buffer.concat([text, bin]));
  });

  it("fetch.binary, fetch.cancel, fetch.end.error, fetch.end.invalid", async () => {
    const want = sample("fetch.binary").data;
    const ac = new AbortController();
    const p = s.agents.fetch(creds.agentId, want.worker, want.path, {
      method: want.method,
      headers: want.headers,
      body: new Uint8Array([0, 1, 2, 3, 4, 5]),
      signal: ac.signal,
    });
    const req = await fa.next("fetch");

    same(req, "fetch.binary");
    ac.abort();
    await assert.rejects(p, { code: "CANCELLED" });
    const cancel = await fa.next("fetch.cancel");

    same(cancel, "fetch.cancel");
    assert.equal(cancel.re, req.id);

    const p2 = s.agents.fetch(creds.agentId, "echo", "/x");
    const req2 = await fa.next("fetch");
    const end = use("fetch.end.error");

    end.re = req2.id;
    fa.send(end);
    await assert.rejects(p2, {
      code: "WORKER_UNAVAILABLE",
      message: end.data.error.message,
    });

    const p3 = s.agents.fetch(creds.agentId, "legacy", "/x");
    const req3 = await fa.next("fetch");
    const inv = use("fetch.end.invalid");

    inv.re = req3.id;
    fa.send(inv);
    await assert.rejects(p3, {
      code: "WORKER_INVALID",
      status: 502,
      message: inv.data.error.message,
    });
  });

  it("fetch.end.route-undeclared и fetch.end.job-unknown — ошибки с кодом агента", async () => {
    for (const [name, status] of [
      ["fetch.end.route-undeclared", 404],
      ["fetch.end.job-unknown", 409],
    ] as const) {
      const p = s.agents.fetch(creds.agentId, "report", "/reports/7", {
        method: "DELETE",
      });
      const req = await fa.next("fetch");
      const end = use(name);

      end.re = req.id;
      fa.send(end);
      await assert.rejects(p, {
        code: end.data.error.code,
        status,
        message: end.data.error.message,
      });
    }
  });

  it("request → request.result (onWorkerRequest); без обработчика — request.result.error", async () => {
    const req = use("request");

    fa.send(req);
    same(
      await fa.next("request.result", e => e.re === req.id),
      "request.result",
    );

    const bare = await startServer({ log: () => {} });

    try {
      const c = await enroll(bare.url);
      const fb = await FakeAgent.connect(bare.ws, c, { configs: {} });

      fb.send(req);
      same(
        await fb.next("request.result", e => e.re === req.id),
        "request.result.error",
      );
      await fb.close();
    } finally {
      await bare.close();
    }
  });

  it("watch и watch.off", async () => {
    const want = sample("watch").data;
    const ref = await s.agents.watch(creds.agentId, {
      metricsIntervalMs: want.metricsIntervalMs,
      logLevel: want.logLevel,
    });
    const w = await fa.next("watch");

    same(w, "watch", ["data.untilMs"]);
    assert.equal(w.data.untilMs, ref.until);
    await s.agents.unwatch(creds.agentId, ref.id);
    same(await fa.next("watch"), "watch.off");
  });

  it("действия и их итоги", async () => {
    const run = async (
      name: string,
      call: () => Promise<unknown>,
      result: string,
      check: (v: unknown) => void,
    ) => {
      const p = call();
      const act = await fa.next("action");

      same(act, name);
      const r = use(result);

      r.re = act.id;
      fa.send(r);
      await fa.ackOf(r.id!);
      check(await p);
    };

    await run(
      "action.worker.restart",
      () => s.agents.restartWorker(creds.agentId, "echo"),
      "action.result.worker.restart",
      v => assert.deepEqual(v, { deferred: false }),
    );
    await run(
      "action.worker.restart.force",
      () => s.agents.restartWorker(creds.agentId, "echo", { force: true }),
      "action.result.worker.restart",
      v => assert.deepEqual(v, { deferred: false }),
    );
    await run(
      "action.worker.update",
      () => s.agents.updateWorker(creds.agentId, "report"),
      "action.result.worker.update",
      v =>
        assert.deepEqual(v, {
          ...sample("action.result.worker.update").data.result,
          deferred: false,
        }),
    );

    // Воркер занят: action.result с deferred сразу, итог — action.done и событие action.
    const acts: ActionRecord[] = [];

    s.agents.on("action", a => acts.push(a));
    const deferred = async (
      call: () => Promise<unknown>,
      result: string,
      done: string,
    ) => {
      const p = call();
      const act = await fa.next("action");
      const r = use(result);

      r.re = act.id;
      fa.send(r);
      await fa.ackOf(r.id!);
      assert.deepEqual(await p, {
        ...r.data.result,
        actionId: act.id,
      });
      const d = use(done);

      d.re = act.id;
      fa.send(d);
      await fa.ackOf(d.id!);

      return acts.find(a => a.id === act.id)!;
    };
    const restarted = await deferred(
      () => s.agents.restartWorker(creds.agentId, "echo"),
      "action.result.deferred",
      "action.done",
    );

    assert.deepEqual(
      [restarted.name, restarted.status, restarted.deferred],
      ["worker.restart", "done", true],
    );
    const updated = await deferred(
      () => s.agents.updateWorker(creds.agentId, "report"),
      "action.result.worker.update.deferred",
      "action.done.worker.update",
    );

    assert.deepEqual(
      updated.result,
      sample("action.done.worker.update").data.result,
    );
    const failed = await deferred(
      () => s.agents.updateWorker(creds.agentId, "report"),
      "action.result.worker.update.deferred",
      "action.done.error",
    );

    assert.deepEqual(
      [failed.status, failed.error],
      ["failed", sample("action.done.error").data.error],
    );
    await run(
      "action.agent.update",
      () => s.agents.updateAgent(creds.agentId),
      "action.result.agent.update",
      v =>
        assert.deepEqual(v, sample("action.result.agent.update").data.result),
    );
    await run(
      "action.agent.logs",
      () => s.agents.logs(creds.agentId, { worker: "echo", lines: 100 }),
      "action.result.agent.logs",
      v =>
        assert.deepEqual(
          v,
          sample("action.result.agent.logs").data.result.entries,
        ),
    );

    // Ответ agent.logs не разобран — ошибка MESSAGE_INVALID (502) и запись в журнал.
    const lp = s.agents.logs(creds.agentId);
    const la = await fa.next("action");
    const bad = use("action.result.agent.logs");

    bad.re = la.id;
    bad.data.result = { entries: "x" };
    fa.send(bad);
    await assert.rejects(lp, { code: "MESSAGE_INVALID", status: 502 });
    assert.ok(logged.includes("ответ agent.logs не принят"));

    // Ошибка действия — AgentsError с кодом из итога.
    const p = s.agents.updateWorker(creds.agentId, "report");
    const act = await fa.next("action");
    const r = use("action.result.error");

    r.re = act.id;
    fa.send(r);
    await assert.rejects(p, { code: "UPDATE_FAILED" });

    // Смена ключа: сервер запоминает хеш и закрывает соединение кодом 1012.
    const rot = s.agents.rotateKey(creds.agentId);
    const ra = await fa.next("action");

    same(ra, "action.agent.rotateKey");
    const rr = use("action.result.agent.rotateKey");

    rr.re = ra.id;
    fa.send(rr);
    await rot;
    await fa.ackOf(rr.id!);
    assert.equal((await fa.closed).code, 1012);
    const rec = await s.agents.store.getAgent(creds.agentId);

    assert.equal(rec?.pendingSecretHash, rr.data.result.secretHash);
  });

  it("задачи: runJob — POST /jobs, итог по событиям job.*", async () => {
    const all = samples();
    const long = all.get("worker.jobs.long")!.request!.body as {
      type: string;
      jobId: string;
      data: unknown;
      files: Record<string, Record<string, string>>;
    };

    // Прежнее соединение закрыто сменой ключа: агент с задачами — свой.
    const jc = await enroll(s.url, { name: "node-03" });
    const fj = await FakeAgent.connect(s.ws, jc, { configs: {} });

    fj.stream(
      "status",
      {
        workers: [
          {
            name: "report",
            state: "running",
            manifest: all.get("worker.manifest")!.response!.body,
          },
        ],
      },
      1,
    );
    await fj.ackSeq(1);
    /** Ответ агента на fetch: заголовок, тело JSON, конец. */
    const reply = (re: string, status: number, body: unknown) => {
      fj.send({
        type: "fetch.head",
        re,
        data: { status, headers: { "content-type": "application/json" } },
      });
      fj.send({
        type: "fetch.chunk",
        re,
        data: { data: JSON.stringify(body), encoding: "utf8" },
      });
      fj.send({ type: "fetch.end", re, data: {} });
    };
    const event = (name: string) => {
      const e = use(name);

      fj.send(e);

      return fj.ackOf(e.id!);
    };

    // Долгая задача: 202, затем job.progress и job.done.
    const p = s.agents.runJob(jc.agentId, "report", {
      type: long.type,
      jobId: long.jobId,
      data: long.data,
      files: long.files,
    });
    const req = await fj.next("fetch");

    assert.deepEqual(
      [req.data.method, req.data.path, JSON.parse(req.data.body)],
      ["POST", "/jobs", long],
    );
    reply(req.id!, 202, all.get("worker.jobs.long")!.response!.body);
    await event("event.job.progress");
    await event("event.job.done");
    assert.deepEqual(await p, {
      jobId: long.jobId,
      id: "b81c",
      state: "done",
      result: sample("event.job.done").data.data.result,
    });

    // Итог пришёл раньше ответа 202 — всё равно дошёл.
    const failed = sample("event.job.failed").data.data;
    const f = s.agents.runJob(jc.agentId, "report", {
      type: "report.build",
      jobId: failed.jobId,
    });
    const freq = await fj.next("fetch");

    await event("event.job.failed");
    reply(freq.id!, 202, { id: failed.id });
    assert.deepEqual(await f, {
      jobId: failed.jobId,
      id: failed.id,
      state: "failed",
      error: failed.error,
    });

    const cancelled = sample("event.job.cancelled").data.data;
    const c = s.agents.runJob(jc.agentId, "report", {
      type: "report.build",
      jobId: cancelled.jobId,
    });

    reply((await fj.next("fetch")).id!, 202, { id: cancelled.id });
    await event("event.job.cancelled");
    assert.deepEqual(await c, { ...cancelled, state: "cancelled" });

    // Быстрая задача — итог сразу; типа нет в манифесте — JOB_UNKNOWN.
    const quick = all.get("worker.jobs.quick")!;
    const q = s.agents.runJob(jc.agentId, "report", {
      type: "report.check",
    });

    reply((await fj.next("fetch")).id!, 200, quick.response!.body);
    assert.deepEqual(
      (await q).result,
      (quick.response!.body as { result: unknown }).result,
    );
    await assert.rejects(
      s.agents.runJob(jc.agentId, "report", { type: "report.unknown" }),
      { code: "JOB_UNKNOWN", status: 409 },
    );
    await fj.close();
  });

  it("незнакомый тип — error UNKNOWN_TYPE", async () => {
    const c2 = await enroll(s.url, { name: "node-02" });
    const fb = await FakeAgent.connect(s.ws, c2, { configs: {} });

    fb.send({ type: "example.unknown", id: "u1" });
    const e = await fb.next("error", x => x.re === "u1");

    assert.equal(e.data.code, "UNKNOWN_TYPE");
    assert.equal(e.data.retryable, false);
    await fb.close();
  });
});
