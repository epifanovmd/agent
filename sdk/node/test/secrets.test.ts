// Секреты не попадают в журнал SDK (опция log): значение настроек, итог и отказ применения,
// тела fetch и заголовок Authorization, data событий и запросов воркера, токен регистрации и
// секрет агента. В журнал — только имена, версии, коды и причины отказа по схемам.
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";

import {
  type Creds,
  enroll,
  FakeAgent,
  startServer,
  type TestServer,
} from "./helpers";

const CANARY = "SECRET-CANARY-sdk";
const TOKEN = "SECRET-CANARY-enroll";

describe("секреты не попадают в журнал SDK", () => {
  const logged: unknown[] = [];
  let s: TestServer;
  let creds: Creds;
  let fa: FakeAgent;

  before(async () => {
    s = await startServer({
      enrollToken: TOKEN,
      validateEvents: "log",
      onWorkerRequest: req => ({ echo: req.data }),
      log: (msg, extra) => logged.push([msg, extra]),
    });
    await assert.rejects(enroll(s.url, { token: `${CANARY}-wrong` }));
    creds = await enroll(s.url, { token: TOKEN });
    fa = await FakeAgent.connect(s.ws, creds, {
      workers: [
        {
          name: "w",
          version: "1.0.0",
          manifest: {
            version: "1.0.0",
            configs: [{ key: "main" }],
            routes: [{ method: "POST", path: "/echo" }],
            events: [
              {
                type: "example.done",
                schema: {
                  type: "object",
                  properties: { n: { type: "integer" } },
                  required: ["n"],
                },
              },
            ],
            requests: [{ type: "example.ask" }],
          },
        },
      ],
      configs: {},
    });
  });
  after(async () => {
    await fa?.close();
    await s?.close();
  });

  it("сценарий целиком — в журнале нет меток", async () => {
    const secret = { token: CANARY };

    // Настройка: итог с меткой, затем отказ с меткой в тексте и неверный итог.
    const rec = await s.agents.setConfig(creds.agentId, "w", "main", secret);
    const put = await fa.next("config.put", e => e.data.key === "main");

    assert.deepEqual(put.data.data, secret);
    const applied = (id: string, data: Record<string, unknown>) =>
      fa.send({
        type: "config.applied",
        id,
        data: { worker: "w", key: "main", version: rec.version, ...data },
      });

    applied("a1", { ok: true, result: { applied: secret } });
    await fa.ackOf("a1");
    applied("a2", {
      ok: false,
      error: { code: "CONFIG_REJECTED", message: `плохой ${CANARY}` },
    });
    await fa.ackOf("a2");
    fa.send({
      type: "config.applied",
      id: "a3",
      data: { worker: "w", key: "main", version: "x", result: CANARY },
    });
    await fa.next("error", e => e.re === "a3");

    // fetch: метка в теле и в Authorization запроса, в заголовке, теле и ошибке ответа.
    const p = s.agents.fetch(creds.agentId, "w", "/echo", {
      method: "POST",
      headers: { authorization: `Bearer ${CANARY}` },
      body: JSON.stringify(secret),
    });
    const req = await fa.next("fetch");

    assert.match(JSON.stringify(req.data), new RegExp(CANARY));
    fa.send({
      type: "fetch.head",
      re: req.id,
      data: { status: 200, headers: { "set-cookie": CANARY } },
    });
    fa.send({
      type: "fetch.chunk",
      re: req.id,
      data: { data: CANARY, encoding: "utf8" },
    });
    fa.send({
      type: "fetch.end",
      re: req.id,
      data: { error: { code: "WORKER_UNAVAILABLE", message: CANARY } },
    });
    const res = await p;

    await assert.rejects(res.text());

    // События: по схеме, не по схеме (замечание — в журнал, значение — нет), неверное.
    const event = (id: string, data: unknown) =>
      fa.send({
        type: "event",
        id,
        data: { worker: "w", type: "example.done", data, at: Date.now() },
      });

    event("e1", { n: 1, token: CANARY });
    await fa.ackOf("e1");
    event("e2", { n: CANARY });
    await fa.ackOf("e2");
    fa.send({ type: "event", id: "e3", data: { worker: "w", data: CANARY } });
    await fa.next("error", e => e.re === "e3");

    // Запрос воркера с меткой в data; ответ с ней же.
    fa.send({
      type: "request",
      id: "r1",
      data: { worker: "w", type: "example.ask", data: secret, timeoutMs: 1000 },
    });
    const reply = await fa.next("request.result", e => e.re === "r1");

    assert.deepEqual(reply.data.data, { echo: secret });

    const text = JSON.stringify(logged);

    assert.match(text, /data события не по схеме/, "журнал не пустой");
    for (const x of [CANARY, TOKEN, creds.secret])
      assert.ok(!text.includes(x), `в журнале ${x}: ${text}`);
  });
});
