// Манифест воркера (§12): приём в hello и status, шаблоны маршрутов, проверка значений по схемам
// манифеста (schemaProblems).
import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";

import {
  type Agent,
  schemaProblems,
  type WorkerManifest,
} from "../src/server/index";
import { jsonSchemaProblems } from "../src/server/lib/json-schema";
import { matchRoute } from "../src/server/model/manifest";
import {
  enroll,
  FakeAgent,
  sample,
  startServer,
  type TestServer,
} from "./helpers";

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

const settingsSchema = {
  type: "object",
  properties: {
    prefix: { type: "string", maxLength: 3 },
    upper: { type: "boolean" },
  },
  required: ["prefix"],
  additionalProperties: false,
};

const manifest: WorkerManifest = {
  version: "1.0.0",
  configs: [{ key: "settings", schema: settingsSchema }, { key: "limits" }],
  routes: [
    { method: "POST", path: "/echo" },
    { method: "GET", path: "/items/{id}/parts/{part}" },
  ],
  events: [{ type: "echo.done" }],
};

const agentWith = (m?: WorkerManifest): Pick<Agent, "workers"> => ({
  workers: [{ name: "echo", manifest: m }, { name: "plain" }],
});

describe("JSON Schema", () => {
  it("подходит — замечаний нет", () => {
    assert.deepEqual(jsonSchemaProblems(settingsSchema, { prefix: "> " }), []);
  });

  it("замечания по-русски с путём к полю, не больше трёх", () => {
    assert.deepEqual(
      jsonSchemaProblems(settingsSchema, { prefix: 5, upper: "да", x: 1 }),
      [
        "prefix: ожидается строка",
        "upper: ожидается true или false",
        "x: лишнее поле",
      ],
    );
    assert.deepEqual(
      jsonSchemaProblems(settingsSchema, { prefix: "длинный" }),
      ["prefix: нужно не больше 3 символов"],
    );
    assert.deepEqual(jsonSchemaProblems(settingsSchema, {}), [
      "нет поля prefix",
    ]);
    assert.deepEqual(
      jsonSchemaProblems(
        {
          type: "array",
          items: { type: "integer", minimum: 1 },
        },
        [1, 0],
      ),
      ["[1]: нужно не меньше 1"],
    );
    assert.deepEqual(jsonSchemaProblems({ enum: ["a", "b"] }, "c"), [
      'ожидается одно из: "a", "b"',
    ]);
  });

  it("версия схемы — по $schema", () => {
    const draft4 = {
      $schema: "http://json-schema.org/draft-04/schema#",
      type: "number",
      maximum: 5,
      exclusiveMaximum: true,
    };

    // exclusiveMaximum: true — правило draft-04: 5 уже не подходит.
    assert.equal(jsonSchemaProblems(draft4, 5).length, 1);
    assert.deepEqual(jsonSchemaProblems(draft4, 4), []);
  });

  it("схему нельзя применить — исключение", () => {
    assert.throws(() =>
      jsonSchemaProblems({ $ref: "https://example.com/schema.json" }, 1),
    );
  });
});

describe("маршруты и схемы", () => {
  it("шаблон маршрута: {name} — один непустой сегмент, параметры не учитываются", () => {
    assert.equal(matchRoute("/items/{id}", "/items/42"), true);
    assert.equal(matchRoute("/items/{id}", "/items/42?full=1"), true);
    assert.equal(matchRoute("/items/{id}", "/items/"), false);
    assert.equal(matchRoute("/items/{id}", "/items/42/x"), false);
    assert.equal(matchRoute("/items", "/items"), true);
    assert.equal(matchRoute("/items", "/other"), false);
    // Как у агента: {name} — не «.» и не «..», %XX раскрываются.
    assert.equal(matchRoute("/items/{id}", "/items/.."), false);
    assert.equal(matchRoute("/items/{id}", "/items/%2e%2e"), false);
    assert.equal(matchRoute("/items/{id}", "/items/."), false);
    assert.equal(matchRoute("/items/{id}", "/items/%D1%8F"), true);
  });

  it("schemaProblems: значение ключа, тело маршрута по шаблону; нечем проверить — пусто", () => {
    const a = agentWith(manifest);

    assert.deepEqual(
      schemaProblems(a, "echo", { config: "settings" }, { prefix: 5 }),
      ["prefix: ожидается строка"],
    );
    assert.deepEqual(
      schemaProblems(a, "echo", { config: "settings" }, { prefix: "> " }),
      [],
    );
    // Ключ без схемы, незнакомый ключ, воркер без манифеста, нет воркера.
    assert.deepEqual(schemaProblems(a, "echo", { config: "limits" }, 5), []);
    assert.deepEqual(schemaProblems(a, "echo", { config: "main" }, 5), []);
    assert.deepEqual(schemaProblems(a, "plain", { config: "settings" }, 5), []);
    assert.deepEqual(schemaProblems(a, "nope", { config: "settings" }, 5), []);
    const withBody = agentWith({
      routes: [
        {
          method: "POST",
          path: "/items/{id}",
          request: { type: "object", required: ["n"] },
        },
      ],
    });

    assert.deepEqual(
      schemaProblems(
        withBody,
        "echo",
        { route: { method: "post", path: "/items/7?x=1" } },
        {},
      ),
      ["нет поля n"],
    );
    assert.deepEqual(
      schemaProblems(
        withBody,
        "echo",
        { route: { method: "GET", path: "/items/7" } },
        {},
      ),
      [],
    );
  });

  it("schemaProblems: схему нельзя применить — исключение", () => {
    const a = agentWith({
      configs: [
        { key: "settings", schema: { $ref: "https://example.com/s.json" } },
      ],
    });

    assert.throws(() => schemaProblems(a, "echo", { config: "settings" }, 1));
  });
});

/** Агент на связи: в hello и status — воркер echo с манифестом m. */
const connect = async (s: TestServer, m: unknown) => {
  const c = await enroll(s.url);
  const fa = await FakeAgent.connect(s.ws, c, {
    workers: [{ name: "echo", manifest: m }],
    configs: {},
  });

  return { c, fa };
};

describe("манифест в hello и status", () => {
  it("воркер агента с манифестом из hello, затем из status", async () => {
    const s = await server();
    const { c, fa } = await connect(s, manifest);
    const worker = async () =>
      (await s.agents.getAgent(c.agentId))?.workers.find(
        w => w.name === "echo",
      );

    assert.deepEqual((await worker())?.manifest, manifest);
    const next = { version: "1.1.0", events: [{ type: "echo.done" }] };

    fa.stream("status", {
      workers: [{ name: "echo", state: "running", manifest: next }],
      outbox: 0,
    });
    await fa.ackSeq(1);
    assert.deepEqual((await worker())?.manifest, next);
  });

  it("неверные элементы пропускаются, неверный манифест — нет манифеста", async () => {
    const s = await server();
    const { c, fa } = await connect(s, {
      version: "1.0.0",
      extra: true,
      configs: [{ key: "Bad" }, { key: "ok", schema: "строка" }],
      routes: [
        { method: "get", path: "/a" },
        { method: "GET", path: "/b" },
      ],
      events: "не список",
      jobs: [
        { type: "report.build", schema: { type: "object" } },
        { type: "Bad" },
        { type: "report.check", schema: [] },
      ],
    });
    const echo = (await s.agents.getAgent(c.agentId))?.workers[0];

    assert.deepEqual(echo?.manifest, {
      version: "1.0.0",
      configs: [{ key: "ok" }],
      routes: [{ method: "GET", path: "/b" }],
      jobs: [
        { type: "report.build", schema: { type: "object" } },
        { type: "report.check" },
      ],
    });
    fa.stream("status", {
      workers: [{ name: "echo", state: "running", manifest: "манифест" }],
      outbox: 0,
    });
    await fa.ackSeq(1);
    const a = await s.agents.getAgent(c.agentId);

    assert.equal(a?.status?.workers[0].state, "running");
    assert.equal(a?.status?.workers[0].manifest, undefined);
  });

  it("события job.* в events пропускаются: они зарезервированы для задач", async () => {
    const s = await server();
    const { c } = await connect(s, {
      version: "1.0.0",
      events: [{ type: "job.done" }, { type: "report.sent" }],
    });

    assert.deepEqual(
      (await s.agents.getAgent(c.agentId))?.workers[0].manifest?.events,
      [{ type: "report.sent" }],
    );
  });

  it("образцы hello и status: манифест принят как есть", async () => {
    const s = await server();
    const c = await enroll(s.url);
    const fa = await FakeAgent.connect(s.ws, c);
    const st = sample("status");

    fa.send(st);
    await fa.ackSeq(st.seq!);
    const a = await s.agents.getAgent(c.agentId);

    assert.deepEqual(
      a?.workers.find(w => w.name === "report")?.manifest,
      st.data.workers[0].manifest,
    );
    assert.deepEqual(
      a?.hello?.workers?.[0].manifest,
      sample("hello").data.workers[0].manifest,
    );
  });
});
