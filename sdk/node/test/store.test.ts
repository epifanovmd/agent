// MemoryStore — то, на что опирается Agents и что должен уметь свой Store (sdk/docs/store.md).
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { type AgentRecord, MemoryStore } from "../src/server/index";

const agent = (id: string): AgentRecord => ({
  id,
  name: id,
  grantedLabels: {},
  labels: {},
  revoked: false,
  online: false,
  enrolledAt: 1,
  secretHash: "h",
  lastSeq: 0,
  configs: {},
  alerts: [],
  rev: 0,
});

describe("MemoryStore", () => {
  it("условная запись агента по rev", async () => {
    const s = new MemoryStore();

    await s.createAgent(agent("a"));
    const x = (await s.getAgent("a"))!;
    const y = (await s.getAgent("a"))!;

    x.name = "x";
    assert.equal(await s.updateAgent(x), true);
    assert.equal(x.rev, 1);
    y.name = "y";
    assert.equal(await s.updateAgent(y), false);
    assert.equal((await s.getAgent("a"))!.name, "x");
    assert.equal(await s.updateAgent(agent("nope")), false);
  });

  it("версия настроек растёт и после удаления; minVersion", async () => {
    const s = new MemoryStore();

    assert.equal((await s.setConfig("a", "w", "k", 1)).version, 1);
    assert.equal((await s.setConfig("a", "w", "k", 2)).version, 2);
    assert.equal(await s.deleteConfig("a", "w", "k"), true);
    assert.equal(await s.deleteConfig("a", "w", "k"), false);
    assert.deepEqual(await s.listConfigs("a"), []);
    assert.equal((await s.setConfig("a", "w", "k", 3)).version, 3);
    assert.equal(
      (await s.setConfig("a", "w", "k", 4, { minVersion: 10 })).version,
      10,
    );
    assert.deepEqual(
      (await s.listConfigs("a")).map(c => c.data),
      [4],
    );
  });

  it("удаление агента убирает его настройки; записи копируются", async () => {
    const s = new MemoryStore();

    await s.createAgent(agent("a"));
    await assert.rejects(s.createAgent(agent("a")));
    await s.setConfig("a", "w", "k", { n: 1 });
    await s.setConfig("b", "w", "k", 1);
    const got = (await s.listConfigs("a"))[0];

    (got.data as { n: number }).n = 2;
    assert.deepEqual((await s.listConfigs("a"))[0].data, { n: 1 });
    assert.equal(await s.deleteAgent("a"), true);
    assert.equal(await s.deleteAgent("a"), false);
    assert.deepEqual(await s.listConfigs("a"), []);
    assert.equal((await s.listConfigs("b")).length, 1);
    assert.deepEqual(await s.listAgents(), []);
  });
});
