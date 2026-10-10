// Вид агента для бэкенда: новая версия агента — из status, пока его нет — из hello.
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { type AgentRecord, publicAgent } from "../src/server";

const record = (patch: Partial<AgentRecord>): AgentRecord => ({
  id: "a1",
  name: "node-01",
  grantedLabels: {},
  labels: {},
  revoked: false,
  online: true,
  enrolledAt: 1,
  secretHash: "x",
  lastSeq: 0,
  configs: {},
  alerts: [],
  rev: 1,
  ...patch,
});

const hello = {
  agent: {
    version: "1.0.0",
    bootId: "b1",
    startedAt: 1,
    update: { latest: "1.1.0", checkedAt: 5 },
  },
  host: { os: "linux", arch: "amd64", hostname: "node-01" },
};

describe("publicAgent: update", () => {
  it("из hello, пока status нет; status — важнее", () => {
    assert.deepEqual(publicAgent(record({ hello })).update, {
      latest: "1.1.0",
      checkedAt: 5,
    });
    const newer = { workers: [], update: { latest: "1.2.0", checkedAt: 9 } };

    assert.equal(
      publicAgent(record({ hello, status: newer })).update?.latest,
      "1.2.0",
    );
    // В status update нет — новой версии больше нет (агент обновился или её отозвали).
    assert.equal(
      publicAgent(record({ hello, status: { workers: [] } })).update,
      undefined,
    );
  });
});
