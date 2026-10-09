// Проверка входящих данных схемами: незнакомые поля, пределы, причины отказа по-русски.
import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  parseConfigApplied,
  parseEnroll,
  parseEvent,
  parseHello,
  parseLabels,
  parseManifest,
  parseMetrics,
  parseName,
  parseStatus,
} from "../src/server/protocol/checks";
import {
  MAX_AGENT_NAME,
  MAX_LABEL,
  MAX_LABELS,
} from "../src/server/protocol/messages";
import { sample } from "./helpers";

const reason = (p: { ok: boolean; error?: string }) => {
  assert.equal(p.ok, false);

  return p.error ?? "";
};

describe("проверка входящих данных", () => {
  it("образцы проходят, незнакомые поля пропускаются", () => {
    const hello = parseHello({
      ...sample("hello").data,
      extra: 1,
      agent: { ...sample("hello").data.agent, build: "x" },
    });

    assert.ok(hello.ok);
    // Тип hello допускает любые поля — они сохраняются.
    assert.equal(hello.value.extra, 1);
    assert.equal(hello.value.agent.build, "x");

    const status = parseStatus({ ...sample("status").data, later: true });

    assert.ok(status.ok);
    assert.equal(status.value.workers.length, 5);
    const legacy = status.value.workers.find(w => w.name === "legacy");

    assert.equal(legacy?.state, "invalid");
    assert.match(legacy?.message ?? "", /GET \/manifest/);

    const metrics = parseMetrics({ ...sample("metrics").data, later: true });

    assert.ok(metrics.ok);
    assert.equal(metrics.value.at, sample("metrics").data.collectedAt);
    assert.equal("later" in metrics.value, false);

    const event = parseEvent({ ...sample("event").data, later: true });

    assert.ok(event.ok);
    assert.equal(event.value.type, "report.sent");
  });

  it("status: health.info не объект — пропускается, status принят", () => {
    const status = parseStatus({
      workers: [
        { name: "echo", state: "running", health: { ok: true, info: [1] } },
        { name: "report", state: "running", health: { ok: true, info: "x" } },
      ],
    });

    assert.ok(status.ok);
    for (const w of status.value.workers) {
      assert.equal(w.health?.ok, true);
      assert.equal(w.health?.info, undefined);
    }
  });

  it("причина отказа — где и что, по-русски", () => {
    assert.equal(
      reason(parseName("worker", "Echo")),
      `worker: "Echo" не по правилу /^[a-z][a-z0-9-]{0,31}$/`,
    );
    assert.equal(
      reason(parseStatus({ workers: [{ name: "echo", state: 1 }] })),
      "status.workers[0].state: ожидается строка",
    );
    assert.equal(reason(parseStatus(undefined)), "status: нет значения");
    assert.equal(
      reason(parseConfigApplied({ worker: "echo", key: "main", ok: true })),
      "config.applied.version: нет значения",
    );
    assert.match(
      reason(
        parseHello({ ...sample("hello").data, agent: { version: "1.0.0" } }),
      ),
      /^hello\.agent\.bootId: нет значения/,
    );
  });

  it("пределы меток и имени — из постоянных протокола", () => {
    const many = Object.fromEntries(
      Array.from({ length: MAX_LABELS + 1 }, (_, i) => [`k${i}`, "v"]),
    );

    assert.match(
      reason(parseLabels(many)),
      new RegExp(`меток больше ${MAX_LABELS}`),
    );
    assert.match(
      reason(parseLabels({ zone: "я".repeat(MAX_LABEL + 1) })),
      new RegExp(`labels.zone: длиннее ${MAX_LABEL} символов`),
    );
    // Предел — в символах, а не в единицах UTF-16.
    assert.ok(parseLabels({ zone: "😀".repeat(MAX_LABEL) }).ok);
    assert.match(
      reason(parseLabels({ "": "v" })),
      /ключ метки — непустая строка/,
    );
    assert.deepEqual(parseLabels(undefined), { ok: true, value: {} });

    const body = { token: "t", name: "n".repeat(MAX_AGENT_NAME + 1) };

    assert.match(reason(parseEnroll(body)), /^name: длиннее/);
    assert.match(reason(parseEnroll({ name: "n" })), /^token: нет значения/);
  });

  it("manifest.json: неверные сборки пропускаются", () => {
    const m = parseManifest({
      version: "1.1.0",
      artifacts: [
        { os: "linux", arch: "amd64", file: "agent", sha256: "aa" },
        { os: "linux", arch: "arm64", sha256: "bb" },
      ],
      workers: "нет",
    });

    assert.ok(m.ok);
    assert.deepEqual(
      m.value.artifacts.map(a => a.arch),
      ["amd64"],
    );
    assert.equal(m.value.workers, undefined);
    assert.match(
      reason(parseManifest({ artifacts: [] })),
      /^manifest\.json\.version/,
    );
  });
});
