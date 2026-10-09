// Сравнение версий агента и воркеров по semver.
import assert from "node:assert/strict";
import { it } from "node:test";

import { newerVersion, sameVersion } from "../src/server/lib/version";

it("semver: предварительные версии, числовые части, v", () => {
  assert.ok(newerVersion("2.0.0", "2.0.0-rc.1"));
  assert.ok(!newerVersion("2.0.0-rc.1", "2.0.0"));
  assert.ok(newerVersion("2.0.0-rc.2", "2.0.0-rc.1"));
  assert.ok(newerVersion("1.10.0", "1.9.0"));
  assert.ok(!newerVersion("1.2.0", "v1.2.0"));
  assert.ok(sameVersion("v1.2.0", "1.2.0"));
  assert.ok(sameVersion("1.2", "1.2.0"));
  assert.ok(!sameVersion("2.0.0-rc.1", "2.0.0"));
});

it("semver: некорректные версии", () => {
  // Версия новее не версии; не версии между собой не новее.
  assert.ok(newerVersion("1.0.0", "latest"));
  assert.ok(!newerVersion("latest", "1.0.0"));
  assert.ok(!newerVersion("latest", "dev"));
  assert.ok(!newerVersion("", ""));
  // Не версии сравниваются как строки.
  assert.ok(sameVersion("dev", "dev"));
  assert.ok(!sameVersion("dev", "1.0.0"));
  assert.ok(!sameVersion("", "1.0.0"));
});
