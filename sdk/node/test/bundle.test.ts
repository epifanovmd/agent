// Архивы папки агента проекта: bundle/install.sh с адресом сервера и новейший архив под
// платформу из bundlesDir; без bundlesDir — маршрутов нет.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { startServer } from "./helpers";

test("bundle: install.sh с адресом сервера, новейший архив под платформу", async () => {
  const dir = mkdtempSync(join(tmpdir(), "agent-bundles-"));
  const old = join(dir, "agent-prod-1.0.0-linux-amd64.tar.gz");
  const fresh = join(dir, "agent-prod-1.1.0-linux-amd64.tar.gz");

  writeFileSync(old, "old");
  writeFileSync(fresh, "fresh");
  writeFileSync(join(dir, "agent-prod-1.1.0-darwin-arm64.tar.gz"), "mac");
  utimesSync(old, new Date(2000, 0, 1), new Date(2000, 0, 1));
  const s = await startServer({ bundlesDir: dir });

  try {
    const res = await fetch(`${s.url}/api/v1/agent-link/bundle/install.sh`);
    const script = await res.text();

    assert.equal(res.status, 200);
    assert.match(script, new RegExp(`^SERVER="${s.url}"$`, "m"));
    assert.match(script, /--run\) MODE=run/);
    assert.match(script, /exec "\$DIR\/agent\/agent" run -config/);
    assert.match(
      script,
      /exec "\$DIR\/agent\/agent" install --server "\$SERVER" "\$@"/,
    );
    // Скрипт — корректный sh.
    assert.equal(
      spawnSync("sh", ["-n"], { input: script }).status,
      0,
      "sh -n install.sh",
    );

    const archive = await fetch(
      `${s.url}/api/v1/agent-link/bundle/linux-amd64.tar.gz`,
    );

    assert.equal(archive.status, 200);
    assert.equal(await archive.text(), "fresh");
    assert.equal(
      (await fetch(`${s.url}/api/v1/agent-link/bundle/linux-arm64.tar.gz`))
        .status,
      404,
    );
    // Под macOS — для разработки.
    assert.equal(
      await (
        await fetch(`${s.url}/api/v1/agent-link/bundle/darwin-arm64.tar.gz`)
      ).text(),
      "mac",
    );
    assert.equal(
      (await fetch(`${s.url}/api/v1/agent-link/bundle/linux-sparc.tar.gz`))
        .status,
      404,
    );
  } finally {
    await s.close();
  }
  // Без bundlesDir маршрутов нет.
  const plain = await startServer();

  try {
    assert.equal(
      (await fetch(`${plain.url}/api/v1/agent-link/bundle/install.sh`)).status,
      404,
    );
  } finally {
    await plain.close();
  }
});
