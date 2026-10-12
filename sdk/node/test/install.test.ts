// Команда установки агента: флаги `agent install` по порядку, значения в кавычках POSIX.
import assert from "node:assert/strict";
import { test } from "node:test";

import {
  Agents,
  AgentsError,
  bundleInstallUrl,
  type InstallOptions,
} from "../src/server/index";

const invalid = (err: unknown) =>
  err instanceof AgentsError && err.code === "MESSAGE_INVALID";

test("installCommand: флаги по порядку, кавычки POSIX, ошибки", async () => {
  const agents = new Agents({ enrollToken: "t", log: () => {} });

  try {
    const full: InstallOptions = {
      baseUrl: "https://api.example.com:8443/agents/",
      instance: "web-2",
      token: "tok'en",
      name: "node 01",
      user: "root",
      config: "/etc/agent/node.yaml",
      privileged: true,
      killMode: "process",
      packages: ["jq", "curl"],
      packagesByManager: { apk: ["bind-tools"] },
      sysctl: { "vm.max_map_count": "262144", "net.core.somaxconn": "1024" },
      rwPaths: ["/etc/example", "/var/lib/it's"],
      caFile: "/etc/agent/ca.pem",
      workers: ["report"],
      stopTimeout: "15min",
      releases: "https://example.com/releases",
    };

    assert.equal(
      agents.installCommand(full),
      "curl -fsSL 'https://api.example.com:8443/agents/api/v1/agent-link/install.sh' | sudo sh -s -- " +
        "--instance 'web-2' --token 'tok'\\''en' --name 'node 01' --user 'root' --config '/etc/agent/node.yaml' --privileged --kill-mode 'process' " +
        "--packages 'jq curl' --packages-apk 'bind-tools' " +
        "--sysctl 'net.core.somaxconn=1024' --sysctl 'vm.max_map_count=262144' " +
        "--rw-path '/etc/example' --rw-path '/var/lib/it'\\''s' --ca-file '/etc/agent/ca.pem' --worker 'report' --stop-timeout '15min' " +
        "--releases 'https://example.com/releases'",
    );
    // Без адреса в опциях и в Agents — ошибка.
    assert.throws(() => agents.installCommand({ token: "it" }), invalid);
    const bad: Record<string, InstallOptions> = {
      "без токена": { baseUrl: "https://api.example.com", token: "" },
      "token и tokenFile": {
        baseUrl: "https://api.example.com",
        token: "t",
        tokenFile: "/run/t",
      },
      killMode: {
        baseUrl: "https://api.example.com",
        token: "t",
        killMode: "all" as "process",
      },
      "менеджер пакетов": {
        baseUrl: "https://api.example.com",
        token: "t",
        packagesByManager: {
          pacman: ["x"],
        } as InstallOptions["packagesByManager"],
      },
      "пакет менеджера": {
        baseUrl: "https://api.example.com",
        token: "t",
        packagesByManager: { apt: ["a b"] },
      },
      "адрес с $": { baseUrl: "https://api.example.com/$(id)", token: "t" },
      "адрес с кавычкой": {
        baseUrl: "https://api.example.com/a'b",
        token: "t",
      },
      "адрес с пробелом": {
        baseUrl: "https://api.example.com/a b",
        token: "t",
      },
      "не http": { baseUrl: "ftp://api.example.com", token: "t" },
      пакет: {
        baseUrl: "https://api.example.com",
        token: "t",
        packages: ["a;b"],
      },
      "пустое имя пакета": {
        baseUrl: "https://api.example.com",
        token: "t",
        packages: [""],
      },
      "ключ sysctl": {
        baseUrl: "https://api.example.com",
        token: "t",
        sysctl: { "net ipv4": "1" },
      },
      "имя воркера": {
        baseUrl: "https://api.example.com",
        token: "t",
        workers: ["a b"],
      },
      "имя экземпляра": {
        baseUrl: "https://api.example.com",
        token: "t",
        instance: "Web",
      },
      "пустое имя воркера": {
        baseUrl: "https://api.example.com",
        token: "t",
        workers: [""],
      },
      "перевод строки": {
        baseUrl: "https://api.example.com",
        token: "t",
        name: "a\nb",
      },
      "адрес сборок": {
        baseUrl: "https://api.example.com",
        token: "t",
        releases: "file:///x",
      },
      "rootless и sysctl": {
        baseUrl: "https://api.example.com",
        token: "t",
        rootless: true,
        sysctl: { "vm.max_map_count": "262144" },
      },
      "rootless и privileged": {
        baseUrl: "https://api.example.com",
        token: "t",
        rootless: true,
        privileged: true,
      },
      "run и воркеры": {
        baseUrl: "https://api.example.com",
        token: "t",
        run: true,
        workers: ["report"],
      },
      "run и rootless": {
        baseUrl: "https://api.example.com",
        token: "t",
        run: true,
        rootless: true,
      },
    };

    for (const [name, opts] of Object.entries(bad))
      assert.throws(() => agents.installCommand(opts), invalid, name);
  } finally {
    await agents.close();
  }
  // Адрес — опция baseUrl Agents.
  const withBase = new Agents({
    enrollToken: "t",
    baseUrl: "http://127.0.0.1:8080",
    log: () => {},
  });

  try {
    assert.equal(
      withBase.installCommand({ token: "it" }),
      "curl -fsSL 'http://127.0.0.1:8080/api/v1/agent-link/install.sh' | sudo sh -s -- --token 'it'",
    );
    // Токен из файла на узле — вместо token.
    assert.equal(
      withBase.installCommand({
        tokenFile: "/root/agent.token",
        killMode: "mixed",
      }),
      "curl -fsSL 'http://127.0.0.1:8080/api/v1/agent-link/install.sh' | sudo sh -s -- " +
        "--token-file '/root/agent.token' --kill-mode 'mixed'",
    );
    // Агент пользователя без root и запуск без установки (контейнер) — без sudo.
    assert.equal(
      withBase.installCommand({ token: "it", rootless: true }),
      "curl -fsSL 'http://127.0.0.1:8080/api/v1/agent-link/install.sh' | sh -s -- --rootless --token 'it'",
    );
    assert.equal(
      withBase.installCommand({ token: "it", run: true, name: "n1" }),
      "curl -fsSL 'http://127.0.0.1:8080/api/v1/agent-link/install.sh' | sh -s -- --run --token 'it' --name 'n1'",
    );
    // Из архива проекта: служба, без root, без установки; ошибки.
    assert.equal(
      withBase.bundleInstallCommand({ token: "a'b", name: "msk 1" }),
      "curl -fsSL 'http://127.0.0.1:8080/api/v1/agent-link/bundle/install.sh' | sudo sh -s -- --token 'a'\\''b' --name 'msk 1'",
    );
    assert.equal(
      withBase.bundleInstallCommand({ tokenFile: "/run/t", rootless: true }),
      "curl -fsSL 'http://127.0.0.1:8080/api/v1/agent-link/bundle/install.sh' | sh -s -- --rootless --token-file '/run/t'",
    );
    assert.equal(
      withBase.bundleInstallCommand({ token: "t", run: true }),
      "curl -fsSL 'http://127.0.0.1:8080/api/v1/agent-link/bundle/install.sh' | sh -s -- --run --token 't'",
    );
    assert.equal(
      bundleInstallUrl("https://api.example.com/"),
      "https://api.example.com/api/v1/agent-link/bundle/install.sh",
    );
    for (const bad of [
      { token: "t", run: true, rootless: true },
      { token: "t", tokenFile: "/run/t" },
      { token: "t", name: "a\nb" },
    ])
      assert.throws(() => withBase.bundleInstallCommand(bad), invalid);
  } finally {
    await withBase.close();
  }
});
