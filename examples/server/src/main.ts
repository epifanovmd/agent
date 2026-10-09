// Сервер стенда: Agents из agent-sdk/server (агенты подключаются сами — по WebSocket) и
// небольшой HTTP API над ним (examples/API.md).
//
//   PORT=8080 ENROLL_TOKEN=demo-token npm start   (node --import tsx src/main.ts)
//   RELEASES_DIR=dist/<VERSION> — каталог выпуска (обновления, install.sh); с AGENT_RELEASES_* —
//     воркеры проекта;
//   AGENT_RELEASES_GITHUB=owner/repo — агент и его воркеры из выпусков GitHub (AGENT_RELEASES_RANGE —
//     диапазон версий, AGENT_RELEASES_TOKEN — токен API) или AGENT_RELEASES_URL — база выпуска;
//     AGENT_RELEASES_PROXY=1 — сборки узлам через сервер потоком;
//   PUBLIC_KEY — ключ проверки выпуска (base64), вписывается в install.sh; UPDATE_PUBLIC_KEYS — ещё
//     ключи через запятую;
//   PUBLIC_URL — адрес сервера для команды установки (по умолчанию — из запроса);
//   VALIDATE_CONFIGS=1 — проверять настройки по схеме из манифеста воркера до отправки агенту;
//   VALIDATE_REQUESTS=1 — проверять тело запросов к воркерам и data запросов воркеров по схемам;
//   VALIDATE_EVENTS=log|reject — проверять data событий по схемам (по умолчанию off).
import { resolve } from "node:path";
import { format } from "node:util";

import { Agents, type AgentsOptions } from "agent-sdk/server";

import { createApp } from "./app";
import { History } from "./history";
import { onWorkerRequest } from "./requests";

const env = process.env;
const port = Number(env.PORT ?? 8080);
const enrollToken = env.ENROLL_TOKEN ?? "demo-token";
const interval = (v: string | undefined) => Number(v ?? 5000);

/** Удалённый источник выпуска агента из окружения; не задан — undefined. */
const agentReleases = (): AgentsOptions["agentReleases"] => {
  const common = {
    proxy: env.AGENT_RELEASES_PROXY === "1",
    checkIntervalMs: env.AGENT_RELEASES_CHECK_INTERVAL_MS
      ? Number(env.AGENT_RELEASES_CHECK_INTERVAL_MS)
      : undefined,
  };

  if (env.AGENT_RELEASES_GITHUB)
    return {
      ...common,
      github: env.AGENT_RELEASES_GITHUB,
      range: env.AGENT_RELEASES_RANGE || undefined,
      token: env.AGENT_RELEASES_TOKEN || undefined,
    };
  if (env.AGENT_RELEASES_URL) return { ...common, url: env.AGENT_RELEASES_URL };

  return undefined;
};

const log = (msg: string, extra?: Record<string, unknown>) => {
  process.stdout.write(
    `${format(new Date().toISOString(), msg, ...(extra ? [extra] : []))}\n`,
  );
};

// Хранилище и история — в памяти: перезапуск сервера — агенты регистрируются заново.
const history = new History({
  metricsEveryMs: interval(env.METRICS_HISTORY_INTERVAL_MS),
});
const agents = new Agents({
  enrollToken,
  onEvent: history.addEvent,
  onWorkerRequest,
  statusIntervalMs: interval(env.STATUS_INTERVAL_MS),
  metricsIntervalMs: interval(env.METRICS_INTERVAL_MS),
  releasesDir: env.RELEASES_DIR ? resolve(env.RELEASES_DIR) : undefined,
  agentReleases: agentReleases(),
  publicKey: env.PUBLIC_KEY || undefined,
  updatePublicKeys: (env.UPDATE_PUBLIC_KEYS ?? "")
    .split(",")
    .map(k => k.trim())
    .filter(Boolean),
  baseUrl: env.PUBLIC_URL || undefined,
  validateConfigs: env.VALIDATE_CONFIGS === "1",
  validateRequests: env.VALIDATE_REQUESTS === "1",
  validateEvents:
    env.VALIDATE_EVENTS === "log" || env.VALIDATE_EVENTS === "reject"
      ? env.VALIDATE_EVENTS
      : "off",
  log,
});

history.follow(agents);
const server = createApp(agents, {
  history,
  enrollToken,
  publicUrl: env.PUBLIC_URL || undefined,
});

server.listen(port, () =>
  log(`сервер на :${port}`, {
    enrollToken,
    releasesDir: env.RELEASES_DIR ?? null,
    agentReleases: env.AGENT_RELEASES_GITHUB ?? env.AGENT_RELEASES_URL ?? null,
  }),
);

// Остановка: агенты переподключатся сразу (1012), их важные сообщения ждут в outbox.
for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    void agents.close();
    server.close();
    setTimeout(() => process.exit(0), 300).unref();
  });
}
