// Сервер стенда: Agents из agent-sdk/server (агенты подключаются сами — по WebSocket) и
// небольшой HTTP API над ним (examples/API.md).
//
//   PORT=8080 ENROLL_TOKEN=demo-token npm start   (node --import tsx src/main.ts)
//   RELEASES_DIR=dist/<VERSION> — каталог сборок воркеров (обновления воркеров; его install.sh —
//     GET /install.sh); агент обновляется из своего каталога сборок (update.releases);
//   PUBLIC_KEY — ключ проверки подписи сборок (base64) и UPDATE_PUBLIC_KEYS — ещё ключи через
//     запятую: флагами --update-key в команде установки;
//   PUBLIC_URL — адрес сервера для команды установки (по умолчанию — из запроса);
//   VALIDATE=1 — проверять настройки и тела запросов к воркерам по схемам из манифеста воркера.
import { resolve } from "node:path";
import { format } from "node:util";

import { Agents } from "agent-sdk/server";

import { createApp } from "./app";
import { History } from "./history";
import { onWorkerRequest } from "./requests";

const env = process.env;
const port = Number(env.PORT ?? 8080);
const enrollToken = env.ENROLL_TOKEN ?? "demo-token";
const interval = (v: string | undefined) => Number(v ?? 5000);

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
  log,
});

history.follow(agents);
const server = createApp(agents, {
  history,
  enrollToken,
  publicUrl: env.PUBLIC_URL || undefined,
  releasesDir: env.RELEASES_DIR ? resolve(env.RELEASES_DIR) : undefined,
  updateKeys: [
    env.PUBLIC_KEY ?? "",
    ...(env.UPDATE_PUBLIC_KEYS ?? "").split(","),
  ]
    .map(k => k.trim())
    .filter(Boolean),
  validate: env.VALIDATE === "1",
});

server.listen(port, () =>
  log(`сервер на :${port}`, {
    enrollToken,
    releasesDir: env.RELEASES_DIR ?? null,
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
