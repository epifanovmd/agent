// Сервер стенда: Agents из agent-sdk/server (агенты подключаются сами — по WebSocket) и
// небольшой HTTP API над ним (examples/API.md).
//
//   PORT=8080 ENROLL_TOKEN=demo-token npm start   (node --import tsx src/main.ts)
//   RELEASES_DIR=dist/<VERSION> — каталог выпуска (обновления, install.sh);
//   PUBLIC_KEY — ключ проверки выпуска (base64), вписывается в install.sh;
//   PUBLIC_URL — адрес сервера для команды установки (по умолчанию — из запроса);
//   VALIDATE_CONFIGS=1 — проверять настройки по схеме из манифеста воркера до отправки агенту.
import { resolve } from "node:path";
import { format } from "node:util";

import { Agents } from "agent-sdk/server";

import { createApp } from "./app";
import { History } from "./history";

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
  statusIntervalMs: interval(env.STATUS_INTERVAL_MS),
  metricsIntervalMs: interval(env.METRICS_INTERVAL_MS),
  releasesDir: env.RELEASES_DIR ? resolve(env.RELEASES_DIR) : undefined,
  publicKey: env.PUBLIC_KEY || undefined,
  baseUrl: env.PUBLIC_URL || undefined,
  validateConfigs: env.VALIDATE_CONFIGS === "1",
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
