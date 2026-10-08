// Пример сервера на Node.js: Agents из agent-sdk/server (агенты подключаются сами —
// WebSocket или HTTP sync) и API веб-интерфейса examples/web.
//
//   PORT=8080 ENROLL_TOKEN=demo-token npm start   (tsx src/main.ts)
//   RELEASES_DIR=dist/<VERSION> — каталог выпуска агента (обновления, install.sh);
//   PUBLIC_KEY — ключ проверки релизов (base64), подставляется в install.sh.
//   METRICS_STORE_INTERVAL_MS — как часто сохранять точку метрик в историю (5000; 0 — каждую).
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Agents, MemoryFiles } from "agent-sdk/server";
import { createApp } from "./app";

const port = Number(process.env.PORT ?? 8080);
const enrollToken = process.env.ENROLL_TOKEN ?? "demo-token";
const webDir = resolve(process.env.WEB_DIR ?? resolve(dirname(fileURLToPath(import.meta.url)), "../../web/dist"));

const log = (msg: string, extra?: Record<string, unknown>) =>
  console.log(new Date().toISOString(), msg, extra ? JSON.stringify(extra) : "");

const files = new MemoryFiles();
const agents = new Agents({
  enrollToken,
  files,
  statusIntervalMs: Number(process.env.STATUS_INTERVAL_MS ?? 5000),
  metricsIntervalMs: Number(process.env.METRICS_INTERVAL_MS ?? 5000),
  // История — каждая точка обычного интервала (5 с): окно графика 15 мин — 180 точек, без
  // ступенек на стыке с частыми точками подписки. По умолчанию SDK сохранял бы раз в 15 с.
  metricsStoreIntervalMs: Number(process.env.METRICS_STORE_INTERVAL_MS ?? 5000),
  releasesDir: process.env.RELEASES_DIR ? resolve(process.env.RELEASES_DIR) : undefined,
  publicKey: process.env.PUBLIC_KEY || undefined,
  log,
});
const server = createApp(agents, files, webDir, { enrollToken, log });

server.listen(port, () =>
  log(`сервер на :${port}`, { enrollToken, webDir, releasesDir: process.env.RELEASES_DIR ?? null }),
);

// Остановка: агенты переподключатся сразу (1012), их итоги ждут в outbox.
for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    agents.close();
    server.close();
    setTimeout(() => process.exit(0), 300).unref();
  });
}
