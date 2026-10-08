// Самопроверка воркеров из терминала — те же сценарии, что в интерфейсе
// (src/selfcheck.ts), против сервера DEMO_SERVER. Код выхода 1 — есть ошибки.
//   DEMO_SERVER=http://localhost:8080 npm run selfcheck   (node --import tsx selfcheck.cli.ts)
import { runSelfCheck, type Step } from "./src/selfcheck";

const base = process.env.DEMO_SERVER ?? "http://localhost:8080";
const nativeFetch = globalThis.fetch;
globalThis.fetch = (input: RequestInfo | URL, init?: RequestInit) =>
  nativeFetch(typeof input === "string" && input.startsWith("/") ? base + input : input, init);

const mark: Record<Step["state"], string> = { waiting: "…", running: "…", passed: "✔", failed: "✖", skipped: "–" };
let failed = 0;
await runSelfCheck((step) => {
  if (step.state === "running") return;
  if (step.state === "failed") failed++;
  console.log(`${mark[step.state]} [${step.worker}] ${step.title} — ${step.detail ?? ""} (${step.ms} мс)`);
});
process.exit(failed ? 1 : 0);
