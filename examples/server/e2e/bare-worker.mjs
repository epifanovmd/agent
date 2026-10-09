// Воркер для сквозного теста регистрации (§12): GET /health — всегда; GET /manifest — тело из
// файла BARE_MANIFEST (нет файла — 404: воркер не зарегистрирован); POST /emit?type=… —
// событие агенту, ответ — { status } ответа агента.
import { readFile } from "node:fs/promises";
import { request } from "node:http";
import { createServer } from "node:http";

const agent = (method, path, body) =>
  new Promise((resolve, reject) => {
    const req = request(
      {
        socketPath: process.env.AGENT_SOCKET,
        method,
        path,
        headers: {
          authorization: `Bearer ${process.env.AGENT_WORKER_TOKEN}`,
          "content-type": "application/json",
        },
      },
      res => {
        res.resume();
        res.on("end", () => resolve(res.statusCode));
      },
    );

    req.on("error", reject);
    req.end(JSON.stringify(body));
  });

const json = (res, status, body) =>
  res
    .writeHead(status, { "content-type": "application/json" })
    .end(JSON.stringify(body));

const server = createServer(async (req, res) => {
  const url = new URL(req.url ?? "/", "http://worker");

  if (req.method === "GET" && url.pathname === "/health")
    return json(res, 200, { ok: true });
  if (req.method === "GET" && url.pathname === "/manifest") {
    const raw = await readFile(process.env.BARE_MANIFEST ?? "", "utf8").catch(
      () => undefined,
    );

    if (raw === undefined) return json(res, 404, { message: "нет манифеста" });
    res.writeHead(200, { "content-type": "application/json" }).end(raw);

    return;
  }
  if (req.method === "POST" && url.pathname === "/emit") {
    const status = await agent("POST", "/events", {
      type: url.searchParams.get("type"),
    });

    return json(res, 200, { status });
  }
  json(res, 404, { message: "нет маршрута" });
});

server.listen(process.env.AGENT_WORKER_SOCKET);
process.on("SIGTERM", () => server.close(() => process.exit(0)));
