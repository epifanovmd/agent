// Мелочи HTTP без фреймворка: тело запроса, ответ JSON, адрес сервера.
import type { IncomingMessage, ServerResponse } from "node:http";
import { AgentsError } from "./model";

const MAX_BODY = 32 << 20;

/**
 * Тело запроса не больше limit байт; больше — AgentsError MESSAGE_INVALID со статусом 413
 * (по Content-Length — сразу, без чтения).
 */
export async function readBody(req: IncomingMessage, limit = MAX_BODY): Promise<Buffer> {
  const tooLarge = () => new AgentsError("MESSAGE_INVALID", `Тело запроса больше ${limit} байт`, 413);
  if (Number(req.headers["content-length"]) > limit) throw tooLarge();
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > limit) throw tooLarge();
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- тело JSON произвольно; any — умолчание публичного API
export async function readJSON(req: IncomingMessage): Promise<any> {
  const raw = await readBody(req);
  return raw.length ? JSON.parse(raw.toString("utf8")) : {};
}

export function sendJSON(res: ServerResponse, status: number, body: unknown): void {
  const raw = JSON.stringify(body);
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(raw),
  });
  res.end(raw);
}

/**
 * Адрес сервера, по которому клиент до него дошёл (ссылки на файлы задач): Host и TLS сокета;
 * trustProxy (за доверенным прокси) — X-Forwarded-Host и X-Forwarded-Proto, если они есть.
 */
export function baseUrl(req: IncomingMessage, trustProxy = false): string {
  const first = (v: string | string[] | undefined) =>
    String(Array.isArray(v) ? v[0] : (v ?? ""))
      .split(",")[0]
      .trim();
  const tls = (req.socket as { encrypted?: boolean } | undefined)?.encrypted === true;
  let proto = tls ? "https" : "http";
  let host = req.headers.host || "localhost";
  if (trustProxy) {
    proto = first(req.headers["x-forwarded-proto"]) || proto;
    host = first(req.headers["x-forwarded-host"]) || host;
  }
  return `${proto}://${host}`;
}

/**
 * Адрес клиента — IP без порта: адрес сокета; trustProxy — первый адрес X-Forwarded-For
 * (за доверенным прокси). IPv4 в виде IPv6 (::ffff:10.0.0.5) — как IPv4.
 */
export function clientAddress(req: IncomingMessage, trustProxy = false): string {
  if (trustProxy) {
    const xff = req.headers["x-forwarded-for"];
    const first = String(Array.isArray(xff) ? xff[0] : (xff ?? ""))
      .split(",")[0]
      .trim();
    if (first) return stripPort(first);
  }
  return plainIp(req.socket?.remoteAddress ?? "");
}

/** «[::1]:443» → «::1», «10.0.0.5:80» → «10.0.0.5»; IPv6 без скобок — как есть. */
function stripPort(addr: string): string {
  const bracket = /^\[([^\]]+)\](?::\d+)?$/.exec(addr);
  if (bracket) return plainIp(bracket[1]);
  const v4 = /^(\d{1,3}(?:\.\d{1,3}){3}):\d+$/.exec(addr);
  return plainIp(v4 ? v4[1] : addr);
}

function plainIp(addr: string): string {
  return addr.toLowerCase().startsWith("::ffff:") && addr.includes(".") ? addr.slice(7) : addr;
}
