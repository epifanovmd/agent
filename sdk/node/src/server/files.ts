// Файлы задач: провайдер ссылок. Бэкенд отдаёт подписанные ссылки своего
// хранилища (S3 и т. п.); MemoryFiles — файлы в памяти, их GET/PUT
// обслуживает транспорт Agents (agents.handle).
import { EventEmitter } from "node:events";
import type { IncomingMessage, ServerResponse } from "node:http";
import type { JobUrls } from "../index";
import { readBody } from "./http";
import type { Job } from "./model";

/** Провайдер файлов задач. */
export interface Files {
  /** Входы запроса задачи при постановке (MemoryFiles — содержимое; свой провайдер — URL и т. п.). */
  saveInputs(jobId: string, inputs: Record<string, string | Uint8Array>): Promise<void>;
  /**
   * Ссылки задачи для агента: все или перечисленные. baseUrl — адрес сервера,
   * по которому пришёл агент.
   */
  urls(job: Job, baseUrl: string, only?: { inputs?: string[]; outputs?: string[] }): Promise<JobUrls>;
  /** Обслужить HTTP-запрос к файлам (если провайдер раздаёт их сам); обработан? */
  handle?(req: IncomingMessage, res: ServerResponse, path: string): Promise<boolean>;
}

export interface MemoryFilesOptions {
  /** Префикс пути (по умолчанию /files): GET/PUT <prefix>/<jobId>/(in|out)/<имя>. */
  prefix?: string;
  /** Срок ссылок, мс (по умолчанию час). */
  ttlMs?: number;
  /** Входов и выходов хранить, задач (по умолчанию 500; старые удаляются). */
  keepJobs?: number;
}

/**
 * Файлы в памяти; ссылки без подписи — для разработки. События: "upload"
 * `{ jobId, name }` — агент загрузил выходной файл.
 */
export class MemoryFiles extends EventEmitter implements Files {
  readonly prefix: string;
  private readonly ttlMs: number;
  private readonly keepJobs: number;
  private files = new Map<string, Map<string, Buffer>>(); // jobId → "in/имя" | "out/имя" → данные

  constructor(opts: MemoryFilesOptions = {}) {
    super();
    this.prefix = (opts.prefix ?? "/files").replace(/\/$/, "");
    this.ttlMs = opts.ttlMs ?? 3600_000;
    this.keepJobs = opts.keepJobs ?? 500;
  }

  async saveInputs(jobId: string, inputs: Record<string, string | Uint8Array>): Promise<void> {
    for (const [name, content] of Object.entries(inputs)) this.put(jobId, "in", name, Buffer.from(content));
  }

  async urls(job: Job, baseUrl: string, only: { inputs?: string[]; outputs?: string[] } = {}): Promise<JobUrls> {
    const base = `${baseUrl}${this.prefix}/${encodeURIComponent(job.id)}`;
    const ins = only.inputs ?? (only.outputs ? [] : job.inputs);
    const outs = only.outputs ?? (only.inputs ? [] : job.outputs);
    return {
      inputs: Object.fromEntries(
        ins.filter((n) => job.inputs.includes(n)).map((n) => [n, `${base}/in/${encodeURIComponent(n)}`]),
      ),
      outputs: Object.fromEntries(
        outs
          .filter((n) => job.outputs.includes(n))
          .map((n) => [n, { url: `${base}/out/${encodeURIComponent(n)}`, contentType: "application/octet-stream" }]),
      ),
      expiresAt: Date.now() + this.ttlMs,
    };
  }

  /** Содержимое файла задачи. */
  get(jobId: string, dir: "in" | "out", name: string): Buffer | undefined {
    return this.files.get(jobId)?.get(`${dir}/${name}`);
  }

  /** Имена загруженных выходных файлов задачи. */
  outputs(jobId: string): string[] {
    return [...(this.files.get(jobId)?.keys() ?? [])].filter((k) => k.startsWith("out/")).map((k) => k.slice(4));
  }

  put(jobId: string, dir: "in" | "out", name: string, data: Buffer): void {
    let m = this.files.get(jobId);
    if (!m) {
      this.files.set(jobId, (m = new Map()));
      for (const id of this.files.keys()) {
        if (this.files.size <= this.keepJobs) break;
        this.files.delete(id);
      }
    }
    m.set(`${dir}/${name}`, data);
    if (dir === "out") this.emit("upload", { jobId, name });
  }

  async handle(req: IncomingMessage, res: ServerResponse, path: string): Promise<boolean> {
    if (!path.startsWith(this.prefix + "/")) return false;
    const parts = path
      .slice(this.prefix.length + 1)
      .split("/")
      .map(decodeURIComponent);
    if (parts.length !== 3 || (parts[1] !== "in" && parts[1] !== "out")) return false;
    const [jobId, dir, name] = parts as [string, "in" | "out", string];
    if (req.method === "PUT") {
      this.put(jobId, dir, name, await readBody(req));
      res.writeHead(200).end();
      return true;
    }
    if (req.method === "GET") {
      const data = this.get(jobId, dir, name);
      if (!data) res.writeHead(404).end();
      else res.writeHead(200, { "Content-Type": "application/octet-stream", "Content-Length": data.length }).end(data);
      return true;
    }
    return false;
  }
}
