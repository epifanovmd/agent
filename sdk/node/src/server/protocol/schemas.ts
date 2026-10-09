// Схемы zod входящих данных: сообщения агента, манифест воркера, тело регистрации, manifest.json
// сборок, имена и метки. Типы протокола выводятся из этих схем (messages.ts), проверка — checks.ts.
// Незнакомые поля получатель пропускает: объект, чей тип допускает любые поля, сохраняет их
// (looseObject), остальные их отбрасывают.
import { z } from "zod";

import {
  EVENT_TYPE_PATTERN,
  JOB_EVENT_PREFIX,
  MAX_AGENT_NAME,
  MAX_LABEL,
  MAX_LABELS,
  NAME_PATTERN,
} from "./constants";

/** id сообщения — символов. */
const MAX_ID = 64;

/** Строка не длиннее max символов (а не единиц UTF-16). */
const chars = (max: number) =>
  z.string().refine(s => [...s].length <= max, `длиннее ${max} символов`);

/** Строка по правилу pattern. */
const matching = (pattern: RegExp) =>
  z.string().regex(pattern, {
    error: iss => `${JSON.stringify(iss.input)} не по правилу ${pattern}`,
  });

/** Объект JSON с любыми полями. */
const jsonObject = z.record(z.string(), z.unknown());

/** Имя воркера и ключа настроек. */
export const nameSchema = matching(NAME_PATTERN);
/** Тип события. */
export const eventTypeSchema = matching(EVENT_TYPE_PATTERN);
/** id сообщения: непустая строка. */
export const messageIdSchema = z.string().min(1).max(MAX_ID);
/** seq потока; нет или не целое больше 0 — 0. */
export const seqSchema = z.number().int().positive().catch(0);
/** Тип сообщения. */
export const messageTypeSchema = z.string();
/** Сообщение — объект JSON. */
export const envelopeSchema = z.looseObject({});

export const logLevelSchema = z.enum(["debug", "info", "warn", "error"]);

/** Метки: объект строк в пределах MAX_LABELS и MAX_LABEL. */
const labelMapSchema = z
  .record(
    chars(MAX_LABEL).min(1, "ключ метки — непустая строка"),
    chars(MAX_LABEL),
  )
  .refine(
    v => Object.keys(v).length <= MAX_LABELS,
    `меток больше ${MAX_LABELS}`,
  );

/** Метки; нет — пустой объект. */
export const labelsSchema = labelMapSchema.nullish().transform(v => v ?? {});

/** Ошибка `{ code, message }`. */
export const errorInfoSchema = z.object({
  code: z.string(),
  message: z.string().default(""),
});

/** Ошибка из ответа агента: нет кода — code, нет текста — пусто. */
const errorOr = (code: string) =>
  z.object({ code: z.string().catch(code), message: z.string().catch("") });

/** Ошибка из ответа агента; её нет — ошибка с кодом code. */
const errorAlways = (code: string) =>
  errorOr(code).catch({ code, message: "" });

export const hostInfoSchema = z.looseObject({
  os: z.string(),
  arch: z.string(),
  hostname: z.string(),
  kernel: z.string().optional(),
});

/** Массив, из которого неверные элементы выброшены. */
const validItems = <T extends z.ZodType>(item: T) =>
  z.array(z.unknown()).transform(list =>
    list.flatMap(v => {
      const r = item.safeParse(v);

      return r.success ? [r.data as z.output<T>] : [];
    }),
  );

/** Список манифеста; неверные элементы пропускаются, неверный список — нет списка. */
const manifestItems = <T extends z.ZodType>(item: T) =>
  validItems(item).optional().catch(undefined);

const description = z.string().optional().catch(undefined);

/** Объект без полей undefined (их оставляет пропуск неверного значения). */
const compact = <T extends object>(o: T): T =>
  Object.fromEntries(Object.entries(o).filter(([, v]) => v !== undefined)) as T;

/** Ключ настроек в манифесте воркера: schema — JSON Schema значения. */
export const manifestConfigSchema = z
  .object({
    key: nameSchema,
    description,
    schema: jsonObject.optional().catch(undefined),
  })
  .transform(compact);

/** JSON Schema из манифеста; не объект — нет схемы. */
const manifestSchemaField = jsonObject.optional().catch(undefined);

/**
 * Маршрут воркера в манифесте: {name} в path — один сегмент пути; request — JSON Schema тела
 * запроса, response — тела ответа 2xx (описание).
 */
export const manifestRouteSchema = z
  .object({
    method: z.string().regex(/^[A-Z]{1,16}$/),
    path: z.string().startsWith("/"),
    description,
    request: manifestSchemaField,
    response: manifestSchemaField,
  })
  .transform(compact);

/** Тип события воркера в манифесте; типы job.* зарезервированы для событий задач. */
export const manifestEventSchema = z
  .object({
    type: eventTypeSchema.refine(
      v => !v.startsWith(JOB_EVENT_PREFIX),
      "типы job.* зарезервированы для событий задач",
    ),
    description,
    /** JSON Schema поля data. */
    schema: manifestSchemaField,
  })
  .transform(compact);

/** Тип задачи воркера в манифесте (§12): schema — JSON Schema поля data. */
export const manifestJobSchema = z
  .object({
    type: eventTypeSchema,
    description,
    schema: manifestSchemaField,
  })
  .transform(compact);

/**
 * Тип запроса воркера к серверу в манифесте (§12): schema — JSON Schema поля data запроса,
 * response — data ответа (описание).
 */
export const manifestRequestSchema = z
  .object({
    type: eventTypeSchema,
    description,
    schema: manifestSchemaField,
    response: manifestSchemaField,
  })
  .transform(compact);

/**
 * Манифест воркера — ответ `GET /manifest` (§12). Воркер обязан указать `version`, но SDK
 * принимает манифест и без неё: неверные и незнакомые поля пропускаются.
 */
export const workerManifestSchema = z
  .object({
    version: z.string().optional().catch(undefined),
    description,
    configs: manifestItems(manifestConfigSchema),
    routes: manifestItems(manifestRouteSchema),
    events: manifestItems(manifestEventSchema),
    jobs: manifestItems(manifestJobSchema),
    requests: manifestItems(manifestRequestSchema),
  })
  .transform(compact);

/** Манифест в hello и status; неверный — нет манифеста. */
const manifestField = workerManifestSchema.optional().catch(undefined);

/** Воркер из `hello.workers`. */
export const helloWorkerSchema = z.object({
  name: z.string(),
  /** Версия сборки у воркера со сборкой с сервера, у остальных — из манифеста. */
  version: z.string().optional(),
  release: z.boolean().optional(),
  /** Манифест воркера (§12); нет — воркер себя не описывает. */
  manifest: manifestField,
});

/** `hello` (§4). */
export const helloSchema = z.looseObject({
  agent: z.looseObject({
    version: z.string(),
    bootId: z.string().min(1),
    startedAt: z.number(),
  }),
  host: hostInfoSchema,
  /** Метки агента; неверные пропускаются. */
  labels: labelMapSchema.optional().catch(undefined),
  workers: z.array(helloWorkerSchema).optional(),
  /** Версии настроек на диске агента: воркер → ключ → версия. */
  configs: z
    .record(z.string(), z.record(z.string(), z.number().int()))
    .optional(),
});

/** Ответ воркера на `GET /health` (§9). */
export const healthSchema = z.object({
  ok: z.boolean(),
  /** Воркер занят долгой работой: плановая замена ждёт (§13). */
  busy: z.boolean().optional(),
  message: z.string().optional(),
  /** Сведения воркера для сервера; не объект — пропускаются, `status` принимается. */
  info: jsonObject.optional().catch(undefined),
});

/** Итог применения ключа настроек у агента. */
export const configReportSchema = z.object({
  version: z.number().int(),
  /** Нет — ещё применяется. */
  ok: z.boolean().optional(),
  error: errorInfoSchema.optional(),
});

type WorkerState =
  "starting" | "running" | "invalid" | "backoff" | "stopped" | (string & {});

/** Состояние воркера в `status` (§6). */
export const workerStatusSchema = z.looseObject({
  name: z.string(),
  state: z.string() as z.ZodType<WorkerState>,
  /** Причина состояния `invalid`: чем ответ `GET /health` или `GET /manifest` не подошёл. */
  message: z.string().optional().catch(undefined),
  version: z.string().optional(),
  release: z.boolean().optional(),
  builtin: z.boolean().optional(),
  restarts: z.number().optional(),
  health: healthSchema.optional(),
  /** Замена ждёт, пока воркер занят: `restart` или `update` (§13). */
  pending: z.enum(["restart", "update"]).optional(),
  configs: z.record(z.string(), configReportSchema).optional(),
  /** Манифест воркера (§12); нет — воркер себя не описывает. */
  manifest: manifestField,
});

/** `status` (§6). */
export const statusSchema = z.looseObject({
  workers: z.array(workerStatusSchema),
  /** Сколько важных сообщений ждут `ack`. */
  outbox: z.number().optional(),
});

/**
 * `metrics` (§9): `host` — метрики узла, `workers` — ответы `GET /metrics` воркеров. Нет
 * времени сбора — время приёма; неверные `host` и `workers` пропускаются.
 */
export const metricsSchema = z.object({
  collectedAt: z.number().catch(() => Date.now()),
  host: jsonObject.optional().catch(undefined),
  workers: jsonObject.optional().catch(undefined),
});

/** Запись журнала (§9). */
export const logEntrySchema = z.object({
  at: z.number(),
  level: logLevelSchema,
  /** `agent` или имя воркера. */
  source: z.string(),
  msg: z.string(),
  attrs: jsonObject.optional(),
});

/** `log` (§9). */
export const logSchema = z.object({ entries: z.array(logEntrySchema) });

/** `event` (§8); неверное время пропускается. */
export const eventSchema = z.object({
  worker: nameSchema,
  type: eventTypeSchema,
  at: z.number().optional().catch(undefined),
  data: z.unknown().optional(),
});

/** `request` (§12): запрос воркера к серверу; неверный срок — по умолчанию. */
export const workerRequestSchema = z.object({
  worker: nameSchema,
  type: eventTypeSchema,
  data: z.unknown().optional(),
  timeoutMs: z.number().int().positive().optional().catch(undefined),
});

/** `config.applied` (§5). */
export const configAppliedSchema = z.object({
  worker: z.string().min(1),
  key: nameSchema,
  version: z.number().int(),
  ok: z.boolean(),
  /** Подробный итог применения — тело ответа воркера 2xx на PUT /config (§8); только при ok. */
  result: z.unknown().optional(),
  error: errorInfoSchema.optional(),
});

/** `action.result` (§10) — сообщение целиком: re — id действия. */
export const actionResultSchema = z
  .object({
    re: z.string(),
    data: z.object({
      ok: z.boolean(),
      result: z.unknown().optional(),
      error: errorAlways("ACTION_FAILED"),
    }),
  })
  .transform(({ re, data }) => ({ re, ...data }));

/** `action.done` (§10) — сообщение целиком: re — id действия, итог отложенной замены воркера. */
export const actionDoneSchema = z
  .object({
    re: z.string(),
    data: z.object({
      name: z.string(),
      worker: z.string().catch(""),
      ok: z.boolean(),
      result: z.unknown().optional(),
      error: errorAlways("ACTION_FAILED"),
    }),
  })
  .transform(({ re, data }) => ({ re, ...data }));

/** Итог worker.restart и worker.update, когда замена отложена: воркер занят. */
export const deferredResultSchema = z.object({
  deferred: z.literal(true),
  pending: z.enum(["restart", "update"]),
});

/** Состояние задачи воркера (§12). */
export const jobStateSchema = z.enum([
  "running",
  "done",
  "failed",
  "cancelled",
]);

/** Ответ воркера на `POST /jobs`: итог быстрой задачи (200) или id долгой (202). */
export const jobReplySchema = z.object({
  id: z.string().min(1).optional(),
  result: z.unknown().optional(),
});

/** Ответ воркера на `GET /jobs/{id}` и `POST /jobs/{id}/cancel`. */
export const jobStatusSchema = z
  .looseObject({
    id: z.string(),
    state: jobStateSchema,
    progress: z.number().optional().catch(undefined),
    result: z.unknown().optional(),
    error: errorInfoSchema.optional().catch(undefined),
  })
  .transform(compact);

/** data событий задач (job.progress, job.done, job.failed, job.cancelled). */
export const jobEventSchema = z.looseObject({
  jobId: z.string().min(1),
  id: z.string().optional().catch(undefined),
  progress: z.number().optional().catch(undefined),
  message: z.string().optional().catch(undefined),
  result: z.unknown().optional(),
  error: errorOr("JOB_FAILED").optional().catch(undefined),
});

/** Итог agent.rotateKey: хеш нового секрета (64 шестнадцатеричных символа). */
export const rotateKeyResultSchema = z.object({
  secretHash: z.string().regex(/^[0-9a-f]{64}$/),
});

/** Ответы агента на fetch (§7) по типу сообщения; неверные заголовок и итог — ошибка запроса. */
export const fetchReplySchemas = {
  "fetch.head": z
    .object({
      status: z.coerce.number().catch(0),
      headers: z.record(z.string(), z.coerce.string()).catch({}),
    })
    .catch({ status: 0, headers: {} })
    .transform(d => ({ kind: "head" as const, ...d })),
  "fetch.chunk": z
    .object({ data: z.string(), encoding: z.string().optional() })
    .transform(d => ({
      kind: "chunk" as const,
      data: Buffer.from(d.data, d.encoding === "base64" ? "base64" : "utf8"),
    })),
  "fetch.end": z
    .object({
      error: errorOr("WORKER_UNAVAILABLE").optional().catch(undefined),
    })
    .catch({})
    .transform(d => ({ kind: "end" as const, error: d.error })),
};

/** Тело регистрации (§2); неверное host пропускается. */
export const enrollSchema = z.object({
  token: z.string().min(1),
  name: chars(MAX_AGENT_NAME).min(1),
  labels: labelsSchema,
  host: z
    .looseObject({
      os: z.string().optional(),
      arch: z.string().optional(),
      hostname: z.string().optional(),
    })
    .catch({}),
});

/** Сборка агента в manifest.json. */
export const releaseArtifactSchema = z.object({
  os: z.string(),
  arch: z.string(),
  file: z.string(),
  sha256: z.string(),
  signature: z.string().optional(),
});

/** Сборка воркера в manifest.json. */
export const workerArtifactSchema = releaseArtifactSchema.extend({
  name: z.string(),
  version: z.string(),
  stopTimeout: z.string().optional(),
  /** Сборка-архив: что в нём запускать (путь внутри архива, например bin/report). */
  command: z.string().optional(),
});

/** manifest.json сборок (§11): неверные сборки пропускаются; artifacts пуст — только сборки воркеров. */
export const releaseManifestSchema = z.object({
  version: z.string(),
  /** Открытый ключ, которым подписаны сборки (base64), — справочно. */
  publicKey: z.string().optional().catch(undefined),
  artifacts: validItems(releaseArtifactSchema).catch([]),
  workers: validItems(workerArtifactSchema).optional().catch(undefined),
});

/** Релиз GitHub (ответ `GET /repos/{owner}/{repo}/releases`): нужное SDK; неверные файлы пропускаются. */
export const githubReleaseSchema = z.looseObject({
  tag_name: z.string(),
  draft: z.boolean().catch(false),
  prerelease: z.boolean().catch(false),
  assets: validItems(
    z.looseObject({ name: z.string(), browser_download_url: z.string() }),
  ).catch([]),
});

/** Список релизов GitHub; неверные записи пропускаются. */
export const githubReleasesSchema = validItems(githubReleaseSchema);
