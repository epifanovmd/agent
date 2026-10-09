// Проверка входящих данных в одном месте: схемы zod (schemas.ts) и понятные причины отказа по-русски.
// Результат — значение или причина отказа; ошибку API (MESSAGE_INVALID) строит вызывающий.
import type { z } from "zod";

import type { Envelope, Hello, LogEntry, Status } from "./messages";
import {
  actionDoneSchema,
  actionResultSchema,
  configAppliedSchema,
  deferredResultSchema,
  enrollSchema,
  eventSchema,
  eventTypeSchema,
  fetchReplySchemas,
  githubReleasesSchema,
  helloSchema,
  jobEventSchema,
  jobReplySchema,
  jobStatusSchema,
  labelsSchema,
  logSchema,
  messageIdSchema,
  metricsSchema,
  nameSchema,
  releaseManifestSchema,
  rotateKeyResultSchema,
  statusSchema,
  workerRequestSchema,
} from "./schemas";

/** Значение или причина отказа. */
export type Parsed<T> = { ok: true; value: T } | { ok: false; error: string };

const ok = <T>(value: T): Parsed<T> => ({ ok: true, value });

/** Название ожидаемого типа. */
const TYPE_NAMES: Record<string, string> = {
  string: "строка",
  number: "число",
  int: "целое число",
  boolean: "true или false",
  object: "объект",
  record: "объект",
  array: "массив",
};

const sizeOf = (origin: string, n: number | bigint) =>
  origin === "string"
    ? `${n} символов`
    : origin === "array" || origin === "set"
      ? `${n} элементов`
      : `${n}`;

/** Текст ошибки по-русски для проверок без своего текста. */
const russian: z.core.$ZodErrorMap = iss => {
  switch (iss.code) {
    case "invalid_type":
      return iss.input === undefined
        ? "нет значения"
        : `ожидается ${TYPE_NAMES[iss.expected] ?? iss.expected}`;
    case "too_small":
      if (iss.origin === "string" && Number(iss.minimum) === 1) {
        return "пустая строка";
      }

      return iss.inclusive
        ? `нужно не меньше ${sizeOf(iss.origin, iss.minimum)}`
        : `нужно больше ${sizeOf(iss.origin, iss.minimum)}`;
    case "too_big":
      return iss.inclusive
        ? `нужно не больше ${sizeOf(iss.origin, iss.maximum)}`
        : `нужно меньше ${sizeOf(iss.origin, iss.maximum)}`;
    case "invalid_format":
      return iss.format === "regex" && iss.pattern
        ? `${JSON.stringify(iss.input)} не по правилу ${iss.pattern}`
        : `${JSON.stringify(iss.input)}: неверный формат`;
    case "invalid_value":
      return `ожидается ${iss.values.map(v => JSON.stringify(v)).join(" | ")}`;
    case "invalid_key":
      return `ключ ${JSON.stringify(iss.input)}: ${iss.issues[0]?.message ?? "неверный"}`;
    case "invalid_element":
      return `элемент ${JSON.stringify(iss.input)}: ${iss.issues[0]?.message ?? "неверный"}`;
    default:
      return "неверное значение";
  }
};

/** Путь к полю: workers[0].state. */
const pathText = (path: readonly PropertyKey[]) =>
  path.reduce<string>(
    (s, p) =>
      typeof p === "number"
        ? `${s}[${p}]`
        : s
          ? `${s}.${String(p)}`
          : String(p),
    "",
  );

/** Причина отказа: где и что не так (не больше трёх замечаний). */
const describe = (error: z.ZodError, what: string): string =>
  error.issues
    .slice(0, 3)
    .map(i => {
      const path = pathText(i.path);
      const where = what && path ? `${what}.${path}` : what || path;

      return where ? `${where}: ${i.message}` : i.message;
    })
    .join("; ");

/** Проверить значение схемой; what — что проверяется (начало причины отказа). */
export const parse = <S extends z.ZodType>(
  schema: S,
  value: unknown,
  what = "",
): Parsed<z.output<S>> => {
  const r = schema.safeParse(value, { error: russian });

  return r.success ? ok(r.data) : { ok: false, error: describe(r.error, what) };
};

export const validName = (v: unknown): v is string =>
  nameSchema.safeParse(v).success;
export const validEventType = (v: unknown): v is string =>
  eventTypeSchema.safeParse(v).success;

/** Имя воркера или ключа настроек по правилу имён. */
export const parseName = (field: string, v: unknown): Parsed<string> =>
  parse(nameSchema, v, field);

/** Метки: объект строк в пределах MAX_LABELS и MAX_LABEL; нет — пустой объект. */
export const parseLabels = (v: unknown): Parsed<Record<string, string>> =>
  parse(labelsSchema, v, "labels");

/** Тело регистрации (§2). */
export type EnrollRequest = z.output<typeof enrollSchema>;

export const parseEnroll = (body: unknown): Parsed<EnrollRequest> =>
  parse(enrollSchema, body);

/** id важного сообщения. */
export const parseMessageId = (type: string, v: unknown): Parsed<string> =>
  parse(messageIdSchema, v, `${type}: id`);

/** hello (§4). */
export const parseHello = (data: unknown): Parsed<Hello> =>
  parse(helloSchema, data, "hello");

/** status (§6). */
export const parseStatus = (data: unknown): Parsed<Status> =>
  parse(statusSchema, data, "status");

/** metrics (§9) с временем точки at — временем сбора. */
export const parseMetrics = (
  data: unknown,
): Parsed<z.output<typeof metricsSchema> & { at: number }> => {
  const m = parse(metricsSchema, data, "metrics");

  return m.ok ? ok({ ...m.value, at: m.value.collectedAt }) : m;
};

/** log (§9). */
export const parseLog = (data: unknown): Parsed<LogEntry[]> => {
  const p = parse(logSchema, data, "log");

  return p.ok ? ok(p.value.entries) : p;
};

/** event (§8). */
export type EventData = z.output<typeof eventSchema>;

export const parseEvent = (data: unknown): Parsed<EventData> =>
  parse(eventSchema, data, "event");

/** request (§12): запрос воркера к серверу. */
export type WorkerRequestData = z.output<typeof workerRequestSchema>;

export const parseWorkerRequest = (data: unknown): Parsed<WorkerRequestData> =>
  parse(workerRequestSchema, data, "request");

/** config.applied (§5). */
export type ConfigApplied = z.output<typeof configAppliedSchema>;

export const parseConfigApplied = (data: unknown): Parsed<ConfigApplied> =>
  parse(configAppliedSchema, data, "config.applied");

/** action.result (§10): re — id действия. */
export type ActionResult = z.output<typeof actionResultSchema>;

export const parseActionResult = (env: Envelope): Parsed<ActionResult> =>
  parse(actionResultSchema, env, "action.result");

/** action.done (§10): re — id действия, итог отложенной замены воркера. */
export type ActionDone = z.output<typeof actionDoneSchema>;

export const parseActionDone = (env: Envelope): Parsed<ActionDone> =>
  parse(actionDoneSchema, env, "action.done");

/** Итог замены, отложенной до окончания работы воркера; иначе — undefined. */
export const parseDeferred = (
  result: unknown,
): z.output<typeof deferredResultSchema> | undefined =>
  deferredResultSchema.safeParse(result).data;

/** data события задачи (§12). */
export type JobEventData = z.output<typeof jobEventSchema>;

export const parseJobEvent = (data: unknown): JobEventData | undefined =>
  jobEventSchema.safeParse(data).data;

/** Ответ воркера на POST /jobs. */
export const parseJobReply = (
  body: unknown,
): Parsed<z.output<typeof jobReplySchema>> =>
  parse(jobReplySchema, body, "ответ POST /jobs");

/** Ответ воркера на GET /jobs/{id} и POST /jobs/{id}/cancel. */
export const parseJobStatus = (
  body: unknown,
): Parsed<z.output<typeof jobStatusSchema>> =>
  parse(jobStatusSchema, body, "состояние задачи");

/** Хеш нового секрета в итоге agent.rotateKey (64 шестнадцатеричных символа). */
export const parseSecretHash = (result: unknown): string | undefined =>
  rotateKeyResultSchema.safeParse(result).data?.secretHash;

/** Ответ агента на fetch (§7). */
export type FetchReply = z.output<
  (typeof fetchReplySchemas)[keyof typeof fetchReplySchemas]
>;

/** fetch.head, fetch.chunk, fetch.end; кусок без data — undefined. */
export const parseFetchReply = (env: Envelope): FetchReply | undefined => {
  const schema = fetchReplySchemas[env.type as keyof typeof fetchReplySchemas];

  return schema?.safeParse(env.data).data;
};

/** manifest.json выпуска (§11). */
export const parseManifest = (
  data: unknown,
): Parsed<z.output<typeof releaseManifestSchema>> =>
  parse(releaseManifestSchema, data, "manifest.json");

/** Список выпусков GitHub (удалённый источник выпуска агента). */
export const parseGithubReleases = (
  data: unknown,
): Parsed<z.output<typeof githubReleasesSchema>> =>
  parse(githubReleasesSchema, data, "выпуски GitHub");
