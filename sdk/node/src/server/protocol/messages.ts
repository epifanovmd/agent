// Сообщения связи сервера и агента: постоянные и типы. Нормативный документ — sdk/spec/README.md.
// Типы входящих сообщений выведены из схем (schemas.ts): тип и проверка — одно описание.
// Незнакомые поля получатель пропускает, поэтому типы описывают только то, что нужно SDK.
import { randomBytes } from "node:crypto";

import type { z } from "zod";

import {
  configReportSchema,
  errorInfoSchema,
  healthSchema,
  helloSchema,
  helloWorkerSchema,
  hostInfoSchema,
  jobStateSchema,
  jobStatusSchema,
  logEntrySchema,
  logLevelSchema,
  manifestConfigSchema,
  manifestEventSchema,
  manifestJobSchema,
  manifestRouteSchema,
  metricsSchema,
  releaseArtifactSchema,
  releaseManifestSchema,
  statusSchema,
  workerArtifactSchema,
  workerManifestSchema,
  workerStatusSchema,
} from "./schemas";

export * from "./constants";

/** Конверт сообщения (§3). */
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- data задаётся типом сообщения
export interface Envelope<T = any> {
  type: string;
  id?: string;
  re?: string;
  seq?: number;
  data?: T;
}

/** Ошибка `{ code, message }`. */
export type ErrorInfo = z.output<typeof errorInfoSchema>;

export type HostInfo = z.output<typeof hostInfoSchema>;

/** Ключ настроек в манифесте воркера: schema — JSON Schema значения. */
export type ManifestConfig = z.output<typeof manifestConfigSchema>;

/** Маршрут воркера в манифесте: {name} в path — один сегмент пути. */
export type ManifestRoute = z.output<typeof manifestRouteSchema>;

/** Тип события воркера в манифесте. */
export type ManifestEvent = z.output<typeof manifestEventSchema>;

/** Тип задачи воркера в манифесте: schema — JSON Schema поля data. */
export type ManifestJob = z.output<typeof manifestJobSchema>;

/** Состояние задачи воркера (§12). */
export type JobState = z.output<typeof jobStateSchema>;

/** Ответ воркера на `GET /jobs/{id}` и `POST /jobs/{id}/cancel` (§12). */
export type JobStatus = z.output<typeof jobStatusSchema>;

/** Манифест воркера — ответ `GET /manifest` (§12). */
export type WorkerManifest = z.output<typeof workerManifestSchema>;

/** Воркер из `hello.workers`. */
export type HelloWorker = z.output<typeof helloWorkerSchema>;

/** `hello` (§4). */
export type Hello = z.output<typeof helloSchema>;

/** `welcome` (§4). */
export interface Welcome {
  serverTime: number;
  metricsIntervalMs: number;
  statusIntervalMs: number;
}

/** Ответ воркера на `GET /health` (§9). */
export type Health = z.output<typeof healthSchema>;

/** Итог применения ключа настроек у агента. */
export type ConfigReport = z.output<typeof configReportSchema>;

/** Состояние воркера в `status` (§6). */
export type WorkerStatus = z.output<typeof workerStatusSchema>;

/** `status` (§6). */
export type Status = z.output<typeof statusSchema>;

/** `metrics` (§9): `host` — метрики узла, `workers` — ответы `GET /metrics` воркеров. */
export type Metrics = z.output<typeof metricsSchema>;

export type LogLevel = z.output<typeof logLevelSchema>;
export const LOG_LEVELS: readonly LogLevel[] = logLevelSchema.options;

/** Запись журнала (§9). */
export type LogEntry = z.output<typeof logEntrySchema>;

/** `watch` (§9); `{}` — снять. */
export interface Watch {
  metricsIntervalMs?: number;
  logLevel?: LogLevel;
  untilMs?: number;
}

/** Сборка агента в manifest.json. */
export type ReleaseArtifact = z.output<typeof releaseArtifactSchema>;

/** Сборка воркера в manifest.json. */
export type WorkerArtifact = z.output<typeof workerArtifactSchema>;

/** manifest.json выпуска (§11). */
export type ReleaseManifest = z.output<typeof releaseManifestSchema>;

/** Идентификатор сообщения: 32 шестнадцатеричных символа. */
export const newId = (): string => randomBytes(16).toString("hex");
