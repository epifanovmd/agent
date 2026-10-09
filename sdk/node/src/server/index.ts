// agent-sdk/server — серверная часть: Agents (связь с агентами), Store, типы сообщений.
export { Actor, Agents } from "./agents";
export type {
  AgentsEvents,
  AlertEvent,
  ChangeEvent,
  LogEvent,
  MetricsEvent,
} from "./core/context";
export { AgentsError } from "./core/errors";
export {
  agentsDefaults,
  type AgentsOptions,
  type EnrollInfo,
  type EnrollVerdict,
  type RelayCall,
  type RelayFunction,
  type RelayMethod,
  type RelayRequest,
} from "./core/options";
export { Session } from "./core/session";
export type {
  ActionOptions,
  Deferred,
  LogsOptions,
  RestartResult,
  UpdateResult,
  WorkerActionOptions,
  WorkerUpdateResult,
} from "./features/actions";
export {
  installCommand,
  type InstallOptions,
  PACKAGE_MANAGERS,
  type PackageManager,
} from "./features/install";
export type { JobFiles, JobOptions, JobResult } from "./features/jobs";
export type {
  EventFilter,
  EventHandler,
  WaitEventOptions,
  WatchOptions,
  WatchRef,
} from "./features/observe";
export { RELAY_SECRET_HEADER } from "./features/relay";
export type { FetchInit } from "./features/tunnel";
export { shellQuote } from "./lib/shell";
export {
  capabilities,
  declaresEvent,
  findRoute,
  matchRoute,
  supports,
  type SupportsQuery,
  workerManifest,
} from "./model/manifest";
export { publicAgent } from "./model/public-agent";
export * from "./model/types";
export { validEventType, validName } from "./protocol/checks";
export * from "./protocol/messages";
export { MemoryStore } from "./store/memory";
export type { SetConfigOptions, Store } from "./store/store";
export {
  baseUrl,
  clientAddress,
  readBody,
  readJSON,
  sendJSON,
} from "./transport/http";
export { transportDefaults } from "./transport/transport";
