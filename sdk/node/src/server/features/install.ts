// Команда установки агента на узел: curl … install.sh | sudo sh -s -- флаги.
// install.sh скачивает сборку агента под узел и передаёт флаги `agent install`.
import { z } from "zod";

import { invalid, valid } from "../core/errors";
import { safeServer, shellQuote } from "../lib/shell";
import { parse } from "../protocol/checks";
import { INSTALL_PATH } from "../protocol/messages";
import { nameSchema } from "../protocol/schemas";

/** Менеджеры пакетов `agent install` — в этом порядке флаги --packages-<m> в команде. */
export const PACKAGE_MANAGERS = ["apt", "dnf", "yum", "apk", "zypper"] as const;
export type PackageManager = (typeof PACKAGE_MANAGERS)[number];

/** Адрес для install.sh: http(s)://хост[/путь] без кавычек и пробелов. */
const serverSchema = z
  .string()
  .refine(v => safeServer(v) && !/['\s]/.test(v), "некорректный адрес");
const packageSchema = z.string().regex(/^[A-Za-z0-9.+_:-]+$/);
const packagesSchema = z.array(packageSchema).optional();

/** Флаги `agent install`; пустые в команду не попадают. */
const installOptionsSchema = z
  .object({
    /** Адрес сервера; пусто — опция baseUrl Agents. */
    baseUrl: z.string().optional(),
    /** Экземпляр агента на узле (--instance): несколько агентов для разных бэкендов; имя — по правилу имён. */
    instance: nameSchema.optional(),
    /** Токен регистрации; ровно одно из token и tokenFile. */
    token: z.string().optional(),
    /** Путь к файлу с токеном на узле (--token-file): токен не виден в списке процессов. */
    tokenFile: z.string().optional(),
    name: z.string().optional(),
    privileged: z.boolean().optional(),
    /** Как systemd останавливает службу: process (по умолчанию) — дочерние процессы воркеров переживают перезапуск агента, или mixed. */
    killMode: z.enum(["process", "mixed"]).optional(),
    packages: packagesSchema,
    /** Пакеты под менеджер (apt, dnf, yum, apk, zypper): для своего менеджера заменяют packages. */
    packagesByManager: z
      .partialRecord(z.enum(PACKAGE_MANAGERS), z.array(packageSchema))
      .optional(),
    /** Параметры ядра ключ → значение; в команде — по алфавиту ключей. */
    sysctl: z
      .record(z.string().regex(/^[A-Za-z0-9_][A-Za-z0-9_./-]*$/), z.string())
      .optional(),
    rwPaths: z.array(z.string()).optional(),
    /** Путь к сертификату CA на узле (--ca-file). */
    caFile: z.string().optional(),
    /** Воркеры из выпуска (--worker NAME, повторяемый); имя — по правилу имён. */
    workers: z.array(nameSchema).optional(),
    stopTimeout: z.string().optional(),
    user: z.string().optional(),
    /** Путь к agent.yaml на узле (--config). */
    config: z.string().optional(),
    /** Откуда скачивать сборки (--releases); по умолчанию — выпуск на сервере. */
    releases: serverSchema.or(z.literal("")).optional(),
  })
  .refine(
    o => !o.token !== !o.tokenFile,
    "нужен ровно один из token и tokenFile",
  );

/** Флаги `agent install`; пустые в команду не попадают. */
export type InstallOptions = z.input<typeof installOptionsSchema>;

/** Команда установки; fallbackBase — адрес, если в opts нет baseUrl. */
export const installCommand = (
  input: InstallOptions,
  fallbackBase = "",
): string => {
  const opts = valid(parse(installOptionsSchema, input, "installCommand"));
  const base = (opts.baseUrl || fallbackBase).replace(/\/+$/, "");

  if (!base) throw invalid("Нужен адрес сервера (baseUrl)");
  valid(parse(serverSchema, base, "baseUrl"));
  const parts = [
    `curl -fsSL ${shellQuote(base + INSTALL_PATH)} | sudo sh -s --`,
  ];
  const flag = (name: string, value: string | undefined) => {
    if (value) parts.push(`${name} ${shellQuote(value)}`);
  };

  flag("--instance", opts.instance);
  flag("--token", opts.token);
  flag("--token-file", opts.tokenFile);
  flag("--name", opts.name);
  flag("--user", opts.user);
  flag("--config", opts.config);
  if (opts.privileged) parts.push("--privileged");
  flag("--kill-mode", opts.killMode);
  const packages = (name: string, list: string[] | undefined) =>
    flag(name, (list ?? []).join(" "));

  packages("--packages", opts.packages);
  for (const m of PACKAGE_MANAGERS)
    packages(`--packages-${m}`, opts.packagesByManager?.[m]);
  const sysctl = opts.sysctl ?? {};
  const keys = Object.keys(sysctl).sort();

  for (const k of keys) flag("--sysctl", `${k}=${sysctl[k]}`);
  for (const p of opts.rwPaths ?? []) flag("--rw-path", p);
  flag("--ca-file", opts.caFile);
  for (const w of opts.workers ?? []) flag("--worker", w);
  flag("--stop-timeout", opts.stopTimeout);
  flag("--releases", opts.releases);
  const cmd = parts.join(" ");

  if (/[\r\n]/.test(cmd)) throw invalid("Перевод строки в значении");

  return cmd;
};
