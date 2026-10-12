// Команды установки агента на узел одной строкой: curl … install.sh | sudo sh -s -- флаги.
// installCommand — установщик сборок агента (install.sh: программа агента и флаги
// `agent install`); bundleInstallCommand — установщик из архива папки агента проекта
// (bundle/install.sh: настройки и воркеры — в архиве). Агент пользователя без root (rootless)
// и запуск без установки (run, например в контейнере) — без sudo.
import { z } from "zod";

import { invalid, valid } from "../core/errors";
import { safeServer, shellQuote } from "../lib/shell";
import { parse } from "../protocol/checks";
import { BUNDLE_PATH, INSTALL_PATH } from "../protocol/messages";
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
    /** Агент пользователя без root (--rootless): ~/.agent, служба systemd --user; команда — без sudo. */
    rootless: z.boolean().optional(),
    /**
     * Без установки (--run): агент запускается здесь же на переднем плане — в контейнере или
     * под своим менеджером процессов; данные — AGENT_DATA_DIR (по умолчанию /var/lib/agent).
     * Команда — без sudo; кроме токена, имени, caFile и releases — ничего.
     */
    run: z.boolean().optional(),
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
    /** Воркеры с сервера (--worker NAME, повторяемый); имя — по правилу имён. */
    workers: z.array(nameSchema).optional(),
    stopTimeout: z.string().optional(),
    user: z.string().optional(),
    /** Путь к agent.yaml на узле (--config). */
    config: z.string().optional(),
    /** Откуда скачивать сборки (--releases); по умолчанию — с этого сервера. */
    releases: serverSchema.or(z.literal("")).optional(),
  })
  .refine(
    o => !o.token !== !o.tokenFile,
    "нужен ровно один из token и tokenFile",
  )
  .refine(
    o =>
      !o.rootless ||
      (!o.user &&
        !o.sysctl &&
        !o.privileged &&
        !o.rwPaths?.length &&
        !o.packages?.length &&
        !o.packagesByManager),
    "rootless — без root: без user, sysctl, privileged, rwPaths и пакетов",
  )
  .refine(
    o =>
      !o.run ||
      (!o.rootless &&
        !o.instance &&
        !o.user &&
        !o.config &&
        !o.privileged &&
        !o.killMode &&
        !o.packages?.length &&
        !o.packagesByManager &&
        !o.sysctl &&
        !o.rwPaths?.length &&
        !o.workers?.length &&
        !o.stopTimeout),
    "run — без установки: только token или tokenFile, name, caFile и releases",
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
  const sudo = opts.rootless || opts.run ? "" : "sudo ";
  const parts = [
    `curl -fsSL ${shellQuote(base + INSTALL_PATH)} | ${sudo}sh -s --`,
  ];
  const flag = (name: string, value: string | undefined) => {
    if (value) parts.push(`${name} ${shellQuote(value)}`);
  };

  if (opts.rootless) parts.push("--rootless");
  if (opts.run) parts.push("--run");
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

/**
 * Адрес установщика из архива проекта (`<адрес>/api/v1/agent-link/bundle/install.sh`) — например,
 * чтобы скачать его по SSH и запустить с флагами `agent install` или `--uninstall`.
 */
export const bundleInstallUrl = (baseUrl: string): string =>
  `${baseUrl.replace(/\/+$/, "")}${BUNDLE_PATH}/install.sh`;

/** Параметры установки из архива проекта; пустые в команду не попадают. */
const bundleOptionsSchema = z
  .object({
    /** Адрес сервера; пусто — опция baseUrl Agents. */
    baseUrl: z.string().optional(),
    /** Токен регистрации; ровно одно из token и tokenFile. */
    token: z.string().optional(),
    /** Путь к файлу с токеном на узле (--token-file). */
    tokenFile: z.string().optional(),
    /** Имя агента (по умолчанию — имя машины). */
    name: z.string().optional(),
    /** Агент пользователя без root (--rootless): команда без sudo. */
    rootless: z.boolean().optional(),
    /** Без установки (--run): агент здесь же на переднем плане — контейнер; команда без sudo. */
    run: z.boolean().optional(),
  })
  .refine(
    o => !o.token !== !o.tokenFile,
    "нужен ровно один из token и tokenFile",
  )
  .refine(o => !(o.rootless && o.run), "rootless и run — что-то одно");

/** Параметры установки из архива проекта (bundleInstallCommand). */
export type BundleInstallOptions = z.input<typeof bundleOptionsSchema>;

/**
 * Команда установки из архива папки агента проекта (опция bundlesDir): `curl -fsSL
 * '<адрес>/api/v1/agent-link/bundle/install.sh' | sudo sh -s -- --token '…'`. Экземпляр,
 * воркеры, пакеты и права — в настройках архива; здесь — токен, имя и способ установки.
 */
export const bundleInstallCommand = (
  input: BundleInstallOptions,
  fallbackBase = "",
): string => {
  const opts = valid(parse(bundleOptionsSchema, input, "bundleInstallCommand"));
  const base = (opts.baseUrl || fallbackBase).replace(/\/+$/, "");

  if (!base) throw invalid("Нужен адрес сервера (baseUrl)");
  valid(parse(serverSchema, base, "baseUrl"));
  const sudo = opts.rootless || opts.run ? "" : "sudo ";
  const parts = [
    `curl -fsSL ${shellQuote(bundleInstallUrl(base))} | ${sudo}sh -s --`,
  ];

  if (opts.rootless) parts.push("--rootless");
  if (opts.run) parts.push("--run");
  parts.push(
    opts.tokenFile
      ? `--token-file ${shellQuote(opts.tokenFile)}`
      : `--token ${shellQuote(opts.token ?? "")}`,
  );
  if (opts.name) parts.push(`--name ${shellQuote(opts.name)}`);
  const cmd = parts.join(" ");

  if (/[\r\n]/.test(cmd)) throw invalid("Перевод строки в значении");

  return cmd;
};
