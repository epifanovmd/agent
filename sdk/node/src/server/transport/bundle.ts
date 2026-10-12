// Архивы папки агента проекта (agent pack) по HTTP: установщик bundle/install.sh с адресом
// сервера и архив под платформу узла из bundlesDir. Публично: секретов в архивах нет (файлы
// переменных — только в архивах `agent pack --with-env`, их не раздают).
import { createReadStream } from "node:fs";
import { readdir, stat } from "node:fs/promises";
import type { IncomingMessage, ServerResponse } from "node:http";
import { join } from "node:path";

import type { Settings } from "../core/options";
import { safeServer } from "../lib/shell";
import { BUNDLE_PATH } from "../protocol/messages";
import { baseUrl, sendJSON } from "./http";

/** Что нужно раздаче архивов. */
export interface BundleSource {
  readonly settings: Pick<
    Settings,
    "baseUrl" | "trustProxy" | "log" | "bundlesDir"
  >;
}

// Архивы под Linux — для узлов; под macOS — для разработки (agent run из архива).
const PLATFORM = /^(linux|darwin)-(amd64|arm64)\.tar\.gz$/;
const ARCHIVE =
  /^agent-[a-z0-9-]+-[0-9][^/]*-(linux|darwin)-(amd64|arm64)\.tar\.gz$/;

/**
 * Установщик из архива проекта: скачивает архив папки агента под машину и запускает из него
 * программу агента. Без флагов способа — `agent install` (служба systemd, нужен root);
 * `--rootless` — агент пользователя без root; `--run` — без установки: `agent run` здесь же на
 * переднем плане (контейнер, свой менеджер процессов); `--uninstall [--purge]` — удалить.
 */
export const bundleInstallScript = (server: string): string => `#!/bin/sh
# Агент проекта с ${server}: архив папки агента (agent pack) под эту машину.
#   curl -fsSL ${server}${BUNDLE_PATH}/install.sh | sudo sh -s -- --token <токен>   служба systemd
#   … | sh -s -- --rootless --token <токен>    агент пользователя без root (~/.agent…)
#   … | sh -s -- --run --token <токен>         без установки: агент здесь, на переднем плане (контейнер)
#   … | sudo sh -s -- --uninstall [--purge]    удалить (агента без root — без sudo)
# Остальные флаги передаются agent install (agent install -h). С --run — только --token,
# --token-file и --name; данные — AGENT_DATA_DIR (по умолчанию /var/lib/agent), агент себя не
# обновляет (новая версия — при следующем запуске).
set -eu
SERVER="${server}"
die() {
  echo "install.sh: $*" >&2
  exit 1
}
[ "$(uname -s)" = Linux ] || die "только Linux"
case "$(uname -m)" in
  x86_64 | amd64) ARCH=amd64 ;;
  aarch64 | arm64) ARCH=arm64 ;;
  *) die "процессор $(uname -m) не поддерживается" ;;
esac
MODE=install
n=$#
while [ "$n" -gt 0 ]; do
  a=$1
  shift
  n=$((n - 1))
  case "$a" in
    --run) MODE=run ;;
    --uninstall) MODE=uninstall ;;
    *) set -- "$@" "$a" ;;
  esac
done
DIR="$(mktemp -d)"
[ "$MODE" = run ] || trap 'rm -rf "$DIR"' EXIT
URL="$SERVER${BUNDLE_PATH}/linux-$ARCH.tar.gz"
if command -v curl >/dev/null 2>&1; then
  curl -fsSL "$URL" -o "$DIR/agent.tar.gz"
elif command -v wget >/dev/null 2>&1; then
  wget -qO "$DIR/agent.tar.gz" "$URL"
elif command -v python3 >/dev/null 2>&1; then
  python3 -c 'import sys, urllib.request; urllib.request.urlretrieve(sys.argv[1], sys.argv[2])' "$URL" "$DIR/agent.tar.gz"
else
  die "нужен curl, wget или python3"
fi
tar xzf "$DIR/agent.tar.gz" -C "$DIR"
case "$MODE" in
  uninstall) exec "$DIR/agent/agent" uninstall "$@" ;;
  install) exec "$DIR/agent/agent" install --server "$SERVER" "$@" ;;
esac
while [ $# -gt 0 ]; do
  case "$1" in
    --token | --token-file | --name) [ $# -ge 2 ] || die "$1: нужно значение" ;;
  esac
  case "$1" in
    --token) AGENT_ENROLL_TOKEN=$2; shift 2 ;;
    --token=*) AGENT_ENROLL_TOKEN=\${1#*=}; shift ;;
    --token-file) AGENT_ENROLL_TOKEN=$(tr -d ' \\t\\r\\n' <"$2"); shift 2 ;;
    --token-file=*) AGENT_ENROLL_TOKEN=$(tr -d ' \\t\\r\\n' <"\${1#*=}"); shift ;;
    --name) AGENT_NAME=$2; shift 2 ;;
    --name=*) AGENT_NAME=\${1#*=}; shift ;;
    *) die "--run: $1 — здесь только --token, --token-file и --name (остальное — в настройках архива)" ;;
  esac
done
CONFIG=$(sed -n 's/.*"config": *"\\([^"]*\\)".*/\\1/p' "$DIR/agent/bundle.json")
AGENT_SERVER_URL="$SERVER"
AGENT_DATA_DIR="\${AGENT_DATA_DIR:-/var/lib/agent}"
AGENT_UPDATE_MODE="\${AGENT_UPDATE_MODE:-external}"
export AGENT_SERVER_URL AGENT_DATA_DIR AGENT_UPDATE_MODE
[ -z "\${AGENT_ENROLL_TOKEN:-}" ] || export AGENT_ENROLL_TOKEN
[ -z "\${AGENT_NAME:-}" ] || export AGENT_NAME
mkdir -p "$AGENT_DATA_DIR"
exec "$DIR/agent/agent" run -config "$DIR/agent/\${CONFIG:-agent.yaml}"
`;

/** Новейший архив под платформу в каталоге архивов. */
const newestArchive = async (
  dir: string,
  os: string,
  arch: string,
): Promise<{ path: string; size: number } | null> => {
  let names: string[];

  try {
    names = await readdir(dir);
  } catch {
    return null;
  }
  let best: { path: string; size: number; mtime: number } | null = null;

  for (const name of names) {
    const m = ARCHIVE.exec(name);

    if (m?.[1] !== os || m[2] !== arch) continue;
    const path = join(dir, name);
    const st = await stat(path).catch(() => null);

    if (st && (!best || st.mtimeMs > best.mtime))
      best = { path, size: st.size, mtime: st.mtimeMs };
  }

  return best && { path: best.path, size: best.size };
};

const notFound = (res: ServerResponse, message: string): true => {
  sendJSON(res, 404, { code: "NOT_FOUND", message });

  return true;
};

/** GET и HEAD bundle/install.sh и bundle/<os>-<arch>.tar.gz; без bundlesDir или не наш путь — false. */
export const serveBundle = async (
  src: BundleSource,
  req: IncomingMessage,
  res: ServerResponse,
  path: string,
): Promise<boolean> => {
  const { bundlesDir, trustProxy } = src.settings;

  if (
    !bundlesDir ||
    (req.method !== "GET" && req.method !== "HEAD") ||
    !path.startsWith(BUNDLE_PATH + "/")
  )
    return false;
  const name = path.slice(BUNDLE_PATH.length + 1);

  if (name === "install.sh") {
    const server =
      src.settings.baseUrl?.replace(/\/$/, "") ?? baseUrl(req, trustProxy);

    if (!safeServer(server) || /["$`\\]/.test(server)) {
      sendJSON(res, 400, {
        code: "MESSAGE_INVALID",
        message: "Некорректный адрес сервера",
      });

      return true;
    }
    const script = bundleInstallScript(server);

    res.writeHead(200, {
      "Content-Type": "text/x-shellscript; charset=utf-8",
      "Content-Length": Buffer.byteLength(script),
    });
    res.end(req.method === "HEAD" ? undefined : script);

    return true;
  }
  const platform = PLATFORM.exec(name);
  const file =
    platform && (await newestArchive(bundlesDir, platform[1], platform[2]));

  if (!file)
    return notFound(res, `нет архива агента ${name} (agent pack, bundlesDir)`);
  res.writeHead(200, {
    "Content-Type": "application/gzip",
    "Content-Length": file.size,
    "Content-Disposition": `attachment; filename="${file.path.split("/").pop()}"`,
  });
  if (req.method === "HEAD") res.end();
  else createReadStream(file.path).pipe(res);

  return true;
};
