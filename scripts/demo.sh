#!/usr/bin/env bash
# Стенд целиком на этой машине: сервер на Node.js (agent-sdk/server и HTTP API над ним), агент
# с воркерами-примерами без SDK (echo — Python, node-echo — Node.js, sysinfo и netprobe — Go).
# Go — в контейнере (scripts/go.sh), Node ≥ 24 и python3 — на машине. Сквозные тесты того же
# стенда — scripts/e2e.sh.
#   scripts/demo.sh build        # агент, sysinfo и netprobe под эту машину (.dev/bin); выпуск агента
#                                # (dist/<VERSION>: сборки, manifest.json, install.sh); agent-sdk
#                                # (sdk/node), зависимости сервера
#   scripts/demo.sh server       # сервер: http://localhost:8080 (PORT — другой порт)
#   scripts/demo.sh agent [N]    # агент demo-N (по умолчанию 1); второй терминал — второй агент
#   scripts/demo.sh status [N]   # agent status агента N: связь, воркеры, последняя ошибка
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VERSION="$(cat "$ROOT/VERSION")"
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$(uname -m)" in arm64 | aarch64) ARCH=arm64 ;; *) ARCH=amd64 ;; esac
BIN="${DEMO_BIN:-$ROOT/.dev/bin}"
RELEASE="$ROOT/dist/$VERSION"
PORT="${PORT:-8080}"
TOKEN="${DEMO_TOKEN:-demo-token}"

case "${1:-}" in
  build)
    # Агент и Go-воркеры для запуска на этой машине — отдельно от каталога выпуска.
    BUILD_DIR=.dev/bin "$ROOT/scripts/go.sh" build "$OS" "$ARCH" agent
    BUILD_DIR=.dev/bin "$ROOT/scripts/go.sh" build "$OS" "$ARCH" sysinfo ./examples/workers/sysinfo
    BUILD_DIR=.dev/bin "$ROOT/scripts/go.sh" build "$OS" "$ARCH" netprobe ./examples/workers/netprobe
    # Выпуск агента: сборки linux/darwin × amd64/arm64, manifest.json (подпись — AGENT_SIGNING_KEY), install.sh.
    "$ROOT/scripts/go.sh" release
    # agent-sdk собирается в dist: сервер подключает его как file:-зависимость.
    (cd "$ROOT/sdk/node" && npm ci --no-audit --no-fund && npm run build)
    (cd "$ROOT/examples/server" && npm ci --no-audit --no-fund) ;;
  server)
    cd "$ROOT/examples/server"
    # Выпуск — из dist/<VERSION> (scripts/demo.sh build); PUBLIC_KEY — ключ проверки выпуска.
    RELEASES_DIR="${RELEASES_DIR:-$RELEASE}" PORT="$PORT" ENROLL_TOKEN="$TOKEN" exec node --import tsx src/main.ts ;;
  status)
    DEMO_ROOT="$ROOT" DEMO_AGENT="${2:-1}" exec "$BIN/agent-$OS-$ARCH" status -config "$ROOT/examples/agent.demo.yaml" ;;
  agent)
    mkdir -p "$ROOT/.dev"
    export DEMO_ROOT="$ROOT" DEMO_AGENT="${2:-1}" SYSINFO_BIN="$BIN/sysinfo-$OS-$ARCH" NETPROBE_BIN="$BIN/netprobe-$OS-$ARCH"
    export AGENT_SERVER_URL="${AGENT_SERVER_URL:-http://localhost:$PORT}" AGENT_ENROLL_TOKEN="$TOKEN"
    exec "$BIN/agent-$OS-$ARCH" run -config "$ROOT/examples/agent.demo.yaml" ;;
  *) echo "использование: $0 build | server | agent [N] | status [N]" >&2; exit 2 ;;
esac
