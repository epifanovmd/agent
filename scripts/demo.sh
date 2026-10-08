#!/usr/bin/env bash
# Пример целиком на этой машине: сервер на Node.js + веб-интерфейс, агент с
# воркерами (Python, Go, Node.js). Go — в контейнере (scripts/go.sh), Node ≥ 24 и
# python3 — на машине.
#   scripts/demo.sh build        # агент и Go-воркер под эту машину (.dev/bin), выпуск агента для панели
#                                # обновлений (dist/<VERSION>: сборки, manifest.json, install.sh — scripts/release.sh),
#                                # SDK agent-sdk (sdk/node), npm-зависимости, сборка интерфейса
#   scripts/demo.sh server       # сервер и интерфейс: http://localhost:8080 (выпуск — RELEASES_DIR=dist/<VERSION>)
#   scripts/demo.sh agent [N]    # агент N (по умолчанию 1); второй терминал — второй агент
#   scripts/demo.sh check        # самопроверка воркеров без браузера
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VERSION="$(cat "$ROOT/VERSION")"
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$(uname -m)" in arm64 | aarch64) ARCH=arm64 ;; *) ARCH=amd64 ;; esac
BIN="$ROOT/.dev/bin"
RELEASE="$ROOT/dist/$VERSION"
PORT="${PORT:-8080}"
TOKEN="${DEMO_TOKEN:-demo-token}"

case "${1:-}" in
  build)
    # Агент и sysinfo для запуска на этой машине — отдельно от каталога выпуска.
    BUILD_DIR=.dev/bin "$ROOT/scripts/go.sh" build "$OS" "$ARCH" agent
    BUILD_DIR=.dev/bin "$ROOT/scripts/go.sh" build "$OS" "$ARCH" sysinfo ./examples/workers/sysinfo
    # Выпуск агента: сборки linux/darwin × amd64/arm64, manifest.json (подпись — AGENT_SIGNING_KEY), install.sh.
    "$ROOT/scripts/go.sh" release
    # agent-sdk собирается в dist: сервер и Node-воркер подключают его как file:-зависимость.
    (cd "$ROOT/sdk/node" && npm ci --no-audit --no-fund && npm run build)
    (cd "$ROOT/examples/server" && npm ci --no-audit --no-fund)
    (cd "$ROOT/examples/workers/node" && npm ci --no-audit --no-fund)
    (cd "$ROOT/examples/web" && npm ci --no-audit --no-fund && npm run build) ;;
  server)
    cd "$ROOT/examples/server"
    # Выпуск агента — из dist/<VERSION> (scripts/demo.sh build); PUBLIC_KEY — ключ проверки релизов.
    RELEASES_DIR="${RELEASES_DIR:-$RELEASE}" PORT="$PORT" ENROLL_TOKEN="$TOKEN" exec node --import tsx src/main.ts ;;
  agent)
    mkdir -p "$ROOT/.dev"
    export DEMO_ROOT="$ROOT" DEMO_AGENT="${2:-1}" SYSINFO_BIN="$BIN/sysinfo-$OS-$ARCH"
    export AGENT_SERVER_URL="${AGENT_SERVER_URL:-http://localhost:$PORT}" AGENT_ENROLL_TOKEN="$TOKEN"
    # Воркеры наследуют окружение агента: Python-воркерам виден agent_sdk.
    export PYTHONPATH="$ROOT/sdk/python${PYTHONPATH:+:$PYTHONPATH}"
    exec "$BIN/agent-$OS-$ARCH" run -config "$ROOT/examples/agent.demo.yaml" ;;
  check)
    cd "$ROOT/examples/web"
    DEMO_SERVER="${DEMO_SERVER:-http://localhost:$PORT}" exec node --import tsx selfcheck.cli.ts ;;
  *) echo "использование: $0 build | server | agent [N] | check" >&2; exit 2 ;;
esac
