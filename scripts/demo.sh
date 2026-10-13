#!/usr/bin/env bash
# Стенд целиком на этой машине: сервер examples/server/server.py (Python, стандартная
# библиотека) и агент с воркерами-примерами echo (Python) и sysinfo (Go). Go — в контейнере
# (scripts/go.sh), python3 — на машине.
#   scripts/demo.sh build        # агент и sysinfo под эту машину (.dev/bin)
#   scripts/demo.sh server       # сервер: http://localhost:8080 (PORT — другой порт)
#   scripts/demo.sh agent [N]    # агент demo-N (по умолчанию 1); второй терминал — второй агент
#   scripts/demo.sh status [N]   # agent status агента N: связь, воркеры, последняя ошибка
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$(uname -m)" in arm64 | aarch64) ARCH=arm64 ;; *) ARCH=amd64 ;; esac
BIN="${DEMO_BIN:-$ROOT/.dev/bin}"
PORT="${PORT:-8080}"
TOKEN="${DEMO_TOKEN:-demo-token}"

case "${1:-}" in
  build)
    BUILD_DIR=.dev/bin "$ROOT/scripts/go.sh" build "$OS" "$ARCH" agent
    BUILD_DIR=.dev/bin "$ROOT/scripts/go.sh" build "$OS" "$ARCH" sysinfo ./examples/workers/sysinfo ;;
  server)
    cd "$ROOT"
    PORT="$PORT" ENROLL_TOKEN="$TOKEN" DATA_FILE="$ROOT/.dev/server.json" exec python3 examples/server/server.py ;;
  status)
    DEMO_ROOT="$ROOT" DEMO_AGENT="${2:-1}" exec "$BIN/agent-$OS-$ARCH" status -config "$ROOT/examples/agent.demo.yaml" ;;
  agent)
    mkdir -p "$ROOT/.dev"
    export DEMO_ROOT="$ROOT" DEMO_AGENT="${2:-1}" SYSINFO_BIN="$BIN/sysinfo-$OS-$ARCH"
    export AGENT_SERVER_URL="${AGENT_SERVER_URL:-http://localhost:$PORT}" AGENT_ENROLL_TOKEN="$TOKEN"
    exec "$BIN/agent-$OS-$ARCH" run -config "$ROOT/examples/agent.demo.yaml" ;;
  *) echo "использование: $0 build | server | agent [N] | status [N]" >&2; exit 2 ;;
esac
