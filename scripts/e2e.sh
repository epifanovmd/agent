#!/usr/bin/env bash
# Сквозные тесты стенда (examples/server/e2e): сервер стенда на agent-sdk/server в процессе
# теста, настоящий агент с воркерами-примерами (echo — Python, node-echo — Node.js, sysinfo и
# netprobe — Go). Go — на машине или в контейнере (scripts/go.sh); нужны Node ≥ 24, python3 и
# собранный sdk/node (make node-sdk).
#   scripts/e2e.sh          # собрать и прогнать
#   scripts/e2e.sh build    # только собрать: агент (VERSION), агент следующей версии (для
#                           # проверки обновления), agent-release, sysinfo, netprobe → .dev/e2e
#   scripts/e2e.sh test     # только прогнать (сборки уже есть)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
VERSION="$(cat VERSION)"
# Следующая версия: patch + 1 (1.2.0 → 1.2.1; у 1.2.0-rc.1 — тоже 1.2.1).
NEXT="$(echo "$VERSION" | awk -F'[.-]' '{ printf "%d.%d.%d", $1, $2, $3 + 1 }')"
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$(uname -m)" in arm64 | aarch64) ARCH=arm64 ;; *) ARCH=amd64 ;; esac
OUT=.dev/e2e
P="$OS-$ARCH"

build() {
  # Одной командой: на машине с Go — сразу, без Go — одним запуском контейнера.
  local script="set -e
b() { GOOS=$OS GOARCH=$ARCH CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags \"-s -w -X main.version=\$1\" -o \"\$2\" \"\$3\"; }
b $VERSION $OUT/agent-$P ./cmd/agent
b $NEXT $OUT/next/agent-$P ./cmd/agent
b $VERSION $OUT/agent-release-$P ./cmd/agent-release
b $VERSION $OUT/sysinfo-$P ./examples/workers/sysinfo
b $VERSION $OUT/netprobe-$P ./examples/workers/netprobe"
  mkdir -p "$OUT/next"
  if command -v go >/dev/null; then sh -c "$script"; else scripts/go.sh sh -c "$script"; fi
  echo "$NEXT" >"$OUT/next/VERSION"
  echo "сборки для e2e: $OUT (агент $VERSION, следующая версия $NEXT)"
}

e2e() {
  [ -f sdk/node/dist/server/index.js ] || { echo "нет сборки sdk/node: make node-sdk" >&2; exit 1; }
  cd examples/server
  npm ci --no-audit --no-fund --silent
  E2E_BIN="$ROOT/$OUT" npm run e2e
}

case "${1:-all}" in
  build) build ;;
  test) e2e ;;
  all) build && e2e ;;
  *) echo "использование: $0 [build | test]" >&2; exit 2 ;;
esac
