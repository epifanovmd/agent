#!/usr/bin/env bash
# Команды Go в контейнере golang — без установки Go на машину.
# Образ — golang:<ветка>-bookworm, ветка — из строки toolchain в go.mod (go1.26.0 → 1.26),
# без неё — из строки go; GO_IMAGE задаёт образ явно (проверка на младшей версии Go:
# GOTOOLCHAIN=local GO_IMAGE=golang:1.24-bookworm scripts/go.sh vet).
# Тесты (test, race) — в образе agent-go-test: golang + node (сервер интеграционных тестов на
# agent-sdk/server, нужен собранный sdk/node); образ собирается здесь же, повторная сборка — из
# кеша Docker. С GO_IMAGE тесты идут в нём.
# Контейнер работает от пользователя машины: файлы в dist/ — его, а не root. Кеши модулей
# и сборки — в томах agent-gomod и agent-gocache.
# AGENT_UPDATE_PUBLIC_KEY (build, release) — открытый ключ проверки обновлений, вшивается в агента.
#   scripts/go.sh test | race [аргументы go test, по умолчанию ./...] | vet | tidy | fmt | fmt-check
#   scripts/go.sh build [os] [arch] [cmd] [pkg]  # <BUILD_DIR>/<cmd>-<os>-<arch>; по умолчанию — эта машина,
#                                           # agent, ./cmd/<cmd>; BUILD_DIR (от корня) — по умолчанию dist/<VERSION>
#   scripts/go.sh release [--worker NAME=VERSION[,stopTimeout=…][,command=…]]…
#                                           # scripts/release.sh dist/<VERSION> <VERSION> … (подпись — AGENT_SIGNING_KEY)
#   scripts/go.sh <любая команда>           # в контейнере с исходниками
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GO_VERSION=$(awk '
  $1 == "toolchain" { sub(/^go/, "", $2); split($2, v, "."); print v[1] "." v[2]; found = 1; exit }
  $1 == "go" && !gover { split($2, v, "."); gover = v[1] "." v[2] }
  END { if (!found) print gover }' "$ROOT/go.mod")
IMAGE=${GO_IMAGE:-golang:$GO_VERSION-bookworm}
VERSION=${AGENT_VERSION:-$(cat "$ROOT/VERSION")}
BUILD_DIR=${BUILD_DIR:-dist/$VERSION}
OWNER="$(id -u):$(id -g)"

# Тома кешей создаются от root: один раз отдаём их пользователю машины.
docker run --rm -v agent-gomod:/gomod -v agent-gocache:/gocache "$IMAGE" sh -c \
  "[ \"\$(stat -c %u:%g /gomod /gocache | sort -u)\" = '$OWNER' ] || chown -R '$OWNER' /gomod /gocache"

# --init: процесс 1 контейнера забирает завершившихся потомков (тесты воркеров проверяют, что
# потомки воркера завершены).
run() {
  docker run --rm --init --user "$OWNER" -v "$ROOT:/src" -w /src \
    -v agent-gomod:/gomod -v agent-gocache:/gocache \
    -e HOME=/tmp -e GOMODCACHE=/gomod -e GOCACHE=/gocache -e GOTOOLCHAIN \
    -e CGO_ENABLED=0 ${AGENT_SIGNING_KEY:+-e AGENT_SIGNING_KEY} ${AGENT_UPDATE_PUBLIC_KEY:+-e AGENT_UPDATE_PUBLIC_KEY} "$@"
}

# test_image — образ для тестов: GO_IMAGE или golang с node.
test_image() {
  if [ -n "${GO_IMAGE:-}" ]; then echo "$GO_IMAGE"; return; fi
  local image="agent-go-test:$GO_VERSION"
  docker build -q -t "$image" - >/dev/null <<EOF
FROM node:24-bookworm-slim AS node
FROM golang:$GO_VERSION-bookworm
COPY --from=node /usr/local/bin/node /usr/local/bin/node
EOF
  echo "$image"
}

host_os() { case "$(uname -s)" in Darwin) echo darwin ;; *) echo linux ;; esac; }
host_arch() { case "$(uname -m)" in arm64 | aarch64) echo arm64 ;; *) echo amd64 ;; esac; }

build() {
  local os=$1 arch=$2 cmd=$3 pkg=$4
  run -e GOOS="$os" -e GOARCH="$arch" "$IMAGE" \
    go build -trimpath -buildvcs=false \
    -ldflags "-s -w -X main.version=$VERSION${AGENT_UPDATE_PUBLIC_KEY:+ -X main.updateKey=$AGENT_UPDATE_PUBLIC_KEY}" \
    -o "$BUILD_DIR/$cmd-$os-$arch" "$pkg"
  echo "$BUILD_DIR/$cmd-$os-$arch"
}

case "${1:-test}" in
  test) shift; run "$(test_image)" go test "${@:-./...}" ;;
  race) shift; run -e CGO_ENABLED=1 "$(test_image)" go test -race "${@:-./...}" ;;
  vet) run "$IMAGE" go vet ./... ;;
  tidy) run "$IMAGE" go mod tidy ;;
  fmt) run "$IMAGE" gofmt -l -w . ;;
  fmt-check)
    # Замечания gofmt — списком файлов и кодом выхода 1, без изменений (переменные — в контейнере).
    # shellcheck disable=SC2016
    run "$IMAGE" sh -c 'out=$(gofmt -l .); [ -z "$out" ] || { echo "gofmt: не отформатированы:"; echo "$out"; exit 1; }' ;;
  build)
    cmd=${4:-agent}
    build "${2:-$(host_os)}" "${3:-$(host_arch)}" "$cmd" "${5:-./cmd/$cmd}" ;;
  release) run "$IMAGE" scripts/release.sh "dist/$VERSION" "$VERSION" "${@:2}" ;;
  *) run "$IMAGE" "$@" ;;
esac
