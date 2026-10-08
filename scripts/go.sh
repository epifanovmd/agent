#!/usr/bin/env bash
# Команды Go в контейнере golang — без установки Go на машину.
# Ветка Go — из go.mod (1.26.0 → образ 1.26) или GO_IMAGE.
#   scripts/go.sh test | race | vet | tidy | fmt | fmt-check
#   scripts/go.sh build [os] [arch] [cmd] [pkg]  # <BUILD_DIR>/<cmd>-<os>-<arch>; по умолчанию — эта машина,
#                                           # agent, ./cmd/<cmd>; BUILD_DIR (от корня) — по умолчанию dist/<VERSION>
#   scripts/go.sh release [--worker NAME=VERSION[,restart=…][,stopTimeout=…]]…
#                                           # scripts/release.sh dist/<VERSION> <VERSION> … (подпись — AGENT_SIGNING_KEY)
#   scripts/go.sh <любая команда>           # в контейнере с исходниками
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GO_VERSION=$(awk '$1 == "go" { split($2, v, "."); print v[1] "." v[2]; exit }' "$ROOT/go.mod")
IMAGE=${GO_IMAGE:-golang:$GO_VERSION-bookworm}
VERSION=${AGENT_VERSION:-$(cat "$ROOT/VERSION")}
BUILD_DIR=${BUILD_DIR:-dist/$VERSION}

run() {
  docker run --rm -v "$ROOT:/src" -w /src \
    -v agent-gomod:/go/pkg/mod -v agent-gocache:/root/.cache/go-build \
    -e CGO_ENABLED=0 ${AGENT_SIGNING_KEY:+-e AGENT_SIGNING_KEY} "$@"
}

host_os() { case "$(uname -s)" in Darwin) echo darwin ;; *) echo linux ;; esac; }
host_arch() { case "$(uname -m)" in arm64 | aarch64) echo arm64 ;; *) echo amd64 ;; esac; }

build() {
  local os=$1 arch=$2 cmd=$3 pkg=$4
  run -e GOOS="$os" -e GOARCH="$arch" "$IMAGE" \
    go build -trimpath -buildvcs=false -ldflags "-s -w -X main.version=$VERSION" \
    -o "$BUILD_DIR/$cmd-$os-$arch" "$pkg"
  echo "$BUILD_DIR/$cmd-$os-$arch"
}

case "${1:-test}" in
  test) run "$IMAGE" go test ./... ;;
  race) run -e CGO_ENABLED=1 "$IMAGE" go test -race ./... ;;
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
