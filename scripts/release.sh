#!/bin/sh
# Выпуск агента в каталог DIR: сборки linux/darwin × amd64/arm64 (agent-<os>-<arch>),
# manifest.json (подпись — AGENT_SIGNING_KEY, base64 seed Ed25519 из agent keygen;
# без него — без подписей, самообновление на такой выпуск не встанет) и install.sh.
# Нужен Go: на машине без него — scripts/go.sh release (контейнер golang).
#   scripts/release.sh DIR VERSION [--worker NAME=VERSION[,restart=…][,stopTimeout=…]]…
# --worker — сборки воркера, положенные в DIR заранее: один файл DIR/<name>-<version>-<os>-<arch>
# или архив DIR/<name>-<version>-<os>-<arch>.tar.gz.
# AGENT_UPDATE_PUBLIC_KEY (base64 открытого ключа из agent keygen) вшивается в сборки агента:
# они проверяют обновления без настройки update.publicKey.
# Раскладка выпуска — dist/<VERSION>/ (make release, CI, образ agent-dist).
set -eu
[ $# -ge 2 ] || { echo "использование: $0 DIR VERSION [--worker NAME=VERSION[,…]]…" >&2; exit 2; }
DIR=$1 VERSION=$2
shift 2
ROOT=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$DIR"
DIR=$(cd "$DIR" && pwd)
cd "$ROOT"

LDFLAGS="-s -w -X main.version=$VERSION${AGENT_UPDATE_PUBLIC_KEY:+ -X main.updateKey=$AGENT_UPDATE_PUBLIC_KEY}"
for os in linux darwin; do
  for arch in amd64 arm64; do
    CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath -buildvcs=false \
      -ldflags "$LDFLAGS" -o "$DIR/agent-$os-$arch" ./cmd/agent
  done
done
go run ./cmd/agent release-manifest "$DIR" "$VERSION" "$@"
cp deploy/install/install.sh "$DIR/install.sh"
echo "выпуск $VERSION: $DIR"
