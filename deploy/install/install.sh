#!/bin/sh
# Установка агента одной командой (Linux с systemd): скачивает сборку агента под эту машину
# и запускает `agent install` — он и ставит службу. Все флаги, кроме своих (ниже), передаются
# agent install как есть: --token, --name, --worker, --packages, --sysctl, --privileged и др.
# (список — agent install -h и docs/ARCHITECTURE.md, раздел «Установка»).
#
# С сервера (он раздаёт этот скрипт со своим адресом и ключами проверки подписи сборок):
#   curl -fsSL https://api.example.com/api/v1/agent-link/install.sh | sudo sh -s -- --token <токен> [флаги]
#   curl -fsSL https://api.example.com/api/v1/agent-link/install.sh | sudo sh -s -- --uninstall [--purge]
#
# Свои флаги:
#   --server URL      адрес сервера (в скрипт с сервера уже вписан)
#   --releases URL    откуда скачать сборку (по умолчанию <server>/api/v1/agent-link/releases);
#                     передаётся и agent install (оттуда же — сборки воркеров)
#   --ca-file ПУТЬ    свой корневой сертификат сервера для загрузки; передаётся и agent install
#   --binary ПУТЬ     не скачивать, а поставить сборку из файла
#   --instance ИМЯ    экземпляр агента (несколько агентов на одном узле — для разных бэкендов);
#                     передаётся и agent install
#   --uninstall [--purge]   удалить агента: /opt/agent/bin/agent uninstall [--purge]
#                     (экземпляр — /opt/agent-ИМЯ/bin/agent uninstall --instance ИМЯ [--purge])
#
# Без скрипта — то же самое вручную: скачать agent-linux-<arch> и выполнить
#   sudo ./agent-linux-<arch> install --server URL --token <токен>
set -eu

# Сервер, раздающий скрипт, подставляет сюда свой адрес и ключи проверки подписи сборок (base64 через
# пробел: автор агента и проект) — они уходят agent install флагами --update-key.
DEFAULT_SERVER=""
DEFAULT_UPDATE_KEYS=""

die() {
  echo "install.sh: $*" >&2
  exit 1
}

SERVER="$DEFAULT_SERVER" RELEASES="" BINARY="" CA_FILE="" UNINSTALL="" PURGE="" INSTANCE=""
# Свои флаги забираются, остальные остаются в "$@" по порядку (для agent install).
n=$#
while [ "$n" -gt 0 ]; do
  a=$1
  shift
  n=$((n - 1))
  case "$a" in
    --uninstall) UNINSTALL=1 ;;
    --purge) PURGE=1 ;;
    --binary | --server | --releases | --ca-file | --instance)
      [ "$n" -gt 0 ] || die "$a: нужно значение"
      v=$1
      shift
      n=$((n - 1))
      case "$a" in
        --binary) BINARY=$v ;;
        --server) SERVER=$v ;;
        --releases)
          RELEASES=$v
          set -- "$@" "$a" "$v"
          ;;
        --ca-file)
          CA_FILE=$v
          set -- "$@" "$a" "$v"
          ;;
        --instance)
          INSTANCE=$v
          set -- "$@" "$a" "$v"
          ;;
      esac
      ;;
    --binary=*) BINARY=${a#*=} ;;
    --server=*) SERVER=${a#*=} ;;
    --releases=*)
      RELEASES=${a#*=}
      set -- "$@" "$a"
      ;;
    --ca-file=*)
      CA_FILE=${a#*=}
      set -- "$@" "$a"
      ;;
    --instance=*)
      INSTANCE=${a#*=}
      set -- "$@" "$a"
      ;;
    *) set -- "$@" "$a" ;;
  esac
done

[ "$(uname -s)" = Linux ] || die "установка службой — только Linux с systemd; на других системах: agent init (папка агента), затем agent run"
[ "$(id -u)" -eq 0 ] || die "нужны права root (sudo)"

SERVER="${SERVER%/}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# fetch_agent — сборка агента в $TMP/agent: --binary или из manifest.json (sha256 сверяется).
fetch_agent() {
  if [ -n "$BINARY" ]; then
    [ -f "$BINARY" ] || die "--binary: нет файла $BINARY"
    cp "$BINARY" "$TMP/agent"
  else
    [ -n "$SERVER" ] || [ -n "$RELEASES" ] || die "--server: адрес сервера (или --binary ПУТЬ к сборке)"
    command -v curl >/dev/null || die "нужен curl"
    case "$(uname -m)" in
      x86_64 | amd64) ARCH=amd64 ;;
      aarch64 | arm64) ARCH=arm64 ;;
      *) die "архитектура $(uname -m) не поддерживается" ;;
    esac
    RELEASES="${RELEASES:-$SERVER/api/v1/agent-link/releases}"
    RELEASES="${RELEASES%/}"
    MANIFEST="$(curl -fsSL ${CA_FILE:+--cacert "$CA_FILE"} "$RELEASES/manifest.json")" || die "манифест сборок не получен: $RELEASES/manifest.json"
    # Без jq: каждый объект манифеста (записи плоские) — отдельной строкой; сборки агента — записи
    # без "name" (у воркеров он есть).
    ENTRY="$(printf '%s' "$MANIFEST" | tr -d '\n\r\t ' | sed 's/{/\n{/g' | grep '"os":"linux"' | grep "\"arch\":\"$ARCH\"" | grep -v '"name":' | head -n 1)" || true
    FILE="$(printf '%s' "$ENTRY" | sed -n 's/.*"file":"\([^"]*\)".*/\1/p')"
    SUM="$(printf '%s' "$ENTRY" | sed -n 's/.*"sha256":"\([^"]*\)".*/\1/p')"
    if [ -z "$FILE" ] || [ -z "$SUM" ]; then die "в manifest.json нет сборки агента linux/$ARCH"; fi
    # file — имя в каталоге сборок или абсолютная ссылка https:// (http:// — при http-каталоге).
    case "$FILE" in
      https://*) URL=$FILE ;;
      http://*)
        case "$RELEASES" in
          http://*) URL=$FILE ;;
          *) die "сборка $FILE: нужна ссылка https://" ;;
        esac
        ;;
      */* | *:*) die "сборка $FILE: нужно имя файла или ссылка https://" ;;
      *) URL="$RELEASES/$FILE" ;;
    esac
    curl -fsSL ${CA_FILE:+--cacert "$CA_FILE"} "$URL" -o "$TMP/agent" || die "сборка $FILE не скачана"
    GOT="$(sha256sum "$TMP/agent" | cut -d' ' -f1)"
    [ "$GOT" = "$SUM" ] || die "sha256 сборки не сходится: $GOT"
  fi
  chmod 0755 "$TMP/agent"
}

case "$INSTANCE" in
  "") OPT=/opt/agent ;;
  [a-z]*)
    case "$INSTANCE" in
      *[!a-z0-9-]*) die "--instance: имя экземпляра — строчная латиница, цифры и «-», первая — буква" ;;
    esac
    OPT="/opt/agent-$INSTANCE"
    ;;
  *) die "--instance: имя экземпляра — строчная латиница, цифры и «-», первая — буква" ;;
esac

if [ -n "$UNINSTALL" ]; then
  # Удаляет установленная программа; её уже нет (удалена без --purge) — скачанная.
  if [ -x "$OPT/bin/agent" ]; then
    "$OPT/bin/agent" uninstall ${INSTANCE:+--instance "$INSTANCE"} ${PURGE:+--purge}
  else
    fetch_agent
    "$TMP/agent" uninstall ${INSTANCE:+--instance "$INSTANCE"} ${PURGE:+--purge}
  fi
  exit
fi

fetch_agent

# Ключи сервера — флагами --update-key перед остальными (ключи — base64 без пробелов).
for k in $DEFAULT_UPDATE_KEYS; do
  set -- --update-key "$k" "$@"
done
# Адрес сервера — первым: такой же флаг в командной строке его заменяет.
"$TMP/agent" install ${SERVER:+--server "$SERVER"} "$@"
