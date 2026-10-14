#!/bin/sh
# Установка агента одной командой (Linux): скачивает сборку агента под эту машину (или архив
# папки агента проекта, --bundle) и запускает `agent install` — он и ставит службу systemd или
# агента пользователя без root (--rootless).
# Все флаги, кроме своих (ниже), передаются agent install как есть: --token, --name, --worker,
# --update-key, --packages, --sysctl, --privileged, --rootless и др.
# (список — agent install -h и docs/ARCHITECTURE.md, раздел «Установка»).
#
# Скрипт — в релизе агента (install.sh рядом со сборками):
#   I=https://github.com/epifanovmd/agent/releases/latest/download/install.sh
#   curl -fsSL $I | sudo sh -s -- --server https://api.example.com --token <токен> [флаги]
#   curl -fsSL $I | sh -s -- --rootless --server https://api.example.com --token <токен>
#   curl -fsSL $I | sh -s -- --run --server https://api.example.com --token <токен>
#   curl -fsSL $I | sudo sh -s -- --bundle 'https://files.example.com/agent-prod.tar.gz' --token <токен>
#   curl -fsSL $I | sudo sh -s -- --uninstall [--purge]
#
# Свои флаги:
#   --server URL      адрес сервера
#   --releases URL    откуда скачать сборку агента (по умолчанию — последний релиз агента на GitHub,
#                     $GITHUB_RELEASES); передаётся и agent install (оттуда же — сборки воркеров)
#   --ca-file ПУТЬ    свой корневой сертификат сервера для загрузки; передаётся и agent install
#   --binary ПУТЬ     не скачивать, а поставить сборку из файла
#   --bundle URL      архив папки агента проекта (agent pack, все платформы) по ссылке; {arch} в ссылке — amd64
#                     или arm64 этой машины. Программа агента, настройки и воркеры — из архива;
#                     --server — только если адреса нет в его настройках
#   --instance ИМЯ    экземпляр агента (несколько агентов на одном узле — для разных бэкендов);
#                     передаётся и agent install
#   --uninstall [--purge]   удалить агента: /opt/agent/bin/agent uninstall [--purge]
#                     (экземпляр — /opt/agent-ИМЯ/bin/agent uninstall --instance ИМЯ [--purge];
#                     без sudo — агента пользователя: ~/.agent[-ИМЯ]/bin/agent uninstall)
#   --run             без установки: агент здесь же на переднем плане (контейнер, свой менеджер
#                     процессов) — `agent run` с --token/--token-file и --name, данные —
#                     AGENT_DATA_DIR (по умолчанию /var/lib/agent); себя не обновляет. С --bundle —
#                     agent run с настройками архива; ключи проверки — --update-key
# Без root — только с --rootless или --run (агент пользователя в ~/.agent или без установки).
#
# Без скрипта — то же самое вручную: скачать agent-linux-<arch> и выполнить
#   sudo ./agent-linux-<arch> install --server URL --token <токен>
set -eu

# Каталог сборок агента по умолчанию — последний релиз агента на GitHub.
GITHUB_RELEASES=https://github.com/epifanovmd/agent/releases/latest/download

die() {
  echo "install.sh: $*" >&2
  exit 1
}

SERVER="" RELEASES="" BINARY="" BUNDLE="" CA_FILE="" UNINSTALL="" PURGE="" INSTANCE="" ROOTLESS="" RUN=""
# Свои флаги забираются, остальные остаются в "$@" по порядку (для agent install).
n=$#
while [ "$n" -gt 0 ]; do
  a=$1
  shift
  n=$((n - 1))
  case "$a" in
    --uninstall) UNINSTALL=1 ;;
    --purge) PURGE=1 ;;
    --rootless)
      ROOTLESS=1
      set -- "$@" "$a"
      ;;
    --run) RUN=1 ;;
    --binary | --bundle | --server | --releases | --ca-file | --instance)
      [ "$n" -gt 0 ] || die "$a: нужно значение"
      v=$1
      shift
      n=$((n - 1))
      case "$a" in
        --binary) BINARY=$v ;;
        --bundle) BUNDLE=$v ;;
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
    --bundle=*) BUNDLE=${a#*=} ;;
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

[ "$(uname -s)" = Linux ] || die "установка — только Linux; на других системах — agent run с файлом настроек agent.yaml"
[ "$(id -u)" -eq 0 ] || [ -n "$ROOTLESS" ] || [ -n "$RUN" ] || [ -n "$UNINSTALL" ] ||
  die "нужны права root (sudo); без root — агент пользователя (--rootless) или без установки (--run)"

SERVER="${SERVER%/}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

arch() {
  case "$(uname -m)" in
    x86_64 | amd64) echo amd64 ;;
    aarch64 | arm64) echo arm64 ;;
    *) die "архитектура $(uname -m) не поддерживается" ;;
  esac
}

# fetch_agent — программа агента в $AGENT: из архива --bundle (распакован в $TMP/agent/), --binary
# или из manifest.json (sha256 сверяется).
AGENT="$TMP/agent"
fetch_agent() {
  if [ -n "$BUNDLE" ]; then
    command -v curl >/dev/null || die "нужен curl"
    URL=$(printf '%s' "$BUNDLE" | sed "s/{arch}/$(arch)/g")
    curl -fsSL ${CA_FILE:+--cacert "$CA_FILE"} "$URL" -o "$TMP/bundle.tar.gz" || die "архив не скачан: $URL"
    tar xzf "$TMP/bundle.tar.gz" -C "$TMP" || die "архив не распакован: $URL"
    AGENT="$TMP/agent/agent"
    if [ ! -x "$AGENT" ] || [ ! -f "$TMP/agent/bundle.json" ]; then
      die "$URL: не архив agent pack (нет agent/agent и agent/bundle.json)"
    fi
    return
  fi
  if [ -n "$BINARY" ]; then
    [ -f "$BINARY" ] || die "--binary: нет файла $BINARY"
    cp "$BINARY" "$TMP/agent"
  else
    command -v curl >/dev/null || die "нужен curl"
    ARCH=$(arch)
    # Свой корневой сертификат — только для своего каталога (--releases), не для GitHub.
    CA=""
    if [ -n "$RELEASES" ]; then CA=$CA_FILE; fi
    RELEASES="${RELEASES:-$GITHUB_RELEASES}"
    RELEASES="${RELEASES%/}"
    MANIFEST="$(curl -fsSL ${CA:+--cacert "$CA"} "$RELEASES/manifest.json")" || die "манифест сборок не получен: $RELEASES/manifest.json"
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
    curl -fsSL ${CA:+--cacert "$CA"} "$URL" -o "$TMP/agent" || die "сборка $FILE не скачана"
    GOT="$(sha256sum "$TMP/agent" | cut -d' ' -f1)"
    [ "$GOT" = "$SUM" ] || die "sha256 сборки не сходится: $GOT"
  fi
  chmod 0755 "$AGENT"
}

case "$INSTANCE" in
  "") SUFFIX="" ;;
  [a-z]*)
    case "$INSTANCE" in
      *[!a-z0-9-]*) die "--instance: имя экземпляра — строчная латиница, цифры и «-», первая — буква" ;;
    esac
    SUFFIX="-$INSTANCE"
    ;;
  *) die "--instance: имя экземпляра — строчная латиница, цифры и «-», первая — буква" ;;
esac
# Программа установленного агента: системного (root) или пользователя (~/.agent).
if [ "$(id -u)" -eq 0 ]; then
  BIN="/opt/agent$SUFFIX/bin/agent"
else
  BIN="${HOME:-}/.agent$SUFFIX/bin/agent"
fi

if [ -n "$UNINSTALL" ]; then
  # Удаляет установленная программа; её уже нет (удалена без --purge) — скачанная. Из архива —
  # его программа: экземпляр она берёт из настроек архива.
  if [ -n "$BUNDLE" ]; then
    fetch_agent
    "$AGENT" uninstall ${PURGE:+--purge}
  elif [ -x "$BIN" ]; then
    "$BIN" uninstall ${INSTANCE:+--instance "$INSTANCE"} ${PURGE:+--purge}
  else
    fetch_agent
    "$TMP/agent" uninstall ${INSTANCE:+--instance "$INSTANCE"} ${PURGE:+--purge}
  fi
  exit
fi

fetch_agent

# Без установки: agent run здесь же (настройки — из окружения или архива), только токен, имя и
# ключи проверки.
if [ -n "$RUN" ]; then
  [ -n "$SERVER" ] || [ -n "$BUNDLE" ] || die "--server: адрес сервера"
  KEYS=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --token | --token-file | --name | --update-key | --releases | --ca-file | --instance) [ $# -ge 2 ] || die "$1: нужно значение" ;;
    esac
    case "$1" in
      --token) AGENT_ENROLL_TOKEN=$2; shift 2 ;;
      --token=*) AGENT_ENROLL_TOKEN=${1#*=}; shift ;;
      --token-file) AGENT_ENROLL_TOKEN=$(tr -d ' \t\r\n' <"$2"); shift 2 ;;
      --token-file=*) AGENT_ENROLL_TOKEN=$(tr -d ' \t\r\n' <"${1#*=}"); shift ;;
      --name) AGENT_NAME=$2; shift 2 ;;
      --name=*) AGENT_NAME=${1#*=}; shift ;;
      --update-key) KEYS="${KEYS:+$KEYS,}$2"; shift 2 ;;
      --update-key=*) KEYS="${KEYS:+$KEYS,}${1#*=}"; shift ;;
      # Флаги скрипта, уже учтённые при загрузке.
      --releases | --ca-file | --instance) shift 2 ;;
      --releases=* | --ca-file=* | --instance=*) shift ;;
      *) die "--run: $1 — здесь только --token, --token-file, --name и --update-key" ;;
    esac
  done
  [ -z "$SERVER" ] || export AGENT_SERVER_URL="$SERVER"
  AGENT_DATA_DIR="${AGENT_DATA_DIR:-/var/lib/agent}"
  AGENT_UPDATE_MODE="${AGENT_UPDATE_MODE:-external}"
  export AGENT_DATA_DIR AGENT_UPDATE_MODE
  [ -z "${AGENT_ENROLL_TOKEN:-}" ] || export AGENT_ENROLL_TOKEN
  [ -z "${AGENT_NAME:-}" ] || export AGENT_NAME
  [ -z "$CA_FILE" ] || export AGENT_SERVER_CA_FILE="$CA_FILE"
  [ -z "$KEYS" ] || export AGENT_UPDATE_PUBLIC_KEYS="$KEYS"
  mkdir -p "$AGENT_DATA_DIR"
  if [ -n "$BUNDLE" ]; then
    CONFIG=$(sed -n 's/.*"config": *"\([^"]*\)".*/\1/p' "$TMP/agent/bundle.json")
    exec "$AGENT" run -config "$TMP/agent/${CONFIG:-agent.yaml}"
  fi
  exec "$AGENT" run
fi

"$AGENT" install ${SERVER:+--server "$SERVER"} "$@"
