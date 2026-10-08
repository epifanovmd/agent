#!/bin/sh
# Установка агента как службы systemd (Linux): исполняемый файл, конфигурация,
# каталог данных, служба. Повторный запуск обновляет файл и службу, не трогая
# учётные данные агента.
#
# Одной командой — с сервера (он раздаёт этот скрипт со своим адресом и сборки):
#   curl -fsSL https://сервер/api/v1/agent-link/install.sh | sudo sh -s -- --token <токен регистрации>
#
# Из локальной сборки:
#   sudo sh install.sh --binary ./agent-linux-amd64 --server https://api.example.com --token <токен> \
#     [--public-key <ключ релизов>] [--name node-01] [--config agent.yaml] [--user agent|root] \
#     [--ca-file ca.pem] [--kill-mode mixed|process] \
#     [--stop-timeout 15min] [--rw-path /etc/example]… \
#     [--packages "jq curl"] [--packages-apk "bind-tools"]… \
#     [--sysctl vm.max_map_count=262144]… [--privileged] \
#     [--worker sysinfo]…
#   sudo sh install.sh --uninstall [--purge]
#
# --worker NAME (повторяемый) — воркер из выпуска: сборка под эту машину из
# manifest.json сервера (старшая версия; sha256 сверяется) кладётся в
# /var/lib/agent/workers/NAME/current (+ version); в создаваемый agent.yaml
# дописывается запись `- name: NAME, release: true` (+ restart/stopTimeout из
# манифеста). Готовый agent.yaml (есть или --config) не меняется. Дальше
# обновлять воркер может сервер (команда worker.update).
#
# --token-file PATH — токен регистрации из файла (не виден в списке процессов);
# вместо --token.
#
# --packages ставит системные пакеты для воркеров; --packages-apt|-dnf|-yum|-apk|-zypper
# — имена пакетов для этого менеджера: на узле с ним заменяют --packages.
# --sysctl (повторяемый) задаёт параметры ядра в /etc/sysctl.d/90-agent.conf.
# Что поставлено этой установкой (пакеты, которых не было до установки), какие
# параметры заданы и их значения до установки — в журнале /etc/agent/install-state.
# --uninstall сначала даёт воркерам убрать за собой (agent cleanup), затем
# удаляет службу, файл параметров ядра и возвращает их значения до установки;
# --purge — ещё конфигурацию, данные и пакеты, поставленные установкой.
#
# --kill-mode: mixed (по умолчанию) — при остановке службы systemd добивает
# все процессы агента по истечении срока; process — только агент (он сам
# останавливает воркеров), фоновые процессы воркеров переживают перезапуск
# агента. Запоминается для повторной установки.
#
# Служба получает Delegate=yes: агент ограничивает ресурсы воркеров
# (workers[].limits в agent.yaml) через cgroup v2.
#
# По умолчанию агент — системный пользователь agent в защищённой службе: система
# только для чтения, запись — в /var/lib/agent, /opt/agent и каталоги --rw-path.
# Воркерам, которые настраивают узел (сеть, firewall, системные конфиги), нужен
# --privileged: агент и воркеры — root без ограничений файловой системы. Только
# если воркерам это действительно нужно.
#
# --ca-file — свой CA сервера (PEM): копируется в /etc/agent/ca.pem, агент
# доверяет ему вместе с системными (server.caFile в создаваемом agent.yaml).
#
# Воркеры описываются в /etc/agent/agent.yaml (workers). Токен регистрации нужен
# только до первой регистрации и хранится в /etc/agent/agent.env (0600).
# Изменения agent.yaml применяются без перезапуска: systemctl reload agent (SIGHUP).
set -eu

# Сервер, раздающий скрипт, подставляет сюда свой адрес и ключ проверки релизов.
DEFAULT_SERVER=""
DEFAULT_PUBLIC_KEY=""

BINARY="" SERVER="$DEFAULT_SERVER" TOKEN="" TOKEN_FILE="" KILL_MODE="" PUBLIC_KEY="$DEFAULT_PUBLIC_KEY" NAME="" CONFIG="" CA_FILE=""
USER_NAME="agent" STOP_TIMEOUT="15min" UNINSTALL="" PURGE=""
PRIVILEGED="" RW_PATHS="" PACKAGES="" SYSCTLS="" WORKERS=""
PACKAGES_APT="" PACKAGES_DNF="" PACKAGES_YUM="" PACKAGES_APK="" PACKAGES_ZYPPER=""
STATE=/etc/agent/install-state SYSCTL_FILE=/etc/sysctl.d/90-agent.conf

die() { echo "install.sh: $*" >&2; exit 1; }

while [ $# -gt 0 ]; do
  case "$1" in
    --binary) BINARY="$2"; shift 2 ;;
    --server) SERVER="$2"; shift 2 ;;
    --token) TOKEN="$2"; shift 2 ;;
    --token-file) TOKEN_FILE="$2"; shift 2 ;;
    --kill-mode)
      case "$2" in process | mixed) KILL_MODE="$2" ;; *) die "--kill-mode: process | mixed, а не $2" ;; esac
      shift 2 ;;
    --public-key) PUBLIC_KEY="$2"; shift 2 ;;
    --name) NAME="$2"; shift 2 ;;
    --config) CONFIG="$2"; shift 2 ;;
    --ca-file) CA_FILE="$2"; shift 2 ;;
    --user) USER_NAME="$2"; shift 2 ;;
    --stop-timeout) STOP_TIMEOUT="$2"; shift 2 ;;
    --privileged) PRIVILEGED=1; shift ;;
    --rw-path) RW_PATHS="$RW_PATHS $2"; shift 2 ;;
    --packages) PACKAGES="$PACKAGES $2"; shift 2 ;;
    --packages-apt) PACKAGES_APT="$PACKAGES_APT $2"; shift 2 ;;
    --packages-dnf) PACKAGES_DNF="$PACKAGES_DNF $2"; shift 2 ;;
    --packages-yum) PACKAGES_YUM="$PACKAGES_YUM $2"; shift 2 ;;
    --packages-apk) PACKAGES_APK="$PACKAGES_APK $2"; shift 2 ;;
    --packages-zypper) PACKAGES_ZYPPER="$PACKAGES_ZYPPER $2"; shift 2 ;;
    --sysctl)
      printf '%s' "$2" | grep -Eq '^[A-Za-z0-9_][A-Za-z0-9_./-]*=.*$' || die "--sysctl KEY=VALUE, а не $2"
      SYSCTLS="$SYSCTLS
$2"; shift 2 ;;
    --worker)
      printf '%s' "$2" | grep -Eq '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$' || die "--worker: имя воркера — латиница, цифры, «.», «_», «-», а не $2"
      WORKERS="$WORKERS $2"; shift 2 ;;
    --uninstall) UNINSTALL=1; shift ;;
    --purge) PURGE=1; shift ;;
    *) die "неизвестный аргумент $1" ;;
  esac
done

if [ -n "$TOKEN_FILE" ]; then
  [ -z "$TOKEN" ] || die "--token и --token-file — что-то одно"
  [ -r "$TOKEN_FILE" ] || die "--token-file: нет файла $TOKEN_FILE"
  TOKEN="$(tr -d ' \t\r\n' <"$TOKEN_FILE")"
  [ -n "$TOKEN" ] || die "--token-file: файл $TOKEN_FILE пуст"
fi
[ "$(id -u)" -eq 0 ] || die "нужен root (sudo)"
command -v systemctl >/dev/null || die "нужен systemd"

# Менеджер пакетов узла: apt | dnf | yum | apk | zypper (пусто — не найден).
pkg_manager() {
  for m in apt-get dnf yum apk zypper; do
    if command -v "$m" >/dev/null; then echo "$m"; return; fi
  done
}

# pkg_installed PKG — пакет уже стоит.
pkg_installed() {
  case "$(pkg_manager)" in
    apt-get) dpkg-query -W -f='${Status}' "$1" 2>/dev/null | grep -q "install ok installed" ;;
    dnf | yum | zypper) rpm -q "$1" >/dev/null 2>&1 ;;
    apk) apk info -e "$1" >/dev/null 2>&1 ;;
    *) return 1 ;;
  esac
}

# sysctl_apply — применить параметры ядра сейчас (без --system — только файл агента).
sysctl_apply() {
  command -v sysctl >/dev/null || { echo "install.sh: sysctl не найден — изменения параметров ядра вступят в силу при загрузке" >&2; return 0; }
  if [ -f "$SYSCTL_FILE" ]; then
    sysctl -q -p "$SYSCTL_FILE" >/dev/null || echo "install.sh: параметры ядра не применены сейчас — применятся при загрузке" >&2
  else
    sysctl -q --system >/dev/null 2>&1 || true
  fi
}

if [ -n "$UNINSTALL" ]; then
  # Служба — пользователь, от которого работали агент и воркеры.
  RUN_AS="$(sed -n 's/^User=//p' /etc/systemd/system/agent.service 2>/dev/null | head -n 1)"
  systemctl disable --now agent.service 2>/dev/null || true
  # Воркеры убирают за собой (без сервера); ошибка — предупреждение, удаление продолжается.
  if [ -x /opt/agent/bin/agent ] && [ -f /etc/agent/agent.yaml ]; then
    CLEANUP='set -a; [ ! -r /etc/agent/agent.env ] || . /etc/agent/agent.env; set +a; exec /opt/agent/bin/agent cleanup -config /etc/agent/agent.yaml'
    if [ -z "$RUN_AS" ] || [ "$RUN_AS" = root ]; then
      sh -c "$CLEANUP"
    elif command -v runuser >/dev/null; then
      runuser -u "$RUN_AS" -- sh -c "$CLEANUP"
    else
      su -s /bin/sh "$RUN_AS" -c "$CLEANUP"
    fi || echo "install.sh: предупреждение: воркеры убрали за собой не всё (agent cleanup) — удаление продолжается" >&2
  fi
  rm -f /etc/systemd/system/agent.service
  systemctl daemon-reload
  rm -rf /opt/agent
  if [ -f "$SYSCTL_FILE" ]; then
    rm -f "$SYSCTL_FILE"
    sysctl_apply
  fi
  if [ -f "$STATE" ]; then
    # Прежние значения параметров ядра — как были до установки.
    sed -n 's/^sysctl-prev //p' "$STATE" | while IFS= read -r kv; do
      [ -n "$kv" ] || continue
      sysctl -q -w "$kv" >/dev/null 2>&1 || echo "install.sh: предупреждение: прежнее значение не возвращено: $kv" >&2
    done
    grep -v -e '^sysctl ' -e '^sysctl-prev ' "$STATE" >"$STATE.tmp" || true
    mv "$STATE.tmp" "$STATE"
  fi
  if [ -n "$PURGE" ]; then
    DELIVERED="$( [ ! -f "$STATE" ] || sed -n 's/^package //p' "$STATE" | tr '\n' ' ')"
    if [ -n "$(printf '%s' "$DELIVERED" | tr -d ' ')" ]; then
      echo "Удаляются пакеты, поставленные установкой: $DELIVERED"
      # Список пакетов — словами через пробел.
      # shellcheck disable=SC2086
      case "$(pkg_manager)" in
        apt-get) DEBIAN_FRONTEND=noninteractive apt-get remove -y -qq $DELIVERED ;;
        dnf) dnf remove -y -q $DELIVERED ;;
        yum) yum remove -y -q $DELIVERED ;;
        apk) apk del $DELIVERED ;;
        zypper) zypper --non-interactive remove $DELIVERED ;;
        *) false ;;
      esac || echo "install.sh: предупреждение: пакеты не удалены:$DELIVERED" >&2
    fi
    rm -rf /etc/agent /var/lib/agent
    id agent >/dev/null 2>&1 && userdel agent || true
  fi
  echo "Агент удалён${PURGE:+ вместе с конфигурацией, данными и поставленными пакетами}"
  exit 0
fi

[ -n "$SERVER" ] || [ -f /etc/agent/agent.yaml ] || die "--server: адрес сервера"
SERVER="${SERVER%/}"
[ -z "$PRIVILEGED" ] || USER_NAME=root

install -d -m 0755 /etc/agent
# Свой CA сервера: агенту (server.caFile) и curl при загрузке сборки.
CA_PEM=/etc/agent/ca.pem
if [ -n "$CA_FILE" ]; then
  [ -f "$CA_FILE" ] || die "--ca-file: нет файла $CA_FILE"
  grep -q 'BEGIN CERTIFICATE' "$CA_FILE" || die "--ca-file: в $CA_FILE нет сертификата PEM"
  [ "$CA_FILE" = "$CA_PEM" ] || install -m 0644 "$CA_FILE" "$CA_PEM"
fi
[ -s "$STATE" ] || echo "# Журнал установки агента: package — пакет поставлен установкой, sysctl — параметр ядра агента, sysctl-prev — его значение до установки, kill-mode — режим остановки службы" >"$STATE"

# Режим остановки службы: заданный — запоминается, без --kill-mode — прежний (или mixed).
[ -n "$KILL_MODE" ] || KILL_MODE="$(sed -n 's/^kill-mode //p' "$STATE" | tail -n 1)"
case "$KILL_MODE" in process | mixed) ;; *) KILL_MODE=mixed ;; esac
grep -v '^kill-mode ' "$STATE" >"$STATE.tmp" || true
echo "kill-mode $KILL_MODE" >>"$STATE.tmp"
mv "$STATE.tmp" "$STATE"

# Пакеты под менеджер узла (--packages-<менеджер>) заменяют общий список.
case "$(pkg_manager)" in
  apt-get) [ -z "$PACKAGES_APT" ] || PACKAGES="$PACKAGES_APT" ;;
  dnf) [ -z "$PACKAGES_DNF" ] || PACKAGES="$PACKAGES_DNF" ;;
  yum) [ -z "$PACKAGES_YUM" ] || PACKAGES="$PACKAGES_YUM" ;;
  apk) [ -z "$PACKAGES_APK" ] || PACKAGES="$PACKAGES_APK" ;;
  zypper) [ -z "$PACKAGES_ZYPPER" ] || PACKAGES="$PACKAGES_ZYPPER" ;;
esac

# Системные пакеты для воркеров (например, jq curl). Каких не
# было до установки — в журнал: --uninstall --purge удалит только их.
if [ -n "$PACKAGES" ]; then
  NEW=""
  for p in $PACKAGES; do pkg_installed "$p" || NEW="$NEW $p"; done
  # Список пакетов — словами через пробел.
  # shellcheck disable=SC2086
  case "$(pkg_manager)" in
    apt-get) DEBIAN_FRONTEND=noninteractive apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq $PACKAGES ;;
    dnf) dnf install -y -q $PACKAGES ;;
    yum) yum install -y -q $PACKAGES ;;
    apk) apk add --no-cache $PACKAGES ;;
    zypper) zypper --non-interactive install $PACKAGES ;;
    *) die "--packages: не найден менеджер пакетов (apt, dnf, yum, apk, zypper)" ;;
  esac || die "--packages: не установлены:$PACKAGES"
  for p in $NEW; do
    grep -qx "package $p" "$STATE" || echo "package $p" >>"$STATE"
  done
fi

# Параметры ядра для воркеров: файл агента в /etc/sysctl.d (заданный ключ
# заменяет прежнее значение), применяются сразу; в журнал — что задано.
if [ -n "$SYSCTLS" ]; then
  install -d -m 0755 "$(dirname "$SYSCTL_FILE")"
  [ -f "$SYSCTL_FILE" ] || echo "# Параметры ядра для воркеров агента (install.sh --sysctl)" >"$SYSCTL_FILE"
  echo "$SYSCTLS" | while IFS= read -r kv; do
    [ -n "$kv" ] || continue
    key="${kv%%=*}"
    # Значение до установки — один раз (повторная установка его не меняет).
    if ! grep -qF "sysctl-prev $key=" "$STATE" && command -v sysctl >/dev/null &&
      prev="$(sysctl -n "$key" 2>/dev/null)"; then
      printf 'sysctl-prev %s=%s\n' "$key" "$prev" >>"$STATE"
    fi
    awk -v k="$key=" 'index($0, k) != 1' "$SYSCTL_FILE" >"$SYSCTL_FILE.tmp"
    echo "$kv" >>"$SYSCTL_FILE.tmp"
    mv "$SYSCTL_FILE.tmp" "$SYSCTL_FILE"
    awk -v k="sysctl $key=" 'index($0, k) != 1' "$STATE" >"$STATE.tmp"
    echo "sysctl $kv" >>"$STATE.tmp"
    mv "$STATE.tmp" "$STATE"
  done
  chmod 0644 "$SYSCTL_FILE"
  sysctl_apply
fi

# Без --binary — сборка с сервера: файл под эту архитектуру из manifest.json, sha256 сверяется.
# Воркеры из выпуска (--worker) — тоже из манифеста.
TMP=""
MANIFEST_LINES=""
if [ -z "$BINARY" ] || [ -n "$WORKERS" ]; then
  [ -n "$SERVER" ] || die "--binary или --server${WORKERS:+ (--worker берёт сборки с сервера)}"
  command -v curl >/dev/null || die "нужен curl"
  case "$(uname -m)" in
    x86_64 | amd64) ARCH=amd64 ;;
    aarch64 | arm64) ARCH=arm64 ;;
    *) die "архитектура $(uname -m) не поддерживается" ;;
  esac
  RELEASES="$SERVER/api/v1/agent-link/releases"
  MANIFEST="$(curl -fsSL ${CA_FILE:+--cacert "$CA_PEM"} "$RELEASES/manifest.json")" || die "манифест выпуска не получен с $RELEASES"
  # Без jq: каждый объект манифеста (записи плоские) — отдельной строкой;
  # сборки агента — записи без "name", воркеров — с "name" (agent release-manifest).
  MANIFEST_LINES="$(printf '%s' "$MANIFEST" | tr -d '\n\r\t ' | sed 's/{/\n{/g' | grep "\"os\":\"linux\"" | grep "\"arch\":\"$ARCH\"")" || true
fi
# field LINE KEY — строковое значение поля записи манифеста.
field() { printf '%s' "$1" | sed -n "s/.*\"$2\":\"\([^\"]*\)\".*/\1/p"; }
# vsort — сортировка версий по числовым частям (sort -V, если есть).
vsort() {
  if printf '1\n' | sort -V >/dev/null 2>&1; then sort -V; else sort -t. -k1,1n -k2,2n -k3,3n -k4,4n; fi
}
if [ -z "$BINARY" ]; then
  ENTRY="$(printf '%s\n' "$MANIFEST_LINES" | grep -v '"name":' | head -n 1)"
  FILE="$(field "$ENTRY" file)"
  SUM="$(field "$ENTRY" sha256)"
  [ -n "$FILE" ] && [ -n "$SUM" ] || die "в манифесте нет сборки linux/$ARCH"
  TMP="$(mktemp -d)"
  BINARY="$TMP/$FILE"
  curl -fsSL ${CA_FILE:+--cacert "$CA_PEM"} "$RELEASES/$FILE" -o "$BINARY" || die "сборка $FILE не скачана"
  GOT="$(sha256sum "$BINARY" | cut -d' ' -f1)"
  [ "$GOT" = "$SUM" ] || die "sha256 сборки не сходится: $GOT"
fi
[ -f "$BINARY" ] || die "--binary: путь к сборке agent-linux-<arch>"

if [ "$USER_NAME" != "root" ] && ! id "$USER_NAME" >/dev/null 2>&1; then
  useradd --system --home-dir /var/lib/agent --shell /usr/sbin/nologin "$USER_NAME"
fi

install -d -m 0700 -o "$USER_NAME" /var/lib/agent

# Воркеры из выпуска: сборка — в /var/lib/agent/workers/NAME/current (+ version),
# запись для agent.yaml — в WORKERS_YAML.
WORKERS_YAML=""
for w in $WORKERS; do
  LINES="$(printf '%s\n' "$MANIFEST_LINES" | grep -F "\"name\":\"$w\"")" || true
  VERSION="$(printf '%s\n' "$LINES" | while IFS= read -r l; do [ -z "$l" ] || field "$l" version; echo; done | grep -v '^$' | vsort | tail -n 1)"
  [ -n "$VERSION" ] || die "--worker $w: в манифесте нет сборки воркера под linux/$ARCH"
  ENTRY="$(printf '%s\n' "$LINES" | grep -F "\"version\":\"$VERSION\"" | tail -n 1)"
  WFILE="$(field "$ENTRY" file)"
  WSUM="$(field "$ENTRY" sha256)"
  [ -n "$WFILE" ] && [ -n "$WSUM" ] && [ "$WFILE" = "$(basename "$WFILE")" ] || die "--worker $w: запись манифеста неполная"
  WDIR="/var/lib/agent/workers/$w"
  install -d -m 0755 -o "$USER_NAME" /var/lib/agent/workers "$WDIR"
  curl -fsSL ${CA_FILE:+--cacert "$CA_PEM"} "$RELEASES/$WFILE" -o "$WDIR/current.new" || { rm -f "$WDIR/current.new"; die "--worker $w: сборка $WFILE не скачана"; }
  GOT="$(sha256sum "$WDIR/current.new" | cut -d' ' -f1)"
  [ "$GOT" = "$WSUM" ] || { rm -f "$WDIR/current.new"; die "--worker $w: sha256 сборки не сходится: $GOT"; }
  chmod 0755 "$WDIR/current.new"
  mv -f "$WDIR/current.new" "$WDIR/current"
  echo "$VERSION" >"$WDIR/version"
  chown "$USER_NAME" "$WDIR/current" "$WDIR/version"
  echo "Воркер $w $VERSION поставлен: $WDIR/current"
  RESTART="$(field "$ENTRY" restart)"
  WSTOP="$(field "$ENTRY" stopTimeout)"
  WORKERS_YAML="${WORKERS_YAML}  - name: $w
    release: true
${RESTART:+    restart: $RESTART
}${WSTOP:+    stopTimeout: $WSTOP
}"
done

# Исполняемый файл — в каталоге, доступном агенту на запись: самообновление
# кладёт рядом новую версию (.new), копию прежней (.prev) и отметку.
install -d -m 0755 -o "$USER_NAME" /opt/agent /opt/agent/bin
install -m 0755 -o "$USER_NAME" "$BINARY" /opt/agent/bin/agent.new
mv -f /opt/agent/bin/agent.new /opt/agent/bin/agent
[ -z "$TMP" ] || rm -rf "$TMP"

CREATED_CONFIG=""
if [ -n "$CONFIG" ]; then
  install -m 0644 "$CONFIG" /etc/agent/agent.yaml
elif [ ! -f /etc/agent/agent.yaml ]; then
  CREATED_CONFIG=1
  cat >/etc/agent/agent.yaml <<YAML
server:
  url: ${SERVER}
${CA_FILE:+  caFile: ${CA_PEM}
}dataDir: /var/lib/agent
${NAME:+name: ${NAME}
}update:
  mode: self
  # Ключ проверки подписи релизов (agent keygen → AGENT_UPDATE_PUBLIC_KEY).
  publicKey: \${AGENT_UPDATE_PUBLIC_KEY}
log:
  format: json
# Воркеры этого узла: дочерние процессы на любом языке.
YAML
  if [ -n "$WORKERS_YAML" ]; then
    printf 'workers:\n%s' "$WORKERS_YAML" >>/etc/agent/agent.yaml
  else
    echo 'workers: []' >>/etc/agent/agent.yaml
  fi
elif [ -n "$CA_FILE" ] && ! grep -q 'caFile:' /etc/agent/agent.yaml; then
  echo "install.sh: CA скопирован в $CA_PEM — пропишите server.caFile: $CA_PEM в /etc/agent/agent.yaml" >&2
fi
# Готовый agent.yaml не меняется: подсказка, если в нём нет воркера из выпуска.
if [ -z "$CREATED_CONFIG" ]; then
  for w in $WORKERS; do
    grep -A6 -E "name:[[:space:]]*[\"']?${w}[\"']?[[:space:]]*\$" /etc/agent/agent.yaml | grep -Eq 'release:[[:space:]]*true' ||
      printf 'install.sh: сборка воркера %s поставлена; agent.yaml не менялся — добавьте в workers:\n  - name: %s\n    release: true\nи выполните systemctl reload agent\n' "$w" "$w" >&2
  done
fi

# Секреты — в agent.env (0600): переданные заменяют прежние, остальные сохраняются.
set_env() {
  touch /etc/agent/agent.env
  grep -v "^$1=" /etc/agent/agent.env >/etc/agent/agent.env.tmp || true
  printf '%s=%s\n' "$1" "$2" >>/etc/agent/agent.env.tmp
  mv /etc/agent/agent.env.tmp /etc/agent/agent.env
}
umask 077
[ -z "$TOKEN" ] || set_env AGENT_ENROLL_TOKEN "$TOKEN"
[ -z "$PUBLIC_KEY" ] || set_env AGENT_UPDATE_PUBLIC_KEY "$PUBLIC_KEY"
if [ -f /etc/agent/agent.env ]; then
  chmod 0600 /etc/agent/agent.env
  chown "$USER_NAME" /etc/agent/agent.env
fi
umask 022

# Ограничения службы: обычный режим — система только для чтения, запись — в свои
# каталоги и --rw-path; --privileged — без ограничений (воркеры настраивают узел).
if [ -n "$PRIVILEGED" ]; then
  HARDENING="# --privileged: агент и воркеры настраивают узел — без ограничений файловой системы."
else
  for p in $RW_PATHS; do install -d -m 0755 -o "$USER_NAME" "$p"; done
  HARDENING="NoNewPrivileges=true
ProtectSystem=full
ProtectHome=read-only
PrivateTmp=true
ReadWritePaths=/var/lib/agent /opt/agent$RW_PATHS"
fi

# Служба: SIGHUP (systemctl reload) — агент перечитывает agent.yaml на ходу;
# SIGTERM — агенту (он останавливает воркеры, дорабатывая задачи), по
# истечении срока systemd добивает группу (--kill-mode process — только агента);
# Delegate=yes — агент сам ведёт подгруппы cgroup (ограничения воркеров); откат обновления делает прежняя
# версия до запуска новой (ExecStartPre), перезапуск — всегда.
cat >/etc/systemd/system/agent.service <<UNIT
[Unit]
Description=Agent
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=${USER_NAME}
EnvironmentFile=-/etc/agent/agent.env
Environment=AGENT_BOOT_GUARD=external
ExecStartPre=-/bin/sh -c '[ ! -x /opt/agent/bin/agent.prev ] || /opt/agent/bin/agent.prev boot-guard /opt/agent/bin/agent'
ExecStart=/opt/agent/bin/agent run -config /etc/agent/agent.yaml
ExecReload=/bin/kill -HUP \$MAINPID
Restart=always
RestartSec=2
StartLimitIntervalSec=0
KillMode=${KILL_MODE}
Delegate=yes
TimeoutStopSec=${STOP_TIMEOUT}
${HARDENING}

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable agent.service >/dev/null
systemctl restart agent.service

echo "Агент $(/opt/agent/bin/agent version) установлен: systemctl status agent, journalctl -u agent -f"
