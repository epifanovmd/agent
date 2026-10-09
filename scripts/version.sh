#!/usr/bin/env bash
# Версия проекта — одна на агента, SDK и примеры. Источник — файл VERSION; агент получает её
# при сборке (-X main.version, scripts/go.sh и scripts/release.sh). Нужен node (файлы
# блокировки npm).
#   scripts/version.sh check        # все места совпадают с VERSION (код выхода 1 — нет)
#   scripts/version.sh set 1.2.0    # записать версию во все места
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Файл и шаблон строки с версией (sed -E; версия — в группе 2).
PLACES=(
  "VERSION|^()([0-9][0-9A-Za-z.+-]*)()$"
  "sdk/node/package.json|^(  \"version\": \")([^\"]+)(\",)$"
  "sdk/node/src/index.ts|^(export const SDK_VERSION = \")([^\"]+)(\";)$"
  "examples/server/package.json|^(  \"version\": \")([^\"]+)(\",)$"
)

# Файлы блокировки npm: версия пакета (version, packages[""]) и, у стенда, версия agent-sdk
# (packages["../../sdk/node"]). Печатает несовпадения с $2; с $3=set — исправляет их.
LOCKS=(sdk/node/package-lock.json examples/server/package-lock.json)
locks() {
  node - "$1" "$2" "${LOCKS[@]}" <<'JS'
const fs = require("node:fs");
const [mode, want, ...files] = process.argv.slice(2);
for (const file of files) {
  const lock = JSON.parse(fs.readFileSync(file, "utf8"));
  const places = [[lock, "version"], [lock.packages?.[""], "version"], [lock.packages?.["../../sdk/node"], "version"]];
  let changed = false;
  for (const [obj, key] of places) {
    if (!obj || obj[key] === want) continue;
    if (mode === "set") { obj[key] = want; changed = true; }
    else console.log(`версия: ${file} — ${obj[key]}, нужно ${want}`);
  }
  if (changed) fs.writeFileSync(file, JSON.stringify(lock, null, 2) + "\n");
}
JS
}

current() { sed -nE "s#$2#\\2#p" "$1" | head -n 1; }

case "${1:-check}" in
  check)
    want=$(cat VERSION)
    bad=0
    for p in "${PLACES[@]}"; do
      file=${p%%|*} re=${p#*|}
      got=$(current "$file" "$re")
      if [ "$got" != "$want" ]; then
        echo "версия: $file — ${got:-не найдена}, нужно $want" >&2
        bad=1
      fi
    done
    out=$(locks check "$want")
    [ -z "$out" ] || { echo "$out" >&2; bad=1; }
    [ "$bad" = 0 ] && echo "версия $want — везде одинаковая"
    exit "$bad"
    ;;
  set)
    v=${2:?scripts/version.sh set ВЕРСИЯ}
    [[ "$v" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]] || { echo "версия: неверный формат $v" >&2; exit 1; }
    for p in "${PLACES[@]}"; do
      file=${p%%|*} re=${p#*|}
      [ -n "$(current "$file" "$re")" ] || { echo "версия: в $file не найдена строка с версией" >&2; exit 1; }
      sed -E -i.bak "s#$re#\\1$v\\3#" "$file" && rm -f "$file.bak"
    done
    locks set "$v"
    echo "версия $v записана"
    ;;
  *) echo "scripts/version.sh check | set ВЕРСИЯ" >&2; exit 2 ;;
esac
