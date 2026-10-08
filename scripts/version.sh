#!/usr/bin/env bash
# Версия проекта — одна на агента, все SDK и примеры. Источник — файл VERSION.
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
  "sdk/python/pyproject.toml|^(version = \")([^\"]+)(\")$"
  "sdk/python/agent_sdk/__init__.py|^(__version__ = \")([^\"]+)(\")$"
  "sdk/go/worker/worker.go|^(const SDKVersion = \")([^\"]+)(\")$"
  "internal/app/app.go|^(const SDK = \"go/)([^\"]+)(\")$"
  "examples/server/package.json|^(  \"version\": \")([^\"]+)(\",)$"
  "examples/web/package.json|^(  \"version\": \")([^\"]+)(\",)$"
  "examples/workers/node/package.json|^(  \"version\": \")([^\"]+)(\",)$"
)

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
    echo "версия $v записана"
    ;;
  *) echo "scripts/version.sh check | set ВЕРСИЯ" >&2; exit 2 ;;
esac
