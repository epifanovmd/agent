#!/usr/bin/env bash
# Версия проекта — файл VERSION; агент получает её при сборке (-X main.version, scripts/go.sh и
# scripts/release.sh).
#   scripts/version.sh check        # VERSION — версия вида 1.2.0 (код выхода 1 — нет)
#   scripts/version.sh set 1.2.0    # записать версию
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
RE='^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$'

case "${1:-check}" in
  check)
    v=$(cat VERSION)
    [[ "$v" =~ $RE ]] || { echo "версия: в VERSION неверный формат: $v" >&2; exit 1; }
    echo "версия $v"
    ;;
  set)
    v=${2:?scripts/version.sh set ВЕРСИЯ}
    [[ "$v" =~ $RE ]] || { echo "версия: неверный формат $v" >&2; exit 1; }
    echo "$v" >VERSION
    echo "версия $v записана"
    ;;
  *) echo "scripts/version.sh check | set ВЕРСИЯ" >&2; exit 2 ;;
esac
