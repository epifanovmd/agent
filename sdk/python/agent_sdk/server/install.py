"""Команда установки агента на узел: ``curl … install.sh | sudo sh -s -- флаги``."""

from __future__ import annotations

import re
from typing import Dict, Iterable, List, Optional

from ..message import INSTALL_PATH, NAME_PATTERN, valid_name
from .session import AgentsError
from .transport import _SAFE_SERVER, _SHELL_UNSAFE

_PACKAGE = re.compile(r"[A-Za-z0-9.+_:-]+")
_SYSCTL_KEY = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_./-]*")
_QUOTE_OR_SPACE = re.compile(r"['\s]")
#: Менеджеры пакетов ``packages_by_manager`` — в этом порядке флаги ``--packages-<m>``.
PACKAGE_MANAGERS = ("apt", "dnf", "yum", "apk", "zypper")
KILL_MODES = ("process", "mixed")


def shell_quote(s: str) -> str:
    """Значение в одинарных кавычках POSIX (``'`` → ``'\\''``)."""
    return "'" + s.replace("'", "'\\''") + "'"


def _invalid(msg: str) -> AgentsError:
    return AgentsError("MESSAGE_INVALID", msg)


def install_command(*, token: str = "", base_url: str = "", name: str = "", privileged: bool = False,
                    packages: Optional[Iterable[str]] = None, sysctl: Optional[Dict[str, str]] = None,
                    rw_paths: Optional[Iterable[str]] = None, ca_file: str = "", stop_timeout: str = "",
                    user: str = "", workers: Optional[Iterable[str]] = None, token_file: str = "",
                    packages_by_manager: Optional[Dict[str, Iterable[str]]] = None, kill_mode: str = "",
                    config: str = "") -> str:
    """Команда установки; ``base_url`` уже с учётом опции ``Agents`` (см. ``Agents.install_command``)."""
    base = (base_url or "").rstrip("/")
    if not base:
        raise _invalid("Нужен адрес сервера (base_url)")
    if not _SAFE_SERVER.fullmatch(base) or _SHELL_UNSAFE.search(base) or _QUOTE_OR_SPACE.search(base):
        raise _invalid("Некорректный адрес сервера")
    if bool(token) == bool(token_file):
        raise _invalid("Нужен ровно один из token и token_file")
    if kill_mode and kill_mode not in KILL_MODES:
        raise _invalid(f"Некорректный kill_mode {shell_quote(str(kill_mode))}: нужно {'|'.join(KILL_MODES)}")
    by_manager = dict(packages_by_manager or {})
    for m in by_manager:
        if m not in PACKAGE_MANAGERS:
            raise _invalid(f"Неизвестный менеджер пакетов {shell_quote(str(m))}: нужно {'|'.join(PACKAGE_MANAGERS)}")
    parts: List[str] = [f"curl -fsSL {shell_quote(base + INSTALL_PATH)} | sudo sh -s --"]

    def flag(flag_name: str, value: Optional[str]) -> None:
        if value:
            parts.append(f"{flag_name} {shell_quote(value)}")

    def package_list(names: Iterable[str]) -> str:
        pkgs = list(names or [])
        for p in pkgs:
            if not isinstance(p, str) or not _PACKAGE.fullmatch(p):
                raise _invalid(f"Некорректное имя пакета {shell_quote(str(p))}")
        return " ".join(pkgs)

    flag("--token", token)
    flag("--token-file", token_file)
    flag("--name", name)
    flag("--user", user)
    flag("--config", config)
    if privileged:
        parts.append("--privileged")
    flag("--kill-mode", kill_mode)
    flag("--packages", package_list(packages or []))
    for m in PACKAGE_MANAGERS:
        if m in by_manager:
            flag(f"--packages-{m}", package_list(by_manager[m]))
    params = dict(sysctl or {})
    for k in params:
        if not _SYSCTL_KEY.fullmatch(k):
            raise _invalid(f"Некорректный ключ sysctl {shell_quote(k)}")
    for k in sorted(params):
        flag("--sysctl", f"{k}={params[k]}")
    for p in rw_paths or []:
        flag("--rw-path", p)
    flag("--ca-file", ca_file)
    for w in workers or []:
        if not valid_name(w):
            raise _invalid(f"Некорректное имя воркера {shell_quote(str(w))}: нужно {NAME_PATTERN}")
        flag("--worker", w)
    flag("--stop-timeout", stop_timeout)
    cmd = " ".join(parts)
    if "\n" in cmd or "\r" in cmd:
        raise _invalid("Перевод строки в значении")
    return cmd
