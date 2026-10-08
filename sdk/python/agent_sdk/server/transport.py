"""Транспорт ``Agents``: регистрация агента, HTTP sync, WebSocket, файлы задач, раздача релизов.

Логика сессии — в ``Agents`` (agents.py); здесь — приём и доставка конвертов.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import inspect
import ipaddress
import json
import os
import re
import secrets
import time
from typing import Any, Callable, Dict, Optional, Tuple

from ..message import (
    ENROLL_PATH, INSTALL_PATH, RELEASES_PATH, Close, MessageError, decode, encode, new_id, now_ms,
)
from .model import Agent
from .session import HttpReply, Reply, Session

#: hello — не позже этого после подключения WebSocket, с.
HELLO_TIMEOUT = 10.0
#: Наибольшее ожидание доставок в HTTP sync, с.
SYNC_MAX_WAIT = 25.0
#: Как часто проверять ушедшего клиента HTTP sync, с.
SYNC_DISCONNECT_POLL = 0.5
#: Адрес сервера для install.sh (попадает в "…" в shell) — только такой, иначе 400.
_SAFE_SERVER = re.compile(r"https?://[A-Za-z0-9.\-:\[\]]+(/.*)?")
#: Символы, опасные внутри "…" в shell.
_SHELL_UNSAFE = re.compile(r'["$`\\\n\r]')
#: Ключ проверки релизов — только алфавит base64, иначе не подставляется.
_SAFE_KEY = re.compile(r"[A-Za-z0-9+/=]*")
#: Предел тела запроса регистрации, байт.
ENROLL_MAX_BODY = 64 << 10
#: Имя агента при регистрации — не длиннее, символов.
ENROLL_MAX_NAME = 128
#: Меток при регистрации — не больше.
ENROLL_MAX_LABELS = 64
#: Ключ и значение метки — не длиннее, символов.
ENROLL_MAX_LABEL = 256


class Transport:
    """Методы транспорта ``Agents`` (примесь: состояние и логика сессии — в ``Agents``)."""

    # ── регистрация и учётные данные (§5) ───────────────────────────────

    def body_limit(self, path: str) -> int:
        """Предел тела запроса для пути, байт: регистрация — 64 КБ, остальное — ``max_body``.
        Транспорт читает тело не больше предела; ``Content-Length`` больше — сразу 413."""
        if path.split("?", 1)[0] == ENROLL_PATH:
            return min(ENROLL_MAX_BODY, self.max_body)
        return self.max_body

    def request_base(self, host: Optional[str], *, secure: bool = False, forwarded_host: Optional[str] = None,
                     forwarded_proto: Optional[str] = None) -> str:
        """Адрес сервера, по которому до него дошёл клиент (ссылки на файлы, ``install.sh``):
        ``host`` — заголовок ``Host``, ``secure`` — соединение по TLS. ``X-Forwarded-Host`` и
        ``X-Forwarded-Proto`` учитываются только с ``trust_proxy``. Нет адреса — пусто."""
        scheme = "https" if secure else "http"
        if self.trust_proxy:
            proto = (forwarded_proto or "").split(",", 1)[0].strip().lower()
            if proto in ("http", "https"):
                scheme = proto
            forwarded = (forwarded_host or "").split(",", 1)[0].strip()
            if forwarded:
                host = forwarded
        return f"{scheme}://{host}" if host else ""

    async def handle_enroll(self, body: Any, remote: Optional[str] = None, *,
                            forwarded_for: Optional[str] = None) -> HttpReply:
        """``POST /api/v1/agent-link/enroll``: токен → ``201 {agentId, secret}``.

        ``remote`` — адрес клиента, ``forwarded_for`` — заголовок ``X-Forwarded-For`` (берётся
        только с ``trust_proxy``); адрес — как ``Agent.address``, транспорт не знает его — все
        клиенты один ``"*"``. Неудачные попытки (неверный токен или запрос) считаются по нему.
        Больше ``enroll_failure_limit`` за ``enroll_failure_window_ms`` — ``429 ENROLL_RATE_LIMITED``
        (даже с верным токеном) до конца окна. Ответ — ``HttpReply``: пара ``(статус, тело)`` и
        ``reply.headers``; у 429 там ``Retry-After`` (секунды) — бэкенд передаёт его заголовком ответа.

        Тело — не больше 64 КБ (иначе ``413 MESSAGE_INVALID``); ``token`` и ``name`` — непустые
        строки, ``name`` — до 128 символов, ``labels`` — объект до 64 меток (ключ — непустая строка
        до 256 символов, значение — строка до 256), иначе ``400 MESSAGE_INVALID``. Хук ``enroll``
        получает ``(token, {"name", "labels", "host"})``.
        """
        client = self._client_address(remote, forwarded_for) or "*"
        now = now_ms()
        retry = self._enroll_rate_limited(client, now)
        if retry is not None:
            return retry
        if isinstance(body, (bytes, bytearray, str)) and len(body) > self.body_limit(ENROLL_PATH):
            return self._enroll_too_large(client, now)
        try:
            req = _parse_body(body)
            token, name, labels = _enroll_request(req)
        except ValueError as err:
            return self._enroll_invalid(client, now, str(err))
        given: Dict[str, str] = {}
        if self._enroll is not None:
            granted = self._enroll(token, {"name": name, "labels": dict(labels), "host": req.get("host")})
            if inspect.isawaitable(granted):
                granted = await granted
            if not granted and granted != {}:
                return self._enroll_failed(client, now)
            if isinstance(granted, dict):
                given = {str(k): str(v) for k, v in (granted.get("labels") or {}).items()}
        elif not hmac.compare_digest(token.encode(), str(self._enroll_token).encode()):
            return self._enroll_failed(client, now)
        secret = secrets.token_hex(24)
        agent = Agent(id=new_id(), name=name, labels={**labels, **given}, granted_labels=given,
                      enrolled_at=now_ms(), secret_hash=_hash(secret))
        async with self._op():
            await self.store.create_agent(agent)
            self._changed("agent", agent.id)
        self._log("агент зарегистрирован", agent=name, id=agent.id)
        return HttpReply(201, {"agentId": agent.id, "secret": secret})

    def enroll_too_large(self, remote: Optional[str] = None, *, forwarded_for: Optional[str] = None) -> HttpReply:
        """Ответ на запрос регистрации с телом больше ``body_limit`` (транспорт его не дочитал):
        ``413 MESSAGE_INVALID``, в счёт неудач клиента (или ``429``, если их уже слишком много)."""
        client = self._client_address(remote, forwarded_for) or "*"
        now = now_ms()
        retry = self._enroll_rate_limited(client, now)
        return retry if retry is not None else self._enroll_too_large(client, now)

    def _enroll_too_large(self, client: str, now: int) -> HttpReply:
        self._count_enroll_failure(client, now)
        return HttpReply(413, {"code": "MESSAGE_INVALID", "message": "Слишком большой запрос регистрации"})

    def _enroll_rate_limited(self, client: str, now: int) -> Optional[HttpReply]:
        retry_ms = self._enroll_blocked(client, now)
        if retry_ms <= 0:
            return None
        secs = max(1, -(-retry_ms // 1000))
        return HttpReply(429, {"code": "ENROLL_RATE_LIMITED",
                               "message": f"Слишком много неудачных регистраций: повторите через {secs} с"},
                         {"Retry-After": str(secs)})

    def _enroll_blocked(self, client: str, now: int) -> int:
        """Сколько мс клиенту ещё ждать (0 — можно); заодно забыть устаревшие попытки."""
        window = self.enroll_failure_window_ms
        if self.enroll_failure_limit <= 0:
            return 0
        for key in [k for k, times in self._enroll_failures.items() if times[-1] <= now - window]:
            del self._enroll_failures[key]
        times = [t for t in self._enroll_failures.get(client) or () if t > now - window]
        if times:
            self._enroll_failures[client] = times
        if len(times) < self.enroll_failure_limit:
            return 0
        # Окно отсчитывается от попытки, после которой стало меньше лимита.
        return times[len(times) - self.enroll_failure_limit] + window - now

    def _enroll_failed(self, client: str, now: int) -> HttpReply:
        self._count_enroll_failure(client, now)
        return HttpReply(401, {"code": "AGENT_ENROLLMENT_TOKEN_INVALID", "message": "Токен регистрации неверен"})

    def _enroll_invalid(self, client: str, now: int, message: str) -> HttpReply:
        """Некорректный запрос регистрации — тоже неудачная попытка (как в Go и Node)."""
        self._count_enroll_failure(client, now)
        return HttpReply(400, {"code": "MESSAGE_INVALID", "message": message})

    def _count_enroll_failure(self, client: str, now: int) -> None:
        if self.enroll_failure_limit > 0:
            self._enroll_failures.setdefault(client, []).append(now)
            self._warn("неудачная регистрация", client=client)

    async def authenticate(self, authorization: Optional[str]) -> Optional[Agent]:
        """Агент по заголовку ``Authorization: Agent <id>.<secret>``; неверно — ``None``.

        Секрет подходит к основному хешу или к ожидающему (после ``agent.rotateKey``); вход с
        ожидающим делает его основным (старый секрет больше не принимается)."""
        if not authorization or not authorization.startswith("Agent "):
            return None
        agent_id, dot, secret = authorization[6:].strip().partition(".")
        if not dot or not agent_id or not secret:
            return None
        agent = await self.store.get_agent(agent_id)
        if agent is None or not agent.secret_hash or agent.revoked:
            return None
        hashed = _hash(secret)
        if hmac.compare_digest(hashed, agent.secret_hash):
            return agent
        if not agent.pending_secret_hash or not hmac.compare_digest(hashed, agent.pending_secret_hash):
            return None
        async with self._op():
            def promote(rec: Agent) -> bool:
                if rec.revoked or not rec.pending_secret_hash \
                        or not hmac.compare_digest(hashed, rec.pending_secret_hash):
                    return False
                rec.secret_hash, rec.pending_secret_hash = rec.pending_secret_hash, ""
                return True

            promoted = await self._mutate_agent(agent_id, promote)
            if promoted is not None:
                self._log("агент вошёл с новым ключом", agent=promoted.name)
                return promoted
            agent = await self.store.get_agent(agent_id)
            if agent is None or agent.revoked or not hmac.compare_digest(hashed, agent.secret_hash):
                return None
            return agent

    # ── HTTP sync (§2.2) ────────────────────────────────────────────────

    async def handle_sync(self, authorization: Optional[str], body: Any,
                          is_disconnected: Optional[Callable[[], Any]] = None, *, base_url: str = "",
                          remote: Optional[str] = None, forwarded_for: Optional[str] = None) -> Reply:
        """``POST /api/v1/agent-link/sync``: пачка сообщений агента → ``(статус, ответ)``.

        ``remote`` — адрес клиента, ``forwarded_for`` — заголовок ``X-Forwarded-For`` (берётся
        только с опцией ``trust_proxy``): адрес попадает в ``Agent.address`` при начале сессии.

        Доставлять нечего — ждёт до ``waitSeconds`` (≤ 25 с). ``is_disconnected()`` —
        клиент ушёл (функция или корутина): тогда доставки остаются в сессии, ответ —
        ``499`` (отправлять его некому).
        """
        agent = await self.authenticate(authorization)
        if agent is None:
            return 401, {"code": "AGENT_CREDENTIALS_INVALID", "message": "Неверные учётные данные"}
        try:
            req = _parse_body(body)
            messages = req.get("messages") or []
            if not isinstance(messages, list):
                raise ValueError("messages — не список")
            messages = [_check(m) for m in messages]
            wait = min(float(req.get("waitSeconds") or 0), SYNC_MAX_WAIT)
        except (ValueError, TypeError, MessageError) as err:
            return 400, {"code": "MESSAGE_INVALID", "message": str(err)}
        session_id = req.get("sessionId")

        async with self._op():
            if session_id is None:
                if not messages or messages[0]["type"] != "hello":
                    return 400, {"code": "AGENT_HELLO_REQUIRED", "message": "Первое сообщение — hello"}
                ss = Session(agent, "http", base_url or self.base_url)
                ss.address = self._client_address(remote, forwarded_for)
                await self._open(ss, messages[0])
                messages = messages[1:]
                if ss.closed:
                    return _closed_reply(ss)
            else:
                cur = self._sessions.get(agent.id)
                if cur is None or cur.id != session_id or cur.closed:
                    code = "AGENT_SESSION_REPLACED" if cur is not None and cur.id != session_id \
                        else "AGENT_SESSION_EXPIRED"
                    return 409, {"code": code, "message": "Сессия недействительна"}
                ss = cur
            ss.active += 1
            ss.touched = time.monotonic()
        try:
            if messages:
                async with self._op():
                    for env in messages:
                        await self._handle(ss, env)
            loop = asyncio.get_running_loop()
            deadline = loop.time() + max(0.0, wait)
            while not ss.outq and not ss.closed:
                left = deadline - loop.time()
                if left <= 0:
                    break
                await ss.wait(min(left, SYNC_DISCONNECT_POLL) if is_disconnected else left)
                if is_disconnected is not None and await _gone(is_disconnected):
                    break
            # Клиент ушёл — доставки не забирать: ответ до него не дойдёт.
            if is_disconnected is not None and await _gone(is_disconnected):
                return 499, {"code": "CLIENT_GONE", "message": "Клиент ушёл"}
            if ss.closed and ss.code == Close.RESTART and ss.outq:
                # Сессия закрыта после ответа (смена ключа): ack — этим ответом, следующий запрос
                # получит AGENT_SESSION_EXPIRED, и агент начнёт новую сессию.
                return 200, {"sessionId": ss.id, "messages": ss.take()}
            if ss.closed:
                return _closed_reply(ss)
            return 200, {"sessionId": ss.id, "messages": ss.take()}
        finally:
            ss.active -= 1
            ss.touched = time.monotonic()

    # ── WebSocket (§2.1) ────────────────────────────────────────────────

    async def serve_websocket(self, authorization: Optional[str], conn: Any, *, base_url: str = "",
                              remote: Optional[str] = None, forwarded_for: Optional[str] = None) -> None:
        """Сессия по принятому WebSocket (канал ``agent.v1``).

        ``remote`` — адрес клиента, ``forwarded_for`` — заголовок ``X-Forwarded-For`` запроса upgrade
        (берётся только с опцией ``trust_proxy``): адрес попадает в ``Agent.address``.

        ``conn`` — объект с ``async receive_text()``, ``async send_text(str)``,
        ``async close(code)`` (Starlette/FastAPI ``WebSocket``; для библиотеки
        ``websockets`` — ``agent_sdk.server.websockets_adapter``). Учётные данные
        проверяются до upgrade (``authenticate``); здесь неверные — закрытие 4401.
        Ping — забота веб-сервера (uvicorn, ``websockets``: ``ping_interval=20``).
        Возвращается, когда сессия закончилась.
        """
        self._start()
        agent = await self.authenticate(authorization)
        if agent is None:
            with contextlib.suppress(Exception):
                await conn.close(Close.UNAUTHORIZED)
            return
        ss = Session(agent, "ws", base_url or self.base_url)
        ss.address = self._client_address(remote, forwarded_for)
        sender = asyncio.ensure_future(self._ws_sender(ss, conn))
        try:
            try:
                raw = await asyncio.wait_for(conn.receive_text(), HELLO_TIMEOUT)
                env = decode(raw)
            except (asyncio.TimeoutError, MessageError):
                ss.close(Close.INVALID)
                return
            if env["type"] != "hello":
                ss.close(Close.INVALID)
                return
            async with self._op():
                await self._open(ss, env)
            while not ss.closed:
                raw = await conn.receive_text()
                try:
                    env = decode(raw)
                except MessageError:
                    ss.close(Close.INVALID)
                    return
                async with self._op():
                    await self._handle(ss, env)
        except Exception:  # noqa: BLE001 — соединение закрыто или оборвалось
            pass
        finally:
            async with self._op():
                await self._closed_session(ss)
            try:
                await asyncio.wait_for(sender, 5)
            except Exception:  # noqa: BLE001
                sender.cancel()

    async def _ws_sender(self, ss: Session, conn: Any) -> None:
        """Исходящее сессии — в сокет по мере появления; закрытие — с кодом (4409, 4410…)."""
        try:
            while True:
                for env in ss.take():
                    await conn.send_text(encode(env))
                if ss.closed:
                    await conn.close(ss.code or Close.NORMAL)
                    return
                await ss.wait(None)
        except Exception:  # noqa: BLE001 — соединения уже нет
            ss.close(Close.NORMAL)

    def _client_address(self, remote: Optional[str], forwarded_for: Optional[str]) -> str:
        """Адрес клиента без порта: с ``trust_proxy`` — первый из ``X-Forwarded-For``, иначе ``remote``."""
        if self.trust_proxy and forwarded_for:
            first = _host(forwarded_for.split(",", 1)[0])
            if first:
                return first
        return _host(remote or "")

    # ── выпуск агента (§7) ──────────────────────────────────────────────

    async def handle_release(self, path: str, base_url: str = "") -> Tuple[int, bytes, str]:
        """Публичная раздача выпуска из ``releases_dir`` → ``(статус, тело, Content-Type)``.

        ``GET /api/v1/agent-link/releases/manifest.json`` — манифест;
        ``GET /api/v1/agent-link/releases/<file>`` — сборка агента или воркера (только файлы из манифеста);
        ``GET /api/v1/agent-link/install.sh`` — установщик: ``DEFAULT_SERVER=""`` и
        ``DEFAULT_PUBLIC_KEY=""`` заменяются на адрес и ``public_key``. Адрес — опция
        ``base_url`` из ``Agents``, если задана, иначе ``base_url`` из запроса; адрес не
        ``http(s)://хост[:порт][/путь]`` или с символами shell — ``400 MESSAGE_INVALID``;
        ключ не base64 — не подставляется (предупреждение в лог).
        """
        path = path.split("?", 1)[0]
        if not self.releases_dir:
            return _not_found("Выпуск агента не настроен")
        if path == INSTALL_PATH:
            data = await _read(os.path.join(self.releases_dir, "install.sh"))
            if data is None:
                return _not_found("install.sh нет в каталоге выпуска")
            server = (self.base_url or base_url).rstrip("/")
            if not _SAFE_SERVER.fullmatch(server) or _SHELL_UNSAFE.search(server):
                body = json.dumps({"code": "MESSAGE_INVALID", "message": "Некорректный адрес сервера"},
                                  ensure_ascii=False).encode()
                return 400, body, "application/json; charset=utf-8"
            key = self.public_key
            if not _SAFE_KEY.fullmatch(key):
                self._log("public_key не base64: в install.sh не подставлен")
                key = ""
            text = data.decode("utf-8")
            text = re.sub(r'(?m)^DEFAULT_SERVER=""', lambda _: f'DEFAULT_SERVER="{server}"', text)
            text = re.sub(r'(?m)^DEFAULT_PUBLIC_KEY=""', lambda _: f'DEFAULT_PUBLIC_KEY="{key}"', text)
            return 200, text.encode("utf-8"), "text/x-shellscript; charset=utf-8"
        prefix = RELEASES_PATH + "/"
        if not path.startswith(prefix):
            return _not_found("Нет такого пути")
        name = path[len(prefix):]
        manifest = self._manifest()
        if manifest is None:
            return _not_found("Манифеста выпуска нет")
        if name == "manifest.json":
            data = await _read(os.path.join(self.releases_dir, name))
            return (200, data, "application/json") if data is not None else _not_found("Манифеста выпуска нет")
        # Только файлы из манифеста и только внутри каталога выпуска.
        files = {a.get("file") for a in manifest["artifacts"] + manifest.get("workers", [])}
        root = os.path.realpath(self.releases_dir)
        file = os.path.realpath(os.path.join(root, name))
        if name not in files or "/" in name or "\\" in name or os.path.dirname(file) != root:
            return _not_found("Нет такой сборки")
        data = await _read(file)
        if data is None:
            return _not_found("Нет такой сборки")
        return 200, data, "application/octet-stream"

    # ── файлы ───────────────────────────────────────────────────────────

    async def handle_file(self, method: str, key: str, body: bytes = b"") -> Tuple[int, bytes]:
        """``GET``/``PUT /files/<key>`` (ключ ``<jobId>/(in|out)/<имя>``) → ``(статус, тело)``."""
        status, data = await self.files.handle(method.upper(), key, body)
        if method.upper() == "PUT" and status < 300:
            async with self._op():
                self._changed("job", key.split("/", 1)[0])
        return status, data


async def _read(path: str) -> Optional[bytes]:
    """Файл целиком (в потоке: сборка агента — десятки МБ); нет файла — ``None``."""
    def read() -> Optional[bytes]:
        try:
            with open(path, "rb") as f:
                return f.read()
        except OSError:
            return None

    return await asyncio.get_running_loop().run_in_executor(None, read)


def _not_found(message: str) -> Tuple[int, bytes, str]:
    body = json.dumps({"code": "NOT_FOUND", "message": message}, ensure_ascii=False).encode()
    return 404, body, "application/json; charset=utf-8"


def _host(value: str) -> str:
    """``1.2.3.4:5678`` → ``1.2.3.4``, ``[::1]:80`` → ``::1``; IP без порта и прочее — как есть."""
    value = value.strip()
    if not value:
        return ""
    if value.startswith("["):
        end = value.find("]")
        return value[1:end] if end > 0 else value
    try:
        ipaddress.ip_address(value)
        return value
    except ValueError:
        pass
    host, sep, port = value.rpartition(":")
    if sep and host and port.isdigit() and ":" not in host:
        return host
    return value


def _hash(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def _parse_body(body: Any) -> Dict[str, Any]:
    if isinstance(body, dict):
        return body
    if body is None or body in (b"", ""):
        return {}
    try:
        parsed = json.loads(body)
    except (TypeError, ValueError) as err:
        raise ValueError(f"тело — не JSON: {err}") from None
    if not isinstance(parsed, dict):
        raise ValueError("тело — не объект JSON")
    return parsed


def _enroll_request(req: Dict[str, Any]) -> Tuple[str, str, Dict[str, str]]:
    """Проверенные ``token``, ``name`` и ``labels`` запроса регистрации; неверно — ``ValueError``."""
    token, name, labels = req.get("token"), req.get("name"), req.get("labels")
    if not isinstance(token, str) or not token or not isinstance(name, str) or not name:
        raise ValueError("Нужны token и name (непустые строки)")
    if len(name) > ENROLL_MAX_NAME:
        raise ValueError(f"name длиннее {ENROLL_MAX_NAME} символов")
    if labels is None:
        return token, name, {}
    if not isinstance(labels, dict):
        raise ValueError("labels — объект")
    if len(labels) > ENROLL_MAX_LABELS:
        raise ValueError(f"Меток больше {ENROLL_MAX_LABELS}")
    for key, value in labels.items():
        if not isinstance(key, str) or not key or len(key) > ENROLL_MAX_LABEL:
            raise ValueError(f"Ключ метки — непустая строка до {ENROLL_MAX_LABEL} символов")
        if not isinstance(value, str) or len(value) > ENROLL_MAX_LABEL:
            raise ValueError(f"Значение метки {key!r} — строка до {ENROLL_MAX_LABEL} символов")
    return token, name, dict(labels)


def _check(env: Dict[str, Any]) -> Dict[str, Any]:
    return decode(json.dumps(env))


def _closed_reply(ss: Session) -> Reply:
    if ss.code == Close.REPLACED:
        return 409, {"code": "AGENT_SESSION_REPLACED", "message": "Сессию вытеснила другая"}
    if ss.code == Close.UNSUPPORTED:
        return 409, {"code": "AGENT_VERSION_UNSUPPORTED", "message": "Нет общей версии формата сообщений"}
    if ss.code == Close.INVALID:
        return 400, {"code": "MESSAGE_INVALID", "message": "Некорректное hello"}
    if ss.code == Close.UNAUTHORIZED:
        return 401, {"code": "AGENT_CREDENTIALS_INVALID", "message": "Агент не найден"}
    return 409, {"code": "AGENT_SESSION_EXPIRED", "message": "Сессия закрыта"}


async def _gone(is_disconnected: Callable[[], Any]) -> bool:
    result = is_disconnected()
    if inspect.isawaitable(result):
        result = await result
    return bool(result)

