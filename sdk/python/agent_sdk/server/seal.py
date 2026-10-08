"""Запечатанные значения состояния: ``{"$sealed": "v1.<eph>.<nonce>.<ct>"}``.

eph — одноразовый открытый ключ X25519, ключ шифра = HKDF-SHA256(ECDH(eph, ключ агента),
salt пусто, info ``agent sealed v1``), шифр AES-256-GCM (nonce 12 байт, AAD пусто, ct с тегом);
части — base64url без паддинга, открытый текст — JSON значения. Раскрывает только агент.

Нужен пакет ``cryptography`` (``pip install agent-sdk[crypto]``); без него — ``AgentsError
SEAL_NOT_AVAILABLE``.
"""

from __future__ import annotations

import base64
import json
import os
from typing import Any, Tuple

from .session import AgentsError

SEALED_PREFIX = "v1"
_INFO = b"agent sealed v1"
_NONCE_SIZE = 12


def _crypto() -> Tuple[Any, Any, Any, Any, Any]:
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        from cryptography.hazmat.primitives.kdf.hkdf import HKDF
    except ImportError:
        raise AgentsError("SEAL_NOT_AVAILABLE",
                          "Запечатывание требует пакета cryptography: pip install 'agent-sdk[crypto]'", 501) from None
    return (X25519PrivateKey, X25519PublicKey, AESGCM, HKDF, (hashes, serialization))


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def _unb64(s: str) -> bytes:
    """base64 или base64url, с паддингом или без."""
    s = s.strip().replace("-", "+").replace("_", "/")
    return base64.b64decode(s + "=" * (-len(s) % 4), validate=True)


def _derive(shared: bytes) -> bytes:
    _, _, _, HKDF, (hashes, _) = _crypto()
    return HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=_INFO).derive(shared)


def seal_value(public_key: str, value: Any) -> str:
    """Значение (JSON) → строка ``v1.…`` для открытого ключа агента (base64, 32 байта)."""
    X25519PrivateKey, X25519PublicKey, AESGCM, _, (_, serialization) = _crypto()
    try:
        raw = _unb64(public_key)
        if len(raw) != 32:
            raise ValueError("не 32 байта")
        peer = X25519PublicKey.from_public_bytes(raw)
    except ValueError as err:
        raise AgentsError("SEAL_NOT_AVAILABLE", f"Неверный ключ шифрования агента: {err}", 409) from None
    eph = X25519PrivateKey.generate()
    key = _derive(eph.exchange(peer))
    nonce = os.urandom(_NONCE_SIZE)
    plain = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
    ct = AESGCM(key).encrypt(nonce, plain, None)
    eph_pub = eph.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return ".".join((SEALED_PREFIX, _b64url(eph_pub), _b64url(nonce), _b64url(ct)))


def _unseal(private_key: str, sealed: str) -> Any:
    """Обратное ``seal_value`` закрытым ключом агента (base64, 32 байта) — для тестов совместимости."""
    X25519PrivateKey, X25519PublicKey, AESGCM, _, _ = _crypto()
    parts = sealed.split(".")
    if len(parts) != 4 or parts[0] != SEALED_PREFIX:
        raise ValueError("не запечатанное значение v1")
    eph, nonce, ct = (_unb64(p) for p in parts[1:])
    priv = X25519PrivateKey.from_private_bytes(_unb64(private_key))
    key = _derive(priv.exchange(X25519PublicKey.from_public_bytes(eph)))
    return json.loads(AESGCM(key).decrypt(nonce, ct, None))
