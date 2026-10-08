"""``agents.seal``: запечатанное значение раскрывается ключом агента; образец sdk/spec/examples/sealed.json.

Нужен пакет ``cryptography`` (extra ``crypto``); без него — проверяется только понятная ошибка.
"""

from __future__ import annotations

import base64
import json
import unittest
from typing import Any, Dict

from agent_sdk.server import Agents, AgentsError

from tests.examples import sealed
from tests.server_helpers import TOKEN, SyncAgent, enroll, hello

try:
    import cryptography  # noqa: F401
    HAVE_CRYPTO = True
except ImportError:
    HAVE_CRYPTO = False

def hello_with_key(public_key: str) -> Dict[str, Any]:
    msg = hello("a1")
    msg["data"]["agent"]["encryptionKey"] = public_key
    return msg


async def agent_with_key(agents: Agents, public_key: str) -> SyncAgent:
    agent_id, secret = await enroll(agents, "a1")
    agent = SyncAgent(agents, agent_id, secret)
    status, body = await agent.sync([hello_with_key(public_key)])
    assert status == 200, body
    return agent


class SealTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.agents = Agents(enroll_token=TOKEN)

    async def asyncTearDown(self) -> None:
        await self.agents.close()

    async def test_without_key_or_agent(self) -> None:
        agent = await SyncAgent.create(self.agents)
        with self.assertRaises(AgentsError) as err:
            await self.agents.seal(agent.id, "x")
        self.assertEqual(err.exception.code, "SEAL_NOT_AVAILABLE")
        with self.assertRaises(AgentsError) as err:
            await self.agents.seal("nope", "x")
        self.assertEqual(err.exception.code, "AGENT_NOT_FOUND")

    @unittest.skipIf(HAVE_CRYPTO, "пакет cryptography установлен")
    async def test_without_package(self) -> None:
        agent = await agent_with_key(self.agents, base64.b64encode(bytes(32)).decode())
        with self.assertRaises(AgentsError) as err:
            await self.agents.seal(agent.id, "x")
        self.assertEqual(err.exception.code, "SEAL_NOT_AVAILABLE")
        self.assertIn("cryptography", err.exception.message)

    @unittest.skipUnless(HAVE_CRYPTO, "нет пакета cryptography (pip install 'agent-sdk[crypto]')")
    async def test_seal_roundtrip(self) -> None:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey

        from agent_sdk.server.seal import _unseal

        priv = X25519PrivateKey.generate()
        raw = serialization.Encoding.Raw
        pub = base64.b64encode(priv.public_key().public_bytes(raw, serialization.PublicFormat.Raw)).decode()
        priv_b64 = base64.b64encode(priv.private_bytes(raw, serialization.PrivateFormat.Raw,
                                                       serialization.NoEncryption())).decode()
        agent = await agent_with_key(self.agents, pub)
        value = {"password": "секрет", "n": [1, 2]}
        sealed = await self.agents.seal(agent.id, value)
        self.assertEqual(list(sealed), ["$sealed"])
        parts = sealed["$sealed"].split(".")
        self.assertEqual(parts[0], "v1")
        self.assertTrue(all("=" not in p and "+" not in p and "/" not in p for p in parts), "base64url без паддинга")
        self.assertEqual(_unseal(priv_b64, sealed["$sealed"]), value)
        again = await self.agents.seal(agent.id, value)
        self.assertNotEqual(again, sealed, "одноразовый ключ и nonce")
        other = X25519PrivateKey.generate().private_bytes(raw, serialization.PrivateFormat.Raw,
                                                          serialization.NoEncryption())
        with self.assertRaises(Exception):
            _unseal(base64.b64encode(other).decode(), sealed["$sealed"])

    @unittest.skipUnless(HAVE_CRYPTO, "нет пакета cryptography (pip install 'agent-sdk[crypto]')")
    async def test_sealed_example(self) -> None:
        example = sealed()
        from agent_sdk.server.seal import _unseal

        private_key, public_key = example["agentKey"]["privateKey"], example["agentKey"]["publicKey"]
        for sample in example["samples"]:
            with self.subTest(value=sample["value"]):
                self.assertEqual(_unseal(private_key, sample["sealed"]["$sealed"]), sample["value"],
                                 "образец раскрывается")
        with self.assertRaises(Exception):
            _unseal(private_key, example["wrongKey"]["$sealed"])
        agent = await agent_with_key(self.agents, public_key)
        for sample in example["samples"]:
            mine = await self.agents.seal(agent.id, sample["value"])
            self.assertEqual(_unseal(private_key, mine["$sealed"]), sample["value"], "своё раскрывается")

if __name__ == "__main__":
    unittest.main()
