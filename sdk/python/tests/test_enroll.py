"""Регистрация: пределы запроса, проверки, хук, адрес клиента; адрес сервера и предел тела."""

from __future__ import annotations

import json
import unittest
from typing import Any, Dict, List

from agent_sdk.message import ENROLL_PATH, SYNC_PATH
from agent_sdk.server import Agents

from tests.server_helpers import TOKEN


class EnrollLimitsTest(unittest.IsolatedAsyncioTestCase):
    async def test_body_and_fields(self) -> None:
        agents = Agents(enroll_token=TOKEN, enroll_failure_limit=0)
        big = json.dumps({"token": TOKEN, "name": "x", "pad": "a" * (64 << 10)}).encode()
        status, body = await agents.handle_enroll(big)
        self.assertEqual((status, body["code"]), (413, "MESSAGE_INVALID"))

        async def code(req: Any) -> Any:
            status, body = await agents.handle_enroll(req)
            return status if status == 201 else (status, body["code"])

        self.assertEqual(await code({"token": TOKEN, "name": "x" * 128}), 201)
        bad = (400, "MESSAGE_INVALID")
        self.assertEqual(await code({"token": TOKEN, "name": "x" * 129}), bad)
        self.assertEqual(await code({"token": TOKEN, "name": 5}), bad)
        self.assertEqual(await code({"token": 1, "name": "x"}), bad)
        self.assertEqual(await code({"token": TOKEN, "name": "x", "labels": []}), bad)
        self.assertEqual(await code({"token": TOKEN, "name": "x", "labels": {str(i): "v" for i in range(65)}}), bad)
        self.assertEqual(await code({"token": TOKEN, "name": "x", "labels": {str(i): "v" for i in range(64)}}), 201)
        self.assertEqual(await code({"token": TOKEN, "name": "x", "labels": {"": "v"}}), bad)
        self.assertEqual(await code({"token": TOKEN, "name": "x", "labels": {"k" * 257: "v"}}), bad)
        self.assertEqual(await code({"token": TOKEN, "name": "x", "labels": {"k": "v" * 257}}), bad)
        self.assertEqual(await code({"token": TOKEN, "name": "x", "labels": {"k": 1}}), bad)
        await agents.close()

    async def test_too_large_counts_as_failure(self) -> None:
        agents = Agents(enroll_token=TOKEN, enroll_failure_limit=1)
        status, _ = agents.enroll_too_large("10.0.0.1")
        self.assertEqual(status, 413)
        status, body = await agents.handle_enroll({"token": TOKEN, "name": "x"}, "10.0.0.1")
        self.assertEqual((status, body["code"]), (429, "ENROLL_RATE_LIMITED"))
        await agents.close()

    async def test_hook_gets_name_labels_host(self) -> None:
        seen: List[Dict[str, Any]] = []

        def check(token: str, info: Dict[str, Any]) -> Any:
            seen.append({"token": token, **info})
            return {"labels": {"zone": "eu"}}

        agents = Agents(enroll=check)
        host = {"hostname": "n1", "os": "linux", "arch": "amd64"}
        status, body = await agents.handle_enroll({"token": "t", "name": "n1", "labels": {"disk": "ssd"},
                                                   "host": host})
        self.assertEqual(status, 201, body)
        self.assertEqual(seen, [{"token": "t", "name": "n1", "labels": {"disk": "ssd"}, "host": host}])
        await agents.close()

    async def test_failures_by_forwarded_for_only_with_trust_proxy(self) -> None:
        for trust in (False, True):
            agents = Agents(enroll_token=TOKEN, enroll_failure_limit=1, trust_proxy=trust)
            status, _ = await agents.handle_enroll({"token": "bad", "name": "x"}, "10.0.0.1",
                                                   forwarded_for="1.1.1.1, 10.0.0.9")
            self.assertEqual(status, 401)
            status, _ = await agents.handle_enroll({"token": TOKEN, "name": "x"}, "10.0.0.1",
                                                   forwarded_for="2.2.2.2")
            self.assertEqual(status, 201 if trust else 429, f"trust_proxy={trust}")
            status, _ = await agents.handle_enroll({"token": TOKEN, "name": "x"}, "10.0.0.1",
                                                   forwarded_for="1.1.1.1:5000")
            self.assertEqual(status, 429)
            await agents.close()


class RequestBaseTest(unittest.TestCase):
    def test_forwarded_only_with_trust_proxy(self) -> None:
        plain = Agents(enroll_token=TOKEN)
        proxied = Agents(enroll_token=TOKEN, trust_proxy=True)
        fwd = {"forwarded_host": "example.org, proxy.local", "forwarded_proto": "https"}
        self.assertEqual(plain.request_base("app:8080", **fwd), "http://app:8080")
        self.assertEqual(plain.request_base("app:8080", secure=True, **fwd), "https://app:8080")
        self.assertEqual(proxied.request_base("app:8080", **fwd), "https://example.org")
        self.assertEqual(proxied.request_base("app:8080"), "http://app:8080")
        self.assertEqual(proxied.request_base("app", forwarded_proto="javascript"), "http://app")
        self.assertEqual(plain.request_base(None), "")

    def test_body_limit(self) -> None:
        agents = Agents(enroll_token=TOKEN)
        self.assertEqual(agents.max_body, 32 << 20)
        self.assertEqual(agents.body_limit(ENROLL_PATH), 64 << 10)
        self.assertEqual(agents.body_limit(ENROLL_PATH + "?x=1"), 64 << 10)
        self.assertEqual(agents.body_limit(SYNC_PATH), 32 << 20)
        self.assertEqual(Agents(enroll_token=TOKEN, max_body=1000).body_limit(SYNC_PATH), 1000)


if __name__ == "__main__":
    unittest.main()
