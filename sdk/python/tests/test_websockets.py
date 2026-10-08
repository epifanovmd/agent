"""Адаптер к библиотеке websockets (тест пропускается, если она не установлена)."""

from __future__ import annotations

import importlib.util
import json
import unittest

from agent_sdk.server import Agents

from tests.server_helpers import TOKEN, enroll, hello

HAVE_WEBSOCKETS = importlib.util.find_spec("websockets") is not None


@unittest.skipUnless(HAVE_WEBSOCKETS, "websockets не установлена")
class WebsocketsAdapterTest(unittest.IsolatedAsyncioTestCase):
    async def test_upgrade_auth_and_hello(self) -> None:
        from websockets.asyncio.client import connect
        from websockets.exceptions import InvalidStatus

        from agent_sdk.server.websockets_adapter import serve

        agents = Agents(enroll_token=TOKEN)
        server = await serve(agents, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        url = f"ws://127.0.0.1:{port}/api/v1/agent-link"
        try:
            agent_id, secret = await enroll(agents)
            with self.assertRaises(InvalidStatus) as ctx:
                await connect(url, subprotocols=["agent.v1"], additional_headers={"Authorization": "Agent x.y"})
            self.assertEqual(ctx.exception.response.status_code, 401)

            async with connect(url, subprotocols=["agent.v1"],
                               additional_headers={"Authorization": f"Agent {agent_id}.{secret}"}) as ws:
                self.assertEqual(ws.subprotocol, "agent.v1")
                await ws.send(json.dumps(hello()))
                welcome = json.loads(await ws.recv())
                self.assertEqual((welcome["type"], welcome["data"]["agentId"]), ("welcome", agent_id))
                info = await agents.get_agent(agent_id)
                assert info is not None
                self.assertEqual((info.online, info.transport, info.address), (True, "ws", "127.0.0.1"))
        finally:
            await agents.close()
            server.close()
            await server.wait_closed()


if __name__ == "__main__":
    unittest.main()
