import os
import asyncio

from httpx import AsyncClient, HTTPError


class _Message:
    def __init__(self, receiver: str, message: str) -> None:
        self.sender = os.environ["SENDER"]
        self.receiver = receiver
        self.message = message


class MessageBrokerClient:
    def __init__(self, token: str) -> None:
        self._message_broker = AsyncClient(
            base_url="http://node-message-broker",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}
        )
        asyncio.run(self._connect())

    async def get_self_config(self, analysis_id: str) -> dict[str, str]:
        response = await self._message_broker.get(f'/analyses/{analysis_id}/participants/self',
                                                  headers=[('Connection', 'close')])
        response.raise_for_status()
        return response.json()

    async def get_partner_nodes(self, self_node_id: str, analysis_id: str) -> list[dict[str, str]]:
        response = await self._message_broker.get(f'/analyses/{analysis_id}/participants',
                                                  headers=[('Connection', 'close')])

        response.raise_for_status()

        response = [node_conf for node_conf in response.json() if node_conf['nodeId'] != self_node_id]
        return response

    async def test_connection(self) -> bool:
        response = await self._message_broker.get("/healthz",
                                                  headers=[('Connection', 'close')])
        try:
            response.raise_for_status()
            return True
        except HTTPError:
            return False

    async def _ask_central_analyzer_or_aggregator(self) -> str:
        message = _Message(receiver="central_analyses_management", message="status_aggregator_or_analyzer")
        self._send(message)
        answer = await self._receive()
        return answer['nodeType']

    async def _connect(self) -> None:
        await self._message_broker.post(
            f'/analyses/{os.getenv("ANALYSIS_ID")}/messages/subscription',
            json={'webhookUrl': f'/po/{os.getenv("DEPLOYMENT_NAME")}'}
        )
        response = await self._message_broker.get(f'/analyses/{os.getenv("ANALYSIS_ID")}/participants/self',
                                                  headers=[('Connection', 'close')])
        response.raise_for_status()

    def _send(self, message: _Message):
        self._message_broker.send(message)

    def _receive(self) -> _Message:
        return self._message_broker.receive()

