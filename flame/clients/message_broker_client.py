import os
import asyncio

from httpx import AsyncClient, HTTPError
from typing import Any


class Message:
    def __init__(self, recipients: list[str], message: Any) -> None:
        self.recipients = recipients
        self.message = message


class MessageBrokerClient:
    def __init__(self, token: str) -> None:
        self._message_broker = AsyncClient(
            base_url="http://flame-node-node-message-broker",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}
        )
        asyncio.run(self._connect())
        self.list_of_incoming_messages = []
        self.list_of_outgoing_messages = []

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
        message = Message(receiver="central_analyses_management", message="status_aggregator_or_analyzer")
        self._send(message)
        answer = await self._receive()
        return answer['nodeType']

    async def _connect(self) -> None:
        await self._message_broker.post(
            f'/analyses/{os.getenv("ANALYSIS_ID")}/messages/subscription',
            json={'webhookUrl': f'http://service-{os.getenv("DEPLOYMENT_NAME")}/webhook'}
        )
        response = await self._message_broker.get(f'/analyses/{os.getenv("ANALYSIS_ID")}/participants/self',
                                                  headers=[('Connection', 'close')])
        response.raise_for_status()

    def send_message(self, message: Message):
        body = {
            "recipients": message.recipients,
            "message": message.message
        }
        asyncio.run(self._message_broker.post(f'/analyses/{os.getenv("ANALYSIS_ID")}/messages',
                                              json=body,
                                              headers=[('Connection', 'close')]))
        self.list_of_outgoing_messages.append(body)

    def receive_message(self, body: Any) -> None:
        self.list_of_incoming_messages.append(body)
        print(body)

