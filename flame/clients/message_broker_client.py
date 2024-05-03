import os
import asyncio
import json

from httpx import AsyncClient, HTTPError
from typing import Any


class Message:
    def __init__(self, recipients: list[str], message: dict) -> None:
        self.recipients = recipients
        self.message = message


class MessageBrokerClient:
    def __init__(self, token: str) -> None:
        self._message_broker = AsyncClient(
            base_url="http://flame-node-node-message-broker",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}
        )
        asyncio.run(self._connect())
        self.list_of_incoming_messages: list[dict] = []
        self.list_of_outgoing_messages: list[dict] = []

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

    async def _connect(self) -> None:
        response = await self._message_broker.post(
            f'/analyses/{os.getenv("ANALYSIS_ID")}/messages/subscriptions',
            json={'webhookUrl': f'http://service-{os.getenv("DEPLOYMENT_NAME")}/webhook'}
        )
        print(f"message broker connect response  {response}")
        print(f'/analyses/{os.getenv("ANALYSIS_ID")}/messages/subscriptions')
        print({'webhookUrl': f'http://service-{os.getenv("DEPLOYMENT_NAME")}/webhook'})

        response = await self._message_broker.get(f'/analyses/{os.getenv("ANALYSIS_ID")}/participants/self',
                                                  headers=[('Connection', 'close')])
        response.raise_for_status()

    def send_message(self, message: Message):
        body = {
            "recipients": message.recipients,
            "message": message.message
        }
        print('body type:', type(body))
        print('body:', body)
        response = asyncio.run(self._message_broker.post(f'/analyses/{os.getenv("ANALYSIS_ID")}/messages',
                                                         json=body,
                                                         headers=[('Connection', 'close'),("Content-Type", "application/json")]))
        print(f"message broker send response  {response}")
        #print(f"message  send   response json {response}")
        print(f"message  send   {body}")

        self.list_of_outgoing_messages.append(body)

    def receive_message(self, body: dict) -> None:
        self.list_of_incoming_messages.append(body)
        print(body)

