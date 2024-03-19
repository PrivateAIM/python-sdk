import os

from httpx import AsyncClient, HTTPError
from ..federated.node_base_client import NodeConfig, Node


class _Message:
    def __init__(self, receiver: str, message: str) -> None:
        self.sender = os.environ["SENDER"]
        self.receiver = receiver
        self.message = message


class MessageBrokerClient:
    def __init__(self, token: str) -> None:
        self._host = os.getenv("MESSAGE_BROKER_HOST")
        self._port = os.getenv("MESSAGE_BROKER_PORT")
        self._token = token

    async def _connect(self) -> None:
        # TODO find out how to connect to the message broker
        await self._message_broker.connect(host=self._host, port=self._port, token=self._token)

    async def test_connection(self) -> bool:
        response = await self._message_broker.get("/healthz")
        try:
            response.raise_for_status()
            return True
        except HTTPError:
            return False

    async def get_node_config(self) -> NodeConfig:
        #     node_id: str
        #     analysis_id: str
        #     project_id: str
        #     node_mode: Literal['analyzer', 'aggregator']
        #     partner_nodes: list[Node]
        # TODO:
        #response = await self._message_broker.get("/config")
        #response.raise_for_status()

        #return NodeConfig(**response.json())
        partner_node_2 = Node(node_id='2', node_mode='analyzer')
        partner_node_3 = Node(node_id='3', node_mode='aggregator')
        node_config_dict = {"node_id": "1", "analysis_id": "1", "project_id": "1", "node_mode": "analyzer", "partner_nodes": [partner_node_2, partner_node_3]}
        node_config = NodeConfig(node_config=node_config_dict)
        return node_config

    async def _ask_central_analyzer_or_aggregator(self) -> str:
        message = _Message( receiver="central_analyses_management", message="status_aggregator_or_analyzer")
        self._send(message)
        answer = await self._receive()
        # TODO must be checked if feald is correct
        return answer['node_mode']

    def _send(self, message: _Message):
        self._message_broker.send(message)

    def _receive(self) -> _Message:
        return self._message_broker.receive()

