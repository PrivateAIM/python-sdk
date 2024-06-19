import asyncio
import os

from typing import Any, Optional, Literal
from enum import Enum

from resources.clients.message_broker_client import MessageBrokerClient


class NodeStatus(Enum):
    STARTED = "Analysis/Aggregation undergoing"
    FINISHED = "Results sent"


class Node:
    node_id: str
    responded: bool = False
    node_mode = Literal['analyzer', 'aggregator']
    status: str
    latest_result: Optional[Any]

    def __init__(self, node_id: str, node_mode: Literal['analyzer', 'aggregator']):
        self.node_id = node_id
        self.node_mode = node_mode
        self.status = NodeStatus.STARTED.value
        self.latest_result = None

    async def check_response(self) -> bool:
        if self.responded:
            return True
        else:
            return False

    def set_result(self, result: Any) -> str:
        self.latest_result = result
        return self.latest_result  # TODO: save result in file and return path

    def get_result(self, message_broker: MessageBrokerClient, node_id: Optional[str] = None) -> Any:
        if node_id is not None:
            msgs = [msg for msg in message_broker.list_of_incoming_messages if msg["sender"] == node_id]
        else:
            msgs = message_broker.list_of_incoming_messages
        for msg in msgs:
            if "resultData" in msg.keys():
                return msg["resultData"]


class NodeConfig:
    node: Node
    analysis_id: str
    project_id: str
    partner_nodes: list[Node]
    aggregator_node: Node

    def __init__(self, message_broker: MessageBrokerClient) -> None:
        self.analysis_id = os.getenv('ANALYSIS_ID')
        self.project_id = os.getenv('PROJECT_ID')

        self_info = asyncio.run(message_broker.get_self_config(self.analysis_id))
        self.node = Node(
            node_id=self_info['nodeId'],
            node_mode='analyzer' if self_info['nodeType'] == 'default' else 'aggregator'
        )

        partner_nodes = [
            Node(
                node_id=partner_n['nodeId'],
                node_mode='analyzer' if partner_n['nodeType'] == 'default' else 'aggregator'
            )
            for partner_n in asyncio.run(message_broker.get_partner_nodes(self.node.node_id, self.analysis_id))
        ]
        self.partner_nodes = partner_nodes
        self.aggregator_node = self.node \
            if self.node.node_mode == 'aggregator' else \
            [node for node in self.partner_nodes if node.node_mode == 'aggregator'][0]
