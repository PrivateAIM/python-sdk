import asyncio
import os

from pydantic import BaseModel
from typing import Optional, Literal
from enum import Enum

from flame.clients.message_broker_client import MessageBrokerClient


class NodeStatus(Enum):
    STARTED = "Analysis/Aggregation undergoing"
    FINISHED = "Results sent"


class Node:
    node_id: str
    responded: bool = False
    node_mode = Literal['analyzer', 'aggregator']
    status: str

    def __init__(self, node_id: str, node_mode: Literal['analyzer', 'aggregator']):
        self.node_id = node_id
        self.node_mode = node_mode
        self.status = NodeStatus.STARTED.value

    async def check_response(self) -> bool:
        if self.responded:
            return True
        else:
            return False


class NodeConfig:
    node_id: str
    analysis_id: str
    project_id: str
    node_mode: Literal['analyzer', 'aggregator']
    partner_nodes: list[Node]

    def __init__(self, message_broker: MessageBrokerClient) -> None:
        self.analysis_id = os.getenv('ANALYSIS_ID')
        self.project_id = os.getenv('PROJECT_ID')
        self_info = asyncio.run(message_broker.get_self_config(self.analysis_id))
        self.node_id = self_info['nodeId']
        self.node_mode = 'analyzer' if self_info['nodeType'] == 'default' else 'aggregator'

        partner_nodes = [
            Node(
                node_id=partner_n['nodeId'],
                node_mode='analyzer' if partner_n['nodeType'] == 'default' else 'aggregator'
            )
            for partner_n in asyncio.run(message_broker.get_partner_nodes(self.node_id, self.analysis_id))
        ]
        self.partner_nodes = partner_nodes
