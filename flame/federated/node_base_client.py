from pydantic import BaseModel
from typing import Optional, Literal
from enum import Enum


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

    def __init__(self, node_config: dict) -> None:
        self.node_id = node_config['node_id']
        self.analysis_id = node_config['analysis_id']
        self.project_id = node_config['project_id']
        self.node_mode = node_config['node_mode']
        self.partner_nodes = node_config['partner_nodes']
