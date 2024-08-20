from typing import Any, Literal, Optional
from enum import Enum


class NodeStatus(Enum):
    STARTED = "Analysis/Aggregation undergoing"
    FINISHED = "Results sent"


class Node:
    id: str
    role = Literal['analyzer', 'aggregator']
    status: str
    latest_result: Optional[Any]
    partner_node_ids: list[str]
    num_iterations: int

    def __init__(self, id: str, partner_node_ids: list[str], role: Literal['analyzer', 'aggregator']):
        self.id = id
        self.role = role
        self.status = NodeStatus.STARTED.value
        self.latest_result = None
        self.partner_node_ids = partner_node_ids
        self.num_iterations: int = 0

    def node_finished(self):
        self.status = NodeStatus.FINISHED.value
