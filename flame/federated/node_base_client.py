from enum import Enum


class NodeStatus(Enum):
    STARTED = "Analysis/Aggregation undergoing"
    FINISHED = "Results sent"


class Node:
    node_id: int
    responded: bool = False
    status: str

    def __init__(self, node_id: int):
        self.node_id = node_id
        self.status = NodeStatus.STARTED.value

    async def check_response(self) -> bool:
        if self.responded:
            return True
        else:
            return False
