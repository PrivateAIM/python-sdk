from typing import Callable
from flame.federated.node_base_client import Node


class Analyzer(Node):
    def __init__(self, node_id: int, analysis_method: Callable) -> None:
        super().__init__(node_id)

        self.analysis_method = analysis_method

    async def analyze(self, cutoff: float) -> None:
        while sum([await node.check_response() for node in self.nodes]) < int((len(self.nodes) + 1) * cutoff):
            pass
        self.analysis_method([await node.get_result() for node in self.nodes])
