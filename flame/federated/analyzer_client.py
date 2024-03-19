from typing import Callable
from flame.federated.node_base_client import Node, NodeConfig


class Analyzer(Node):
    def __init__(self, analysis_method: Callable, node_config: NodeConfig) -> None:
        if node_config.node_mode != 'analyzer':
            raise ValueError(f'Attempted to initialize analyzer node with mismatching configuration '
                             f'(expected: node_mode="analyzer", received="{node_config.node_mode}").')
        super().__init__(node_config.node_id, 'analyzer')

        self.analysis_method = analysis_method

    async def analyze(self, cutoff: float = .8) -> None:
        while sum([await node.check_response() for node in self.nodes]) < int((len(self.nodes) + 1) * cutoff):
            pass
        self.analysis_method([await node.get_result() for node in self.nodes])
