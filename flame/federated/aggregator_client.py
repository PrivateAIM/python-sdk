import asyncio
from typing import Any, Callable
from flame.federated.node_base_client import Node, NodeConfig
from flame.federated.analyzer_client import Analyzer


class Aggregator(Node):
    nodes: list[Node]
    base_model: Any
    model_params: dict[str: str | float | int | bool]
    weights: list[Any]
    gradients: list[list[float]]
    aggr_method: Callable

    def __init__(self,
                 node_config: NodeConfig,
                 base_model: Any,
                 model_params: dict[str: str | float | int | bool],
                 weights: list[Any],
                 gradients: list[list[float]],
                 aggr_method: Callable) -> None:
        if node_config.node_mode != 'aggregator':
            raise ValueError(f'Attempted to initialize aggregator node with mismatching configuration '
                             f'(expected: node_mode="aggregator", received="{node_config.node_mode}").')
        super().__init__(node_config.node_id, 'aggregator')
        
        self.nodes = node_config.partner_nodes
        self.base_model = base_model
        self.model_params = model_params
        self.weights = weights
        self.gradients = gradients
        self.aggr_method = aggr_method
        self.converged = False

    async def aggregate(self, cutoff: float = .8) -> None:
        while sum([await node.responded for node in self.nodes]) < int((len(self.nodes) + 1) * cutoff):
            pass
        self.weights = self.aggr_method([await node.get_result() for node in self.nodes])

    def get_weights(self) -> list[Any]:
        return self.weights

    def _converge(self):
        self.converged = True
