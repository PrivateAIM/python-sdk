import asyncio
from typing import Any, Callable


class Aggregator:
    def __init__(self,
                 nodes: list[Node],
                 base_model: Any,
                 model_params: dict[str: str | float | int | bool],
                 weights: list[Any],
                 gradients: list[list[float]],
                 aggr_method: Callable) -> None:
        self.nodes = nodes
        self.base_model = base_model
        self.model_params = model_params
        self.weights = weights
        self.gradients = gradients
        self.aggr_method = aggr_method
        self.converged = False

    async def aggregate(self, cutoff: float) -> None:
        while sum([await node.responded for node in self.nodes]) < int((len(self.nodes) + 1) * cutoff):
            pass
        self.weights = self.aggr_method([await node.get_result() for node in self.nodes])

    def get_weights(self) -> list[Any]:
        return self.weights

    def _converge(self):
        self.converged = True
