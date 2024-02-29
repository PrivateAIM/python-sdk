from typing import Any, Callable

from pydantic import BaseModel


class Aggregator(BaseModel):
    nodes: list[Node]
    base_model: Any
    model_params: dict[str: str | float | int | bool]
    weights: list[float]
    gradients: list[list[float]]
    aggr_method: Callable

    def aggregate(self, weights: list[float]) -> None:
        self.weights = await self.aggr_method(weights)

    async def get_weights(self, cutoff: float) -> list[float]:
        if sum([await node.responded for node in self.nodes]) > int((len(self.nodes) + 1) * cutoff):
            return self.weights
