from flame.flame import FlameSDK
from typing import Any, Callable
from flame import Node, NodeConfig
from flame.federated import Aggregator
from flame.protocols import StandartNNAggregatorMethod
import asyncio


class my_Aggregator(Aggregator):
    def __init__(self,
                 node_config: NodeConfig,
                 base_model: Any,
                 model_params: dict[str: str | float | int | bool],
                 weights: list[Any],
                 gradients: list[list[float]],
                 aggr_method: Callable) -> None:
        super().__init__(node_config, base_model, model_params, weights, gradients, aggr_method)
