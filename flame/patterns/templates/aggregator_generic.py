from typing import Any, Callable, Union
from flame import NodeConfig
from flame.federated import Aggregator


class my_Aggregator(Aggregator):
    def __init__(self,
                 node_config: NodeConfig,
                 base_model: Any,
                 model_params: dict[str: Union[str, float, int, bool]],
                 weights: list[Any],
                 gradients: list[list[float]],
                 aggr_method: Callable) -> None:
        super().__init__(node_config, base_model, model_params, weights, gradients, aggr_method)
