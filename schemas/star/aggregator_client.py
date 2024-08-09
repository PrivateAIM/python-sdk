from abc import abstractmethod
from typing import Any, Optional

from flame import FlameCoreSDK

from schemas.star.node_base_client import Node


class Aggregator(Node):
    partner_node_ids: list[str]
    num_iterations: int
    model_params: Optional[dict[str, str | float | int | bool]]
    weights: Optional[list[Any]]
    gradients: list[list[Optional[float]]]

    def __init__(self,
                 flame: FlameCoreSDK,
                 model_params: Optional[dict[str, str | float | int | bool]] = None,
                 weights: Optional[list[Any]] = None) -> None:
        node_config = flame.config

        if node_config.node_role != 'aggregator':
            raise ValueError(f'Attempted to initialize aggregator node with mismatching configuration '
                             f'(expected: node_role="aggregator", received="{node_config.node_role}").')
        super().__init__(node_config.node_id, flame.get_participant_ids(), node_config.node_role)

        self.model_params = model_params
        self.weights = weights
        self.gradients = [[None for _ in weights]] if weights is not None else None

    def aggregate(self, node_results: list[str], simple_analysis: bool = True) -> tuple[Any, bool]:
        result = self.aggregation_method([res for res in node_results if res])

        self.latest_result = result
        self.num_iterations += 1

        return self.latest_result, simple_analysis or self.has_converged(result, self.latest_result)

    @abstractmethod
    def aggregation_method(self, analysis_results: list[str]) -> Any:
        """
        This method will be used to aggregate the data. It has to be overridden.
        :return: aggregated_result
        """
        pass

    @abstractmethod
    def has_converged(self, result: Any, last_result: Optional[Any]) -> bool:
        """
        This method will be used to check if the aggregator has converged. It has to be overridden.
        :return: converged
        """
        pass
