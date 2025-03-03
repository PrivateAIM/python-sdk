from abc import abstractmethod
from typing import Any, Optional

from flame import FlameCoreSDK
from flame.patterns.star.node_base_client import Node


class Aggregator(Node):

    def __init__(self, flame: FlameCoreSDK) -> None:
        node_config = flame.config

        if node_config.node_role != 'aggregator':
            raise ValueError(f'Attempted to initialize aggregator node with mismatching configuration '
                             f'(expected: node_role="aggregator", received="{node_config.node_role}").')
        super().__init__(node_config.node_id, flame.get_participant_ids(), node_config.node_role)

    def aggregate(self, node_results: list[Any], simple_analysis: bool = True) -> tuple[Any, bool]:
        result = self.aggregation_method(node_results)

        self.latest_result = result
        self.num_iterations += 1

        return self.latest_result, simple_analysis or self.has_converged(result, self.latest_result, self.num_iterations)

    @abstractmethod
    def aggregation_method(self, analysis_results: list[Any]) -> Any:
        """
        This method will be used to aggregate the data. It has to be overridden.
        :return: aggregated_result
        """
        pass

    @abstractmethod
    def has_converged(self, result: Any, last_result: Optional[Any], num_iterations: int) -> bool:
        """
        This method will be used to check if the aggregator has converged. It has to be overridden.
        :return: converged
        """
        pass
