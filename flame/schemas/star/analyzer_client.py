from abc import abstractmethod
from typing import Any, Optional

from flame import FlameCoreSDK

from flame.schemas.star.node_base_client import Node


class Analyzer(Node):

    def __init__(self, flame: FlameCoreSDK) -> None:
        node_config = flame.config

        if node_config.node_role != 'default':
            raise ValueError(f'Attempted to initialize analyzer node with mismatching configuration '
                             f'(expected: node_mode="default", received="{node_config.node_role}").')
        super().__init__(node_config.node_role, flame.get_participant_ids(), node_config.node_role)

        self.latest_data = None

    def analyze(self, data: Any, aggregator_results: Optional[str], simple_analysis: bool = True) -> tuple[Any, bool]:
        result = self.analysis_method(data, aggregator_results)

        identical_analysis = (self.latest_result == result) and (self.latest_data == data)

        self.latest_result = result
        self.latest_data = data
        self.num_iterations += 1

        return self.latest_result, simple_analysis or identical_analysis

    @abstractmethod
    def analysis_method(self, data: Any, aggregator_results: Optional[Any]) -> Any:
        """
        This method will be used to analyze the data. It has to be overridden.
        :return: analysis_result
        """
        pass
