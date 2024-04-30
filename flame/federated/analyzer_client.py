from abc import abstractmethod
from typing import Any, Optional

from flame.clients.data_api_client import DataApiClient
from flame.federated.node_base_client import Node, NodeConfig


class Analyzer(Node):
    num_iterations: float = 0

    def __init__(self, node_config: NodeConfig) -> None:
        if node_config.node_mode != 'analyzer':
            raise ValueError(f'Attempted to initialize analyzer node with mismatching configuration '
                             f'(expected: node_mode="analyzer", received="{node_config.node_mode}").')
        super().__init__(node_config.node_id, 'analyzer')

    async def analyze(self, data: Any, aggregator_results: Optional[Any]) -> str:
        result = await self.analysis_method(data, aggregator_results)
        result_file = self.set_result(result)
        self.num_iterations += 1

        return result_file

    @abstractmethod
    def analysis_method(self, data: Any, aggregator_results: Optional[Any]) -> Any:
        """
        This method will be used to analyze the data. It has to be overridden.
        :return: analysis_result
        """
        pass
