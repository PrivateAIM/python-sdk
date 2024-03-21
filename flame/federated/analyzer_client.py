from abc import abstractmethod
from typing import Any

from flame.clients.data_api_client import DataApiClient
from flame.federated.node_base_client import Node, NodeConfig


class Analyzer(Node):
    def __init__(self, node_config: NodeConfig) -> None:
        if node_config.node_mode != 'analyzer':
            raise ValueError(f'Attempted to initialize analyzer node with mismatching configuration '
                             f'(expected: node_mode="analyzer", received="{node_config.node_mode}").')
        super().__init__(node_config.node_id, 'analyzer')

    async def analyze(self, data_api_client: DataApiClient, aggregator_results: Any) -> Any:
        results = await self.analysis_method(data_api_client, aggregator_results)
        return results

    @abstractmethod
    async def analysis_method(self, data_api_client: DataApiClient, aggregator_results: Any) -> Any:
        """
        This method will be used to analyze the data. It has to be overridden.
        :return: result
        """
        pass
