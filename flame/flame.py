import asyncio
import time
import threading
from enum import Enum

from typing import Any, Callable, Optional

from flame.api import FlameAPI
from flame.clients.data_api_client import DataApiClient
from flame.clients.result_client import ResultClient
from flame.clients.message_broker_client import MessageBrokerClient
from flame.federated.aggregator_client import Aggregator
from flame.federated.analyzer_client import Analyzer
from flame.federated.node_base_client import NodeConfig
from flame.utils.token import get_tokens


class _ERROR_MESSAGES(Enum):
    IS_ANALYZER = 'Node is configured as analyzer. Unable to execute command associated to aggregator.'
    IS_AGGREGATOR = 'Node is configured as aggregator. Unable to execute command associated to analyzer.'


class FlameSDK:
    message_broker: MessageBrokerClient
    node_config: NodeConfig
    result_service_client: ResultClient
    data_api_client: DataApiClient
    flame_api: FlameAPI
    flame_api_thread: threading.Thread
    minio_service: Any

    aggregator: Optional[Aggregator]
    analyzer: Optional[Analyzer]

    def __init__(self) -> None:
        tokens = get_tokens()
        self.message_broker = MessageBrokerClient(tokens['MESSAGE_BROKER_TOKEN'])  # TODO
        self.node_config = asyncio.run(self.message_broker.get_node_config())  # TODO: Get Node config from MsgBroker

        # connection to result service
        self.result_service_client = ResultClient(tokens['RESULT_SERVICE_TOKEN'])
        asyncio.run(self.result_service_client.test_connection())

        if self.is_analyzer():
            # connection to kong
            self.data_api_client = DataApiClient(tokens['DATA_SOURCE_TOKEN'])
            # get available data sources
            asyncio.run(self.data_api_client.test_connection())
            # TODO: Get data sources

            # start flame api
            self.flame_api_thread = threading.Thread(target=self._start_flame_api, args=('analyzer',))
            self.flame_api_thread.start()
        elif self.is_aggregator():
            # start flame api
            self.flame_api_thread = threading.Thread(target=self._start_flame_api, args=('aggregator',))
            self.flame_api_thread.start()
        else:
            raise BrokenPipeError("Unable to determine action mode.")

    def _start_flame_api(self, node_mode: str) -> None:
        self.flame_api = FlameAPI(node_mode)

    async def test_apis(self) -> None:
        await self.data_api_client.test_connection()

    def is_aggregator(self) -> bool:
        return self.node_config.node_mode == 'aggregator'

    def is_analyzer(self) -> bool:
        return self.node_config.node_mode == 'analyzer'

    def get_node_config(self) -> NodeConfig:
        return self.node_config

    async def start_aggregator(self, aggregator: Aggregator) -> None:
        if self.is_aggregator():
            self.aggregator = aggregator
            await self.aggregator.aggregate()
            if self.aggregator.converged:
                self.push_results()
            else:
                self.commit_results()
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_ANALYZER.value)

    async def start_analyzer(self, analyzer: Analyzer) -> None:
        if self.is_analyzer():
            self.analyzer = analyzer
            await self.analyzer.analyze()
            self.commit_results()
            pass
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_AGGREGATOR.value)

    def commit_results(self) -> None:

        pass

    def push_results(self) -> None:
        pass

    def get_weights(self, cutoff: float) -> list[float]:
        return self.aggregator.get_weights(cutoff)

    def converged(self) -> bool:
        if self.is_aggregator():
            return self.aggregator.converged
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_ANALYZER)
