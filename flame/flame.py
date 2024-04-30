import asyncio
import threading
from enum import Enum

from typing import Any, Callable, Optional, Type

from flame.api import FlameAPI
from flame.clients.data_api_client import DataApiClient
from flame.clients.result_client import ResultClient
from flame.clients.message_broker_client import MessageBrokerClient, Message
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

        # connect to message broker
        self.message_broker = MessageBrokerClient(tokens['KEYCLOAK_TOKEN'])

        # extract node config
        self.node_config = NodeConfig(self.message_broker)

        # connection to result service
        self.result_service_client = ResultClient(tokens['KEYCLOAK_TOKEN'])
        asyncio.run(self.result_service_client.test_connection())

        if self.is_analyzer():
            # connection to kong
            self.data_api_client = DataApiClient(tokens['DATA_SOURCE_TOKEN'])
            # connection to data sources
            asyncio.run(self.data_api_client.test_connection())

            # start flame api
            self.flame_api_thread = threading.Thread(target=self._start_flame_api,
                                                     args=('analyzer', self.message_broker, self.converged))
            self.flame_api_thread.start()
        elif self.is_aggregator():
            # start flame api
            self.flame_api_thread = threading.Thread(target=self._start_flame_api,
                                                     args=('aggregator', self.message_broker, self.converged))
            self.flame_api_thread.start()
        else:
            raise BrokenPipeError("Unable to determine action mode.")

    def _start_flame_api(self, node_mode: str, message_broker: MessageBrokerClient, converged: Callable) -> None:
        self.flame_api = FlameAPI(node_mode, message_broker, converged)

    async def test_apis(self) -> None:
        await self.data_api_client.test_connection()

    def is_aggregator(self) -> bool:
        return self.node_config.node.node_mode == 'aggregator'

    def is_analyzer(self) -> bool:
        return self.node_config.node.node_mode == 'analyzer'

    def get_node_config(self) -> NodeConfig:
        return self.node_config

    async def start_aggregator(self, aggregator: Aggregator, cutoff: float = 1.0) -> None:
        if self.is_aggregator():
            # Init
            self.aggregator = aggregator

            # Calculate number of necessary nodes (number of nodes * cutoff)
            num_necessary_nodes = int(len(self.node_config.partner_nodes) * cutoff)

            while not await self.converged():  # (**)
                # Await number of responses reaching number of necessary nodes
                # TODO

                # Aggregate results
                result_file = await self.aggregator.aggregate()

                # If converged send aggregated result over ResultService to Hub, send converged status to Hub
                if await self.converged():
                    self.push_results(result_file)
                    # TODO: Send converged status to Hub

                # Else send aggregated results to MinIO for analyzers, loop back to (**)
                else:
                    self.commit_results(result_file)

        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_ANALYZER.value)

    async def start_analyzer(self, analyzer: Analyzer, query: str = '') -> None:
        if self.is_analyzer():
            # Init
            self.analyzer = analyzer
            aggregator_id = self.node_config.aggregator_node.node_id
            aggregated_results = None

            # Get data
            data = self.data_api_client.get_data(datasource=self.node_config.project_id, query=query)

            # Check converged status on Hub
            while not await self.converged():  # (**)
                # Analyze data
                result_file = await self.analyzer.analyze(data, aggregated_results)

                # Send result to MinIO for aggregator
                self.commit_results(result_file)

                # Await converged status or result update by aggregator on MinIO
                aggregated_results = await self.node_config.node.get_result(aggregator_id)


            # while (not await self.converged()) and (count < 1):  # until aggregator converges
            #     intermediate_results = await self.analyzer.analyze(self.data_api_client, aggregator_results)
            #     self.commit_results(intermediate_results)
            #     # await next epoch or termination
            #     aggregator_results = await self._get_aggregator_results()  # get intermediate result
            #     count += 1

        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_AGGREGATOR.value)

    def commit_results(self, intermediate_results: Any) -> None:
        # TODO: Commit results to MinIO
        pass

    async def _get_aggregator_results(self) -> Any:
        pass

    def push_results(self, latest_result: Any) -> None:
        if self.is_aggregator():
            self.result_service_client.push_result()  # TODO: save latest result in path
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_ANALYZER.value)

    def get_data_source_client(self) -> DataApiClient:
        return self.data_api_client

    def get_weights(self, cutoff: float) -> list[float]:
        return self.aggregator.get_weights(cutoff)

    async def converged(self) -> bool:
        if self.is_aggregator():
            return self.aggregator.converged
        else:
            return False  # await self.message_broker() # ask for aggregator convergence state

    def send_message(self, recipients: list[str], message: Any) -> None:
        self.message_broker.send_message(Message(recipients, message))
