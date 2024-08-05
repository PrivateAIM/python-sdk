from threading import Thread
from enum import Enum

from typing import Any, Callable, Optional, Type

from flame.resources.rest_api import FlameAPI
from flame.resources.client_apis.clients import DataApiClient
from flame.resources.client_apis.clients import ResultClient
from flame.resources.client_apis.clients import MessageBrokerClient, Message
from flame.federated.aggregator_client import Aggregator
from schemas.star.analyzer_client import Analyzer
from flame.federated.node_base_client import Node, NodeConfig
from flame.utils.envs import get_envs
from flame.utils.nginx import wait_until_nginx_online


class _ERROR_MESSAGES(Enum):
    IS_ANALYZER = 'Node is configured as analyzer. Unable to execute command associated to aggregator.'
    IS_AGGREGATOR = 'Node is configured as aggregator. Unable to execute command associated to analyzer.'


class FlameSDK:
    message_broker: MessageBrokerClient
    node_config: NodeConfig
    result_service_client: ResultClient
    data_api_client: DataApiClient
    flame_api: FlameAPI
    flame_api_thread: Thread
    minio_service: Any

    aggregator: Optional[Aggregator]
    analyzer: Optional[Analyzer]

    def __init__(self) -> None:
        print("Starting Flame")
        envs = get_envs()
        wait_until_nginx_online(envs)

        # connect to message broker
        self.message_broker = MessageBrokerClient(envs)

        # extract node config
        self.node_config = NodeConfig(self.message_broker)

        # connection to result service
        self.result_service_client = ResultClient(envs)

        if self.is_analyzer():
            # connection to kong
            self.data_api_client = DataApiClient(self.node_config.project_id, envs)

            self.flame_api_thread = Thread(target=self._start_flame_api,
                                           args=('analyzer', self.message_broker, self.converged))
            self.flame_api_thread.start()
        elif self.is_aggregator():
            self.flame_api_thread = Thread(target=self._start_flame_api,
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

    async def start_aggregator(self, aggregator: Type[Aggregator] | Aggregator,
                               cutoff: float = 1.0,
                               is_federated: Optional[bool] = False) -> None:
        if self.is_aggregator():
            # Init
            self.aggregator = aggregator(self.node_config, is_federated) \
                if issubclass(aggregator, Aggregator) else aggregator
            partner_nodes = self.node_config.partner_nodes

            while not self.converged():  # (**)
                # Await number of responses reaching number of necessary nodes
                self.aggregator.await_results(self.message_broker,
                                              cutoff)

                # Aggregate results
                result_file = await self.aggregator.aggregate(self.message_broker)

                # If converged send aggregated result over ResultService to Hub, send converged status to Hub
                if self.converged():
                    await self.push_results(result_file)

                # Else send aggregated results to MinIO for analyzers, loop back to (**)
                else:
                    await self.commit_results(partner_nodes, result_file)

        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_ANALYZER.value)

    async def start_analyzer(self, analyzer: Type[Analyzer] | Analyzer, query: str) -> None:
        if self.is_analyzer():
            # Init
            self.analyzer = analyzer(self.node_config) if issubclass(analyzer, Analyzer) else analyzer
            aggregator_id = self.node_config.aggregator_node.node_id
            aggregator_results = None

            # Get data
            data = await self.data_api_client.get_data(query=query)

            # Check converged status on Hub
            while not self.converged():  # (**)
                # Analyze data
                result_file = await self.analyzer.analyze(data, aggregator_results)

                # Send result to MinIO for aggregator
                await self.commit_results([self.node_config.aggregator_node], result_file)
                self.analyzer.await_results(aggregator_id, self.message_broker)

                # Await converged status or result update by aggregator on MinIO
                aggregator_results = self.node_config.node.get_result(self.message_broker, aggregator_id)
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_AGGREGATOR.value)

    async def commit_results(self, recipients: list[Node], intermediate_results: Any) -> None:
        await self.send_message([r.node_id for r in recipients], {"sender": self.node_config.node.node_id,
                                                                  "resultData": intermediate_results})

    async def _get_aggregator_results(self) -> Any:
        pass

    async def push_results(self, result_file: str) -> None:
        if self.is_aggregator():
            await self.result_service_client.push_result(result_file)
        else:
            raise BrokenPipeError(_ERROR_MESSAGES.IS_ANALYZER.value)

    def get_data_source_client(self) -> DataApiClient:
        return self.data_api_client

    def get_weights(self, cutoff: float) -> list[float]:
        return self.aggregator.get_weights(cutoff)

    def converged(self) -> bool:
        if self.is_aggregator():
            return self.aggregator.converged
        else:
            return self._convergence_status()

    def _convergence_status(self) -> bool:
        for msg in self.message_broker.list_of_incoming_messages:
            if ((msg["sender"] == self.node_config.aggregator_node.node_id) and
                    ("convStatus" in msg.keys())):
                return bool(msg["convStatus"])

    async def send_message(self, recipients: list[str], message: dict) -> None:
        await self.message_broker.send_message(Message(recipients, message))
