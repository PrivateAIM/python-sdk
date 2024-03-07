from typing import Any, Callable, Optional

from flame.api import FlameAPI
from flame.clients.data_api_client import DataApiClient
from flame.clients.message_broker_client import MessageBrokerClient
from flame.federated.aggregator_client import Aggregator
from flame.federated.analyzer_client import Analyzer


class FlameSDK:
    message_broker: MessageBrokerClient
    flame_api: FlameAPI
    data_api_client: DataApiClient
    minio_service: Any

    aggregator: Optional[Aggregator]
    analyzer: Optional[Analyzer]

    def __init__(self) -> None:
        if self.is_analyzer() or self.is_aggregator():
            self.flame_api = FlameAPI('analyzer')
        else:
            raise BrokenPipeError("Unable to determine action mode.")

    def test_apis(self) -> None:
        pass

    def is_aggregator(self) -> bool:
        return False  # TODO: change when tokens and message broker are implemented

    def is_analyzer(self) -> bool:
        return True  # TODO: change when tokens and message broker are implemented

    def start_aggregator(self) -> None:
        pass

    def start_analyzer(self) -> None:
        pass

    def get_weights(self, cutoff: float) -> list[float]:
        return self.aggregator.get_weights(cutoff)

    def converged(self) -> bool:
        return self.aggregator.converged
