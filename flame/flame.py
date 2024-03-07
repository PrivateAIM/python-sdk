import threading

from typing import Any, Callable, Optional

import time
from flame.api import FlameAPI
from flame.clients.data_api_client import DataApiClient
from flame.clients.message_broker_client import MessageBrokerClient
from flame.federated.aggregator_client import Aggregator
from flame.federated.analyzer_client import Analyzer


class FlameSDK:
    message_broker: MessageBrokerClient
    flame_api: FlameAPI
    flame_api_thread: threading.Thread
    data_api_client: DataApiClient
    minio_service: Any

    aggregator: Optional[Aggregator]
    analyzer: Optional[Analyzer]

    def __init__(self) -> None:
        # TODO: MessageBroker sending config
        if self.is_analyzer():
            self.flame_api_thread = threading.Thread(target=self._start_flame_api, args=('analyzer',))
            self.flame_api_thread.start()
            print('Analysing stuff')
            time.sleep(10)
            print('Done analysing')
        elif self.is_aggregator():
            self.flame_api_thread = threading.Thread(target=self._start_flame_api, args=('aggregator',))
            self.flame_api_thread.start()
        else:
            raise BrokenPipeError("Unable to determine action mode.")

    def _start_flame_api(self, node_mode: str) -> None:
        self.flame_api = FlameAPI(node_mode)

    def test_apis(self) -> None:
        pass

    def is_aggregator(self) -> bool:
        return False  # TODO: change when tokens and message broker are implemented

    def is_analyzer(self) -> bool:
        return True  # TODO: change when tokens and message broker are i    def stop_api(self) -> None:


    def start_aggregator(self) -> None:
        pass

    def start_analyzer(self) -> None:
        pass

    def get_weights(self, cutoff: float) -> list[float]:
        return self.aggregator.get_weights(cutoff)

    def converged(self) -> bool:
        return self.aggregator.converged
