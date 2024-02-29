from fastapi import APIRouter
from fastapi.responses import JSONResponse
from typing import Any, Callable, Optional

from pydantic import BaseModel

from .federated.aggregator_client import Aggregator
from .federated.analysis_client import Analyzer


class FlameAggregator(Aggregator):
    def __init__(self):
        pass


class FlameAnalyzer(Analyzer):
    def __init__(self):
        pass


class FlameSDK:
    flame_api: APIRouter
    aggregator: Optional[FlameAggregator]
    analyzer: Optional[FlameAnalyzer]

    def __init__(self) -> None:
        if self.is_analyzer() or self.is_aggregator():
            pass
        else:
            raise BrokenPipeError("Unable to determine action mode.")

    def test_apis(self) -> None:
        pass

    def is_aggregator(self) -> bool:
        pass

    def is_analyzer(self) -> bool:
        pass

    def start_aggregator(self) -> None:
        pass

    def start_analyzer(self) -> None:
        pass

    def get_weights(self) -> list[float]:
        return self.aggregator.get_weights()
