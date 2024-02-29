from flame.flame import FlameSDK
from typing import Any, Callable
from flame import Node
from flame.federated import Aggregator
from flame.protocols import StandartNNAggregatorMethod
import asyncio






async def aggregate(flame: FlameSDK):

    while flame.not_converged():
        # wait for all the nodes to send weights
        weights = await flame.get_weights()
        #
        aggregator = FlameSDK.StandartNNAggregator()
        aggregated_weights = aggregator(weights)

        flame.send_weights(aggregated_weights)




