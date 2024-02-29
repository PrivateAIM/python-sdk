from flame.flame import FlameSDK
from flame.protocols import StandartNNAggregator
import asyncio


async def aggregate(flame: FlameSDK):

    while flame.not_converged():
        # wait for all the nodes to send weights
        weights = await flame.get_weights()
        #
        aggregator = FlameSDK.StandartNNAggregator()
        aggregated_weights = aggregator(weights)

        flame.send_weights(aggregated_weights)




