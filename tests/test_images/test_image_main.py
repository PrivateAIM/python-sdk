import asyncio
from typing import Any

from flame.flame import (FlameSDK,
                         Analyzer,
                         DataApiClient,
                         NodeConfig)


class My_Analyzer(Analyzer):
    def __init__(self, node_config: NodeConfig):
        super().__init__(node_config)
        pass

    async def analysis_method(self, data_api_client: DataApiClient, aggregator_results: Any) -> Any:
        # get all available sources
        #data_sources = await data_api_client.get_available_sources()
        # select a source
        #source_xy = data_sources[0]
        #data_xy1 = await data_api_client.get_data(datasource=source_xy, query='1')
        #data_xy2 = await data_api_client.get_data(datasource=source_xy, query='2')
        pass

class My_Aggregator:
    def __init__(self):
        pass

    async def aggregate(self):
        pass




def main():
    flame = FlameSDK()
    if flame.is_analyzer():
        print("Analyzer")
        flame.send_message([flame.node_config.node_id], "Hello")
        print("after send_message")
        asyncio.run(flame.start_analyzer(My_Analyzer))
        

if __name__ == "__main__":
    main()
