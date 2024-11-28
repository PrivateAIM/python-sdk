import asyncio
from typing import Any, Literal

from flame.resources.client_apis.clients.result_client import ResultClient
from flame.resources.node_config import NodeConfig


class StorageAPI:
    def __init__(self, config: NodeConfig):
        self.result_client = ResultClient(config.nginx_name, config.keycloak_token)

    def submit_final_result(self, result: Any, output_type: Literal['str', 'bytes', 'pickle'] = 'str') -> dict[str, str]:
        """
        sends the final result to the hub. Making it available for analysts to download.
        This method is only available for nodes for which the method `get_role(self)` returns "aggregator”.
        :param result: the final result
        :param output_type: output type of final results (default: string)
        :return: the request status code
        """
        return asyncio.run(self.result_client.push_result(result, "final", output_type))

    def save_intermediate_data(self, location: Literal["global", "local"], data: Any) -> dict[str, str]:
        """
        saves intermediate results/data either on the hub (location="global"), or locally
        :param location: the location to save the result, local saves in the node, global saves in central instance of MinIO
        :param data: the result to save
        :return: the request status code
        """
        return asyncio.run(self.result_client.push_result(data, type=location))

    def get_intermediate_data(self, location: Literal["local", "global"], id: str) -> Any:
        """
        returns the intermediate data with the specified id
        :param location: the location to get the result, local gets in the node, global gets in central instance of MinIO
        :param id: the id of the result to get
        :return: the result
        """
        return asyncio.run(self.result_client.get_intermediate_data(id, type=location))
