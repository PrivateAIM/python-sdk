import asyncio
from typing import List, Literal, IO

from flame.resources.client_apis.clients.result_client import ResultClient
from flame.resources.node_config import NodeConfig


class StorageAPI:
    def __init__(self, config: NodeConfig):
        self.result_client = ResultClient(config.nginx_name, config.keycloak_token)

    def submit_final_result(self, result: IO) ->  dict[str, str]:
        """
        sends the final result to the hub. Making it available for analysts to download.
        This method is only available for nodes for which the method `get_role(self)` returns "aggregator”.
        :param result: the final result
        :return: the request status code
        """
        return asyncio.run(self.result_client.push_result(result))

    def save_intermediate_data(self, location: Literal["local", "global"], data: IO) -> dict[str, str]:
        """
        saves intermediate results/data either on the hub (location="global"), or locally
        :param location: the location to save the result, local saves in the node, global saves in central instance of MinIO
        :param data: the result to save
        :return: the request status code
        """
        return asyncio.run(self.result_client.push_result(data))

    def list_intermediate_data(self, location: Literal["local", "global"]) -> List[str]:
        """
        returns a list of all locally/globally saved intermediate data available
        :param location: the location to list the result, local lists in the node, global lists in central instance of MinIO
        :return: the list of results
        """

        if location == "local":
            return self.result_client.list_local_results()
        elif location == "global":
            return self.result_client.list_global_results()
        else:
            raise ValueError("Location must be either 'local' or 'global'")

    def get_intermediate_data(self, location: Literal["local", "global"], id: str) -> IO:
        """
        returns the intermediate data with the specified id
        :param location: the location to get the result, local gets in the node, global gets in central instance of MinIO
        :param id: the id of the result to get
        :return: the result
        """
        if location == "local":
            return self.result_client.get_intermediate_data_local(id)
        elif location == "global":
            return self.result_client.get_intermediate_data_global(id)
        else:
            raise ValueError("Location must be either 'local' or 'global'")