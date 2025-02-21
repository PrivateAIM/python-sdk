import asyncio
from typing import Any, Literal, Optional

from flame.resources.client_apis.clients.result_client import ResultClient
from flame.resources.node_config import NodeConfig


class StorageAPI:
    def __init__(self, config: NodeConfig):
        self.result_client = ResultClient(config.nginx_name, config.keycloak_token)

    def submit_final_result(self,
                            result: Any,
                            output_type: Literal['str', 'bytes', 'pickle'] = 'str') -> dict[str, str]:
        """
        sends the final result to the hub. Making it available for analysts to download.
        This method is only available for nodes for which the method `get_role(self)` returns "aggregator”.
        :param result: the final result
        :param output_type: output type of final results (default: string)
        :return: the request status code
        """
        return self.result_client.push_result(result, None,"final", output_type)

    def save_intermediate_data(self,
                               data: Any,
                               location: Literal["global", "local"],
                               tag: Optional[str] = None) -> dict[str, str]:
        """
        saves intermediate results/data either on the hub (location="global"), or locally
        :param data: the result to save
        :param location: the location to save the result, local saves in the node, global saves in central instance of MinIO
        :param tag: optional storage tag
        :return: the request status code
        """
        return self.result_client.push_result(data, tag=tag, type=location)

    def get_intermediate_data(self,
                              location: Literal["local", "global"],
                              id: Optional[str] = None,
                              tag: Optional[str] = None,
                              tag_option: Optional[Literal["all", "last","first"]]= "all") -> Any:
        """
        returns the intermediate data with the specified id
        :param location: the location to get the result, local gets in the node, global gets in central instance of MinIO
        :param id: the id of the result to get
        :param tag: optional storage tag of targeted local result
        :return: the result
        """
        return self.result_client.get_intermediate_data(id, tag, type=location, tag_option=tag_option)

    def get_local_tags(self, filter: Optional[str] = None) -> list[str]:
        """
        returns the list of tags used to save local results
        :param filter: filter tags by a substring
        :return: the list of tags
        """
        return self.result_client.get_local_tags(filter)
