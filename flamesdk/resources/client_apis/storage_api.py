from typing import Any, Literal, Optional, Union

from flamesdk.resources.client_apis.clients.result_client import ResultClient, LocalDifferentialPrivacyParams
from flamesdk.resources.node_config import NodeConfig
from flamesdk.resources.utils.logging import FlameLogger


class StorageAPI:
    def __init__(self, config: NodeConfig,  flame_logger: FlameLogger) -> None:
        self.result_client = ResultClient(config.nginx_name, config.keycloak_token, flame_logger)

    def submit_final_result(self,
                            result: Any,
                            output_type: Literal['str', 'bytes', 'pickle'] = 'str',
                            local_dp: Optional[LocalDifferentialPrivacyParams] = None) -> dict[str, str]:
        """
        sends the final result to the hub. Making it available for analysts to download.
        This method is only available for nodes for which the method `get_role(self)` returns "aggregator”.
        :param result: the final result
        :param output_type: output type of final results (default: string)
        :param local_dp: tba
        :return: the request status code
        """
        return self.result_client.push_result(result,
                                              type="final",
                                              output_type=output_type,
                                              local_dp=local_dp)

    def save_intermediate_data(self,
                               data: Any,
                               location: Literal["global", "local"],
                               remote_node_ids: Optional[list[str]] = None,
                               tag: Optional[str] = None) -> Union[dict[str, dict[str, str]], dict[str, str]]:
        """
        saves intermediate results/data either on the hub (location="global"), or locally
        :param data: the result to save
        :param location: the location to save the result, local saves in the node, global saves in central instance of MinIO
        :param remote_node_ids: optional remote node ids (used for accessing remote node's public key for encryption)
        :param tag: optional storage tag
        :return: list of the request status codes and url access and ids
        """
        returns = {}
        if remote_node_ids:
            for remote_node_id in remote_node_ids:
                returns[remote_node_id] = self.result_client.push_result(data,
                                                                         remote_node_id=remote_node_id,
                                                                         type=location)
            return returns
        else:
            return self.result_client.push_result(data, tag=tag, type=location)

    def get_intermediate_data(self,
                              location: Literal["local", "global"],
                              id: Optional[str] = None,
                              tag: Optional[str] = None,
                              tag_option: Optional[Literal["all", "last","first"]] = "all",
                              sender_node_id: Optional[str] = None) -> Any:
        """
        returns the intermediate data with the specified id
        :param location: the location to get the result, local gets in the node, global gets in central instance of MinIO
        :param id: the id of the result to get
        :param tag: optional storage tag of targeted local result
        :param tag_option: return mode if multiple tagged data are found
        :param sender_node_id:
        :return: the result
        """
        return self.result_client.get_intermediate_data(id,
                                                        tag,
                                                        type=location,
                                                        tag_option=tag_option,
                                                        sender_node_id=sender_node_id)

    def get_local_tags(self, filter: Optional[str] = None) -> list[str]:
        """
        returns the list of tags used to save local results
        :param filter: filter tags by a substring
        :return: the list of tags
        """
        return self.result_client.get_local_tags(filter)
