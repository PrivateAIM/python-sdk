from pathlib import PurePath
from typing import Any, Literal, Optional, Union

from flamesdk.resources.client_apis.clients.storage_client import StorageClient, LocalDifferentialPrivacyParams
from flamesdk.resources.node_config import NodeConfig
from flamesdk.resources.utils.constants import LogTypeLiteral
from flamesdk.resources.utils.logging import FlameLogger

_EXT_TO_OUTPUT_TYPE: dict[str, str] = {
    ".pkl": "pickle",
    ".pickle": "pickle",
    ".bin": "bytes",
    ".txt": "str",
    ".csv": "str",
    ".json": "str",
    ".xml": "str",
    ".tsv": "str",
    ".yaml": "str",
    ".yml": "str",
}


class StorageAPI:
    def __init__(self, config: NodeConfig,  flame_logger: FlameLogger) -> None:
        self.flame_logger = flame_logger
        self.storage_client = StorageClient(config.nginx_name, config.keycloak_token, flame_logger)

    def _warn_filename_extension(self, filename: str, output_type: str) -> None:
        ext = PurePath(filename).suffix.lower()
        expected_type = _EXT_TO_OUTPUT_TYPE.get(ext)
        if expected_type is not None and expected_type != output_type:
            self.flame_logger.new_log(
                f"Filename extension '{ext}' suggests output_type='{expected_type}' "
                f"but output_type='{output_type}' was specified — the file may be unreadable after download.",
                log_type=LogTypeLiteral.WARNING.value,
            )

    def submit_final_result(self,
                            result: Any,
                            output_type: Union[Literal['str', 'bytes', 'pickle'], list] = 'str',
                            multiple_results: bool = False,
                            local_dp: Optional[LocalDifferentialPrivacyParams] = None,
                            filename: Optional[Union[str, list[str]]] = None) -> Union[dict[str, str], list[dict[str, str]]]:
        """
        sends the final result to the hub. Making it available for analysts to download.
        This method is only available for nodes for which the method `get_role(self)` returns "aggregator".
        :param result: the final result (single object or list of objects)
        :param output_type: output type of final results (default: string)
        :param multiple_results: whether the result is to be split into separate results (per element in tuple) or a single result
        :param local_dp: tba
        :param filename: optional filename for the result file on the hub. For multiple_results, pass a list of names
                         (one per element) or a single string (auto-indexed as name_0, name_1, …). Defaults to an
                         auto-generated name when None.
        :return: the request status code (single dict if result is not a list, list of dicts if result is a list)
        """
        # Check if result is a tuple or list and multiple_results is true
        if multiple_results and (isinstance(result, list) or isinstance(result, tuple)):
            if isinstance(filename, list) and len(filename) != len(result):
                self.flame_logger.raise_error(
                    f"filename list length ({len(filename)}) does not match result list length ({len(result)})")
            # Submit each element in the list separately
            responses = []
            has_multiple_types = isinstance(output_type, list) and (len(output_type) == len(result))
            for i, item in enumerate(result):
                if isinstance(filename, list):
                    resolved_filename = filename[i]
                elif isinstance(filename, str):
                    p = PurePath(filename)
                    resolved_filename = f"{p.stem}_{i}{p.suffix}"
                else:
                    resolved_filename = None
                effective_type = output_type[i] if has_multiple_types else output_type
                if resolved_filename and isinstance(effective_type, str):
                    self._warn_filename_extension(resolved_filename, effective_type)
                response = self.storage_client.push_result(item,
                                                           type="final",
                                                           output_type=effective_type,
                                                           local_dp=local_dp,
                                                           filename=resolved_filename)
                responses.append(response)
            return responses
        else:
            if isinstance(filename, list):
                self.flame_logger.new_log(
                    "filename list provided for a single result submission — using auto-generated name instead",
                    log_type=LogTypeLiteral.WARNING.value)
                resolved_filename = None
            else:
                resolved_filename = filename
            if resolved_filename and isinstance(output_type, str):
                self._warn_filename_extension(resolved_filename, output_type)
            return self.storage_client.push_result(result,
                                                   type="final",
                                                   output_type=output_type,
                                                   local_dp=local_dp,
                                                   filename=resolved_filename)

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
                returns[remote_node_id] = self.storage_client.push_result(data,
                                                                          remote_node_id=remote_node_id,
                                                                          type=location)
            return returns
        else:
            return self.storage_client.push_result(data, tag=tag, type=location)

    def get_intermediate_data(self,
                              location: Literal["local", "global"],
                              query: Optional[str] = None,
                              tag: Optional[str] = None,
                              tag_option: Optional[Literal["all", "last","first"]] = "all") -> Any:
        """
        returns the intermediate data with the specified query
        :param location: the location to get the result, local gets in the node, global gets in central instance of MinIO
        :param query: the query of the result to get
        :param tag: optional storage tag of targeted local result
        :param tag_option: return mode if multiple tagged data are found
        :return: the result
        """
        return self.storage_client.get_intermediate_data(query=query,
                                                         tag=tag,
                                                         type=location,
                                                         tag_option=tag_option)

    def get_local_tags(self, filter: Optional[str] = None) -> list[str]:
        """
        returns the list of tags used to save local results
        :param filter: filter tags by a substring
        :return: the list of tags
        """
        return self.storage_client.get_local_tags(filter)
