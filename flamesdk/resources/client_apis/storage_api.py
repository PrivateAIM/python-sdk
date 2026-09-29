"""High-level API for submitting results and exchanging intermediate data."""

from pathlib import PurePath
from typing import Any, Literal, Optional, Union

from flamesdk.resources.client_apis.clients.storage_client import (
    StorageClient,
    LocalDifferentialPrivacyParams,
    EXT_TO_OUTPUT_TYPE,
)
from flamesdk.resources.node_config import NodeConfig
from flamesdk.resources.utils.constants import LogTypeLiteral
from flamesdk.resources.utils.logging import FlameLogger


class StorageAPI:
    """Submits analysis results and exchanges intermediate data between nodes.

    Final results are published to the hub for analysts to download, while
    intermediate data is shared between the nodes of a running analysis, either
    globally or tagged for local reuse.
    """

    def __init__(self, config: NodeConfig, flame_logger: FlameLogger) -> None:
        """Open the storage/result service connection for this node.

        :param config: node configuration providing the sidecar host and token
        :param flame_logger: logger used to report submission problems
        """
        self.flame_logger = flame_logger
        self.storage_client = StorageClient(
            config.nginx_name, config.keycloak_token, flame_logger
        )

    def submit_final_result(
        self,
        result: Any,
        output_type: Union[Literal["str", "bytes", "pickle"], list] = "str",
        multiple_results: bool = False,
        filename: Optional[Union[str, list[str]]] = None,
        local_dp: Optional[LocalDifferentialPrivacyParams] = None,
    ) -> Union[dict[str, str], list[dict[str, str]]]:
        """
        sends the final result to the hub. Making it available for analysts to download.
        This method is only available for nodes for which the method `get_role(self)` returns "aggregator".
        :param result: the final result (single object or list of objects)
        :param output_type: output type of final results (default: "str")
        :param multiple_results: whether the result is to be split into separate results (per element in tuple) or a single result
        :param filename: optional filename for the result file on the hub. For multiple_results, pass a list of names
                         (one per element) or a single string (auto-indexed as name_1, name_2, …). Defaults to an
                         auto-generated name when None.
        :param local_dp: parameters for local differential privacy, only for final floating-point type results
        :return: the request status code (single dict if result is not a list, list of dicts if result is a list)
        """
        requested_multiple = multiple_results
        result_is_sequence = isinstance(result, (list, tuple))
        result_len = len(result) if requested_multiple and result_is_sequence else 1
        multiple_results = result_len != 1
        output_type, filename = self._check_multi_result_validity(
            multiple_result=multiple_results,
            output_type=output_type,
            filename=filename,
            result_length=result_len,
        )
        # Check if result is a tuple or list and multiple_results is true
        if multiple_results and isinstance(result, (list, tuple)):
            # Submit each element in the list separately
            responses = []
            for i, item in enumerate(result):
                if isinstance(filename, list):
                    resolved_filename = filename[i]
                elif isinstance(filename, str):
                    p = PurePath(filename)
                    output_type_i = (
                        output_type[i] if isinstance(output_type, list) else output_type
                    )
                    ext = (
                        p.suffix
                        if p.suffix in EXT_TO_OUTPUT_TYPE[output_type_i]
                        else EXT_TO_OUTPUT_TYPE[output_type_i][0]
                    )
                    resolved_filename = f"{p.stem}_{i + 1}{ext}"
                else:
                    resolved_filename = None
                effective_type = (
                    output_type[i] if isinstance(output_type, list) else output_type
                )
                if resolved_filename and isinstance(effective_type, str):
                    self._warn_filename_extension(resolved_filename, effective_type)
                response = self.storage_client.push_result(
                    item,
                    type="final",
                    output_type=effective_type,
                    filename=resolved_filename,
                    local_dp=local_dp,
                )
                responses.append(response)
            return responses
        else:
            if requested_multiple and not result_is_sequence:
                self.flame_logger.new_log(
                    f"Warning! Given multiple_results={requested_multiple}, "
                    f"but result is neither of type 'list' nor 'tuple' "
                    f"(found {type(result)} instead) -> multiple_results will be ignored.",
                    log_type=LogTypeLiteral.WARNING.value,
                )
            # A filename list has already been reduced to its first element by
            # _check_multi_result_validity, so filename is a plain string or None here.
            resolved_filename = filename
            if resolved_filename and isinstance(output_type, str):
                self._warn_filename_extension(resolved_filename, output_type)
            return self.storage_client.push_result(
                result,
                type="final",
                output_type=output_type,
                local_dp=local_dp,
                filename=resolved_filename,
            )

    def save_intermediate_data(
        self,
        data: Any,
        location: Literal["global", "local"],
        remote_node_ids: Optional[list[str]] = None,
        tag: Optional[str] = None,
    ) -> Union[dict[str, dict[str, str]], dict[str, str]]:
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
                returns[remote_node_id] = self.storage_client.push_result(
                    data, remote_node_id=remote_node_id, type=location
                )
            return returns
        else:
            return self.storage_client.push_result(data, tag=tag, type=location)

    def get_intermediate_data(
        self,
        location: Literal["local", "global"],
        query: Optional[str] = None,
        tag: Optional[str] = None,
        tag_option: Optional[Literal["all", "last", "first"]] = "all",
    ) -> Any:
        """
        returns the intermediate data with the specified query
        :param location: the location to get the result, local gets in the node, global gets in central instance of MinIO
        :param query: the query of the result to get
        :param tag: optional storage tag of targeted local result
        :param tag_option: return mode if multiple tagged data are found
        :return: the result
        """
        return self.storage_client.get_intermediate_data(
            query=query, tag=tag, type=location, tag_option=tag_option
        )

    def get_local_tags(self, filter: Optional[str] = None) -> list[str]:
        """
        returns the list of tags used to save local results
        :param filter: filter tags by a substring
        :return: the list of tags
        """
        return self.storage_client.get_local_tags(filter)

    def _check_multi_result_validity(
        self,
        multiple_result: bool,
        output_type: Union[str, list[str]],
        filename: Optional[Union[str, list[str]]],
        result_length: int,
    ) -> tuple[Union[str, list[str]], Optional[Union[str, list[str]]]]:
        """Validate and normalise the ``output_type``/``filename`` arguments.

        Rejects unknown output types and lists that are shorter than the result
        they describe, then reduces the arguments to the simplest form that
        still covers every result: a list whose entries are all identical
        collapses to a single value, and a list longer than the result list is
        truncated with a warning.

        Length is checked before any reduction, so a too-short list is rejected
        whether or not its entries happen to be uniform.

        :param multiple_result: whether the result is submitted as several files
        :param output_type: requested output type, or one per result
        :param filename: requested filename, or one per result
        :param result_length: number of results being submitted
        :return: the normalised ``(output_type, filename)`` pair
        """
        # General output type check
        if isinstance(output_type, str) and (
            output_type not in EXT_TO_OUTPUT_TYPE.keys()
        ):
            self.flame_logger.raise_error(
                f"Invalid output_type={output_type} given (allowed output_types={EXT_TO_OUTPUT_TYPE.keys()})"
            )
        elif isinstance(output_type, list) and any(
            [o not in EXT_TO_OUTPUT_TYPE.keys() for o in output_type]
        ):
            self.flame_logger.raise_error(
                f"Found at least one invalid output_type in given list of output_types={output_type} "
                f"(allowed output_types={EXT_TO_OUTPUT_TYPE.keys()})"
            )

        # Length checks run against the lists as given, before any reduction, so that a
        # too-short list is rejected whether or not its elements happen to be uniform.
        if multiple_result:
            if isinstance(output_type, list) and (len(output_type) < result_length):
                self.flame_logger.raise_error(
                    f"Output_type list length={len(output_type)} has to be at least as large as result "
                    f"list length={result_length}."
                )
            if isinstance(filename, list) and (len(filename) < result_length):
                self.flame_logger.raise_error(
                    f"Filename list length={len(filename)} has to be at least as large as result "
                    f"list length={result_length}."
                )

        # Reduce lists if they only contain one element
        if isinstance(output_type, list) and (len(set(output_type)) == 1):
            output_type = output_type[0]
        if isinstance(filename, list) and (len(set(filename)) == 1):
            filename = filename[0]

        # Check output_type to filename relation
        if multiple_result:
            if isinstance(output_type, list) and (len(output_type) > result_length):
                self.flame_logger.new_log(
                    f"Output_type list length={len(output_type)} is larger than result list length={result_length} - "
                    f"using first {result_length} elements of output_type list as output_types",
                    log_type=LogTypeLiteral.WARNING.value,
                )
                output_type = output_type[:result_length]
            if isinstance(filename, list) and (len(filename) > result_length):
                self.flame_logger.new_log(
                    f"Filename list length={len(filename)} is larger than result list length={result_length} - "
                    f"using first {result_length} elements of filename list as filenames",
                    log_type=LogTypeLiteral.WARNING.value,
                )
                filename = filename[:result_length]
            if (result_length == 1) and isinstance(output_type, list):
                output_type = output_type[0]
            if (result_length == 1) and isinstance(filename, list):
                filename = filename[0]
        else:
            if isinstance(output_type, list):
                self.flame_logger.new_log(
                    "Output_type list provided for a single result submission -"
                    " using first element of list as output_type instead",
                    log_type=LogTypeLiteral.WARNING.value,
                )
                output_type = output_type[0]
            if isinstance(filename, list):
                self.flame_logger.new_log(
                    "Filename list provided for a single result submission -"
                    " using first element of list as filename instead",
                    log_type=LogTypeLiteral.WARNING.value,
                )
                filename = filename[0]
        return output_type, filename

    def _warn_filename_extension(self, filename: str, output_type: str) -> None:
        """Warn when a filename's extension does not match its output type.

        A file named ``result.pkl`` but submitted as ``'str'`` is most likely a
        mistake and would be hard to read back, so it is flagged. Filenames
        without an extension are left alone.

        :param filename: the resolved filename for the result
        :param output_type: the output type the result is serialised as
        """
        ext = PurePath(filename).suffix.lower()
        allowed_ext = EXT_TO_OUTPUT_TYPE[output_type]
        if ext and (ext not in allowed_ext):
            self.flame_logger.new_log(
                f"Filename extension '{ext}' not in expected extensions='{allowed_ext}' "
                f"for output_type='{output_type}' - the file may be unreadable after download.",
                log_type=LogTypeLiteral.WARNING.value,
            )
