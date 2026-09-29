"""High-level API for reaching the project's FHIR and S3 data sources."""

from httpx import AsyncClient
from typing import Optional, Union, Any

from flamesdk.resources.client_apis.clients.data_api_client import DataApiClient
from flamesdk.resources.node_config import NodeConfig
from flamesdk.resources.utils.logging import FlameLogger


class DataAPI:
    """Access to the data sources attached to this project.

    Only connected for nodes that actually read data - that is, nodes whose
    role is ``'default'``, or aggregators started with
    ``aggregator_requires_data=True``.
    """

    def __init__(
        self,
        config: NodeConfig,
        flame_logger: FlameLogger,
        default_requires_data: bool = True,
    ) -> None:
        """Connect to the data sources registered for this project.

        :param config: node configuration providing the project id and tokens
        :param flame_logger: logger used to report connection problems
        :param default_requires_data: whether a node without an explicit data
            requirement should still be given its project's data sources
        """
        self.data_client = DataApiClient(
            project_id=config.project_id,
            nginx_name=config.nginx_name,
            data_source_token=config.data_source_token,
            keycloak_token=config.keycloak_token,
            flame_logger=flame_logger,
            default_requires_data=default_requires_data,
        )

    def get_data_client(self, data_id: str) -> AsyncClient:
        """
        Returns the data client for a specific fhir or S3 store used for this project.
        :param data_id: the id of the data source
        :return: the data client
        """
        return self.data_client.get_data_source_client(data_id)

    def get_data_sources(self) -> list[dict[str, Any]]:
        """
        Returns a list of all data sources available for this project.
        :return: the list of data sources
        """
        return self.data_client.available_sources

    def get_fhir_data(
        self, fhir_queries: Optional[list[str]] = []
    ) -> Optional[list[dict[str, Union[str, dict]]]]:
        """
        Returns the data from the FHIR store for each of the specified fhir queries.

        :param fhir_queries: list of fhir queries to get the data
        :return: the data, or ``None`` if ``fhir_queries`` is ``None``
        """
        if fhir_queries is None:
            return None
        return self.data_client.get_data(fhir_queries=fhir_queries)

    def get_s3_data(
        self, s3_keys: Optional[list[str]] = []
    ) -> Optional[list[dict[str, bytes]]]:
        """
        Returns s3 data for each key

        :param s3_keys: name of s3 datasets
        :return: one mapping of key to raw bytes per requested key, or ``None``
            if ``s3_keys`` is ``None``
        """
        if s3_keys is None:
            return None
        return self.data_client.get_data(s3_keys=s3_keys)
