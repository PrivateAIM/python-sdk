from httpx import AsyncClient
from typing import Optional

from flamesdk.resources.client_apis.clients.data_api_client import DataApiClient
from flamesdk.resources.node_config import NodeConfig
from flamesdk.resources.utils.logging import FlameLogger

class DataAPI:
    def __init__(self, config: NodeConfig, flame_logger: FlameLogger) -> None:
        self.data_client = DataApiClient(config.project_id,
                                         config.nginx_name,
                                         config.data_source_token,
                                         config.keycloak_token,
                                         flame_logger= flame_logger)

    def get_data_client(self, data_id: str) -> AsyncClient:
        """
        Returns the data client for a specific fhir or S3 store used for this project.
        :param data_id: the id of the data source
        :return: the data client
        """
        return self.data_client.get_data_source_client(data_id)

    def get_data_sources(self) -> list[str]:
        """
        Returns a list of all data sources available for this project.
        :return: the list of data sources
        """
        return self.data_client.available_sources

    def get_fhir_data(self, fhir_queries: Optional[list[str]] = None) -> list[dict[str, dict]]:
        """
        Returns the data from the FHIR store for each of the specified fhir queries.
        :param fhir_queries: list of fhir queries to get the data
        :return: the data
        """
        return self.data_client.get_data(fhir_queries=fhir_queries)

    def get_s3_data(self, s3_keys: Optional[list[str]] = None) -> list[dict[str, str]]:
        """
        Returns s3 data for each key
        :param s3_keys: name of s3 datasets
        :return:
        """
        return self.data_client.get_data(s3_keys=s3_keys)
