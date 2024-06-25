from httpx import AsyncClient

from typing import List, Literal, IO


class DataAPI:
    def __init__(self):
        pass
        # TODO Implement this

    def get_data_client(self, data_id: str) -> AsyncClient:
        """
        Returns the data client for a specific fhir or S3 store used for this project.
        :param data_id: the id of the data source
        :return: the data client
        """
        pass
        # TODO Implement this

    def get_data_sources(self) -> List[str]:
        """
        Returns a list of all data sources available for this project.
        :return: the list of data sources
        """
        pass
        # TODO Implement this

    def get_fhir_data(self, data_id: str, queries: List[str]) -> List[dict]:
        """
        Returns the data from the FHIR store for each of the specified queries.
        :param data_id: the id of the data source
        :param query: the query to get the data
        :return: the data
        """
        pass
        # TODO Implement this

    def get_s3_data(self, key: str, local_path: str) -> IO:
        """
        Returns the data from the S3 store associated with the given key.
        :param key:
        :param local_path:
        :return:
        """
