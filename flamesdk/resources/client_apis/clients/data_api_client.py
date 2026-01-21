from typing import Optional, Any
import asyncio
from httpx import AsyncClient, HTTPStatusError
import re
from flamesdk.resources.utils.logging import FlameLogger


class DataApiClient:
    def __init__(self, project_id: str, nginx_name: str, data_source_token: str, keycloak_token: str, flame_logger: FlameLogger) -> None:
        self.nginx_name = nginx_name
        self.flame_logger = flame_logger
        self.client = AsyncClient(base_url=f"http://{nginx_name}/kong",
                                  headers={"apikey": data_source_token,
                                           "Content-Type": "application/json"},
                                  follow_redirects=True)
        self.hub_client = AsyncClient(base_url=f"http://{nginx_name}/hub-adapter",
                                      headers={"Authorization": f"Bearer {keycloak_token}",
                                               "accept": "application/json"},
                                      follow_redirects=True)

        self.project_id = project_id
        self.available_sources = asyncio.run(self._retrieve_available_sources())

    def refresh_token(self, keycloak_token: str) -> None:
        self.hub_client = AsyncClient(base_url=f"http://{self.nginx_name}/hub-adapter",
                                      headers={"Authorization": f"Bearer {keycloak_token}",
                                               "accept": "application/json"},
                                      follow_redirects=True)

    def get_available_sources(self) -> list[dict[str, Any]]:
        return self.available_sources

    def get_data(self,
                 s3_keys: Optional[list[str]] = None,
                 fhir_queries: Optional[list[str]] = None) -> Optional[list[dict[str, Any]]]:
        if (s3_keys is None) and ((fhir_queries is None) or (len(fhir_queries) == 0)):
            return None
        dataset_sources = []
        for source in self.available_sources:
            datasets = {}
            # get fhir data
            if fhir_queries is not None:
                for fhir_query in fhir_queries:  # premise: retrieves data for each fhir_query from each data source
                    response = asyncio.run(self.client.get(f"{source['name']}/fhir/{fhir_query}",
                                                           headers=[('Connection', 'close')]))
                    try:
                        response.raise_for_status()
                    except HTTPStatusError as e:
                        self.flame_logger.new_log(f"Failed to retrieve fhir data for query {fhir_query} "
                                                  f"from source {source['name']}: {repr(e)}", log_type='warning')
                        continue
                    datasets[fhir_query] = response.json()
            # get s3 data
            else:
                response_names = asyncio.run(self._get_s3_dataset_names(source['name']))
                for res_name in response_names:  # premise: only retrieves data corresponding to s3_keys from each data source
                    if (len(s3_keys) == 0) or (res_name in s3_keys):
                        response = asyncio.run(self.client.get(f"{source['name']}/s3/{res_name}",
                                                               headers=[('Connection', 'close')]))
                        try:
                            response.raise_for_status()
                        except HTTPStatusError as e:
                            self.flame_logger.raise_error(f"Failed to retrieve s3 data for key {res_name} "
                                                          f"from source {source['name']}: {repr(e)}")
                        datasets[res_name] = response.content
            dataset_sources.append(datasets)
        return dataset_sources

    async def _get_s3_dataset_names(self, source_name: str) -> list[str]:
        response = await self.client.get(f"{source_name}/s3", headers=[('Connection', 'close')])
        try:
            response.raise_for_status()
        except HTTPStatusError as e:
            self.flame_logger.raise_error(f"Failed to retrieve S3 dataset names from source {source_name}: {repr(e)}")
        responses = re.findall(r'<Key>(.*?)</Key>', str(response.text))
        return responses

    def get_data_source_client(self, data_id: str) -> AsyncClient:
        """
        Returns the data client for a specific fhir or S3 store used for this project.
        :param data_id:
        :return:
        """
        path = None
        for source in self.available_sources:
            if source["id"] == data_id:
                path = source["paths"][0]
        if path is None:
            self.flame_logger.raise_error(f"Data source with id={data_id} not found")
        client = AsyncClient(base_url=f"{path}")
        return client

    async def _retrieve_available_sources(self) -> list[dict[str, Any]]:
        response = await self.hub_client.get(f"/kong/datastore/{self.project_id}")
        try:
            response.raise_for_status()
        except HTTPStatusError as e:
            self.flame_logger.raise_error(f"Failed to retrieve available data sources for project {self.project_id}:"
                                      f" {repr(e)}")

        return response.json()['data']
