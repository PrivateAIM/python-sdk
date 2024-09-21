from typing import Any, Optional, Union
import asyncio
from httpx import AsyncClient, HTTPError
import re

class DataApiClient:
    def __init__(self, project_id: str, nginx_name: str, data_source_token: str, keycloak_token: str) -> None:
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

    async def _retrieve_available_sources(self) -> list[dict[str, str]]:
        response = await self.hub_client.get(f"/kong/datastore/{self.project_id}")
        response.raise_for_status()
        return [{'name': source['name']} for source in response.json()['data']]

    def get_available_sources(self):
        return self.available_sources

    def get_data(self, s3_keys: Optional[list[str]] = None, fhir_queries: Optional[list[str]] = None) \
            -> list[dict[str, Union[dict, str]]]:
        dataset_sources = []
        for source in self.available_sources:
            datasets = {}
            if fhir_queries is not None:
                for fhir_query in fhir_queries:  # premise: retrieves data for each fhir_query from each data source
                    response = asyncio.run(self.client.get(f"{source['name']}/fhir/{fhir_query}",
                                                     headers=[('Connection', 'close')]))
                    response.raise_for_status()
                    datasets[fhir_query] = response.json()
            else:
                response_names = asyncio.run(self._get_s3_dataset_names(source['name']))
                print(f"dataset names {response_names}")
                for res_name in response_names:  # premise: only retrieves data corresponding to s3_keys from each data source
                    print(f"res_name {res_name}")
                    if (s3_keys is None) or (res_name in s3_keys):
                        response = asyncio.run(self.client.get(f"{source['name']}/s3/{res_name}",
                                                         headers=[('Connection', 'close')]))
                        print(f"response {response}")
                        print(f"response.text {response.text}")
                        response.raise_for_status()
                        datasets[res_name] = response.text
            dataset_sources.append(datasets)
        return dataset_sources

    async def _get_s3_dataset_names(self, source_name: str) -> list[str]:
        response = await self.client.get(f"{source_name}/s3", headers=[('Connection', 'close')])
        response.raise_for_status()

        responses = re.findall(r'<Key>(.*?)</Key>', str(response.text))
        return responses

    def get_data_source_client(self, data_id: str) -> AsyncClient:
        """
        Returns the data client for a specific fhir or S3 store used for this project.
        :param data_id:
        :return:
        """
        path = None
        for sources in self.available_sources:
            if sources["id"] == data_id:
                path = sources["paths"][0]
        if path is None:
            raise ValueError(f"Data source with id {data_id} not found")
        client = AsyncClient(base_url=f"{path}",)
        return client

    def parse_data(self, json_data: dict) -> list[Any]:
        pass

