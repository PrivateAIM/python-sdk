from typing import Any
import asyncio
from httpx import AsyncClient, HTTPError


class DataApiClient:
    def __init__(self, project_id: str, envs: dict[str, str]) -> None:
        self.client = AsyncClient(base_url=f"http://{envs['NGINX_NAME']}/kong",
                                  headers={"apikey": envs["DATA_SOURCE_TOKEN"],
                                           "Content-Type": "application/json"})
        self.hub_client = AsyncClient(base_url=f"http://{envs['NGINX_NAME']}/hub-adapter",
                                      headers={"Authorization": f"Bearer {envs['KEYCLOAK_TOKEN']}",
                                               "accept": "application/json"})

        self.project_id = project_id
        self.available_sources = asyncio.run(self.get_available_sources())

        asyncio.run(self.test_connection(project_id))

    async def test_connection(self, project_id: str) -> bool:
        # TODO: find a better way to test the connection
        response = await self.client.get(f"/{project_id}/fhir/Patient?_summary=count",
                                         headers=[('Connection', 'close')])
        try:
            response.raise_for_status()
            return True
        except HTTPError:
            return False

    async def get_available_sources(self) -> list[dict]:
        response = await self.hub_client.get(f"/kong/datastore/{self.project_id}")

        response.raise_for_status()
        return response.json()

    def query_in_image(self, image: str) -> bool:
        return False

    async def get_data(self, query: str = "Patient?_summary=count") -> list[dict]:
        print(self.available_sources)
        paths = [set["paths"][0] for set in self.available_sources["data"]]

        datasets = []
        for path in paths:
            response = await self.client.get(f"{path}/{query}",
                                             headers=[('Connection', 'close')])
            response.raise_for_status()
            datasets.append(response.json())
        return datasets

    def parse_data(self, json_data: dict) -> list[Any]:
        pass

