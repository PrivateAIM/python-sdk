import asyncio
from httpx import AsyncClient, HTTPError


class DataApiClient:
    def __init__(self, token: str) -> None:
        self.client = AsyncClient(base_url="http://kong-kong-proxy", headers={"apikey": token,
                                                                              "Content-Type": "application/json"})
        self.available_sources = []  # asyncio.run(self._get_available_sources())

    async def test_connection(self) -> bool:
        response = await self.client.get("/project1/fhir/Patient?_summary=count")
        try:
            response.raise_for_status()
            print(response.json())
            return True
        except HTTPError:
            return False

    async def get_available_sources(self) -> list[str]:
        response = await self.client.get("/services")
        response.raise_for_status()
        self.available_sources = response.json()
        return self.available_sources

    def query_in_image(self, image: str) -> bool:
        return False

    async def get_data(self, datasource: str, query: str) -> dict:
        if datasource in self.available_sources:
            # TODO: needs to be cleared up how the call to the data source will be made
            response = await self.client.get(f"/api/data/{datasource}")
            response.raise_for_status()
            return response.json()
        else:
            raise ValueError(f"Data source {datasource} is not available.")
