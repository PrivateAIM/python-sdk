from httpx import AsyncClient, HTTPError


class DataApiClient:
    def __init__(self, data_source_client: str) -> None:
        self.available_sources = []
        self.token = data_source_client
        self.base_url = "http://kong-kong-proxy"
        self.client = AsyncClient(base_url=self.base_url, headers={"apikey": self.token,
                                                                   "Content-Type": "application/json"})

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

    async def get_data(self, datasource: str) -> dict:
        if datasource in self.available_sources:
            # TODO: needs to be cleared up how the call to the data source will be made
            response = await self.client.get(f"/api/data/{datasource}")
            response.raise_for_status()
            return response.json()
        else:
            raise ValueError(f"Data source {datasource} is not available.")
