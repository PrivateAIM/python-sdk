from httpx import AsyncClient, HTTPError

from ..utils.token import get_token


class DataApiClient:
    def __init__(self, available_sources: list[str]) -> None:
        self.available_sources = available_sources
        self.token = get_token()
        self.client = AsyncClient(base_url=self.base_url)
        self.test_connection()

    async def test_connection(self) -> bool:
        response = await self.client.get("/kong/healthz")
        try:
            response.raise_for_status()
            return True
        except HTTPError:
            return False

    async def get_data(self, datasource: str) -> dict:
        if datasource in self.available_sources:
            # TODO: needs to be cleared up how the call to the data source will be made
            response = await self.client.get(f"/api/data/{datasource}")
            response.raise_for_status()
            return response.json()
        else:
            raise ValueError(f"Data source {datasource} is not available.")
