from httpx import AsyncClient, HTTPError


class ResultClient:
    def __init__(self, token: str) -> None:
        self.client = AsyncClient(base_url="http://node-result-service:8080", headers={"apikey": token, "Content-Type": "application/json"})

    async def test_connection(self) -> None:
        result = await self.push_result("test_image_main.py")
        print(result)

    async def push_result(self, result_path: str) -> None:
        response = await self.client.put("/upload/", files={"file": open(result_path, "rb")})
        response.raise_for_status()
        return response.json()
