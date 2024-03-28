from httpx import AsyncClient, HTTPError


class ResultClient:
    def __init__(self, token: str) -> None:
        self.client = AsyncClient(base_url="http://node-result-service:8080",
                                  headers={"Authorization": f"Bearer {token}"})

    async def test_connection(self) -> None:
        await self.push_result("test_image_main.py")

    async def push_result(self, result_path: str) -> dict:
        response = await self.client.put("/upload/",
                                         files={"file": open(result_path, "rb")},
                                         headers=[('Connection', 'close')])
        response.raise_for_status()
        if response.status_code != 204:
            raise HTTPError
        return {"status": "success"}
