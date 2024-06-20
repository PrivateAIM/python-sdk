from typing import Any
from httpx import AsyncClient, HTTPError


class ResultClient:
    def __init__(self, nginx_name, keycloak_token) -> None:
        self.client = AsyncClient(base_url=f"http://{nginx_name}/storage",
                                  headers={"Authorization": f"Bearer {keycloak_token}"})

    async def test_connection(self) -> None:
        await self.push_result("test_image_main.py")

    async def push_result(self, result: Any) -> dict:
        result_path = "result.txt"
        self._write_result(result, result_path)
        response = await self.client.put("/upload/",
                                         files={"file": open(result_path, "rb")},
                                         headers=[('Connection', 'close')])
        response.raise_for_status()
        if response.status_code != 204:
            raise HTTPError
        return {"status": "success"}

    def _write_result(self, result: Any, result_path: str) -> None:
        with open(result_path, 'w') as f:
            f.write(str(result))

