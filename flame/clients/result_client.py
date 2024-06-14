from typing import Any
from httpx import AsyncClient, HTTPError


class ResultClient:
    def __init__(self, envs: dict[str, str]) -> None:
        self.client = AsyncClient(base_url=f"http://{envs['NGINX_NAME']}/storage",
                                  headers={"Authorization": f"Bearer {envs['KEYCLOAK_TOKEN']}"})

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

