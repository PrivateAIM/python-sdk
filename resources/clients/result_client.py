from typing import Any, IO
from httpx import AsyncClient, HTTPError


class ResultClient:
    def __init__(self, nginx_name, keycloak_token) -> None:
        self.client = AsyncClient(base_url=f"http://{nginx_name}/storage",
                                  headers={"Authorization": f"Bearer {keycloak_token}"})

    async def test_connection(self) -> None:
        await self.push_result("test_image_main.py")

    async def push_result(self, result: IO) -> dict[str, str]:
        result_path = "result.txt"
        self._write_result(result, result_path)
        response = await self.client.put("/upload/",
                                         data=result,
                                         headers=[('Connection', 'close')])
        response.raise_for_status()
        if response.status_code != 204:
            raise HTTPError
        return {"status": "success"}

    def _write_result(self, result: Any, result_path: str) -> None:
        with open(result_path, 'w') as f:
            f.write(str(result))

    def list_local_results(self):
        pass

    def list_global_results(self):
        pass

    def get_intermediate_data_local(self, id):
        pass

    def get_intermediate_data_global(self, id):
        pass
