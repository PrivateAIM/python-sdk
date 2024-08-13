from io import BytesIO
from typing import Any
from httpx import AsyncClient, HTTPError


class ResultClient:
    def __init__(self, nginx_name, keycloak_token) -> None:
        self.client = AsyncClient(base_url=f"http://{nginx_name}/storage",
                                  headers={"Authorization": f"Bearer {keycloak_token}"})

    async def test_connection(self) -> None:
        await self.push_result(BytesIO(open("../tests/test_images/test_image_main.py", 'rb').read()))

    async def push_result(self, result: BytesIO) -> dict[str, str]:
        response = await self.client.put("/final",
                                         files={"file": result})
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
