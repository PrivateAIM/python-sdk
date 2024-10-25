from io import BytesIO
from typing import Any, Literal
from httpx import AsyncClient, HTTPError
import pickle


class ResultClient:
    def __init__(self, nginx_name, keycloak_token) -> None:
        self.client = AsyncClient(base_url=f"http://{nginx_name}/storage",
                                  headers={"Authorization": f"Bearer {keycloak_token}"},
                                  follow_redirects=True)

    async def push_result(self, result: Any, type: Literal["final", "global", "local"] = "final") -> dict[str, str]:
        type = "intermediate" if type == "global" else type
        response = await self.client.put(f"/{type}/",
                                         files={"file": BytesIO(pickle.dumps(result))},
                                         headers=[('Connection', 'close')])
        response.raise_for_status()
        print(f"respones push_results: {response.json()}")

        return {"status": "success",
                "url": response.json()["url"],
                "id": response.json()["object_id"]}

    def _write_result(self, result: Any, result_path: str) -> None:
        with open(result_path, 'w') as f:
            f.write(str(result))

    def list_results(self, type: Literal["local", "global"] = "global") -> list[str]:
        # Endpoint not implemented in the result service
        pass

    async def get_intermediate_data(self, id: str, type: Literal["local", "global"] = "global") -> Any:
        type = "intermediate" if type == "global" else type
        print(f"URL : /{type}/{id}")
        response = await self.client.get(f"/{type}/{id}",headers=[('Connection', 'close')])
        response.raise_for_status()


        return pickle.loads(BytesIO(response.content).read())
