from io import BytesIO
from typing import Any, Literal
from httpx import AsyncClient, HTTPError
import pickle


class ResultClient:

    def __init__(self, nginx_name, keycloak_token) -> None:
        self.client = AsyncClient(base_url=f"http://{nginx_name}/storage",
                                  headers={"Authorization": f"Bearer {keycloak_token}"},
                                  follow_redirects=True)

    async def push_result(self,
                          result: Any,
                          type: Literal["final", "global", "local"] = "final",
                          output_type: Literal['str', 'bytes', 'pickle'] = 'pickle') -> dict[str, str]:
        """
        Pushes the result to the hub. Making it available for analysts to download.

        :param result: the Object to push
        :param type: location to save the result, final saves in the hub to be downloaded, global saves in central instance of MinIO, local saves in the node
        :param output_type: the type of the result, str, bytes or pickle only for final results
        :return:
        """
        type = "intermediate" if type == "global" else type

        if (type == 'final') and (output_type == 'str'):
            file_body = str(result).encode('utf-8')
        elif (type == 'final') and (output_type == 'bytes'):
            file_body = bytes(result)
        else:
            file_body = pickle.dumps(result)

        response = await self.client.put(f"/{type}/",
                                         files={"file": BytesIO(file_body)},
                                         headers=[('Connection', 'close')])

        response.raise_for_status()
        print(f"response push_results: {response.json()}")

        return {"status": "success",
                "url": response.json()["url"],
                "id":  response.json()["url"].split("/")[-1]}

    async def get_intermediate_data(self, id: str, type: Literal["local", "global"] = "global") -> Any:
        """
        Returns the intermediate data with the specified id

        :param id: ID of the intermediate data
        :param type: location to get the result, local gets in the node, global gets in central instance of MinIO
        :return:
        """
        type = "intermediate" if type == "global" else type
        print(f"URL : /{type}/{id}")
        response = await self.client.get(f"/{type}/{id}", headers=[('Connection', 'close')])
        response.raise_for_status()

        return pickle.loads(BytesIO(response.content).read())
