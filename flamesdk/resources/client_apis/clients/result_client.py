from io import BytesIO
from typing import Any, Literal, Optional
from httpx import Client
import pickle
import re


class ResultClient:

    def __init__(self, nginx_name, keycloak_token) -> None:
        self.nginx_name = nginx_name
        self.client = Client(base_url=f"http://{nginx_name}/storage",
                             headers={"Authorization": f"Bearer {keycloak_token}"},
                             follow_redirects=True)

    def refresh_token(self, keycloak_token: str):
        self.client = Client(base_url=f"http://{self.nginx_name}/storage",
                             headers={"Authorization": f"Bearer {keycloak_token}"},
                             follow_redirects=True)

    def push_result(self,
                    result: Any,
                    tag: Optional[str] = None,
                    remote_node_id: Optional[str] = None,
                    type: Literal["final", "global", "local"] = "final",
                    output_type: Literal['str', 'bytes', 'pickle'] = 'pickle') -> dict[str, str]:
        """
        Pushes the result to the hub. Making it available for analysts to download.

        :param result: the Object to push
        :param tag: optional storage tag
        :param remote_node_id: optional remote node id (used for accessing remote node's public key for encryption)
        :param type: location to save the result, final saves in the hub to be downloaded, global saves in central instance of MinIO, local saves in the node
        :param output_type: the type of the result, str, bytes or pickle only for final results
        :return:
        """
        if tag and (type != "local"):
            raise ValueError("Tag can only be used with local type, in current implementation")
        elif remote_node_id and (type != "global"):
            raise ValueError("Remote_node_id can only be used with global type, in current implementation")

        type = "intermediate" if type == "global" else type

        if tag and not re.match(r'^[a-z0-9]{1,2}|[a-z0-9][a-z0-9-]{,30}[a-z0-9]+$', tag):
            raise ValueError("Tag must consist only of lowercase letters, numbers, and hyphens")

        if (type == 'final') and (output_type == 'str'):
            file_body = str(result).encode('utf-8')
        elif (type == 'final') and (output_type == 'bytes'):
            file_body = bytes(result)
        else:
            file_body = pickle.dumps(result)

        if remote_node_id:
            data = {"remote_node_id": remote_node_id}
        elif tag:
            data = {"tag": tag}
        else:
            data = {}
        response = self.client.put(f"/{type}/",
                                   files={"file": BytesIO(file_body)},
                                   data=data,
                                   headers=[('Connection', 'close')])

        print(response.text)
        response.raise_for_status()
        if type != "final":
            print(f"response push_results: {response.json()}")
        else:
            return {"status": "success"}

        return {"status": "success",
                "url": response.json()["url"],
                "id":  response.json()["url"].split("/")[-1]}

    def get_intermediate_data(self,
                              id: Optional[str] = None,
                              tag: Optional[str] = None,
                              type: Literal["local", "global"] = "global",
                              tag_option: Optional[Literal["all", "last", "first"]] = "all") -> Any:
        """
        Returns the intermediate data with the specified id

        :param tag_option: for a tag return the object for all files or the last or the first
        :param id: ID of the intermediate data
        :param tag: optional storage tag of targeted local result
        :param type: location to get the result, local gets in the node, global gets in central instance of MinIO
        :param tag_option: return mode if multiple tagged data are found
        :return:
        """
        if (tag is not None) and (type != "local"):
            raise ValueError("Tag can only be used with local type")
        if (id is None) and (tag is None):
            raise ValueError("Either id or tag should be provided")

        if tag and not re.match(r'^[a-z0-9]{1,2}|[a-z0-9][a-z0-9-]{,30}[a-z0-9]+$', tag):
            raise ValueError("Tag must consist only of lowercase letters, numbers, and hyphens")

        type = "intermediate" if type == "global" else type
        print(f"URL : /{type}/{f'tags/{tag}' if tag is not None else id}")

        if tag:
            urls = self._get_location_url_for_tag(tag)
            if tag_option == "last":
                urls = urls[-1:]
            elif tag_option == "first":
                urls = urls[:1]
            data = []
            for url in urls:
                data.append(self._get_file(url))
            return data
        else:
            return self._get_file(f"/{type}/{id}")

    def _get_location_url_for_tag(self, tag: str) -> str:
        """
        Retrieves the URL associated with the specified tag.
        :param tag:
        :return:
        """
        response = self.client.get(f"/local/tags/{tag}")
        response.raise_for_status()
        urls = []
        for item in response.json()["results"]:
            item["url"] = item["url"].split("/local/")[1]
            urls.append( "/local/" + item["url"])
        return urls

    def _get_file(self, url: str) -> Any:
        """
        Retrieves a file from the specified URL.
        :param url:
        :return:
        """
        response = self.client.get(url)
        response.raise_for_status()
        return pickle.loads(BytesIO(response.content).read())

    def get_local_tags(self, filter: Optional[str] = None) -> list[str]:
        """
        Retrieves all project-specific files and their corresponding URLs.

        This method fetches a list of all project files tagged in the system,
        along with a URL where files with a specific tag can be accessed.
        An optional `filter` can be applied to only return files containing
        the specified substring in their names.

        Args:
            filter (str, optional): A substring to filter the file names.
                                    Only files whose names include this substring
                                    will be returned. Defaults to None.
                                    With None all files are returned.

        Returns:
            list[dict[str, str]]: A list of dictionaries containing file information.
                                  Each dictionary has the following structure:
                                  - "url": The URL associated with the file tag.
                                  - "name": The name of the tag.

                                  Example:
                                  [
                                      {
                                          "url": "http://localhost:8080/local/tags/foobar",
                                          "name": "foobar"
                                      }
                                  ]

        Raises:
            HTTPError: If the request to fetch tags fails.
        """
        response = self.client.get("/local/tags")
        response.raise_for_status()
        tag_name_list = [tag["name"] for tag in response.json()["tags"]]

        if filter is not None:
            tag_name_list = [tag for tag in tag_name_list if filter in tag]

        return tag_name_list
