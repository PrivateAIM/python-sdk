"""HTTP transport for the project's FHIR and S3 data sources."""

from typing import Optional, Any
import asyncio
from httpx import AsyncClient, HTTPStatusError, ConnectError, TimeoutException, Timeout
import re


from flamesdk.resources.utils.logging import FlameLogger
from flamesdk.resources.utils.constants import LogTypeLiteral


class DataApiClient:
    """Thin HTTP transport for the data sources attached to a project.

    Talks to two sidecars: the hub adapter, which lists the data sources
    registered for the project, and kong, which proxies the sources themselves.
    The source list is fetched once during construction and cached on
    :attr:`available_sources`.
    """

    def __init__(
        self,
        project_id: str,
        nginx_name: str,
        data_source_token: str,
        keycloak_token: str,
        flame_logger: FlameLogger,
        default_requires_data: bool = True,
    ) -> None:
        """Connect to the project's data sources and cache the source list.

        :param project_id: id of the project whose data sources are used
        :param nginx_name: hostname of the local nginx sidecar
        :param data_source_token: api key authenticating against kong
        :param keycloak_token: bearer token authenticating against the hub adapter
        :param flame_logger: logger used to report connection problems
        :param default_requires_data: when ``True``, an empty or unavailable
            source list is treated as fatal
        :raises ValueError: if the sources cannot be retrieved, or if the
            project has none and ``default_requires_data`` is ``True``
        """
        self.nginx_name = nginx_name
        self.flame_logger = flame_logger
        self.client = AsyncClient(
            base_url=f"http://{nginx_name}/kong",
            headers={"apikey": data_source_token, "Content-Type": "application/json"},
            follow_redirects=True,
        )
        self.hub_client = AsyncClient(
            base_url=f"http://{nginx_name}/hub-adapter",
            headers={
                "Authorization": f"Bearer {keycloak_token}",
                "accept": "application/json",
            },
            follow_redirects=True,
        )

        self.project_id = project_id
        self.available_sources = asyncio.run(
            self._retrieve_available_sources(default_requires_data)
        )
        if default_requires_data and (not self.available_sources):
            if self.available_sources == []:
                self.flame_logger.new_log(
                    f"No data sources found for project {project_id}",
                    log_type=LogTypeLiteral.CRITICAL.value,
                )
                raise ValueError(f"No data sources found for project {project_id}")
            else:
                self.flame_logger.new_log(
                    f"Failed to retrieve available data sources for project {project_id}",
                    log_type=LogTypeLiteral.WARNING.value,
                )
                raise ValueError(
                    f"Failed to retrieve available data sources for project {project_id}"
                )

    def refresh_token(self, keycloak_token: str) -> None:
        """Replace the hub adapter client with one using a fresh token.

        :param keycloak_token: the renewed bearer token
        """
        self.hub_client = AsyncClient(
            base_url=f"http://{self.nginx_name}/hub-adapter",
            headers={
                "Authorization": f"Bearer {keycloak_token}",
                "accept": "application/json",
            },
            follow_redirects=True,
        )

    def get_available_sources(self) -> list[dict[str, Any]]:
        """Return the data sources cached at construction time.

        :return: one descriptor per data source registered for the project
        """
        return self.available_sources

    def get_data(
        self,
        s3_keys: Optional[list[str]] = None,
        fhir_queries: Optional[list[str]] = None,
    ) -> Optional[list[dict[str, Any]]]:
        """Retrieve FHIR or S3 data from every source available to this project.

        FHIR takes precedence: if ``fhir_queries`` is given, each query is run
        against each source and S3 is not consulted. A FHIR query that fails
        against one source is logged and skipped, so a partial result set is
        returned rather than none at all.

        :param s3_keys: names of the S3 datasets to fetch; an empty list fetches
            every dataset the source offers
        :param fhir_queries: fhir queries to run against each source
        :return: one mapping of query/key to payload per data source, or
            ``None`` if neither argument selects anything
        """
        if (s3_keys is None) and ((fhir_queries is None) or (len(fhir_queries) == 0)):
            return None
        dataset_sources = []
        for source in self.available_sources:
            datasets = {}
            # get fhir data
            if fhir_queries is not None:
                for fhir_query in (
                    fhir_queries
                ):  # premise: retrieves data for each fhir_query from each data source
                    try:
                        response = asyncio.run(
                            self.client.get(
                                f"{source['name']}/fhir/{fhir_query}",
                                headers=[("Connection", "close")],
                                timeout=Timeout(5, write=None, read=None),
                            )
                        )
                        response.raise_for_status()
                    except (HTTPStatusError, ConnectError, TimeoutException) as e:
                        self.flame_logger.new_log(
                            f"Failed to retrieve fhir data for query {fhir_query} "
                            f"from source {source['name']}",
                            log_type=LogTypeLiteral.WARNING.value,
                            hidden_error_msg=repr(e),
                        )
                        continue
                    datasets[fhir_query] = response.json()
            # get s3 data
            else:
                response_names = asyncio.run(self._get_s3_dataset_names(source["name"]))
                for res_name in response_names:  # premise: only retrieves data corresponding to s3_keys from each data source
                    if (len(s3_keys) == 0) or (res_name in s3_keys):
                        try:
                            response = asyncio.run(
                                self.client.get(
                                    f"{source['name']}/s3/{res_name}",
                                    headers=[("Connection", "close")],
                                    timeout=Timeout(5, write=None, read=None),
                                )
                            )
                            response.raise_for_status()
                        except (HTTPStatusError, ConnectError, TimeoutException) as e:
                            self.flame_logger.raise_error(
                                f"Failed to retrieve s3 data for key {res_name} "
                                f"from source {source['name']}",
                                hidden_error_msg=repr(e),
                            )
                        datasets[res_name] = response.content
            dataset_sources.append(datasets)
        return dataset_sources

    async def _get_s3_dataset_names(self, source_name: str) -> list[str]:
        """List the S3 object keys a source holds.

        :param source_name: name of the data source to list
        :return: the object keys parsed out of the bucket listing
        """
        try:
            response = await self.client.get(
                f"{source_name}/s3", headers=[("Connection", "close")]
            )
            response.raise_for_status()
        except (HTTPStatusError, ConnectError, TimeoutException) as e:
            self.flame_logger.raise_error(
                f"Failed to retrieve S3 dataset names from source {source_name}",
                hidden_error_msg=repr(e),
            )
        responses = re.findall(r"<Key>(.*?)</Key>", str(response.text))
        return responses

    def get_data_source_client(self, data_id: str) -> AsyncClient:
        """
        Returns the data client for a specific fhir or S3 store used for this project.

        :param data_id: the id of the data source to open a client for
        :return: a client whose base url points at that data source
        """
        path = None
        for source in self.available_sources:
            if source["id"] == data_id:
                path = source["paths"][0]
        if path is None:
            self.flame_logger.raise_error(f"Data source with id={data_id} not found")
        client = AsyncClient(base_url=f"{path}")
        return client

    async def _retrieve_available_sources(
        self, default_requires_data: bool = True
    ) -> list[dict[str, Any]]:
        """Ask the hub adapter which data sources this project may use.

        :param default_requires_data: when ``True``, a failed lookup is also
            reported as a critical log before the error is raised
        :return: one descriptor per registered data source
        :raises ValueError: if the hub adapter cannot be reached
        """
        try:
            response = await self.hub_client.get(f"/kong/datastore/{self.project_id}")
            response.raise_for_status()
        except (HTTPStatusError, ConnectError, TimeoutException) as e:
            if default_requires_data:
                self.flame_logger.new_log(
                    f"Failed to retrieve available data sources for project "
                    f"{self.project_id}",
                    log_type=LogTypeLiteral.CRITICAL.value,
                )
            raise ValueError(
                f"Failed to retrieve available data sources for project {self.project_id}: {repr(e)}"
            )

        return response.json()["data"]
