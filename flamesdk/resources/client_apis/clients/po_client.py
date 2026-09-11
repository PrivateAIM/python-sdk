"""HTTP transport for the PO service sidecar."""

from httpx import Client, HTTPStatusError, ConnectError, TimeoutException

from flamesdk.resources.utils.logging import FlameLogger


class POClient:
    """Thin HTTP transport for the PO service.

    Holds no orchestration logic - see
    :class:`~flamesdk.resources.client_apis.po_api.POAPI` for that.
    """

    def __init__(
        self, nginx_name: str, keycloak_token: str, flame_logger: FlameLogger
    ) -> None:
        """Open a client against the PO service behind the nginx sidecar.

        :param nginx_name: hostname of the local nginx sidecar
        :param keycloak_token: bearer token authenticating this node
        :param flame_logger: logger used to report transport errors
        """
        self.nginx_name = nginx_name
        self.client = Client(
            base_url=f"http://{nginx_name}/po",
            headers={
                "Authorization": f"Bearer {keycloak_token}",
                "accept": "application/json",
            },
            follow_redirects=True,
        )
        self.flame_logger = flame_logger

    def refresh_token(self, keycloak_token: str) -> None:
        """Replace the client with one using a freshly issued token.

        :param keycloak_token: the renewed bearer token
        """
        self.client = Client(
            base_url=f"http://{self.nginx_name}/po",
            headers={
                "Authorization": f"Bearer {keycloak_token}",
                "accept": "application/json",
            },
            follow_redirects=True,
        )

    def stream_logs(
        self, log: str, log_type: str, analysis_id: str, status: str, progress: int
    ) -> None:
        """Post a single log entry to the PO service.

        Submission is best-effort: any transport or unexpected error is written
        to the local log and swallowed, so that logging can never abort the
        analysis it is reporting on.

        :param log: the log message
        :param log_type: type of the log (e.g., 'info', 'error')
        :param analysis_id: id of the analysis the log belongs to
        :param status: status of the log
        :param progress: analysis progress
        """
        log_dict = {
            "log": log,
            "log_type": log_type,
            "analysis_id": analysis_id,
            "status": status,
            "progress": progress,
        }
        try:
            response = self.client.post(
                "/stream_logs",
                json=log_dict,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
        except (HTTPStatusError, ConnectError, TimeoutException) as e:
            self.flame_logger.logger.error(f"HTTP Error in po api: {repr(e)}")
        except Exception as e:
            self.flame_logger.logger.error(f"Unforeseen Error in po api: {repr(e)}")
