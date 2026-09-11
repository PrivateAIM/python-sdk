"""High-level API for reporting analysis logs to the PO service."""

from flamesdk.resources.client_apis.clients.po_client import POClient
from flamesdk.resources.node_config import NodeConfig
from flamesdk.resources.utils.logging import FlameLogger
from flamesdk.resources.utils.constants import LogTypeLiteral


class POAPI:
    """Forwards analysis logs to the PO service, filtered by severity.

    Only logs whose severity reaches ``stream_log_level`` leave the node; the
    rest stay in the container's local log.
    """

    def __init__(
        self, config: NodeConfig, flame_logger: FlameLogger, stream_log_level: int
    ) -> None:
        """Open the PO service connection for this analysis.

        :param config: node configuration providing the sidecar host and token
        :param flame_logger: logger used to report transport errors
        :param stream_log_level: minimum numeric severity that is submitted,
            compared against :attr:`LogTypeLiteral.level`
        """
        self.po_client = POClient(
            config.nginx_name, config.keycloak_token, flame_logger
        )
        self.analysis_id = config.analysis_id
        self.stream_log_level = stream_log_level

    def stream_logs(self, log: str, log_type: str, status: str, progress: int) -> None:
        """
        Streams logs to the PO service.
        :param log: the log message
        :param log_type: type of the log (e.g., 'info', 'error')
        :param status: status of the log
        :param progress: analysis progress
        """
        if LogTypeLiteral(log_type).level >= self.stream_log_level:
            self.po_client.stream_logs(
                log, log_type, self.analysis_id, status, progress
            )
