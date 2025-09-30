import asyncio

from flamesdk.resources.client_apis.clients.po_client import POClient
from flamesdk.resources.node_config import NodeConfig
from flamesdk.resources.utils.logging import FlameLogger

class POAPI:
    def __init__(self, config: NodeConfig, flame_logger: FlameLogger) -> None:
        self.po_client = POClient(config.nginx_name, config.keycloak_token, flame_logger)
        self.analysis_id = config.analysis_id

    def stream_logs(self, log: str, log_type: str, status: str) -> None:
        """
        Streams logs to the PO service.
        :param log: the log message
        :param log_type: type of the log (e.g., 'info', 'error')
        :param status: status of the log
        """
        self.po_client.stream_logs(log, log_type, self.analysis_id, status)
