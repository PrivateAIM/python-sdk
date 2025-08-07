from typing import Optional, Union
import asyncio
from httpx import AsyncClient, HTTPError

from flamesdk.resources.utils.logging import FlameLogger

class POClient:
    def __init__(self, nginx_name: str, keycloak_token: str, flame_logger: FlameLogger) -> None:
        self.nginx_name = nginx_name
        self.client = AsyncClient(base_url=f"http://{nginx_name}/po",
                                  headers={"Authorization": f"Bearer {keycloak_token}",
                                           "accept": "application/json"},
                                  follow_redirects=True)
        self.flame_logger = flame_logger

    def refresh_token(self, keycloak_token: str):
        self.client = AsyncClient(base_url=f"http://{self.nginx_name}/po",
                                  headers={"Authorization": f"Bearer {keycloak_token}",
                                           "accept": "application/json"},
                                  follow_redirects=True)

    async def stream_logs(self,
                          log: str,
                          log_type: str,
                          analysis_id: str,
                          node_id: str,
                          status: str) -> None:
        log_dict = {
            "log": log,
            "log_type": log_type,
            "analysis_id": analysis_id,
            "node_id": node_id,
            "status": status
        }
        response = await self.client.put("/stream_logs",
                                         data=log_dict,
                                         headers={"Content-Type": "application/json"})
        try:
            response.raise_for_status()
        except HTTPError as e:
            self.flame_logger.new_log("Failed to stream logs to PO", log_type='error')
            self.flame_logger.new_log(repr(e))
