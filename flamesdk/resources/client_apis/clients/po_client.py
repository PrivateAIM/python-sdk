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
            "status": status
        }
        print("Sending logs to PO:", log_dict)
        response = await self.client.post("/stream_logs",
                                         json=log_dict,
                                         headers={"Content-Type": "application/json"},
                                         timeout=120.0)
        try:
            response.raise_for_status()
            print("Successfully streamed logs to PO")
        except HTTPError as e:
            self.flame_logger.new_log(f"Failed to stream logs to PO: {repr(e)}", log_type='error')
        except Exception as e:
            print("Unforeseen Error:", repr(e))
