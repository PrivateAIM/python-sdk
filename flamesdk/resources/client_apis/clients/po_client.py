from httpx import Client, HTTPError

from flamesdk.resources.utils.logging import FlameLogger


class POClient:
    def __init__(self, nginx_name: str, keycloak_token: str, flame_logger: FlameLogger) -> None:
        self.nginx_name = nginx_name
        self.client = Client(base_url=f"http://{nginx_name}/po",
                             headers={"Authorization": f"Bearer {keycloak_token}",
                                      "accept": "application/json"},
                             follow_redirects=True)
        self.flame_logger = flame_logger

    def refresh_token(self, keycloak_token: str) -> None:
        self.client = Client(base_url=f"http://{self.nginx_name}/po",
                             headers={"Authorization": f"Bearer {keycloak_token}",
                                      "accept": "application/json"},
                             follow_redirects=True)

    def stream_logs(self, log: str, log_type: str, analysis_id: str, status: str, progress: int) -> None:
        log_dict = {
            "log": log,
            "log_type": log_type,
            "analysis_id": analysis_id,
            "status": status,
            #"progress": progress
        }
        response = self.client.post("/stream_logs",
                                    json=log_dict,
                                    headers={"Content-Type": "application/json"})
        try:
            response.raise_for_status()
        except HTTPError as e:
            print("HTTP Error in po api:", repr(e))
        except Exception as e:
            print("Unforeseen Error in po api:", repr(e))
