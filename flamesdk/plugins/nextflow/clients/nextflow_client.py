from pydantic import BaseModel
from httpx import Client, HTTPStatusError, ConnectError, TimeoutException

from flamesdk.resources.node_config import NodeConfig
from flamesdk.resources.utils.logging import FlameLogger
from flamesdk.resources.utils.constants import LogTypeLiteral


class NFRunManifest(BaseModel):
    run_id: str
    analysis_id: str

    end_status: str
    result_urls: dict[str, str]

    started_at: str
    finished_at: str


class NextFlowClient(Client):
    def __init__(self,
                 config: NodeConfig,
                 flame_logger: FlameLogger) -> None:
        self.config = config
        self.flame_logger = flame_logger

        self.run_id = None
        self.run_manifest = None

        super().__init__(
            base_url=f"http://{self.config.nginx_name}/nextflow",
            headers={
                "Authorization": f"Bearer {self.config.keycloak_token}",
                "accept": "application/json",
                "Connection": "close"
            },
            follow_redirects=True,
        )

    def start_nf_run(self,
                     pipeline_name: str,
                     run_args: list[str],
                     s3_keys: list[str] = None) -> bool:
        body = {
            "analysis_id": self.config.analysis_id,
            "pipeline_name": pipeline_name,
            "run_args": run_args,
            "keycloak_token": self.config.keycloak_token,
            "inputs": [{"key": s3_keys, "param_name": "input", "samplesheet": True}],
            "kong_apikey": self.config.data_source_token
        }
        try:
            responses = self.post("/run", data=body)
            self.run_id = responses.json()["run_id"]
            self.flame_logger.new_log(
                f"Successfully started nf-run (run_id={self.run_id})",
                log_type=LogTypeLiteral.INFO.value
            )
            return True
        except (HTTPStatusError, ConnectError, TimeoutException) as e:
            self.flame_logger.raise_error("Failed to start nf-run", hidden_error_msg=repr(e))
            return False

    def receive_feedback(self, body: NFRunManifest) -> None:
        self.run_manifest = body

    def reset_feedback(self) -> None:
        self.run_manifest = None

    def stop_nf_run(self, salvaged_paths: list[str] = []) -> bool:
        body = {
            "analysis_id": self.config.analysis_id,
            "salvaged_paths": salvaged_paths
        }
        try:
            response = self.post("/delete", data=body)
            response.raise_for_status()
            return True
        except (HTTPStatusError, ConnectError, TimeoutException) as e:
            self.flame_logger.raise_error("Failed to stop nf-run", hidden_error_msg=repr(e))
            return False
