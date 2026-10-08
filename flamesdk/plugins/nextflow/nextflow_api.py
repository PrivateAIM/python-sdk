import time

from typing import Optional

from flamesdk.resources.node_config import NodeConfig
from flamesdk.resources.utils.logging import FlameLogger
from flamesdk.resources.utils.constants import LogTypeLiteral

from flamesdk.plugins.nextflow.clients.nextflow_client import NextFlowClient, NFRunManifest


_TERMINATION_CHECK_INTERVAL_SECONDS = 10


class NextFlowAPI:
    def __init__(self, config: NodeConfig, flame_logger: FlameLogger) -> None:
        self.client = NextFlowClient(config, flame_logger)
        self.flame_logger = flame_logger
        self.run_locked = False

    def start_nf_run(self, pipeline_name: str, run_args: list[str], s3_keys: list[str] = None) -> None:
        if not self.run_locked:
            if self.client.start_nf_run(pipeline_name, run_args, s3_keys):
                self.run_locked = True
        else:
            self.flame_logger.new_log(
                "A Next Flow Run is already locked. Wait until it is finished before starting a new one.",
                log_type=LogTypeLiteral.WARNING.value
            )

    def stop_nf_run(self, salvaged_paths: list[str] = []) -> None:
        if self.run_locked:
            if self.client.stop_nf_run(salvaged_paths):
                self.run_locked = False
                self.client.reset_feedback()
        else:
            self.flame_logger.new_log(
                "No Next Flow Run found. Ignoring attempt to stop it.",
                log_type=LogTypeLiteral.WARNING.value
            )

    def await_termination(self) -> Optional[NFRunManifest]:
        while self.run_locked:
            current_manifest = self.client.run_manifest.copy()
            if current_manifest is not None:
                self.run_locked = False
                self.client.reset_feedback()
                return current_manifest
            else:
                time.sleep(_TERMINATION_CHECK_INTERVAL_SECONDS)
        return None
