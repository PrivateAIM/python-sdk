import sys
import threading
import time

import uvicorn
from typing import Any, Callable, Union, Optional, Literal


from fastapi import FastAPI, APIRouter, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import HTTPException
from flamesdk.resources.client_apis.clients.message_broker_client import MessageBrokerClient
from flamesdk.resources.client_apis.clients.data_api_client import DataApiClient
from flamesdk.resources.client_apis.clients.storage_client import StorageClient
from flamesdk.resources.client_apis.clients.po_client import POClient
from flamesdk.resources.utils.utils import extract_remaining_time_from_token
from flamesdk.resources.utils.logging import FlameLogger
from flamesdk.resources.utils.constants import AnalysisStatus, LogTypeLiteral


class FlameAPI:
    def __init__(self,
                 message_broker: MessageBrokerClient,
                 data_client: Union[DataApiClient, Optional[bool]],
                 storage_client: StorageClient,
                 po_client: POClient,
                 flame_logger: FlameLogger,
                 keycloak_token: str,
                 finished_check: Callable,
                 finishing_call: Callable,
                 status_sync: Optional[tuple[Literal['executed', 'stopped', 'failed']]] = None) -> None:
        app = FastAPI(title=f"FLAME node",
                      docs_url="/api/docs",
                      redoc_url="/api/redoc",
                      openapi_url="/api/v1/openapi.json", )

        origins = [
            "http://localhost:8080/",
        ]
        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        router = APIRouter()

        self.message_broker = message_broker
        self.data_client = data_client
        self.storage_client = storage_client
        self.po_client = po_client
        self.flame_logger = flame_logger
        self.keycloak_token = keycloak_token
        self.status_sync = status_sync
        self.finished_check = finished_check
        self.finishing_call = finishing_call
        self.start_time = time.time()
        async def get_body(request: Request) -> dict[str, dict]:
            return await request.json()

        def apply_partner_status_to_self(
                partner_status: dict[str,
                    Literal["starting", "started", "executing", "executed", "stopping", "stopped", "failed"]
                ]
        ) -> None:
            if (AnalysisStatus.EXECUTED.value in self.status_sync) and (AnalysisStatus.EXECUTED.value in partner_status.values()):
                changed_statuses = AnalysisStatus.EXECUTED.value
            elif (AnalysisStatus.STOPPED.value in self.status_sync) and (AnalysisStatus.STOPPED.value in partner_status.values()):
                changed_statuses = AnalysisStatus.STOPPED.value
            elif (AnalysisStatus.FAILED.value in self.status_sync) and (AnalysisStatus.FAILED.value in partner_status.values()):
                changed_statuses = AnalysisStatus.FAILED.value
            else:
                changed_statuses = None

            if changed_statuses is not None:
                self.flame_logger.new_log(f"Set analysis status to {changed_statuses}, "
                                          f"because of partner statuses: {partner_status}",
                                          log_type=LogTypeLiteral.INFO.value)
                self.flame_logger.set_runstatus(changed_statuses)

        @router.post("/token_refresh", response_class=JSONResponse)
        async def token_refresh(request: Request) -> JSONResponse:
            try:
                # get body
                body = await request.json()
                new_token = body.get("token")
                if not new_token:
                    self.flame_logger.raise_error(f"No token provided for refresh")
                    raise HTTPException(status_code=400, detail="Token is required")

                # refresh token in po client
                self.po_client.refresh_token(new_token)
                # refresh token in message-broker
                self.message_broker.refresh_token(new_token)
                if isinstance(data_client, DataApiClient):
                    # refresh token in data client
                    self.data_client.refresh_token(new_token)
                # refresh token in result client
                self.storage_client.refresh_token(new_token)
                # refresh token in self
                self.keycloak_token = new_token
                return JSONResponse(content={"message": "Token refreshed successfully"})
            except Exception as e:
                self.flame_logger.raise_error(f"stack trace {repr(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @router.post("/webhook", response_class=JSONResponse)
        def get_message(msg: dict = Depends(get_body)) -> None:
            message_broker.receive_message(msg)

            # check message category for finished
            if msg['meta']['category'] == "analysis_finished":
                self.finished = True
                self.finishing_call()

        @router.post("/partner_status", response_class=JSONResponse)
        async def get_partner_status(request: Request) -> JSONResponse:
            try:
                if time.time() - self.start_time > 300:
                    body = await request.json()
                    partner_status = body.get("partner_status")
                    apply_partner_status_to_self(partner_status)
                    return JSONResponse(content={"status": self.flame_logger.runstatus})
                else:
                    return JSONResponse(content={"status": self.flame_logger.runstatus})
            except Exception as e:
                self.flame_logger.raise_error(f"stack trace {repr(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @router.get("/healthz", response_class=JSONResponse)
        def health() -> dict[str, Union[str, int]]:
            response_json = {"status": self._finished([self.message_broker, self.data_client, self.storage_client]),
                             "token_remaining_time": extract_remaining_time_from_token(self.keycloak_token,
                                                                                       self.flame_logger)}
            self.flame_logger.new_log(f"Forwarding status={response_json['status']} via health endpoint",
                                      log_type=LogTypeLiteral.DEBUG.value)
            return response_json

        app.include_router(
            router,
            prefix='',
        )

        uvicorn.run(app, host="0.0.0.0", port=8000)

    def _finished(self, clients: list[Any]) -> str:
        init_failed = None in clients
        main_alive = threading.main_thread().is_alive()
        self.flame_logger.new_log(f"Finished check: runstatus={self.flame_logger.runstatus}, "
                                  f"init_failed={init_failed}, main_alive={main_alive}",
                                  log_type=LogTypeLiteral.DEBUG.value)
        if init_failed:
            return AnalysisStatus.STUCK.value
        elif self.flame_logger.runstatus == AnalysisStatus.STOPPED.value:
            return AnalysisStatus.STOPPED.value
        elif (not main_alive) and (not self.finished_check()):
            return AnalysisStatus.FAILED.value
        elif self.flame_logger.runstatus == AnalysisStatus.FAILED.value:
            return AnalysisStatus.FAILED.value

        try:
            if self.flame_logger.runstatus == AnalysisStatus.EXECUTED.value:
                return AnalysisStatus.EXECUTED.value
            elif self.finished_check():
                self.flame_logger.set_runstatus(AnalysisStatus.EXECUTED.value)
                return AnalysisStatus.EXECUTED.value
            else:
                return AnalysisStatus.EXECUTING.value
        except AttributeError:
            sys.exit()

