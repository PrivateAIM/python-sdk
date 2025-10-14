import sys
import threading
import uvicorn
from typing import Any, Callable, Union, Optional

from fastapi import FastAPI, APIRouter, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import HTTPException
from flamesdk.resources.client_apis.clients.message_broker_client import MessageBrokerClient
from flamesdk.resources.client_apis.clients.data_api_client import DataApiClient
from flamesdk.resources.client_apis.clients.result_client import ResultClient
from flamesdk.resources.client_apis.clients.po_client import POClient
from flamesdk.resources.utils.utils import extract_remaining_time_from_token
from flamesdk.resources.utils.logging import FlameLogger


class FlameAPI:
    def __init__(self,
                 message_broker: MessageBrokerClient,
                 data_client: Union[DataApiClient, Optional[bool]],
                 result_client: ResultClient,
                 po_client: POClient,
                 flame_logger: FlameLogger,
                 keycloak_token: str,
                 progress_call: Callable,
                 finished_check: Callable,
                 finishing_call: Callable) -> None:
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

        self.flame_logger = flame_logger
        self.keycloak_token = keycloak_token
        self.finished = False
        self.get_progress = progress_call
        self.finished_check = finished_check
        self.finishing_call = finishing_call

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
                po_client.refresh_token(new_token)
                # refresh token in message-broker
                message_broker.refresh_token(new_token)
                if isinstance(data_client, DataApiClient):
                    # refresh token in data client
                    data_client.refresh_token(new_token)
                # refresh token in result client
                result_client.refresh_token(new_token)
                # refresh token in self
                self.keycloak_token = new_token
                return JSONResponse(content={"message": "Token refreshed successfully"})
            except Exception as e:
                self.flame_logger.raise_error(f"stack trace {repr(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @router.get("/healthz", response_class=JSONResponse)
        def health() -> dict[str, Union[str, int]]:
            return {"status": self._finished([message_broker, data_client, result_client]),
                    "progress": self.get_progress,
                    "token_remaining_time": extract_remaining_time_from_token(self.keycloak_token, self.flame_logger)}

        async def get_body(request: Request) -> dict[str, Any]:
            return await request.json()

        @router.post("/webhook", response_class=JSONResponse)
        def get_message(msg: dict = Depends(get_body)) -> None:
            if msg['meta']['sender'] != message_broker.nodeConfig.node_id:
                self.flame_logger.new_log(f"received message webhook: {msg}", log_type='info')

            message_broker.receive_message(msg)

            # check message category for finished
            if msg['meta']['category'] == "analysis_finished":
                self.finished = True
                self.finishing_call()

        app.include_router(
            router,
            prefix='',
        )

        uvicorn.run(app, host="0.0.0.0", port=8000)

    def _finished(self, clients: list[Any]) -> str:
        init_failed = None in clients
        main_alive = threading.main_thread().is_alive()

        if init_failed:
            return "stuck"
        elif (not main_alive) and (not self.finished_check()):
            return "failed"
        elif self.flame_logger.runstatus == "failed":
            return "failed"

        try:
            if self.finished:
                return "finished"
            elif self.finished_check():
                self.finished = True
                return "finished"
            else:
                return "running"
        except AttributeError:
            sys.exit()
