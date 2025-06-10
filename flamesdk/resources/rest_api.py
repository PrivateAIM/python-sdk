import sys
import threading
import uvicorn
from typing import Any, Callable, Union

from fastapi import FastAPI, APIRouter, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import HTTPException
from flamesdk.resources.client_apis.clients.message_broker_client import MessageBrokerClient
from flamesdk.resources.client_apis.clients.data_api_client import DataApiClient
from flamesdk.resources.client_apis.clients.result_client import ResultClient
from flamesdk.resources.utils.utils import extract_remaining_time_from_token
from flamesdk.resources.utils.logging import flame_log


class FlameAPI:
    def __init__(self,
                 message_broker: MessageBrokerClient,
                 data_client: DataApiClient,
                 result_client: ResultClient,
                 keycloak_token: str,
                 silent: bool,
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

        self.keycloak_token = keycloak_token
        self.finished = False
        self.finished_check = finished_check
        self.finishing_call = finishing_call

        @router.post("/token_refresh", response_class=JSONResponse)
        async def token_refresh(request: Request) -> JSONResponse:
            try:
                # get body
                body = await request.json()
                new_token = body.get("token")
                if not new_token:
                    flame_log("No token, raising HTTPException", silent)
                    raise HTTPException(status_code=400, detail="Token is required")

                # refresh token in message-broker
                message_broker.refresh_token(new_token)
                # refresh token in data client
                data_client.refresh_token(new_token)
                # refresh token in result client
                result_client.refresh_token(new_token)
                # refresh token in self
                self.keycloak_token = new_token
                return JSONResponse(content={"message": "Token refreshed successfully"})
            except Exception as e:
                flame_log(f"stack trace {e}", silent)
                raise HTTPException(status_code=500, detail=str(e))

        @router.get("/healthz", response_class=JSONResponse)
        def health() -> dict[str, Union[str, int]]:
            return {"status": self._finished([message_broker, data_client, result_client]),
                    "token_remaining_time": extract_remaining_time_from_token(self.keycloak_token)}

        async def get_body(request: Request) -> dict[str, Any]:
            return await request.json()

        @router.post("/webhook", response_class=JSONResponse)
        def get_message(msg: dict = Depends(get_body)) -> None:
            if msg['meta']['sender'] != message_broker.nodeConfig.node_id:
                flame_log(f"received message webhook: {msg}", silent)

            message_broker.receive_message(msg, silent)

            # check message category for finished
            if msg['meta']['category'] == "analysis_finished":
                self.finished = True
                self.finishing_call()

        app.include_router(
            router,
            prefix='',
        )

        uvicorn.run(app, host="0.0.0.0", port=8000)

    def _finished(self,
                  clients: list[Any]) -> str:
        init_failed = None in clients
        main_alive = threading.main_thread().is_alive()

        if init_failed:
            return "stuck"
        elif (not main_alive) and (not self.finished_check()):
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
