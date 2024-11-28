import sys
import uvicorn
from typing import Any, Callable

from fastapi import FastAPI, APIRouter, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from flame.resources.client_apis.clients.message_broker_client import MessageBrokerClient


class FlameAPI:
    def __init__(self, message_broker: MessageBrokerClient, finished_check: Callable, finishing_call: Callable) -> None:
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

        self.finished = False
        self.finished_check = finished_check
        self.finishing_call = finishing_call

        @router.get("/healthz", response_class=JSONResponse)
        def health() -> dict[str, str]:
            return {"status": self._finished()}

        async def get_body(request: Request) -> dict[str, Any]:
            return await request.json()

        @router.post("/webhook", response_class=JSONResponse)
        def get_message(msg: dict = Depends(get_body)) -> None:
            if msg['meta']['sender'] != message_broker.nodeConfig.node_id:
                print(f"received message webhook: {msg}")

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

    def _finished(self) -> str:
        try:
            # print(f"finished:\n\tself:{self.finished},\n\tcheck:{self.finished_check()}")
            if self.finished:
                return "finished"
            elif self.finished_check():
                self.finished = True
                return "finished"
            else:
                return "ongoing"
        except AttributeError:
            sys.exit()
