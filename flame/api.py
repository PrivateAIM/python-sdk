import json
import os
import sys


import uvicorn
import asyncio

from typing import Callable, Any

from fastapi import FastAPI, APIRouter, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from flame.clients.message_broker_client import MessageBrokerClient


class FlameAPI:
    def __init__(self, node_mode: str, message_broker: MessageBrokerClient, converged: Callable) -> None:
        app = FastAPI(title=f"FLAME {'Analysis' if node_mode == 'analyzer' else 'Aggregation'}",
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

        self.converged = converged

        @router.get("/healthz", response_class=JSONResponse)
        def health() -> dict[str, str]:
            return {"status": self._finished()}

        async def get_body(request: Request)-> dict[str, Any]:
            return await request.json()
        @router.post("/webhook", response_class=JSONResponse)
        def get_message(msg: dict = Depends(get_body)) -> None:
            message_broker.receive_message(msg)

        app.include_router(
            router,
            prefix='',
        )

        uvicorn.run(app, host="0.0.0.0", port=8000)

    def _finished(self) -> str:
        try:
            if self.converged():
                return "finished"
            else:
                return "ongoing"
        except AttributeError:
            sys.exit()
