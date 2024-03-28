import os
import uvicorn
import asyncio

from typing import Callable

from fastapi import FastAPI, APIRouter
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware


class FlameAPI:
    def __init__(self, node_mode: str, converged: Callable) -> None:
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
        def health():
            return {"status": self._finished()}

        app.include_router(
            router,
            prefix=f"/po/{os.getenv('DEPLOYMENT_NAME')}",
        )

        uvicorn.run(app, host="0.0.0.0", port=8000)

    def _finished(self) -> str:
        if asyncio.run(self.converged()):
            return "finished"
        else:
            return "ongoing"
