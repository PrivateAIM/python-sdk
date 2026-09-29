"""FastAPI app exposing this node to the platform and its partner nodes."""

import sys
import threading
import time

import uvicorn
from typing import Callable, Union, Optional, Literal

from fastapi import FastAPI, APIRouter, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import HTTPException
from flamesdk.resources.client_apis.clients.message_broker_client import (
    MessageBrokerClient,
)
from flamesdk.resources.client_apis.clients.data_api_client import DataApiClient
from flamesdk.resources.client_apis.clients.storage_client import StorageClient
from flamesdk.resources.client_apis.clients.po_client import POClient
from flamesdk.resources.utils.utils import extract_remaining_time_from_token
from flamesdk.resources.utils.logging import FlameLogger
from flamesdk.resources.utils.constants import AnalysisStatus, LogTypeLiteral


_SYNC_TIMER_IN_SECONDS = 100


class FlameAPI:
    """The node's inbound HTTP surface, served on port 8000.

    Runs on a background thread for the lifetime of the analysis and answers
    four kinds of request: the platform's health check, token refreshes,
    messages delivered by the broker's webhook, and status exchanges with
    partner nodes.

    Constructing this starts the server and blocks, so it is expected to be
    given a thread of its own.
    """

    def __init__(
        self,
        message_broker: MessageBrokerClient,
        data_client: Union[DataApiClient, Optional[bool]],
        storage_client: StorageClient,
        po_client: POClient,
        flame_logger: FlameLogger,
        keycloak_token: str,
        finished_check: Callable,
        finishing_call: Callable,
        status_sync: tuple[Literal["executed", "stopped", "failed"]] = (),
    ) -> None:
        """Build the app and serve it; does not return while the node runs.

        :param message_broker: broker client fed by the webhook endpoint
        :param data_client: data client to refresh tokens on, if this node has one
        :param storage_client: storage client to refresh tokens on
        :param po_client: PO service client to refresh tokens on
        :param flame_logger: logger holding the run status this API reports
        :param keycloak_token: current bearer token, replaced on refresh
        :param finished_check: predicate telling whether the analysis is done
        :param finishing_call: invoked when a partner reports the analysis finished
        :param status_sync: terminal states this node adopts when a partner
            reports them; an empty tuple disables status syncing
        """
        app = FastAPI(
            title="FLAME node",
            docs_url="/api/docs",
            redoc_url="/api/redoc",
            openapi_url="/api/v1/openapi.json",
        )

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
            """Dependency returning the parsed json body of a request."""
            return await request.json()

        def apply_partner_status_to_self(
            partner_status: dict[
                str,
                Literal[
                    "starting",
                    "started",
                    "executing",
                    "executed",
                    "stopping",
                    "stopped",
                    "failed",
                ],
            ],
        ) -> None:
            """Adopt a partner's terminal status when this node syncs on it.

            Only states listed in ``status_sync`` are adopted, and the first
            match in ``executed``, ``stopped``, ``failed`` order wins.

            :param partner_status: the reporting node's status, per node id
            """
            if (AnalysisStatus.EXECUTED.value in self.status_sync) and (
                AnalysisStatus.EXECUTED.value in partner_status.values()
            ):
                changed_statuses = AnalysisStatus.EXECUTED.value
            elif (AnalysisStatus.STOPPED.value in self.status_sync) and (
                AnalysisStatus.STOPPED.value in partner_status.values()
            ):
                changed_statuses = AnalysisStatus.STOPPED.value
            elif (AnalysisStatus.FAILED.value in self.status_sync) and (
                AnalysisStatus.FAILED.value in partner_status.values()
            ):
                changed_statuses = AnalysisStatus.FAILED.value
            else:
                changed_statuses = None

            if changed_statuses is not None:
                self.flame_logger.new_log(
                    f"Set analysis status to {changed_statuses}, "
                    f"because of partner statuses: {partner_status}",
                    log_type=LogTypeLiteral.INFO.value,
                )
                self.flame_logger.set_runstatus(changed_statuses)

        @router.post("/token_refresh", response_class=JSONResponse)
        async def token_refresh(request: Request) -> JSONResponse:
            """Replace the keycloak token on every sidecar client.

            Called by the platform before the current token expires.

            :param request: request whose json body carries the new ``token``
            :return: a confirmation payload
            """
            try:
                # get body
                body = await request.json()
                new_token = body.get("token")
                if not new_token:
                    self.flame_logger.raise_error("No token provided for refresh")
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
                self.flame_logger.raise_error(
                    "stack trace see in node", hidden_error_msg=repr(e)
                )
                raise HTTPException(status_code=500, detail=str(e))

        @router.post("/webhook", response_class=JSONResponse)
        def get_message(msg: dict = Depends(get_body)) -> None:
            """Receive a message the broker delivered to this node.

            A message in the ``analysis_finished`` category also triggers this
            node's finishing call.

            :param msg: the raw message body
            """
            message_broker.receive_message(msg)

            # check message category for finished
            if msg["meta"]["category"] == "analysis_finished":
                self.finished = True
                self.finishing_call()

        @router.post("/partner_status", response_class=JSONResponse)
        async def get_partner_status(request: Request) -> JSONResponse:
            """Exchange run status with a partner node.

            Partner statuses are ignored for the first few seconds after start,
            so that a node still coming up is not dragged into a terminal state
            by partners that are further along.

            :param request: request whose json body carries ``partner_status``
            :return: this node's current run status
            """
            try:
                if time.time() - self.start_time > _SYNC_TIMER_IN_SECONDS:
                    body = await request.json()
                    partner_status = body.get("partner_status")
                    apply_partner_status_to_self(partner_status)
                    return JSONResponse(content={"status": self.flame_logger.runstatus})
                else:
                    return JSONResponse(content={"status": self.flame_logger.runstatus})
            except Exception as e:
                self.flame_logger.raise_error(
                    "stack trace see in node", hidden_error_msg=repr(e)
                )
                raise HTTPException(status_code=500, detail=str(e))

        @router.get("/healthz", response_class=JSONResponse)
        def health() -> dict[str, Union[str, int]]:
            """Report node health to the platform.

            :return: the node's status and the seconds left on its token
            """
            response_json = {
                "status": self._status(),
                "token_remaining_time": extract_remaining_time_from_token(
                    self.keycloak_token, self.flame_logger
                ),
            }
            self.flame_logger.new_log(
                f"Forwarding status={response_json['status']} via health endpoint",
                log_type=LogTypeLiteral.DEBUG.value,
            )
            return response_json

        app.include_router(
            router,
            prefix="",
        )

        uvicorn.run(app, host="0.0.0.0", port=8000)

    def _status(self) -> str:
        """Derive the status reported to the platform.

        Startup is tolerant, so a missing sidecar client shows up here as
        ``STUCK``. A main thread that died without the analysis finishing means
        the analysis code crashed, which is reported as ``FAILED``. Reaching
        ``EXECUTED`` is sticky: once the finish check passes, the run status is
        updated so later polls stay consistent.

        :return: the :class:`AnalysisStatus` value to report
        """
        init_failed = None in [
            self.message_broker,
            self.data_client,
            self.storage_client,
        ]
        main_alive = threading.main_thread().is_alive()
        self.flame_logger.new_log(
            f"Finished check: runstatus={self.flame_logger.runstatus}, "
            f"init_failed={init_failed}, main_alive={main_alive}",
            log_type=LogTypeLiteral.DEBUG.value,
        )
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
