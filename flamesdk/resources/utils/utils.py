"""Startup and token helpers shared by the SDK's client APIs."""

from httpx import AsyncClient, Timeout, TransportError, HTTPStatusError
import asyncio
import time
import base64
import json

from flamesdk.resources.utils.logging import FlameLogger
from flamesdk.resources.utils.constants import LogTypeLiteral


def wait_until_nginx_online(nginx_name: str, flame_logger: FlameLogger) -> None:
    """Block until the local nginx sidecar answers its health check.

    Polls ``/healthz`` once a second and only returns once the sidecar responds
    successfully, so the caller can assume the sidecar is reachable afterwards.
    Transport errors are treated as "not up yet" and retried indefinitely; an
    HTTP error status is logged as a warning and also retried.

    :param nginx_name: hostname of the sidecar, e.g. ``nginx-<DEPLOYMENT_NAME>``
    :param flame_logger: logger used to report progress and connection warnings
    """
    flame_logger.new_log("\tConnecting to nginx...", end="", halt_submission=True)
    nginx_is_online = False
    while not nginx_is_online:
        try:
            client = AsyncClient(base_url=f"http://{nginx_name}")
            response = asyncio.run(
                client.get("/healthz", timeout=Timeout(5, connect=60.05, pool=3.05))
            )
            try:
                response.raise_for_status()
                nginx_is_online = True
            except HTTPStatusError as e:
                flame_logger.new_log(
                    "HTTPStatusError while waiting for nginx",
                    log_type=LogTypeLiteral.WARNING.value,
                    hidden_error_msg=repr(e),
                )
        except TransportError:
            time.sleep(1)
    flame_logger.new_log("success", append=True)


def extract_remaining_time_from_token(token: str, flame_logger: FlameLogger) -> int:
    """
    Extracts the remaining time until the expiration of the token.

    The token is not verified - only its payload segment is base64-decoded to
    read the ``exp`` claim. A token that already expired yields ``0`` rather
    than a negative number.

    :param token: the JWT whose ``exp`` claim should be read
    :param flame_logger: logger used to report a missing claim or a decode error
    :return: int in seconds until the expiration of the token
    """
    try:
        token = token.split(".")[1]
        missing_padding = len(token) % 4
        if missing_padding != 0:
            token += "=" * (4 - missing_padding)
        payload = base64.b64decode(token).decode("utf-8")
        payload = json.loads(payload)
        exp_time = payload.get("exp")
        if exp_time is None:
            flame_logger.raise_error(
                "Error extracting expiration time from token: "
                "Token does not contain expiration ('exp') claim."
            )
            return 0

        # Calculate the time remaining until the expiration
        current_time = int(time.time())
        remaining_time = exp_time - current_time
        return remaining_time if remaining_time > 0 else 0
    except Exception as e:
        flame_logger.raise_error(
            "Error extracting remaining time from token", hidden_error_msg=repr(e)
        )
