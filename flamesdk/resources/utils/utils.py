from httpx import AsyncClient, TransportError, HTTPStatusError
import asyncio
import time
import base64
import json

from flamesdk.resources.utils.logging import FlameLogger


def wait_until_nginx_online(nginx_name: str, flame_logger: FlameLogger) -> None:
    flame_logger.new_log("\tConnecting to nginx...", end='', halt_submission=True)
    nginx_is_online = False
    while not nginx_is_online:
        try:
            client = AsyncClient(base_url=f"http://{nginx_name}")
            response = asyncio.run(client.get("/healthz"))
            try:
                response.raise_for_status()
                nginx_is_online = True
            except HTTPStatusError as e:
                flame_logger.new_log(f"{repr(e)}", log_type="warning")
        except TransportError:
            time.sleep(1)
    flame_logger.new_log("success", suppress_head=True)


def extract_remaining_time_from_token(token: str, flame_logger: FlameLogger) -> int:
    """
    Extracts the remaining time until the expiration of the token.
    :param token:
    :param flame_logger:
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
            try:
                raise ValueError("Token does not contain expiration ('exp') claim.")
            except ValueError as e:
                flame_logger.raise_error(f"Error extracting expiration time from token: {repr(e)}")
                return 0

        # Calculate the time remaining until the expiration
        current_time = int(time.time())
        remaining_time = exp_time - current_time
        return remaining_time if remaining_time > 0 else 0
    except Exception as e:
        flame_logger.raise_error(f"{repr(e)}")
        return 0
