from httpx import AsyncClient, ConnectError
import asyncio
import time
import re
import base64
import json

def wait_until_nginx_online(nginx_name) -> None:
    print("\tConnecting to nginx...", end='')
    nginx_is_online = False
    while not nginx_is_online:
        try:
            client = AsyncClient(base_url=f"http://{nginx_name}")
            response = asyncio.run(client.get("/healthz"))
            response.raise_for_status()
            nginx_is_online = True
        except ConnectError:
            time.sleep(1)
    print("success")


def extract_remaining_time_from_token(token: str) -> int:
    """
    Extracts the remaining time until the expiration of the token.
    :param token:
    :return: int in seconds until the expiration of the token
    """
    try:
        token = token.split(".")[1]
        missing_padding = len(token) % 4
        if missing_padding != 0:
            token += "=" * (4 - missing_padding)
        payload = base64.b64decode(token).decode("utf-8")
        payload = json.loads(payload)
        print(payload)
        exp_time = payload.get("exp")
        if exp_time is None:
            raise ValueError("Token does not contain expiration ('exp') claim.")

        # Calculate the time remaining until the expiration
        current_time = int(time.time())
        remaining_time = exp_time - current_time
        return remaining_time if remaining_time > 0 else 0
    except Exception as e:
        raise ValueError(f"Invalid token: {str(e)}")

def flame_log(msg: str, sep: str = ' ', end: str = '\n', file = None, flush: bool = False) -> None:
    msg_cleaned = re.sub(r'[^\x00-\x7f]', '?', msg)
    print(f"[flame {time.time()}] {msg_cleaned}", sep=sep, end=end, file=file, flush=flush)