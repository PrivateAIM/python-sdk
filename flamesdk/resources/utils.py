import string
from typing import Union
from httpx import AsyncClient, ConnectError
import asyncio
import time
import base64
import json


def wait_until_nginx_online(nginx_name: str, silent: bool) -> None:
    flame_log("\tConnecting to nginx...", silent, end='')
    nginx_is_online = False
    while not nginx_is_online:
        try:
            client = AsyncClient(base_url=f"http://{nginx_name}")
            response = asyncio.run(client.get("/healthz"))
            response.raise_for_status()
            nginx_is_online = True
        except ConnectError:
            time.sleep(1)
    flame_log("success", silent, suppress_head=True)


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
        exp_time = payload.get("exp")
        if exp_time is None:
            raise ValueError("Token does not contain expiration ('exp') claim.")

        # Calculate the time remaining until the expiration
        current_time = int(time.time())
        remaining_time = exp_time - current_time
        return remaining_time if remaining_time > 0 else 0
    except Exception as e:
        raise ValueError(f"Invalid token: {str(e)}")

def flame_log(msg: Union[str, bytes],
              silent: bool,
              sep: str = ' ',
              end: str = '\n',
              file = None,
              flush: bool = False,
              suppress_head: bool = False) -> None:
    """
    Print logs to console, if silent is set to False.
    :param msg:
    :param silent:
    :param sep:
    :param end:
    :param file:
    :param flush:
    :param suppress_head:
    :return:
    """
    if not silent:
        if isinstance(msg, bytes):
            msg = msg.decode('utf-8', errors='replace')
        msg_cleaned = ''.join(filter(lambda x: x in string.printable, msg))
        head = f"[flame {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}] " if not suppress_head else ""
        print(f"{head}{msg_cleaned}", sep=sep, end=end, file=file, flush=flush)
