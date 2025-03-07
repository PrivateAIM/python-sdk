from httpx import AsyncClient, ConnectError
import asyncio
import time


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
