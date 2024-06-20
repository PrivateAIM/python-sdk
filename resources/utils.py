from httpx import AsyncClient, ConnectError
import asyncio
import time


def wait_until_nginx_online(nginx_name) -> None:
    nginx_is_online = False
    while not nginx_is_online:
        try:
            #print(f"Trying to connect to {nginx_name}")
            client = AsyncClient(base_url=f"http://{nginx_name}")
            response = asyncio.run(client.get("/healthz"))
            response.raise_for_status()
            nginx_is_online = True
        except ConnectError as e:
            time.sleep(1)
    print(f"Connected to {nginx_name}")