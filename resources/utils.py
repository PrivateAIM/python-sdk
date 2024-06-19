import os

from httpx import AsyncClient, ConnectError
import asyncio
import time


def get_envs() -> dict[str, str]:
    return {'KEYCLOAK_TOKEN': os.getenv('KEYCLOAK_TOKEN'),
            'DATA_SOURCE_TOKEN': os.getenv('DATA_SOURCE_TOKEN'),
            'NGINX_NAME': f'service-nginx-{os.getenv("DEPLOYMENT_NAME")}'}

def wait_until_nginx_online(envs: dict[str, str]) -> None:
    nginx_is_online = False
    while not nginx_is_online:
        try:
            #print(f"Trying to connect to {envs['NGINX_NAME']}")
            client = AsyncClient(base_url=f"http://{envs['NGINX_NAME']}")
            response = asyncio.run(client.get("/healthz"))
            response.raise_for_status()
            nginx_is_online = True
        except ConnectError as e:
            time.sleep(1)
    print(f"Connected to {envs['NGINX_NAME']}")