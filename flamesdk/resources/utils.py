from httpx import AsyncClient, ConnectError
import asyncio
import time
import jwt

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
    try:
        # Decode the token without verifying the signature
        payload = jwt.decode(token, options={"verify_signature": False})
        exp_time = payload.get("exp")
        if exp_time is None:
            raise ValueError("Token does not contain expiration ('exp') claim.")

        # Calculate the time remaining until the expiration
        current_time = int(time.time())
        remaining_time = exp_time - current_time
        return remaining_time if remaining_time > 0 else 0
    except Exception as e:
        raise ValueError(f"Invalid token: {str(e)}")