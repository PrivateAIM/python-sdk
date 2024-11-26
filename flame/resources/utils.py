from httpx import AsyncClient, ConnectError
import asyncio
from datetime import datetime
from typing import Optional
import time

from flame import FlameCoreSDK

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


def wait_until_partners_ready(flame: FlameCoreSDK,
                              nodes: list[str],
                              attempt_interval: int = 30,
                              timeout: Optional[int] = None) -> dict[str, bool]:
    received = {node: False for node in nodes}

    start_time = datetime.now()

    time_passed = (datetime.now() - start_time).seconds
    while (not all(received.values())) and ((timeout is None) or (time_passed < timeout)):
        acknowledged_list, _ = flame.send_message(receivers=nodes,
                                                 message_category='ready_check',
                                                 message={},
                                                 timeout=attempt_interval)
        for node in acknowledged_list:
            received[node] = True
            nodes.remove(node)

        time.sleep(1)
        time_passed = (datetime.now() - start_time).seconds

    return received
