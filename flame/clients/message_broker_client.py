import os

from httpx import AsyncClient, HTTPError


class _Message:
    def __init__(self, receiver: str, message: str) -> None:
        self.sender = os.environ["SENDER"]
        self.receiver = receiver
        self.message = message


class MessageBrokerClient:
    def __init__(self) -> None:
        self._host = os.getenv("MESSAGE_BROKER_HOST")
        self._port = os.getenv("MESSAGE_BROKER_PORT")
        self._token = os.getenv("MESSAGE_BROKER_TOKEN")

    async def _connect(self) -> None:
        # TODO find out how to connect to the message broker
        await self._message_broker.connect(host=self._host, port=self._port, token=self._token)

    async def test_connection(self) -> bool:
        response = await self._message_broker.get("/healthz")
        try:
            response.raise_for_status()
            return True
        except HTTPError:
            return False

    async def _ask_central_analyzer_or_aggregator(self) -> str:
        message = _Message( receiver="central_analyses_management", message="status_aggregator_or_analyzer")
        self._send(message)
        answer = await self._receive()
        # TODO must be checked if feald is correct
        return answer['node_mode']

    def _send(self, message: _Message):
        self._message_broker.send(message)

    def _receive(self) -> _Message:
        return self._message_broker.receive()
