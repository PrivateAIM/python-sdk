import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi.testclient import TestClient

from flamesdk.resources.client_apis.clients.data_api_client import DataApiClient
from flamesdk.resources.client_apis.clients.message_broker_client import MessageBrokerClient
from flamesdk.resources.client_apis.data_api import DataAPI
from flamesdk.resources.client_apis.message_broker_api import MessageBrokerAPI
from flamesdk.resources.client_apis.po_api import POAPI
from flamesdk.resources.client_apis.storage_api import StorageAPI
from flamesdk.resources.node_config import NodeConfig
from flamesdk.resources.rest_api import FlameAPI


@pytest.mark.parametrize(
    ("authup", "keycloak", "expected"),
    [
        ("authup-token", "legacy-token", "authup-token"),
        ("authup-token", None, "authup-token"),
        (None, "legacy-token", "legacy-token"),
        ("", "legacy-token", "legacy-token"),
        (None, None, None),
    ],
)
def test_node_config_token_preference(monkeypatch, authup, keycloak, expected):
    for name, value in (("AUTHUP_TOKEN", authup), ("KEYCLOAK_TOKEN", keycloak)):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)

    assert NodeConfig().analysis_token == expected


@pytest.mark.parametrize("authup", ["authup-token", None])
def test_node_config_token_reaches_all_client_headers(monkeypatch, authup):
    monkeypatch.setenv("DEPLOYMENT_NAME", "test")
    monkeypatch.setenv("DATA_SOURCE_TOKEN", "data-token")
    monkeypatch.setenv("KEYCLOAK_TOKEN", "legacy-token")
    if authup is None:
        monkeypatch.delenv("AUTHUP_TOKEN", raising=False)
    else:
        monkeypatch.setenv("AUTHUP_TOKEN", authup)
    monkeypatch.setattr(MessageBrokerClient, "_connect", AsyncMock())
    monkeypatch.setattr(
        MessageBrokerClient,
        "get_self_config",
        AsyncMock(return_value={"nodeType": "default", "nodeId": "node-1"}),
    )
    monkeypatch.setattr(MessageBrokerClient, "get_partner_nodes", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        DataApiClient, "_retrieve_available_sources", AsyncMock(return_value=["source"])
    )

    config = NodeConfig()
    logger = Mock()
    broker = MessageBrokerAPI(config, logger).message_broker_client
    data = DataAPI(config, logger).data_client
    storage = StorageAPI(config, logger).storage_client
    po = POAPI(config, logger, 0).po_client
    expected = f"Bearer {authup or 'legacy-token'}"
    try:
        assert broker._message_broker.headers["Authorization"] == expected
        assert data.hub_client.headers["Authorization"] == expected
        assert data.client.headers["apikey"] == "data-token"
        assert storage.client.headers["Authorization"] == expected
        assert po.client.headers["Authorization"] == expected
    finally:
        asyncio.run(broker._message_broker.aclose())
        asyncio.run(data.hub_client.aclose())
        asyncio.run(data.client.aclose())
        storage.client.close()
        po.client.close()


@pytest.mark.parametrize("with_data", [True, False])
def test_health_and_token_refresh(monkeypatch, with_data):
    run = Mock()
    monkeypatch.setattr("flamesdk.resources.rest_api.uvicorn.run", run)
    remaining_time = Mock(side_effect=lambda token, logger: 60)
    monkeypatch.setattr(
        "flamesdk.resources.rest_api.extract_remaining_time_from_token", remaining_time
    )
    broker = Mock()
    storage = Mock()
    po = Mock()
    data = Mock(spec=DataApiClient) if with_data else None
    api = FlameAPI(broker, data, storage, po, Mock(), "old-token", lambda: False, Mock())
    run.assert_called_once()
    app = run.call_args.args[0]

    with TestClient(app) as client:
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.json() == {
            "status": "executing" if with_data else "stuck",
            "token_remaining_time": 60,
        }
        assert remaining_time.call_args.args[0] == "old-token"

        response = client.post("/token_refresh", json={"token": "new-token"})
        assert response.status_code == 200
        assert response.json() == {"message": "Token refreshed successfully"}
        assert client.get("/healthz").status_code == 200
        assert remaining_time.call_args.args[0] == "new-token"

    for downstream in (broker, storage, po):
        downstream.refresh_token.assert_called_once_with("new-token")
    if data is not None:
        data.refresh_token.assert_called_once_with("new-token")
    assert api.analysis_token == "new-token"
