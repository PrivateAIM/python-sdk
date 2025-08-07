import pytest
from unittest.mock import MagicMock, AsyncMock
from flamesdk.resources.client_apis.message_broker_api import MessageBrokerAPI
from flamesdk.resources.node_config import NodeConfig
from flamesdk.resources.utils.logging import FlameLogger

class DummyMessageBrokerClient:
    def __init__(self, config, flame_logger):
        self.nodeConfig = config
        self.message_number = 1
    async def get_partner_nodes(self, node_id, analysis_id):
        return ["nodeA", "nodeB"]
    async def send_message(self, *args, **kwargs):
        return (["nodeA"], ["nodeB"])

@pytest.fixture
def dummy_config():
    config = NodeConfig()
    config.node_id = "dummy_node"
    config.analysis_id = "dummy_analysis"
    config.nginx_name = "dummy_nginx"
    config.keycloak_token = "dummy_token"
    return config

@pytest.fixture
def dummy_logger():
    return FlameLogger()

@pytest.fixture
def patch_message_broker_client(monkeypatch):
    monkeypatch.setattr(
        "flamesdk.resources.client_apis.message_broker_api.MessageBrokerClient",
        DummyMessageBrokerClient
    )

@pytest.mark.asyncio
async def test_message_broker_api_init(dummy_config, dummy_logger, patch_message_broker_client):
    api = MessageBrokerAPI(dummy_config, dummy_logger)
    assert api.config == dummy_config
    assert api.participants == ["nodeA", "nodeB"]

@pytest.mark.asyncio
async def test_send_message(dummy_config, dummy_logger, patch_message_broker_client):
    api = MessageBrokerAPI(dummy_config, dummy_logger)
    receivers = ["nodeA", "nodeB"]
    message_category = "test"
    message = {"data": "hello"}
    acknowledged, not_acknowledged = await api.send_message(receivers, message_category, message)
    assert acknowledged == ["nodeA"]
    assert not_acknowledged == ["nodeB"]
