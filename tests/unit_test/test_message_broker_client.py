import os
import pytest
import asyncio
import datetime
from httpx import Response, Request
#from asyncmock import AsyncMock
from flamesdk.resources.client_apis.clients.message_broker_client import MessageBrokerClient, Message
from flamesdk.resources.utils.logging import FlameLogger

# Dummy stub for NodeConfig used by MessageBrokerClient.
class DummyNodeConfig:
    def __init__(self):
        self.node_id = "dummy_node"
        self.analysis_id = "dummy_analysis"
        self.nginx_name = "dummy_nginx"
        self.keycloak_token = "dummy_token"

    def set_role(self, role: str):
        self.role = role

    def set_node_id(self, node_id: str):
        self.node_id = node_id

# Set environment variable needed by MessageBrokerClient.
@pytest.fixture(autouse=True)
def set_analysis_id(monkeypatch):
    monkeypatch.setenv("ANALYSIS_ID", "test_analysis")

# Patch get_self_config so that __init__ does not perform real network calls.
@pytest.fixture
def dummy_get_self_config():
    async def fake_get_self_config(self, analysis_id: str):
        return {"nodeType": "test_role", "nodeId": "test_node"}
    return fake_get_self_config

# Create a test client with patched network methods.
@pytest.fixture
def client(monkeypatch, dummy_get_self_config):
    monkeypatch.setattr(MessageBrokerClient, "get_self_config", dummy_get_self_config)
    async def fake_connect(self):
        pass
    monkeypatch.setattr(MessageBrokerClient, "_connect", fake_connect)
    flame_logger = FlameLogger()
    return MessageBrokerClient(DummyNodeConfig(), flame_logger)

def test_refresh_token(client):
    new_token = "new_dummy_token"
    client.refresh_token(new_token)
    updated_auth = client._message_broker.headers.get("Authorization")
    assert updated_auth == f"Bearer {new_token}"

def test_delete_message_by_id(client):
    # Create a dummy outgoing message without 'meta' field.
    msg_body = {
        "data": "test message"
    }
    dummy_message = Message(message=msg_body, config=client.nodeConfig, outgoing=True,
                            message_number=1, category="test", recipients=["rec1"])
    client.list_of_outgoing_messages.append(dummy_message)
    deleted_count = client.delete_message_by_id(dummy_message.body["meta"]["id"], "outgoing")
    assert deleted_count == 1
    # Verify the message is removed from the outgoing list
    assert all(m.body["meta"]["id"] != dummy_message.body["meta"]["id"] for m in client.list_of_outgoing_messages)

def test_clear_messages(client):
    # Create dummy incoming messages with different status.
    current_time = str(datetime.datetime.now())
    msg_body_read = {
        "data": "message1",
        "meta": {
            "id": "msg-1",
            "sender": "nodeA",
            "status": "read",
            "type": "incoming",
            "category": "test",
            "number": 1,
            "created_at": current_time,
            "arrived_at": None,
            "akn_id": "nodeX",
        }
    }
    msg_body_unread = {
        "data": "message2",
        "meta": {
            "id": "msg-2",
            "sender": "nodeA",
            "status": "unread",
            "type": "incoming",
            "category": "test",
            "number": 2,
            "created_at": current_time,
            "arrived_at": None,
            "akn_id": "nodeX",
        }
    }
    msg1 = Message(message=msg_body_read, config=client.nodeConfig, outgoing=False)
    msg2 = Message(message=msg_body_unread, config=client.nodeConfig, outgoing=False)
    client.list_of_incoming_messages.extend([msg1, msg2])
    deleted_count = client.clear_messages("incoming", status="read")
    assert deleted_count == 1
    # Verify only the message with status "unread" remains.
    assert client.list_of_incoming_messages[0].body["meta"]["status"] == "unread"

def test_receive_message(client, monkeypatch):
    # Create an incoming message with missing akn_id.
    current_time = str(datetime.datetime.now())
    msg_body = {
        "data": "incoming test",
        "meta": {
            "id": "incoming-1",
            "sender": "nodeB",
            "status": "unread",
            "type": "incoming",
            "category": "test",
            "number": 1,
            "created_at": current_time,
            "arrived_at": None,
            "akn_id": None,
        }
    }
    async def dummy_ack(self, message):
        return
    monkeypatch.setattr(MessageBrokerClient, "acknowledge_message", dummy_ack)
    client.receive_message(msg_body)
    received_msg = client.list_of_incoming_messages[-1]
    # Verify that recipients have been set to the sender.
    assert received_msg.recipients == [msg_body["meta"]["sender"]]