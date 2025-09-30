import pytest
import uuid
import datetime
from flamesdk.resources.client_apis.clients.message_broker_client import Message

# Dummy stub for NodeConfig used by Message.
class DummyNodeConfig:
    def __init__(self):
        self.node_id = "node_1"
        self.analysis_id = "analysis_dummy"
        self.nginx_name = "localhost"
        self.keycloak_token = "dummy_token"

    def set_role(self, role: str):
        self.role = role

    def set_node_id(self, node_id: str):
        self.node_id = node_id

# Helper function to generate the current time string
def current_time_str():
    return str(datetime.datetime.now())

# Test for a valid outgoing message.
def test_outgoing_message_valid():
    node_config = DummyNodeConfig()
    # Outgoing message must not include "meta"
    body = {"data": "test outgoing message"}
    message_number = 1
    category = "notification"
    recipients = ["recipient1", "recipient2"]
    msg = Message(message=body,
                  config=node_config,
                  outgoing=True,
                  message_number=message_number,
                  category=category,
                  recipients=recipients)
    # Check if meta was created and contains expected fields.
    meta = msg.body.get("meta")
    assert meta is not None
    assert meta["type"] == "outgoing"
    assert meta["category"] == category
    assert meta["number"] == message_number
    assert meta["sender"] == node_config.node_id
    # Recipients should be preserved
    assert msg.recipients == recipients

# Test for error when outgoing message includes a "meta" field.
def test_outgoing_message_with_meta_error():
    node_config = DummyNodeConfig()
    body = {"meta": {"dummy": "field"}, "data": "test"}
    with pytest.raises(ValueError, match=r"Cannot use field 'meta' in message body"):
        Message(message=body,
                config=node_config,
                outgoing=True,
                message_number=1,
                category="notification",
                recipients=["recipient1"])

# Test for error when message_number is not an integer.
def test_outgoing_message_invalid_message_number():
    node_config = DummyNodeConfig()
    body = {"data": "test"}
    with pytest.raises(ValueError, match=r"did not specify integer value for message_number"):
        Message(message=body,
                config=node_config,
                outgoing=True,
                message_number="not_an_int",
                category="notification",
                recipients=["recipient1"])

# Test for error when category is not a string.
def test_outgoing_message_invalid_category():
    node_config = DummyNodeConfig()
    body = {"data": "test"}
    with pytest.raises(ValueError, match=r"did not specify string value for category"):
        Message(message=body,
                config=node_config,
                outgoing=True,
                message_number=1,
                category=123,
                recipients=["recipient1"])

# Test for error when recipients is not a list of strings.
def test_outgoing_message_invalid_recipients():
    node_config = DummyNodeConfig()
    body = {"data": "test"}
    with pytest.raises(ValueError, match=r"did not specify list of strings"):
        Message(message=body,
                config=node_config,
                outgoing=True,
                message_number=1,
                category="notification",
                recipients="not_a_list")

# Test for an incoming message where meta exists.
def test_incoming_message():
    node_config = DummyNodeConfig()
    # Simulate an incoming message with pre-existing meta data.
    meta = {"sender": "node_2", "status": "unread", "type": "incoming", "akn_id": None, "created_at": current_time_str()}
    body = {"data": "incoming message", "meta": meta.copy()}
    msg = Message(message=body, config=node_config, outgoing=False)
    # Incoming messages set recipients to the sender.
    assert msg.recipients == [meta["sender"]]
    # The meta type should be updated to 'incoming'
    assert msg.body["meta"]["type"] == "incoming"

# Test set_read method.
def test_set_read():
    node_config = DummyNodeConfig()
    body = {"data": "test", "meta" : {"sender": "node_2", "status": "unread", "type": "incoming", "akn_id": None, "created_at": current_time_str()}}
    msg = Message(message=body, config=node_config, outgoing=False)
    # concatenate the meta data, meta is not set in the body
    msg.set_read()
    assert msg.body["meta"]["status"] == "read"