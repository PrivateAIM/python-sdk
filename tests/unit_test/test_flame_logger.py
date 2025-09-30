import pytest
from unittest.mock import MagicMock
from flamesdk.resources.utils.logging import FlameLogger, _LOG_TYPE_LITERALS

@pytest.fixture
def mock_po_client():
    return MagicMock()

@pytest.fixture
def flame_logger():
    return FlameLogger()

def test_add_po_client(flame_logger, mock_po_client):
    flame_logger.add_po_api(mock_po_client)
    assert flame_logger.po_api == mock_po_client

def test_set_runstatus(flame_logger):
    flame_logger.set_runstatus("running")
    assert flame_logger.runstatus == "running"

def test_new_log_without_po_client(flame_logger):
    flame_logger.new_log("Test log message", log_type="info")
    assert flame_logger.queue.empty() == False


def test_send_logs_from_queue(flame_logger, mock_po_client):
    
    flame_logger.new_log("Test log message", log_type="info")
    flame_logger.add_po_client(mock_po_client)

    flame_logger.send_logs_from_queue()
    assert flame_logger.queue.empty() == True

def test_declare_log_types(flame_logger):
    new_log_types = {"custom": "info"}
    flame_logger.declare_log_types(new_log_types)
    assert "custom" in _LOG_TYPE_LITERALS
    assert _LOG_TYPE_LITERALS["custom"] == "info"
