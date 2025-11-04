# Python
import pytest
from unittest.mock import AsyncMock, patch
from flamesdk.resources.utils.logging import FlameLogger
from httpx import AsyncClient

from flamesdk.resources.client_apis.clients.data_api_client import DataApiClient

# Dummy response mimicking httpx.Response
class DummyResponse:
    def __init__(self, json_data=None, text_data="", content=b""):
        self._json = json_data or {}
        self.text = text_data
        self.content = content

    def json(self):
        return self._json

    def raise_for_status(self):
        pass

# Dummy async get function to return our DummyResponse instance.
async def dummy_get(url, **kwargs):
    if url.startswith("/kong/datastore/"):
        # Response for _retrieve_available_sources
        return DummyResponse({"data": [{"name": "source1"}]})
    elif "/fhir/" in url:
        # Response for fhir endpoint requests
        return DummyResponse({"result": "fhir-data"})
    elif url.endswith("/s3"):
        # Response for _get_s3_dataset_names returning a key wrapped in <Key> tags
        return DummyResponse(text_data="<Key>key1</Key>")
    elif "/s3/" in url:
        # Response for individual S3 requests
        return DummyResponse(content=b"s3-data")
    return DummyResponse()

def test_data_api_client_init():
    with patch("flamesdk.resources.client_apis.clients.data_api_client.AsyncClient.get", new=AsyncMock(side_effect=dummy_get)):
        flame_logger = FlameLogger()
        client = DataApiClient("proj_id", "nginx", "data_token", "key_token", flame_logger)
        # Verify available sources were set as expected.
        assert client.get_available_sources() == [{"name": "source1"}]


def test_refresh_token():
    # Patch the get method to prevent actual HTTP calls during initialization.
    with patch("flamesdk.resources.client_apis.clients.data_api_client.AsyncClient.get", new=AsyncMock(side_effect=dummy_get)):
        flame_logger = FlameLogger()
        # Create DataApiClient with initial keycloak_token "key_token"
        client = DataApiClient("proj_id", "nginx", "data_token", "key_token", flame_logger)
        # Check initial Authorization header in hub_client
        initial_auth = client.hub_client.headers.get("Authorization")
        assert initial_auth == "Bearer key_token"
        # Refresh token with a new keycloak token
        new_token = "new_key_token"
        client.refresh_token(new_token)
        # Verify that the hub_client has been updated with the new token
        updated_auth = client.hub_client.headers.get("Authorization")
        assert updated_auth == f"Bearer {new_token}"

def test_get_data_fhir():
    fhir_queries = ["query1", "query2"]
    with patch("flamesdk.resources.client_apis.clients.data_api_client.AsyncClient.get", new=AsyncMock(side_effect=dummy_get)):
        flame_logger = FlameLogger()
        client = DataApiClient("proj_id", "nginx", "data_token", "key_token", flame_logger)
        # Call get_data with fhir_queries provided
        results = client.get_data(fhir_queries=fhir_queries)
        # Expect one source with fhir data responses returned for each query.
        expected = {"query1": {"result": "fhir-data"}, "query2": {"result": "fhir-data"}}
        assert results == [expected]

def test_get_data_s3():
    s3_keys = ["key1"]
    with patch("flamesdk.resources.client_apis.clients.data_api_client.AsyncClient.get", new=AsyncMock(side_effect=dummy_get)):
        flame_logger = FlameLogger()
        client = DataApiClient("proj_id", "nginx", "data_token", "key_token", flame_logger)
        # Call get_data with s3_keys provided and without fhir_queries.
        results = client.get_data(s3_keys=s3_keys)
        # Expected one source with S3 data for key1.
        expected = {"key1": b"s3-data"}
        assert results == [expected]

@pytest.fixture
def dummy_sources(monkeypatch):
    sources = [
        {"id": "test_id", "paths": ["http://test_path"]},
        {"id": "other_id", "paths": ["http://other_path"]}
    ]
    async def dummy_retrieve_available_sources(self):
        return sources
    monkeypatch.setattr(DataApiClient, "_retrieve_available_sources", dummy_retrieve_available_sources)
    return sources

@pytest.fixture
def client(dummy_sources):
    flame_logger = FlameLogger()
    return DataApiClient("proj_id", "nginx", "data_token", "key_token",flame_logger)

def test_get_data_source_client_success(client):
    client_obj = client.get_data_source_client("test_id")
    assert isinstance(client_obj, AsyncClient)
    assert str(client_obj.base_url) == "http://test_path"

def test_get_data_source_client_not_found(client):
    with pytest.raises(ValueError) as exc_info:
        client.get_data_source_client("invalid_id")
    assert "Data source with id invalid_id not found" in str(exc_info.value)