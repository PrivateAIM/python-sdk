import pytest
from flamesdk.resources.client_apis.clients.result_client import ResultClient
from flamesdk.resources.utils.logging import FlameLogger

class DummyClient:
    def __init__(self, *args, **kwargs):
        self.base_url = kwargs.get('base_url')
        self.headers = kwargs.get('headers')
        self.follow_redirects = kwargs.get('follow_redirects')
        self.last_request = None
    def post(self, *args, **kwargs):
        self.last_request = (args, kwargs)
        class DummyResponse:
            def json(self):
                return {'status': 'success'}
            def raise_for_status(self):
                pass
        return DummyResponse()
    def put(self, *args, **kwargs):
        self.last_request = (args, kwargs)
        class DummyResponse:
            def json(self):
                return {'status': 'success'}
            def raise_for_status(self):
                pass
        return DummyResponse()

def test_result_client_init(monkeypatch):
    monkeypatch.setattr('flamesdk.resources.client_apis.clients.result_client.Client', DummyClient)
    flame_logger = FlameLogger()
    client = ResultClient('nginx', 'token', flame_logger)
    assert client.nginx_name == 'nginx'
    assert isinstance(client.client, DummyClient)

def test_refresh_token(monkeypatch):
    monkeypatch.setattr('flamesdk.resources.client_apis.clients.result_client.Client', DummyClient)
    flame_logger = FlameLogger()
    client = ResultClient('nginx', 'token', flame_logger)
    client.refresh_token('newtoken')
    assert client.client.headers['Authorization'] == 'Bearer newtoken'

def test_push_result(monkeypatch):
    monkeypatch.setattr('flamesdk.resources.client_apis.clients.result_client.Client', DummyClient)
    flame_logger = FlameLogger()
    client = ResultClient('nginx', 'token', flame_logger)
    # Patch client.post to simulate a response
    client.client.post = lambda *a, **k: type('R', (), {'json': lambda self: {'status': 'success'}})()
    result = client.push_result(result={'foo': 'bar'})
    assert result['status'] == 'success'
