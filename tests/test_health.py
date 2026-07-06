"""
Sprint 0 smoke test: confirms the environment is wired correctly
before we build any real logic on top of it.
"""
from fastapi.testclient import TestClient
from mock_api.server import app

client = TestClient(app)


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"