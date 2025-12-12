from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def test_get_gold_files():
    response = client.get("/kpis/files")
    assert response.status_code == 200
    assert "files" in response.json()


def test_get_gold_kpi4():
    response = client.get("/kpis/kpi4")
    assert response.status_code == 200
    assert "data" in response.json()
