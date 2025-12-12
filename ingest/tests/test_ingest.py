import pytest
from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def test_ingest_world_bank():
    resp = client.get(
        "/ingest/world-bank",
        params={
            "countries": "ARG,BRA",
            "indicators": "NY.GDP.PCAP.CD",
            "start_year": 2000,
            "end_year": 2001,
            "isTest": True,
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {"status": "success"}


def test_ingest_open_meteo_simple():
    resp = client.get(
        "/ingest/open-meteo-simple",
        params={
            "country": "ARG",
            "latitude": -34.6,
            "longitude": -58.4,
            "start_date": "2023-01-01",
            "end_date": "2023-01-10",
            "isTest": True,
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {"status": "success"}


@pytest.mark.asyncio
async def test_ingest_open_meteo():
    resp = client.get(
        "/ingest/open-meteo",
        params={
            "country": "ARG",
            "start_date": "2024-01-01",
            "end_date": "2024-01-05",
            "isTest": True,
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {"status": "success"}


def test_ingest_jmp_isTest():
    resp = client.get("/ingest/jmp", params={"isTest": True})
    assert resp.status_code == 200
    assert resp.json() == {"status": "success"}
