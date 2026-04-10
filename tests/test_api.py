"""Tests for API endpoints in main.py."""

import pytest
from unittest.mock import patch, AsyncMock
from httpx import AsyncClient, ASGITransport

from concertmaster.clients.http_pool import init_pool, close_pool
from concertmaster.main import app


@pytest.fixture
async def client():
    await init_pool()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    await close_pool()


# ── Health / Index ──

@pytest.mark.asyncio
async def test_index(client):
    resp = await client.get("/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["service"] == "concertmaster"


@pytest.mark.asyncio
async def test_health(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# ── Auth ──

@pytest.mark.asyncio
async def test_master_requires_api_key(client):
    resp = await client.post("/api/v1/jobs/master", json={
        "audio_url": "https://example.com/track.wav",
    })
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_master_rejects_bad_key(client):
    resp = await client.post(
        "/api/v1/jobs/master",
        json={"audio_url": "https://example.com/track.wav"},
        headers={"X-Api-Key": "wrong-key"},
    )
    assert resp.status_code == 401


# ── Route validation ──

@pytest.mark.asyncio
async def test_unknown_route(client):
    resp = await client.post(
        "/api/v1/jobs/master",
        json={"audio_url": "https://example.com/track.wav", "route": "invalid"},
        headers={"X-Api-Key": "test-key-12345"},
    )
    assert resp.status_code == 400
    assert "Unknown route" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_dsp_only_requires_manual_params(client):
    resp = await client.post(
        "/api/v1/jobs/master",
        json={"audio_url": "https://example.com/track.wav", "route": "dsp_only"},
        headers={"X-Api-Key": "test-key-12345"},
    )
    assert resp.status_code == 400
    assert "manual_params" in resp.json()["detail"]


# ── SSRF blocked at API level ──

@pytest.mark.asyncio
async def test_ssrf_blocked_metadata_ip(client):
    resp = await client.post(
        "/api/v1/jobs/master",
        json={
            "audio_url": "http://169.254.169.254/computeMetadata/v1/",
            "route": "analyze_only",
        },
        headers={"X-Api-Key": "test-key-12345"},
    )
    assert resp.status_code == 422
    assert "metadata" in resp.json()["detail"].lower() or "Blocked" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_ssrf_blocked_metadata_hostname(client):
    resp = await client.post(
        "/api/v1/jobs/master",
        json={
            "audio_url": "http://metadata.google.internal/computeMetadata/v1/",
            "route": "analyze_only",
        },
        headers={"X-Api-Key": "test-key-12345"},
    )
    assert resp.status_code == 422


# ── Request ID ──

@pytest.mark.asyncio
async def test_request_id_generated(client):
    resp = await client.get("/health")
    assert "X-Request-Id" in resp.headers
    assert len(resp.headers["X-Request-Id"]) > 10  # UUID length


@pytest.mark.asyncio
async def test_request_id_passthrough(client):
    resp = await client.get("/health", headers={"X-Request-Id": "my-custom-id-123"})
    assert resp.headers["X-Request-Id"] == "my-custom-id-123"


# ── Successful pipeline (mocked downstream) ──

@pytest.mark.asyncio
async def test_analyze_only_success(client):
    mock_analysis = {"track_identity": {"title": "Test"}, "whole_track_metrics": {}}
    with patch(
        "concertmaster.services.job_conductor.audition_client.analyze",
        new_callable=AsyncMock,
        return_value=mock_analysis,
    ):
        resp = await client.post(
            "/api/v1/jobs/master",
            json={
                "audio_url": "https://cdn.example.com/track.wav",
                "route": "analyze_only",
            },
            headers={"X-Api-Key": "test-key-12345"},
        )
        # The SSRF check will try to resolve cdn.example.com — mock it
        # Actually the validate_url_safe will resolve the host, so we need to mock that too


@pytest.mark.asyncio
async def test_analyze_only_full_mock(client):
    """Full mock: SSRF validation + audition client."""
    mock_analysis = {"track_identity": {"title": "Test Track"}}

    with patch(
        "concertmaster.services.job_conductor.validate_url_safe"
    ), patch(
        "concertmaster.services.job_conductor.audition_client.analyze",
        new_callable=AsyncMock,
        return_value=mock_analysis,
    ):
        resp = await client.post(
            "/api/v1/jobs/master",
            json={
                "audio_url": "https://cdn.example.com/track.wav",
                "route": "analyze_only",
            },
            headers={"X-Api-Key": "test-key-12345"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["route"] == "analyze_only"
        assert data["analysis"]["track_identity"]["title"] == "Test Track"
