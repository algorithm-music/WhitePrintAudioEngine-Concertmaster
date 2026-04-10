"""Tests for RetryTransport in http_pool."""

import pytest
import httpx
from concertmaster.clients.http_pool import RetryTransport


class FakeTransport(httpx.AsyncBaseTransport):
    """Controllable transport for testing retry logic."""

    def __init__(self, responses: list):
        """responses: list of (status_code,) or Exception instances."""
        self._responses = list(responses)
        self.call_count = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.call_count += 1
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        status_code = item
        return httpx.Response(status_code=status_code, request=request)


@pytest.mark.asyncio
async def test_no_retry_on_200():
    fake = FakeTransport([200])
    transport = RetryTransport(fake, max_retries=2)
    req = httpx.Request("GET", "https://example.com")
    resp = await transport.handle_async_request(req)
    assert resp.status_code == 200
    assert fake.call_count == 1


@pytest.mark.asyncio
async def test_retry_on_503_then_success():
    fake = FakeTransport([503, 200])
    transport = RetryTransport(fake, max_retries=2)
    req = httpx.Request("GET", "https://example.com")
    resp = await transport.handle_async_request(req)
    assert resp.status_code == 200
    assert fake.call_count == 2


@pytest.mark.asyncio
async def test_retry_exhausted_returns_last_response():
    fake = FakeTransport([502, 503, 504])
    transport = RetryTransport(fake, max_retries=2)
    req = httpx.Request("GET", "https://example.com")
    resp = await transport.handle_async_request(req)
    # After max retries exhausted, returns the last response
    assert resp.status_code == 504
    assert fake.call_count == 3


@pytest.mark.asyncio
async def test_no_retry_on_400():
    fake = FakeTransport([400])
    transport = RetryTransport(fake, max_retries=2)
    req = httpx.Request("GET", "https://example.com")
    resp = await transport.handle_async_request(req)
    assert resp.status_code == 400
    assert fake.call_count == 1


@pytest.mark.asyncio
async def test_retry_on_connect_error_then_success():
    fake = FakeTransport([httpx.ConnectError("connection refused"), 200])
    transport = RetryTransport(fake, max_retries=2)
    req = httpx.Request("GET", "https://example.com")
    resp = await transport.handle_async_request(req)
    assert resp.status_code == 200
    assert fake.call_count == 2


@pytest.mark.asyncio
async def test_connect_error_exhausted_raises():
    fake = FakeTransport([
        httpx.ConnectError("fail1"),
        httpx.ConnectError("fail2"),
        httpx.ConnectError("fail3"),
    ])
    transport = RetryTransport(fake, max_retries=2)
    req = httpx.Request("GET", "https://example.com")
    with pytest.raises(httpx.ConnectError):
        await transport.handle_async_request(req)
    assert fake.call_count == 3
