"""
Shared httpx.AsyncClient pool with retry transport.

Usage:
    from concertmaster.clients.http_pool import get_client

    client = get_client()
    resp = await client.post(url, json=payload, headers=headers)

Lifecycle managed by FastAPI lifespan in main.py.
"""

import httpx

_client: httpx.AsyncClient | None = None


class RetryTransport(httpx.AsyncBaseTransport):
    """Wraps an httpx transport with automatic retries on transient failures."""

    RETRYABLE_STATUS = {502, 503, 504}
    RETRYABLE_EXCEPTIONS = (httpx.ConnectError, httpx.RemoteProtocolError)

    def __init__(
        self,
        wrapped: httpx.AsyncBaseTransport,
        max_retries: int = 2,
    ):
        self._wrapped = wrapped
        self._max_retries = max_retries

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        last_exc: Exception | None = None
        for attempt in range(1 + self._max_retries):
            try:
                response = await self._wrapped.handle_async_request(request)
                if (
                    attempt < self._max_retries
                    and response.status_code in self.RETRYABLE_STATUS
                ):
                    await response.aclose()
                    continue
                return response
            except self.RETRYABLE_EXCEPTIONS as exc:
                last_exc = exc
                if attempt == self._max_retries:
                    raise
        raise last_exc  # type: ignore[misc]


async def init_pool() -> None:
    """Create the shared client. Call once at app startup."""
    global _client
    transport = RetryTransport(
        httpx.AsyncHTTPTransport(retries=0),
        max_retries=2,
    )
    _client = httpx.AsyncClient(
        transport=transport,
        follow_redirects=True,
        max_redirects=5,
        timeout=httpx.Timeout(300.0, connect=10.0),
    )


async def close_pool() -> None:
    """Close the shared client. Call once at app shutdown."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def get_client() -> httpx.AsyncClient:
    """Return the shared client. Raises if pool not initialized."""
    if _client is None:
        raise RuntimeError("HTTP pool not initialized — call init_pool() first")
    return _client
