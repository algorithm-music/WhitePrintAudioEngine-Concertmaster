"""
Client for audition service (internal).
POST /internal/analyze-url — JSON {audio_url} → analysis_json
"""

import os
import logging

from concertmaster.clients.auth import get_auth_header
from concertmaster.clients.http_pool import get_client

logger = logging.getLogger("concertmaster.client.audition")

AUDITION_URL = os.environ.get(
    "AUDITION_URL",
    "http://localhost:8081",
)
TIMEOUT = 120.0  # 2 minutes for long tracks


async def analyze(audio_url: str) -> dict:
    """Send audio URL to audition, return analysis_json.

    Args:
        audio_url: Direct download URL for audio file

    Returns:
        analysis_json dict (track_identity, whole_track_metrics, etc.)

    Raises:
        httpx.HTTPStatusError: on 4xx/5xx from audition
        httpx.TimeoutException: on timeout
    """
    url = f"{AUDITION_URL}/internal/analyze-url"
    headers = get_auth_header(AUDITION_URL)
    headers["Content-Type"] = "application/json"

    client = get_client()
    resp = await client.post(
        url,
        json={"audio_url": audio_url},
        headers=headers,
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()
