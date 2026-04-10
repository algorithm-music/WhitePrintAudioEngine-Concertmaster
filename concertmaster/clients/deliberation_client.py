"""
Client for deliberation service (internal).
POST /internal/deliberate — analysis_data → opinions + adopted_params
"""

import os
import logging

from concertmaster.clients.auth import get_auth_header
from concertmaster.clients.http_pool import get_client

logger = logging.getLogger("concertmaster.client.deliberation")

DELIBERATION_URL = os.environ.get(
    "DELIBERATION_URL",
    "http://localhost:8082",
)
TIMEOUT = 180.0  # 3 minutes (3 LLM calls in parallel)


async def deliberate(
    analysis: dict,
    target_lufs: float,
    target_true_peak: float,
    target_platform: str = "streaming",
    sage_config: dict | None = None,
) -> dict:
    """Send analysis to deliberation, return opinions + adopted params.

    Args:
        analysis: analysis_json from audition
        target_lufs: target integrated LUFS
        target_true_peak: target true peak dBTP
        target_platform: target platform (streaming/cd/vinyl/broadcast)
        sage_config: optional sage vendor/model overrides

    Returns:
        {
            "opinions": [...],                  # 3 sages' independent opinions
            "adopted_params": {...},             # Weighted median DSP params
            "deliberation_score": float,         # Global agent agreement level (0-1)
            "deliberation_score_detail": dict,   # Per-category breakdown
        }
    """
    url = f"{DELIBERATION_URL}/internal/deliberate"
    headers = get_auth_header(DELIBERATION_URL)
    headers["Content-Type"] = "application/json"

    payload = {
        "analysis_data": analysis,
        "target_lufs": target_lufs,
        "target_true_peak": target_true_peak,
        "target_platform": target_platform,
    }
    if sage_config:
        payload["sage_config"] = sage_config

    client = get_client()
    resp = await client.post(url, json=payload, headers=headers, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()
