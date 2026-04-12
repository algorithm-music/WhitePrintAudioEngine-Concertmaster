"""
Client for RENDITION_DSP service (internal).
POST /internal/master-url — JSON (audio_url + params) → mastered WAV
"""

import json
import os
import logging

from concertmaster.clients.auth import get_auth_header
from concertmaster.clients.http_pool import get_client

logger = logging.getLogger("concertmaster.client.rendition_dsp")

RENDITION_DSP_URL = os.environ.get(
    "RENDITION_DSP_URL",
    "http://localhost:8083",
)
TIMEOUT = 900.0  # 15 minutes for heavy RENDITION_DSP


async def master(
    audio_url: str,
    params: dict,
    target_lufs: float,
    target_true_peak: float,
    output_url: str | None = None,
) -> tuple[bytes | None, dict]:
    """Send audio URL + params to RENDITION_DSP, return mastered WAV + metrics.

    Args:
        audio_url: Direct download URL for audio file
        params: RENDITION_DSP parameter dict (from formplan or manual)
        target_lufs: target LUFS
        target_true_peak: target true peak dBTP

    Returns:
        (mastered_wav_bytes, metrics_dict)
        metrics_dict parsed from X-Metrics response header.
    """
    url = f"{RENDITION_DSP_URL}/internal/master-url"
    headers = get_auth_header(RENDITION_DSP_URL)
    headers["Content-Type"] = "application/json"

    req_body = {
        "audio_url": audio_url,
        "params": params,
        "target_lufs": target_lufs,
        "target_true_peak": target_true_peak,
    }
    if output_url is not None:
        req_body["output_url"] = output_url

    client = get_client()
    resp = await client.post(
        url,
        json=req_body,
        headers=headers,
        timeout=TIMEOUT,
    )
    resp.raise_for_status()

    if resp.headers.get("content-type", "").startswith("application/json"):
        # Output was uploaded directly to output_url
        data = resp.json()
        return None, data.get("metrics", {})
    else:
        # Parse metrics from X-Metrics header
        metrics_raw = resp.headers.get("X-Metrics", "{}")
        try:
            metrics = json.loads(metrics_raw)
        except json.JSONDecodeError:
            metrics = {"error": "failed to parse X-Metrics header"}
            logger.warning(f"Failed to parse X-Metrics: {metrics_raw[:200]}")

        return resp.content, metrics


async def master_file(
    file_path: str,
    params: dict,
    target_lufs: float,
    target_true_peak: float,
    output_url: str | None = None,
) -> tuple[bytes | None, dict]:
    """Send audio file path and params to RENDITION_DSP.

    By using local paths on the GCS FUSE mount, we bypass HTTP payload limits.
    """
    url = f"{RENDITION_DSP_URL}/internal/master"
    headers = get_auth_header(RENDITION_DSP_URL)
    headers["Content-Type"] = "application/json"

    client = get_client()

    req_body = {
        "local_path": file_path,
        "params": params,
        "target_lufs": target_lufs,
        "target_true_peak": target_true_peak,
    }
    
    if output_url:
        req_body["output_url"] = output_url

    # If an output_url (signed PUT URL) is provided, we can either have Rendition DSP push it
    # directly using X-Output-URL or we continue reading the response bytes here.
    # We will let Rendition DSP return the master file path by passing output_path,
    # OR we can let Rendition DSP stream it back.
    # Right now, Rendition supports returning paths if we pass output_path. We will just stream back normally for now,
    # unless we want to use the output_url mechanism.
    
    # Wait, earlier we allowed Rendition to upload it itself, let's keep the stream return for now
    # but the new /internal/master doesn't support output_url pushing yet?
    # Actually wait, `master_file` in Concertmaster pushes to Supabase afterwards!
    
    # Let's request Rendition to stream back the bytes, so Concertmaster pushes it to Supabase.
    resp = await client.post(
        url,
        json=req_body,
        headers=headers,
        timeout=TIMEOUT,
    )
    resp.raise_for_status()

    metrics_raw = resp.headers.get("X-Metrics", "{}")
    try:
        metrics = json.loads(metrics_raw)
    except json.JSONDecodeError:
        metrics = {"error": "failed to parse X-Metrics header"}
        logger.warning(f"Failed to parse X-Metrics: {metrics_raw[:200]}")

    return resp.content, metrics

