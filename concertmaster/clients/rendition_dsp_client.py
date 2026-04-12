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
TIMEOUT = 300.0  # 5 minutes for heavy RENDITION_DSP


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
    """Upload audio file + params to RENDITION_DSP iteratively via stream.

    Bypasses 32MB Cloud Run HTTP/1 constraints and allows downstream
    fetching from a secure local temporal directory context.
    
    Args:
        file_path: Absolute local path to audio file
        params: RENDITION_DSP parameter dict (from formplan or manual)
        target_lufs: target LUFS
        target_true_peak: target true peak dBTP

    Returns:
        (mastered_wav_bytes, metrics_dict)
    """
    url = f"{RENDITION_DSP_URL}/internal/master-stream"
    headers = get_auth_header(RENDITION_DSP_URL)
    headers["X-DSP-Params"] = json.dumps(params)
    headers["X-Target-LUFS"] = str(target_lufs)
    headers["X-Target-True-Peak"] = str(target_true_peak)
    if output_url is not None:
        headers["X-Output-URL"] = output_url
    headers["Content-Type"] = "application/octet-stream"

    client = get_client()

    async def file_streamer():
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                yield chunk

    resp = await client.post(
        url,
        content=file_streamer(),
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
