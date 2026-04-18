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
    output_path: str | None = None,
    output_url: str | None = None,
) -> tuple[bytes | None, dict]:
    """Send audio file path + params to RENDITION_DSP.

    Both input and (optional) output paths must live on the shared GCSFuse
    mount so rendition-dsp can read/write without any HTTP transfer.

    Parameter precedence:
      - output_path: rendition-dsp writes the mastered WAV to this path and
        returns JSON with metrics. Preferred.
      - output_url:  rendition-dsp PUTs the mastered WAV to this signed URL.
      - neither:     rendition-dsp streams the WAV back in the HTTP response
        body (subject to Cloud Run's 32 MiB limit).
    """
    url = f"{RENDITION_DSP_URL}/internal/master"
    headers = get_auth_header(RENDITION_DSP_URL)
    headers["Content-Type"] = "application/json"

    client = get_client()

    req_body: dict = {
        "local_path": file_path,
        "params": params,
        "target_lufs": target_lufs,
        "target_true_peak": target_true_peak,
    }
    if output_path:
        req_body["output_path"] = output_path
    if output_url:
        req_body["output_url"] = output_url

    resp = await client.post(
        url,
        json=req_body,
        headers=headers,
        timeout=TIMEOUT,
    )
    resp.raise_for_status()

    if resp.headers.get("content-type", "").startswith("application/json"):
        # output_path or output_url was honored — only metrics came back.
        data = resp.json()
        return None, data.get("metrics", {})

    metrics_raw = resp.headers.get("X-Metrics", "{}")
    try:
        metrics = json.loads(metrics_raw)
    except json.JSONDecodeError:
        metrics = {"error": "failed to parse X-Metrics header"}
        logger.warning(f"Failed to parse X-Metrics: {metrics_raw[:200]}")

    return resp.content, metrics

