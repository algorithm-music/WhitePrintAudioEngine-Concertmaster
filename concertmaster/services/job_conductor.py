"""
Job Conductor — URL fetch + pipeline orchestration.

Fetches audio from shared URLs (Dropbox, Google Drive, OneDrive, S3, any HTTPS).
Routes through audition → deliberation → merge_rule → rendition_dsp based on selected route.

Routes:
  full:          fetch → analyze → deliberation → merge_rule → rendition_dsp → return
  analyze_only:  fetch → analyze → return
  deliberation_only:  fetch → analyze → deliberation → merge_rule → return
  dsp_only:      fetch → rendition_dsp (manual_params required) → return

Stores nothing. Returns everything. Forgets immediately.
"""

import re
import time
import logging
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import httpx

from concertmaster.clients import audition_client, deliberation_client, rendition_dsp_client

logger = logging.getLogger("concertmaster.conductor")

FETCH_TIMEOUT = 60.0  # 60s to download audio from external URL
MAX_AUDIO_SIZE = 200 * 1024 * 1024  # 200MB hard limit


# ══════════════════════════════════════════
# URL Auto-Conversion
# ══════════════════════════════════════════
def normalize_audio_url(url: str) -> str:
    """Convert sharing URLs to direct download URLs.

    Supports:
      - Google Drive: /file/d/ID/view → /uc?export=download&id=ID
      - Dropbox: ?dl=0 → ?dl=1
      - OneDrive: adds download=1
      - S3 signed URLs: pass through
      - Any HTTPS: pass through
    """
    # Google Drive
    gd_match = re.match(
        r"https?://drive\.google\.com/file/d/([^/]+)", url
    )
    if gd_match:
        file_id = gd_match.group(1)
        return f"https://drive.google.com/uc?export=download&id={file_id}"

    # Dropbox
    if "dropbox.com" in url:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        qs["dl"] = ["1"]
        new_query = urlencode(qs, doseq=True)
        return urlunparse(parsed._replace(query=new_query))

    # OneDrive
    if "1drv.ms" in url or "onedrive.live.com" in url:
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}download=1"

    # S3 / generic HTTPS: pass through
    return url


# ══════════════════════════════════════════
# Audio Fetch
# ══════════════════════════════════════════
async def fetch_audio(url: str) -> bytes:
    """Fetch audio from external URL into memory.

    Raises:
        ValueError: URL validation failure
        httpx.HTTPStatusError: non-2xx from source
        httpx.TimeoutException: download timeout
    """
    normalized = normalize_audio_url(url)
    parsed = urlparse(normalized)

    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Unsupported URL scheme: {parsed.scheme}")

    logger.info(f"Fetching audio: {parsed.netloc}{parsed.path[:60]}...")

    async with httpx.AsyncClient(
        timeout=FETCH_TIMEOUT,
        follow_redirects=True,
        max_redirects=5,
    ) as client:
        resp = await client.get(normalized)
        resp.raise_for_status()

        audio_bytes = resp.content

        if len(audio_bytes) > MAX_AUDIO_SIZE:
            raise ValueError(
                f"Audio file too large: {len(audio_bytes) / 1024 / 1024:.1f}MB "
                f"(max {MAX_AUDIO_SIZE / 1024 / 1024:.0f}MB)"
            )

        if len(audio_bytes) < 44:
            raise ValueError("Downloaded file too small to be valid audio")

        logger.info(f"Fetched {len(audio_bytes) / 1024 / 1024:.1f}MB")
        return audio_bytes


# ══════════════════════════════════════════
# Pipeline Routes
# ══════════════════════════════════════════
async def run_full(
    audio_url: str,
    target_lufs: float,
    target_true_peak: float,
    sage_config: dict | None = None,
    dsp_config: dict | None = None,
    output_url: str | None = None,
) -> dict:
    """Full route: analyze(url) → deliberation → fetch → rendition_dsp → return."""
    t0 = time.time()

    # 1. Normalize URL for direct download
    normalized_url = normalize_audio_url(audio_url)

    # 2. Analyze (audition fetches audio itself — no 32MB limit)
    analysis = await audition_client.analyze(normalized_url)

    # 3. Deliberation (3 sages → adopted_params via weighted median merge)
    deliberation_result = await deliberation_client.deliberate(
        analysis=analysis,
        target_lufs=target_lufs,
        target_true_peak=target_true_peak,
        sage_config=sage_config,
    )

    # Extract DSP params from deliberation result (weighted median output)
    dsp_params = deliberation_result.get("adopted_params", {})

    # Apply RENDITION_DSP config overrides
    if dsp_config:
        if "overrides" in dsp_config:
            dsp_params.update(dsp_config["overrides"])

    # 4. RENDITION_DSP (fetches audio itself — no 32MB limit)
    mastered_bytes, dsp_metrics = await rendition_dsp_client.master(
        audio_url=normalized_url,
        params=dsp_params,
        target_lufs=target_lufs,
        target_true_peak=target_true_peak,
        output_url=output_url,
    )

    elapsed_ms = int((time.time() - t0) * 1000)

    return {
        "route": "full",
        "analysis": analysis,
        "deliberation": deliberation_result,
        "dsp_metrics": dsp_metrics,
        "mastered_audio": mastered_bytes,
        "elapsed_ms": elapsed_ms,
    }


async def run_analyze_only(audio_url: str) -> dict:
    """Analyze-only route: analyze(url) → return."""
    t0 = time.time()
    normalized_url = normalize_audio_url(audio_url)
    analysis = await audition_client.analyze(normalized_url)
    elapsed_ms = int((time.time() - t0) * 1000)

    return {
        "route": "analyze_only",
        "analysis": analysis,
        "elapsed_ms": elapsed_ms,
    }


async def run_deliberation_only(
    audio_url: str,
    target_lufs: float,
    target_true_peak: float,
    sage_config: dict | None = None,
) -> dict:
    """Deliberation-only route: analyze(url) → deliberation → return."""
    t0 = time.time()
    normalized_url = normalize_audio_url(audio_url)
    analysis = await audition_client.analyze(normalized_url)
    deliberation_result = await deliberation_client.deliberate(
        analysis=analysis,
        target_lufs=target_lufs,
        target_true_peak=target_true_peak,
        sage_config=sage_config,
    )
    elapsed_ms = int((time.time() - t0) * 1000)

    return {
        "route": "deliberation_only",
        "analysis": analysis,
        "deliberation": deliberation_result,
        "elapsed_ms": elapsed_ms,
    }


async def run_dsp_only(
    audio_url: str,
    manual_params: dict,
    target_lufs: float,
    target_true_peak: float,
    output_url: str | None = None,
) -> dict:
    """RENDITION_DSP-only route: rendition_dsp (manual params, fetches audio itself) → return."""
    t0 = time.time()
    normalized_url = normalize_audio_url(audio_url)
    mastered_bytes, dsp_metrics = await rendition_dsp_client.master(
        audio_url=normalized_url,
        params=manual_params,
        target_lufs=target_lufs,
        target_true_peak=target_true_peak,
        output_url=output_url,
    )
    elapsed_ms = int((time.time() - t0) * 1000)

    return {
        "route": "dsp_only",
        "dsp_metrics": dsp_metrics,
        "mastered_audio": mastered_bytes,
        "elapsed_ms": elapsed_ms,
    }
