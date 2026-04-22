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

import ipaddress
import re
import socket
import time
import logging
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import httpx

from concertmaster.clients import audition_client, deliberation_client, rendition_dsp_client
from concertmaster.clients.http_pool import get_client
from concertmaster.services.url_resolver import resolve_audio_url, is_known_provider

import os

logger = logging.getLogger("concertmaster.conductor")

FETCH_TIMEOUT = 60.0  # 60s to download audio from external URL


def _get_audio_url(resolved: dict) -> str:
    """Get the audio URL from resolved data. For local files, convert to file:// URL."""
    if resolved["type"] == "url":
        return resolved["value"]
    # For local files, we need to upload to Audition differently
    # For now, use file:// scheme — Audition will need to handle this
    return resolved["value"]


def _cleanup_resolved(resolved: dict):
    """Clean up local files after processing."""
    if resolved["type"] == "file" and os.path.exists(resolved["value"]):
        try:
            os.remove(resolved["value"])
            parent = os.path.dirname(resolved["value"])
            if parent.startswith("/tmp/ytdlp_"):
                os.rmdir(parent)
        except Exception:
            pass
MAX_AUDIO_SIZE = 200 * 1024 * 1024  # 200MB hard limit

# ══════════════════════════════════════════
# SSRF Protection
# ══════════════════════════════════════════
_BLOCKED_HOSTNAMES = {
    "metadata.google.internal",
    "metadata",
}


def _is_private_ip(ip_str: str) -> bool:
    """Check if an IP address is private, loopback, or link-local."""
    try:
        addr = ipaddress.ip_address(ip_str)
        return (
            addr.is_private
            or addr.is_loopback
            or addr.is_link_local
            or addr.is_reserved
            or addr.is_multicast
        )
    except ValueError:
        return False


def validate_url_safe(url: str) -> None:
    """Reject URLs targeting internal/private networks (SSRF protection).

    Raises:
        ValueError: if the URL targets a blocked or private address.
    """
    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Unsupported URL scheme: {parsed.scheme}")

    hostname = parsed.hostname or ""

    # Block known metadata endpoints
    if hostname.lower() in _BLOCKED_HOSTNAMES:
        raise ValueError("Blocked: metadata service access is not allowed")

    # Block GCP metadata IP
    if hostname in ("169.254.169.254",):
        raise ValueError("Blocked: metadata service access is not allowed")

    # Resolve hostname and check all IPs
    try:
        addrinfos = socket.getaddrinfo(hostname, parsed.port or 443)
    except socket.gaierror:
        raise ValueError(f"Cannot resolve hostname: {hostname}")

    for _family, _type, _proto, _canonname, sockaddr in addrinfos:
        ip_str = sockaddr[0]
        if _is_private_ip(ip_str):
            raise ValueError(
                f"Blocked: URL resolves to private/internal address"
            )


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
        ValueError: URL validation failure or SSRF attempt
        httpx.HTTPStatusError: non-2xx from source
        httpx.TimeoutException: download timeout
    """
    normalized = normalize_audio_url(url)
    validate_url_safe(normalized)

    parsed = urlparse(normalized)
    logger.info(f"Fetching audio: {parsed.netloc}{parsed.path[:60]}...")

    client = get_client()
    resp = await client.get(normalized, timeout=FETCH_TIMEOUT)
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
    audio_url: str | None = None,
    target_lufs: float = -14.0,
    target_true_peak: float = -1.0,
    sage_config: dict | None = None,
    dsp_config: dict | None = None,
    input_path: str | None = None,
    output_path: str | None = None,
    output_url: str | None = None,
    on_stage=None,
) -> dict:
    """Full route: analyze → deliberation → rendition_dsp.

    Input selection:
      - input_path: absolute local path on the shared GCSFuse mount. Preferred.
      - audio_url:  remote URL; resolved and (if needed) downloaded first.

    on_stage: optional async callback(stage: str, data: dict) called after each
              pipeline stage completes, for real-time progress reporting.
    """
    t0 = time.time()

    if input_path:
        # Skip url_resolver — input is already on the mount.
        resolved = {"type": "file", "value": input_path}
        normalized_url = input_path
    else:
        if not audio_url:
            raise ValueError("Either input_path or audio_url is required.")
        resolved = await resolve_audio_url(audio_url)
        audio_path = _get_audio_url(resolved)
        if resolved["type"] == "url":
            normalized_url = normalize_audio_url(audio_path)
            validate_url_safe(normalized_url)
        else:
            normalized_url = audio_path

    # 2. Analyze
    if resolved["type"] == "file":
        analysis = await audition_client.analyze_file(normalized_url)
    else:
        analysis = await audition_client.analyze(normalized_url)

    # Emit analysis stage with real measured values
    if on_stage:
        metrics = analysis.get("whole_track_metrics", {})
        guardrails = analysis.get("param_guardrails")
        track_id = analysis.get("track_identity", {})
        problems = analysis.get("detected_problems", [])
        await on_stage("analysis_done", {
            "intermediate": {
                "track_identity": track_id,
                "metrics": {
                    "integrated_lufs": metrics.get("integrated_lufs"),
                    "true_peak_dbtp": metrics.get("true_peak_dbtp"),
                    "crest_db": metrics.get("crest_db"),
                    "low_mono_correlation": metrics.get("low_mono_correlation_below_120hz"),
                    "stereo_width_mean": metrics.get("stereo_width_mean"),
                    "harshness_risk": metrics.get("harshness_risk"),
                    "mud_risk": metrics.get("mud_risk"),
                },
                "guardrails": guardrails,
                "problem_count": len(problems),
            },
        })

    # 3. Deliberation (3 sages → adopted_params via weighted median merge)
    deliberation_result = await deliberation_client.deliberate(
        analysis=analysis,
        target_lufs=target_lufs,
        target_true_peak=target_true_peak,
        sage_config=sage_config,
    )

    # Extract DSP params from deliberation result (weighted median output)
    dsp_params = deliberation_result.get("adopted_params", {})

    # Emit deliberation stage with adopted params
    if on_stage:
        await on_stage("deliberation_done", {
            "intermediate": {
                "track_identity": track_id,
                "metrics": {
                    "integrated_lufs": metrics.get("integrated_lufs"),
                    "true_peak_dbtp": metrics.get("true_peak_dbtp"),
                    "crest_db": metrics.get("crest_db"),
                    "low_mono_correlation": metrics.get("low_mono_correlation_below_120hz"),
                    "stereo_width_mean": metrics.get("stereo_width_mean"),
                    "harshness_risk": metrics.get("harshness_risk"),
                    "mud_risk": metrics.get("mud_risk"),
                },
                "guardrails": guardrails,
                "problem_count": len(problems),
                "adopted_params": dsp_params,
                "target_lufs": deliberation_result.get("target_lufs"),
                "target_true_peak": deliberation_result.get("target_true_peak"),
            },
        })

    # Apply RENDITION_DSP config overrides
    if dsp_config:
        if "overrides" in dsp_config:
            dsp_params.update(dsp_config["overrides"])

    # 4. RENDITION_DSP
    if resolved["type"] == "file":
        mastered_bytes, dsp_metrics = await rendition_dsp_client.master_file(
            file_path=normalized_url,
            params=dsp_params,
            target_lufs=target_lufs,
            target_true_peak=target_true_peak,
            output_path=output_path,
            output_url=output_url,
        )
    else:
        mastered_bytes, dsp_metrics = await rendition_dsp_client.master(
            audio_url=normalized_url,
            params=dsp_params,
            target_lufs=target_lufs,
            target_true_peak=target_true_peak,
            output_url=output_url,
        )

    elapsed_ms = int((time.time() - t0) * 1000)
    if not input_path:
        _cleanup_resolved(resolved)

    return {
        "route": "full",
        "analysis": analysis,
        "deliberation": deliberation_result,
        "dsp_metrics": dsp_metrics,
        "mastered_audio": mastered_bytes,
        "elapsed_ms": elapsed_ms,
    }


async def run_analyze_only(
    audio_url: str | None = None,
    input_path: str | None = None,
) -> dict:
    """Analyze-only route: analyze → return."""
    t0 = time.time()
    if input_path:
        analysis = await audition_client.analyze_file(input_path)
    else:
        if not audio_url:
            raise ValueError("Either input_path or audio_url is required.")
        resolved = await resolve_audio_url(audio_url)
        if resolved["type"] == "file":
            analysis = await audition_client.analyze_file(resolved["value"])
            _cleanup_resolved(resolved)
        else:
            normalized_url = normalize_audio_url(resolved["value"])
            validate_url_safe(normalized_url)
            analysis = await audition_client.analyze(normalized_url)
    elapsed_ms = int((time.time() - t0) * 1000)

    return {
        "route": "analyze_only",
        "analysis": analysis,
        "elapsed_ms": elapsed_ms,
    }


async def run_deliberation_only(
    audio_url: str | None = None,
    target_lufs: float = -14.0,
    target_true_peak: float = -1.0,
    sage_config: dict | None = None,
    input_path: str | None = None,
) -> dict:
    """Deliberation-only route: analyze → deliberation → return."""
    t0 = time.time()
    if input_path:
        analysis = await audition_client.analyze_file(input_path)
    else:
        if not audio_url:
            raise ValueError("Either input_path or audio_url is required.")
        resolved = await resolve_audio_url(audio_url)
        if resolved["type"] == "file":
            analysis = await audition_client.analyze_file(resolved["value"])
            _cleanup_resolved(resolved)
        else:
            normalized_url = normalize_audio_url(resolved["value"])
            validate_url_safe(normalized_url)
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
    audio_url: str | None = None,
    manual_params: dict | None = None,
    target_lufs: float = -14.0,
    target_true_peak: float = -1.0,
    input_path: str | None = None,
    output_path: str | None = None,
    output_url: str | None = None,
) -> dict:
    """RENDITION_DSP-only route: rendition_dsp (manual params) → return."""
    t0 = time.time()
    params = manual_params or {}
    if input_path:
        mastered_bytes, dsp_metrics = await rendition_dsp_client.master_file(
            file_path=input_path,
            params=params,
            target_lufs=target_lufs,
            target_true_peak=target_true_peak,
            output_path=output_path,
            output_url=output_url,
        )
        resolved = None
    else:
        if not audio_url:
            raise ValueError("Either input_path or audio_url is required.")
        resolved = await resolve_audio_url(audio_url)
        if resolved["type"] == "file":
            mastered_bytes, dsp_metrics = await rendition_dsp_client.master_file(
                file_path=resolved["value"],
                params=params,
                target_lufs=target_lufs,
                target_true_peak=target_true_peak,
                output_path=output_path,
                output_url=output_url,
            )
        else:
            normalized_url = normalize_audio_url(resolved["value"])
            validate_url_safe(normalized_url)
            mastered_bytes, dsp_metrics = await rendition_dsp_client.master(
                audio_url=normalized_url,
                params=params,
                target_lufs=target_lufs,
                target_true_peak=target_true_peak,
                output_url=output_url,
            )
    elapsed_ms = int((time.time() - t0) * 1000)
    if resolved is not None:
        _cleanup_resolved(resolved)

    return {
        "route": "dsp_only",
        "dsp_metrics": dsp_metrics,
        "mastered_audio": mastered_bytes,
        "elapsed_ms": elapsed_ms,
    }
